import posixpath as path
from itertools import chain
from threading import Lock
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple
from urllib.parse import urlparse
from uuid import uuid4

import agate
from botocore.exceptions import ClientError

from dbt.adapters.athena import AthenaConnectionManager
from dbt.adapters.athena.config import get_boto3_config
from dbt.adapters.athena.relation import AthenaRelation, AthenaSchemaSearchMap
from dbt.adapters.athena.utils import clean_sql_comment
from dbt.adapters.base import available
from dbt.adapters.base.impl import GET_CATALOG_MACRO_NAME
from dbt.adapters.base.relation import BaseRelation, InformationSchema
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import CompiledNode
from dbt.events import AdapterLogger
from dbt.exceptions import DbtRuntimeError

logger = AdapterLogger("Athena")

boto3_client_lock = Lock()


class AthenaAdapter(SQLAdapter):
    ConnectionManager = AthenaConnectionManager
    Relation = AthenaRelation

    relation_type_map = {
        "EXTERNAL_TABLE": "table",
        "MANAGED_TABLE": "table",
        "VIRTUAL_VIEW": "view",
        "table": "table",
        "view": "view",
        "cte": "cte",
        "materializedview": "materializedview",
    }

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "double" if decimals else "integer"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "timestamp"

    @available
    def s3_table_prefix(self, s3_data_dir: Optional[str]) -> str:
        """
        Returns the root location for storing tables in S3.
        This is `s3_data_dir`, if set, and `s3_staging_dir/tables/` if not.
        We generate a value here even if `s3_data_dir` is not set,
        since creating a seed table requires a non-default location.
        """
        conn = self.connections.get_thread_connection()
        creds = conn.credentials
        if s3_data_dir is not None:
            return s3_data_dir
        else:
            return path.join(creds.s3_staging_dir, "tables")

    @available
    def s3_table_location(
        self,
        s3_data_dir: Optional[str],
        s3_data_naming: str,
        schema_name: str,
        table_name: str,
        external_location: Optional[str] = None,
        is_temporary_table: bool = False,
    ) -> str:
        """
        Returns either a UUID or database/table prefix for storing a table,
        depending on the value of s3_table
        """
        if external_location and not is_temporary_table:
            return external_location.rstrip("/")

        mapping = {
            "uuid": path.join(self.s3_table_prefix(s3_data_dir), str(uuid4())),
            "table": path.join(self.s3_table_prefix(s3_data_dir), table_name),
            "table_unique": path.join(self.s3_table_prefix(s3_data_dir), table_name, str(uuid4())),
            "schema_table": path.join(self.s3_table_prefix(s3_data_dir), schema_name, table_name),
            "schema_table_unique": path.join(self.s3_table_prefix(s3_data_dir), schema_name, table_name, str(uuid4())),
        }
        table_location = mapping.get(s3_data_naming)

        if table_location is None:
            raise ValueError(f"Unknown value for s3_data_naming: {s3_data_naming}")

        return table_location

    @available
    def get_table_location(self, database_name: str, table_name: str) -> [str, None]:
        """
        Helper function to S3 get table location
        """
        conn = self.connections.get_thread_connection()
        client = conn.handle
        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name, config=get_boto3_config())
        try:
            table = glue_client.get_table(DatabaseName=database_name, Name=table_name)
            table_location = table["Table"]["StorageDescriptor"]["Location"]
            logger.debug(f"{database_name}.{table_name} is stored in {table_location}")
            return table_location
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                logger.debug(f"Table '{table_name}' does not exists - Ignoring")
                return

    @available
    def clean_up_partitions(self, database_name: str, table_name: str, where_condition: str):
        conn = self.connections.get_thread_connection()
        client = conn.handle

        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name, config=get_boto3_config())
        paginator = glue_client.get_paginator("get_partitions")
        partition_params = {
            "DatabaseName": database_name,
            "TableName": table_name,
            "Expression": where_condition,
            "ExcludeColumnSchema": True,
        }
        partition_pg = paginator.paginate(**partition_params)
        partitions = partition_pg.build_full_result().get("Partitions")
        for partition in partitions:
            self.delete_from_s3(partition["StorageDescriptor"]["Location"])

    @available
    def clean_up_table(self, database_name: str, table_name: str):
        table_location = self.get_table_location(database_name, table_name)

        # this check avoid issues for when the table location is an empty string
        # or when the table do not exist and table location is None
        if table_location:
            self.delete_from_s3(table_location)

    @available
    def quote_seed_column(self, column: str, quote_config: Optional[bool]) -> str:
        return super().quote_seed_column(column, False)

    @available
    def delete_from_s3(self, s3_path: str):
        """
        Deletes files from s3 given a s3 path in the format: s3://my_bucket/prefix
        Additionally, parses the response from the s3 delete request and raises
        a DbtRuntimeError in case it included errors.
        """
        conn = self.connections.get_thread_connection()
        client = conn.handle
        bucket_name, prefix = self._parse_s3_path(s3_path)
        if self._s3_path_exists(bucket_name, prefix):
            s3_resource = client.session.resource("s3", region_name=client.region_name, config=get_boto3_config())
            s3_bucket = s3_resource.Bucket(bucket_name)
            logger.debug(f"Deleting table data: path='{s3_path}', bucket='{bucket_name}', prefix='{prefix}'")
            response = s3_bucket.objects.filter(Prefix=prefix).delete()
            is_all_successful = True
            for res in response:
                if "Errors" in res:
                    for err in res["Errors"]:
                        is_all_successful = False
                        logger.error(
                            "Failed to delete files: Key='{}', Code='{}', Message='{}', s3_bucket='{}'",
                            err["Key"],
                            err["Code"],
                            err["Message"],
                            bucket_name,
                        )
            if is_all_successful is False:
                raise DbtRuntimeError("Failed to delete files from S3.")
        else:
            logger.debug("S3 path does not exist")

    @staticmethod
    def _parse_s3_path(s3_path: str) -> Tuple[str, str]:
        """
        Parses and splits a s3 path into bucket name and prefix.
        This assumes that s3_path is a prefix instead of a URI. It adds a
        trailing slash to the prefix, if there is none.
        """
        o = urlparse(s3_path, allow_fragments=False)
        bucket_name = o.netloc
        prefix = o.path.lstrip("/").rstrip("/") + "/"
        return bucket_name, prefix

    def _s3_path_exists(self, s3_bucket: str, s3_prefix: str) -> bool:
        """Checks whether a given s3 path exists."""
        conn = self.connections.get_thread_connection()
        client = conn.handle
        with boto3_client_lock:
            s3_client = client.session.client("s3", region_name=client.region_name, config=get_boto3_config())
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
        return True if "Contents" in response else False

    def _join_catalog_table_owners(self, table: agate.Table, manifest: Manifest) -> agate.Table:
        owners = []
        # Get the owner for each model from the manifest
        for node in manifest.nodes.values():
            if node.resource_type == "model":
                owners.append(
                    {
                        "table_database": node.database,
                        "table_schema": node.schema,
                        "table_name": node.alias,
                        "table_owner": node.config.meta.get("owner"),
                    }
                )
        owners_table = agate.Table.from_object(owners)

        # Join owners with the results from catalog
        join_keys = ["table_database", "table_schema", "table_name"]
        return table.join(
            right_table=owners_table,
            left_key=join_keys,
            right_key=join_keys,
        )

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Dict[str, Optional[Set[str]]],
        manifest: Manifest,
    ) -> agate.Table:
        kwargs = {"information_schema": information_schema, "schemas": schemas}
        table = self.execute_macro(
            GET_CATALOG_MACRO_NAME,
            kwargs=kwargs,
            # pass in the full manifest so we get any local project
            # overrides
            manifest=manifest,
        )

        filtered_table = self._catalog_filter_table(table, manifest)
        return self._join_catalog_table_owners(filtered_table, manifest)

    def _get_catalog_schemas(self, manifest: Manifest) -> AthenaSchemaSearchMap:
        info_schema_name_map = AthenaSchemaSearchMap()
        nodes: Iterator[CompiledNode] = chain(
            [node for node in manifest.nodes.values() if (node.is_relational and not node.is_ephemeral_model)],
            manifest.sources.values(),
        )
        for node in nodes:
            relation = self.Relation.create_from(self.config, node)
            info_schema_name_map.add(relation)
        return info_schema_name_map

    def _get_data_catalog(self, catalog_name):
        conn = self.connections.get_thread_connection()
        client = conn.handle
        with boto3_client_lock:
            athena_client = client.session.client("athena", region_name=client.region_name, config=get_boto3_config())

        response = athena_client.get_data_catalog(Name=catalog_name)
        return response["DataCatalog"]

    def list_relations_without_caching(
        self,
        schema_relation: AthenaRelation,
    ) -> List[BaseRelation]:
        catalog_id = None
        if schema_relation.database is not None and schema_relation.database.lower() != "awsdatacatalog":
            data_catalog = self._get_data_catalog(schema_relation.database.lower())
            # For non-Glue Data Catalogs, use the original Athena query against INFORMATION_SCHEMA approach
            if data_catalog["Type"] != "GLUE":
                return super().list_relations_without_caching(schema_relation)
            else:
                catalog_id = data_catalog["Parameters"]["catalog-id"]

        conn = self.connections.get_thread_connection()
        client = conn.handle
        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name, config=get_boto3_config())
        paginator = glue_client.get_paginator("get_tables")

        kwargs = {
            "DatabaseName": schema_relation.schema,
        }
        # If the catalog is `awsdatacatalog` we don't need to pass CatalogId as boto3 infers it from the account Id.
        if catalog_id:
            kwargs["CatalogId"] = catalog_id
        page_iterator = paginator.paginate(**kwargs)

        relations = []
        quote_policy = {"database": True, "schema": True, "identifier": True}

        try:
            for page in page_iterator:
                tables = page["TableList"]
                for table in tables:
                    if "TableType" not in table:
                        logger.debug(f"Table '{table['Name']}' has no TableType attribute - Ignoring")
                        continue
                    _type = table["TableType"]
                    if _type == "VIRTUAL_VIEW":
                        _type = self.Relation.View
                    else:
                        _type = self.Relation.Table

                    relations.append(
                        self.Relation.create(
                            schema=schema_relation.schema,
                            database=schema_relation.database,
                            identifier=table["Name"],
                            quote_policy=quote_policy,
                            type=_type,
                        )
                    )
        except ClientError as e:
            # don't error out when schema doesn't exist
            # this allows dbt to create and manage schemas/databases
            logger.debug(f"Schema '{schema_relation.schema}' does not exist - Ignoring: {e}")

        return relations

    @available
    def get_table_type(self, db_name, table_name):
        conn = self.connections.get_thread_connection()
        client = conn.handle

        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name, config=get_boto3_config())

        try:
            response = glue_client.get_table(DatabaseName=db_name, Name=table_name)
            _type = self.relation_type_map.get(response.get("Table", {}).get("TableType", "Table"))
            _specific_type = response.get("Table", {}).get("Parameters", {}).get("table_type", "")

            if _specific_type.lower() == "iceberg":
                _type = "iceberg_table"

            if _type is None:
                raise ValueError("Table type cannot be None")

            logger.debug("table_name : " + table_name)
            logger.debug("table type : " + _type)

            return _type

        except glue_client.exceptions.EntityNotFoundException as e:
            logger.debug(f"Error calling Glue get_table: {e}")

    @available
    def swap_table(self, src_database: str, src_table_name: str, target_database: str, target_table_name: str):
        conn = self.connections.get_thread_connection()
        client = conn.handle

        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name, config=get_boto3_config())

        src_table = glue_client.get_table(DatabaseName=src_database, Name=src_table_name).get("Table")
        src_table_partitions = glue_client.get_partitions(DatabaseName=src_database, TableName=src_table_name).get(
            "Partitions"
        )

        target_table_partitions = glue_client.get_partitions(
            DatabaseName=target_database, TableName=target_table_name
        ).get("Partitions")

        target_table_version = {
            "Name": target_table_name,
            "StorageDescriptor": src_table["StorageDescriptor"],
            "PartitionKeys": src_table["PartitionKeys"],
            "TableType": src_table["TableType"],
            "Parameters": src_table["Parameters"],
            "Description": src_table.get("Description", ""),
        }

        # perform a table swap
        glue_client.update_table(DatabaseName=target_database, TableInput=target_table_version)
        logger.debug(
            f"Table {target_database}.{target_table_name} swapped with the contend of {src_database}.{src_table}"
        )

        # we delete the target table partitions in any case
        # if source table has partitions we need to delete and add partitions
        # it source table hasn't any partitions we need to delete target table partitions
        if target_table_partitions:
            glue_client.batch_delete_partition(
                DatabaseName=target_database,
                TableName=target_table_name,
                PartitionsToDelete=[{"Values": i["Values"]} for i in target_table_partitions],
            )

        if src_table_partitions:
            glue_client.batch_create_partition(
                DatabaseName=target_database,
                TableName=target_table_name,
                PartitionInputList=[
                    {"Values": p["Values"], "StorageDescriptor": p["StorageDescriptor"], "Parameters": p["Parameters"]}
                    for p in src_table_partitions
                ],
            )

    def _get_glue_table_versions_to_expire(self, database_name: str, table_name: str, to_keep: int):
        """
        Given a table an the amount of its version to keep, it returns the versions to delete
        """
        conn = self.connections.get_thread_connection()
        client = conn.handle

        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name, config=get_boto3_config())

        paginator = glue_client.get_paginator("get_table_versions")
        response_iterator = paginator.paginate(
            **{
                "DatabaseName": database_name,
                "TableName": table_name,
            }
        )
        table_versions = response_iterator.build_full_result().get("TableVersions")
        logger.debug(f"Total table versions: {[v['VersionId'] for v in table_versions]}")
        table_versions_ordered = sorted(table_versions, key=lambda i: int(i["Table"]["VersionId"]), reverse=True)
        return table_versions_ordered[int(to_keep) :]

    @available
    def expire_glue_table_versions(self, database_name: str, table_name: str, to_keep: int, delete_s3: bool):
        conn = self.connections.get_thread_connection()
        client = conn.handle

        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name, config=get_boto3_config())

        versions_to_delete = self._get_glue_table_versions_to_expire(database_name, table_name, to_keep)
        logger.debug(f"Versions to delete: {[v['VersionId'] for v in versions_to_delete]}")

        deleted_versions = []
        for v in versions_to_delete:
            version = v["Table"]["VersionId"]
            location = v["Table"]["StorageDescriptor"]["Location"]
            try:
                glue_client.delete_table_version(
                    DatabaseName=database_name, TableName=table_name, VersionId=str(version)
                )
                deleted_versions.append(version)
                logger.debug(f"Deleted version {version} of table {database_name}.{table_name} ")
                if delete_s3:
                    self.delete_from_s3(location)
            except Exception as err:
                logger.debug(f"There was an error when expiring table version {version} with error: {err}")

            logger.debug(f"{location} was deleted")

        return deleted_versions

    @available
    def persist_docs_to_glue(
        self,
        relation: AthenaRelation,
        model: Dict[str, Any],
        persist_relation_docs: bool = False,
        persist_column_docs: bool = False,
    ):
        conn = self.connections.get_thread_connection()
        client = conn.handle

        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name, config=get_boto3_config())

        table = glue_client.get_table(DatabaseName=relation.schema, Name=relation.name).get("Table")
        updated_table = {
            "Name": table["Name"],
            "StorageDescriptor": table["StorageDescriptor"],
            "PartitionKeys": table.get("PartitionKeys", []),
            "TableType": table["TableType"],
            "Parameters": table.get("Parameters", {}),
            "Description": table.get("Description", ""),
        }
        if persist_relation_docs:
            table_comment = clean_sql_comment(model["description"])
            updated_table["Description"] = table_comment
            updated_table["Parameters"]["comment"] = table_comment

        if persist_column_docs:
            for col_obj in updated_table["StorageDescriptor"]["Columns"]:
                col_name = col_obj["Name"]
                col_comment = model["columns"].get(col_name, {}).get("description")
                if col_comment:
                    col_obj["Comment"] = clean_sql_comment(col_comment)

        glue_client.update_table(DatabaseName=relation.schema, TableInput=updated_table)
