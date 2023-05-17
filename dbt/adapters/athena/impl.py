import csv
import os
import posixpath as path
import tempfile
from itertools import chain
from textwrap import dedent
from threading import Lock
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple
from urllib.parse import urlparse
from uuid import uuid4

import agate
from botocore.exceptions import ClientError
from mypy_boto3_athena.type_defs import DataCatalogTypeDef

from dbt.adapters.athena import AthenaConnectionManager
from dbt.adapters.athena.column import AthenaColumn
from dbt.adapters.athena.config import get_boto3_config
from dbt.adapters.athena.constants import LOGGER
from dbt.adapters.athena.exceptions import (
    S3LocationException,
    SnapshotMigrationRequired,
)
from dbt.adapters.athena.relation import (
    RELATION_TYPE_MAP,
    AthenaRelation,
    AthenaSchemaSearchMap,
    get_table_type,
)
from dbt.adapters.athena.s3 import S3DataNaming
from dbt.adapters.athena.utils import clean_sql_comment, get_catalog_id
from dbt.adapters.base import ConstraintSupport, available
from dbt.adapters.base.relation import BaseRelation, InformationSchema
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import CompiledNode, ConstraintType
from dbt.exceptions import DbtRuntimeError

boto3_client_lock = Lock()


class AthenaAdapter(SQLAdapter):
    ConnectionManager = AthenaConnectionManager
    Relation = AthenaRelation

    # There is no such concept as constraints in Athena
    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
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

    @classmethod
    def parse_lf_response(
        cls,
        response: Dict[str, Any],
        database: str,
        table: Optional[str],
        columns: Optional[List[str]],
        lf_tags: Dict[str, str],
    ) -> str:
        failures = response.get("Failures", [])
        tbl_appendix = f".{table}" if table else ""
        columns_appendix = f" for columns {columns}" if columns else ""
        msg_appendix = tbl_appendix + columns_appendix
        if failures:
            base_msg = f"Failed to add LF tags: {lf_tags} to {database}" + msg_appendix
            for failure in failures:
                tag = failure.get("LFTag", {}).get("TagKey")
                error = failure.get("Error", {}).get("ErrorMessage")
                LOGGER.error(f"Failed to set {tag} for {database}" + msg_appendix + f" - {error}")
            raise DbtRuntimeError(base_msg)
        return f"Added LF tags: {lf_tags} to {database}" + msg_appendix

    @classmethod
    def lf_tags_columns_is_valid(cls, lf_tags_columns: Dict[str, Dict[str, List[str]]]) -> Optional[bool]:
        if not lf_tags_columns:
            return False
        for tag_key, tag_config in lf_tags_columns.items():
            if isinstance(tag_config, Dict):
                for tag_value, columns in tag_config.items():
                    if not isinstance(columns, List):
                        raise DbtRuntimeError(f"Not a list: {columns}. " + "Expected format: ['c1', 'c2']")
            else:
                raise DbtRuntimeError(f"Not a dict: {tag_config}. " + "Expected format: {'tag_value': ['c1', 'c2']}")
        return True

    # TODO: Add more lf-tag unit tests when moto supports lakeformation
    # moto issue: https://github.com/getmoto/moto/issues/5964
    @available
    def add_lf_tags(
        self,
        database: str,
        table: str = None,
        lf_tags: Optional[Dict[str, str]] = None,
        lf_tags_columns: Optional[Dict[str, Dict[str, List[str]]]] = None,
    ):
        conn = self.connections.get_thread_connection()
        client = conn.handle

        lf_tags = lf_tags or conn.credentials.lf_tags

        if not lf_tags and not lf_tags_columns:
            LOGGER.debug("No LF tags configured")
        else:
            with boto3_client_lock:
                lf_client = client.session.client(
                    "lakeformation", region_name=client.region_name, config=get_boto3_config()
                )

            if lf_tags:
                resource = {"Database": {"Name": database}}
                if table:
                    resource = {"Table": {"DatabaseName": database, "Name": table}}

                response = lf_client.add_lf_tags_to_resource(
                    Resource=resource, LFTags=[{"TagKey": key, "TagValues": [value]} for key, value in lf_tags.items()]
                )
                LOGGER.debug(self.parse_lf_response(response, database, table, None, lf_tags))

            if self.lf_tags_columns_is_valid(lf_tags_columns):
                for tag_key, tag_config in lf_tags_columns.items():
                    for tag_value, columns in tag_config.items():
                        response = lf_client.add_lf_tags_to_resource(
                            Resource={
                                "TableWithColumns": {"DatabaseName": database, "Name": table, "ColumnNames": columns}
                            },
                            LFTags=[{"TagKey": tag_key, "TagValues": [tag_value]}],
                        )
                        LOGGER.debug(self.parse_lf_response(response, database, table, columns, {tag_key: tag_value}))

    @available
    def is_work_group_output_location_enforced(self) -> bool:
        conn = self.connections.get_thread_connection()
        creds = conn.credentials
        client = conn.handle

        with boto3_client_lock:
            athena_client = client.session.client("athena", region_name=client.region_name, config=get_boto3_config())

        if creds.work_group:
            work_group = athena_client.get_work_group(WorkGroup=creds.work_group)
            output_location = (
                work_group.get("WorkGroup", {})
                .get("Configuration", {})
                .get("ResultConfiguration", {})
                .get("OutputLocation", None)
            )

            output_location_enforced = (
                work_group.get("WorkGroup", {}).get("Configuration", {}).get("EnforceWorkGroupConfiguration", False)
            )

            return output_location is not None and output_location_enforced
        else:
            return False

    def _s3_table_prefix(self, s3_data_dir: Optional[str]) -> str:
        """
        Returns the root location for storing tables in S3.
        This is `s3_data_dir`, if set at the model level, the s3_data_dir of the connection if provided,
        and `s3_staging_dir/tables/` if nothing provided as data dir.
        We generate a value here even if `s3_data_dir` is not set,
        since creating a seed table requires a non-default location.
        """
        conn = self.connections.get_thread_connection()
        creds = conn.credentials
        if s3_data_dir is not None:
            return s3_data_dir

        return path.join(creds.s3_staging_dir, "tables")

    def _s3_data_naming(self, s3_data_naming: Optional[str]) -> Optional[S3DataNaming]:
        """
        Returns the s3 data naming strategy if provided, otherwise the value from the connection.
        """
        conn = self.connections.get_thread_connection()
        creds = conn.credentials
        if s3_data_naming is not None:
            return S3DataNaming(s3_data_naming)

        return S3DataNaming(creds.s3_data_naming) or S3DataNaming.TABLE_UNIQUE

    @available
    def generate_s3_location(
        self,
        relation: AthenaRelation,
        s3_data_dir: Optional[str] = None,
        s3_data_naming: Optional[str] = None,
        external_location: Optional[str] = None,
        is_temporary_table: bool = False,
    ) -> str:
        """
        Returns either a UUID or database/table prefix for storing a table,
        depending on the value of s3_table
        """
        if external_location and not is_temporary_table:
            return external_location.rstrip("/")

        s3_path_table_part = relation.s3_path_table_part or relation.identifier
        schema_name = relation.schema
        s3_data_naming = self._s3_data_naming(s3_data_naming)
        table_prefix = self._s3_table_prefix(s3_data_dir)

        mapping = {
            S3DataNaming.UNIQUE: path.join(table_prefix, str(uuid4())),
            S3DataNaming.TABLE: path.join(table_prefix, s3_path_table_part),
            S3DataNaming.TABLE_UNIQUE: path.join(table_prefix, s3_path_table_part, str(uuid4())),
            S3DataNaming.SCHEMA_TABLE: path.join(table_prefix, schema_name, s3_path_table_part),
            S3DataNaming.SCHEMA_TABLE_UNIQUE: path.join(table_prefix, schema_name, s3_path_table_part, str(uuid4())),
        }

        return mapping[s3_data_naming]

    @available
    def get_glue_table_location(self, relation: AthenaRelation) -> Optional[str]:
        """
        Helper function to get location of a relation in S3.
        Will return None if the table does not exist or does not have a location (views)
        """
        conn = self.connections.get_thread_connection()
        client = conn.handle
        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name, config=get_boto3_config())

        try:
            table = glue_client.get_table(DatabaseName=relation.schema, Name=relation.identifier)
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                LOGGER.debug(f"Table {relation.render()} does not exists - Ignoring")
                return None
            raise e

        table_type = get_table_type(table["Table"])
        table_location = table["Table"].get("StorageDescriptor", {}).get("Location")
        if table_type.is_physical():
            if not table_location:
                raise S3LocationException(
                    f"Relation {relation.render()} is of type '{table_type.value}' which requires a location, "
                    f"but no location returned by Glue."
                )
            LOGGER.debug(f"{relation.render()} is stored in {table_location}")
            return table_location

        return None

    @available
    def clean_up_partitions(self, relation: AthenaRelation, where_condition: str):
        conn = self.connections.get_thread_connection()
        client = conn.handle

        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name, config=get_boto3_config())
        paginator = glue_client.get_paginator("get_partitions")
        partition_params = {
            "DatabaseName": relation.schema,
            "TableName": relation.identifier,
            "Expression": where_condition,
            "ExcludeColumnSchema": True,
        }
        partition_pg = paginator.paginate(**partition_params)
        partitions = partition_pg.build_full_result().get("Partitions")
        for partition in partitions:
            self.delete_from_s3(partition["StorageDescriptor"]["Location"])

    @available
    def clean_up_table(self, relation: AthenaRelation):
        table_location = self.get_glue_table_location(relation)

        # this check avoid issues for when the table location is an empty string
        # or when the table do not exist and table location is None
        if table_location:
            self.delete_from_s3(table_location)

    @available
    def quote_seed_column(self, column: str, quote_config: Optional[bool]) -> str:
        return super().quote_seed_column(column, False)

    @available
    def upload_seed_to_s3(
        self,
        relation: AthenaRelation,
        table: agate.Table,
        s3_data_dir: Optional[str] = None,
        s3_data_naming: Optional[str] = None,
        external_location: Optional[str] = None,
    ) -> str:
        conn = self.connections.get_thread_connection()
        client = conn.handle

        # TODO: consider using the workgroup default location when configured
        s3_location = self.generate_s3_location(
            relation, s3_data_dir, s3_data_naming, external_location=external_location
        )
        bucket, prefix = self._parse_s3_path(s3_location)

        file_name = f"{relation.identifier}.csv"
        object_name = path.join(prefix, file_name)

        with boto3_client_lock:
            s3_client = client.session.client("s3", region_name=client.region_name, config=get_boto3_config())
            # This ensures cross-platform support, tempfile.NamedTemporaryFile does not
            tmpfile = os.path.join(tempfile.gettempdir(), os.urandom(24).hex())
            table.to_csv(tmpfile, quoting=csv.QUOTE_NONNUMERIC)
            s3_client.upload_file(tmpfile, bucket, object_name)
            os.remove(tmpfile)

        return s3_location

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
            LOGGER.debug(f"Deleting table data: path='{s3_path}', bucket='{bucket_name}', prefix='{prefix}'")
            response = s3_bucket.objects.filter(Prefix=prefix).delete()
            is_all_successful = True
            for res in response:
                if "Errors" in res:
                    for err in res["Errors"]:
                        is_all_successful = False
                        LOGGER.error(
                            "Failed to delete files: Key='{}', Code='{}', Message='{}', s3_bucket='{}'",
                            err["Key"],
                            err["Code"],
                            err["Message"],
                            bucket_name,
                        )
            if is_all_successful is False:
                raise DbtRuntimeError("Failed to delete files from S3.")
        else:
            LOGGER.debug("S3 path does not exist")

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

    def _get_one_table_for_catalog(self, table: dict, database: str) -> list:
        table_catalog = {
            "table_database": database,
            "table_schema": table["DatabaseName"],
            "table_name": table["Name"],
            "table_type": RELATION_TYPE_MAP[table.get("TableType", "EXTERNAL_TABLE")].value,
            "table_comment": table.get("Parameters", {}).get("comment", table.get("Description", "")),
        }
        return [
            {
                **table_catalog,
                **{
                    "column_name": col["Name"],
                    "column_index": idx,
                    "column_type": col["Type"],
                    "column_comment": col.get("Comment", ""),
                },
            }
            for idx, col in enumerate(table["StorageDescriptor"]["Columns"] + table.get("PartitionKeys", []))
        ]

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Dict[str, Optional[Set[str]]],
        manifest: Manifest,
    ) -> agate.Table:
        data_catalog = self._get_data_catalog(information_schema.path.database)
        catalog_id = get_catalog_id(data_catalog)
        conn = self.connections.get_thread_connection()
        client = conn.handle
        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name, config=get_boto3_config())

        catalog = []
        paginator = glue_client.get_paginator("get_tables")
        for schema, relations in schemas.items():
            kwargs = {
                "DatabaseName": schema,
                "MaxResults": 100,
            }
            # If the catalog is `awsdatacatalog` we don't need to pass CatalogId as boto3 infers it from the account Id.
            if catalog_id:
                kwargs["CatalogId"] = catalog_id

            for page in paginator.paginate(**kwargs):
                for table in page["TableList"]:
                    if table["Name"] in relations:
                        catalog.extend(self._get_one_table_for_catalog(table, information_schema.path.database))

        table = agate.Table.from_object(catalog)
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

    def _get_data_catalog(self, database: str) -> Optional[DataCatalogTypeDef]:
        if database:
            conn = self.connections.get_thread_connection()
            client = conn.handle
            if database.lower() == "awsdatacatalog":
                with boto3_client_lock:
                    sts = client.session.client("sts", region_name=client.region_name, config=get_boto3_config())
                catalog_id = sts.get_caller_identity()["Account"]
                return {"Name": database, "Type": "GLUE", "Parameters": {"catalog-id": catalog_id}}
            else:
                with boto3_client_lock:
                    athena = client.session.client("athena", region_name=client.region_name, config=get_boto3_config())
                return athena.get_data_catalog(Name=database)["DataCatalog"]

    def list_relations_without_caching(self, schema_relation: AthenaRelation) -> List[BaseRelation]:
        data_catalog = self._get_data_catalog(schema_relation.database)
        catalog_id = get_catalog_id(data_catalog)
        if data_catalog and data_catalog["Type"] != "GLUE":
            # For non-Glue Data Catalogs, use the original Athena query against INFORMATION_SCHEMA approach
            return super().list_relations_without_caching(schema_relation)

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
                        LOGGER.debug(f"Table '{table['Name']}' has no TableType attribute - Ignoring")
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
            LOGGER.debug(f"Schema '{schema_relation.schema}' does not exist - Ignoring: {e}")

        return relations

    @available
    def swap_table(self, src_relation: AthenaRelation, target_relation: AthenaRelation):
        conn = self.connections.get_thread_connection()
        client = conn.handle

        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name, config=get_boto3_config())

        src_table = glue_client.get_table(DatabaseName=src_relation.schema, Name=src_relation.identifier).get("Table")
        src_table_partitions = glue_client.get_partitions(
            DatabaseName=src_relation.schema, TableName=src_relation.identifier
        ).get("Partitions")

        target_table_partitions = glue_client.get_partitions(
            DatabaseName=target_relation.schema, TableName=target_relation.identifier
        ).get("Partitions")

        target_table_version = {
            "Name": target_relation.identifier,
            "StorageDescriptor": src_table["StorageDescriptor"],
            "PartitionKeys": src_table["PartitionKeys"],
            "TableType": src_table["TableType"],
            "Parameters": src_table["Parameters"],
            "Description": src_table.get("Description", ""),
        }

        # perform a table swap
        glue_client.update_table(DatabaseName=target_relation.schema, TableInput=target_table_version)
        LOGGER.debug(f"Table {target_relation.render()} swapped with the content of {src_relation.render()}")

        # we delete the target table partitions in any case
        # if source table has partitions we need to delete and add partitions
        # it source table hasn't any partitions we need to delete target table partitions
        if target_table_partitions:
            glue_client.batch_delete_partition(
                DatabaseName=target_relation.schema,
                TableName=target_relation.identifier,
                PartitionsToDelete=[{"Values": i["Values"]} for i in target_table_partitions],
            )

        if src_table_partitions:
            glue_client.batch_create_partition(
                DatabaseName=target_relation.schema,
                TableName=target_relation.identifier,
                PartitionInputList=[
                    {"Values": p["Values"], "StorageDescriptor": p["StorageDescriptor"], "Parameters": p["Parameters"]}
                    for p in src_table_partitions
                ],
            )

    def _get_glue_table_versions_to_expire(self, relation: AthenaRelation, to_keep: int):
        """
        Given a table and the amount of its version to keep, it returns the versions to delete
        """
        conn = self.connections.get_thread_connection()
        client = conn.handle

        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name, config=get_boto3_config())

        paginator = glue_client.get_paginator("get_table_versions")
        response_iterator = paginator.paginate(
            **{
                "DatabaseName": relation.schema,
                "TableName": relation.identifier,
            }
        )
        table_versions = response_iterator.build_full_result().get("TableVersions")
        LOGGER.debug(f"Total table versions: {[v['VersionId'] for v in table_versions]}")
        table_versions_ordered = sorted(table_versions, key=lambda i: int(i["Table"]["VersionId"]), reverse=True)
        return table_versions_ordered[int(to_keep) :]

    @available
    def expire_glue_table_versions(self, relation: AthenaRelation, to_keep: int, delete_s3: bool):
        conn = self.connections.get_thread_connection()
        client = conn.handle

        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name, config=get_boto3_config())

        versions_to_delete = self._get_glue_table_versions_to_expire(relation, to_keep)
        LOGGER.debug(f"Versions to delete: {[v['VersionId'] for v in versions_to_delete]}")

        deleted_versions = []
        for v in versions_to_delete:
            version = v["Table"]["VersionId"]
            location = v["Table"]["StorageDescriptor"]["Location"]
            try:
                glue_client.delete_table_version(
                    DatabaseName=relation.schema, TableName=relation.identifier, VersionId=str(version)
                )
                deleted_versions.append(version)
                LOGGER.debug(f"Deleted version {version} of table {relation.render()} ")
                if delete_s3:
                    self.delete_from_s3(location)
            except Exception as err:
                LOGGER.debug(f"There was an error when expiring table version {version} with error: {err}")

            LOGGER.debug(f"{location} was deleted")

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

    @available
    def list_schemas(self, database: str) -> List[str]:
        conn = self.connections.get_thread_connection()
        client = conn.handle

        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name, config=get_boto3_config())

        paginator = glue_client.get_paginator("get_databases")
        result = []
        for page in paginator.paginate():
            result.extend([schema["Name"] for schema in page["DatabaseList"]])
        return result

    @staticmethod
    def _is_current_column(col: dict) -> bool:
        """
        Check if a column is explicit set as not current. If not, it is considered as current.
        """
        if col.get("Parameters", {}).get("iceberg.field.current") == "false":
            return False
        return True

    @available
    def get_columns_in_relation(self, relation: AthenaRelation) -> List[AthenaColumn]:
        conn = self.connections.get_thread_connection()
        client = conn.handle

        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name, config=get_boto3_config())

        try:
            table = glue_client.get_table(DatabaseName=relation.schema, Name=relation.identifier)["Table"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                LOGGER.debug("table not exist, catching the error")
                return []
            else:
                LOGGER.error(e)
                raise e

        table_type = get_table_type(table)

        columns = [c for c in table["StorageDescriptor"]["Columns"] if self._is_current_column(c)]
        partition_keys = table.get("PartitionKeys", [])

        LOGGER.debug(f"Columns in relation {relation.identifier}: {columns + partition_keys}")

        return [
            AthenaColumn(column=c["Name"], dtype=c["Type"], table_type=table_type) for c in columns + partition_keys
        ]

    @available
    def delete_from_glue_catalog(self, relation: AthenaRelation):
        schema_name = relation.schema
        table_name = relation.identifier

        conn = self.connections.get_thread_connection()
        client = conn.handle

        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name, config=get_boto3_config())

        try:
            glue_client.delete_table(DatabaseName=schema_name, Name=table_name)
            LOGGER.debug(f"Deleted table from glue catalog: {relation.render()}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                LOGGER.debug(f"Table {relation.render()} does not exist and will not be deleted, ignoring")
            else:
                LOGGER.error(e)
                raise e

    @available.parse_none
    def valid_snapshot_target(self, relation: BaseRelation) -> None:
        """Log an error to help developers migrate to the new snapshot logic"""
        super().valid_snapshot_target(relation)
        columns = self.get_columns_in_relation(relation)
        names = {c.name.lower() for c in columns}

        table_columns = [col for col in names if not col.startswith("dbt_") and col != "is_current_record"]

        if "dbt_unique_key" in names:
            sql = self._generate_snapshot_migration_sql(relation=relation, table_columns=table_columns)
            msg = (
                f"{'!'*90}\n"
                "The snapshot logic of dbt-athena has changed in an incompatible way to be more consistent "
                "with the dbt-core implementation.\nYou will need to migrate your existing snapshot tables to be "
                "able to keep using them with the latest dbt-athena version.\nYou can find more information "
                "in the release notes:\nhttps://github.com/dbt-athena/dbt-athena/releases\n"
                f"{'!'*90}\n\n"
                "You can use the example query below as a baseline to perform the migration:\n\n"
                f"{'-'*90}\n"
                f"{sql}\n"
                f"{'-'*90}\n\n"
            )
            LOGGER.error(msg)
            raise SnapshotMigrationRequired("Look into 1.5 dbt-athena docs for the complete migration procedure")

    def _generate_snapshot_migration_sql(self, relation: AthenaRelation, table_columns: List[str]) -> str:
        """Generate a sequence of queries that can be used to migrate the existing table to the new format.

        The queries perform the following steps:
        - Backup the existing table
        - Make the necessary modifications and store the results in a staging table
        - Delete the target table (users might have to delete the S3 files manually)
        - Copy the content of the staging table to the final table
        - Delete the staging table
        """
        col_csv = f",\n{' '*16}".join(table_columns)
        staging_relation = relation.incorporate(
            path={"identifier": relation.identifier + "__dbt_tmp_migration_staging"}
        )
        ctas = dedent(
            f"""\
            select
                {col_csv},
                dbt_snapshot_at as dbt_updated_at,
                dbt_valid_from,
                if(dbt_valid_to > cast('9000-01-01' as timestamp), null, dbt_valid_to) as dbt_valid_to,
                dbt_scd_id
            from {relation}
            where dbt_change_type != 'delete'
            ;
            """
        )
        staging_sql = self.execute_macro(
            "create_table_as", kwargs=dict(temporary=True, relation=staging_relation, compiled_code=ctas)
        )

        backup_relation = relation.incorporate(path={"identifier": relation.identifier + "__dbt_tmp_migration_backup"})
        backup_sql = self.execute_macro(
            "create_table_as",
            kwargs=dict(temporary=True, relation=backup_relation, compiled_code=f"select * from {relation};"),
        )

        drop_target_sql = f"drop table {relation.render_hive()};"

        copy_to_target_sql = self.execute_macro(
            "create_table_as", kwargs=dict(relation=relation, compiled_code=f"select * from {staging_relation};")
        )

        drop_staging_sql = f"drop table {staging_relation.render_hive()};"

        return "\n".join(
            [
                "-- Backup original table",
                backup_sql.strip(),
                "\n\n-- Store new results in staging table",
                staging_sql.strip(),
                "\n\n-- Drop target table\n"
                "-- Note: you will need to manually remove the S3 files if you have a static table location\n",
                drop_target_sql.strip(),
                "\n\n-- Copy staging to target",
                copy_to_target_sql.strip(),
                "\n\n-- Drop staging table",
                drop_staging_sql.strip(),
            ]
        )
