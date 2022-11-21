import re
from itertools import chain
from os import path
from threading import Lock
from typing import Dict, Iterator, List, Optional, Set
from uuid import uuid4

import agate
from botocore.exceptions import ClientError
from dbt.adapters.base import available
from dbt.adapters.base.impl import GET_CATALOG_MACRO_NAME
from dbt.adapters.base.relation import BaseRelation, InformationSchema
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.graph.manifest import Manifest
from dbt.events import AdapterLogger

from dbt.adapters.athena import AthenaConnectionManager
from dbt.adapters.athena.relation import AthenaRelation, AthenaSchemaSearchMap

logger = AdapterLogger("Athena")

boto3_client_lock = Lock()


class AthenaAdapter(SQLAdapter):
    ConnectionManager = AthenaConnectionManager
    Relation = AthenaRelation

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
    def s3_uuid_table_location(self):
        conn = self.connections.get_thread_connection()
        client = conn.handle
        return f"{client.s3_staging_dir}tables/{str(uuid4())}/"

    @available
    def s3_unique_location(self, external_location, strict_location, staging_dir, relation_name):
        """
        Generate a unique not overlapping location.
        """
        unique_id = str(uuid4())
        if external_location is not None:
            if not strict_location:
                if external_location.endswith("/"):
                    external_location = external_location[:-1]
                external_location = f"{external_location}_{unique_id}/"
        else:
            base_path = path.join(staging_dir, f"{relation_name}_{unique_id}")
            external_location = f"{base_path}/"
        return external_location

    @available
    def clean_up_partitions(self, database_name: str, table_name: str, where_condition: str):
        # Look up Glue partitions & clean up
        conn = self.connections.get_thread_connection()
        client = conn.handle

        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name)
        s3_resource = client.session.resource("s3", region_name=client.region_name)
        paginator = glue_client.get_paginator("get_partitions")
        partition_params = {
            "DatabaseName": database_name,
            "TableName": table_name,
            "Expression": where_condition,
            "ExcludeColumnSchema": True,
        }
        partition_pg = paginator.paginate(**partition_params)
        partitions = partition_pg.build_full_result().get("Partitions")
        s3_rg = re.compile("s3://([^/]*)/(.*)")
        for partition in partitions:
            logger.debug(
                f"Deleting objects for partition '{partition['Values']}' "
                f"at '{partition['StorageDescriptor']['Location']}'"
            )
            m = s3_rg.match(partition["StorageDescriptor"]["Location"])
            if m is not None:
                bucket_name = m.group(1)
                prefix = m.group(2)
                s3_bucket = s3_resource.Bucket(bucket_name)
                s3_bucket.objects.filter(Prefix=prefix).delete()

    @available
    def clean_up_table(self, database_name: str, table_name: str):
        # Look up Glue partitions & clean up
        conn = self.connections.get_thread_connection()
        client = conn.handle
        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name)
        try:
            table = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                logger.debug(f"Table '{table_name}' does not exists - Ignoring")
                return

        if table is not None:
            logger.debug(f"Deleting table data from '{table['Table']['StorageDescriptor']['Location']}'")
            p = re.compile("s3://([^/]*)/(.*)")
            m = p.match(table["Table"]["StorageDescriptor"]["Location"])
            if m is not None:
                bucket_name = m.group(1)
                prefix = m.group(2)
                s3_resource = client.session.resource("s3", region_name=client.region_name)
                s3_bucket = s3_resource.Bucket(bucket_name)
                s3_bucket.objects.filter(Prefix=prefix).delete()

    @available
    def quote_seed_column(self, column: str, quote_config: Optional[bool]) -> str:
        return super().quote_seed_column(column, False)

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
        nodes: Iterator[CompileResultNode] = chain(
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
            athena_client = client.session.client("athena", region_name=client.region_name)

        response = athena_client.get_data_catalog(Name=catalog_name)
        return response["DataCatalog"]

    def list_relations_without_caching(
        self,
        schema_relation: AthenaRelation,
    ) -> List[BaseRelation]:
        catalog_id = None
        if schema_relation.database.lower() != "awsdatacatalog":
            data_catalog = self._get_data_catalog(schema_relation.database.lower())
            # For non-Glue Data Catalogs, use the original Athena query against INFORMATION_SCHEMA approach
            if data_catalog["Type"] != "GLUE":
                return super().list_relations_without_caching(schema_relation)
            else:
                catalog_id = data_catalog["Parameters"]["catalog-id"]

        conn = self.connections.get_thread_connection()
        client = conn.handle
        with boto3_client_lock:
            glue_client = client.session.client("glue", region_name=client.region_name)
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

        for page in page_iterator:
            tables = page["TableList"]
            for table in tables:
                _type = table["TableType"]
                if _type == "VIRTUAL_VIEW":
                    _type = self.Relation.View
                else:
                    _type = self.Relation.Table

                relations.append(
                    self.Relation.create(
                        schema=table["DatabaseName"],
                        database=schema_relation.database,
                        identifier=table["Name"],
                        quote_policy=quote_policy,
                        type=_type,
                    )
                )

        return relations
