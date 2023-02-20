import decimal
import os
from unittest import mock
from unittest.mock import patch

import agate
import boto3
import pytest
from moto import mock_athena, mock_glue, mock_s3

from dbt.adapters.athena import AthenaAdapter
from dbt.adapters.athena import Plugin as AthenaPlugin
from dbt.adapters.athena.connections import AthenaCursor, AthenaParameterFormatter
from dbt.adapters.athena.relation import AthenaRelation
from dbt.clients import agate_helper
from dbt.contracts.connection import ConnectionState
from dbt.contracts.files import FileHash
from dbt.contracts.graph.nodes import CompiledNode, DependsOn, NodeConfig
from dbt.exceptions import ConnectionError, DbtRuntimeError
from dbt.node_types import NodeType

from .constants import AWS_REGION, BUCKET, DATA_CATALOG_NAME, DATABASE_NAME
from .utils import (
    MockAWSService,
    TestAdapterConversions,
    config_from_parts_or_dicts,
    inject_adapter,
)


class TestAthenaAdapter:
    mock_aws_service = MockAWSService()

    def setup_method(self, _):
        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "config-version": 2,
        }
        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "athena",
                    "s3_staging_dir": "s3://my-bucket/test-dbt/",
                    "region_name": AWS_REGION,
                    "database": DATA_CATALOG_NAME,
                    "work_group": "dbt-athena-adapter",
                    "schema": DATABASE_NAME,
                }
            },
            "target": "test",
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self._adapter = None
        self.mock_manifest = mock.MagicMock()
        self.mock_manifest.get_used_schemas.return_value = {("dbt", "foo"), ("dbt", "quux")}
        self.mock_manifest.nodes = {
            "model.root.model1": CompiledNode(
                name="model1",
                database="dbt",
                schema="foo",
                resource_type=NodeType.Model,
                unique_id="model.root.model1",
                alias="bar",
                fqn=["root", "model1"],
                package_name="root",
                refs=[],
                sources=[],
                depends_on=DependsOn(),
                config=NodeConfig.from_dict(
                    {
                        "enabled": True,
                        "materialized": "table",
                        "persist_docs": {},
                        "post-hook": [],
                        "pre-hook": [],
                        "vars": {},
                        "meta": {"owner": "data-engineers"},
                        "quoting": {},
                        "column_types": {},
                        "tags": [],
                    }
                ),
                tags=[],
                path="model1.sql",
                original_file_path="model1.sql",
                compiled=True,
                extra_ctes_injected=False,
                extra_ctes=[],
                checksum=FileHash.from_contents(""),
                raw_code="select * from source_table",
                language="",
            ),
            "model.root.model2": CompiledNode(
                name="model2",
                database="dbt",
                schema="quux",
                resource_type=NodeType.Model,
                unique_id="model.root.model2",
                alias="bar",
                fqn=["root", "model2"],
                package_name="root",
                refs=[],
                sources=[],
                depends_on=DependsOn(),
                config=NodeConfig.from_dict(
                    {
                        "enabled": True,
                        "materialized": "table",
                        "persist_docs": {},
                        "post-hook": [],
                        "pre-hook": [],
                        "vars": {},
                        "meta": {"owner": "data-analysts"},
                        "quoting": {},
                        "column_types": {},
                        "tags": [],
                    }
                ),
                tags=[],
                path="model2.sql",
                original_file_path="model2.sql",
                compiled=True,
                extra_ctes_injected=False,
                extra_ctes=[],
                checksum=FileHash.from_contents(""),
                raw_code="select * from source_table",
                language="",
            ),
        }

    @property
    def adapter(self):
        if self._adapter is None:
            self._adapter = AthenaAdapter(self.config)
            inject_adapter(self._adapter, AthenaPlugin)
        return self._adapter

    @mock.patch("dbt.adapters.athena.connections.AthenaConnection")
    def test_acquire_connection_validations(self, connection_cls):
        try:
            connection = self.adapter.acquire_connection("dummy")
        except DbtRuntimeError as e:
            pytest.fail(f"got ValidationException: {e}")
        except BaseException as e:
            pytest.fail(f"acquiring connection failed with unknown exception: {e}")

        connection_cls.assert_not_called()
        connection.handle
        connection_cls.assert_called_once()
        _, arguments = connection_cls.call_args_list[0]
        assert arguments["s3_staging_dir"] == "s3://my-bucket/test-dbt/"
        assert arguments["endpoint_url"] is None
        assert arguments["schema_name"] == "test_dbt_athena"
        assert arguments["work_group"] == "dbt-athena-adapter"
        assert arguments["cursor_class"] == AthenaCursor
        assert isinstance(arguments["formatter"], AthenaParameterFormatter)
        assert arguments["poll_interval"] == 1.0
        assert arguments["retry_config"].attempt == 5
        assert arguments["retry_config"].exceptions == (
            "ThrottlingException",
            "TooManyRequestsException",
            "InternalServerException",
        )

    @mock.patch("dbt.adapters.athena.connections.AthenaConnection")
    def test_acquire_connection(self, connection_cls):
        connection = self.adapter.acquire_connection("dummy")

        connection_cls.assert_not_called()
        connection.handle
        assert connection.state == ConnectionState.OPEN
        assert connection.handle is not None
        connection_cls.assert_called_once()

    @mock.patch("dbt.adapters.athena.connections.AthenaConnection")
    def test_acquire_connection_exc(self, connection_cls, dbt_error_caplog):
        connection_cls.side_effect = lambda **_: (_ for _ in ()).throw(Exception("foobar"))
        connection = self.adapter.acquire_connection("dummy")
        conn_res = None
        with pytest.raises(ConnectionError) as exc:
            conn_res = connection.handle

        assert conn_res is None
        assert connection.state == ConnectionState.FAIL
        assert exc.value.__str__() == "foobar"
        assert "Got an error when attempting to open a Athena connection due to foobar" in dbt_error_caplog.getvalue()

    @pytest.mark.parametrize(
        ("s3_data_dir", "s3_data_naming", "external_location", "is_temporary_table", "expected"),
        (
            pytest.param(None, "table", None, False, "s3://my-bucket/test-dbt/tables/table", id="table naming"),
            pytest.param(None, "uuid", None, False, "s3://my-bucket/test-dbt/tables/uuid", id="uuid naming"),
            pytest.param(
                None, "table_unique", None, False, "s3://my-bucket/test-dbt/tables/table/uuid", id="table_unique naming"
            ),
            pytest.param(
                None,
                "schema_table",
                None,
                False,
                "s3://my-bucket/test-dbt/tables/schema/table",
                id="schema_table naming",
            ),
            pytest.param(
                None,
                "schema_table_unique",
                None,
                False,
                "s3://my-bucket/test-dbt/tables/schema/table/uuid",
                id="schema_table_unique naming",
            ),
            pytest.param(
                "s3://my-data-bucket/",
                "schema_table_unique",
                None,
                False,
                "s3://my-data-bucket/schema/table/uuid",
                id="data_dir set",
            ),
            pytest.param(
                "s3://my-data-bucket/",
                "schema_table_unique",
                "s3://path/to/external/",
                False,
                "s3://path/to/external",
                id="external_location set and not temporary",
            ),
            pytest.param(
                "s3://my-data-bucket/",
                "schema_table_unique",
                "s3://path/to/external/",
                True,
                "s3://my-data-bucket/schema/table/uuid",
                id="external_location set and temporary",
            ),
        ),
    )
    @patch("dbt.adapters.athena.impl.uuid4", return_value="uuid")
    def test_s3_table_location(self, _, s3_data_dir, s3_data_naming, external_location, is_temporary_table, expected):
        self.adapter.acquire_connection("dummy")
        assert expected == self.adapter.s3_table_location(
            s3_data_dir, s3_data_naming, "schema", "table", external_location, is_temporary_table
        )

    def test_s3_table_location_exc(self):
        self.adapter.acquire_connection("dummy")
        with pytest.raises(ValueError) as exc:
            self.adapter.s3_table_location(None, "other", "schema", "table")
        assert exc.value.__str__() == "Unknown value for s3_data_naming: other"

    @mock_glue
    @mock_s3
    @mock_athena
    def test_get_table_location(self, dbt_debug_caplog):
        table_name = "test_table"
        self.adapter.acquire_connection("dummy")
        self.mock_aws_service.create_data_catalog()
        self.mock_aws_service.create_database()
        self.mock_aws_service.create_table(table_name)
        assert self.adapter.get_table_location(DATABASE_NAME, table_name) == "s3://test-dbt-athena/tables/test_table"

    @mock_glue
    @mock_s3
    @mock_athena
    def test_get_table_location_with_failure(self, dbt_debug_caplog):
        table_name = "test_table"
        self.adapter.acquire_connection("dummy")
        self.mock_aws_service.create_data_catalog()
        self.mock_aws_service.create_database()
        assert self.adapter.get_table_location(DATABASE_NAME, table_name) is None
        assert f"Table '{table_name}' does not exists - Ignoring" in dbt_debug_caplog.getvalue()

    @pytest.fixture(scope="function")
    def aws_credentials(self):
        """Mocked AWS Credentials for moto."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = AWS_REGION

    @mock_glue
    @mock_s3
    @mock_athena
    def test_clean_up_partitions_will_work(self, dbt_debug_caplog, aws_credentials):
        table_name = "table"
        self.mock_aws_service.create_data_catalog()
        self.mock_aws_service.create_database()
        self.mock_aws_service.create_table(table_name)
        self.mock_aws_service.add_data_in_table(table_name)
        self.adapter.acquire_connection("dummy")
        self.adapter.clean_up_partitions(DATABASE_NAME, table_name, "dt < '2022-01-03'")
        log_records = dbt_debug_caplog.getvalue()
        assert (
            "Deleting table data: path="
            "'s3://test-dbt-athena/tables/table/dt=2022-01-01', "
            "bucket='test-dbt-athena', "
            "prefix='tables/table/dt=2022-01-01/'" in log_records
        )
        assert (
            "Deleting table data: path="
            "'s3://test-dbt-athena/tables/table/dt=2022-01-02', "
            "bucket='test-dbt-athena', "
            "prefix='tables/table/dt=2022-01-02/'" in log_records
        )
        s3 = boto3.client("s3", region_name=AWS_REGION)
        keys = [obj["Key"] for obj in s3.list_objects_v2(Bucket=BUCKET)["Contents"]]
        assert set(keys) == {"tables/table/dt=2022-01-03/data1.parquet", "tables/table/dt=2022-01-03/data2.parquet"}

    @mock_glue
    @mock_athena
    def test_clean_up_table_table_does_not_exist(self, dbt_debug_caplog, aws_credentials):
        self.mock_aws_service.create_data_catalog()
        self.mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        result = self.adapter.clean_up_table(DATABASE_NAME, "table")
        assert result is None
        assert "Table 'table' does not exists - Ignoring" in dbt_debug_caplog.getvalue()

    @mock_glue
    @mock_athena
    def test_clean_up_table_view(self, dbt_debug_caplog, aws_credentials):
        self.mock_aws_service.create_data_catalog()
        self.mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        self.mock_aws_service.create_view("test_view")
        result = self.adapter.clean_up_table(DATABASE_NAME, "test_view")
        assert result is None

    @mock_glue
    @mock_s3
    @mock_athena
    def test_clean_up_table_delete_table(self, dbt_debug_caplog, aws_credentials):
        self.mock_aws_service.create_data_catalog()
        self.mock_aws_service.create_database()
        self.mock_aws_service.create_table("table")
        self.mock_aws_service.add_data_in_table("table")
        self.adapter.acquire_connection("dummy")
        self.adapter.clean_up_table(DATABASE_NAME, "table")
        assert (
            "Deleting table data: path='s3://test-dbt-athena/tables/table', "
            "bucket='test-dbt-athena', "
            "prefix='tables/table/'" in dbt_debug_caplog.getvalue()
        )
        s3 = boto3.client("s3", region_name=AWS_REGION)
        objs = s3.list_objects_v2(Bucket=BUCKET)
        assert objs["KeyCount"] == 0

    @patch("dbt.adapters.athena.impl.SQLAdapter.quote_seed_column")
    def test_quote_seed_column(self, parent_quote_seed_column):
        self.adapter.quote_seed_column("col", None)
        parent_quote_seed_column.assert_called_once_with("col", False)

    @mock.patch.object(AthenaAdapter, "execute_macro")
    def test__get_one_catalog(self, mock_execute):
        column_names = [
            "table_database",
            "table_schema",
            "table_name",
            "table_type",
            "table_comment",
            "column_name",
            "column_index",
            "column_type",
            "column_comment",
        ]
        rows = [
            ("dbt", "foo", "bar", "table", None, "id", 0, "string", None),
            ("dbt", "foo", "bar", "table", None, "dt", 1, "date", None),
            ("dbt", None, "bar", "table", None, "id", 0, "string", None),
            ("dbt", None, "bar", "table", None, "dt", 1, "date", None),
            ("dbt", "quux", "bar", "table", None, "id", 0, "string", None),
            ("dbt", "quux", "bar", "table", None, "category", 1, "string", None),
            ("dbt", "skip", "bar", "table", None, "id", 0, "string", None),
            ("dbt", "skip", "bar", "table", None, "category", 1, "string", None),
        ]
        mock_execute.return_value = agate.Table(rows=rows, column_names=column_names)

        expected_column_names = (
            "table_database",
            "table_schema",
            "table_name",
            "table_type",
            "table_comment",
            "column_name",
            "column_index",
            "column_type",
            "column_comment",
            "table_owner",
        )
        expected_rows = [
            ("dbt", "foo", "bar", "table", None, "id", 0, "string", None, "data-engineers"),
            ("dbt", "foo", "bar", "table", None, "dt", 1, "date", None, "data-engineers"),
            ("dbt", "quux", "bar", "table", None, "id", 0, "string", None, "data-analysts"),
            ("dbt", "quux", "bar", "table", None, "category", 1, "string", None, "data-analysts"),
        ]
        actual = self.adapter._get_one_catalog(
            # No need to mock information_schemas relation and schemas since we are mock execute_macro
            mock.MagicMock(),
            mock.MagicMock(),
            self.mock_manifest,
        )
        assert actual.column_names == expected_column_names
        assert len(actual.rows) == len(expected_rows)
        for row in actual.rows.values():
            assert row.values() in expected_rows

    def test__get_catalog_schemas(self):
        res = self.adapter._get_catalog_schemas(self.mock_manifest)
        assert len(res.keys()) == 1
        information_schema = list(res.keys())[0]
        assert information_schema.name == "INFORMATION_SCHEMA"
        assert information_schema.schema is None
        assert information_schema.database == "dbt"
        relations = list(res.values())[0]
        assert set(relations.keys()) == {"foo", "quux"}
        assert list(relations.values()) == [{"bar"}, {"bar"}]

    @mock_athena
    def test__get_data_catalog(self, aws_credentials):
        self.mock_aws_service.create_data_catalog()
        self.adapter.acquire_connection("dummy")
        res = self.adapter._get_data_catalog(DATA_CATALOG_NAME)
        assert {"Name": "awsdatacatalog", "Type": "GLUE", "Parameters": {"catalog-id": "catalog_id"}} == res

    @mock_glue
    @mock_s3
    @mock_athena
    def test__get_relation_type_table(self, aws_credentials):
        self.mock_aws_service.create_data_catalog()
        self.mock_aws_service.create_database()
        self.mock_aws_service.create_table("test_table")
        self.adapter.acquire_connection("dummy")
        table_type = self.adapter.get_table_type(DATABASE_NAME, "test_table")
        assert table_type == "table"

    @mock_glue
    @mock_s3
    @mock_athena
    def test__get_relation_type_with_no_type(self, aws_credentials):
        self.mock_aws_service.create_data_catalog()
        self.mock_aws_service.create_database()
        self.mock_aws_service.create_table_without_table_type("test_table")
        self.adapter.acquire_connection("dummy")

        with pytest.raises(ValueError):
            self.adapter.get_table_type(DATABASE_NAME, "test_table")

    @mock_glue
    @mock_s3
    @mock_athena
    def test__get_relation_type_view(self, aws_credentials):
        self.mock_aws_service.create_data_catalog()
        self.mock_aws_service.create_database()
        self.mock_aws_service.create_view("test_view")
        self.adapter.acquire_connection("dummy")
        table_type = self.adapter.get_table_type(DATABASE_NAME, "test_view")
        assert table_type == "view"

    @mock_glue
    @mock_s3
    @mock_athena
    def test__get_relation_type_iceberg(self, aws_credentials):
        self.mock_aws_service.create_data_catalog()
        self.mock_aws_service.create_database()
        self.mock_aws_service.create_iceberg_table("test_iceberg")
        self.adapter.acquire_connection("dummy")
        table_type = self.adapter.get_table_type(DATABASE_NAME, "test_iceberg")
        assert table_type == "iceberg_table"

    def _test_list_relations_without_caching(self, schema_relation):
        self.adapter.acquire_connection("dummy")
        relations = self.adapter.list_relations_without_caching(schema_relation)
        assert len(relations) == 3
        assert all(isinstance(rel, AthenaRelation) for rel in relations)
        relations.sort(key=lambda rel: rel.name)
        other = relations[0]
        table = relations[1]
        view = relations[2]
        assert other.name == "other"
        assert other.type == "table"
        assert table.name == "table"
        assert table.type == "table"
        assert view.name == "view"
        assert view.type == "view"

    @mock_athena
    @mock_glue
    def test_list_relations_without_caching_with_awsdatacatalog(self, aws_credentials):
        self.mock_aws_service.create_data_catalog()
        self.mock_aws_service.create_database()
        self.mock_aws_service.create_table("table")
        self.mock_aws_service.create_table("other")
        self.mock_aws_service.create_view("view")
        self.mock_aws_service.create_table_without_table_type("without_table_type")
        schema_relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            quote_policy=self.adapter.config.quoting,
        )
        self._test_list_relations_without_caching(schema_relation)

    @mock_athena
    @mock_glue
    def test_list_relations_without_caching_with_other_glue_data_catalog(self, aws_credentials):
        data_catalog_name = "other_data_catalog"
        self.mock_aws_service.create_data_catalog(data_catalog_name)
        self.mock_aws_service.create_database()
        self.mock_aws_service.create_table("table")
        self.mock_aws_service.create_table("other")
        self.mock_aws_service.create_view("view")
        self.mock_aws_service.create_table_without_table_type("without_table_type")
        schema_relation = self.adapter.Relation.create(
            database=data_catalog_name,
            schema=DATABASE_NAME,
            quote_policy=self.adapter.config.quoting,
        )
        self._test_list_relations_without_caching(schema_relation)

    @mock_athena
    @patch("dbt.adapters.athena.impl.SQLAdapter.list_relations_without_caching", return_value=[])
    def test_list_relations_without_caching_with_non_glue_data_catalog(self, parent_list_relations_without_caching):
        data_catalog_name = "other_data_catalog"
        self.mock_aws_service.create_data_catalog(data_catalog_name, "HIVE")
        schema_relation = self.adapter.Relation.create(
            database=data_catalog_name,
            schema=DATABASE_NAME,
            quote_policy=self.adapter.config.quoting,
        )
        self.adapter.acquire_connection("dummy")
        self.adapter.list_relations_without_caching(schema_relation)
        parent_list_relations_without_caching.assert_called_once_with(schema_relation)

    @pytest.mark.parametrize(
        "s3_path,expected",
        [
            ("s3://my-bucket/test-dbt/tables/schema/table", ("my-bucket", "test-dbt/tables/schema/table/")),
            ("s3://my-bucket/test-dbt/tables/schema/table/", ("my-bucket", "test-dbt/tables/schema/table/")),
        ],
    )
    def test_parse_s3_path(self, s3_path, expected):
        assert self.adapter._parse_s3_path(s3_path) == expected

    @mock_athena
    @mock_glue
    @mock_s3
    def test_swap_table_with_partitions(self, aws_credentials):
        self.mock_aws_service.create_data_catalog()
        self.mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        target_table = "target_table"
        source_table = "source_table"
        self.mock_aws_service.create_table(source_table)
        self.mock_aws_service.add_partitions_to_table(DATABASE_NAME, source_table)
        self.mock_aws_service.create_table(target_table)
        self.mock_aws_service.add_partitions_to_table(DATABASE_NAME, source_table)
        self.adapter.swap_table(DATABASE_NAME, source_table, DATABASE_NAME, target_table)
        assert self.adapter.get_table_location(DATABASE_NAME, target_table) == f"s3://{BUCKET}/tables/{source_table}"

    @mock_athena
    @mock_glue
    @mock_s3
    def test_swap_table_without_partitions(self, aws_credentials):
        self.mock_aws_service.create_data_catalog()
        self.mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        target_table = "target_table"
        source_table = "source_table"
        self.mock_aws_service.create_table_without_partitions(source_table)
        self.mock_aws_service.create_table_without_partitions(target_table)
        self.adapter.swap_table(DATABASE_NAME, source_table, DATABASE_NAME, target_table)
        assert self.adapter.get_table_location(DATABASE_NAME, target_table) == f"s3://{BUCKET}/tables/{source_table}"

    @mock_athena
    @mock_glue
    @mock_s3
    def test_swap_table_with_partitions_to_one_without(self, aws_credentials):
        self.mock_aws_service.create_data_catalog()
        self.mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        target_table = "target_table"
        source_table = "source_table"
        # source table does not have partitions
        self.mock_aws_service.create_table_without_partitions(source_table)

        # the target table has partitions
        self.mock_aws_service.create_table(target_table)
        self.mock_aws_service.add_partitions_to_table(DATABASE_NAME, target_table)

        self.adapter.swap_table(DATABASE_NAME, source_table, DATABASE_NAME, target_table)
        glue_client = boto3.client("glue", region_name=AWS_REGION)

        target_table_partitions = glue_client.get_partitions(DatabaseName=DATABASE_NAME, TableName=target_table).get(
            "Partitions"
        )

        assert self.adapter.get_table_location(DATABASE_NAME, target_table) == f"s3://{BUCKET}/tables/{source_table}"
        assert len(target_table_partitions) == 0

    @mock_athena
    @mock_glue
    @mock_s3
    def test_swap_table_with_no_partitions_to_one_with(self, aws_credentials):
        self.mock_aws_service.create_data_catalog()
        self.mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        target_table = "target_table"
        source_table = "source_table"
        self.mock_aws_service.create_table(source_table)
        self.mock_aws_service.add_partitions_to_table(DATABASE_NAME, source_table)
        self.mock_aws_service.create_table_without_partitions(target_table)
        glue_client = boto3.client("glue", region_name=AWS_REGION)
        target_table_partitions = glue_client.get_partitions(DatabaseName=DATABASE_NAME, TableName=target_table).get(
            "Partitions"
        )
        assert len(target_table_partitions) == 0
        self.adapter.swap_table(DATABASE_NAME, source_table, DATABASE_NAME, target_table)
        target_table_partitions_after = glue_client.get_partitions(
            DatabaseName=DATABASE_NAME, TableName=target_table
        ).get("Partitions")

        assert self.adapter.get_table_location(DATABASE_NAME, target_table) == f"s3://{BUCKET}/tables/{source_table}"
        assert len(target_table_partitions_after) == 3

    @mock_athena
    @mock_glue
    def test__get_glue_table_versions_to_expire(self, aws_credentials, dbt_debug_caplog):
        self.mock_aws_service.create_data_catalog()
        self.mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        table_name = "my_table"
        self.mock_aws_service.create_table(table_name)
        self.mock_aws_service.add_table_version(DATABASE_NAME, table_name)
        self.mock_aws_service.add_table_version(DATABASE_NAME, table_name)
        self.mock_aws_service.add_table_version(DATABASE_NAME, table_name)
        glue = boto3.client("glue", region_name=AWS_REGION)
        table_versions = glue.get_table_versions(DatabaseName=DATABASE_NAME, TableName=table_name).get("TableVersions")
        assert len(table_versions) == 4
        version_to_keep = 1
        versions_to_expire = self.adapter._get_glue_table_versions_to_expire(DATABASE_NAME, table_name, version_to_keep)
        assert len(versions_to_expire) == 3
        assert [v["VersionId"] for v in versions_to_expire] == ["3", "2", "1"]

    @mock_athena
    @mock_glue
    @mock_s3
    def test_expire_glue_table_versions(self, aws_credentials):
        self.mock_aws_service.create_data_catalog()
        self.mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        table_name = "my_table"
        self.mock_aws_service.create_table(table_name)
        self.mock_aws_service.add_table_version(DATABASE_NAME, table_name)
        self.mock_aws_service.add_table_version(DATABASE_NAME, table_name)
        self.mock_aws_service.add_table_version(DATABASE_NAME, table_name)
        glue = boto3.client("glue", region_name=AWS_REGION)
        table_versions = glue.get_table_versions(DatabaseName=DATABASE_NAME, TableName=table_name).get("TableVersions")
        assert len(table_versions) == 4
        version_to_keep = 1
        self.adapter.expire_glue_table_versions(DATABASE_NAME, table_name, version_to_keep, False)
        # TODO delete_table_version is not implemented in moto
        # TODO moto issue https://github.com/getmoto/moto/issues/5952
        # assert len(result) == 3


class TestAthenaFilterCatalog:
    def test__catalog_filter_table(self):
        manifest = mock.MagicMock()
        manifest.get_used_schemas.return_value = [["a", "B"], ["a", "1234"]]
        column_names = ["table_name", "table_database", "table_schema", "something"]
        rows = [
            ["foo", "a", "b", "1234"],  # include
            ["foo", "a", "1234", "1234"],  # include, w/ table schema as str
            ["foo", "c", "B", "1234"],  # skip
            ["1234", "A", "B", "1234"],  # include, w/ table name as str
        ]
        table = agate.Table(rows, column_names, agate_helper.DEFAULT_TYPE_TESTER)

        result = AthenaAdapter._catalog_filter_table(table, manifest)
        assert len(result) == 3
        for row in result.rows:
            assert isinstance(row["table_schema"], str)
            assert isinstance(row["table_database"], str)
            assert isinstance(row["table_name"], str)
            assert isinstance(row["something"], decimal.Decimal)


class TestAthenaAdapterConversions(TestAdapterConversions):
    def test_convert_text_type(self):
        rows = [
            ["", "a1", "stringval1"],
            ["", "a2", "stringvalasdfasdfasdfa"],
            ["", "a3", "stringval3"],
        ]
        agate_table = self._make_table_of(rows, agate.Text)
        expected = ["string", "string", "string"]
        for col_idx, expect in enumerate(expected):
            assert AthenaAdapter.convert_text_type(agate_table, col_idx) == expect

    def test_convert_number_type(self):
        rows = [
            ["", "23.98", "-1"],
            ["", "12.78", "-2"],
            ["", "79.41", "-3"],
        ]
        agate_table = self._make_table_of(rows, agate.Number)
        expected = ["integer", "double", "integer"]
        for col_idx, expect in enumerate(expected):
            assert AthenaAdapter.convert_number_type(agate_table, col_idx) == expect

    def test_convert_boolean_type(self):
        rows = [
            ["", "false", "true"],
            ["", "false", "false"],
            ["", "false", "true"],
        ]
        agate_table = self._make_table_of(rows, agate.Boolean)
        expected = ["boolean", "boolean", "boolean"]
        for col_idx, expect in enumerate(expected):
            assert AthenaAdapter.convert_boolean_type(agate_table, col_idx) == expect

    def test_convert_datetime_type(self):
        rows = [
            ["", "20190101T01:01:01Z", "2019-01-01 01:01:01"],
            ["", "20190102T01:01:01Z", "2019-01-01 01:01:01"],
            ["", "20190103T01:01:01Z", "2019-01-01 01:01:01"],
        ]
        agate_table = self._make_table_of(rows, [agate.DateTime, agate_helper.ISODateTime, agate.DateTime])
        expected = ["timestamp", "timestamp", "timestamp"]
        for col_idx, expect in enumerate(expected):
            assert AthenaAdapter.convert_datetime_type(agate_table, col_idx) == expect

    def test_convert_date_type(self):
        rows = [
            ["", "2019-01-01", "2019-01-04"],
            ["", "2019-01-02", "2019-01-04"],
            ["", "2019-01-03", "2019-01-04"],
        ]
        agate_table = self._make_table_of(rows, agate.Date)
        expected = ["date", "date", "date"]
        for col_idx, expect in enumerate(expected):
            assert AthenaAdapter.convert_date_type(agate_table, col_idx) == expect
