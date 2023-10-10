import decimal
from unittest import mock
from unittest.mock import patch

import agate
import boto3
import botocore
import pytest

# from botocore.client.BaseClient import _make_api_call
from moto import mock_athena, mock_glue, mock_s3, mock_sts
from moto.core import DEFAULT_ACCOUNT_ID

from dbt.adapters.athena import AthenaAdapter
from dbt.adapters.athena import Plugin as AthenaPlugin
from dbt.adapters.athena.column import AthenaColumn
from dbt.adapters.athena.connections import AthenaCursor, AthenaParameterFormatter
from dbt.adapters.athena.exceptions import S3LocationException
from dbt.adapters.athena.relation import AthenaRelation, TableType
from dbt.adapters.athena.utils import AthenaCatalogType
from dbt.clients import agate_helper
from dbt.contracts.connection import ConnectionState
from dbt.contracts.files import FileHash
from dbt.contracts.graph.nodes import CompiledNode, DependsOn, NodeConfig
from dbt.contracts.relation import RelationType
from dbt.exceptions import ConnectionError, DbtRuntimeError
from dbt.node_types import NodeType

from .constants import (
    ATHENA_WORKGROUP,
    AWS_REGION,
    BUCKET,
    DATA_CATALOG_NAME,
    DATABASE_NAME,
    FEDERATED_QUERY_CATALOG_NAME,
    S3_STAGING_DIR,
    SHARED_DATA_CATALOG_NAME,
)
from .fixtures import seed_data
from .utils import TestAdapterConversions, config_from_parts_or_dicts, inject_adapter


class TestAthenaAdapter:
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
                    "s3_staging_dir": S3_STAGING_DIR,
                    "region_name": AWS_REGION,
                    "database": DATA_CATALOG_NAME,
                    "work_group": ATHENA_WORKGROUP,
                    "schema": DATABASE_NAME,
                }
            },
            "target": "test",
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self._adapter = None
        self.mock_manifest = mock.MagicMock()
        self.mock_manifest.get_used_schemas.return_value = {
            ("awsdatacatalog", "foo"),
            ("awsdatacatalog", "quux"),
            ("awsdatacatalog", "baz"),
            (SHARED_DATA_CATALOG_NAME, "foo"),
            (FEDERATED_QUERY_CATALOG_NAME, "foo"),
        }
        self.mock_manifest.nodes = {
            "model.root.model1": CompiledNode(
                name="model1",
                database="awsdatacatalog",
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
                database="awsdatacatalog",
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
            "model.root.model3": CompiledNode(
                name="model2",
                database="awsdatacatalog",
                schema="baz",
                resource_type=NodeType.Model,
                unique_id="model.root.model3",
                alias="qux",
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
                        "meta": {"owner": "data-engineers"},
                        "quoting": {},
                        "column_types": {},
                        "tags": [],
                    }
                ),
                tags=[],
                path="model3.sql",
                original_file_path="model3.sql",
                compiled=True,
                extra_ctes_injected=False,
                extra_ctes=[],
                checksum=FileHash.from_contents(""),
                raw_code="select * from source_table",
                language="",
            ),
            "model.root.model4": CompiledNode(
                name="model4",
                database=SHARED_DATA_CATALOG_NAME,
                schema="foo",
                resource_type=NodeType.Model,
                unique_id="model.root.model4",
                alias="bar",
                fqn=["root", "model4"],
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
                path="model4.sql",
                original_file_path="model4.sql",
                compiled=True,
                extra_ctes_injected=False,
                extra_ctes=[],
                checksum=FileHash.from_contents(""),
                raw_code="select * from source_table",
                language="",
            ),
            "model.root.model5": CompiledNode(
                name="model5",
                database=FEDERATED_QUERY_CATALOG_NAME,
                schema="foo",
                resource_type=NodeType.Model,
                unique_id="model.root.model5",
                alias="bar",
                fqn=["root", "model5"],
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
                path="model5.sql",
                original_file_path="model5.sql",
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
        assert arguments["retry_config"].attempt == 6
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
        (
            "s3_data_dir",
            "s3_data_naming",
            "s3_path_table_part",
            "s3_tmp_table_dir",
            "external_location",
            "is_temporary_table",
            "expected",
        ),
        (
            pytest.param(
                None, "table", None, None, None, False, "s3://my-bucket/test-dbt/tables/table", id="table naming"
            ),
            pytest.param(
                None, "unique", None, None, None, False, "s3://my-bucket/test-dbt/tables/uuid", id="unique naming"
            ),
            pytest.param(
                None,
                "table_unique",
                None,
                None,
                None,
                False,
                "s3://my-bucket/test-dbt/tables/table/uuid",
                id="table_unique naming",
            ),
            pytest.param(
                None,
                "schema_table",
                None,
                None,
                None,
                False,
                "s3://my-bucket/test-dbt/tables/schema/table",
                id="schema_table naming",
            ),
            pytest.param(
                None,
                "schema_table_unique",
                None,
                None,
                None,
                False,
                "s3://my-bucket/test-dbt/tables/schema/table/uuid",
                id="schema_table_unique naming",
            ),
            pytest.param(
                "s3://my-data-bucket/",
                "schema_table_unique",
                None,
                None,
                None,
                False,
                "s3://my-data-bucket/schema/table/uuid",
                id="data_dir set",
            ),
            pytest.param(
                "s3://my-data-bucket/",
                "schema_table_unique",
                None,
                None,
                "s3://path/to/external/",
                False,
                "s3://path/to/external",
                id="external_location set and not temporary",
            ),
            pytest.param(
                "s3://my-data-bucket/",
                "schema_table_unique",
                None,
                "s3://my-bucket/test-dbt-temp/",
                "s3://path/to/external/",
                True,
                "s3://my-bucket/test-dbt-temp/schema/table/uuid",
                id="s3_tmp_table_dir set, external_location set and temporary",
            ),
            pytest.param(
                "s3://my-data-bucket/",
                "schema_table_unique",
                None,
                None,
                "s3://path/to/external/",
                True,
                "s3://my-data-bucket/schema/table/uuid",
                id="s3_tmp_table_dir is empty, external_location set and temporary",
            ),
            pytest.param(
                None,
                "schema_table_unique",
                "other_table",
                None,
                None,
                False,
                "s3://my-bucket/test-dbt/tables/schema/other_table/uuid",
                id="s3_path_table_part set",
            ),
        ),
    )
    @patch("dbt.adapters.athena.impl.uuid4", return_value="uuid")
    def test_generate_s3_location(
        self,
        _,
        s3_data_dir,
        s3_data_naming,
        s3_tmp_table_dir,
        external_location,
        s3_path_table_part,
        is_temporary_table,
        expected,
    ):
        self.adapter.acquire_connection("dummy")
        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema="schema",
            identifier="table",
            s3_path_table_part=s3_path_table_part,
        )
        assert expected == self.adapter.generate_s3_location(
            relation, s3_data_dir, s3_data_naming, s3_tmp_table_dir, external_location, is_temporary_table
        )

    @mock_glue
    @mock_s3
    @mock_athena
    @mock_sts
    def test_get_table_location(self, dbt_debug_caplog, mock_aws_service):
        table_name = "test_table"
        self.adapter.acquire_connection("dummy")
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        mock_aws_service.create_table(table_name)
        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier=table_name,
        )
        assert self.adapter.get_glue_table_location(relation) == "s3://test-dbt-athena/tables/test_table"

    @mock_glue
    @mock_s3
    @mock_athena
    @mock_sts
    def test_get_table_location_raise_s3_location_exception(self, dbt_debug_caplog, mock_aws_service):
        table_name = "test_table"
        self.adapter.acquire_connection("dummy")
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        mock_aws_service.create_table(table_name, location="")
        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier=table_name,
        )
        with pytest.raises(S3LocationException) as exc:
            self.adapter.get_glue_table_location(relation)
        assert exc.value.args[0] == (
            'Relation "awsdatacatalog"."test_dbt_athena"."test_table" is of type \'table\' which requires a '
            "location, but no location returned by Glue."
        )

    @mock_glue
    @mock_s3
    @mock_athena
    @mock_sts
    def test_get_table_location_for_view(self, dbt_debug_caplog, mock_aws_service):
        view_name = "view"
        self.adapter.acquire_connection("dummy")
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        mock_aws_service.create_view(view_name)
        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME, schema=DATABASE_NAME, identifier=view_name, type=RelationType.View
        )
        assert self.adapter.get_glue_table_location(relation) is None

    @mock_glue
    @mock_s3
    @mock_athena
    @mock_sts
    def test_get_table_location_with_failure(self, dbt_debug_caplog, mock_aws_service):
        table_name = "test_table"
        self.adapter.acquire_connection("dummy")
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier=table_name,
        )
        assert self.adapter.get_glue_table_location(relation) is None
        assert f"Table {relation.render()} does not exists - Ignoring" in dbt_debug_caplog.getvalue()

    @mock_glue
    @mock_s3
    @mock_athena
    def test_clean_up_partitions_will_work(self, dbt_debug_caplog, mock_aws_service):
        table_name = "table"
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        mock_aws_service.create_table(table_name)
        mock_aws_service.add_data_in_table(table_name)
        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier=table_name,
        )
        self.adapter.acquire_connection("dummy")
        self.adapter.clean_up_partitions(relation, "dt < '2022-01-03'")
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
    @mock_sts
    def test_clean_up_table_table_does_not_exist(self, dbt_debug_caplog, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier="table",
        )
        result = self.adapter.clean_up_table(relation)
        assert result is None
        assert (
            'Table "awsdatacatalog"."test_dbt_athena"."table" does not exists - Ignoring' in dbt_debug_caplog.getvalue()
        )

    @mock_glue
    @mock_athena
    @mock_sts
    def test_clean_up_table_view(self, dbt_debug_caplog, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        mock_aws_service.create_view("test_view")
        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier="test_view",
            type=RelationType.View,
        )
        result = self.adapter.clean_up_table(relation)
        assert result is None

    @mock_glue
    @mock_s3
    @mock_athena
    @mock_sts
    def test_clean_up_table_delete_table(self, dbt_debug_caplog, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        mock_aws_service.create_table("table")
        mock_aws_service.add_data_in_table("table")
        self.adapter.acquire_connection("dummy")
        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier="table",
        )
        self.adapter.clean_up_table(relation)
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

    @mock_glue
    @mock_athena
    @mock_sts
    def test__get_one_catalog(self, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database("foo")
        mock_aws_service.create_database("quux")
        mock_aws_service.create_database("baz")
        mock_aws_service.create_table(table_name="bar", database_name="foo")
        mock_aws_service.create_table(table_name="bar", database_name="quux")
        mock_aws_service.create_table_without_type(table_name="qux", database_name="baz")
        mock_information_schema = mock.MagicMock()
        mock_information_schema.path.database = "awsdatacatalog"

        self.adapter.acquire_connection("dummy")
        actual = self.adapter._get_one_catalog(
            mock_information_schema,
            {
                "foo": {"bar"},
                "quux": {"bar"},
                "baz": {"qux"},
            },
            self.mock_manifest,
        )

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
            ("awsdatacatalog", "foo", "bar", "table", None, "id", 0, "string", None, "data-engineers"),
            ("awsdatacatalog", "foo", "bar", "table", None, "country", 1, "string", None, "data-engineers"),
            ("awsdatacatalog", "foo", "bar", "table", None, "dt", 2, "date", None, "data-engineers"),
            ("awsdatacatalog", "quux", "bar", "table", None, "id", 0, "string", None, "data-analysts"),
            ("awsdatacatalog", "quux", "bar", "table", None, "country", 1, "string", None, "data-analysts"),
            ("awsdatacatalog", "quux", "bar", "table", None, "dt", 2, "date", None, "data-analysts"),
            ("awsdatacatalog", "baz", "qux", "table", None, "id", 0, "string", None, "data-engineers"),
            ("awsdatacatalog", "baz", "qux", "table", None, "country", 1, "string", None, "data-engineers"),
        ]

        assert actual.column_names == expected_column_names
        assert len(actual.rows) == len(expected_rows)
        for row in actual.rows.values():
            assert row.values() in expected_rows

    @mock_glue
    @mock_athena
    def test__get_one_catalog_shared_catalog(self, mock_aws_service):
        mock_aws_service.create_data_catalog(catalog_name=SHARED_DATA_CATALOG_NAME, catalog_id=SHARED_DATA_CATALOG_NAME)
        mock_aws_service.create_database("foo", catalog_id=SHARED_DATA_CATALOG_NAME)
        mock_aws_service.create_table(table_name="bar", database_name="foo", catalog_id=SHARED_DATA_CATALOG_NAME)
        mock_information_schema = mock.MagicMock()
        mock_information_schema.path.database = SHARED_DATA_CATALOG_NAME

        self.adapter.acquire_connection("dummy")
        actual = self.adapter._get_one_catalog(
            mock_information_schema,
            {
                "foo": {"bar"},
            },
            self.mock_manifest,
        )

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
            ("9876543210", "foo", "bar", "table", None, "id", 0, "string", None, "data-engineers"),
            ("9876543210", "foo", "bar", "table", None, "country", 1, "string", None, "data-engineers"),
            ("9876543210", "foo", "bar", "table", None, "dt", 2, "date", None, "data-engineers"),
        ]

        assert actual.column_names == expected_column_names
        assert len(actual.rows) == len(expected_rows)
        for row in actual.rows.values():
            assert row.values() in expected_rows

    @mock_athena
    def test__get_one_catalog_federated_query_catalog(self, mock_aws_service):
        mock_aws_service.create_data_catalog(
            catalog_name=FEDERATED_QUERY_CATALOG_NAME, catalog_type=AthenaCatalogType.LAMBDA
        )
        mock_information_schema = mock.MagicMock()
        mock_information_schema.path.database = FEDERATED_QUERY_CATALOG_NAME

        # Original botocore _make_api_call function
        orig = botocore.client.BaseClient._make_api_call

        # Mocking this as list_table_metadata and creating non glue tables is not supported by moto.
        # Followed this guide: http://docs.getmoto.org/en/latest/docs/services/patching_other_services.html
        def mock_athena_list_table_metadata(self, operation_name, kwarg):
            if operation_name == "ListTableMetadata":
                return {
                    "TableMetadataList": [
                        {
                            "Name": "bar",
                            "TableType": "EXTERNAL_TABLE",
                            "Columns": [
                                {
                                    "Name": "id",
                                    "Type": "string",
                                },
                                {
                                    "Name": "country",
                                    "Type": "string",
                                },
                            ],
                            "PartitionKeys": [
                                {
                                    "Name": "dt",
                                    "Type": "date",
                                },
                            ],
                        }
                    ],
                }
            # If we don't want to patch the API call
            return orig(self, operation_name, kwarg)

        self.adapter.acquire_connection("dummy")
        with patch("botocore.client.BaseClient._make_api_call", new=mock_athena_list_table_metadata):
            actual = self.adapter._get_one_catalog(
                mock_information_schema,
                {
                    "foo": {"bar"},
                },
                self.mock_manifest,
            )

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
            (FEDERATED_QUERY_CATALOG_NAME, "foo", "bar", "table", None, "id", 0, "string", None, "data-engineers"),
            (FEDERATED_QUERY_CATALOG_NAME, "foo", "bar", "table", None, "country", 1, "string", None, "data-engineers"),
            (FEDERATED_QUERY_CATALOG_NAME, "foo", "bar", "table", None, "dt", 2, "date", None, "data-engineers"),
        ]

        assert actual.column_names == expected_column_names
        assert len(actual.rows) == len(expected_rows)
        for row in actual.rows.values():
            assert row.values() in expected_rows

    def test__get_catalog_schemas(self):
        res = self.adapter._get_catalog_schemas(self.mock_manifest)
        assert len(res.keys()) == 3

        information_schema_0 = list(res.keys())[0]
        assert information_schema_0.name == "INFORMATION_SCHEMA"
        assert information_schema_0.schema is None
        assert information_schema_0.database == "awsdatacatalog"
        relations = list(res.values())[0]
        assert set(relations.keys()) == {"foo", "quux", "baz"}
        assert list(relations.values()) == [{"bar"}, {"bar"}, {"qux"}]

        information_schema_1 = list(res.keys())[1]
        assert information_schema_1.name == "INFORMATION_SCHEMA"
        assert information_schema_1.schema is None
        assert information_schema_1.database == SHARED_DATA_CATALOG_NAME
        relations = list(res.values())[1]
        assert set(relations.keys()) == {"foo"}
        assert list(relations.values()) == [{"bar"}]

        information_schema_1 = list(res.keys())[2]
        assert information_schema_1.name == "INFORMATION_SCHEMA"
        assert information_schema_1.schema is None
        assert information_schema_1.database == FEDERATED_QUERY_CATALOG_NAME
        relations = list(res.values())[1]
        assert set(relations.keys()) == {"foo"}
        assert list(relations.values()) == [{"bar"}]

    @mock_athena
    @mock_sts
    def test__get_data_catalog(self, mock_aws_service):
        mock_aws_service.create_data_catalog()
        self.adapter.acquire_connection("dummy")
        res = self.adapter._get_data_catalog(DATA_CATALOG_NAME)
        assert {"Name": "awsdatacatalog", "Type": "GLUE", "Parameters": {"catalog-id": DEFAULT_ACCOUNT_ID}} == res

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
    @mock_sts
    def test_list_relations_without_caching_with_awsdatacatalog(self, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        mock_aws_service.create_table("table")
        mock_aws_service.create_table("other")
        mock_aws_service.create_view("view")
        mock_aws_service.create_table_without_table_type("without_table_type")
        schema_relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            quote_policy=self.adapter.config.quoting,
        )
        self._test_list_relations_without_caching(schema_relation)

    @mock_athena
    @mock_glue
    def test_list_relations_without_caching_with_other_glue_data_catalog(self, mock_aws_service):
        data_catalog_name = "other_data_catalog"
        mock_aws_service.create_data_catalog(data_catalog_name)
        mock_aws_service.create_database()
        mock_aws_service.create_table("table")
        mock_aws_service.create_table("other")
        mock_aws_service.create_view("view")
        mock_aws_service.create_table_without_table_type("without_table_type")
        schema_relation = self.adapter.Relation.create(
            database=data_catalog_name,
            schema=DATABASE_NAME,
            quote_policy=self.adapter.config.quoting,
        )
        self._test_list_relations_without_caching(schema_relation)

    @mock_athena
    @patch("dbt.adapters.athena.impl.SQLAdapter.list_relations_without_caching", return_value=[])
    def test_list_relations_without_caching_with_non_glue_data_catalog(
        self, parent_list_relations_without_caching, mock_aws_service
    ):
        data_catalog_name = "other_data_catalog"
        mock_aws_service.create_data_catalog(data_catalog_name, AthenaCatalogType.HIVE)
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
    @mock_sts
    def test_swap_table_with_partitions(self, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        target_table = "target_table"
        source_table = "source_table"
        mock_aws_service.create_table(source_table)
        mock_aws_service.add_partitions_to_table(DATABASE_NAME, source_table)
        mock_aws_service.create_table(target_table)
        mock_aws_service.add_partitions_to_table(DATABASE_NAME, target_table)
        source_relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier=source_table,
        )
        target_relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier=target_table,
        )
        self.adapter.swap_table(source_relation, target_relation)
        assert self.adapter.get_glue_table_location(target_relation) == f"s3://{BUCKET}/tables/{source_table}"

    @mock_athena
    @mock_glue
    @mock_s3
    @mock_sts
    def test_swap_table_without_partitions(self, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        target_table = "target_table"
        source_table = "source_table"
        mock_aws_service.create_table_without_partitions(source_table)
        mock_aws_service.create_table_without_partitions(target_table)
        source_relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier=source_table,
        )
        target_relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier=target_table,
        )
        self.adapter.swap_table(source_relation, target_relation)
        assert self.adapter.get_glue_table_location(target_relation) == f"s3://{BUCKET}/tables/{source_table}"

    @mock_athena
    @mock_glue
    @mock_s3
    @mock_sts
    def test_swap_table_with_partitions_to_one_without(self, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        target_table = "target_table"
        source_table = "source_table"
        # source table does not have partitions
        mock_aws_service.create_table_without_partitions(source_table)

        # the target table has partitions
        mock_aws_service.create_table(target_table)
        mock_aws_service.add_partitions_to_table(DATABASE_NAME, target_table)

        source_relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier=source_table,
        )
        target_relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier=target_table,
        )

        self.adapter.swap_table(source_relation, target_relation)
        glue_client = boto3.client("glue", region_name=AWS_REGION)

        target_table_partitions = glue_client.get_partitions(DatabaseName=DATABASE_NAME, TableName=target_table).get(
            "Partitions"
        )

        assert self.adapter.get_glue_table_location(target_relation) == f"s3://{BUCKET}/tables/{source_table}"
        assert len(target_table_partitions) == 0

    @mock_athena
    @mock_glue
    @mock_s3
    @mock_sts
    def test_swap_table_with_no_partitions_to_one_with(self, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        target_table = "target_table"
        source_table = "source_table"
        mock_aws_service.create_table(source_table)
        mock_aws_service.add_partitions_to_table(DATABASE_NAME, source_table)
        mock_aws_service.create_table_without_partitions(target_table)
        glue_client = boto3.client("glue", region_name=AWS_REGION)
        target_table_partitions = glue_client.get_partitions(DatabaseName=DATABASE_NAME, TableName=target_table).get(
            "Partitions"
        )
        assert len(target_table_partitions) == 0
        source_relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier=source_table,
        )
        target_relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier=target_table,
        )
        self.adapter.swap_table(source_relation, target_relation)
        target_table_partitions_after = glue_client.get_partitions(
            DatabaseName=DATABASE_NAME, TableName=target_table
        ).get("Partitions")

        assert self.adapter.get_glue_table_location(target_relation) == f"s3://{BUCKET}/tables/{source_table}"
        assert len(target_table_partitions_after) == 26

    @mock_athena
    @mock_glue
    def test__get_glue_table_versions_to_expire(self, mock_aws_service, dbt_debug_caplog):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        table_name = "my_table"
        mock_aws_service.create_table(table_name)
        mock_aws_service.add_table_version(DATABASE_NAME, table_name)
        mock_aws_service.add_table_version(DATABASE_NAME, table_name)
        mock_aws_service.add_table_version(DATABASE_NAME, table_name)
        glue = boto3.client("glue", region_name=AWS_REGION)
        table_versions = glue.get_table_versions(DatabaseName=DATABASE_NAME, TableName=table_name).get("TableVersions")
        assert len(table_versions) == 4
        version_to_keep = 1
        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier=table_name,
        )
        versions_to_expire = self.adapter._get_glue_table_versions_to_expire(relation, version_to_keep)
        assert len(versions_to_expire) == 3
        assert [v["VersionId"] for v in versions_to_expire] == ["3", "2", "1"]

    @mock_athena
    @mock_glue
    @mock_s3
    @mock_sts
    def test_expire_glue_table_versions(self, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        table_name = "my_table"
        mock_aws_service.create_table(table_name)
        mock_aws_service.add_table_version(DATABASE_NAME, table_name)
        mock_aws_service.add_table_version(DATABASE_NAME, table_name)
        mock_aws_service.add_table_version(DATABASE_NAME, table_name)
        glue = boto3.client("glue", region_name=AWS_REGION)
        table_versions = glue.get_table_versions(DatabaseName=DATABASE_NAME, TableName=table_name).get("TableVersions")
        assert len(table_versions) == 4
        version_to_keep = 1
        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier=table_name,
        )
        self.adapter.expire_glue_table_versions(relation, version_to_keep, False)
        # TODO delete_table_version is not implemented in moto
        # TODO moto issue https://github.com/getmoto/moto/issues/5952
        # assert len(result) == 3

    @mock_s3
    def test_upload_seed_to_s3(self, mock_aws_service):
        seed_table = agate.Table.from_object(seed_data)
        self.adapter.acquire_connection("dummy")

        database = "db_seeds"
        table = "data"

        s3_client = boto3.client("s3", region_name=AWS_REGION)
        s3_client.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": AWS_REGION})

        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=database,
            identifier=table,
        )

        location = self.adapter.upload_seed_to_s3(
            relation,
            seed_table,
            s3_data_dir=f"s3://{BUCKET}",
            s3_data_naming="schema_table",
            external_location=None,
        )

        prefix = "db_seeds/data"
        objects = s3_client.list_objects(Bucket=BUCKET, Prefix=prefix).get("Contents")

        assert location == f"s3://{BUCKET}/{prefix}"
        assert len(objects) == 1
        assert objects[0].get("Key").endswith(".csv")

    @mock_s3
    def test_upload_seed_to_s3_external_location(self, mock_aws_service):
        seed_table = agate.Table.from_object(seed_data)
        self.adapter.acquire_connection("dummy")

        bucket = "my-external-location"
        prefix = "seeds/one"
        external_location = f"s3://{bucket}/{prefix}"

        s3_client = boto3.client("s3", region_name=AWS_REGION)
        s3_client.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": AWS_REGION})

        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema="db_seeds",
            identifier="data",
        )

        location = self.adapter.upload_seed_to_s3(
            relation,
            seed_table,
            s3_data_dir=None,
            s3_data_naming="schema_table",
            external_location=external_location,
        )

        objects = s3_client.list_objects(Bucket=bucket, Prefix=prefix).get("Contents")

        assert location == f"s3://{bucket}/{prefix}"
        assert len(objects) == 1
        assert objects[0].get("Key").endswith(".csv")

    @mock_athena
    def test_get_work_group_output_location(self, mock_aws_service):
        self.adapter.acquire_connection("dummy")
        mock_aws_service.create_work_group_with_output_location_enforced(ATHENA_WORKGROUP)
        work_group_location_enforced = self.adapter.is_work_group_output_location_enforced()
        assert work_group_location_enforced

    @mock_athena
    def test_get_work_group_output_location_no_location(self, mock_aws_service):
        self.adapter.acquire_connection("dummy")
        mock_aws_service.create_work_group_no_output_location(ATHENA_WORKGROUP)
        work_group_location_enforced = self.adapter.is_work_group_output_location_enforced()
        assert not work_group_location_enforced

    @mock_athena
    def test_get_work_group_output_location_not_enforced(self, mock_aws_service):
        self.adapter.acquire_connection("dummy")
        mock_aws_service.create_work_group_with_output_location_not_enforced(ATHENA_WORKGROUP)
        work_group_location_enforced = self.adapter.is_work_group_output_location_enforced()
        assert not work_group_location_enforced

    @mock_athena
    @mock_glue
    @mock_s3
    @mock_sts
    def test_persist_docs_to_glue_no_comment(self, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        table_name = "my_table"
        mock_aws_service.create_table(table_name)
        schema_relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier=table_name,
        )
        self.adapter.persist_docs_to_glue(
            schema_relation,
            {
                "description": """
                        A table with str, 123, &^% \" and '

                          and an other paragraph.
                    """,
                "columns": {
                    "id": {
                        "description": """
                        A column with str, 123, &^% \" and '

                          and an other paragraph.
                    """,
                    }
                },
            },
            False,
            False,
        )
        glue = boto3.client("glue", region_name=AWS_REGION)
        table = glue.get_table(DatabaseName=DATABASE_NAME, Name=table_name).get("Table")
        assert not table.get("Description", "")
        assert not table["Parameters"].get("comment")
        assert all(not col.get("Comment") for col in table["StorageDescriptor"]["Columns"])

    @mock_athena
    @mock_glue
    @mock_s3
    @mock_sts
    def test_persist_docs_to_glue_comment(self, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        table_name = "my_table"
        mock_aws_service.create_table(table_name)
        schema_relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
            identifier=table_name,
        )
        self.adapter.persist_docs_to_glue(
            schema_relation,
            {
                "description": """
                        A table with str, 123, &^% \" and '

                          and an other paragraph.
                    """,
                "columns": {
                    "id": {
                        "description": """
                        A column with str, 123, &^% \" and '

                          and an other paragraph.
                    """,
                    }
                },
            },
            True,
            True,
        )
        glue = boto3.client("glue", region_name=AWS_REGION)
        table = glue.get_table(DatabaseName=DATABASE_NAME, Name=table_name).get("Table")
        assert table["Description"] == "A table with str, 123, &^% \" and ' and an other paragraph."
        assert table["Parameters"]["comment"] == "A table with str, 123, &^% \" and ' and an other paragraph."
        col_id = [col for col in table["StorageDescriptor"]["Columns"] if col["Name"] == "id"][0]
        assert col_id["Comment"] == "A column with str, 123, &^% \" and ' and an other paragraph."

    @mock_athena
    @mock_glue
    def test_list_schemas(self, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database(name="foo")
        mock_aws_service.create_database(name="bar")
        mock_aws_service.create_database(name="quux")
        self.adapter.acquire_connection("dummy")
        res = self.adapter.list_schemas("")
        assert sorted(res) == ["bar", "foo", "quux"]

    @mock_athena
    @mock_glue
    @mock_sts
    def test_get_columns_in_relation(self, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        mock_aws_service.create_table("tbl_name")
        self.adapter.acquire_connection("dummy")
        columns = self.adapter.get_columns_in_relation(
            self.adapter.Relation.create(
                database=DATA_CATALOG_NAME,
                schema=DATABASE_NAME,
                identifier="tbl_name",
            )
        )
        assert columns == [
            AthenaColumn(column="id", dtype="string", table_type=TableType.TABLE),
            AthenaColumn(column="country", dtype="string", table_type=TableType.TABLE),
            AthenaColumn(column="dt", dtype="date", table_type=TableType.TABLE),
        ]

    @mock_athena
    @mock_glue
    @mock_sts
    def test_get_columns_in_relation_not_found_table(self, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        self.adapter.acquire_connection("dummy")
        columns = self.adapter.get_columns_in_relation(
            self.adapter.Relation.create(
                database=DATA_CATALOG_NAME,
                schema=DATABASE_NAME,
                identifier="tbl_name",
            )
        )
        assert columns == []

    @mock_athena
    @mock_glue
    @mock_sts
    def test_delete_from_glue_catalog(self, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        mock_aws_service.create_table("tbl_name")
        self.adapter.acquire_connection("dummy")
        relation = self.adapter.Relation.create(database=DATA_CATALOG_NAME, schema=DATABASE_NAME, identifier="tbl_name")
        self.adapter.delete_from_glue_catalog(relation)
        glue = boto3.client("glue", region_name=AWS_REGION)
        tables_list = glue.get_tables(DatabaseName=DATABASE_NAME).get("TableList")
        assert tables_list == []

    @mock_athena
    @mock_glue
    @mock_sts
    def test_delete_from_glue_catalog_not_found_table(self, dbt_debug_caplog, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        mock_aws_service.create_table("tbl_name")
        self.adapter.acquire_connection("dummy")
        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME, schema=DATABASE_NAME, identifier="tbl_does_not_exist"
        )
        delete_table = self.adapter.delete_from_glue_catalog(relation)
        assert delete_table is None
        error_msg = f"Table {relation.render()} does not exist and will not be deleted, ignoring"
        assert error_msg in dbt_debug_caplog.getvalue()

    @mock_glue
    @mock_s3
    @mock_athena
    @mock_sts
    def test__get_relation_type_table(self, dbt_debug_caplog, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        mock_aws_service.create_table("test_table")
        self.adapter.acquire_connection("dummy")
        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME, schema=DATABASE_NAME, identifier="test_table"
        )
        table_type = self.adapter.get_glue_table_type(relation)
        assert table_type == TableType.TABLE

    @mock_glue
    @mock_s3
    @mock_athena
    @mock_sts
    def test__get_relation_type_with_no_type(self, dbt_debug_caplog, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        mock_aws_service.create_table_without_table_type("test_table")
        self.adapter.acquire_connection("dummy")
        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME, schema=DATABASE_NAME, identifier="test_table"
        )
        with pytest.raises(ValueError):
            self.adapter.get_glue_table_type(relation)

    @mock_glue
    @mock_s3
    @mock_athena
    @mock_sts
    def test__get_relation_type_view(self, dbt_debug_caplog, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        mock_aws_service.create_view("test_view")
        self.adapter.acquire_connection("dummy")
        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME, schema=DATABASE_NAME, identifier="test_view"
        )
        table_type = self.adapter.get_glue_table_type(relation)
        assert table_type == TableType.VIEW

    @mock_glue
    @mock_s3
    @mock_athena
    @mock_sts
    def test__get_relation_type_iceberg(self, dbt_debug_caplog, mock_aws_service):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_database()
        mock_aws_service.create_iceberg_table("test_iceberg")
        self.adapter.acquire_connection("dummy")
        relation = self.adapter.Relation.create(
            database=DATA_CATALOG_NAME, schema=DATABASE_NAME, identifier="test_iceberg"
        )
        table_type = self.adapter.get_glue_table_type(relation)
        assert table_type == TableType.ICEBERG

    @pytest.mark.parametrize(
        "column,expected",
        [
            pytest.param({"Name": "user_id", "Type": "int", "Parameters": {"iceberg.field.current": "true"}}, True),
            pytest.param({"Name": "user_id", "Type": "int", "Parameters": {"iceberg.field.current": "false"}}, False),
            pytest.param({"Name": "user_id", "Type": "int"}, True),
        ],
    )
    def test__is_current_column(self, column, expected):
        assert self.adapter._is_current_column(column) == expected


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
