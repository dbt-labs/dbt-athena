"""
Run the basic dbt test suite on hive tables when applicable.

Some test classes are not included here, because they don't contain table models.
Those are run in the hive test suite.
"""
import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.adapter.basic.files import (
    base_ephemeral_sql,
    base_materialized_var_sql,
    base_table_sql,
    base_view_sql,
    config_materialized_incremental,
    config_materialized_table,
    ephemeral_table_sql,
    ephemeral_view_sql,
    generic_test_table_yml,
    generic_test_view_yml,
    incremental_sql,
    model_base,
    model_ephemeral,
    schema_base_yml,
)
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.util import run_dbt

iceberg_config_materialized_table = """
  {{ config(materialized="table", table_type="iceberg") }}
"""

iceberg_config_materialized_incremental = """
  {{ config(materialized="incremental", table_type="iceberg", incremental_strategy="merge", unique_key="id") }}
"""

iceberg_model_base = """
  select
      id,
      name,
      {{ cast_timestamp('some_date') }} as some_date
  from {{ source('raw', 'seed') }}
"""

iceberg_model_ephemeral = """
  select
      id,
      name,
      {{ cast_timestamp('some_date') }} as some_date
  from {{ ref('ephemeral') }}
"""


def configure_single_model_to_use_iceberg(model):
    """Adjust a given model configuration to use iceberg instead of hive."""
    replacements = [
        (config_materialized_table, iceberg_config_materialized_table),
        (config_materialized_incremental, iceberg_config_materialized_incremental),
        (model_base, iceberg_model_base),
        (model_ephemeral, iceberg_model_ephemeral),
    ]
    for original, new in replacements:
        model = model.replace(original.strip(), new.strip())

    return model


def configure_models_to_use_iceberg(models):
    """Loop over all the dbt models and set the table configuration to iceberg."""
    return {key: configure_single_model_to_use_iceberg(val) for key, val in models.items()}


class TestIcebergMergeNonUniqueLocation:
    """Incremental merge requires a unique location"""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+materialized": "incremental",
                "+incremental_strategy": "merge",
                "+table_type": "iceberg",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        """Setting non-unique S3 naming configs"""

        return {
            "incremental_model_table.sql": '{{ config(s3_data_naming="table") }} select 1 as col_1',
            "incremental_model_schema_table.sql": '{{ config(s3_data_naming="schema_table") }} select 1 as col_1',
        }

    def test__nonunique_location(self, project):
        """Runs should fail for compilation error"""

        def assert_compilation_error(dbt_run):
            run_result = dbt_run.results[0]
            status, message = run_result.status, run_result.message
            assert status == RunStatus.Error
            assert message.startswith("Compilation Error")

        run = run_dbt(["run", "--select", "incremental_model_table"], expect_pass=False)
        assert_compilation_error(run)

        run = run_dbt(["run", "--select", "incremental_model_schema_table"], expect_pass=False)
        assert_compilation_error(run)


@pytest.mark.skip(
    reason="The materialized var doesn't work well, because we only want to change tables, not views. "
    "It's hard to come up with an elegant fix."
)
class TestSimpleMaterializationsIceberg(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def models(self):
        return configure_models_to_use_iceberg(
            {
                "view_model.sql": base_view_sql,
                "table_model.sql": base_table_sql,
                "swappable.sql": base_materialized_var_sql,
                "schema.yml": schema_base_yml,
            }
        )


class TestEphemeralIceberg(BaseEphemeral):
    @pytest.fixture(scope="class")
    def models(self):
        return configure_models_to_use_iceberg(
            {
                "ephemeral.sql": base_ephemeral_sql,
                "view_model.sql": ephemeral_view_sql,
                "table_model.sql": ephemeral_table_sql,
                "schema.yml": schema_base_yml,
            }
        )


class TestIncrementalIceberg(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return configure_models_to_use_iceberg(
            {
                "incremental.sql": incremental_sql,
                "schema.yml": schema_base_yml,
            }
        )


class TestGenericTestsIceberg(BaseGenericTests):
    @pytest.fixture(scope="class")
    def models(self):
        return configure_models_to_use_iceberg(
            {
                "view_model.sql": base_view_sql,
                "table_model.sql": base_table_sql,
                "schema.yml": schema_base_yml,
                "schema_view.yml": generic_test_view_yml,
                "schema_table.yml": generic_test_table_yml,
            }
        )


@pytest.mark.skip(reason="The in-place update is not supported for seeds. We need our own implementation instead.")
class TestSnapshotCheckColsIceberg(BaseSnapshotCheckCols):
    pass


@pytest.mark.skip(reason="The in-place update is not supported for seeds. We need our own implementation instead.")
class TestSnapshotTimestampIceberg(BaseSnapshotTimestamp):
    pass


@pytest.mark.skip(
    reason="Fails because the test tries to fetch the table metadata during the compile step, "
    "before the models are actually run. Not sure how this test is intended to work."
)
class TestBaseAdapterMethodIceberg(BaseAdapterMethod):
    pass
