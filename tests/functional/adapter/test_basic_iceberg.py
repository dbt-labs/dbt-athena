"""
Run the basic dbt test suite on hive tables when applicable.

Some test classes are not included here, because they don't contain table models.
Those are run in the hive test suite.
"""
import pytest

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
