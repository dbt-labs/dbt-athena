import pytest

from dbt.tests.adapter.basic.files import (
    base_ephemeral_sql,
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
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
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


def replace_single_model(model):
    replacements = [
        (config_materialized_table, iceberg_config_materialized_table),
        (config_materialized_incremental, iceberg_config_materialized_incremental),
        (model_base, iceberg_model_base),
        (model_ephemeral, iceberg_model_ephemeral),
    ]
    for original, new in replacements:
        model = model.replace(original.strip(), new.strip())

    return model


def set_iceberg_models(models):
    """Add the table_type keyword to the model configurations"""
    return {key: replace_single_model(val) for key, val in models.items()}


class TestSimpleMaterializationsGlue(BaseSimpleMaterializations):
    pass


class TestSingularTestsGlue(BaseSingularTests):
    pass


class TestSingularTestsEphemeralGlue(BaseSingularTestsEphemeral):
    pass


class TestEmptyGlue(BaseEmpty):
    pass


class TestEphemeralGlue(BaseEphemeral):
    pass


class TestEphemeralIceberg(BaseEphemeral):
    @pytest.fixture(scope="class")
    def models(self):
        return set_iceberg_models(
            {
                "ephemeral.sql": base_ephemeral_sql,
                "view_model.sql": ephemeral_view_sql,
                "table_model.sql": ephemeral_table_sql,
                "schema.yml": schema_base_yml,
            }
        )


class TestIncrementalGlue(BaseIncremental):
    pass


class TestIncrementalIceberg(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return set_iceberg_models({"incremental.sql": incremental_sql, "schema.yml": schema_base_yml})


class TestGenericTestsGlue(BaseGenericTests):
    pass


class TestGenericTestsIceberg(BaseGenericTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": base_view_sql,
            "table_model.sql": base_table_sql,
            "schema.yml": schema_base_yml,
            "schema_view.yml": generic_test_view_yml,
            "schema_table.yml": generic_test_table_yml,
        }


@pytest.mark.skip(
    reason="Skipped, because the in-place update is not supported for seeds. We need our own implementation instead."
)
class TestSnapshotCheckColsGlue(BaseSnapshotCheckCols):
    pass


@pytest.mark.skip(
    reason="Skipped, because the in-place update is not supported for seeds. We need our own implementation instead."
)
class TestSnapshotTimestampGlue(BaseSnapshotTimestamp):
    pass


@pytest.mark.skip(
    reason="Fails because the test tries to fetch the table metadata during the compile step, "
    "before the models are actually run. Not sure how this test is intended to work."
)
class TestBaseAdapterMethod(BaseAdapterMethod):
    pass
