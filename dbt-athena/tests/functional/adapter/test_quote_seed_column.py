import pytest

from dbt.tests.adapter.basic.files import (
    base_materialized_var_sql,
    base_table_sql,
    base_view_sql,
    schema_base_yml,
    seeds_base_csv,
)
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations

seed_base_csv_underscore_column = seeds_base_csv.replace("name", "_name")

quote_columns_seed_schema = """

seeds:
- name: base
  config:
    quote_columns: true

"""


class TestSimpleMaterializationsHive(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def models(self):
        schema = schema_base_yml + quote_columns_seed_schema
        return {
            "view_model.sql": base_view_sql,
            "table_model.sql": base_table_sql,
            "swappable.sql": base_materialized_var_sql,
            "schema.yml": schema,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seed_base_csv_underscore_column,
        }
