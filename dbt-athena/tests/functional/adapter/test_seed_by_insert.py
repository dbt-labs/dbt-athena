import pytest

from dbt.tests.adapter.basic.files import (
    base_materialized_var_sql,
    base_table_sql,
    base_view_sql,
    schema_base_yml,
)
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations

seed_by_insert_schema = """

seeds:
- name: base
  config:
    seed_by_insert: True

"""


class TestSimpleMaterializationsHive(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def models(self):
        schema = schema_base_yml + seed_by_insert_schema
        return {
            "view_model.sql": base_view_sql,
            "table_model.sql": base_table_sql,
            "swappable.sql": base_materialized_var_sql,
            "schema.yml": schema,
        }
