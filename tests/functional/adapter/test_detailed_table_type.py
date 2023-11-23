import re

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

get_detailed_table_type_sql = """
{% macro get_detailed_table_type(schema) %}
    {% if execute %}
    {% set relation = api.Relation.create(database="awsdatacatalog", schema=schema) %}
    {% set schema_tables = adapter.list_relations_without_caching(schema_relation = relation) %}
    {% for rel in schema_tables %}
        {% do log('Detailed Table Type: ' ~ rel.detailed_table_type, info=True) %}
    {% endfor %}
    {% endif %}
{% endmacro %}
"""

# Model SQL for an Iceberg table
iceberg_model_sql = """
  select 1 as id, 'iceberg' as name
  {{ config(materialized='table', table_type='iceberg') }}
"""


@pytest.mark.usefixtures("project")
class TestDetailedTableType:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"get_detailed_table_type.sql": get_detailed_table_type_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_model.sql": iceberg_model_sql}

    def test_detailed_table_type(self, project):
        # Run the models
        run_results = run_dbt(["run"])
        assert len(run_results) == 1  # Ensure model ran successfully

        args_str = f'{{"schema": "{project.test_schema}"}}'
        run_macro, stdout = run_dbt_and_capture(["run-operation", "get_detailed_table_type", "--args", args_str])
        iceberg_table_type = re.search(r"Detailed Table Type: (\w+)", stdout).group(1)
        assert iceberg_table_type == "ICEBERG"
