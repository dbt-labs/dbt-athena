import pytest
import yaml

from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonIncrementalTests,
    BasePythonModelTests,
)
from dbt.tests.util import run_dbt

basic_sql = """
{{ config(materialized="table") }}
select 1 as column_1, 2 as column_2, '{{ run_started_at.strftime("%Y-%m-%d") }}' as run_date
"""

basic_python = """
def model(dbt, spark):
    dbt.config(
        materialized='table',
    )
    df =  dbt.ref("model")
    return df
"""

basic_spark_python = """
def model(dbt, spark_session):
    dbt.config(materialized="table")

    data = [(1,), (2,), (3,), (4,)]

    df = spark_session.createDataFrame(data, ["A"])

    return df
"""

second_sql = """
select * from {{ref('my_python_model')}}
"""

schema_yml = """version: 2
models:
  - name: model
    versions:
      - v: 1
"""


class TestBasePythonModelTests(BasePythonModelTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml,
            "model.sql": basic_sql,
            "my_python_model.py": basic_python,
            "spark_model.py": basic_spark_python,
            "second_sql_model.sql": second_sql,
        }


incremental_python = """
def model(dbt, spark_session):
    dbt.config(materialized="incremental")
    df = dbt.ref("model")

    if dbt.is_incremental:
        max_from_this = (
            f"select max(run_date) from {dbt.this.schema}.{dbt.this.identifier}"
        )
        df = df.filter(df.run_date >= spark_session.sql(max_from_this).collect()[0][0])

    return df
"""


class TestBasePythonIncrementalTests(BasePythonIncrementalTests):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "append"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": basic_sql, "incremental.py": incremental_python}

    def test_incremental(self, project):
        vars_dict = {
            "test_run_schema": project.test_schema,
        }

        results = run_dbt(["run", "--vars", yaml.safe_dump(vars_dict)])
        assert len(results) == 2
