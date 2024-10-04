# import os
# import shutil
# from collections import Counter
# from copy import deepcopy

# import pytest
# import yaml

# from dbt.tests.adapter.python_model.test_python_model import (
#     BasePythonIncrementalTests,
#     BasePythonModelTests,
# )
# from dbt.tests.util import run_dbt

# basic_sql = """
# {{ config(materialized="table") }}
# select 1 as column_1, 2 as column_2, '{{ run_started_at.strftime("%Y-%m-%d") }}' as run_date
# """

# basic_python = """
# def model(dbt, spark):
#     dbt.config(
#         materialized='table',
#     )
#     df =  dbt.ref("model")
#     return df
# """

# basic_spark_python = """
# def model(dbt, spark_session):
#     dbt.config(materialized="table")

#     data = [(1,), (2,), (3,), (4,)]

#     df = spark_session.createDataFrame(data, ["A"])

#     return df
# """

# second_sql = """
# select * from {{ref('my_python_model')}}
# """

# schema_yml = """version: 2
# models:
#   - name: model
#     versions:
#       - v: 1
# """


# class TestBasePythonModelTests(BasePythonModelTests):
#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "schema.yml": schema_yml,
#             "model.sql": basic_sql,
#             "my_python_model.py": basic_python,
#             "spark_model.py": basic_spark_python,
#             "second_sql_model.sql": second_sql,
#         }


# incremental_python = """
# def model(dbt, spark_session):
#     dbt.config(materialized="incremental")
#     df = dbt.ref("model")

#     if dbt.is_incremental:
#         max_from_this = (
#             f"select max(run_date) from {dbt.this.schema}.{dbt.this.identifier}"
#         )
#         df = df.filter(df.run_date >= spark_session.sql(max_from_this).collect()[0][0])

#     return df
# """


# class TestBasePythonIncrementalTests(BasePythonIncrementalTests):
#     @pytest.fixture(scope="class")
#     def project_config_update(self):
#         return {"models": {"+incremental_strategy": "append"}}

#     @pytest.fixture(scope="class")
#     def models(self):
#         return {"model.sql": basic_sql, "incremental.py": incremental_python}

#     def test_incremental(self, project):
#         vars_dict = {
#             "test_run_schema": project.test_schema,
#         }

#         results = run_dbt(["run", "--vars", yaml.safe_dump(vars_dict)])
#         assert len(results) == 2


# class TestPythonClonePossible:
#     """Test that basic clone operations are possible on Python models. This
#     class has been adapted from the BaseClone and BaseClonePossible classes in
#     dbt-core and modified to test Python models in addition to SQL models."""

#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "schema.yml": schema_yml,
#             "model.sql": basic_sql,
#             "my_python_model.py": basic_python,
#         }

#     @pytest.fixture(scope="class")
#     def other_schema(self, unique_schema):
#         return unique_schema + "_other"

#     @pytest.fixture(scope="class")
#     def profiles_config_update(self, dbt_profile_target, unique_schema, other_schema):
#         """Update the profiles config to duplicate the default schema to a
#         separate schema called `otherschema`."""
#         outputs = {"default": dbt_profile_target, "otherschema": deepcopy(dbt_profile_target)}
#         outputs["default"]["schema"] = unique_schema
#         outputs["otherschema"]["schema"] = other_schema
#         return {"test": {"outputs": outputs, "target": "default"}}

#     def copy_state(self, project_root):
#         """Copy the manifest.json project for a run into a separate `state/`
#         directory inside the project root, so that we can reference it
#         for cloning."""
#         state_path = os.path.join(project_root, "state")
#         if not os.path.exists(state_path):
#             os.makedirs(state_path)
#         shutil.copyfile(f"{project_root}/target/manifest.json", f"{state_path}/manifest.json")

#     def run_and_save_state(self, project_root):
#         """Run models and save the state to a separate directory to prepare
#         for testing clone operations."""
#         results = run_dbt(["run"])
#         assert len(results) == 2
#         self.copy_state(project_root)

#     def assert_relation_types_match_counter(self, project, schema, counter):
#         """Check that relation types in a given database and schema match the
#         counts specified by a Counter object."""
#         schema_relations = project.adapter.list_relations(database=project.database, schema=schema)
#         schema_types = [str(r.type) for r in schema_relations]
#         assert Counter(schema_types) == counter

#     def test_can_clone_true(self, project, unique_schema, other_schema):
#         """Test that Python models can be cloned using `dbt clone`. Adapted from
#         the BaseClonePossible.test_can_clone_true test in dbt-core."""
#         project.create_test_schema(other_schema)
#         self.run_and_save_state(project.project_root)

#         # Base models should be materialized as tables
#         self.assert_relation_types_match_counter(project, unique_schema, Counter({"table": 2}))

#         clone_args = [
#             "clone",
#             "--state",
#             "state",
#             "--target",
#             "otherschema",
#         ]

#         results = run_dbt(clone_args)
#         assert len(results) == 2

#         # Cloned models should be materialized as views
#         self.assert_relation_types_match_counter(project, other_schema, Counter({"view": 2}))

#         # Objects already exist, so this is a no-op
#         results = run_dbt(clone_args)
#         assert len(results) == 2
#         assert all("no-op" in r.message.lower() for r in results)

#         # Recreate all objects
#         results = run_dbt([*clone_args, "--full-refresh"])
#         assert len(results) == 2
#         assert not any("no-op" in r.message.lower() for r in results)
