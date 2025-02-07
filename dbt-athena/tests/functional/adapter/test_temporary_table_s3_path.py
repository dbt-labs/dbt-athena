import pytest


from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt
import json

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("dbt_test")

models__tmp_s3_path_sql = """
{{ config(
        materialized='incremental',
        incremental_strategy='merge',
        partitioned_by=['dt'],
        unique_key=['date_key'],
        force_batch='true',
        table_type='iceberg',
        s3_data_naming='schema_table'
    )
}}


SELECT 1 as date_key, 1 as id, '2022-01-01' AS dt
union all
SELECT 2, 2, '2022-01-02'
"""


class TestTmpTableS3Path:
    @pytest.fixture(scope="class")
    def models(self):
        return {"temporary_table_s3_path.sql": models__tmp_s3_path_sql}

    @staticmethod
    def extract_segment(input_string):
        # Define the prefix to split on
        prefix = "models/"

        if prefix in input_string:
            # Split the string on the prefix
            _, after_prefix = input_string.split(prefix, 1)
            # Split the remaining string on "/" and return the first part
            return after_prefix.split("/", 1)[0]
        return None

    @staticmethod
    def extract_s3_path_folder_name(dbt_run_capsys_output: str) -> str:
        result = None
        for events_msg in dbt_run_capsys_output.split("\n")[1:]:
            base_msg_data = None
            try:
                base_msg_data = json.loads(events_msg).get("data")
            except json.JSONDecodeError:
                pass
            """s3_data_dir will be stored in data folder and
               s3_tmp_table_dir will be stored in temporary folder
               With the code change now tmp table should be stored in temporary folder"""
            if base_msg_data:
                base_msg = base_msg_data.get("base_msg")
                if "is stored in" in str(base_msg):
                    result = TestTmpTableS3Path.extract_segment(base_msg)
                    return result

    def test__temporary_table_s3_path(self, project, capsys):
        relation_name = "temporary_table_s3_path"
        model_run = run_dbt(
            [
                "--debug",
                "--log-format",
                "json",
                "run",
                "--select",
                relation_name,
            ]
        )

        model_run_result = model_run.results[0]

        assert model_run_result.status == RunStatus.Success

        out, _ = capsys.readouterr()
        s3_folder_name = TestTmpTableS3Path.extract_s3_path_folder_name(out)

        assert s3_folder_name == "temporary"
