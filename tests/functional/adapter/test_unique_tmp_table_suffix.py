import pytest
import re
import json
from typing import List

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt

models__unique_tmp_table_suffix_sql = """
{{ config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partitioned_by=['date_column'],
        unique_tmp_table_suffix=True
    )
}}
select
    random() as rnd,
    cast(from_iso8601_date('{{ var('logical_date') }}') as date) as date_column
"""


def extract_running_statements(dbt_run_capsys_output: str) -> List[str]:
    base_msgs = []
    # Skipping "Invoking dbt with ['run', '--select', 'unique_tmp_table_suffix'..."
    for events_msg in dbt_run_capsys_output.split('\n')[1:]:
        base_msg = None
        # Best effort solution to avoid invalid records and blank lines
        try:
            base_msg = json.loads(events_msg).get('data').get('base_msg')
        except json.JSONDecodeError:
            pass
        if base_msg and "Running Athena query:" in base_msg:
            base_msgs.append(base_msg)
    return base_msgs


class TestUniqueTmpTableSuffix:
    @pytest.fixture(scope="class")
    def models(self):
        return {"unique_tmp_table_suffix.sql": models__unique_tmp_table_suffix_sql}

    def test__unique_tmp_table_suffix(self, project, capsys):
        relation_name = "unique_tmp_table_suffix"
        model_run_result_row_count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"
        expected_unique_table_name_re = r"unique_tmp_table_suffix__dbt_tmp_[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}"

        first_model_run = run_dbt([
            "run",
            "--select", relation_name,
            "--vars", '{"logical_date": "2024-01-01"}',
            "--log-level", "debug", "--log-format", "json"
        ])

        first_model_run_result = first_model_run.results[0]

        assert first_model_run_result.status == RunStatus.Success

        out, _ = capsys.readouterr()
        athena_running_statements = extract_running_statements(out)

        # Run statements logged output should not contain unique table suffix after first run
        assert not bool(re.search(expected_unique_table_name_re, ''.join(athena_running_statements)))

        records_count_first_run = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]

        assert records_count_first_run == 1

        incremental_model_run = run_dbt([
            "run",
            "--select", relation_name,
            "--vars", '{"logical_date": "2024-01-02"}',
            "--log-level", "debug", "--log-format", "json"
        ])

        incremental_model_run_result = incremental_model_run.results[0]

        assert incremental_model_run_result.status == RunStatus.Success

        out, _ = capsys.readouterr()
        athena_running_statements = extract_running_statements(out)

        # Run statements logged for subsequent incremental model runs should use unique table suffix
        assert bool(re.search(expected_unique_table_name_re, ''.join(athena_running_statements)))
