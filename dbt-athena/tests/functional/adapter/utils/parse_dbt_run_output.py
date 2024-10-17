import json
import re
from typing import List


def extract_running_create_statements(dbt_run_capsys_output: str, relation_name: str) -> List[str]:
    sql_create_statements = []
    # Skipping "Invoking dbt with ['run', '--select', 'unique_tmp_table_suffix'..."
    for events_msg in dbt_run_capsys_output.split("\n")[1:]:
        base_msg_data = None
        # Best effort solution to avoid invalid records and blank lines
        try:
            base_msg_data = json.loads(events_msg).get("data")
        except json.JSONDecodeError:
            pass
        """First run will not produce data.sql object in the execution logs, only data.base_msg
        containing the "Running Athena query:" initial create statement.
        Subsequent incremental runs will only contain the insert from the tmp table into the model
        table destination.
        Since we want to compare both run create statements, we need to handle both cases"""
        if base_msg_data:
            base_msg = base_msg_data.get("base_msg")
            if "Running Athena query:" in str(base_msg):
                if "create table" in base_msg:
                    sql_create_statements.append(base_msg)

            if base_msg_data.get("conn_name") == f"model.test.{relation_name}" and "sql" in base_msg_data:
                if "create table" in base_msg_data.get("sql"):
                    sql_create_statements.append(base_msg_data.get("sql"))

    return sql_create_statements


def extract_create_statement_table_names(sql_create_statement: str) -> List[str]:
    table_names = re.findall(r"(?s)(?<=create table ).*?(?=with)", sql_create_statement)
    return [table_name.rstrip() for table_name in table_names]
