import json
import re
from typing import Dict, List


def extract_running_ddl_statements(dbt_run_capsys_output: str, relation_name: str, ddl_type: str) -> List[str]:
    sql_ddl_statements = []
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
                if ddl_type in base_msg:
                    sql_ddl_statements.append(base_msg)

            if base_msg_data.get("conn_name") == f"model.test.{relation_name}" and "sql" in base_msg_data:
                if ddl_type in base_msg_data.get("sql"):
                    sql_ddl_statements.append(base_msg_data.get("sql"))

    return sql_ddl_statements


def extract_create_statement_table_names(sql_create_statement: str) -> List[str]:
    table_names = re.findall(r"(?s)(?<=create table ).*?(?=with)", sql_create_statement)
    return [table_name.rstrip() for table_name in table_names]


def extract_rename_statement_table_names(sql_alter_rename_statement: str) -> Dict[str, List[str]]:
    alter_table_names = re.findall(r"(?s)(?<=alter table ).*?(?= rename)", sql_alter_rename_statement)
    rename_to_table_names = re.findall(r"(?s)(?<=rename to ).*?(?=$)", sql_alter_rename_statement)

    return {
        "alter_table_names": [table_name.rstrip() for table_name in alter_table_names],
        "rename_to_table_names": [table_name.rstrip() for table_name in rename_to_table_names],
    }
