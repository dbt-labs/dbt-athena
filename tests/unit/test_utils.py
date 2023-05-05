import pytest

from dbt.adapters.athena.relation import TableType
from dbt.adapters.athena.utils import clean_sql_comment, get_table_type


def test_clean_comment():
    assert (
        clean_sql_comment(
            """
       my long comment
         on several lines
        with weird spaces and indents.
    """
        )
        == "my long comment on several lines with weird spaces and indents."
    )


def test__get_relation_type_table():
    assert get_table_type({"Name": "name", "TableType": "table"}) == TableType.TABLE


def test__get_relation_type_with_no_type():
    with pytest.raises(ValueError):
        get_table_type({"Name": "name"})


def test__get_relation_type_view():
    assert get_table_type({"Name": "name", "TableType": "VIRTUAL_VIEW"}) == TableType.VIEW


def test__get_relation_type_iceberg():
    assert (
        get_table_type({"Name": "name", "TableType": "EXTERNAL_TABLE", "Parameters": {"table_type": "ICEBERG"}})
        == TableType.ICEBERG
    )
