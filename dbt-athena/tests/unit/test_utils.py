import pytest

from dbt.adapters.athena.utils import (
    clean_sql_comment,
    ellipsis_comment,
    get_chunks,
    is_valid_table_parameter_key,
    stringify_table_parameter_value,
)


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


def test_stringify_table_parameter_value():
    class NonStringifiableObject:
        def __str__(self):
            raise ValueError("Non-stringifiable object")

    assert stringify_table_parameter_value(True) == "True"
    assert stringify_table_parameter_value(123) == "123"
    assert stringify_table_parameter_value("dbt-athena") == "dbt-athena"
    assert stringify_table_parameter_value(["a", "b", 3]) == '["a", "b", 3]'
    assert stringify_table_parameter_value({"a": 1, "b": "c"}) == '{"a": 1, "b": "c"}'
    assert len(stringify_table_parameter_value("a" * 512001)) == 512000
    assert stringify_table_parameter_value(NonStringifiableObject()) is None
    assert stringify_table_parameter_value([NonStringifiableObject()]) is None


def test_is_valid_table_parameter_key():
    assert is_valid_table_parameter_key("valid_key") is True
    assert is_valid_table_parameter_key("Valid Key 123*!") is True
    assert is_valid_table_parameter_key("invalid \n key") is False
    assert is_valid_table_parameter_key("long_key" * 100) is False


def test_get_chunks_empty():
    assert len(list(get_chunks([], 5))) == 0


def test_get_chunks_uneven():
    chunks = list(get_chunks([1, 2, 3], 2))
    assert chunks[0] == [1, 2]
    assert chunks[1] == [3]
    assert len(chunks) == 2


def test_get_chunks_more_elements_than_chunk():
    chunks = list(get_chunks([1, 2, 3], 4))
    assert chunks[0] == [1, 2, 3]
    assert len(chunks) == 1


@pytest.mark.parametrize(
    ("max_len", "expected"),
    (
        pytest.param(12, "abc def ghi", id="ok string"),
        pytest.param(6, "abc...", id="ellipsis"),
    ),
)
def test_ellipsis_comment(max_len, expected):
    assert expected == ellipsis_comment("abc def ghi", max_len=max_len)
