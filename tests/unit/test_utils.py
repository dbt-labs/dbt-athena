from dbt.adapters.athena.utils import clean_sql_comment, get_chunks


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
