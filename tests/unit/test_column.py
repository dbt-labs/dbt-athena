import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.athena.column import AthenaColumn
from dbt.adapters.athena.relation import TableType


class TestAthenaColumn:
    def setup_column(self, **kwargs):
        base_kwargs = {"column": "foo", "dtype": "varchar"}
        return AthenaColumn(**{**base_kwargs, **kwargs})

    @pytest.mark.parametrize(
        "table_type,expected",
        [
            pytest.param(TableType.TABLE, False),
            pytest.param(TableType.ICEBERG, True),
        ],
    )
    def test_is_iceberg(self, table_type, expected):
        column = self.setup_column(table_type=table_type)
        assert column.is_iceberg() is expected

    @pytest.mark.parametrize(
        "dtype,expected_type_func",
        [
            pytest.param("varchar", "is_string"),
            pytest.param("string", "is_string"),
            pytest.param("binary", "is_binary"),
            pytest.param("varbinary", "is_binary"),
            pytest.param("timestamp", "is_timestamp"),
            pytest.param("array<string>", "is_array"),
            pytest.param("array(string)", "is_array"),
        ],
    )
    def test_is_type(self, dtype, expected_type_func):
        column = self.setup_column(dtype=dtype)
        for type_func in ["is_string", "is_binary", "is_timestamp", "is_array"]:
            if type_func == expected_type_func:
                assert getattr(column, type_func)()
            else:
                assert not getattr(column, type_func)()

    @pytest.mark.parametrize("size,expected", [pytest.param(1, "varchar(1)"), pytest.param(0, "varchar")])
    def test_string_type(self, size, expected):
        assert AthenaColumn.string_type(size) == expected

    @pytest.mark.parametrize(
        "table_type,expected",
        [pytest.param(TableType.TABLE, "timestamp"), pytest.param(TableType.ICEBERG, "timestamp(6)")],
    )
    def test_timestamp_type(self, table_type, expected):
        column = self.setup_column(table_type=table_type)
        assert column.timestamp_type() == expected

    def test_array_type(self):
        assert AthenaColumn.array_type("varchar") == "array(varchar)"

    @pytest.mark.parametrize(
        "dtype,expected",
        [
            pytest.param("array<string>", "string"),
            pytest.param("array<varchar(10)>", "varchar(10)"),
            pytest.param("array<array<int>>", "array<int>"),
            pytest.param("array", "array"),
        ],
    )
    def test_array_inner_type(self, dtype, expected):
        column = self.setup_column(dtype=dtype)
        assert column.array_inner_type() == expected

    def test_array_inner_type_raises_for_non_array_type(self):
        column = self.setup_column(dtype="varchar")
        with pytest.raises(DbtRuntimeError, match=r"Called array_inner_type\(\) on non-array field!"):
            column.array_inner_type()

    @pytest.mark.parametrize(
        "char_size,expected",
        [
            pytest.param(10, 10),
            pytest.param(None, 0),
        ],
    )
    def test_string_size(self, char_size, expected):
        column = self.setup_column(dtype="varchar", char_size=char_size)
        assert column.string_size() == expected

    def test_string_size_raises_for_non_string_type(self):
        column = self.setup_column(dtype="int")
        with pytest.raises(DbtRuntimeError, match=r"Called string_size\(\) on non-string field!"):
            column.string_size()

    @pytest.mark.parametrize(
        "dtype,expected",
        [
            pytest.param("string", "varchar(10)"),
            pytest.param("decimal", "decimal(1,2)"),
            pytest.param("binary", "varbinary"),
            pytest.param("timestamp", "timestamp(6)"),
            pytest.param("array<string>", "array(varchar(10))"),
            pytest.param("array<array<string>>", "array(array(varchar(10)))"),
        ],
    )
    def test_data_type(self, dtype, expected):
        column = self.setup_column(
            table_type=TableType.ICEBERG, dtype=dtype, char_size=10, numeric_precision=1, numeric_scale=2
        )
        assert column.data_type == expected
