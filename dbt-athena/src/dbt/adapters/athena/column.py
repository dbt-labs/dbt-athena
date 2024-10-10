import re
from dataclasses import dataclass
from typing import ClassVar, Dict

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.athena.relation import TableType
from dbt.adapters.base.column import Column


@dataclass
class AthenaColumn(Column):
    table_type: TableType = TableType.TABLE

    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        "STRING": "VARCHAR",
        "TEXT": "VARCHAR",
    }

    def is_iceberg(self) -> bool:
        return self.table_type == TableType.ICEBERG

    def is_string(self) -> bool:
        return self.dtype.lower() in {"varchar", "string"}

    def is_binary(self) -> bool:
        return self.dtype.lower() in {"binary", "varbinary"}

    def is_timestamp(self) -> bool:
        return self.dtype.lower() in {"timestamp"}

    def is_array(self) -> bool:
        return self.dtype.lower().startswith("array")  # type: ignore

    @classmethod
    def string_type(cls, size: int) -> str:
        return f"varchar({size})" if size > 0 else "varchar"

    @classmethod
    def binary_type(cls) -> str:
        return "varbinary"

    def timestamp_type(self) -> str:
        return "timestamp(6)" if self.is_iceberg() else "timestamp"

    @classmethod
    def array_type(cls, inner_type: str) -> str:
        return f"array({inner_type})"

    def array_inner_type(self) -> str:
        if not self.is_array():
            raise DbtRuntimeError("Called array_inner_type() on non-array field!")
        # Match either `array<inner_type>` or `array(inner_type)`. Don't bother
        # parsing nested arrays here, since we will expect the caller to be
        # responsible for formatting the inner type, including nested arrays
        pattern = r"^array[<(](.*)[>)]$"
        match = re.match(pattern, self.dtype)
        if match:
            return match.group(1)
        # If for some reason there's no match, fall back to the original string
        return self.dtype  # type: ignore

    def string_size(self) -> int:
        if not self.is_string():
            raise DbtRuntimeError("Called string_size() on non-string field!")
        # Handle error: '>' not supported between instances of 'NoneType' and 'NoneType' for union relations macro
        return self.char_size or 0

    @property
    def data_type(self) -> str:
        if self.is_string():
            return self.string_type(self.string_size())

        if self.is_numeric():
            return self.numeric_type(self.dtype, self.numeric_precision, self.numeric_scale)  # type: ignore

        if self.is_binary():
            return self.binary_type()

        if self.is_timestamp():
            return self.timestamp_type()

        if self.is_array():
            # Resolve the inner type of the array, using an AthenaColumn
            # instance to properly convert the inner type. Note that this will
            # cause recursion in cases of nested arrays
            inner_type = self.array_inner_type()
            inner_type_col = AthenaColumn(
                column=self.column,
                dtype=inner_type,
                char_size=self.char_size,
                numeric_precision=self.numeric_precision,
                numeric_scale=self.numeric_scale,
            )
            return self.array_type(inner_type_col.data_type)

        return self.dtype  # type: ignore
