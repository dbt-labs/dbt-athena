from dataclasses import dataclass
from typing import ClassVar, Dict

from dbt.adapters.athena.relation import TableType
from dbt.adapters.base.column import Column
from dbt.exceptions import DbtRuntimeError


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

    @classmethod
    def string_type(cls, size: int) -> str:
        return f"varchar({size})" if size > 0 else "varchar"

    @classmethod
    def binary_type(cls) -> str:
        return "varbinary"

    def timestamp_type(self) -> str:
        return "timestamp(6)" if self.is_iceberg() else "timestamp"

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

        return self.dtype  # type: ignore
