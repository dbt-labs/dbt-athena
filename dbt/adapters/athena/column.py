from dataclasses import dataclass

from dbt.adapters.base.column import Column
from dbt.exceptions import DbtRuntimeError


@dataclass
class AthenaColumn(Column):
    is_iceberg: bool = True

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

    @classmethod
    def timestamp_type(cls, is_iceberg: bool) -> str:
        return "timestamp(6)" if is_iceberg else "timestamp"

    def string_size(self) -> int:
        if not self.is_string():
            raise DbtRuntimeError("Called string_size() on non-string field!")
        if not self.char_size:
            # Handle error '>' not supported between instances of 'NoneType' and 'NoneType' for union relations macro
            return 0
        return self.char_size

    @property
    def data_type(self) -> str:
        if self.is_string():
            return self.string_type(self.string_size())
        elif self.is_numeric():
            return self.numeric_type(self.dtype, self.numeric_precision, self.numeric_scale)
        elif self.is_binary():
            return self.binary_type()
        elif self.is_timestamp():
            return self.timestamp_type(self.is_iceberg)
        return self.dtype
