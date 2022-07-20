from dbt.adapters.base import Column as BaseColumn


class AthenaColumn(BaseColumn):
    @property
    def data_type(self) -> str:
        # NOTE: Athena has different types between DML and DDL, use DDL type in this case
        #       ref: https://docs.aws.amazon.com/athena/latest/ug/data-types.html
        if self.dtype.lower() == "integer":
            return "int"
        elif self.is_string():
            return self.string_type(self.string_size())
        elif self.is_numeric():
            return self.numeric_type(self.dtype, self.numeric_precision, self.numeric_scale)
        else:
            return self.dtype

    @classmethod
    def string_type(cls, size: int) -> str:
        return "varchar({})".format(size)
