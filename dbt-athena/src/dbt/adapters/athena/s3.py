from enum import Enum


class S3DataNaming(Enum):
    UNIQUE = "unique"
    TABLE = "table"
    TABLE_UNIQUE = "table_unique"
    SCHEMA_TABLE = "schema_table"
    SCHEMA_TABLE_UNIQUE = "schema_table_unique"
