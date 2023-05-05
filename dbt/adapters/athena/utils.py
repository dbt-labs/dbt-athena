from typing import Optional

from mypy_boto3_athena.type_defs import DataCatalogTypeDef
from mypy_boto3_glue.type_defs import TableTypeDef

from dbt.adapters.athena.constants import LOGGER, RELATION_TYPE_MAP
from dbt.adapters.athena.relation import TableType


def clean_sql_comment(comment: str) -> str:
    split_and_strip = [line.strip() for line in comment.split("\n")]
    return " ".join(line for line in split_and_strip if line)


def get_catalog_id(catalog: Optional[DataCatalogTypeDef]) -> Optional[str]:
    if catalog:
        return catalog["Parameters"]["catalog-id"]


def get_table_type(table: TableTypeDef) -> Optional[TableType]:
    _type = RELATION_TYPE_MAP.get(table.get("TableType"))
    _specific_type = table.get("Parameters", {}).get("table_type", "")

    if _specific_type.lower() == "iceberg":
        _type = TableType.ICEBERG

    if _type is None:
        raise ValueError("Table type cannot be None")

    LOGGER.debug(f"table_name : {table.get('Name')}")
    LOGGER.debug(f"table type : {_type}")

    return _type
