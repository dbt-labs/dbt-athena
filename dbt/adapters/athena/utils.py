import json
from enum import Enum
from typing import Any, Generator, List, Optional, TypeVar

from mypy_boto3_athena.type_defs import DataCatalogTypeDef


def clean_sql_comment(comment: str) -> str:
    split_and_strip = [line.strip() for line in comment.split("\n")]
    return " ".join(line for line in split_and_strip if line)


def stringify_for_table_property(value: Any) -> str:
    """Convert any variable to string for Glue Table property."""
    if isinstance(value, (str, bool, int, float)):
        # Convert simple formats to strings
        value_str: str = str(value)
    else:
        # Convert complex format to json string
        value_str = json.dumps(value)
    return value_str


def get_catalog_id(catalog: Optional[DataCatalogTypeDef]) -> Optional[str]:
    return catalog["Parameters"]["catalog-id"] if catalog and catalog["Type"] == AthenaCatalogType.GLUE.value else None


class AthenaCatalogType(Enum):
    GLUE = "GLUE"
    LAMBDA = "LAMBDA"
    HIVE = "HIVE"


def get_catalog_type(catalog: Optional[DataCatalogTypeDef]) -> Optional[AthenaCatalogType]:
    return AthenaCatalogType(catalog["Type"]) if catalog else None


T = TypeVar("T")


def get_chunks(lst: List[T], n: int) -> Generator[List[T], None, None]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
