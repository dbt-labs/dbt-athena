import json
import re
from enum import Enum
from typing import Any, Generator, List, Optional, TypeVar

from mypy_boto3_athena.type_defs import DataCatalogTypeDef


def clean_sql_comment(comment: str) -> str:
    split_and_strip = [line.strip() for line in comment.split("\n")]
    return " ".join(line for line in split_and_strip if line)


def stringify_table_parameter_value(value: Any) -> str:
    """Convert any variable to string for Glue Table property."""
    try:
        if isinstance(value, (dict, list)):
            value_str: str = json.dumps(value)
        else:
            value_str = str(value)
    except (TypeError, ValueError) as e:
        # Handle non-stringifiable objects and non-serializable objects
        value_str = f"Non-stringifiable object. Error: {str(e)}"
    return value_str[:512000]


def is_valid_table_parameter_key(key: str) -> bool:
    """Check if key is valid for Glue Table property according to official documentation."""
    # Simplified version of key pattern which works with re
    # Original pattern can be found here https://docs.aws.amazon.com/glue/latest/webapi/API_Table.html
    key_pattern: str = r"^[\u0020-\uD7FF\uE000-\uFFFD\t]*$"
    return len(key) <= 255 and bool(re.match(key_pattern, key))


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
