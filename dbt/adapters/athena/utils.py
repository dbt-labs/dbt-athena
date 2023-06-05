from typing import Optional, TypeVar, List, Collection

from mypy_boto3_athena.type_defs import DataCatalogTypeDef


def clean_sql_comment(comment: str) -> str:
    split_and_strip = [line.strip() for line in comment.split("\n")]
    return " ".join(line for line in split_and_strip if line)


def get_catalog_id(catalog: Optional[DataCatalogTypeDef]) -> Optional[str]:
    if catalog:
        return catalog["Parameters"]["catalog-id"]


T = TypeVar('T')


def get_chunks(lst: Collection[T], n: int) -> Collection[List[T]]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i: i + n]
