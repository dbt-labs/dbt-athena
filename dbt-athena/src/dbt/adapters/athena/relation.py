from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Set

from mypy_boto3_glue.type_defs import TableTypeDef

from dbt.adapters.athena.constants import LOGGER
from dbt.adapters.base.relation import BaseRelation, InformationSchema, Policy


class TableType(Enum):
    TABLE = "table"
    VIEW = "view"
    CTE = "cte"
    MATERIALIZED_VIEW = "materializedview"
    ICEBERG = "iceberg_table"

    def is_physical(self) -> bool:
        return self in [TableType.TABLE, TableType.ICEBERG]


@dataclass
class AthenaIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class AthenaHiveIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class AthenaRelation(BaseRelation):
    quote_character: str = '"'  # Presto quote character
    include_policy: Policy = field(default_factory=lambda: AthenaIncludePolicy())
    s3_path_table_part: Optional[str] = None
    detailed_table_type: Optional[str] = None  # table_type option from the table Parameters in Glue Catalog
    require_alias: bool = False

    def render_hive(self) -> str:
        """
        Render relation with Hive format. Athena uses a Hive format for some DDL statements.

        See:
        - https://aws.amazon.com/athena/faqs/ "Q: How do I create tables and schemas for my data on Amazon S3?"
        - https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
        """

        old_quote_character = self.quote_character
        object.__setattr__(self, "quote_character", "`")  # Hive quote char
        old_include_policy = self.include_policy
        object.__setattr__(self, "include_policy", AthenaHiveIncludePolicy())
        rendered = self.render()
        object.__setattr__(self, "quote_character", old_quote_character)
        object.__setattr__(self, "include_policy", old_include_policy)
        return str(rendered)

    def render_pure(self) -> str:
        """
        Render relation without quotes characters.
        This is needed for not standard executions like optimize and vacuum
        """
        old_value = self.quote_character
        object.__setattr__(self, "quote_character", "")
        rendered = self.render()
        object.__setattr__(self, "quote_character", old_value)
        return str(rendered)


class AthenaSchemaSearchMap(Dict[InformationSchema, Dict[str, Set[Optional[str]]]]):
    """A utility class to keep track of what information_schema tables to
    search for what schemas and relations. The schema and relation values are all
    lowercase to avoid duplication.
    """

    def add(self, relation: AthenaRelation) -> None:
        key = relation.information_schema_only()
        if key not in self:
            self[key] = {}
        if relation.schema is not None:
            schema = relation.schema.lower()
            relation_name = relation.name.lower()
            if schema not in self[key]:
                self[key][schema] = set()
            self[key][schema].add(relation_name)


RELATION_TYPE_MAP = {
    "EXTERNAL_TABLE": TableType.TABLE,
    "EXTERNAL": TableType.TABLE,  # type returned by federated query tables
    "GOVERNED": TableType.TABLE,
    "MANAGED_TABLE": TableType.TABLE,
    "VIRTUAL_VIEW": TableType.VIEW,
    "table": TableType.TABLE,
    "view": TableType.VIEW,
    "cte": TableType.CTE,
    "materializedview": TableType.MATERIALIZED_VIEW,
}


def get_table_type(table: TableTypeDef) -> TableType:
    table_full_name = ".".join(filter(None, [table.get("CatalogId"), table.get("DatabaseName"), table["Name"]]))

    input_table_type = table.get("TableType")
    if input_table_type and input_table_type not in RELATION_TYPE_MAP:
        raise ValueError(f"Table type {table['TableType']} is not supported for table {table_full_name}")

    if table.get("Parameters", {}).get("table_type", "").lower() == "iceberg":
        _type = TableType.ICEBERG
    elif not input_table_type:
        raise ValueError(f"Table type cannot be None for table {table_full_name}")
    else:
        _type = RELATION_TYPE_MAP[input_table_type]

    LOGGER.debug(f"table_name : {table_full_name}")
    LOGGER.debug(f"table type : {_type}")

    return _type
