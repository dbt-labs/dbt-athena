from dbt.adapters.athena.relation import TableType
from dbt.events import AdapterLogger

RELATION_TYPE_MAP = {
    "EXTERNAL_TABLE": TableType.TABLE,
    "MANAGED_TABLE": TableType.TABLE,
    "VIRTUAL_VIEW": TableType.VIEW,
    "table": TableType.TABLE,
    "view": TableType.VIEW,
    "cte": TableType.CTE,
    "materializedview": TableType.MATERIALIZED_VIEW,
}

LOGGER = AdapterLogger("Athena")
