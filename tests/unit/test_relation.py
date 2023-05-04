from dbt.adapters.athena.relation import AthenaRelation, TableType
from dbt.contracts.relation import RelationType

from .constants import DATA_CATALOG_NAME, DATABASE_NAME

TABLE_NAME = "test_table"


class TestAthenaRelation:
    def test_render_hive_uses_hive_style_quotation(self):
        relation = AthenaRelation.create(
            identifier=TABLE_NAME,
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
        )
        assert relation.render_hive() == f"`{DATA_CATALOG_NAME}`.`{DATABASE_NAME}`.`{TABLE_NAME}`"

    def test_render_hive_resets_quote_character_after_call(self):
        relation = AthenaRelation.create(
            identifier=TABLE_NAME,
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
        )
        relation.render_hive()
        assert relation.render() == f'"{DATA_CATALOG_NAME}"."{DATABASE_NAME}"."{TABLE_NAME}"'

    def test_render_pure_resets_quote_character_after_call(self):
        relation = AthenaRelation.create(
            identifier=TABLE_NAME,
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
        )
        assert relation.render_pure() == f"{DATA_CATALOG_NAME}.{DATABASE_NAME}.{TABLE_NAME}"

    def test_table_type_with_none(self):
        relation = AthenaRelation.create(
            identifier=TABLE_NAME,
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
        )
        assert relation.table_type == TableType.TABLE

    def test_table_type_with_inferred_from_type(self):
        relation = AthenaRelation.create(
            identifier=TABLE_NAME, database=DATA_CATALOG_NAME, schema=DATABASE_NAME, type=RelationType.View
        )
        assert relation.table_type == TableType.VIEW

    def test_table_type_with_table_type(self):
        relation = AthenaRelation.create(
            identifier=TABLE_NAME, database=DATA_CATALOG_NAME, schema=DATABASE_NAME, _table_type=TableType.ICEBERG
        )
        assert relation.table_type == TableType.ICEBERG
