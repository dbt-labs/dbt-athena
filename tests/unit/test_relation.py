from dbt.adapters.athena.relation import AthenaRelation

from .constants import DATA_CATALOG_NAME, DATABASE_NAME

TABLE_NAME = "test_table"


class TestAthenaRelation:
    def test_render_hive_uses_hive_style_quotation(self):
        relation = AthenaRelation.create(
            identifier=TABLE_NAME,
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
        )
        assert relation.render_hive() == f"`{DATABASE_NAME}`.`{TABLE_NAME}`"

    def test_render_hive_resets_quote_character_after_call(self):
        relation = AthenaRelation.create(
            identifier=TABLE_NAME,
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
        )
        relation.render_hive()
        assert relation.render() == f'"{DATABASE_NAME}"."{TABLE_NAME}"'

    def test_render_pure_resets_quote_character_after_call(self):
        relation = AthenaRelation.create(
            identifier=TABLE_NAME,
            database=DATA_CATALOG_NAME,
            schema=DATABASE_NAME,
        )
        assert relation.render_pure() == f"{DATABASE_NAME}.{TABLE_NAME}"
