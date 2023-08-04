import pytest

from dbt.tests.adapter.relations.test_changing_relation_type import (
    BaseChangeRelationTypeValidator,
)


class TestChangeRelationTypesHive(BaseChangeRelationTypeValidator):
    pass


class TestChangeRelationTypesIceberg(BaseChangeRelationTypeValidator):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+table_type": "iceberg",
            }
        }
