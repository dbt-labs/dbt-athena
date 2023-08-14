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

    def test_changing_materialization_changes_relation_type(self, project):
        self._run_and_check_materialization("view")
        self._run_and_check_materialization("table")
        self._run_and_check_materialization("view")
        # skip incremntal that doesn't work with Iceberg
        self._run_and_check_materialization("table", extra_args=["--full-refresh"])
