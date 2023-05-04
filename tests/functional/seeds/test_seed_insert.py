import pytest
from tests.functional.seeds.fixtures import my_seed_csv, my_seed_yaml

from dbt.tests.util import run_dbt


class TestSeedByInsert:
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        return {
            "test": {"outputs": {"default": {**dbt_profile_target, **{"seed_by_insert": True}}}},
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": my_seed_csv, "my_seed.yaml": my_seed_yaml}

    def test_seed(self, project):
        # not sure how best to check that this actually ended up being an insert vs. an upload...?
        run_dbt(["build"])
