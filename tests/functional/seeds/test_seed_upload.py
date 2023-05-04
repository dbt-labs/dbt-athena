import pytest
from tests.functional.seeds.fixtures import my_seed_csv, my_seed_yaml

from dbt.tests.util import run_dbt


class TestSeedByUpload:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": my_seed_csv, "my_seed.yaml": my_seed_yaml}

    def test_seed(self, project):
        run_dbt(["build"])
