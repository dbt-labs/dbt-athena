import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt

model_sql = """
select 1 as id
"""


class TestDocsGenerate:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": model_sql}

    def test_generate_docs(
        self,
        project,
    ):
        results = run_dbt(["run"])
        assert len(results) == 1

        docs_generate = run_dbt(["--warn-error", "docs", "generate"])
        assert len(docs_generate._compile_results.results) == 1
        assert docs_generate._compile_results.results[0].status == RunStatus.Success
        assert docs_generate.errors is None
