import os

import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.adapter.basic.expected_catalog import base_expected_catalog, no_stats
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    run_and_generate,
    verify_metadata,
)
from dbt.tests.util import get_artifact, run_dbt

model_sql = """
select 1 as id
"""

iceberg_model_sql = """
{{
    config(
        materialized="table",
        table_type="iceberg",
        post_hook="alter table model drop column to_drop"
    )
}}
select 1 as id, 'to_drop' as to_drop
"""

override_macros_sql = """
{% macro get_catalog_relations(information_schema, relations) %}
    {{ return(adapter.get_catalog_by_relations(information_schema, relations)) }}
{% endmacro %}
"""


def custom_verify_catalog_athena(project, expected_catalog, start_time):
    # get the catalog.json
    catalog_path = os.path.join(project.project_root, "target", "catalog.json")
    assert os.path.exists(catalog_path)
    catalog = get_artifact(catalog_path)

    # verify the catalog
    assert set(catalog) == {"errors", "nodes", "sources", "metadata"}
    verify_metadata(
        catalog["metadata"],
        "https://schemas.getdbt.com/dbt/catalog/v1.json",
        start_time,
    )
    assert not catalog["errors"]

    for key in "nodes", "sources":
        for unique_id, expected_node in expected_catalog[key].items():
            found_node = catalog[key][unique_id]
            for node_key in expected_node:
                assert node_key in found_node
                # the value of found_node[node_key] is not exactly expected_node[node_key]


class TestDocsGenerate(BaseDocsGenerate):
    """
    Override of BaseDocsGenerate to make it working with Athena
    """

    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return base_expected_catalog(
            project,
            role="test",
            id_type="integer",
            text_type="text",
            time_type="timestamp without time zone",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=no_stats(),
        )

    def test_run_and_generate_no_compile(self, project, expected_catalog):
        start_time = run_and_generate(project, ["--no-compile"])
        assert not os.path.exists(os.path.join(project.project_root, "target", "manifest.json"))
        custom_verify_catalog_athena(project, expected_catalog, start_time)

    # Test generic "docs generate" command
    def test_run_and_generate(self, project, expected_catalog):
        start_time = run_and_generate(project)
        custom_verify_catalog_athena(project, expected_catalog, start_time)

        # Check that assets have been copied to the target directory for use in the docs html page
        assert os.path.exists(os.path.join(".", "target", "assets"))
        assert os.path.exists(os.path.join(".", "target", "assets", "lorem-ipsum.txt"))
        assert not os.path.exists(os.path.join(".", "target", "non-existent-assets"))


class TestDocsGenerateOverride:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": model_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"override_macros_sql.sql": override_macros_sql}

    def test_generate_docs(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        docs_generate = run_dbt(["--warn-error", "docs", "generate"])
        assert len(docs_generate._compile_results.results) == 1
        assert docs_generate._compile_results.results[0].status == RunStatus.Success
        assert docs_generate.errors is None


class TestDocsGenerateIcebergNonCurrentColumn:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": iceberg_model_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"override_macros_sql.sql": override_macros_sql}

    def test_generate_docs(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        docs_generate = run_dbt(["--warn-error", "docs", "generate"])
        assert len(docs_generate._compile_results.results) == 1
        assert docs_generate._compile_results.results[0].status == RunStatus.Success
        assert docs_generate.errors is None

        catalog_path = os.path.join(project.project_root, "target", "catalog.json")
        assert os.path.exists(catalog_path)
        catalog = get_artifact(catalog_path)
        columns = catalog["nodes"]["model.test.model"]["columns"]
        assert "to_drop" not in columns
        assert "id" in columns
