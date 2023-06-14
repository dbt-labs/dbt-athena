"""
Run optional dbt functional tests for Iceberg incremental merge, including delete and incremental predicates.

"""
import re

import pytest

from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
)
from dbt.tests.adapter.incremental.test_incremental_unique_id import (
    BaseIncrementalUniqueKey,
    models__duplicated_unary_unique_key_list_sql,
    models__empty_str_unique_key_sql,
    models__empty_unique_key_list_sql,
    models__expected__one_str__overwrite_sql,
    models__expected__unique_key_list__inplace_overwrite_sql,
    models__no_unique_key_sql,
    models__nontyped_trinary_unique_key_list_sql,
    models__not_found_unique_key_list_sql,
    models__not_found_unique_key_sql,
    models__str_unique_key_sql,
    models__trinary_unique_key_list_sql,
    models__unary_unique_key_list_sql,
    seeds__add_new_rows_sql,
    seeds__duplicate_insert_sql,
    seeds__seed_csv,
)

seeds__expected_incremental_predicates_csv = """id,msg,color
3,anyway,purple
1,hey,blue
2,goodbye,red
2,yo,green
"""

seeds__expected_delete_condition_csv = """id,msg,color
1,hey,blue
3,anyway,purple
"""

seeds__expected_predicates_and_delete_condition_csv = """id,msg,color
1,hey,blue
1,hello,blue
3,anyway,purple
"""


class TestIcebergIncrementalUniqueKey(BaseIncrementalUniqueKey):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+table_type": "iceberg", "incremental_strategy": "merge"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "trinary_unique_key_list.sql": models__trinary_unique_key_list_sql,
            "nontyped_trinary_unique_key_list.sql": models__nontyped_trinary_unique_key_list_sql,
            "unary_unique_key_list.sql": models__unary_unique_key_list_sql,
            "not_found_unique_key.sql": models__not_found_unique_key_sql,
            "empty_unique_key_list.sql": models__empty_unique_key_list_sql,
            "no_unique_key.sql": models__no_unique_key_sql,
            "empty_str_unique_key.sql": models__empty_str_unique_key_sql,
            "str_unique_key.sql": models__str_unique_key_sql,
            "duplicated_unary_unique_key_list.sql": models__duplicated_unary_unique_key_list_sql,
            "not_found_unique_key_list.sql": models__not_found_unique_key_list_sql,
            "expected": {
                "one_str__overwrite.sql": replace_cast_date(models__expected__one_str__overwrite_sql),
                "unique_key_list__inplace_overwrite.sql": replace_cast_date(
                    models__expected__unique_key_list__inplace_overwrite_sql
                ),
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "duplicate_insert.sql": replace_cast_date(seeds__duplicate_insert_sql),
            "seed.csv": seeds__seed_csv,
            "add_new_rows.sql": replace_cast_date(seeds__add_new_rows_sql),
        }

    @pytest.mark.xfail(reason="Model config 'unique_keys' is required for incremental merge.")
    def test__no_unique_keys(self, project):
        super().test__no_unique_keys(project)

    @pytest.mark.skip(
        reason=""""
            If 'unique_keys' does not contain columns then the join condition will fail.
            The adapter isn't handling this input scenario.
        """
    )
    def test__empty_str_unique_key(self):
        pass

    @pytest.mark.skip(
        reason="""
            If 'unique_keys' does not contain columns then the join condition will fail.
            The adapter isn't handling this input scenario.
        """
    )
    def test__empty_unique_key_list(self):
        pass


class TestIcebergIncrementalPredicates(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "merge",
                "+table_type": "iceberg",
                "+incremental_predicates": ["src.id <> 3", "target.id <> 2"],
            }
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected_delete_insert_incremental_predicates.csv": seeds__expected_incremental_predicates_csv}


class TestIcebergDeleteCondition(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "merge",
                "+table_type": "iceberg",
                "+delete_condition": "src.id = 2 and target.color = 'red'",
            }
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected_delete_insert_incremental_predicates.csv": seeds__expected_delete_condition_csv}

    # Modifying the seed_rows number from the base class method
    def test__incremental_predicates(self, project):
        """seed should match model after two incremental runs"""

        expected_fields = self.get_expected_fields(
            relation="expected_delete_insert_incremental_predicates", seed_rows=2
        )
        test_case_fields = self.get_test_fields(
            project,
            seed="expected_delete_insert_incremental_predicates",
            incremental_model="delete_insert_incremental_predicates",
            update_sql_file=None,
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)


class TestIcebergPredicatesAndDeleteCondition(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "merge",
                "+table_type": "iceberg",
                "+delete_condition": "src.msg = 'yo' and target.color = 'red'",
                "+incremental_predicates": ["src.id <> 1", "target.msg <> 'blue'"],
            }
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_delete_insert_incremental_predicates.csv": seeds__expected_predicates_and_delete_condition_csv
        }

    # Modifying the seed_rows number from the base class method
    def test__incremental_predicates(self, project):
        """seed should match model after two incremental runs"""

        expected_fields = self.get_expected_fields(
            relation="expected_delete_insert_incremental_predicates", seed_rows=3
        )
        test_case_fields = self.get_test_fields(
            project,
            seed="expected_delete_insert_incremental_predicates",
            incremental_model="delete_insert_incremental_predicates",
            update_sql_file=None,
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)


def replace_cast_date(model: str) -> str:
    """Wrap all date strings with a cast date function"""

    new_model = re.sub("'[0-9]{4}-[0-9]{2}-[0-9]{2}'", r"cast(\g<0> as date)", model)
    return new_model
