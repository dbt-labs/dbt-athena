import boto3
import pytest
from tests.unit.constants import AWS_REGION, DATA_CATALOG_NAME, DATABASE_NAME

import dbt.adapters.athena.lakeformation as lakeformation
from dbt.adapters.athena.lakeformation import LfTagsConfig, LfTagsManager
from dbt.adapters.athena.relation import AthenaRelation


# TODO: add more tests for lakeformation once moto library implements required methods:
# https://docs.getmoto.org/en/4.1.9/docs/services/lakeformation.html
# get_resource_lf_tags
class TestLfTagsManager:
    @pytest.mark.parametrize(
        "response,identifier,columns,lf_tags,verb,expected",
        [
            pytest.param(
                {
                    "Failures": [
                        {
                            "LFTag": {"CatalogId": "test_catalog", "TagKey": "test_key", "TagValues": ["test_values"]},
                            "Error": {"ErrorCode": "test_code", "ErrorMessage": "test_err_msg"},
                        }
                    ]
                },
                "tbl_name",
                ["column1", "column2"],
                {"tag_key": "tag_value"},
                "add",
                None,
                id="lf_tag error",
                marks=pytest.mark.xfail,
            ),
            pytest.param(
                {"Failures": []},
                "tbl_name",
                None,
                {"tag_key": "tag_value"},
                "add",
                "Success: add LF tags {'tag_key': 'tag_value'} to test_dbt_athena.tbl_name",
                id="add lf_tag",
            ),
            pytest.param(
                {"Failures": []},
                None,
                None,
                {"tag_key": "tag_value"},
                "add",
                "Success: add LF tags {'tag_key': 'tag_value'} to test_dbt_athena",
                id="add lf_tag_to_database",
            ),
            pytest.param(
                {"Failures": []},
                "tbl_name",
                None,
                {"tag_key": "tag_value"},
                "remove",
                "Success: remove LF tags {'tag_key': 'tag_value'} to test_dbt_athena.tbl_name",
                id="remove lf_tag",
            ),
            pytest.param(
                {"Failures": []},
                "tbl_name",
                ["c1", "c2"],
                {"tag_key": "tag_value"},
                "add",
                "Success: add LF tags {'tag_key': 'tag_value'} to test_dbt_athena.tbl_name for columns ['c1', 'c2']",
                id="lf_tag database table and columns",
            ),
        ],
    )
    def test__parse_lf_response(self, dbt_debug_caplog, response, identifier, columns, lf_tags, verb, expected):
        relation = AthenaRelation.create(database=DATA_CATALOG_NAME, schema=DATABASE_NAME, identifier=identifier)
        lf_client = boto3.client("lakeformation", region_name=AWS_REGION)
        manager = LfTagsManager(lf_client, relation, LfTagsConfig())
        manager._parse_and_log_lf_response(response, columns, lf_tags, verb)
        assert expected in dbt_debug_caplog.getvalue()

    @pytest.mark.parametrize(
        "lf_tags_columns,lf_inherited_tags,expected",
        [
            pytest.param(
                [{"Name": "my_column", "LFTags": [{"TagKey": "inherited", "TagValues": ["oh-yes-i-am"]}]}],
                {"inherited"},
                {},
                id="retains-inherited-tag",
            ),
            pytest.param(
                [{"Name": "my_column", "LFTags": [{"TagKey": "not-inherited", "TagValues": ["oh-no-im-not"]}]}],
                {},
                {"not-inherited": {"oh-no-im-not": ["my_column"]}},
                id="removes-non-inherited-tag",
            ),
            pytest.param(
                [
                    {
                        "Name": "my_column",
                        "LFTags": [
                            {"TagKey": "not-inherited", "TagValues": ["oh-no-im-not"]},
                            {"TagKey": "inherited", "TagValues": ["oh-yes-i-am"]},
                        ],
                    }
                ],
                {"inherited"},
                {"not-inherited": {"oh-no-im-not": ["my_column"]}},
                id="removes-non-inherited-tag-among-inherited",
            ),
            pytest.param([], {}, {}, id="handles-empty"),
        ],
    )
    def test__column_tags_to_remove(self, lf_tags_columns, lf_inherited_tags, expected):
        assert lakeformation.LfTagsManager._column_tags_to_remove(lf_tags_columns, lf_inherited_tags) == expected

    @pytest.mark.parametrize(
        "lf_tags_table,lf_tags,lf_inherited_tags,expected",
        [
            pytest.param(
                [
                    {"TagKey": "not-inherited", "TagValues": ["oh-no-im-not"]},
                    {"TagKey": "inherited", "TagValues": ["oh-yes-i-am"]},
                ],
                {"not-inherited": "some-preexisting-value"},
                {"inherited"},
                {},
                id="retains-being-set-and-inherited",
            ),
            pytest.param(
                [
                    {"TagKey": "not-inherited", "TagValues": ["oh-no-im-not"]},
                    {"TagKey": "inherited", "TagValues": ["oh-yes-i-am"]},
                ],
                {},
                {"inherited"},
                {"not-inherited": ["oh-no-im-not"]},
                id="removes-preexisting-not-being-set",
            ),
            pytest.param(
                [{"TagKey": "inherited", "TagValues": ["oh-yes-i-am"]}], {}, {"inherited"}, {}, id="retains-inherited"
            ),
            pytest.param([], None, {}, {}, id="handles-empty"),
        ],
    )
    def test__table_tags_to_remove(self, lf_tags_table, lf_tags, lf_inherited_tags, expected):
        assert lakeformation.LfTagsManager._table_tags_to_remove(lf_tags_table, lf_tags, lf_inherited_tags) == expected
