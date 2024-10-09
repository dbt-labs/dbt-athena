import pytest
from tests.functional.adapter.fixture_datediff import (
    models__test_datediff_sql,
    seeds__data_datediff_csv,
)
from tests.functional.adapter.fixture_split_parts import (
    models__test_split_part_sql,
    models__test_split_part_yml,
)

from dbt.tests.adapter.utils.fixture_datediff import models__test_datediff_yml
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_array_append import BaseArrayAppend
from dbt.tests.adapter.utils.test_array_concat import BaseArrayConcat
from dbt.tests.adapter.utils.test_array_construct import BaseArrayConstruct
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestampNaive
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_escape_single_quotes import (
    BaseEscapeSingleQuotesQuote,
)
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_listagg import BaseListagg
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral

models__array_concat_expected_sql = """
select 1 as id, {{ array_construct([1,2,3,4,5,6]) }} as array_col
"""


models__array_concat_actual_sql = """
select 1 as id, {{ array_concat(array_construct([1,2,3]), array_construct([4,5,6])) }} as array_col
"""


class TestAnyValue(BaseAnyValue):
    pass


class TestArrayAppend(BaseArrayAppend):
    pass


class TestArrayConstruct(BaseArrayConstruct):
    pass


# Altered test because can't merge empty and non-empty arrays.
class TestArrayConcat(BaseArrayConcat):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": models__array_concat_actual_sql,
            "expected.sql": models__array_concat_expected_sql,
        }


class TestBoolOr(BaseBoolOr):
    pass


class TestConcat(BaseConcat):
    pass


class TestEscapeSingleQuotes(BaseEscapeSingleQuotesQuote):
    pass


class TestExcept(BaseExcept):
    pass


class TestHash(BaseHash):
    pass


class TestIntersect(BaseIntersect):
    pass


class TestLength(BaseLength):
    pass


class TestPosition(BasePosition):
    pass


class TestReplace(BaseReplace):
    pass


class TestRight(BaseRight):
    pass


class TestSplitPart(BaseSplitPart):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_split_part.yml": models__test_split_part_yml,
            "test_split_part.sql": self.interpolate_macro_namespace(models__test_split_part_sql, "split_part"),
        }


class TestStringLiteral(BaseStringLiteral):
    pass


class TestDateTrunc(BaseDateTrunc):
    pass


class TestDateAdd(BaseDateAdd):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_date_add",
            "seeds": {
                "test": {
                    "date_add": {
                        "data_dateadd": {
                            "+column_types": {
                                "from_time": "timestamp",
                                "result": "timestamp",
                            },
                        }
                    }
                },
            },
        }


class TestDateDiff(BaseDateDiff):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_date_diff",
            "seeds": {
                "test": {
                    "date_diff": {
                        "data_datediff": {
                            "+column_types": {
                                "first_date": "timestamp",
                                "second_date": "timestamp",
                            },
                        }
                    }
                },
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_datediff.csv": seeds__data_datediff_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_datediff.yml": models__test_datediff_yml,
            "test_datediff.sql": self.interpolate_macro_namespace(models__test_datediff_sql, "datediff"),
        }


# TODO: Activate this once we have datatypes.sql macro
# class TestCastBoolToText(BaseCastBoolToText):
#     pass


# TODO: Activate this once we have datatypes.sql macro
# class TestSafeCast(BaseSafeCast):
#     pass


class TestListagg(BaseListagg):
    pass


# TODO: Implement this macro when needed
# class TestLastDay(BaseLastDay):
#     pass


class TestCurrentTimestamp(BaseCurrentTimestampNaive):
    pass
