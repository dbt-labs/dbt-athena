from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_array_append import BaseArrayAppend
from dbt.tests.adapter.utils.test_array_construct import BaseArrayConstruct
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_escape_single_quotes import (
    BaseEscapeSingleQuotesQuote,
)
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral


class TestAnyValue(BaseAnyValue):
    pass


class TestArrayAppend(BaseArrayAppend):
    pass


class TestArrayConstruct(BaseArrayConstruct):
    pass


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
    pass


class TestStringLiteral(BaseStringLiteral):
    pass


class TestDateTrunc(BaseDateTrunc):
    pass


# TODO: Why is this failing ?
# class TestDateAdd(BaseDateAdd):
#     pass


# TODO: Why is this failing ?
# class TestDateDiff(BaseDateDiff):
#    pass


# TODO: Activate this once we have datatypes.sql macro
# class TestCastBoolToText(BaseCastBoolToText):
#     pass


# TODO: Activate this once we have datatypes.sql macro
# class TestSafeCast(BaseSafeCast):
#     pass


# TODO: Implement this macro when needed
# class TestListagg(BaseListagg):
#     pass


# TODO: Implement this macro when needed
# class TestLastDay(BaseLastDay):
#     pass


# TODO Implement this macro since right now can't concat empty array with non-empty array
# class TestArrayConcat(BaseArrayConcat):
#     pass


# TODO: Right now this test is failing
#  pyathena.error.OperationalError: NOT_SUPPORTED: Casting a Timestamp with Time Zone to Timestamp is not supported
# class TestCurrentTimestamp(BaseCurrentTimestampNaive):
#     pass
