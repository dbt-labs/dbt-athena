import pytest

from dbt.tests.adapter.unit_testing.test_case_insensitivity import (
    BaseUnitTestCaseInsensivity,
)
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput
from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes


class TestAthenaUnitTestingTypes(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["2.0", "2.0"],
            ["'12345'", "12345"],
            ["'string'", "string"],
            ["true", "true"],
            ["date '2024-04-01'", "2024-04-01"],
            ["timestamp '2024-04-01 00:00:00.000'", "'2024-04-01 00:00:00.000'"],
            # TODO: activate once safe_cast supports complex structures
            # ["array[1, 2, 3]", "[1, 2, 3]"],
            # [
            #     "map(array['10', '15', '20'], array['t', 'f', NULL])",
            #     """'{"10: "t", "15": "f", "20": null}'""",
            # ],
        ]


class TestAthenaUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass


class TestAthenaUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass
