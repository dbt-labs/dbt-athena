from unittest import mock

import pytest
from pyathena.model import AthenaQueryExecution

from dbt.adapters.athena import AthenaConnectionManager
from dbt.contracts.connection import AdapterResponse


class TestAthenaConnectionManager:
    @pytest.mark.parametrize(
        ("state", "result"),
        (
            pytest.param(AthenaQueryExecution.STATE_SUCCEEDED, "OK"),
            pytest.param(AthenaQueryExecution.STATE_CANCELLED, "ERROR"),
        ),
    )
    def test_get_response(self, state, result):
        cursor = mock.MagicMock()
        cursor.rowcount = 1
        cursor.state = state
        cm = AthenaConnectionManager(mock.MagicMock())
        response = cm.get_response(cursor)
        assert isinstance(response, AdapterResponse)
        assert response.code == result
        assert response.rows_affected == 1

    def test_data_type_code_to_name(self):
        cm = AthenaConnectionManager(mock.MagicMock())
        assert cm.data_type_code_to_name("array<string>") == "ARRAY"
        assert cm.data_type_code_to_name("map<int, boolean>") == "MAP"
        assert cm.data_type_code_to_name("DECIMAL(3, 7)") == "DECIMAL"
