from multiprocessing import get_context
from unittest import mock

import pytest
from pyathena.model import AthenaQueryExecution

from dbt.adapters.athena import AthenaConnectionManager
from dbt.adapters.athena.connections import AthenaAdapterResponse


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
        cursor.data_scanned_in_bytes = 123
        cm = AthenaConnectionManager(mock.MagicMock(), get_context("spawn"))
        response = cm.get_response(cursor)
        assert isinstance(response, AthenaAdapterResponse)
        assert response.code == result
        assert response.rows_affected == 1
        assert response.data_scanned_in_bytes == 123

    def test_data_type_code_to_name(self):
        cm = AthenaConnectionManager(mock.MagicMock(), get_context("spawn"))
        assert cm.data_type_code_to_name("array<string>") == "ARRAY"
        assert cm.data_type_code_to_name("map<int, boolean>") == "MAP"
        assert cm.data_type_code_to_name("DECIMAL(3, 7)") == "DECIMAL"
