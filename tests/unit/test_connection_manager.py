from unittest import mock

import pytest
from dbt.contracts.connection import AdapterResponse
from pyathena.model import AthenaQueryExecution

from dbt.adapters.athena import AthenaConnectionManager


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
