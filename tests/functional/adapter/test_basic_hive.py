"""
Run the basic dbt test suite on hive tables when applicable.
"""
import pytest

from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.util import check_result_nodes_by_name, run_dbt


class TestSimpleMaterializationsHive(BaseSimpleMaterializations):
    pass


class TestSingularTestsHive(BaseSingularTests):
    pass


class TestSingularTestsEphemeralHive(BaseSingularTestsEphemeral):
    def test_singular_tests_ephemeral(self, project):
        # check results from seed command
        results = run_dbt(["seed"])
        assert len(results) == 1
        check_result_nodes_by_name(results, ["base"])

        # Check results from test command
        results = run_dbt(["--debug", "test"], expect_pass=False)
        assert len(results) == 2
        check_result_nodes_by_name(results, ["passing", "failing"])

        # Check result status
        for result in results:
            if result.node.name == "passing":
                assert result.status == "pass"
            elif result.node.name == "failing":
                assert result.status == "fail"

        # check results from run command
        results = run_dbt()
        assert len(results) == 2
        check_result_nodes_by_name(results, ["failing_model", "passing_model"])


class TestEmptyHive(BaseEmpty):
    pass


class TestEphemeralHive(BaseEphemeral):
    pass


class TestIncrementalHive(BaseIncremental):
    pass


class TestGenericTestsHive(BaseGenericTests):
    pass


@pytest.mark.skip(reason="The in-place update is not supported for seeds. We need our own implementation instead.")
class TestSnapshotCheckColsHive(BaseSnapshotCheckCols):
    pass


@pytest.mark.skip(reason="The in-place update is not supported for seeds. We need our own implementation instead.")
class TestSnapshotTimestampHive(BaseSnapshotTimestamp):
    pass


@pytest.mark.skip(
    reason="Fails because the test tries to fetch the table metadata during the compile step, "
    "before the models are actually run. Not sure how this test is intended to work."
)
class TestBaseAdapterMethodHive(BaseAdapterMethod):
    pass
