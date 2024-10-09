import importlib.metadata
from unittest.mock import Mock

import pytest

from dbt.adapters.athena.config import AthenaSparkSessionConfig, get_boto3_config


class TestConfig:
    def test_get_boto3_config(self):
        importlib.metadata.version = Mock(return_value="2.4.6")
        num_boto3_retries = 5
        get_boto3_config.cache_clear()
        config = get_boto3_config(num_retries=num_boto3_retries)
        assert config._user_provided_options["user_agent_extra"] == "dbt-athena/2.4.6"
        assert config.retries == {"max_attempts": num_boto3_retries, "mode": "standard"}


class TestAthenaSparkSessionConfig:
    """
    A class to test AthenaSparkSessionConfig
    """

    @pytest.fixture
    def spark_config(self, request):
        """
        Fixture for providing Spark configuration parameters.

        This fixture returns a dictionary containing the Spark configuration parameters. The parameters can be
        customized using the `request.param` object. The default values are:
        - `timeout`: 7200 seconds
        - `polling_interval`: 5 seconds
        - `engine_config`: {"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 2, "DefaultExecutorDpuSize": 1}

        Args:
            self: The test class instance.
            request: The pytest request object.

        Returns:
            dict: The Spark configuration parameters.

        """
        return {
            "timeout": request.param.get("timeout", 7200),
            "polling_interval": request.param.get("polling_interval", 5),
            "engine_config": request.param.get(
                "engine_config", {"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 2, "DefaultExecutorDpuSize": 1}
            ),
        }

    @pytest.fixture
    def spark_config_helper(self, spark_config):
        """Fixture for testing AthenaSparkSessionConfig class.

        Args:
            spark_config (dict): Fixture for default spark config.

        Returns:
            AthenaSparkSessionConfig: An instance of AthenaSparkSessionConfig class.
        """
        return AthenaSparkSessionConfig(spark_config)

    @pytest.mark.parametrize(
        "spark_config",
        [
            {"timeout": 5},
            {"timeout": 10},
            {"timeout": 20},
            {},
            pytest.param({"timeout": -1}, marks=pytest.mark.xfail),
            pytest.param({"timeout": None}, marks=pytest.mark.xfail),
        ],
        indirect=True,
    )
    def test_set_timeout(self, spark_config_helper):
        timeout = spark_config_helper.set_timeout()
        assert timeout == spark_config_helper.config.get("timeout", 7200)

    @pytest.mark.parametrize(
        "spark_config",
        [
            {"polling_interval": 5},
            {"polling_interval": 10},
            {"polling_interval": 20},
            {},
            pytest.param({"polling_interval": -1}, marks=pytest.mark.xfail),
        ],
        indirect=True,
    )
    def test_set_polling_interval(self, spark_config_helper):
        polling_interval = spark_config_helper.set_polling_interval()
        assert polling_interval == spark_config_helper.config.get("polling_interval", 5)

    @pytest.mark.parametrize(
        "spark_config",
        [
            {"engine_config": {"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 2, "DefaultExecutorDpuSize": 1}},
            {"engine_config": {"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 2, "DefaultExecutorDpuSize": 2}},
            {},
            pytest.param({"engine_config": {"CoordinatorDpuSize": 1}}, marks=pytest.mark.xfail),
            pytest.param({"engine_config": [1, 1, 1]}, marks=pytest.mark.xfail),
        ],
        indirect=True,
    )
    def test_set_engine_config(self, spark_config_helper):
        engine_config = spark_config_helper.set_engine_config()
        diff = set(engine_config.keys()) - {
            "CoordinatorDpuSize",
            "MaxConcurrentDpus",
            "DefaultExecutorDpuSize",
            "SparkProperties",
            "AdditionalConfigs",
        }
        assert len(diff) == 0
