import importlib.metadata
from unittest.mock import Mock

import pytest

from dbt.adapters.athena.config import (
    AthenaSparkSessionConfig,
    EmrServerlessSparkSessionConfig,
    get_boto3_config,
)
from dbt.adapters.athena.constants import (
    EMR_SERVERLESS_SPARK_PROPERTIES,
    ENFORCE_SPARK_PROPERTIES,
)


class TestConfig:
    def test_get_boto3_config(self):
        importlib.metadata.version = Mock(return_value="2.4.6")
        num_boto3_retries = 5
        get_boto3_config.cache_clear()
        config = get_boto3_config(num_retries=num_boto3_retries)
        assert config._user_provided_options["user_agent_extra"] == "dbt-athena-community/2.4.6"
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
            {"engine_config": {"CoordinatorDpuSize": 1}},
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


class TestEmrServerlessSparkSessionConfig:
    """
    A class to test EmrServerlessSparkSessionConfig
    """

    @pytest.fixture
    def emr_serverless_config(self, request):
        """
        Fixture for providing EMR Serverless configuration parameters.

        This fixture returns a dictionary containing the EMR Serverless configuration parameters.
        The parameters can be customized using the `request.param` object. The default values are:
        - `s3_staging_dir`: "s3://example-bucket/staging/"
        - `emr_job_execution_role_arn`: "arn:aws:iam::123456789012:role/EMRServerlessRole"
        - `emr_application_id`: "application-id"
        - `emr_applicationname`: "application-name"
        - `table_type`: "hive"
        - `spark_encryption`: False

        Args:
            self: The test class instance.
            request: The pytest request object.

        Returns:
            dict: The EMR Serverless configuration parameters.
        """
        return {
            "s3_staging_dir": request.param.get("s3_staging_dir", None),
            "emr_job_execution_role_arn": request.param.get("emr_job_execution_role_arn", None),
            "emr_application_id": request.param.get("emr_application_id", "application-id"),
            "emr_application_name": request.param.get("emr_application_name", "application-name"),
            "table_type": request.param.get("table_type", "hive"),
            "spark_encryption": request.param.get("spark_encryption", False),
            "spark_properties": request.param.get("spark_properties"),
        }

    @pytest.fixture
    def emr_serverless_config_helper(self, emr_serverless_config):
        """
        Fixture for testing EmrServerlessSparkSessionConfig class.

        Args:
            emr_serverless_config (dict): Fixture for default EMR Serverless config.

        Returns:
            EmrServerlessSparkSessionConfig: An instance of EmrServerlessSparkSessionConfig class.
        """
        return EmrServerlessSparkSessionConfig(emr_serverless_config)

    @pytest.mark.parametrize(
        "emr_serverless_config",
        [
            {"s3_staging_dir": "s3://new-bucket/staging/"},
            pytest.param({}, marks=pytest.mark.xfail(raises=ValueError)),
            pytest.param({"s3_staging_dir": None}, marks=pytest.mark.xfail(raises=ValueError)),
        ],
        indirect=True,
    )
    def test_get_s3_uri(self, emr_serverless_config_helper):
        s3_uri = emr_serverless_config_helper.get_s3_uri()
        assert s3_uri == emr_serverless_config_helper.config["s3_staging_dir"]

    @pytest.mark.parametrize(
        "emr_serverless_config",
        [
            {"emr_job_execution_role_arn": "arn:aws:iam::987654321098:role/NewEMRServerlessRole"},
            pytest.param({}, marks=pytest.mark.xfail(raises=ValueError)),
            pytest.param({"emr_job_execution_role_arn": None}, marks=pytest.mark.xfail(raises=ValueError)),
        ],
        indirect=True,
    )
    def test_get_emr_job_execution_role_arn(self, emr_serverless_config_helper):
        role_arn = emr_serverless_config_helper.get_emr_job_execution_role_arn()
        assert role_arn == emr_serverless_config_helper.config["emr_job_execution_role_arn"]

    @pytest.mark.parametrize(
        "emr_serverless_config",
        [
            {"emr_application_id": "new-application-id"},
            {"emr_application_name": "new-application-name"},
            {},
            pytest.param({"emr_application_id": None, "emr_application_name": None}, marks=pytest.mark.xfail),
        ],
        indirect=True,
    )
    def test_get_emr_application(self, emr_serverless_config_helper):
        emr_application = emr_serverless_config_helper.get_emr_application()
        assert "emr_application_id" in emr_application or "emr_application_name" in emr_application

    @pytest.mark.parametrize(
        "emr_serverless_config",
        [
            {
                "spark_properties": '{"spark.jars": "s3://bucket/jar1.jar,s3://bucket/jar2.jar"}',
            },
            {"table_type": "iceberg"},
        ],
        indirect=True,
    )
    def test_get_spark_properties(self, emr_serverless_config_helper):
        spark_properties = emr_serverless_config_helper.get_spark_properties()
        assert isinstance(spark_properties, dict)
        assert "spark.jars" in spark_properties
        for key in ENFORCE_SPARK_PROPERTIES:
            if key in spark_properties:
                assert spark_properties[key] == ENFORCE_SPARK_PROPERTIES[key]
        for key in EMR_SERVERLESS_SPARK_PROPERTIES["default"]:
            assert spark_properties[key] == EMR_SERVERLESS_SPARK_PROPERTIES["default"][key]
