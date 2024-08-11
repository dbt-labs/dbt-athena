import importlib.metadata
from functools import lru_cache
from typing import Any, Dict
import json

from botocore import config

from dbt.adapters.athena.constants import (
    DEFAULT_CALCULATION_TIMEOUT,
    DEFAULT_POLLING_INTERVAL,
    DEFAULT_SPARK_COORDINATOR_DPU_SIZE,
    DEFAULT_SPARK_EXECUTOR_DPU_SIZE,
    DEFAULT_SPARK_MAX_CONCURRENT_DPUS,
    DEFAULT_SPARK_PROPERTIES,
    ENFORCE_SPARK_PROPERTIES,
    EMR_SERVERLESS_SPARK_PROPERTIES,
    LOGGER,
)


@lru_cache()
def get_boto3_config(num_retries: int) -> config.Config:
    return config.Config(
        user_agent_extra="dbt-athena-community/" + importlib.metadata.version("dbt-athena-community"),
        retries={"max_attempts": num_retries, "mode": "standard"},
    )


class SparkSessionConfig:
    """
    A helper class to manage Spark Session Configuration.
    """

    def __init__(self, config: Dict[str, Any], **session_kwargs: Any) -> None:
        self.config = config
        self.session_kwargs = session_kwargs

    def set_timeout(self) -> int:
        """
        Get the timeout value.

        This function retrieves the timeout value from the parsed model's configuration. If the timeout value
        is not defined, it falls back to the default timeout value. If the retrieved timeout value is less than or
        equal to 0, a ValueError is raised as timeout must be a positive integer.

        Returns:
            int: The timeout value in seconds.

        Raises:
            ValueError: If the timeout value is not a positive integer.

        """
        timeout = self.config.get("timeout", DEFAULT_CALCULATION_TIMEOUT)
        if not isinstance(timeout, int):
            raise TypeError("Timeout must be an integer")
        if timeout <= 0:
            raise ValueError("Timeout must be a positive integer")
        LOGGER.debug(f"Setting timeout: {timeout}")
        return timeout

    def get_polling_interval(self) -> Any:
        """
        Get the polling interval for the configuration.

        Returns:
            Any: The polling interval value.

        Raises:
            KeyError: If the polling interval is not found in either `self.config`
                or `self.session_kwargs`.
        """
        try:
            return self.config["polling_interval"]
        except KeyError:
            try:
                return self.session_kwargs["polling_interval"]
            except KeyError:
                return DEFAULT_POLLING_INTERVAL

    def set_polling_interval(self) -> float:
        """
        Set the polling interval for the configuration.

        Returns:
            float: The polling interval value.

        Raises:
            ValueError: If the polling interval is not a positive integer.
        """
        polling_interval = self.get_polling_interval()
        if not (isinstance(polling_interval, float) or isinstance(polling_interval, int)) or polling_interval <= 0:
            raise ValueError(f"Polling_interval must be a positive number. Got: {polling_interval}")
        LOGGER.debug(f"Setting polling_interval: {polling_interval}")
        return float(polling_interval)

    def try_parse_json(self, config) -> dict:
        if isinstance(config, str):
            try:
                config = json.loads(config)
            except json.JSONDecodeError:
                raise ValueError(f"Invalid JSON string: {config}")

        return config


class AthenaSparkSessionConfig(SparkSessionConfig):
    """
    A helper class to manage Athena Spark Session Configuration.
    """

    def set_engine_config(self) -> Dict[str, Any]:
        """Set the engine configuration.

        Returns:
            Dict[str, Any]: The engine configuration.

        Raises:
            TypeError: If the engine configuration is not of type dict.
            KeyError: If the keys of the engine configuration dictionary do not match the expected format.
        """
        table_type = self.config.get("table_type", "hive")
        spark_encryption = self.config.get("spark_encryption", False)
        spark_cross_account_catalog = self.config.get("spark_cross_account_catalog", False)
        spark_requester_pays = self.config.get("spark_requester_pays", False)

        default_spark_properties: Dict[str, str] = dict(
            **(
                DEFAULT_SPARK_PROPERTIES.get(table_type)
                if table_type.lower() in ["iceberg", "hudi", "delta_lake"]
                else {}
            ),
            **DEFAULT_SPARK_PROPERTIES.get("spark_encryption") if spark_encryption else {},
            **DEFAULT_SPARK_PROPERTIES.get("spark_cross_account_catalog") if spark_cross_account_catalog else {},
            **DEFAULT_SPARK_PROPERTIES.get("spark_requester_pays") if spark_requester_pays else {},
        )

        default_engine_config = {
            "CoordinatorDpuSize": DEFAULT_SPARK_COORDINATOR_DPU_SIZE,
            "MaxConcurrentDpus": DEFAULT_SPARK_MAX_CONCURRENT_DPUS,
            "DefaultExecutorDpuSize": DEFAULT_SPARK_EXECUTOR_DPU_SIZE,
            "SparkProperties": default_spark_properties,
        }
        engine_config = self.config.get("engine_config", None)
        engine_config = self.try_parse_json(engine_config)

        if engine_config:
            provided_spark_properties = self.config.get("spark_properties", engine_config.get("SparkProperties", None))
            provided_spark_properties = self.try_parse_json(provided_spark_properties)
            if provided_spark_properties:
                default_spark_properties.update(provided_spark_properties)
                # Enforce certain properties
                for key in ENFORCE_SPARK_PROPERTIES:
                    if key in default_spark_properties:
                        default_spark_properties[key] = ENFORCE_SPARK_PROPERTIES[key]
                default_engine_config["SparkProperties"] = default_spark_properties
                engine_config.pop("SparkProperties", None)
            default_engine_config.update(engine_config)
        engine_config = default_engine_config

        if not isinstance(engine_config, dict):
            raise TypeError("Engine configuration has to be of type dict")

        expected_keys = {
            "CoordinatorDpuSize",
            "MaxConcurrentDpus",
            "DefaultExecutorDpuSize",
            "SparkProperties",
            "AdditionalConfigs",
        }

        if set(engine_config.keys()) - {
            "CoordinatorDpuSize",
            "MaxConcurrentDpus",
            "DefaultExecutorDpuSize",
            "SparkProperties",
            "AdditionalConfigs",
        }:
            raise KeyError(
                f"The engine configuration keys provided do not match the expected athena engine keys: {expected_keys}"
            )

        if engine_config["MaxConcurrentDpus"] == 1:
            raise KeyError("The lowest value supported for MaxConcurrentDpus is 2")
        LOGGER.debug(f"Setting engine configuration: {engine_config}")
        return engine_config


class EmrServerlessSparkSessionConfig(SparkSessionConfig):
    """
    A helper class to manage EMR Serverless Spark Session Configuration.
    """

    def get_s3_uri(self) -> str:
        """
        Get the s3_staging_dir bucket for the configuration.

        Returns:
            Any: The s3_staging_dir bucket value.

        Raises:
            KeyError: If the s3_staging_dir value is not found in either `self.config`
                or `self.session_kwargs`.
        """
        try:
            return self.config["s3_staging_dir"]
        except KeyError:
            try:
                return self.session_kwargs["s3_staging_dir"]
            except KeyError:
                raise ValueError("s3_staging_dir is required configuration")

    def get_emr_job_execution_role_arn(self) -> str:
        """
        Get the emr_job_execution_role_arn for the configuration.

        Returns:
            Any: The emr_job_execution_role_arn value.

        Raises:
            KeyError: If the emr_job_execution_role_arn value is not found in either `self.config`
                or `self.session_kwargs`.
        """
        try:
            return self.config["emr_job_execution_role_arn"]
        except KeyError:
            try:
                return self.session_kwargs["emr_job_execution_role_arn"]
            except KeyError:
                raise ValueError("emr_job_execution_role_arn is required configuration for EMR serverless job")

    def get_emr_application(self) -> dict:
        """
        Get the emr_application_id or emr_application_name for the configuration.

        Returns:
            Any: The emr_application_id or emr_application_name value.

        Raises:
            KeyError: If the emr_application_id or emr_application_name value is not found in either `self.config`
                or `self.session_kwargs`.
        """
        if self.config.get("emr_application_id", None):
            return {"emr_application_id": self.config["emr_application_id"]}
        elif self.config.get("emr_application_name", None):
            return {"emr_application_name": self.config["emr_application_name"]}
        elif self.session_kwargs.get("emr_application_id", None):
            return {"emr_application_id": self.session_kwargs["emr_application_id"]}
        elif self.session_kwargs.get("emr_application_name", None):
            return {"emr_application_name": self.session_kwargs["emr_application_name"]}
        else:
            raise ValueError(
                "emr_application_id or emr_application_name is required configuration for EMR serverless job"
            )

    def get_spark_properties(self) -> Dict[str, str]:
        """
        Gets the default spark properties after updated with the provided configuration from parsed model

        Returns:
            Dict[str, str]: Spark properties
        """
        table_type = self.config.get("table_type", "hive")
        spark_encryption = self.config.get("spark_encryption", False)
        default_spark_properties: Dict[str, str] = dict(
            **EMR_SERVERLESS_SPARK_PROPERTIES.get("default"),
            **(
                EMR_SERVERLESS_SPARK_PROPERTIES.get(table_type)
                if table_type.lower() in ["iceberg", "hudi", "delta_lake"]
                else {}
            ),
            **EMR_SERVERLESS_SPARK_PROPERTIES.get("spark_encryption") if spark_encryption else {},
        )

        provided_spark_properties = self.config.get("spark_properties", None)
        provided_spark_properties = self.try_parse_json(provided_spark_properties)

        if provided_spark_properties:
            spark_jars = provided_spark_properties.get("spark.jars", None)
            if spark_jars:
                jar_list = spark_jars.split(",")
                def_spark_jars = default_spark_properties.get("spark.jars", "")
                def_jar_list = def_spark_jars.split(",")
                jar_updated_list = list(set([item.strip() for item in jar_list + def_jar_list if item.strip()]))
                final_jars = ", ".join(jar_updated_list)
                provided_spark_properties["spark.jars"] = final_jars
            default_spark_properties.update(provided_spark_properties)
            # Enforce certain properties
            for key in ENFORCE_SPARK_PROPERTIES:
                if key in default_spark_properties:
                    default_spark_properties[key] = ENFORCE_SPARK_PROPERTIES[key]
        return default_spark_properties

