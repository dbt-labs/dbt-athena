import importlib.metadata
from functools import lru_cache
from typing import Any, Dict

from botocore import config

from dbt.adapters.athena.constants import (
    DEFAULT_CALCULATION_TIMEOUT,
    DEFAULT_POLLING_INTERVAL,
    DEFAULT_SPARK_COORDINATOR_DPU_SIZE,
    DEFAULT_SPARK_EXECUTOR_DPU_SIZE,
    DEFAULT_SPARK_MAX_CONCURRENT_DPUS,
    DEFAULT_SPARK_PROPERTIES,
    LOGGER,
)


@lru_cache()
def get_boto3_config(num_retries: int) -> config.Config:
    return config.Config(
        user_agent_extra="dbt-athena-community/" + importlib.metadata.version("dbt-athena-community"),
        retries={"max_attempts": num_retries, "mode": "standard"},
    )


class AthenaSparkSessionConfig:
    """
    A helper class to manage Athena Spark Session Configuration.
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
            **DEFAULT_SPARK_PROPERTIES.get(table_type)
            if table_type.lower() in ["iceberg", "hudi", "delta_lake"]
            else {},
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

        if engine_config:
            provided_spark_properties = engine_config.get("SparkProperties", None)
            if provided_spark_properties:
                default_spark_properties.update(provided_spark_properties)
                default_engine_config["SparkProperties"] = default_spark_properties
                engine_config.pop("SparkProperties")
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
