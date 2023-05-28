from functools import lru_cache

import pkg_resources
from botocore import config

from dbt.adapters.athena.constants import LOGGER

DEFAULT_POLLING_INTERVAL = 5
DEFAULT_ENGINE_CONFIG = {"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 2, "DefaultExecutorDpuSize": 1}
DEFAULT_TIMEOUT = 60 * 60 * 2


@lru_cache()
def get_boto3_config() -> config.Config:
    return config.Config(
        user_agent_extra="dbt-athena-community/" + pkg_resources.get_distribution("dbt-athena-community").version
    )


class AthenaSparkSessionConfig:
    """
    A helper class to manage Athena Spark Session Configuration.
    """

    def __init__(self, config: dict) -> None:
        self.config = config

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
        timeout = self.config.get("timeout", DEFAULT_TIMEOUT)
        if not isinstance(timeout, int):
            raise TypeError("Timeout must be an integer")
        if timeout <= 0:
            raise ValueError("Timeout must be a positive integer")
        LOGGER.debug(f"Setting timeout: {timeout}")
        return timeout

    def set_polling_interval(self) -> float:
        polling_interval = self.config.get("polling_interval", DEFAULT_POLLING_INTERVAL)
        if not isinstance(polling_interval, int) or polling_interval <= 0:
            raise ValueError("Polling_interval must be a positive integer")
        LOGGER.debug(f"Setting polling_interval: {polling_interval}")
        return polling_interval

    def set_engine_config(self) -> dict:
        engine_config = self.config.get("engine_config", DEFAULT_ENGINE_CONFIG)
        if not isinstance(engine_config, dict):
            raise TypeError("Engine configuration has to be of type dict")

        expected_keys = {"CoordinatorDpuSize", "MaxConcurrentDpus", "DefaultExecutorDpuSize"}
        if set(engine_config.keys()) != expected_keys:
            raise KeyError(f"The keys of the dictionary entered do not match the expected format: {expected_keys}")

        if engine_config["MaxConcurrentDpus"] == 1:
            raise KeyError("The lowest value supported for MaxConcurrentDpus is 2")
        LOGGER.debug(f"Setting engine configuration: {engine_config}")
        return engine_config
