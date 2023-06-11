from functools import lru_cache

import pkg_resources
from botocore import config

from dbt.adapters.athena.constants import (
    DEFAULT_POLLING_INTERVAL,
    DEFAULT_SPARK_ENGINE_CONFIG,
    DEFAULT_SPARK_SESSION_TIMEOUT,
    LOGGER,
)


@lru_cache()
def get_boto3_config() -> config.Config:
    return config.Config(
        user_agent_extra="dbt-athena-community/" + pkg_resources.get_distribution("dbt-athena-community").version
    )


class AthenaSparkSessionConfig:
    """
    A helper class to manage Athena Spark Session Configuration.
    """

<<<<<<< HEAD
    def __init__(self, config: dict) -> None:
=======
    def __init__(self, config: Dict[str, Any], **session_kwargs: Any) -> None:
>>>>>>> 5f40faf (Fixed readme. Moved some defaults to constants.)
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
        timeout = self.config.get("timeout", DEFAULT_SPARK_SESSION_TIMEOUT)
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

<<<<<<< HEAD
    def set_engine_config(self) -> dict:
        engine_config = self.config.get("engine_config", DEFAULT_ENGINE_CONFIG)
=======
    def set_engine_config(self) -> Dict[str, Any]:
        """Set the engine configuration.

        Returns:
            Dict[str, Any]: The engine configuration.

        Raises:
            TypeError: If the engine configuration is not of type dict.
            KeyError: If the keys of the engine configuration dictionary do not match the expected format.
        """
        engine_config = self.config.get("engine_config", DEFAULT_SPARK_ENGINE_CONFIG)
>>>>>>> 5f40faf (Fixed readme. Moved some defaults to constants.)
        if not isinstance(engine_config, dict):
            raise TypeError("Engine configuration has to be of type dict")

        expected_keys = {"CoordinatorDpuSize", "MaxConcurrentDpus", "DefaultExecutorDpuSize"}
        if set(engine_config.keys()) != expected_keys:
            raise KeyError(f"The keys of the dictionary entered do not match the expected format: {expected_keys}")

        if engine_config["MaxConcurrentDpus"] == 1:
            raise KeyError("The lowest value supported for MaxConcurrentDpus is 2")
        LOGGER.debug(f"Setting engine configuration: {engine_config}")
        return engine_config
