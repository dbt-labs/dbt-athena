import importlib.metadata
from unittest.mock import Mock

from dbt.adapters.athena.config import get_boto3_config


class TestConfig:
    def test_get_boto3_config(self):
        importlib.metadata.version = Mock(return_value="2.4.6")
        num_boto3_retries = 5
        get_boto3_config.cache_clear()
        config = get_boto3_config(num_retries=num_boto3_retries)
        assert config._user_provided_options["user_agent_extra"] == "dbt-athena-community/2.4.6"
        assert config.retries == {"max_attempts": num_boto3_retries, "mode": "standard"}
