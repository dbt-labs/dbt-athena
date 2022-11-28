from unittest.mock import Mock

import pkg_resources

from dbt.adapters.athena.config import get_boto3_config


class TestConfig:
    def test_get_boto3_config(self):
        pkg_resources.get_distribution = Mock(return_value=pkg_resources.Distribution(version="2.4.6"))
        get_boto3_config.cache_clear()
        config = get_boto3_config()
        assert config._user_provided_options["user_agent_extra"] == "dbt-athena-community/2.4.6"
