import pkg_resources
from botocore import config


def get_boto3_config() -> config.Config:
    return config.Config(
        user_agent_extra="dbt-athena-community/" + pkg_resources.get_distribution("dbt-athena-community").version
    )
