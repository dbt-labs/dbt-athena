import importlib.metadata
from functools import lru_cache

from botocore import config


@lru_cache()
def get_boto3_config(num_retries: int) -> config.Config:
    return config.Config(
        user_agent_extra="dbt-athena-community/" + importlib.metadata.version("dbt-athena-community"),
        retries={"max_attempts": num_retries, "mode": "standard"},
    )
