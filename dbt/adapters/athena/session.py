from typing import Optional

import boto3.session
from dbt.contracts.connection import Connection

__BOTO3_SESSION__: Optional[boto3.session.Session] = None


def get_boto3_session(connection: Connection = None) -> boto3.session.Session:
    def init_session():
        global __BOTO3_SESSION__
        __BOTO3_SESSION__ = boto3.session.Session(
            region_name=connection.credentials.region_name,
            profile_name=connection.credentials.aws_profile_name,
        )

    if not __BOTO3_SESSION__:
        if connection is None:
            raise RuntimeError(
                "A Connection object needs to be passed to initialize the boto3 session for the first time"
            )
        init_session()

    return __BOTO3_SESSION__
