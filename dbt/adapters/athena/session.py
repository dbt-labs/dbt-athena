import boto3.session
from dbt.contracts.connection import Connection


def get_boto3_session(connection: Connection = None) -> boto3.session.Session:
    if connection is None:
        raise RuntimeError("A Connection object needs to be passed to initialize the boto3 session")

    return boto3.session.Session(
        region_name=connection.credentials.region_name,
        profile_name=connection.credentials.aws_profile_name,
    )
