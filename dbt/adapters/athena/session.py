import boto3.session

from dbt.contracts.connection import Connection


def get_boto3_session(connection: Connection) -> boto3.session.Session:
    return boto3.session.Session(
        region_name=connection.credentials.region_name,
        profile_name=connection.credentials.aws_profile_name,
    )
