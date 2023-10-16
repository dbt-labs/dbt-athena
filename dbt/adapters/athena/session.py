import boto3.session

from dbt.contracts.connection import Connection


def get_boto3_session(connection: Connection) -> boto3.session.Session:
    return boto3.session.Session(
        aws_access_key_id=connection.credentials.aws_access_key_id,
        aws_secret_access_key=connection.credentials.aws_secret_access_key,
        aws_session_token=connection.credentials.aws_session_token,
        region_name=connection.credentials.region_name,
        profile_name=connection.credentials.aws_profile_name,
    )
