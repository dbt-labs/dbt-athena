import botocore.session
import pytest
from dbt.contracts.connection import Connection

from dbt.adapters.athena import AthenaCredentials
from dbt.adapters.athena.session import get_boto3_session


class TestSession:
    @pytest.mark.parametrize(
        ("credentials_profile_name", "boto_profile_name"),
        (
            pytest.param(None, "default", id="no_profile_in_credentials"),
            pytest.param("my_profile", "my_profile", id="profile_in_credentials"),
        ),
    )
    def test_session_should_be_called_with_correct_parameters(
        self, monkeypatch, credentials_profile_name, boto_profile_name
    ):
        def mock___build_profile_map(_):
            return {**{"default": {}}, **({} if not credentials_profile_name else {credentials_profile_name: {}})}

        monkeypatch.setattr(botocore.session.Session, "_build_profile_map", mock___build_profile_map)
        connection = Connection(
            type="test",
            name="test_session",
            credentials=AthenaCredentials(
                database="db",
                schema="schema",
                s3_staging_dir="dir",
                region_name="eu-west-1",
                aws_profile_name=credentials_profile_name,
            ),
        )
        session = get_boto3_session(connection)
        assert session.region_name == "eu-west-1"
        assert session.profile_name == boto_profile_name
