import pytest

from dbt.adapters.sail.connections import SailConnectionMethod, SailCredentials
from dbt_common.exceptions import DbtConfigError


def test_embedded_credentials_defaults() -> None:
    credentials = SailCredentials(
        mode=SailConnectionMethod.EMBEDDED,  # type: ignore
        schema="analytics",
    )
    assert credentials.type == "sail"
    assert credentials.host == "127.0.0.1"
    assert credentials.schema == "analytics"
    assert credentials.database is None


def test_remote_credentials() -> None:
    credentials = SailCredentials(
        mode=SailConnectionMethod.REMOTE,  # type: ignore
        host="myorg.sailhost.com",
        port=50051,
        schema="analytics",
    )
    assert credentials.type == "sail"
    assert credentials.host == "myorg.sailhost.com"
    assert credentials.port == 50051


def test_remote_credentials_requires_host() -> None:
    with pytest.raises(DbtConfigError):
        SailCredentials(
            mode=SailConnectionMethod.REMOTE,  # type: ignore
            schema="analytics",
        )


def test_credentials_schema_required() -> None:
    with pytest.raises(DbtConfigError):
        SailCredentials(
            mode=SailConnectionMethod.EMBEDDED,  # type: ignore
        )


def test_credentials_database_must_match_schema() -> None:
    with pytest.raises(DbtConfigError):
        SailCredentials(
            mode=SailConnectionMethod.EMBEDDED,  # type: ignore
            schema="analytics",
            database="different",
        )


def test_credentials_server_side_parameters() -> None:
    credentials = SailCredentials(
        mode=SailConnectionMethod.EMBEDDED,  # type: ignore
        schema="analytics",
        server_side_parameters={"spark.driver.memory": "4g"},
    )
    assert credentials.server_side_parameters["spark.driver.memory"] == "4g"


def test_credentials_unique_field_embedded() -> None:
    credentials = SailCredentials(
        mode=SailConnectionMethod.EMBEDDED,  # type: ignore
        schema="analytics",
    )
    assert credentials.unique_field == "127.0.0.1"


def test_credentials_unique_field_remote() -> None:
    credentials = SailCredentials(
        mode=SailConnectionMethod.REMOTE,  # type: ignore
        host="myorg.sailhost.com",
        schema="analytics",
    )
    assert credentials.unique_field == "myorg.sailhost.com"


def _remote_url(**kwargs) -> str:
    from dbt.adapters.sail.connections import SailConnectionManager

    credentials = SailCredentials(
        mode=SailConnectionMethod.REMOTE,  # type: ignore
        host="myorg.sailhost.com",
        schema="analytics",
        **kwargs,
    )
    return SailConnectionManager._remote_url(credentials)


def test_remote_url_default_port_uses_sail_port() -> None:
    assert _remote_url() == "sc://myorg.sailhost.com:50051/"


def test_remote_url_explicit_port() -> None:
    assert _remote_url(port=15002) == "sc://myorg.sailhost.com:15002/"


def test_remote_url_with_token_keeps_port_443() -> None:
    assert _remote_url(token="abc") == "sc://myorg.sailhost.com:443/;token=abc"


def test_remote_url_with_use_ssl_keeps_port_443() -> None:
    assert _remote_url(use_ssl=True) == "sc://myorg.sailhost.com:443/;use_ssl=true"


def test_remote_credentials_hide_token() -> None:
    credentials = SailCredentials(
        mode=SailConnectionMethod.REMOTE,  # type: ignore
        host="myorg.sailhost.com",
        schema="analytics",
        token="abc",
    )
    assert "token" not in credentials._connection_keys()
