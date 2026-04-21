import pytest

from tests.unit.utils import config_from_parts_or_dicts


@pytest.fixture(scope="session", autouse=True)
def base_project_cfg():
    return {
        "name": "X",
        "version": "0.1",
        "profile": "test",
        "project-root": "/tmp/dbt/does-not-exist",
        "quoting": {
            "identifier": False,
            "schema": False,
        },
        "config-version": 2,
    }


@pytest.fixture(scope="session", autouse=True)
def target_sail_embedded(base_project_cfg):
    return config_from_parts_or_dicts(
        base_project_cfg,
        {
            "outputs": {
                "test": {
                    "type": "sail",
                    "mode": "embedded",
                    "schema": "analytics",
                    "host": "127.0.0.1",
                }
            },
            "target": "test",
        },
    )


@pytest.fixture(scope="session", autouse=True)
def target_sail_remote(base_project_cfg):
    return config_from_parts_or_dicts(
        base_project_cfg,
        {
            "outputs": {
                "test": {
                    "type": "sail",
                    "mode": "remote",
                    "schema": "analytics",
                    "host": "myorg.sailhost.com",
                    "port": 50051,
                }
            },
            "target": "test",
        },
    )
