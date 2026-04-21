import pytest


pytest_plugins = ["dbt.tests.fixtures.project"]


@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "sail",
        "mode": "embedded",
        "host": "127.0.0.1",
        "schema": "test_sail",
        "threads": 1,
    }
