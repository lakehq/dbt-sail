import pytest
from tests.suite_policy import relative_test_path, suite_policy

pytest_plugins = ["dbt.tests.fixtures.project", "pytester"]


def pytest_addoption(parser):
    parser.addoption("--sail-connection-mode", choices=("embedded", "remote"), default="embedded")
    parser.addoption("--file-format", choices=("delta", "iceberg"), default=None)


def pytest_configure(config):
    for marker in ("spark", "skip_profile(*profiles)", "external"):
        config.addinivalue_line("markers", marker)


def pytest_collection_modifyitems(items):
    for item in items:
        source, _ = suite_policy(relative_test_path(item.path))
        if source:
            item.add_marker(getattr(pytest.mark, source))
