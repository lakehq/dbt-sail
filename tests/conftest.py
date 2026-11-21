pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption("--sail-connection-mode", choices=("embedded", "remote"), default="embedded")
