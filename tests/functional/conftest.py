import pytest


@pytest.fixture(scope="session")
def sail_server_address(request):
    if request.config.getoption("--sail-connection-mode") == "embedded":
        yield "127.0.0.1", 443
        from dbt.adapters.sail.connections import SailConnectionManager

        server = SailConnectionManager._server
        if server is not None:
            server.stop()
            SailConnectionManager._server = None
        return

    from pysail.spark import SparkConnectServer

    server = SparkConnectServer(ip="127.0.0.1", port=0)
    server.start(background=True)
    try:
        yield server.listening_address
    finally:
        server.stop()


@pytest.fixture(scope="class", autouse=True)
def close_sail_session(sail_server_address):
    # Release clients before server shutdown and isolate per-class SQL settings.
    yield
    from pyspark.sql.connect.session import SparkSession
    from pysail.testing.spark.session import patch_spark_connect_session

    sessions = (SparkSession.getActiveSession(), SparkSession._get_default_session())
    seen = set()
    for session in sessions:
        if session is not None and id(session) not in seen:
            seen.add(id(session))
            patch_spark_connect_session(session)
            session.stop()


@pytest.fixture(scope="class")
def dbt_profile_target(request, tmp_path_factory, sail_server_address):
    host, port = sail_server_address
    return {
        "type": "sail",
        "mode": request.config.getoption("--sail-connection-mode"),
        "host": host,
        "port": port,
        "schema": "test_sail",
        "threads": 1,
        "server_side_parameters": {
            "spark.sql.warehouse.dir": str(tmp_path_factory.mktemp("warehouse").resolve()),
            "spark.sql.session.timeZone": "UTC",
        },
    }
