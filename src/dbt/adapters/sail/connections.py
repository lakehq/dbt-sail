from dataclasses import dataclass, field
from typing import Any, Dict, Tuple

from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.spark.connections import SparkConnectionManager, SparkCredentials
from dbt_common.dataclass_schema import StrEnum
from dbt_common.exceptions import DbtConfigError

logger = AdapterLogger("Sail")


class SailConnectionMethod(StrEnum):
    EMBEDDED = "embedded"
    REMOTE = "remote"


@dataclass
class SailCredentials(SparkCredentials):
    mode: SailConnectionMethod = SailConnectionMethod.EMBEDDED  # type: ignore
    server_side_parameters: Dict[str, str] = field(default_factory=dict)

    @property
    def type(self) -> str:
        return "sail"

    @property
    def unique_field(self) -> str:
        return self.host or "embedded"  # type: ignore

    def _connection_keys(self) -> Tuple[str, ...]:
        return "mode", "host", "port", "schema"

    def __post_init__(self) -> None:
        # For embedded mode, host is not required — set defaults
        if self.mode == SailConnectionMethod.EMBEDDED:
            if self.host is None:  # type: ignore
                self.host = "127.0.0.1"
            if self.method is None:  # type: ignore
                # Set method to session so SparkCredentials validation doesn't fail
                from dbt.adapters.spark.connections import SparkConnectionMethod

                self.method = SparkConnectionMethod.SESSION
        elif self.mode == SailConnectionMethod.REMOTE:
            if self.host is None:
                raise DbtConfigError("Must specify `host` for remote mode")
            if self.method is None:
                from dbt.adapters.spark.connections import SparkConnectionMethod

                self.method = SparkConnectionMethod.SESSION

        if self.schema is None:
            raise DbtConfigError("Must specify `schema` in profile")

        # Spark treats database and schema as the same thing
        if self.database is not None and self.database != self.schema:  # type: ignore
            raise DbtConfigError(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"  # type: ignore
                f"On Sail, database must be omitted or have the same value as schema."
            )
        self.database = None


class SailConnectionManager(SparkConnectionManager):
    TYPE = "sail"

    # Track the embedded server so it can be reused/stopped
    _server = None

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        creds: SailCredentials = connection.credentials  # type: ignore

        try:
            if creds.mode == SailConnectionMethod.EMBEDDED:
                handle = cls._open_embedded(creds)
            elif creds.mode == SailConnectionMethod.REMOTE:
                handle = cls._open_remote(creds)
            else:
                raise DbtConfigError(f"invalid Sail connection mode: {creds.mode}")
        except Exception as e:
            logger.debug(f"Error opening connection: {e}")
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise FailedToConnectError(f"failed to connect to Sail: {e}") from e

        connection.handle = handle
        connection.state = ConnectionState.OPEN
        return connection

    @classmethod
    def _open_embedded(cls, creds: SailCredentials) -> Any:
        """Start a SparkConnectServer in-process via pysail, then connect PySpark to it."""
        from pysail.spark import SparkConnectServer
        from dbt.adapters.spark.session import (
            SessionConnectionWrapper,
            Connection as SessionConnection,
        )

        if cls._server is None or not cls._server.running:  # type: ignore
            port = creds.port if creds.port != 443 else 0
            cls._server = SparkConnectServer(ip=creds.host or "127.0.0.1", port=port)  # type: ignore
            cls._server.start(background=True)  # type: ignore
            logger.debug(f"Started embedded Sail server at {cls._server.listening_address}")  # type: ignore

        ip, port = cls._server.listening_address  # type: ignore
        remote_url = f"sc://{ip}:{port}"

        params = dict(creds.server_side_parameters)
        params["spark.remote"] = remote_url
        return SessionConnectionWrapper(SessionConnection(server_side_parameters=params))

    @classmethod
    def _open_remote(cls, creds: SailCredentials) -> Any:
        """Connect PySpark to an already-running Sail server via Spark Connect."""
        from dbt.adapters.spark.session import (
            SessionConnectionWrapper,
            Connection as SessionConnection,
        )

        port = creds.port if creds.port != 443 else 50051
        remote_url = f"sc://{creds.host}:{port}"

        params = dict(creds.server_side_parameters)
        params["spark.remote"] = remote_url
        return SessionConnectionWrapper(SessionConnection(server_side_parameters=params))
