from dbt.adapters.base import AdapterPlugin

from dbt.adapters.sail.connections import SailConnectionManager, SailCredentials  # noqa: F401
from dbt.adapters.sail.impl import SailAdapter
from dbt.include import sail

Plugin = AdapterPlugin(
    adapter=SailAdapter,  # type: ignore
    credentials=SailCredentials,
    include_path=sail.PACKAGE_PATH,
    dependencies=["spark"],
)
