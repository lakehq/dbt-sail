from dbt.adapters.spark.impl import SparkAdapter
from dbt.adapters.sail.connections import SailConnectionManager


class SailAdapter(SparkAdapter):
    ConnectionManager = SailConnectionManager

    @classmethod
    def type(cls) -> str:
        return "sail"
