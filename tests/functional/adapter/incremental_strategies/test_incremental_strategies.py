import pytest

from dbt.tests.util import run_dbt, check_relations_equal
from dbt.tests.adapter.simple_seed.test_seed import SeedConfigBase
from tests.functional.adapter.incremental_strategies.seeds import (
    expected_append_csv,
    expected_upsert_csv,
)


class BaseIncrementalStrategies(SeedConfigBase):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_append.csv": expected_append_csv,
            "expected_upsert.csv": expected_upsert_csv,
        }

    @staticmethod
    def seed_and_run_once():
        run_dbt(["seed"])
        run_dbt(["run"])

    @staticmethod
    def seed_and_run_twice():
        run_dbt(["seed"])
        run_dbt(["run"])
        run_dbt(["run"])


class TestDeltaStrategies(BaseIncrementalStrategies):
    @pytest.mark.skip(
        reason="this feature is incompatible with databricks settings required for grants"
    )
    def test_delta_strategies_overwrite(self, project):
        self.seed_and_run_twice()
        check_relations_equal(
            project.adapter, ["insert_overwrite_partitions_delta", "expected_upsert"]
        )
