import pytest
from dbt.tests.util import run_dbt

# Modified: source-scoped fixture imports for Sail's shared test harness.
# [CREDIT]: https://github.com/dbt-labs/dbt-adapters/blob/f353ce0c30820b7aa363b6a7b39d5ef3b4e910d9/dbt-spark/tests/functional/adapter/seed_column_types/test_seed_column_types.py
from tests.functional.spark.seed_column_types.fixtures import (
    _MACRO_TEST_IS_TYPE_SQL,
    _SEED_CSV,
    _SEED_YML,
)


@pytest.mark.skip_profile("spark_session")
class TestSeedColumnTypesCast:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"test_is_type.sql": _MACRO_TEST_IS_TYPE_SQL}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"payments.csv": _SEED_CSV, "schema.yml": _SEED_YML}

    #  We want to test seed types because hive would cause all fields to be strings.
    # setting column_types in project.yml should change them and pass.
    def test_column_seed_type(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        run_dbt(["test"], expect_pass=False)
