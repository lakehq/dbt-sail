from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
import pytest


class TestSimpleMaterializationsSpark(BaseSimpleMaterializations):
    pass


class TestSingularTestsSpark(BaseSingularTests):
    pass


# The local cluster currently tests on spark 2.x, which does not support this
# if we upgrade it to 3.x, we can enable this test
class TestSingularTestsEphemeralSpark(BaseSingularTestsEphemeral):
    pass


class TestEmptySpark(BaseEmpty):
    pass


class TestEphemeralSpark(BaseEphemeral):
    pass


class TestIncrementalSpark(BaseIncremental):
    pass


class TestGenericTestsSpark(BaseGenericTests):
    pass


# Skipped to match dbt-spark upstream, which excludes these tests for the
# apache_spark and spark_session profiles (requires delta file_format).
@pytest.mark.skip(reason="snapshot tests require delta file_format; skipped upstream for session profile")
class TestSnapshotCheckColsSpark(BaseSnapshotCheckCols):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "+file_format": "delta",
            },
            "snapshots": {
                "+file_format": "delta",
            },
        }


@pytest.mark.skip(reason="snapshot tests require delta file_format; skipped upstream for session profile")
class TestSnapshotTimestampSpark(BaseSnapshotTimestamp):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "+file_format": "delta",
            },
            "snapshots": {
                "+file_format": "delta",
            },
        }


class TestBaseAdapterMethod(BaseAdapterMethod):
    pass
