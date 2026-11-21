import pytest
from dbt.tests.util import run_dbt
from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonModelTests,
    BasePythonIncrementalTests,
)
from dbt.tests.adapter.python_model.test_spark import BasePySparkTests


@pytest.mark.skip_profile("apache_spark", "spark_session", "spark_http_odbc")
class TestPythonModelSpark(BasePythonModelTests):
    pass


@pytest.mark.skip_profile("apache_spark", "spark_session", "spark_http_odbc")
class TestPySpark(BasePySparkTests):
    def test_different_dataframes(self, project):
        """
        Test that python models are supported using dataframes from:
        - pandas
        - pyspark
        - pyspark.pandas (formerly dataspark.koalas)

        """
        results = run_dbt(["run", "--exclude", "koalas_df"])
        assert len(results) == 3


@pytest.mark.skip_profile("apache_spark", "spark_session", "spark_http_odbc")
class TestPythonIncrementalModelSpark(BasePythonIncrementalTests):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {}
