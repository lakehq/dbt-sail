from unittest import mock

import dbt.adapters.__about__
import dbt.adapters.sail.__version__

from dbt.adapters.sail.impl import SailAdapter
from dbt.adapters.base.relation import AdapterTrackingRelationInfo


def assert_telemetry_data(adapter_name: str, model_adapter_type: str, file_format: str):
    table_formats_within_file_formats = ["delta", "iceberg", "hive", "hudi"]
    expected_table_format = None
    if file_format in table_formats_within_file_formats:
        expected_table_format = file_format

    mock_model_config = mock.MagicMock()
    mock_model_config._extra = mock.MagicMock()
    mock_model_config._extra = {
        "adapter_type": adapter_name,
        "file_format": file_format,
    }

    res = SailAdapter.get_adapter_run_info(mock_model_config)

    assert res.adapter_name == adapter_name
    assert res.base_adapter_version == dbt.adapters.__about__.version
    assert res.adapter_version == dbt.adapters.sail.__version__.version

    assert res.model_adapter_details == {
        "adapter_type": model_adapter_type,
        "table_format": expected_table_format,
    }

    assert type(res) is AdapterTrackingRelationInfo


def test_telemetry_with_sail_details():
    sail_file_formats = [
        "text",
        "csv",
        "json",
        "jdbc",
        "parquet",
        "orc",
        "hive",
        "delta",
        "iceberg",
        "libsvm",
        "hudi",
    ]
    for file_format in sail_file_formats:
        assert_telemetry_data("sail", "spark", file_format)
