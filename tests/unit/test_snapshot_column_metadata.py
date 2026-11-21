"""Exercise installed dbt snapshot macros with Sail's actual column parser.

Only database I/O is replaced: DESCRIBE returns agate rows, not Column objects.
"""

from pathlib import Path
from types import SimpleNamespace

import agate
import jinja2
import pytest
from dbt.adapters.sail.impl import SailAdapter
from dbt.adapters.spark.column import SparkColumn
from dbt.clients.jinja import MacroGenerator
from dbt.include import global_project, sail, spark
from dbt_common.clients.jinja import parse
from dbt_common.exceptions import MacroReturn


def macro_return(value):
    raise MacroReturn(value)


@pytest.fixture
def macros():
    adapter = object.__new__(SailAdapter)
    table = agate.Table(
        [
            ("id", "bigint", None),
            ("business_value", "string", None),
            ("", "", None),
            ("# Detailed Table Information", "", None),
            ("Owner", "snapshot_owner", None),
        ],
        ["col_name", "data_type", "comment"],
        [agate.Text(), agate.Text(), agate.Text()],
    )
    statements = []
    executions = []

    def statement(name, caller, **kwargs):
        statements.append((name, " ".join(caller().split())))
        return ""

    context = {
        "adapter": adapter,
        "return": macro_return,
        "config": {},
        "statement": statement,
        "load_result": lambda name: SimpleNamespace(table=table),
    }
    paths = [
        Path(global_project.PACKAGE_PATH) / "macros/adapters/columns.sql",
        Path(global_project.PACKAGE_PATH) / "macros/materializations/snapshots/helpers.sql",
        Path(spark.PACKAGE_PATH) / "macros/adapters.sql",
        Path(sail.PACKAGE_PATH) / "macros/adapters.sql",
    ]
    for path in paths:
        sql = path.read_text()
        for node in parse(sql).find_all(jinja2.nodes.Macro):
            name = node.name.removeprefix("dbt_macro__")
            macro = SimpleNamespace(
                name=name,
                macro_sql=sql,
                unique_id=name,
                resource_type="macro",
                original_file_path=str(path),
                package_name=path.parts[-3],
            )
            context[name] = MacroGenerator(macro, context)

    def dispatch(name, namespace):
        for prefix in ("sail", "spark", "default"):
            if f"{prefix}__{name}" in context:
                return context[f"{prefix}__{name}"]
        raise AssertionError(f"Unexpected dispatch: {namespace}.{name}")

    def execute_macro(name, kwargs):
        executions.append(name)
        assert executions == ["get_columns_in_relation_raw"], "Recursive column lookup"
        return context[name](**kwargs)

    adapter.dispatch = dispatch
    adapter.execute_macro = execute_macro
    relation = adapter.Relation.create(schema="snapshots", identifier="target", type="table")
    return SimpleNamespace(
        context=context,
        relation=relation,
        table=table,
        statements=statements,
        executions=executions,
    )


def test_column_lookup_returns_named_columns_via_raw_describe(macros):
    columns = macros.context["get_columns_in_relation"](macros.relation)
    assert all(isinstance(column, SparkColumn) for column in columns)
    assert [column.name for column in columns] == ["id", "business_value"]
    assert [column.dtype for column in columns] == ["bigint", "string"]
    assert macros.context["get_list_of_column_names"](columns) == ["id", "business_value"]
    assert macros.executions == ["get_columns_in_relation_raw"]
    assert macros.statements == [
        ("get_columns_in_relation_raw", f"describe extended {macros.relation}")
    ]


def test_raw_lookup_returns_agate_without_python_column_lookup(macros):
    result = macros.context["get_columns_in_relation_raw"](macros.relation)
    assert result is macros.table
    assert result.rows[0]["col_name"] == "id"
    assert macros.executions == []
    assert len(macros.statements) == 1


@pytest.mark.parametrize("new_columns", [[], ["new_business_value"]])
def test_snapshot_deletion_records_choose_existing_and_new_columns(macros, new_columns):
    macros.context.update(
        get_columns_in_query=lambda sql: ["id", "business_value", *new_columns],
        snapshot_get_time=lambda: "current_timestamp()",
        snapshot_hash_arguments=lambda args: "'new_scd_id'",
    )
    strategy = dict(
        hard_deletes="new_record",
        unique_key="id",
        updated_at="updated_at",
        scd_id="scd_id",
        row_changed="true",
    )
    sql = macros.context["snapshot_staging_table"](
        strategy, "select * from source", macros.relation
    )
    deletion_records = sql.split("deletion_records as (", 1)[1].split("from snapshotted_data", 1)[
        0
    ]
    for name in ("id", "business_value"):
        assert f"snapshotted_data.`{name}`" in deletion_records
        assert f"source_data.`{name}`" not in deletion_records
    for name in new_columns:
        assert f"source_data.`{name}`" in deletion_records
        assert f"snapshotted_data.`{name}`" not in deletion_records
    assert macros.executions == ["get_columns_in_relation_raw"]
