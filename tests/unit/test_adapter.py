import unittest
import pytest
from multiprocessing import get_context
from unittest import mock

from dbt_common.exceptions import DbtRuntimeError
from dbt.adapters.sail import SailAdapter
from dbt.adapters.spark.impl import TABLE_OR_VIEW_NOT_FOUND_MESSAGES

# TODO: dbt-spark (PyPI) no longer exports SCHEMA_NOT_FOUND_MESSAGES. Hardcoded here
# from the archived dbt-adapters monorepo. Revisit once upstream re-exposes the list
# or we settle on a cleaner source for these strings.
SCHEMA_NOT_FOUND_MESSAGES = (
    "[SCHEMA_NOT_FOUND]",
    "Schema not found",
    "Database not found",
    "NoSuchNamespaceException",
    "NoSuchDatabaseException",
)


class TestSailAdapter(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def set_up_fixtures(self, target_sail_embedded, base_project_cfg):
        self.base_project_cfg = base_project_cfg
        self.target_sail_embedded = target_sail_embedded

    def test_adapter_type(self):
        assert SailAdapter.type() == "sail"

    def test_relation_with_database(self):
        adapter = SailAdapter(self.target_sail_embedded, get_context("spawn"))
        # fine
        adapter.Relation.create(schema="different", identifier="table")
        with self.assertRaises(DbtRuntimeError):
            # not fine - database set
            adapter.Relation.create(database="something", schema="different", identifier="table")


class TestListRelationsWithoutCaching(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def set_up_fixtures(self, target_sail_embedded):
        self.target_sail_embedded = target_sail_embedded

    def _make_adapter(self):
        return SailAdapter(self.target_sail_embedded, get_context("spawn"))

    def _make_schema_relation(self, adapter, schema="analytics"):
        return adapter.Relation.create(schema=schema, identifier="").without_identifier()

    def test_schema_not_found_returns_empty(self):
        adapter = self._make_adapter()
        schema_relation = self._make_schema_relation(adapter, schema="nonexistent")

        with mock.patch.object(
            adapter,
            "execute_macro",
            side_effect=DbtRuntimeError("Database not found"),
        ):
            result = adapter.list_relations_without_caching(schema_relation)
            self.assertEqual(result, [])


@pytest.mark.parametrize("not_found_msg", SCHEMA_NOT_FOUND_MESSAGES)
def test_all_schema_not_found_messages_return_empty(not_found_msg, target_sail_embedded):
    adapter = SailAdapter(target_sail_embedded, get_context("spawn"))
    schema_relation = adapter.Relation.create(
        schema="nonexistent", identifier=""
    ).without_identifier()

    with mock.patch.object(
        adapter,
        "execute_macro",
        side_effect=DbtRuntimeError(not_found_msg),
    ):
        result = adapter.list_relations_without_caching(schema_relation)
        assert result == []


@pytest.mark.parametrize("not_found_msg", TABLE_OR_VIEW_NOT_FOUND_MESSAGES)
def test_all_table_or_view_not_found_messages_return_empty(not_found_msg, target_sail_embedded):
    adapter = SailAdapter(target_sail_embedded, get_context("spawn"))
    schema_relation = adapter.Relation.create(
        schema="nonexistent", identifier=""
    ).without_identifier()

    with mock.patch.object(
        adapter,
        "execute_macro",
        side_effect=DbtRuntimeError(not_found_msg),
    ):
        result = adapter.list_relations_without_caching(schema_relation)
        assert result == []
