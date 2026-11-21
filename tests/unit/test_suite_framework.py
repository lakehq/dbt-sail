"""Framework regressions exercise real pytest fixture consumption, without Sail."""

import importlib
import json
from pathlib import Path

import pytest

from tests.suite_policy import format_defaults, suite_policy

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    "path,expected",
    [
        ("spark/test_basic.py", ("spark", "delta")),
        ("adapter/test_basic.py", (None, None)),
    ],
)
def test_source_defaults(path, expected):
    assert suite_policy(path) == expected


def test_defaults_copy_and_explicit_precedence():
    original = {
        "models": {"file_format": "parquet"},
        "seeds": {"+file_format": "delta"},
        "snapshots": {"project": {"+file_format": "iceberg"}},
    }
    result = format_defaults(original, "iceberg")
    assert result["models"] == original["models"]
    assert result["seeds"] == original["seeds"]
    assert result["snapshots"]["project"] == original["snapshots"]["project"]
    assert "+file_format" not in original["snapshots"]
    assert result["snapshots"] is not original["snapshots"]


def test_same_module_names_coexist():
    names = ["adapter.test_basic", "spark.test_basic"]
    modules = [importlib.import_module("tests.functional." + name) for name in names]
    assert len({module.__name__ for module in modules}) == len(names)
    assert len({module.__file__ for module in modules}) == len(names)


def test_manifest_targets_and_inventory():
    manifest = json.loads((ROOT / "tests/suites.json").read_text())
    counts = {source: 0 for source in manifest["sources"]}
    tests = dict.fromkeys(counts, 0)
    for entry in manifest["files"]:
        counts[entry["source"]] += 1
        tests[entry["source"]] += entry["is_test_file"]
        assert entry["status"] in {
            "direct_import",
            "semantic_port",
            "not_ported",
            "not_applicable",
        }
        if entry["status"] == "not_ported":
            assert entry["target_paths"] == []
            assert entry["coverage"] == "none"
        if entry["status"] == "semantic_port":
            assert entry["coverage"] == "partial"
        if entry["status"] == "not_applicable":
            assert entry["coverage"] == "none"
            assert set(entry["case_statuses"]) == {"not_applicable"}
        for target in entry["target_paths"]:
            assert not Path(target).is_absolute()
            assert (ROOT / target).is_file()
    assert counts == {"spark": 33}
    assert tests == {"spark": 23}
    assert (
        sum(
            e["source"] == "spark" and e["is_test_file"] and e["status"] == "direct_import"
            for e in manifest["files"]
        )
        == 21
    )
    for source in manifest["sources"].values():
        assert len(source["sha"]) == 40
    assert "exclusions" not in manifest
    assert "/Users/" not in json.dumps(manifest)


@pytest.mark.parametrize("option", [None, "delta", "iceberg"])
def test_real_fixture_injection(pytester, option):
    """Copy the actual hooks; override only server lifecycle to avoid engine use."""
    tests = pytester.path / "tests"
    functional = tests / "functional"
    functional.mkdir(parents=True)
    for path in (tests, functional):
        (path / "__init__.py").write_text("")
    for name in ("conftest.py", "suite_policy.py"):
        (tests / name).write_text((ROOT / "tests" / name).read_text())
    hook = (ROOT / "tests/functional/conftest.py").read_text()
    hook += (
        '\n@pytest.fixture(scope="class", autouse=True)\ndef close_sail_session():\n    yield\n'
    )
    (functional / "conftest.py").write_text(hook)
    for source, default in [
        ("spark", "delta"),
        ("local", None),
    ]:
        directory = functional / source
        directory.mkdir(parents=True, exist_ok=True)
        for parent in (directory, directory.parent):
            (parent / "__init__.py").write_text("")
        expected = (option or default) if default else None
        (directory / "test_defaults.py").write_text(
            """import pytest
SHARED = {"models": {"project": {"+file_format": "parquet"}}}
EXPLICIT = {"models": {"+file_format": "parquet"}, "seeds": {"file_format": "parquet"}}
@pytest.fixture(scope="class")
def project_config_update():
    return SHARED
@pytest.fixture(scope="class")
def consumed(project_config_update):
    return project_config_update
class TestDefault:
    def test_consumed(self, consumed):
        assert consumed["models"].get("+file_format") == EXPECTED
        assert consumed["models"]["project"]["+file_format"] == "parquet"
        assert SHARED == {"models": {"project": {"+file_format": "parquet"}}}
    def test_cached(self, request, consumed):
        assert request.getfixturevalue("project_config_update") is consumed
        assert consumed["models"].get("+file_format") == EXPECTED
class TestClassOverride:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return EXPLICIT
    def test_consumed(self, consumed):
        assert consumed["models"] == {"+file_format": "parquet"}
        assert consumed["seeds"] == {"file_format": "parquet"}
        assert EXPLICIT == {"models": {"+file_format": "parquet"}, "seeds": {"file_format": "parquet"}}
""".replace(
                "EXPECTED", repr(expected)
            )
        )
    args = ["tests", "-q", "--sail-connection-mode=remote"]
    if option:
        args.append("--file-format=" + option)
    result = pytester.runpytest_subprocess(*args)
    result.assert_outcomes(passed=6)
