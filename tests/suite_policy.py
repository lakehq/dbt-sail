"""Source-scoped collection and format defaults (no engine dependencies)."""

from copy import deepcopy
from pathlib import Path

FUNCTIONAL_ROOT = Path(__file__).parent / "functional"


def relative_test_path(path):
    try:
        return Path(path).resolve().relative_to(FUNCTIONAL_ROOT.resolve()).as_posix()
    except ValueError:
        return ""


def suite_policy(relative_path):
    if relative_path.startswith("spark/"):
        return "spark", "delta"
    return None, None


def format_defaults(config, file_format):
    """Copy before injecting; nested and inline explicit dbt configs keep precedence."""
    result = deepcopy(config)
    for resource in ("models", "seeds", "snapshots"):
        settings = result.setdefault(resource, {})
        if "file_format" not in settings and "+file_format" not in settings:
            settings["+file_format"] = file_format
    return result
