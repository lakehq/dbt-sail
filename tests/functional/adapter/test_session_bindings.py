"""Binding coverage alongside dbt-tests-adapter 1.20.0's simple_seed contracts.

Semantic adaptations: BaseBasicSeedTests (seed/reseed/full-refresh),
BaseSimpleSeedColumnOverride (column_types), and BaseSeedSpecificFormats
(batching/Unicode). Spark's seed macro casts each binding to its column type.
"""

import csv
import datetime as dt
import io
import math
from decimal import Decimal

import pytest
from dbt.tests.util import run_dbt
from dbt_common.exceptions import DbtRuntimeError


@pytest.fixture(scope="class", params=["false", "true"], ids=["unescape", "literal-escapes"])
def dbt_profile_target(dbt_profile_target, request):
    return {
        **dbt_profile_target,
        "server_side_parameters": {
            **dbt_profile_target.get("server_side_parameters", {}),
            "spark.sql.session.timeZone": "UTC",
            "spark.sql.ansi.enabled": "true",
            "spark.sql.parser.escapedStringLiterals": request.param,
        },
    }


def _csv(header, rows):
    contents = io.StringIO()
    writer = csv.writer(contents)
    writer.writerow(header)
    writer.writerows(rows)
    return contents.getvalue()


def _micros(value):
    delta = value - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds


class TestSessionBindings:
    @pytest.mark.parametrize(
        "value",
        [
            None,
            "None",
            "O'Reilly",
            "\\n\\t\\'",
            "\x0012",
            "\0\b\n\r\t\x1a\fé中",
            "'); drop table users; --",
        ],
    )
    def test_uncast_string_and_null_roundtrip(self, project, value):
        with project.adapter.connection_named("uncast-string-bindings"):
            _, cursor = project.adapter.connections.add_query(
                "select %s", bindings=[value], auto_begin=False
            )
            assert cursor.fetchall()[0][0] == value

    def test_explicit_decimal_cast_preserves_precision(self, project):
        value = Decimal("9007199254740993.123456789")
        with project.adapter.connection_named("decimal-cast-binding"):
            _, cursor = project.adapter.connections.add_query(
                "select cast(%s as decimal(38,9))", bindings=[value], auto_begin=False
            )
            assert cursor.fetchall()[0][0] == value

    def test_strings_and_null(self, project):
        values = [
            None,
            "None",
            "",
            "O'Reilly",
            "\\n\\t\\'",
            "\x0012",
            "\0\b\t\n\r\x1a\f\x7f",
            "é中🙂%_",
            "'); DROP TABLE users; --",
        ]
        with project.adapter.connection_named("string-bindings"):
            _, cursor = project.adapter.connections.add_query(
                "select " + ", ".join(["cast(%s as string)"] * len(values)),
                bindings=values,
                auto_begin=False,
            )
            assert tuple(cursor.fetchall()[0]) == tuple(values)
            assert [column[1] for column in cursor.description] == ["string"] * len(values)

    def test_numeric_values_and_types(self, project):
        values = [
            True,
            False,
            2**63 - 1,
            -(2**63),
            Decimal("9" * 38),
            Decimal("1E-38"),
            Decimal("1E+38"),
            Decimal("1E-39"),
            Decimal("1E+100"),
            Decimal("1E-100"),
            1.25,
            -0.0,
        ]
        expressions = ["%s", "%s", "cast(%s as bigint)", "cast(%s as bigint)"]
        with project.adapter.connection_named("numeric-bindings"):
            _, cursor = project.adapter.connections.add_query(
                "select " + ", ".join(expressions + ["%s"] * (len(values) - 4)),
                bindings=values,
                auto_begin=False,
            )
            row = tuple(cursor.fetchall()[0])
            assert row == (
                True,
                False,
                2**63 - 1,
                -(2**63),
                Decimal("9" * 38),
                Decimal("1E-38"),
                1e38,
                1e-39,
                1e100,
                1e-100,
                1.25,
                -0.0,
            )
            assert math.copysign(1, row[-1]) == -1
            assert [column[1] for column in cursor.description] == [
                "boolean",
                "boolean",
                "bigint",
                "bigint",
                "decimal(38,0)",
                "decimal(38,38)",
                "double",
                "double",
                "double",
                "double",
                "double",
                "double",
            ]

    def test_oversized_integer_double_fallback(self, project):
        integers = [10**38, -(10**38), 10**40 + 1, -(10**40 + 1)]
        values = integers + [Decimal(value) for value in integers]
        # Python's int-to-float conversion is independent of SQL serialization.
        expected = tuple(float(value) for value in integers) * 2
        with project.adapter.connection_named("oversized-integer-bindings"):
            _, cursor = project.adapter.connections.add_query(
                "select " + ", ".join(["cast(%s as double)"] * len(values)),
                bindings=values,
                auto_begin=False,
            )
            assert tuple(cursor.fetchall()[0]) == expected
            assert [column[1] for column in cursor.description] == ["double"] * len(values)
            for value in (10**309, -(10**309)):
                with pytest.raises(DbtRuntimeError, match="finite nonzero DOUBLE range"):
                    project.adapter.connections.add_query(
                        "select cast(%s as double)", bindings=[value], auto_begin=False
                    )
            _, cursor = project.adapter.connections.add_query("select 42", auto_begin=False)
            assert cursor.fetchall()[0][0] == 42

    def test_temporal_precision_and_offsets(self, project):
        aware = dt.datetime(
            2024,
            2,
            29,
            12,
            34,
            56,
            123456,
            dt.timezone(dt.timedelta(hours=5, minutes=30)),
        )
        naive = dt.datetime(1969, 12, 31, 23, 59, 59, 999999)
        values = [
            aware,
            naive,
            dt.date(2024, 2, 29),
            dt.time(12, 34, 56, 123456),
            dt.time(12, 34, tzinfo=dt.timezone(dt.timedelta(hours=-7))),
        ]
        with project.adapter.connection_named("temporal-bindings"):
            _, cursor = project.adapter.connections.add_query(
                "select unix_micros(cast(%s as timestamp)), "
                "unix_micros(cast(%s as timestamp)), cast(%s as date), %s, %s",
                bindings=values,
                auto_begin=False,
            )
            assert tuple(cursor.fetchall()[0]) == (
                _micros(aware),
                -1,
                dt.date(2024, 2, 29),
                "12:34:56.123456",
                "12:34:00-07:00",
            )


class TestSeedBindings:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "bindings.csv": _csv(
                [
                    "seq",
                    "id",
                    "amount",
                    "flag",
                    "tiny",
                    "large",
                    "label",
                    "born",
                    "happened",
                ],
                [
                    [
                        1,
                        9007199254740993,
                        "12345678901234567890.123456789",
                        "true",
                        "1E-100",
                        "1.25",
                        "O'Reilly\\n\nnext",
                        "2024-02-29",
                        "2024-02-29T12:34:56.123456+05:30",
                    ],
                    [
                        2,
                        -(2**63),
                        "-0.000000001",
                        "false",
                        "-1.234E-100",
                        "1E+100",
                        "None",
                        "1969-12-31",
                        "1969-12-31T23:59:59.999999Z",
                    ],
                    [3, None, None, None, None, None, None, None, None],
                    [
                        4,
                        42,
                        "0",
                        "true",
                        "0",
                        "0",
                        "'\\\0\b\n\r\t\x1aé中%_; --",
                        None,
                        None,
                    ],
                ],
            )
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
                "+column_types": {"id": "bigint", "amount": "decimal(38,9)"},
            }
        }

    def test_seed_reseed_and_full_refresh(self, project):
        relation = f"{project.test_schema}.bindings"
        expected = [
            (
                1,
                9007199254740993,
                Decimal("12345678901234567890.123456789"),
                True,
                1e-100,
                1.25,
                "O'Reilly\\n\nnext",
                dt.date(2024, 2, 29),
                _micros(dt.datetime(2024, 2, 29, 7, 4, 56, 123456, dt.timezone.utc)),
            ),
            (
                2,
                -(2**63),
                Decimal("-0.000000001"),
                False,
                -1.234e-100,
                1e100,
                "None",
                dt.date(1969, 12, 31),
                -1,
            ),
            (3, None, None, None, None, None, None, None, None),
            # dbt opens CSV in universal-newline mode: CR becomes LF before
            # binding. The direct binding test above covers unmodified CR.
            (
                4,
                42,
                Decimal("0"),
                True,
                0.0,
                0.0,
                "'\\\0\b\n\n\t\x1aé中%_; --",
                None,
                None,
            ),
        ]
        for args in (["seed"], ["seed"], ["seed", "--full-refresh"]):
            assert len(run_dbt(args)) == 1
            rows = project.run_sql(
                f"select seq, id, amount, flag, tiny, large, label, born, "
                f"unix_micros(happened) from {relation} order by seq",
                fetch="all",
            )
            assert [tuple(row) for row in rows] == expected
            schema = project.run_sql(f"describe {relation}", fetch="all")
            assert [(row[0], row[1]) for row in schema[:9]] == [
                ("seq", "bigint"),
                ("id", "bigint"),
                ("amount", "decimal(38,9)"),
                ("flag", "boolean"),
                ("tiny", "double"),
                ("large", "double"),
                ("label", "string"),
                ("born", "date"),
                ("happened", "timestamp"),
            ]


class TestSeedBindingBatches:
    @pytest.fixture(scope="class")
    def seeds(self):
        # dbt's default get_batch_size() is 10,000; exercise both sides of it.
        return {
            "batches.csv": _csv(
                ["id", "label"],
                ((i, "boundary'\\é%" if i >= 9999 else "plain") for i in range(10001)),
            )
        }

    def test_seed_multiple_batches(self, project):
        assert len(run_dbt(["seed"])) == 1
        rows = project.run_sql(
            f"select id, label from {project.test_schema}.batches order by id",
            fetch="all",
        )
        assert [tuple(row) for row in rows] == [
            (i, "boundary'\\é%" if i >= 9999 else "plain") for i in range(10001)
        ]
