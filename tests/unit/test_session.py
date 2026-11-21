import datetime as dt
from decimal import Decimal, localcontext
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.sail.connections import SailConnectionManager, SailCredentials
from dbt.adapters.sail.session import SailSessionConnectionWrapper


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, "NULL"),
        (True, "TRUE"),
        (False, "FALSE"),
        (0, "0"),
        (2**63 - 1, "9223372036854775807"),
        (-(2**63), "-9223372036854775808"),
        (10**38 - 1, "9" * 38),
        (-(10**38 - 1), "-" + "9" * 38),
        (10**38, "1e+38D"),
        (-(10**38), "-1e+38D"),
        (10**40 + 1, "1e+40D"),
        (-(10**40 + 1), "-1e+40D"),
        (1.25, "1.25D"),
        (-0.0, "-0.0D"),
        (1e-100, "1e-100D"),
        (1e100, "1e+100D"),
    ],
)
def test_scalar_literals(value, expected):
    with localcontext() as context:
        context.prec = 2
        assert SailSessionConnectionWrapper._fix_binding(value) == expected


@pytest.mark.parametrize(
    "text, expected",
    [
        ("9007199254740993.123456789", "9007199254740993.123456789"),
        ("-123.45000", "-123.45"),
        ("1E+30", "1000000000000000000000000000000"),
        ("0.000000000000000001", "0.000000000000000001"),
        ("0E-100", "0"),
        ("9" * 38, "9" * 38),
        ("1E+37", "1" + "0" * 37),
        ("1E-38", "0." + "0" * 37 + "1"),
        ("0." + "9" * 38, "0." + "9" * 38),
        ("1.000E-38", "0." + "0" * 37 + "1"),
        ("1." + "0" * 100, "1"),
        ("0E+1000000", "0"),
        ("-0E-1000000", "0"),
        ("1E+38", "1e+38D"),
        ("1E-39", "1e-39D"),
        ("1E+100", "1e+100D"),
        ("1E-100", "1e-100D"),
        ("-1.23456789E-100", "-1.23456789e-100D"),
        ("1." + "2" * 38, "1.2222222222222223D"),
        ("5E-324", "5e-324D"),
        ("-5E-324", "-5e-324D"),
        ("1.7976931348623157E+308", "1.7976931348623157e+308D"),
    ],
)
def test_decimal_domain_without_context_rounding(text, expected):
    # Decimal.normalize() would lose digits with this context.
    with localcontext() as context:
        context.prec = 2
        assert SailSessionConnectionWrapper._fix_binding(Decimal(text)) == expected


@pytest.mark.parametrize(
    "value, text",
    [
        ("", ""),
        ("None", "None"),
        ("O'Reilly", "O'Reilly"),
        ("\\n\\t\\'", "\\n\\t\\'"),
        ("\0\b\t\n\r\x1a\f", "\0\b\t\n\r\x1a\f"),
        ("'); drop table users; --", "'); drop table users; --"),
        ("O'Reilly\\n\n\x0012é中%_", "O'Reilly\\n\n\x0012é中%_"),
        (dt.date(2024, 2, 29), "2024-02-29"),
        (dt.datetime(2024, 2, 29, 12, 34, 56, 123456), "2024-02-29 12:34:56.123456"),
        (
            dt.datetime(
                2024,
                2,
                29,
                12,
                34,
                56,
                123456,
                dt.timezone(dt.timedelta(hours=5, minutes=30)),
            ),
            "2024-02-29 12:34:56.123456+05:30",
        ),
        (dt.time(12, 34, 56, 123456), "12:34:56.123456"),
        (dt.time(12, 34, tzinfo=dt.timezone.utc), "12:34:00+00:00"),
    ],
)
def test_utf8_and_temporal_literals(value, text):
    assert SailSessionConnectionWrapper._fix_binding(value) == (
        f"CAST(X'{text.encode('utf-8').hex()}' AS STRING)"
    )


@pytest.mark.parametrize(
    "value",
    [
        float("nan"),
        float("inf"),
        float("-inf"),
        Decimal("NaN"),
        Decimal("sNaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
        Decimal("1.7976931348623159E+308"),
        Decimal("-1E+1000000"),
        Decimal("1E-324"),
        Decimal("-1E-1000000"),
        pytest.param(10**309, id="positive-integer-overflow"),
        pytest.param(-(10**309), id="negative-integer-overflow"),
        [],
        {},
        b"bytes",
        object(),
        dt.timedelta(seconds=1),
    ],
)
def test_invalid_binding_does_not_execute(value):
    handle = Mock()
    wrapper = SailSessionConnectionWrapper(handle).cursor()
    with pytest.raises(DbtRuntimeError):
        wrapper.execute("select %s, %s", [1, value])
    handle.cursor.return_value.execute.assert_not_called()


def test_execute_uses_sail_literals():
    handle = Mock()
    wrapper = SailSessionConnectionWrapper(handle).cursor()
    wrapper.execute("select %s, %s, %s;", [None, 2**53 + 1, "a'b"])
    handle.cursor.return_value.execute.assert_called_once_with(
        "select %s, %s, %s", "NULL", "9007199254740993", "CAST(X'612762' AS STRING)"
    )


@pytest.mark.parametrize("bindings", [None, []])
def test_no_bindings_preserve_upstream_execution(bindings):
    handle = Mock()
    wrapper = SailSessionConnectionWrapper(handle).cursor()
    wrapper.execute("select '100%';", bindings)
    handle.cursor.return_value.execute.assert_called_once_with("select '100%'")


@pytest.mark.parametrize("mode", ["embedded", "remote"])
@pytest.mark.parametrize(
    "port, auth", [(12345, {}), (443, {"use_ssl": True}), (443, {"token": "abc"})]
)
def test_connection_modes_preserve_configuration(monkeypatch, mode, port, auth):
    monkeypatch.setattr(
        SailConnectionManager,
        "_server",
        SimpleNamespace(running=True, listening_address=("127.0.0.1", 12345)),
    )
    parameters = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.parser.escapedStringLiterals": "true",
        "spark.remote": "sc://ignored:1",
    }
    credentials = SailCredentials(
        mode=mode,
        schema="test",
        host="127.0.0.1",
        port=port,
        **auth,
        server_side_parameters=parameters.copy(),
    )
    wrapper = getattr(SailConnectionManager, f"_open_{mode}")(credentials)
    remote_url = f"sc://127.0.0.1:{port}/"
    if auth.get("use_ssl"):
        remote_url += ";use_ssl=true"
    if "token" in auth:
        remote_url += f";token={auth['token']}"
    assert isinstance(wrapper, SailSessionConnectionWrapper)
    assert wrapper.handle.server_side_parameters == {
        **parameters,
        "spark.remote": remote_url if mode == "remote" else "sc://127.0.0.1:12345",
    }
    assert credentials.server_side_parameters == parameters
