"""SQL literals for dbt-spark's session binding protocol."""

import datetime as dt
import math
from decimal import Decimal
from typing import Any

from dbt.adapters.spark.session import SessionConnectionWrapper
from dbt_common.exceptions import DbtRuntimeError


def _string_literal(value: str) -> str:
    # Hex avoids dependence on spark.sql.parser.escapedStringLiterals, including
    # for quotes, backslashes and control characters. Spark strings are UTF-8.
    return f"CAST(X'{value.encode('utf-8').hex()}' AS STRING)"


def _decimal_literal(value: Decimal) -> str:
    if not value.is_finite():
        raise DbtRuntimeError("Non-finite Decimal SQL binding is not supported")
    if value.is_zero():
        return "0"

    sign, digits, exponent = value.as_tuple()
    exponent = int(exponent)
    # Remove redundant zeros without normalize(), which can round according to
    # the active Decimal context. Check precision BEFORE fixed-point rendering.
    end = len(digits)
    while digits[end - 1] == 0:
        end -= 1
    exponent += len(digits) - end
    digits = digits[:end]
    scale = max(-exponent, 0)
    precision = max(len(digits) + exponent, 0) + scale
    if precision <= 38:
        return format(Decimal((sign, digits, exponent)), "f")

    # agate uses Decimal for inferred DOUBLE columns too. Outside decimal(38)'s
    # domain use approximate binary64 semantics; reject overflow and nonzero
    # values that underflow to zero rather than silently erasing them. The seed
    # macro supplies the target CAST; arbitrary-precision decimal casts are not
    # guaranteed by this target-independent fallback.
    approximate = float(value)
    if not math.isfinite(approximate) or approximate == 0:
        raise DbtRuntimeError("Decimal SQL binding is outside the finite nonzero DOUBLE range")
    return f"{approximate!r}D"


class SailSessionConnectionWrapper(SessionConnectionWrapper):
    @classmethod
    def _fix_binding(cls, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, int):
            return _decimal_literal(Decimal(value))
        if isinstance(value, Decimal):
            return _decimal_literal(value)
        if isinstance(value, float):
            if not math.isfinite(value):
                raise DbtRuntimeError("Non-finite float SQL binding is not supported")
            # A DOUBLE literal also preserves the sign of negative zero.
            return f"{value!r}D"
        if isinstance(value, dt.datetime):
            return _string_literal(value.isoformat(sep=" ", timespec="microseconds"))
        if isinstance(value, (dt.date, dt.time)):
            return _string_literal(value.isoformat())
        if isinstance(value, str):
            return _string_literal(value)
        raise DbtRuntimeError(f"Unsupported SQL binding type: {type(value).__name__}")
