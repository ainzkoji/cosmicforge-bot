"""Exact decimal arithmetic for runtime money: quantities, prices, fees, funding, balances, notional, margin, PnL.

Why this exists. A venue sends money as decimal text (``"-0.01234"``); parsing every value with ``float()`` and
adding the floats accumulates binary rounding error: ten thousand fees of ``0.1`` added one at a time (a stored
running total) come to ``1000.0000000001588``, and even a compensated ``sum([0.1, 0.7])`` is ``0.7999999999999999``,
which is *less than* ``0.8`` (it is the exact sum of the two binary approximations). A daily-loss latch,
an "is the exit complete" test or a report total built that way can be wrong at the boundary.

The rule used with this module:

* **Convert once, at the boundary**, with :func:`to_dec`. Text and ints are exact. A ``float`` becomes
  ``Decimal(repr(x))``: its shortest round-trip decimal, which is the decimal the float was parsed from (a venue
  string) or stored as (a legacy REAL column), the same convention as ``shared_lib.persistence.legacy_money``.
* **Compute in Decimal** under :func:`exact` (or with :func:`dsum`), a context wide enough that adding, subtracting
  and multiplying money never rounds, and that *raises* if it ever would (``Inexact`` is trapped).
* **Round only where a venue or a report defines precision**: :func:`floor_to_step` / :func:`ceil_to_step` for
  exchange steps and ticks (direction stays the caller's choice), :func:`div` for an average or a ratio (the one
  operation that is inherently inexact; it rounds half-even at 50 significant digits).
* **Return the caller's type.** :func:`to_float` converts the exact result once, so a float caller receives the
  correctly rounded value of the exact result instead of an accumulation of rounding errors.

Nothing here changes a stored value: storage is the persistence layer's job (``legacy_money``, ``decimal_in``).
"""
from __future__ import annotations

import math
from contextlib import contextmanager
from decimal import (
    ROUND_CEILING,
    ROUND_FLOOR,
    ROUND_HALF_EVEN,
    Context,
    Decimal,
    DivisionByZero,
    Inexact,
    InvalidOperation,
    Overflow,
    localcontext,
)
from typing import Any, Iterable, Iterator, Optional

ZERO = Decimal(0)

#: Add / subtract / multiply on money never round under this context; if one ever would, ``Inexact`` is raised.
#: 200 significant digits hold the sum of any finite money values from 1e-30 to 1e30 and their products.
_EXACT = Context(prec=200, rounding=ROUND_HALF_EVEN, Emin=-999999, Emax=999999,
                 traps=[InvalidOperation, DivisionByZero, Overflow, Inexact])
#: Division (averages, ratios) is inherently inexact: 50 significant digits, half-even, far below a float's 17.
_DIVIDE = Context(prec=50, rounding=ROUND_HALF_EVEN, Emin=-999999, Emax=999999,
                  traps=[InvalidOperation, DivisionByZero, Overflow])


class MoneyValueError(ValueError):
    """A value that is not a finite decimal number. Never coerced to zero or rounded silently."""


def to_dec(value: Any, *, default: Optional[Decimal] = None) -> Decimal:
    """The exact Decimal of a runtime money value.

    ``None`` and empty text give ``default`` when one is given (``to_dec(x, default=ZERO)`` mirrors the old
    ``float(x or 0)``) and raise otherwise. ``bool`` is refused (an ``int`` subclass, never money). Non-finite values
    (NaN, infinity) raise :class:`MoneyValueError`; callers that must fail closed with their own reason code check
    :func:`is_finite_money` first or catch it."""
    if value is None or (isinstance(value, str) and not value.strip()):
        if default is None:
            raise MoneyValueError("MONEY_VALUE_MISSING")
        return default
    if isinstance(value, bool):
        raise MoneyValueError("MONEY_VALUE_BOOL")
    if isinstance(value, Decimal):
        out = value
    elif isinstance(value, float):
        out = Decimal(repr(value))
    elif isinstance(value, int):
        out = Decimal(value)
    elif isinstance(value, str):
        try:
            out = Decimal(value.strip())
        except InvalidOperation as exc:
            raise MoneyValueError(f"MONEY_VALUE_NOT_DECIMAL:{value[:40]!r}") from exc
    else:
        raise MoneyValueError(f"MONEY_VALUE_TYPE:{type(value).__name__}")
    if not out.is_finite():
        raise MoneyValueError("MONEY_VALUE_NOT_FINITE")
    return out


def is_finite_money(value: Any) -> bool:
    """True when :func:`to_dec` accepts ``value`` (``None`` and empty text count as not finite)."""
    try:
        to_dec(value)
    except MoneyValueError:
        return False
    return True


@contextmanager
def exact() -> Iterator[Context]:
    """Decimal arithmetic inside the block is exact for add, subtract and multiply (``Inexact`` raises)."""
    with localcontext(_EXACT) as ctx:
        yield ctx


def dsum(values: Iterable[Any], start: Any = ZERO) -> Decimal:
    """The exact sum of ``values`` (each converted with :func:`to_dec`; ``None`` / empty count as zero)."""
    with localcontext(_EXACT):
        total = to_dec(start)
        for value in values:
            total += to_dec(value, default=ZERO)
        return total


def dsub(a: Any, b: Any) -> Decimal:
    """The exact difference ``a - b`` (``None`` / empty count as zero)."""
    with localcontext(_EXACT):
        return to_dec(a, default=ZERO) - to_dec(b, default=ZERO)


def mul(a: Any, b: Any) -> Decimal:
    """The exact product of two money values (for example quantity x price = notional)."""
    with localcontext(_EXACT):
        return to_dec(a) * to_dec(b)


def div(numerator: Any, denominator: Any) -> Decimal:
    """``numerator / denominator`` at 50 significant digits, half-even. A zero denominator raises."""
    with localcontext(_DIVIDE):
        return to_dec(numerator) / to_dec(denominator)


def to_float(value: Any) -> float:
    """One correctly rounded conversion of an exact result to the float a caller expects."""
    return float(to_dec(value))


def fsum_exact(values: Iterable[Any]) -> float:
    """``to_float(dsum(values))``: the float nearest the exact sum."""
    return float(dsum(values))


# ── float in, float out ─────────────────────────────────────────────────────────────────────────────────────
# For modules whose interface is float (dataclasses, EPSILON comparisons) and stays float: each result is computed
# exactly from the operands' decimals and returned as the nearest float, so a chain of operations carries no
# accumulated binary error. A non-finite operand keeps plain float semantics (NaN or infinity, as before), so no new
# exception reaches an order path; the gates that refuse non-finite money stay where they are.
def _finite(*values: Any) -> bool:
    return all(math.isfinite(float(v)) for v in values)


def f_add(a: Any, b: Any) -> float:
    return float(dsum((a, b))) if _finite(a, b) else float(a) + float(b)


def f_sub(a: Any, b: Any) -> float:
    return float(dsub(a, b)) if _finite(a, b) else float(a) - float(b)


def f_mul(a: Any, b: Any) -> float:
    return float(mul(a, b)) if _finite(a, b) else float(a) * float(b)


def f_div(a: Any, b: Any) -> float:
    """``a / b``; a zero divisor raises ``ZeroDivisionError`` as float division does."""
    if not _finite(a, b) or float(b) == 0:
        return float(a) / float(b)
    return float(div(a, b))


def f_sum(values: Iterable[Any]) -> float:
    values = [float(v) for v in values]
    return float(dsum(values)) if _finite(*values) else sum(values)


def floor_to_step(value: Any, step: Any) -> Decimal:
    """The largest multiple of ``step`` not above ``value`` (a quantity rounded DOWN to the lot step)."""
    return _to_step(value, step, ROUND_FLOOR)


def ceil_to_step(value: Any, step: Any) -> Decimal:
    """The smallest multiple of ``step`` not below ``value``."""
    return _to_step(value, step, ROUND_CEILING)


def _to_step(value: Any, step: Any, rounding: str) -> Decimal:
    v, s = to_dec(value), to_dec(step)
    if s <= 0:
        raise MoneyValueError("MONEY_STEP_NOT_POSITIVE")
    with localcontext(_EXACT):
        # ``//`` is exact integer division, truncated toward zero; the remainder decides the direction.
        q = v // s
        if q * s != v:
            if rounding == ROUND_FLOOR and v < 0:
                q -= 1
            elif rounding == ROUND_CEILING and v > 0:
                q += 1
        return q * s


def money_text(value: Any) -> str:
    """Plain decimal text, no exponent (``Decimal('1E+1')`` -> ``'10'``), for reports and decimal storage."""
    d = to_dec(value)
    text = format(d, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in ("-0", "") else text


__all__ = ["ZERO", "MoneyValueError", "to_dec", "is_finite_money", "exact", "dsum", "dsub", "mul", "div", "to_float",
           "fsum_exact", "f_add", "f_sub", "f_mul", "f_div", "f_sum", "floor_to_step", "ceil_to_step",
           "money_text"]
