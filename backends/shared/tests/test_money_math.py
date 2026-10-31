"""money_math: exact decimal arithmetic for runtime money (quantities, prices, fees, funding, balances, PnL).

Every float comparison below names the float artifact the exact path removes; every exact result is compared with
an independent Decimal reference computed at high precision.
"""
from __future__ import annotations

import math
import os
import random
import struct
import sys
from decimal import Decimal, Inexact, localcontext

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from shared_lib.core import money_math as M  # noqa: E402


def reference_sum(values):
    with localcontext() as ctx:
        ctx.prec = 400
        return sum((Decimal(str(v)) for v in values), Decimal(0))


# ════════════════════════════════════════════════════════════════════════════════════════════ conversion
@pytest.mark.parametrize("value, expected", [
    ("0.1", Decimal("0.1")), ("-0.00012345", Decimal("-0.00012345")), (" 12.5 ", Decimal("12.5")),
    (0.1, Decimal("0.1")), (0.1 + 0.2, Decimal("0.30000000000000004")), (7, Decimal(7)),
    (Decimal("1.230"), Decimal("1.230")), ("1e-8", Decimal("1E-8")), (5e-324, Decimal("5E-324")),
])
def test_to_dec_is_exact(value, expected):
    out = M.to_dec(value)
    assert out == expected and str(out) == str(expected)


@pytest.mark.parametrize("bad", ["nan", "NaN", "inf", "-Infinity", float("nan"), float("inf"), Decimal("NaN"),
                                 "abc", "1,5", True, object()])
def test_to_dec_refuses_what_is_not_a_finite_decimal(bad):
    with pytest.raises(M.MoneyValueError):
        M.to_dec(bad)
    assert not M.is_finite_money(bad)


def test_missing_values_need_an_explicit_default():
    for missing in (None, "", "   "):
        with pytest.raises(M.MoneyValueError):
            M.to_dec(missing)
        assert M.to_dec(missing, default=M.ZERO) == 0


def test_float_round_trip_is_identity_over_random_bit_patterns():
    """``to_float(to_dec(x)) == x`` for every finite binary64: converting once never changes a float."""
    rng = random.Random(20261031)
    seen = 0
    while seen < 50_000:
        x = struct.unpack("<d", rng.getrandbits(64).to_bytes(8, "little"))[0]
        if not math.isfinite(x):
            continue
        seen += 1
        d = M.to_dec(x)
        assert M.to_float(d) == x or (x == 0 and M.to_float(d) == 0)
        assert Decimal(repr(x)) == d


def test_decimal_text_round_trip():
    for text in ("0", "0.00000001", "123456789.12345678", "-42.5", "1000000000000.000000000001"):
        assert M.to_dec(M.money_text(text)) == Decimal(text)
    assert M.money_text(Decimal("1E+1")) == "10" and M.money_text("-0.000") == "0"
    assert M.money_text("0.00012300") == "0.000123" and M.money_text(1e-8) == "0.00000001"


# ════════════════════════════════════════════════════════════════════════════════════════════ accumulation
def test_ten_thousand_fees_of_a_tenth_sum_exactly():
    running = 0.0
    for _ in range(10_000):
        running += 0.1
    assert running == 1000.0000000001588                  # the float artifact
    assert M.dsum(["0.1"] * 10_000) == Decimal("1000")
    assert M.dsum([0.1] * 10_000) == Decimal("1000")      # floats parsed from "0.1" stand for 0.1
    assert M.fsum_exact(["0.1"] * 10_000) == 1000.0


def test_repeated_running_total_through_float_storage_stays_exact():
    """A stored running total (float column), updated 10,000 times with the exact helper, never drifts."""
    stored = 0.0
    for _ in range(10_000):
        stored = M.to_float(M.dsum((stored, "0.1")))
    assert stored == 1000.0


def test_sum_that_floats_put_on_the_wrong_side_of_a_threshold():
    assert sum([0.1, 0.7]) < 0.8                           # compensated float sum is still below
    assert M.dsum([0.1, 0.7]) >= M.to_dec(0.8)
    assert M.dsum(["0.1", "0.2"]) == Decimal("0.3") and 0.1 + 0.2 != 0.3


def test_very_small_balances_and_fractional_quantities():
    dust = ["0.00000001"] * 123
    assert M.dsum(dust) == Decimal("0.00000123")
    qty = ["0.001", "0.002", "0.003", "0.0005", "0.0005"]
    assert M.dsum(qty) == Decimal("0.007")
    assert M.dsub("0.00000003", "0.00000002") == Decimal("0.00000001")


def test_large_notionals_keep_every_digit():
    with localcontext() as ctx:
        ctx.prec = 100
        expected = Decimal("98765432109.12345678") * Decimal("123456789.123456789")
    assert M.mul("98765432109.12345678", "123456789.123456789") == expected   # no digit rounded
    assert len(expected.as_tuple().digits) > 28                       # more than the default Decimal context
    assert M.dsum(["1000000000000", "0.00000001"]) == Decimal("1000000000000.00000001")
    assert (1e12 + 1e-8) - 1e12 == 0.0                                 # the float artifact: the dust is gone


def test_negative_amounts_pnl_and_funding():
    funding = ["-0.01234", "0.00500", "-0.00266"]
    assert M.dsum(funding) == Decimal("-0.01")
    pnl = ["12.5", "-13.75", "1.25"]
    assert M.dsum(pnl) == 0
    assert M.dsub("-1", "-1.5") == Decimal("0.5")


def test_random_fee_streams_match_the_reference():
    rng = random.Random(7)
    fees = [format(Decimal(rng.randrange(1, 10 ** 10)) / Decimal(10 ** 8), "f") for _ in range(5_000)]
    assert M.dsum(fees) == reference_sum(fees)
    assert M.fsum_exact(fees) == float(reference_sum(fees))


def test_reconciliation_sums_balance_exactly():
    """Opening wallet + every income event == closing wallet, exactly, whatever the order of the events."""
    rng = random.Random(11)
    events = [format(Decimal(rng.randrange(-10 ** 9, 10 ** 9)) / Decimal(10 ** 8), "f") for _ in range(2_000)]
    opening = Decimal("10000.12345678")
    closing = M.dsum(events, start=opening)
    shuffled = events[:]
    rng.shuffle(shuffled)
    assert M.dsum(shuffled, start=opening) == closing
    assert M.dsub(closing, M.dsum(events)) == opening


# ════════════════════════════════════════════════════════════════════════════════════════════ rounding
@pytest.mark.parametrize("value, step, down, up", [
    ("1.23456", "0.001", "1.234", "1.235"),
    ("0.3", "0.1", "0.3", "0.3"),                         # float: 0.3 / 0.1 = 2.9999999999999996 -> floor 0.2
    ("0.00999", "0.001", "0.009", "0.010"),
    ("65000.55", "0.5", "65000.5", "65001.0"),
    ("-1.2345", "0.01", "-1.24", "-1.23"),
    ("7", "5", "5", "10"),
    ("0.9", "0.3", "0.9", "0.9"),
])
def test_step_rounding_direction(value, step, down, up):
    assert M.floor_to_step(value, step) == Decimal(down)
    assert M.ceil_to_step(value, step) == Decimal(up)
    assert M.floor_to_step(value, step) <= Decimal(value) <= M.ceil_to_step(value, step)


def test_floor_to_step_fixes_the_float_floor_artifact():
    assert math.floor(0.3 / 0.1) * 0.1 == pytest.approx(0.2)          # the float artifact: one step too low
    assert M.floor_to_step(0.3, 0.1) == Decimal("0.3")


def test_step_must_be_positive():
    with pytest.raises(M.MoneyValueError):
        M.floor_to_step("1", "0")


def test_division_rounds_once_and_only_division_rounds():
    assert M.div("1", "3") == Decimal("0." + "3" * 50)
    with pytest.raises(ZeroDivisionError):
        M.div("1", "0")
    with M.exact():
        with pytest.raises(Inexact):                   # add/sub/mul never round silently
            Decimal("1E+300") + Decimal("1E-300")
        assert Decimal("0.1") * Decimal("0.2") == Decimal("0.02")


# ════════════════════════════════════════════════════════════════════════════════════════ float interface
def test_float_interface_helpers_return_the_nearest_float_of_the_exact_result():
    assert M.f_add(0.1, 0.2) == 0.3 and 0.1 + 0.2 != 0.3
    assert M.f_sub(0.3, 0.1) == 0.2 and 0.3 - 0.1 != 0.2
    assert M.f_mul(0.1, 3) == 0.3 and 0.1 * 3 != 0.3
    assert M.f_div(0.3, 0.1) == 3.0 and 0.3 / 0.1 != 3.0
    assert M.f_sum([0.1] * 7) == 0.7 and sum([0.1] * 7) != 0.7
    assert M.f_sum([]) == 0.0


def test_float_interface_helpers_keep_float_semantics_for_non_finite_operands():
    nan, inf = float("nan"), float("inf")
    assert math.isnan(M.f_add(nan, 1.0)) and M.f_mul(inf, 2.0) == inf and math.isnan(M.f_sum([1.0, nan]))
    assert M.f_sub(inf, 1.0) == inf and math.isnan(M.f_div(nan, 2.0))
    with pytest.raises(ZeroDivisionError):
        M.f_div(1.0, 0.0)
