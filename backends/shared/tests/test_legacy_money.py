"""legacy_money: the exact conversion of legacy SQLite REAL values to Decimal (Step 5.1, EXT-DB-01).

Property-style without a dependency: a fixed-seed generator draws binary64 values from random BIT PATTERNS
(every exponent, subnormals included) plus a hand-picked set of edge values, so the run is reproducible.
"""
from __future__ import annotations

import math
import os
import random
import struct
import sys
from decimal import Decimal

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from shared_lib.persistence import legacy_money as LM  # noqa: E402

EDGE = [0.0, -0.0, 5e-324, -5e-324, 2.2250738585072014e-308, 2.225073858507201e-308, 1.7976931348623157e308,
        -1.7976931348623157e308, 0.1, 0.2, 0.1 + 0.2, 1e-7, 1e-8, 1e-18, 1e-19, 123456789.123456789, 2 ** 53,
        2 ** 53 + 2.0, 9007199254740993.0, 1e20, 1e21, 99999999999999999999.0, 0.30000000000000004, 1 / 3, 2 / 3,
        65000.5, 0.00012345, 1e15 + 0.3, 4.35, 2.675, 1.005, 0.07, 3.3333333333333335]


def random_finite_floats(n: int, seed: int = 20261029):
    rng = random.Random(seed)
    out = []
    while len(out) < n:
        x = struct.unpack("<d", rng.getrandbits(64).to_bytes(8, "little"))[0]
        if math.isfinite(x):
            out.append(x)
    return out


def money_like_floats(n: int, seed: int = 7):
    """Values shaped like real money: prices, fractional quantities, tiny fees, sums of them."""
    rng = random.Random(seed)
    out = []
    for _ in range(n):
        kind = rng.randrange(5)
        if kind == 0:
            out.append(round(rng.uniform(0, 100_000), rng.randrange(0, 9)))
        elif kind == 1:
            out.append(rng.randrange(1, 10 ** 8) * 10 ** -rng.randrange(0, 12))
        elif kind == 2:
            out.append(sum(rng.uniform(0, 1) * 1e-4 for _ in range(rng.randrange(1, 50))))
        elif kind == 3:
            out.append(rng.uniform(-1e9, 1e9))
        else:
            out.append(rng.random())
    return out


# ════════════════════════════════════════════════════════════════════════════════════════ round trip
@pytest.mark.parametrize("x", EDGE)
def test_edge_floats_round_trip_exactly(x):
    d = LM.to_decimal(x)
    assert float(d) == x
    assert math.copysign(1.0, float(d)) == math.copysign(1.0, x)          # -0.0 keeps its sign in Decimal
    assert d == Decimal(repr(x))


def test_every_finite_binary64_round_trips_property():
    """200,000 random bit patterns (every exponent, subnormals and huge values) and 50,000 money-shaped values:
    ``float(to_decimal(x)) == x`` for all, and the Decimal is the SHORTEST round-tripping decimal."""
    values = random_finite_floats(200_000) + money_like_floats(50_000)
    subnormal = huge = 0
    for x in values:
        d = LM.to_decimal(x)
        assert float(d) == x, x
        assert len(d.as_tuple().digits) <= 17
        subnormal += 0 < abs(x) < 2.2250738585072014e-308
        huge += abs(x) > 1e300
    assert subnormal > 0 and huge > 0                                       # the generator reached both ends


def test_conversion_is_deterministic_and_does_not_invent_digits():
    assert LM.to_decimal(0.1) == Decimal("0.1") and str(LM.to_decimal(0.1)) == "0.1"
    assert LM.to_decimal(0.1 + 0.2) == Decimal("0.30000000000000004")
    assert Decimal(0.1) != LM.to_decimal(0.1)                               # the exact binary expansion is not money
    for x in money_like_floats(1000, seed=3):
        assert LM.to_decimal(x).as_tuple() == LM.to_decimal(x).as_tuple()


def test_integers_are_exact_and_text_is_validated():
    assert LM.to_decimal(0) == 0 and LM.to_decimal(10 ** 30) == Decimal(10 ** 30)
    assert LM.to_decimal(-(2 ** 63)) == Decimal(-(2 ** 63))
    assert LM.to_decimal("0.00000001") == Decimal("1E-8")
    assert LM.to_decimal(" 65000.5 ") == Decimal("65000.5")
    assert LM.to_decimal("1e-7") == Decimal("1E-7") and LM.to_decimal("-.5") == Decimal("-0.5")
    assert LM.to_decimal(Decimal("12.3400")) == Decimal("12.3400")
    assert LM.to_decimal(None) is None


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), "NaN", "inf", "-Infinity", "sNaN",
                                 Decimal("NaN"), Decimal("Infinity")])
def test_non_finite_money_is_refused(bad):
    with pytest.raises(LM.NonFiniteMoney):
        LM.to_decimal(bad)


@pytest.mark.parametrize("bad", ["", "abc", "1,5", "1_000", "0x10", "12 USD", b"1.0", True, False, [1], object()])
def test_non_numeric_values_are_type_mismatches(bad):
    with pytest.raises(LM.MoneyTypeMismatch):
        LM.to_decimal(bad)
    assert issubclass(LM.MoneyTypeMismatch, LM.LegacyMoneyError)


# ════════════════════════════════════════════════════════════════════════════════════════ NUMERIC(38,18) fit
def test_numeric_misfit_reports_instead_of_rounding():
    assert LM.numeric_misfit(Decimal("0.123456789012345678")) is None                 # exactly 18 fractional
    assert LM.numeric_misfit(Decimal("1.50000000000000000000000")) is None             # trailing zeros
    assert LM.numeric_misfit(Decimal("99999999999999999999.999999999999999999")) is None
    assert LM.numeric_misfit(Decimal("0.1234567890123456789")).startswith("FRACTIONAL_DIGITS:19")
    assert LM.numeric_misfit(Decimal("123456789012345678901")).startswith("INTEGER_DIGITS:21")
    assert LM.numeric_misfit(Decimal("1E+20")).startswith("INTEGER_DIGITS:21")
    assert LM.numeric_misfit(LM.to_decimal(5e-324)).startswith("FRACTIONAL_DIGITS")
    assert LM.numeric_misfit(None) is None and LM.numeric_misfit(Decimal(0)) is None
    with pytest.raises(LM.MoneyPrecisionLoss):
        LM.to_numeric_exact(1.23456789012345e-07)                                       # 21 fractional digits
    assert LM.to_numeric_exact(0.1 + 0.2) == Decimal("0.30000000000000004")


def test_decimal_text_is_plain_notation():
    assert LM.decimal_text(Decimal("1E-7")) == "0.0000001"
    assert LM.decimal_text(Decimal("1.2E+3")) == "1200"
    assert LM.decimal_text(None) is None


# ════════════════════════════════════════════════════════════════════════════════════════ classification
def test_classification_maps_are_disjoint_and_well_formed():
    assert LM.PG_MONEY_TYPE == "NUMERIC" and LM.PG_NON_MONEY_TYPE == "DOUBLE PRECISION"
    assert LM.PLATFORM_MONEY_TYPE == "NUMERIC(38,18)"
    for table in set(LM.MONETARY_COLUMNS) & set(LM.NON_MONETARY_REAL_COLUMNS):
        assert not LM.MONETARY_COLUMNS[table] & LM.NON_MONETARY_REAL_COLUMNS[table], table
    for mapping in (LM.MONETARY_COLUMNS, LM.NON_MONETARY_REAL_COLUMNS):
        for table, cols in mapping.items():
            assert isinstance(cols, frozenset) and cols, table
    with pytest.raises(TypeError):
        LM.MONETARY_COLUMNS["x"] = frozenset({"y"})                                     # read-only view


def test_classification_answers_and_refuses_unknown_columns():
    assert LM.classify_real_column("trade_fills", "fee") == LM.MONETARY
    assert LM.classify_real_column("cati_reservation_ledger", "notional") == LM.MONETARY
    assert LM.classify_real_column("cati_instrument_rules", "price_tick") == LM.MONETARY
    assert LM.classify_real_column("trade_fills", "r_multiple") == LM.NON_MONETARY
    assert LM.classify_real_column("cati_feed_funding", "rate") == LM.NON_MONETARY
    assert LM.postgres_real_type("positions", "realized_pnl") == "NUMERIC"
    assert LM.postgres_real_type("positions", "leverage") == "DOUBLE PRECISION"
    with pytest.raises(LM.UnclassifiedRealColumn):
        LM.classify_real_column("trade_fills", "a_new_real_column")
    assert LM.is_real_type("REAL") and LM.is_real_type("double precision") and LM.is_real_type("FLOAT")
    assert not LM.is_real_type("INTEGER") and not LM.is_real_type("TEXT") and not LM.is_real_type(None)
