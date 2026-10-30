"""Financial correctness of the legacy money move: SQLite REAL -> ``to_decimal`` -> PostgreSQL NUMERIC(38,18) -> back
(Step 5.1, EXT-DB-01).

Every scenario produces its values the way the legacy runtime does today -- binary-float arithmetic stored in a SQLite
``REAL`` column -- and proves that the stored float survives the move exactly: the Decimal read back from PostgreSQL
equals ``to_decimal`` of the stored float, converts back to the very same float, and ``SUM`` in NUMERIC equals the
exact Decimal sum. It also proves that PostgreSQL's own ``float8 -> numeric`` cast would NOT preserve them, which is
why the migration binds ``Decimal(repr(x))`` instead of casting floats on the server.

The PostgreSQL half needs ``PLATFORM_TEST_POSTGRES_URL`` (skipped without it, a failure with
``PLATFORM_TEST_POSTGRES_REQUIRED=1``). Nothing here changes runtime arithmetic.
"""
from __future__ import annotations

import math
import random
import sqlite3
import uuid
from decimal import Decimal, localcontext

import pytest

from _pg_server import PG_REQUIRED, PG_URL, _pg_admin, _pg_url
from shared_lib.persistence import legacy_money as LM


@pytest.fixture(scope="module")
def pg():
    if not PG_URL:
        if PG_REQUIRED:
            pytest.fail("PLATFORM_TEST_POSTGRES_REQUIRED=1 but PLATFORM_TEST_POSTGRES_URL is not set")
        pytest.skip("PLATFORM_TEST_POSTGRES_URL not set")
    import psycopg
    name = f"cftest_money_{uuid.uuid4().hex[:12]}"
    _pg_admin(f"CREATE DATABASE {name}")
    conn = psycopg.connect(_pg_url(name), autocommit=True)
    try:
        yield conn
    finally:
        conn.close()
        _pg_admin(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


def exact_sum(values):
    with localcontext() as ctx:
        ctx.prec = 80                                   # never round while summing
        return sum(values, Decimal(0))


# ════════════════════════════════════════════════════════════════════════════ scenarios (legacy float arithmetic)
def fractional_quantities(rng):
    step = 0.001
    return [round(rng.randrange(1, 100_000) * step, 3) for _ in range(200)] + [0.1 + 0.2, 0.001 * 3, 1e-08, 0.00001]


def fee_accumulation(rng):
    out, total = [], 0.0
    for _ in range(300):
        fee = rng.uniform(1, 500) * 0.0004            # taker fee on a notional, as trade_fills.fee is computed
        total += fee                                    # running float total, as strategy_performance.fees is
        out += [fee, total]
    return out


def funding(rng):
    out, acc = [], 0.0
    for _ in range(200):
        payment = rng.uniform(-1, 1) * rng.uniform(100, 50_000) * 0.0001    # notional * funding rate, signed
        acc += payment
        out += [payment, acc]
    return out


def margin_and_notional(rng):
    out = []
    for _ in range(200):
        qty, price, lev = rng.randrange(1, 10_000) * 0.001, round(rng.uniform(0.01, 90_000), 2), rng.choice([1, 2, 3, 5, 10, 20])
        notional = qty * price
        out += [notional, notional / lev]
    return out


def reservations(rng):
    out = []
    for _ in range(200):
        equity, risk, entry = rng.uniform(100, 1e6), 0.004, rng.uniform(1, 70_000)
        stop = entry * (1 - rng.uniform(0.005, 0.15))
        risk_amount = equity * risk
        out += [risk_amount, risk_amount / (entry - stop), stop]
    return out


def rounding_traps(_rng):
    return [2.675, 1.005, 0.015, 4.35, 0.285, 1.115, 8.345, 0.07 * 3, 1.1 * 1.1, 100.0 / 3, 2 / 3, 0.1 * 3,
            round(2.675, 2), round(1.005, 2), 1e-18, 9.99999999999999e19]


def pnl(rng):
    out, realized = [], 0.0
    for _ in range(200):
        qty, entry = rng.randrange(1, 5_000) * 0.001, rng.uniform(0.5, 80_000)
        exit_ = entry * (1 + rng.uniform(-0.1, 0.1))
        gross = (exit_ - entry) * qty
        fees = (entry + exit_) * qty * 0.0004
        realized += gross - fees
        out += [gross, gross - fees, realized]
    return out


def exchange_precision(rng):
    out = []
    for tick, step in ((0.01, 0.001), (0.1, 0.01), (0.0001, 1.0), (0.00001, 0.1), (0.5, 0.001)):
        for _ in range(40):
            price = math.floor(rng.uniform(0.01, 90_000) / tick) * tick      # floor-to-tick in float, as legacy code does
            qty = math.floor(rng.uniform(0.001, 1_000) / step) * step
            out += [tick, step, price, qty]
    return out


def account_equity(rng):
    out = []
    for _ in range(200):
        wallet = rng.uniform(10, 2e6)
        unrealized = rng.uniform(-0.2, 0.2) * wallet
        out += [wallet, unrealized, wallet + unrealized, max(0.0, wallet + unrealized - abs(unrealized) * 0.5)]
    return out


SCENARIOS = {f.__name__: f for f in (fractional_quantities, fee_accumulation, funding, margin_and_notional,
                                     reservations, rounding_traps, pnl, exchange_precision, account_equity)}


def through_sqlite_real(values):
    """Store and read back through a SQLite REAL column, as the legacy schema holds them."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE m (id INTEGER PRIMARY KEY, v REAL)")
    conn.executemany("INSERT INTO m (v) VALUES (?)", [(v,) for v in values])
    out = [r[0] for r in conn.execute("SELECT v FROM m ORDER BY id")]
    conn.close()
    assert out == values                                # SQLite REAL is binary64: nothing changed yet
    return out


#: values of each scenario that NUMERIC(38,18) cannot hold exactly (more than 18 fractional digits): small results
#: of float arithmetic carry 17 significant digits, so a fee of 0.0063 computed in float has 19 fractional digits.
EXPECTED_38_18_MISFITS = {"fee_accumulation": True, "reservations": True}


def _round_trip(pg, table, column_type, decimals):
    pg.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, v {column_type})")
    with pg.cursor() as cur:
        cur.executemany(f"INSERT INTO {table} (id, v) VALUES (%s, %s)", list(enumerate(decimals)))
    back = [r[0] for r in pg.execute(f"SELECT v FROM {table} ORDER BY id").fetchall()]
    total = pg.execute(f"SELECT SUM(v) FROM {table}").fetchone()[0]
    return back, total


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
def test_scenario_survives_numeric_round_trip_exactly(pg, scenario):
    """Every stored float -> to_decimal -> NUMERIC -> back is the same Decimal and the same float, and SUM in
    NUMERIC equals the exact Decimal sum. In NUMERIC(38,18) for every value it can hold; the values it cannot hold
    are REPORTED by ``numeric_misfit`` (never rounded) and are exact in unconstrained NUMERIC."""
    stored = through_sqlite_real(SCENARIOS[scenario](random.Random(scenario)))
    decimals = [LM.to_decimal(v) for v in stored]
    fits = [d for d in decimals if LM.numeric_misfit(d) is None]
    misfits = [d for d in decimals if LM.numeric_misfit(d) is not None]
    assert all(LM.numeric_misfit(d).startswith("FRACTIONAL_DIGITS") for d in misfits)   # tiny values, never huge
    assert bool(misfits) == EXPECTED_38_18_MISFITS.get(scenario, False), (scenario, len(misfits), misfits[:3])

    back, total = _round_trip(pg, f"m38_{scenario}", "NUMERIC(38,18)", fits)
    assert back == fits and [float(d) for d in back] == [float(d) for d in fits]
    assert all(isinstance(d, Decimal) for d in back)
    assert total == exact_sum(fits)

    back, total = _round_trip(pg, f"mx_{scenario}", "NUMERIC", decimals)
    assert back == decimals                                             # Decimal equality, exact
    assert [float(d) for d in back] == stored                           # and the very same binary64 again
    assert total == exact_sum(decimals)                                 # NUMERIC sum == exact Decimal sum
    for d in misfits:                                                   # what NUMERIC(38,18) would have done
        rounded = pg.execute("SELECT %s::numeric(38,18)", (d,)).fetchone()[0]
        assert rounded != d


def test_unconstrained_numeric_holds_every_finite_binary64(pg):
    """Random bit patterns, subnormals and the largest doubles: unconstrained NUMERIC is exact for all of them."""
    import struct
    rng = random.Random(5)
    values = [5e-324, 1.7976931348623157e308, -2.2250738585072014e-308]
    while len(values) < 3000:
        x = struct.unpack("<d", rng.getrandbits(64).to_bytes(8, "little"))[0]
        if math.isfinite(x):
            values.append(x)
    decimals = [LM.to_decimal(v) for v in values]
    back, _ = _round_trip(pg, "m_bits", "NUMERIC", decimals)
    assert back == decimals and [float(d) for d in back] == values


def test_sums_in_numeric_are_exact_where_float_sums_drift(pg):
    fees = [0.1] * 10 + [0.2] * 10 + [1e-8] * 1000
    float_total = 0.0
    for f in fees:
        float_total += f
    decimals = [LM.to_decimal(f) for f in fees]
    pg.execute("CREATE TABLE fee_sum (id BIGINT PRIMARY KEY, v NUMERIC(38,18))")
    with pg.cursor() as cur:
        cur.executemany("INSERT INTO fee_sum VALUES (%s, %s)", list(enumerate(decimals)))
    numeric_total = pg.execute("SELECT SUM(v) FROM fee_sum").fetchone()[0]
    assert numeric_total == exact_sum(decimals) == Decimal("3.00001")
    assert Decimal(repr(float_total)) != Decimal("3.00001")             # the legacy float total has drifted


def test_postgres_float8_to_numeric_cast_loses_digits(pg):
    """Why the migration must bind ``Decimal(repr(x))``: the server's own cast rounds to 15 significant digits."""
    values = [0.1 + 0.2, 123456789.12345679, 1234567890123.4568, 3.3333333333333335, 27000.123456789012]
    lost = []
    for v in values:
        cast = pg.execute("SELECT %s::float8::numeric", (v,)).fetchone()[0]
        bound = pg.execute("SELECT %s::numeric(38,18)", (LM.to_decimal(v),)).fetchone()[0]
        assert bound == LM.to_decimal(v) and float(bound) == v
        if cast != LM.to_decimal(v):
            lost.append((repr(v), str(cast)))
        assert float(cast) != v or cast == LM.to_decimal(v)
    assert len(lost) == len(values), lost                               # every one of them is changed by the cast
    assert ("0.30000000000000004", "0.3") in lost


def test_values_numeric_cannot_hold_are_reported_not_rounded(pg):
    tiny = 1.23456789012345e-07                                         # 21 fractional digits
    with pytest.raises(LM.MoneyPrecisionLoss):
        LM.to_numeric_exact(tiny)
    assert LM.numeric_misfit(LM.to_decimal(tiny)).startswith("FRACTIONAL_DIGITS")
    # what the server would silently do with it: round to 18 places
    rounded = pg.execute("SELECT %s::numeric(38,18)", (LM.to_decimal(tiny),)).fetchone()[0]
    assert rounded != LM.to_decimal(tiny)
    for bad in (float("inf"), float("-inf"), "NaN"):
        with pytest.raises(LM.NonFiniteMoney):
            LM.to_decimal(bad)
