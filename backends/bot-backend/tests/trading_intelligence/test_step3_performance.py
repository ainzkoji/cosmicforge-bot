"""Step 3.12: robustness and performance of the platform cycle.

The measurements themselves are in ``docs/implementation/evidence/step3/cycle_performance.json`` (written by
``pipeline.performance``). These tests assert what must hold on any machine: counts, the shape of scaling and
exact equality of results. No test compares a duration with a threshold.
"""
from __future__ import annotations

import ast
import json
import math
import tracemalloc
from pathlib import Path

import numpy as np
import pytest

import _step2
import _step3 as S
from app.trading_intelligence.families.daily_trend import targets as T
from app.trading_intelligence.pipeline import contracts as C
from app.trading_intelligence.pipeline import daily_trend as DT
from app.trading_intelligence.pipeline import driver as D
from app.trading_intelligence.pipeline import performance as P
from app.trading_intelligence.pipeline import shadow_book
from app.trading_intelligence.pipeline.market_data import FrozenPanelBars
from app.trading_intelligence.research.evaluator import simulator as SIM

START = DT.day_of(P.FIRST_CYCLE)
REQUIRED = ("end-to-end cycle duration", "stage duration", "market-data request volume", "database statements", "memory usage",
            "account processing throughput", "queue depth", "recovery time", "symbol failure rate")


@pytest.fixture(scope="module")
def register(tmp_path_factory):
    return _step2.new_register(tmp_path_factory.mktemp("register") / "register.jsonl")


@pytest.fixture(scope="module")
def market():
    return P.synthetic_market()


@pytest.fixture(scope="module")
def measured(tmp_path_factory, register):
    """The whole harness, small: the real driver on synthetic fixtures."""
    return P.measure(tmp_path_factory.mktemp("harness"), account_counts=(2, 8), cycles=3, market_sizes=((240, 12),),
                     recovery_accounts=6, feed_sizes=((150, 8),), research_register=register)


def driver_for(tmp_path, register, market, accounts, **kw):
    db = S.fresh_db(tmp_path)
    return db, P.build_driver(db, data=FrozenPanelBars(*market), accounts=P.SyntheticAccounts(accounts), research_register=register, **kw)


def run(driver, day):
    return driver.run_cycle("DAILY_TREND", DT.day_text(day), now_ms=DT.close_ms(day) + 60_000)


# ------------------------------------------------------------------------------------- what is measured
def test_the_harness_records_every_required_measurement_from_the_real_driver(measured):
    json.dumps(measured)                               # the evidence file is plain JSON
    scenario = measured["scenarios"][1]
    cycle, counted = scenario["timed"][-1], scenario["counted"][-1]
    found = {
        "end-to-end cycle duration": cycle["end_to_end_ms"] > 0,
        "stage duration": {"VERIFY_MANDATE", "MARKET_STAGE", "ACCOUNT_LIST", "ACCOUNT_STAGE"} <= set(cycle["stage_ms"]),
        "market-data request volume": measured["market_data_feed"]["requests_per_daily_cycle"] == [2 + 2 * 26],
        "database statements": counted["statements"] > 0 and counted["statements_per_account"]["mean"] > 0,
        "memory usage": scenario["memory"]["tracemalloc_peak_bytes_per_cycle"][0] > 0,
        "account processing throughput": cycle["accounts_per_second"] > 0,
        "queue depth": cycle["queue"]["max_depth"] == 8 and 1 <= cycle["queue"]["time_weighted_mean_depth"] <= 8,
        "recovery time": measured["recovery"]["recovery_ms"] > 0,
        "symbol failure rate": measured["market_data_feed"]["failing_cycle"]["symbol_failure_rate"] == pytest.approx(2 / 26, abs=1e-6),
    }
    assert set(found) == set(REQUIRED) and all(found.values()), found
    assert cycle["accounts_done"] == 8 and sum(cycle["stage_ms"].values()) == pytest.approx(cycle["end_to_end_ms"], rel=1e-6)
    assert measured["fixtures"]["market"] == {"kind": "deterministic random walk (synthetic_market)", "days": 547, "symbols": 26,
                                              "seed": 7, "first_day": "2020-01-01"}
    host = P.host_description()
    assert host["system"] and host["cpu_count"] and "not a Linux certification" in host["statement"]
    assert "Step 5 scale certification is not claimed" in host["statement"]


def test_before_and_after_are_only_compared_on_the_same_fixtures(measured):
    rows = P.compare(measured, measured)
    assert rows and all(r["before"] == r["after"] and r["change_percent"] in (0.0, None) for r in rows)
    assert {r["metric"] for r in rows} >= {"end-to-end cycle, ms (median)", "database statements per cycle", "accounts per second",
                                           "database connections per cycle", "peak memory of a cycle, MiB", "market stage, ms (median)"}
    other = json.loads(json.dumps(measured))
    other["fixtures"]["market"]["seed"] = 8
    with pytest.raises(ValueError, match="same fixtures"):
        P.compare(measured, other)


# --------------------------------------------------------------------------- bounded database work per cycle
def test_a_cycle_opens_one_connection_and_its_database_work_grows_only_with_the_accounts(measured):
    small, large = measured["scenarios"]
    for scenario in (small, large):
        n = scenario["accounts"]
        assert scenario["market_data_provider_calls"] == scenario["cycles"]         # one market stage serves every account
        for cycle in scenario["counted"]:
            assert cycle["connections_opened"] == 1                               # before: two per account and seven per cycle
            # still one transaction per account (never several accounts in one), plus a few for the cycle itself
            assert n + 2 <= cycle["by_verb"]["BEGIN"] == cycle["by_verb"]["COMMIT"] <= n + 8
            assert cycle["statements"] == cycle["account_stage_statements"] + cycle["cycle_overhead_statements"]
            assert "TRIGGER" not in cycle["by_verb"]                                # nothing was updated or deleted in an append-only table
    # outside the account stage a cycle runs a fixed number of statements plus one enrolment row per account
    overhead = {s["accounts"]: s["counted"][-1]["cycle_overhead_statements"] for s in (small, large)}
    assert overhead[8] - overhead[2] == 6 and overhead[2] - 2 <= 25
    # per account: a fixed part, plus one row per fill of the day and per open position (at most eight positions)
    for scenario in (small, large):
        for cycle in scenario["counted"]:
            assert 11 <= cycle["statements_per_account"]["min"] and cycle["statements_per_account"]["max"] <= 13 + 3 * 8
    per_account = [s["counted"][-1]["statements_per_account"]["mean"] for s in (small, large)]
    assert abs(per_account[0] - per_account[1]) <= 3   # it does not grow with the number of accounts


def test_memory_of_a_cycle_does_not_grow_with_the_number_of_accounts(measured):
    small, large = (s["memory"]["tracemalloc_peak_bytes_per_cycle"] for s in measured["scenarios"])
    assert max(large) <= max(small) * 1.25 + 1_000_000  # the market stage dominates; nothing is kept per account


def test_an_account_that_fails_under_the_shared_connection_leaves_nothing_behind(tmp_path, register, market, monkeypatch):
    """One connection serves the whole cycle. An account is still exactly one transaction: when it fails after
    its book and its day were written, none of it is kept and the other accounts are complete."""
    db, driver = driver_for(tmp_path, register, market, 5)
    for day in range(START, START + 3):
        assert run(driver, day).status == D.COMPLETED
    real = shadow_book.save
    victim = "perf-00002"

    def failing_save(conn, account_id, *a, **k):
        real(conn, account_id, *a, **k)                # the book is written, then the account dies
        if account_id == victim:
            raise RuntimeError("after the book was written")

    monkeypatch.setattr(shadow_book, "save", failing_save)
    with db.connect() as conn:
        before = [tuple(r) for r in conn.execute("SELECT * FROM cati_shadow_accounts WHERE account_id=?", (victim,))]
    result = run(driver, START + 3)
    assert (result.status, result.accounts_done, result.accounts_failed) == (D.PARTIAL, 4, 1)
    with db.connect() as conn:
        assert [tuple(r) for r in conn.execute("SELECT * FROM cati_shadow_accounts WHERE account_id=?", (victim,))] == before
        assert conn.execute("SELECT COUNT(*) FROM cati_shadow_days WHERE account_id=? AND day=?", (victim, START + 3)).fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM cati_family_decisions WHERE account_id=? AND data_cutoff=?",
                            (victim, DT.day_text(START + 3))).fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM cati_shadow_days WHERE day=?", (START + 3,)).fetchone()[0] == 4
    monkeypatch.setattr(shadow_book, "save", real)
    assert run(driver, START + 3).status == D.COMPLETED


def test_an_interrupted_cycle_recovers_to_exactly_the_uninterrupted_result(measured):
    recovery = measured["recovery"]
    assert recovery["state_left_behind"] == {"cycle_status": D.MARKET_DECIDED, "accounts_done": 3}
    assert recovery["recovered_status"] == D.COMPLETED
    assert (recovery["accounts_left_alone"], recovery["accounts_processed_on_recovery"]) == (3, 3)
    assert recovery["books_equal_uninterrupted_run"] is True and recovery["decisions_equal_uninterrupted_run"] is True
    assert recovery["duplicate_decisions"] == 0


# ------------------------------------------------------------------------------ the market stage, unchanged
def test_a_panel_that_is_already_cut_is_not_copied_again_and_gives_the_same_market_stage():
    panel, metadata = P.synthetic_market(days=600, symbols=60)
    day = int(panel.days[-1]) - 40
    cut = panel.truncated(day)
    for matrix in cut.fields.values():
        matrix.setflags(write=False)                   # the market stage may read the provider's panel, never write it
    full, served = DT.market_stage(panel, metadata, day), DT.market_stage(cut, metadata, day)
    assert served.summary() == full.summary() and served.data_snapshot_id == DT.snapshot_id(cut)
    for name in ("strength", "stop_distance", "member", "volume", "open", "close"):
        assert np.array_equal(getattr(served.features, name), getattr(full.features, name), equal_nan=True)
    assert not np.shares_memory(served.features.close, cut.fields["close"])        # the features are the evaluator's own copy
    panel_bytes = sum(v.nbytes for v in cut.fields.values())

    def peak(source):
        tracemalloc.start()
        try:
            DT.market_stage(source, metadata, day)
            return tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()

    assert peak(panel) - peak(cut) >= 0.8 * panel_bytes     # one whole copy of the history less


def test_the_market_stage_is_the_evaluators_own_computation_on_any_cut(market):
    """No window is shortened: the features of the decision day are those the evaluator computes from the whole
    history, and a shorter history is a different (and unused) computation."""
    panel, metadata = market
    for day in (START, START + 37, int(panel.days[-1])):
        decision, features = DT.market_stage(panel, metadata, day), SIM.prepare(panel.truncated(day), metadata)
        for name in ("strength", "stop_distance", "member", "volume"):
            assert np.array_equal(getattr(decision.features, name), getattr(features, name), equal_nan=True), name


def test_a_shortened_history_window_is_not_the_frozen_rule():
    """Why the market stage reads the whole history and no bounded window replaces it: a sub-signal is latched.
    A contract that broke out and then swings ever more narrowly makes no new high and no new low for as long
    as one likes; its signal is ON only because of a close that a shorter window no longer contains."""
    panel, metadata = P.synthetic_market(days=700, symbols=4)
    rise = 810.0 + 6.0 * np.arange(201)                                  # 201 days up to 2010: every sub-signal turns ON
    swing = 2000.0 + 10.0 * (-0.99) ** np.arange(1, 500)                 # then 499 days inside the previous day's range
    close = np.concatenate([rise, swing])
    fields = {k: v.copy() for k, v in panel.fields.items()}
    fields["close"][:, 0], fields["open"][:, 0] = close, close
    fields["high"][:, 0], fields["low"][:, 0] = close * 1.001, close * 0.999
    built = type(panel)(panel.days, panel.symbols, fields)
    whole = SIM.prepare(built, metadata)
    assert whole.strength[-1, 0] == 1.0 and (whole.strength[201:, 0] == 1.0).all()
    for kept in (160, 300, 450):                       # longer than the longest lookback (150 days), and much longer
        window = type(panel)(built.days[-kept:], built.symbols, {k: v[-kept:] for k, v in fields.items()})
        assert SIM.prepare(window, metadata).strength[-1, 0] == 0.0
    decision = DT.market_stage(built, metadata, int(built.days[-1]))     # the pipeline uses the whole history
    assert decision.features.strength[decision.row, 0] == 1.0


def test_the_platform_cycle_runs_neither_the_forecast_lookup_nor_the_rolling_standard_deviation(tmp_path, register, market, monkeypatch):
    """Two items of the Step 3.12 list belong to the forecast line of the setup specialists (``forecast.engine``
    row matching, ``market_state.indicators.rolling_std_causal``). The platform cycle does not call them."""
    from app.trading_intelligence.forecast import engine
    from app.trading_intelligence.market_state import indicators, volatility

    def forbidden(*a, **k):
        raise AssertionError("the platform cycle reached the forecast pipeline")

    for module, name in ((engine, "_rows_matching"), (engine, "_family_rows"), (engine, "build_outcome_forecast"),
                         (engine, "forecast_from_dimensions"), (indicators, "rolling_std_causal"), (volatility, "rolling_std_causal")):
        monkeypatch.setattr(module, name, forbidden)
    db, driver = driver_for(tmp_path, register, market, 3)
    for day in range(START, START + 4):
        assert run(driver, day).status == D.COMPLETED
    with db.connect() as conn:
        stages = json.loads(conn.execute("SELECT detail FROM cati_platform_cycles LIMIT 1").fetchone()[0])["market"]["stages"]
    assert stages["MARKET_STATE"]["status"] == stages["REGIME"]["status"] == "NOT_APPLICABLE"
    pipeline = Path(D.__file__).resolve().parent
    for name in ("driver", "daily_trend", "shadow_book", "accounts", "market_data", "performance", "provenance", "contracts"):
        tree = ast.parse((pipeline / f"{name}.py").read_text(encoding="utf-8"))
        modules = {n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)} | \
                  {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        assert not [m for m in modules if "forecast" in m or "market_state" in m], name


# -------------------------------------------------------------------------------------- adverse input
def tied_panel(columns=24, days=260):
    """Every contract has the same prices and the same volume: every strength, stop distance and liquidity ties."""
    panel, metadata = P.synthetic_market(days=days, symbols=columns)
    fields = {k: np.repeat(v[:, :1], columns, axis=1).copy() for k, v in panel.fields.items()}
    return type(panel)(panel.days, list(panel.symbols), fields), metadata


def test_ranking_is_deterministic_when_everything_ties():
    panel, metadata = tied_panel()
    day = int(panel.days[-1])
    first, second = DT.market_stage(panel, metadata, day), DT.market_stage(panel, metadata, day)
    assert first.summary() == second.summary()
    assert list(first.universe) == panel.symbols[:20]  # twenty of twenty-four equal contracts: the first by symbol
    ranked = [r["symbol"] for r in first.summary()["ranking"]]
    assert ranked == sorted(ranked) and len({(r["strength"], r["volume"]) for r in first.summary()["ranking"]}) <= 1
    books = [DT.decide(DT.new_book("aggressive", T.MANDATE, 10_000.0, day + 1), m) for m in (first, second)]
    assert books[0].document == books[1].document
    chosen = [o.symbol for o in books[0].orders if o.kind == T.ENTRY]
    assert chosen == sorted(chosen) == ranked[:len(chosen)]            # ties are broken by symbol, never by position in memory
    # the same market with its contracts renamed in another order ranks by the new names, not by the old columns
    renamed = type(panel)(panel.days, sorted(f"Z{n:02d}USDT" for n in range(24)), panel.fields)
    assert list(DT.market_stage(renamed, metadata, day).universe) == renamed.symbols[:20]


def test_a_missing_or_non_finite_input_never_becomes_a_candidate_a_rank_or_an_order(market):
    panel, metadata = market
    day = START + 20
    clean = DT.market_stage(panel, metadata, day)
    member = clean.universe[0]
    j, row = clean.index(member), int(np.searchsorted(panel.days, day))
    # a missing volume or a missing bar inside the windows the rules read takes the contract out; an infinite
    # high (which the feed, like the dataset, never stores: it is not a bar) can only widen the stop to its cap
    for field, value, leaves in (("quote_volume", np.nan, True), ("close", np.nan, True), ("high", np.inf, False)):
        broken = panel.truncated(day)
        broken.fields = {k: v.copy() for k, v in broken.fields.items()}
        broken.fields[field][row - 3, j] = value
        decision = DT.market_stage(broken, metadata, day)
        for c in decision.candidates:                  # whatever remains a candidate is finite where a rule orders by it
            assert math.isfinite(c.volume) and 0.0 <= c.strength <= 1.0 and (math.isnan(c.stop_distance) or 0.05 <= c.stop_distance <= 0.40)
        ranked = decision.summary()["ranking"]
        assert all(math.isfinite(r["stop_distance"]) and math.isfinite(r["volume"]) and r["strength"] > 0 for r in ranked)
        assert decision.summary() == DT.market_stage(broken, metadata, day).summary()
        made = DT.decide(DT.new_book("aggressive", T.MANDATE, 10_000.0, day + 1), decision)
        assert all(math.isfinite(o.quantity) and math.isfinite(o.target_notional) and 0.05 <= o.stop_distance <= 0.40 for o in made.orders)
        assert len(made.intents) == len(made.orders)   # every order became a validated contract
        if leaves:
            assert member not in decision.universe and member not in [r["symbol"] for r in ranked]
            assert member not in [o.symbol for o in made.orders]
        else:
            assert [c.stop_distance for c in decision.candidates if c.symbol == member] == [0.40]
    # a candidate built by hand with a value the rules cannot produce is not ranked and not ordered
    for strength, stop in ((1.0, float("nan")), (float("nan"), 0.1), (1.0, float("inf")), (1.0, 0.0), (-1.0, 0.1)):
        decision = DT.market_stage(panel, metadata, day)
        decision.candidates += (T.Candidate("ZZZUSDT", strength, stop, 1e9, 1.0),)
        decision.universe += ("ZZZUSDT",)
        decision._index["ZZZUSDT"] = j
        assert "ZZZUSDT" not in [r["symbol"] for r in decision.summary()["ranking"]]
        assert "ZZZUSDT" not in [o.symbol for o in DT.decide(DT.new_book("aggressive", T.MANDATE, 10_000.0, day + 1), decision).orders]


@pytest.mark.parametrize("change,code", [
    ({"quantity": float("nan")}, "INVALID_QUANTITY"), ({"quantity": -1.0}, "INVALID_QUANTITY"),
    ({"reference_price": float("inf")}, "INVALID_PRICE"), ({"stop_price": float("nan")}, "INVALID_STOP_GEOMETRY"),
    ({"stop_price": 200.0}, "INVALID_STOP_GEOMETRY"), ({"risk_fraction": float("nan")}, "MISSING_RISK_INFORMATION")])
def test_a_contract_cannot_be_constructed_from_a_non_finite_or_inconsistent_value(change, code):
    fields = dict(symbol="BTCUSDT", direction=C.LONG, quantity=1.0, reference_price=100.0, stop_price=90.0, risk_fraction=0.004)
    assert C.EntryIntent(**fields).notional == 100.0
    with pytest.raises(C.ContractViolation) as refused:
        C.EntryIntent(**{**fields, **change})
    assert refused.value.code == code


def test_a_decision_that_violates_the_contracts_fails_its_account_and_is_never_stored(tmp_path, register, market, monkeypatch):
    """Construction is the validation: an account whose decision would carry a non-finite order is failed and
    rolled back; the other accounts of the cycle are unaffected."""
    db, driver = driver_for(tmp_path, register, market, 4)
    assert run(driver, START).status == D.COMPLETED
    real = DT.DailyTrendPipeline.account

    def corrupted(self, conn, market_decision, account, **kw):
        out = real(self, conn, market_decision, account, **kw)
        if account.account_id == "perf-00001" and out.get("intents"):
            bad = object.__new__(C.EntryIntent)        # a value no constructor would have accepted
            for name, value in vars(out["intents"][0]).items():
                object.__setattr__(bad, name, value)
            object.__setattr__(bad, "symbol", "NOTINUNIVERSEUSDT")
            out = {**out, "intents": (bad,) + tuple(out["intents"][1:])}
        return out

    monkeypatch.setattr(DT.DailyTrendPipeline, "account", corrupted)
    results = [run(driver, START + n) for n in range(1, 6)]
    failed = [r for r in results if r.status == D.PARTIAL]
    assert failed, "the synthetic account never had an entry to corrupt"
    assert failed[0].account_results["perf-00001"]["status"] == D.ACCOUNT_FAILED and failed[0].accounts_done == 3
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM cati_family_decisions WHERE document LIKE '%NOTINUNIVERSEUSDT%'").fetchone()[0] == 0


def test_a_symbol_that_disappears_and_comes_back_never_collapses_the_cycle_or_the_other_symbols(tmp_path, register, market):
    panel, metadata = market
    db, driver = driver_for(tmp_path, register, market, 6)
    for day in range(START, START + 6):
        assert run(driver, day).status == D.COMPLETED
    day = START + 6
    with db.connect() as conn:
        held = [r[0] for r in conn.execute("SELECT symbol FROM cati_shadow_positions GROUP BY symbol ORDER BY COUNT(*) DESC, symbol")]
    assert held, "no synthetic account holds anything"
    gone = held[0]
    j, row = panel.symbols.index(gone), int(np.searchsorted(panel.days, day))
    gapped = type(panel)(panel.days, panel.symbols, {k: v.copy() for k, v in panel.fields.items()})
    for name in ("open", "high", "low", "close", "quote_volume"):
        gapped.fields[name][row, j] = np.nan           # the contract has no bar on the decision day, then trades again
    before, missing = DT.market_stage(panel, metadata, day), DT.market_stage(gapped, metadata, day)
    assert gone in before.universe and gone not in missing.universe
    rank = lambda m: [r["symbol"] for r in m.summary()["ranking"]]     # noqa: E731
    common = set(rank(before)) & set(rank(missing))
    assert [s for s in rank(missing) if s in common] == [s for s in rank(before) if s in common]   # the others keep their order
    for other in common:                               # ... and their signal, stop distance and liquidity, exactly
        a, b = before.index(other), missing.index(other)
        assert before.features.strength[before.row, a] == missing.features.strength[missing.row, b]
        assert before.features.stop_distance[before.row, a] == missing.features.stop_distance[missing.row, b]
    back = DT.market_stage(gapped, metadata, day + 1)  # it trades again: no history is invented for it
    assert gone not in back.universe and back.features.strength[back.row, back.index(gone)] == 0.0
    gap_driver = P.build_driver(db, data=FrozenPanelBars(gapped, metadata), accounts=P.SyntheticAccounts(6), research_register=register)
    for d in (day, day + 1, day + 2):
        result = run(gap_driver, d)
        assert (result.status, result.accounts_done, result.accounts_failed) == (D.COMPLETED, 6, 0)
    with db.connect() as conn:
        exits = [tuple(r) for r in conn.execute("SELECT kind, day FROM cati_shadow_fills WHERE symbol=? AND day>=?", (gone, day))]
        reentered = conn.execute("SELECT COUNT(*) FROM cati_shadow_positions WHERE symbol=?", (gone,)).fetchone()[0]
    assert exits and {k for k, _ in exits} == {DT.EXIT_DATA_GAP} and {d for _, d in exits} == {day} and reentered == 0


# ------------------------------------------------------------------------------------------- shadow only
def test_the_harness_cannot_place_an_order_and_reads_no_secret():
    source = Path(P.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    modules = {("." * n.level) + (n.module or "") for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)} | \
              {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    forbidden = ("app.runner", "app.exchange", "app.execution", "app.trading_intelligence.execution", "app.trading_intelligence.integration",
                 "app.core", "requests", "dotenv", "urllib", "http", "socket")
    assert not [m for m in modules if m.startswith(forbidden)], modules
    assert "handoff=None" in source and "api_key" not in source.lower() and "secret" not in source.lower()
    assert not [text for text in ('".env"', "'.env'", "load_dotenv", "DB()") if text in source]   # only databases it creates
    assert P.ACCOUNT_COUNTS == (1, 10, 100, 500)
