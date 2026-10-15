"""Steps 3.6 and 3.15: the daily trend family in the pipeline, and its daily reconciliation with the evaluator.

The market is synthetic and the tests say nothing about the strategy. They say that the pipeline, which keeps
its state in the database and moves one day at a time, takes the same decisions and keeps the same book as the
evaluator, which recomputes the whole period in memory.
"""
from __future__ import annotations

import csv
import json
import logging

import numpy as np
import pytest

import _step2
import _step3 as S
from app.trading_intelligence.families.daily_trend import spec
from app.trading_intelligence.families.daily_trend import targets as T
from app.trading_intelligence.pipeline import contracts as C
from app.trading_intelligence.pipeline import daily_trend as DT
from app.trading_intelligence.pipeline import driver as D
from app.trading_intelligence.pipeline import family_registry as FR
from app.trading_intelligence.pipeline import shadow_compare as SC
from app.trading_intelligence.pipeline.accounts import ModelAccounts, model_account_id
from app.trading_intelligence.pipeline.market_data import FrozenPanelBars
from app.trading_intelligence.research.evaluator import simulator as SIM
from app.trading_intelligence.research.governance.register import repository_root

EQUITY = spec.INTERPRETATION["initial_equity_usdt"]
FIRST_CYCLE = DT.day_of("2020-08-01")
LAST = DT.day_of("2021-06-30")
ACCOUNTS = [(T.MANDATE, "balanced"), (T.MANDATE, "aggressive"), (T.EXECUTABLE, "conservative"), (T.EXECUTABLE, "balanced")]


@pytest.fixture(scope="module")
def market():
    dataset = _step2.SyntheticDataset()
    panel = dataset._load("2020-01-01", "2021-06-30")
    return dataset, panel, SIM.prepare(panel, dataset.metadata)


@pytest.fixture(scope="module")
def replay(market, tmp_path_factory):
    """The real driver over 334 synthetic days for four model accounts."""
    dataset, panel, _ = market
    tmp = tmp_path_factory.mktemp("replay")
    db = S.fresh_db(tmp)
    registry = FR.FamilyRegistry(db, research_register=_step2.new_register(tmp / "register.jsonl"))
    registry.register_all(actor_ref="system:test")

    class Four:
        def accounts(self, entry):
            wanted = {model_account_id(p, level) for p, level in ACCOUNTS}
            return [a for a in ModelAccounts(initial_equity=EQUITY).accounts(entry) if a.account_id in wanted]

    drv = D.PlatformCycleDriver(db, accounts=Four(), data={"1d": FrozenPanelBars(panel, dataset.metadata)}, registry=registry)
    for day in range(FIRST_CYCLE, LAST + 1):
        result = drv.run_cycle("DAILY_TREND", DT.day_text(day), now_ms=DT.close_ms(day) + 60_000)
        assert result.status == D.COMPLETED, (DT.day_text(day), result.reason, result.account_results)
    return db


# -------------------------------------------------------------------------------- the pipeline reuses Step 2
def test_the_mandate_is_verified_against_the_register_and_the_rule_hash_is_the_registered_one(tmp_path):
    register = _step2.new_register(tmp_path / "register.jsonl")
    pinned = DT.verify_mandate(register)
    assert pinned["mandate_id"] == "MANDATE_004" and pinned["hypothesis_id"] == "H-018"
    assert pinned["rule_artifact_hash"] == FR.definitions()["DAILY_TREND"].rule_hash
    assert DT.PIPELINE.verify(register) == pinned


def test_the_pipeline_adds_no_selection_or_sizing_rule_of_its_own():
    """The frozen functions are called, not copied: the adapter has no ranking, no sizing and no breakout."""
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(DT))
    called = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    assert {"decide_targets", "orders_from_targets", "apply_exchange_filters", "prepare", "risk_level"} <= called
    source = inspect.getsource(DT.decide)
    for forbidden in ("key=lambda", "max_positions]", "risk_per_trade *", "/ c.stop_distance", "strength >"):
        assert forbidden not in source


def test_a_stronger_new_candidate_displaces_a_weaker_held_position(market):
    """No rule favours what is already held: the ranking is strength, then liquidity, then symbol."""
    dataset, panel, _ = market
    m = DT.market_stage(panel, dataset.metadata, LAST)
    level = T.risk_level("conservative", T.MANDATE)             # four positions
    strong = [T.Candidate(f"S{n}USDT", 1.0, 0.10, 1e9) for n in range(level.max_positions)]
    weak = T.Candidate(m.universe[0], 1 / 7, 0.10, 1e9)
    j = m.index(weak.symbol)
    m.candidates = tuple(strong) + (weak,)
    m.universe = tuple(c.symbol for c in m.candidates)
    close = float(m.features.close[m.row, j])
    m._index.update({c.symbol: j for c in strong})              # price them like the held coin
    book = DT.new_book("conservative", T.MANDATE, EQUITY, LAST + 1)
    book.positions[weak.symbol] = DT.Position(quantity=1.0, stop=close * 0.8, mark=close, entry_day=LAST - 5,
                                              entry_price=close, entry_stop_distance=0.2)
    decision = DT.decide(book, m)
    kinds = {o.symbol: o.kind for o in decision.orders}
    assert kinds[weak.symbol] == T.EXIT and sum(1 for k in kinds.values() if k == T.ENTRY) == level.max_positions
    assert {"symbol": weak.symbol, "reason": T.REJECT_POSITION_LIMIT, "stage": "DECISION"} in decision.document["rejections"]


def test_blocked_entries_keep_exits_and_reductions(market):
    dataset, panel, _ = market
    m = DT.market_stage(panel, dataset.metadata, LAST)
    book = DT.new_book("balanced", T.MANDATE, EQUITY, LAST + 1)
    free = DT.decide(book, m)
    blocked = DT.decide(book, m, entries_blocked=True, blocked_reason="FAMILY_KILL_SWITCH")
    assert any(o.kind == T.ENTRY for o in free.orders) and blocked.orders == [] and blocked.intents == ()
    assert {r["reason"] for r in blocked.document["rejections"] if r["stage"] == "PLATFORM_CONTROL"} == {DT.REJECT_ENTRIES_BLOCKED}
    held = free.orders[0].symbol
    book.positions[held] = DT.Position(1e-9, 1.0, 100.0, LAST, 100.0, 0.1)     # a holding the rule no longer targets
    m.candidates = tuple(c for c in m.candidates if c.symbol != held)
    kept = DT.decide(book, m, entries_blocked=True)
    assert [(o.symbol, o.kind) for o in kept.orders] == [(held, T.EXIT)]
    assert isinstance(kept.intents[0], C.ExitIntent)


def test_intents_are_valid_family_neutral_contracts_without_a_profit_target(market):
    dataset, panel, _ = market
    m = DT.market_stage(panel, dataset.metadata, LAST)
    for policy in (T.MANDATE, T.EXECUTABLE):
        decision = DT.decide(DT.new_book("aggressive", policy, EQUITY, LAST + 1), m)
        batch = C.IntentBatch(
            provenance=C.DecisionProvenance("DAILY_TREND", "v", "MANDATE_004", "H-018", "r", "d", "h", DT.close_ms(LAST),
                                            m.data_snapshot_id, "1d", C.SHADOW),
            account=C.AccountRef("u", "a", "binance", "DEMO"), intents=decision.intents, constraints=decision.constraints,
            instruments=frozenset(decision.instruments), created_at_ms=DT.close_ms(LAST),
            expires_at_ms=DT.close_ms(LAST) + 86_400_000)
        entries = [i for i in batch.intents if i.kind == C.ENTRY]
        assert entries and all(i.take_profit_price is None and i.direction == C.LONG for i in entries)
        level = T.risk_level("aggressive", policy)
        assert all(i.risk_fraction <= level.risk_per_trade * (1 + 1e-9) for i in entries)
        if policy == T.EXECUTABLE:
            assert all(i.stop_distance <= 0.15 * (1 + 1e-9) and i.risk_fraction <= 0.004 * (1 + 1e-9) for i in entries)
            wide = [r for r in decision.document["rejections"] if r["reason"] == T.REJECT_STOP_DISTANCE]
            assert all(r["symbol"] not in {i.symbol for i in entries} for r in wide)    # refused, never clipped


def test_the_market_stage_is_causal_and_names_its_data(market):
    dataset, panel, _ = market
    day = DT.day_of("2021-01-15")
    full, cut = DT.market_stage(panel, dataset.metadata, day), DT.market_stage(panel.truncated(day), dataset.metadata, day)
    assert full.summary() == cut.summary() and full.data_snapshot_id == DT.snapshot_id(panel.truncated(day))
    changed = panel.truncated(day)
    changed.fields = {k: v.copy() for k, v in changed.fields.items()}
    changed.fields["close"][-1, 0] *= 1.0000001
    assert DT.market_stage(changed, dataset.metadata, day).data_snapshot_id != full.data_snapshot_id
    with pytest.raises(DT.DataNotFresh, match="DECISION_DAY_HAS_NO_BARS"):
        DT.market_stage(panel, dataset.metadata, LAST + 1)


# -------------------------------------------------------------------------- the shadow book, without a database
@pytest.mark.parametrize("policy,level", [(T.MANDATE, "conservative"), (T.MANDATE, "aggressive"), (T.EXECUTABLE, "balanced")])
def test_the_incremental_shadow_book_equals_the_evaluator_bit_for_bit(market, policy, level):
    dataset, panel, feat = market
    first = DT.day_of("2020-07-01")
    sim = SIM.simulate(feat, SIM.RunConfig(level=T.risk_level(level, policy), start_day=first, end_day=LAST, initial_equity=EQUITY))
    book = DT.new_book(level, policy, EQUITY, first)
    book.pending = DT.pending_from(DT.decide(book, DT.market_stage(panel, dataset.metadata, first - 1)).orders)
    fills = []
    for row, day in enumerate(range(first, LAST + 1)):
        m = DT.market_stage(panel, dataset.metadata, day)
        result = DT.advance_day(book, m)
        fills += result["fills"]
        assert result["equity"] == sim.daily["equity"][row], DT.day_text(day)
        assert (result["paused"], result["halved"], result["halted"]) == tuple(bool(sim.daily[k][row]) for k in ("paused", "halved", "halted"))
        book.pending = DT.pending_from(DT.decide(book, m).orders)
    assert [(f["day"], f["symbol"], f["quantity"], f["price"]) for f in fills] == \
        [(f["day"], f["symbol"], f["quantity"], f["price"]) for f in sim.fills]
    assert len(fills) > 50 and any(f["quantity"] < 0 for f in fills)


def test_a_book_cannot_skip_or_repeat_a_day(market):
    dataset, panel, _ = market
    book = DT.new_book("balanced", T.MANDATE, EQUITY, LAST - 3)
    with pytest.raises(ValueError, match="first day"):
        DT.advance_day(book, DT.market_stage(panel, dataset.metadata, LAST - 2))
    DT.advance_day(book, DT.market_stage(panel, dataset.metadata, LAST - 3))
    for wrong in (LAST - 3, LAST - 1):
        with pytest.raises(ValueError, match="cannot jump"):
            DT.advance_day(book, DT.market_stage(panel, dataset.metadata, wrong))


# ------------------------------------------------------------------- the real driver against the evaluator
@pytest.mark.parametrize("policy,level", ACCOUNTS)
def test_the_driver_reproduces_the_evaluator_on_every_day(replay, market, policy, level):
    """Universe, selected contracts, strength, stop distance, equity basis, fractions, notionals, the caps, the
    rejections, the brake flags and the cash left over: 334 days, largest difference exactly zero."""
    _, _, feat = market
    out = SC.compare_account_forward(replay, family_id="DAILY_TREND", account_id=model_account_id(policy, level),
                                     features=feat, risk_level=level, policy=policy, first_day=FIRST_CYCLE + 1,
                                     last_day=LAST, initial_equity=EQUITY, now_ms=1)
    assert out["summary"]["by_status"] == {SC.MATCH: LAST - FIRST_CYCLE + 1}, out["alerts"][:3]
    assert out["summary"]["largest_relative_difference"] == 0.0 and out["alerts"] == []
    targeted = sum(1 for c in SC.shadow_from_db(replay, model_account_id(policy, level), "DAILY_TREND").values() if c["targets"])
    assert targeted > 100                              # the comparison is not vacuous


def test_the_committed_reference_format_is_read_and_reproduced(replay, market, tmp_path):
    """``development_primary_daily_targets.csv`` and the files beside it, in the evaluator's own column layout."""
    _, _, feat = market
    sim = SC.evaluator_simulation(feat, risk_level="balanced", policy=T.MANDATE, first_day=FIRST_CYCLE + 1,
                                  last_day=LAST, initial_equity=EQUITY)
    targets, daily, rejections = tmp_path / "targets.csv", tmp_path / "daily.csv", tmp_path / "rejections.csv"
    with open(targets, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["day", "symbol", "target_fraction_of_equity", "target_notional", "strength", "stop_distance", "equity"])
        for t in sim.targets:
            for symbol, v in t["targets"].items():
                w.writerow([DT.day_text(t["day"]), symbol, *(repr(float(x)) for x in (
                    v["fraction_of_equity"], v["notional"], v["strength"], v["stop_distance"], t["equity"]))])
    with open(daily, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["day", "equity", "paused", "halved", "halted"])
        for row, day in enumerate(sim.days):
            w.writerow([DT.day_text(day), repr(float(sim.daily["equity"][row])), *(float(sim.daily[k][row]) for k in ("paused", "halved", "halted"))])
    with open(rejections, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["day", "stage", "symbol", "reason"])
        for r in sim.rejections:
            w.writerow([DT.day_text(r["day"]), r["stage"], r["symbol"], r["reason"]])
    reference = SC.reference_from_csv(targets, daily_csv=daily, rejections_csv=rejections)
    shadow = SC.shadow_from_db(replay, model_account_id(T.MANDATE, "balanced"), "DAILY_TREND")
    comparisons = SC.compare(shadow, reference, days=[d for d in shadow if d > FIRST_CYCLE])
    assert SC.summary(comparisons)["by_status"] == {SC.MATCH: LAST - FIRST_CYCLE}
    committed = repository_root() / "docs/research/mandate_004/runs/run_0b88d389f8da50dc48de5e51"
    with open(committed / "development_primary_daily_targets.csv", newline="", encoding="utf-8") as f:
        assert next(csv.reader(f)) == ["day", "symbol", "target_fraction_of_equity", "target_notional", "strength",
                                       "stop_distance", "equity"]
    real = SC.reference_from_csv(committed / "development_primary_daily_targets.csv",
                                 daily_csv=committed / "development_primary_daily.csv",
                                 rejections_csv=committed / "development_primary_rejections.csv")
    assert len(real) >= 1827 and real[DT.day_of("2020-04-29")]["targets"]["BTCUSDT"]["strength"] == 0.5714285714285714


# --------------------------------------------------------------------------------- divergences are alerts
def _one_day(replay, market):
    _, _, feat = market
    account = model_account_id(T.MANDATE, "balanced")
    shadow = SC.shadow_from_db(replay, account, "DAILY_TREND")
    sim = SC.evaluator_simulation(feat, risk_level="balanced", policy=T.MANDATE, first_day=FIRST_CYCLE + 1, last_day=LAST,
                                  initial_equity=EQUITY)
    reference = SC.reference_from_simulation(sim)
    day = max(d for d, v in shadow.items() if len(v["targets"]) >= 2)
    return account, day, shadow[day], json.loads(json.dumps(reference[day]))


@pytest.mark.parametrize("change,classification,field", [
    (lambda r, s: r["targets"][s].__setitem__("strength", r["targets"][s]["strength"] + 1 / 7), SC.RULE_MISMATCH, "strength"),
    (lambda r, s: r["targets"][s].__setitem__("stop_distance", r["targets"][s]["stop_distance"] * 1.01), SC.RULE_MISMATCH, "stop_distance"),
    (lambda r, s: r["targets"][s].__setitem__("notional", r["targets"][s]["notional"] * 1.02), SC.RISK_POLICY_MISMATCH, "target_notional"),
    (lambda r, s: r["targets"][s].__setitem__("notional", r["targets"][s]["notional"] * (1 + 1e-8)), SC.PRECISION_MISMATCH, "target_notional"),
    (lambda r, s: r.__setitem__("equity", r["equity"] * 1.001), SC.UNKNOWN_DEFECT, "equity_basis"),
    (lambda r, s: r.__setitem__("universe", r["universe"] + 1), SC.DATA_MISMATCH, "universe"),
    (lambda r, s: r["flags"].__setitem__("halved", not r["flags"]["halved"]), SC.RISK_POLICY_MISMATCH, "brake_flags"),
    (lambda r, s: r["targets"].pop(s), SC.RULE_MISMATCH, "selected_contracts"),
    (lambda r, s: r["rejections"].__setitem__(s, "INVENTED"), SC.RISK_POLICY_MISMATCH, "rejections"),
    (lambda r, s: r["scale"].__setitem__("leverage", 0.5), SC.RISK_POLICY_MISMATCH, "portfolio_cap:leverage"),
])
def test_a_deliberate_divergence_is_detected_and_classified(replay, market, change, classification, field):
    account, day, shadow, reference = _one_day(replay, market)
    assert SC.compare_day(day, shadow, reference)["status"] == SC.MATCH
    change(reference, sorted(reference["targets"])[0])
    result = SC.compare_day(day, shadow, reference)
    assert result["status"] == SC.DIVERGED
    assert (field, classification) in {(d["field"], d["classification"]) for d in result["divergences"]}, result["divergences"]


def test_divergences_are_durable_records_and_structured_alerts(replay, market, caplog):
    account, day, shadow, reference = _one_day(replay, market)
    reference["targets"][sorted(reference["targets"])[0]]["strength"] += 1 / 7
    blocked = dict(shadow, entries_blocked=True, targets={})
    comparisons = [SC.compare_day(day, shadow, reference), SC.compare_day(day + 1, None, reference),
                   SC.compare_day(day + 2, shadow, None), SC.compare_day(day + 3, blocked, reference)]
    assert [c["status"] for c in comparisons] == [SC.DIVERGED, SC.NO_SHADOW, SC.NO_REFERENCE, SC.DIVERGED]
    assert comparisons[1]["divergences"][0]["classification"] == SC.TIMING_MISMATCH
    assert {d["classification"] for d in comparisons[3]["divergences"] if d["field"] == "selected_contracts"} == {SC.EXPECTED_OPERATIONAL_DIFFERENCE}
    with caplog.at_level(logging.WARNING, logger="cati.shadow_compare"):
        alerts = SC.record(replay, family_id="DAILY_TREND", account_id=account, reference_label="TEST_TAMPERED",
                           comparisons=comparisons, now_ms=5)
    assert len(alerts) == 4 and alerts[0]["alert"] == "CATI_SHADOW_DIVERGENCE"
    assert alerts[0]["classifications"] == [SC.RULE_MISMATCH] and alerts[0]["day"] == DT.day_text(day)
    logged = [json.loads(r.getMessage()) for r in caplog.records if r.name == "cati.shadow_compare"]
    assert [a["comparison_id"] for a in logged] == [a["comparison_id"] for a in alerts]
    with replay.connect() as conn:
        stored = conn.execute("SELECT COUNT(*) FROM cati_shadow_comparisons WHERE reference='TEST_TAMPERED'").fetchone()[0]
        with pytest.raises(Exception, match="append-only"):
            conn.execute("DELETE FROM cati_shadow_comparisons")
    assert stored == 4
    assert SC.record(replay, family_id="DAILY_TREND", account_id=account, reference_label="TEST_TAMPERED",
                     comparisons=comparisons, now_ms=9) == []           # recorded once
    assert SC.summary(comparisons)["divergences_by_classification"][SC.RULE_MISMATCH] >= 1


def test_a_pipeline_that_drifts_from_the_evaluator_is_caught_by_the_daily_comparison(market, tmp_path):
    """A defect injected into the pipeline's book (one fee charged twice) surfaces as an unexplained difference."""
    dataset, panel, feat = market
    db = S.fresh_db(tmp_path)
    registry = FR.FamilyRegistry(db, research_register=_step2.new_register(tmp_path / "register.jsonl"))
    registry.register_all(actor_ref="system:test")
    accounts = ModelAccounts(initial_equity=EQUITY, levels=("balanced",), policies=(T.MANDATE,))
    drv = D.PlatformCycleDriver(db, accounts=accounts, data={"1d": FrozenPanelBars(panel, dataset.metadata)}, registry=registry)
    last = FIRST_CYCLE + 60
    for day in range(FIRST_CYCLE, last + 1):
        if day == FIRST_CYCLE + 30:
            with db.connect() as conn:
                conn.execute("UPDATE cati_shadow_accounts SET equity = equity - 1.0")
        assert drv.run_cycle("DAILY_TREND", DT.day_text(day), now_ms=DT.close_ms(day) + 1).status == D.COMPLETED
    out = SC.compare_account_forward(db, family_id="DAILY_TREND", account_id=model_account_id(T.MANDATE, "balanced"),
                                     features=SIM.prepare(panel.truncated(last), dataset.metadata), risk_level="balanced",
                                     policy=T.MANDATE, first_day=FIRST_CYCLE + 1, last_day=last, initial_equity=EQUITY, now_ms=1)
    by_status = out["summary"]["by_status"]
    assert by_status[SC.MATCH] == 30 and by_status[SC.DIVERGED] == 31
    assert out["alerts"][0]["day"] == DT.day_text(FIRST_CYCLE + 30)
    assert SC.UNKNOWN_DEFECT in out["alerts"][0]["classifications"] and "equity_basis" in out["alerts"][0]["fields"]


def test_relative_difference():
    assert SC.relative_difference(1.0, 1.0) == 0.0 and SC.relative_difference(0.0, 0.0) == 0.0
    assert SC.relative_difference(100.0, 101.0) == pytest.approx(1 / 101)
    assert SC.relative_difference(1.0, float("nan")) == float("inf")
    assert np.isclose(SC.TOLERANCE, 1e-9) and SC.PRECISION_BAND > SC.TOLERANCE
