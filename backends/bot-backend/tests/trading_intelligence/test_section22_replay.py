"""Section 22 replay / pipeline / CLI on the CANONICAL CATI path.

Synthetic candles (data_source ``synthetic_test``) exercise the machinery and
must come back refused for certification. One module-scoped replay is shared
so the expensive causal pass runs once.
"""
from __future__ import annotations

import dataclasses
import inspect
import json
import sqlite3

import pytest
from _cert import SYNTHETIC_SOURCES, meta, series

from app.trading_intelligence.research.certification import evaluation as E, replay as RP
from app.trading_intelligence.research.certification.freeze import build_policy_freeze
from app.trading_intelligence.research.certification.pipeline import certify
from app.trading_intelligence.research.certification.policy import default_certification_policy
from app.trading_intelligence.research.certification.registry import (
    CertificationRunStore, ExperimentRegistry, HoldoutRegistry, SqliteResearchStore,
)
from app.trading_intelligence.research.certification.replay import ReplayConfig, run_replay

CFG = ReplayConfig(symbols=("BTCUSDT", "ETHUSDT"), timeframe="15m", label_horizon_bars=24, warmup_bars=250, folds=3)
N = 620


@pytest.fixture(scope="module")
def data():
    return series(n=N), meta()


@pytest.fixture(scope="module")
def replayed(data):
    s, m = data
    return run_replay(s, m, CFG, data_sources=SYNTHETIC_SOURCES)


# ============================== CANONICAL PARITY ==============================
def test_replay_drives_the_canonical_controller_not_a_copy(data, replayed, monkeypatch):
    src = inspect.getsource(RP)
    assert "CATIController" in src and "evaluate_symbol" in src and "CATICycleCoordinator" in src
    assert "canonical_economics" in src and "label_candidate" in src and "run_build" in src
    for banned in ("class ResearchCATI", "class ReplayCATI", "def discover_setups", "def compute_regime"):
        assert banned not in src
    from app.trading_intelligence.controller.cati_controller import CATIController

    calls = []
    real = CATIController.evaluate_symbol
    monkeypatch.setattr(CATIController, "evaluate_symbol", lambda self, **kw: (calls.append(kw["snapshot"].symbol),
                                                                              real(self, **kw))[1])
    s, m = data
    run_replay(s, m, CFG, data_sources=SYNTHETIC_SOURCES, max_decisions=3, library_rows=replayed.library_rows,
               library_template=replayed.library_template)
    assert calls and set(calls) <= {"BTCUSDT", "ETHUSDT"}
    r = replayed.records[0]
    assert r["venue_source_quality"] in ("VALID", "DEGRADED") and r["rank"] is None or isinstance(r["rank"], int)
    assert replayed.integrity["portfolio_selection"] == "NOT_REPLAYED_ACCOUNT_STATEFUL"


def test_replay_venue_economics_carry_explicit_modeled_provenance():
    from app.trading_intelligence.contracts.instrument import instrument_key_for
    from app.trading_intelligence.research.certification.replay_venue import REPLAY_REASON, replay_venue_context

    ctx = replay_venue_context("BTCUSDT", 50_000.0, 1_767_225_600_000)
    obs = ctx.observe(instrument_key_for(venue="binance", venue_symbol="BTCUSDT"), 1_767_225_600_000)
    assert REPLAY_REASON in obs.reason_codes and obs.environment == "REAL"
    assert "DEPTH_UNAVAILABLE" in obs.reason_codes  # no depth history -> never fabricated


# ============================== FAST / DETERMINISM ==============================
def test_fast_replay_is_deterministic(data, replayed):
    s, m = data
    kw = dict(data_sources=SYNTHETIC_SOURCES, max_decisions=40, library_rows=replayed.library_rows,
              library_template=replayed.library_template)
    a, b = run_replay(s, m, CFG, **kw), run_replay(s, m, CFG, **kw)
    assert a.replay_hash == b.replay_hash and a.records == b.records
    ids = [r["setup_candidate_id"] for r in a.records]
    full = [r["setup_candidate_id"] for r in replayed.records if r["decision_time"] <= max(
        (x["decision_time"] for x in a.records), default=0)]
    assert ids == full  # a bounded re-run reproduces the full run's prefix exactly


def test_integrity_diagnostics_and_no_lookahead(replayed):
    assert replayed.integrity["lookahead_violations"] == 0
    assert replayed.counts["candidates"] >= replayed.counts["labeled"] > 0
    plan = replayed.plan
    for fold in plan.evaluation_folds:
        info = replayed.library["folds"][fold.name]
        cutoff = plan.library_cutoff_for(fold)
        horizon = CFG.label_horizon_bars * 900_000
        used = [r for r in replayed.library_rows if r.label.decision_time + horizon < cutoff]
        assert info["rows"] == len(used) and info["cutoff_ms"] == cutoff
        assert all(r.label.decision_time + horizon < fold.start_ms for r in used)  # purged labels never cross
    assert {r["fold"] for r in replayed.records} <= {f.name for f in plan.evaluation_folds}
    assert all(not plan.holdout.contains(r["decision_time"]) for r in replayed.records)


def test_pre_holdout_pass_never_loads_holdout_candles(data, replayed, monkeypatch):
    from app.replay import historical_provider as hp

    seen = []
    real_init = hp.HistoricalMarketDataProvider.__init__

    def spy(self, data_, clock, **kw):
        seen.append(max(int(r[6]) for tfs in data_.values() for rows in tfs.values() for r in rows))
        real_init(self, data_, clock, **kw)

    monkeypatch.setattr(hp.HistoricalMarketDataProvider, "__init__", spy)
    s, m = data
    run_replay(s, m, CFG, data_sources=SYNTHETIC_SOURCES, max_decisions=2, library_rows=replayed.library_rows,
               library_template=replayed.library_template)
    assert seen and max(seen) < replayed.plan.holdout.start_ms


# ============================== LEAKAGE ==============================
def test_future_candles_and_future_statistics_cannot_change_past_decisions(data, replayed):
    s, m = data
    kw = dict(data_sources=SYNTHETIC_SOURCES, max_decisions=30, library_rows=replayed.library_rows,
              library_template=replayed.library_template)
    base = run_replay(s, m, CFG, **kw)
    last = max(r["decision_time"] for r in base.records)
    cut = last + (CFG.label_horizon_bars + 2) * 900_000
    shocked = {sym: {tf: [r if int(r[6]) <= cut else [r[0], r[1] * 3, r[2] * 3, r[3] * 3, r[4] * 3, r[5] * 50, *r[6:]]
                          for r in rows] for tf, rows in tfs.items()} for sym, tfs in s.items()}
    after = run_replay(shocked, m, CFG, **kw)
    # rolling percentiles / volatility / normalization are past-only: tripling every later bar changes nothing
    assert [RP._identity(r) for r in after.records] == [RP._identity(r) for r in base.records]


def test_volume_liquidity_proxy_reads_only_past_bars():
    rows = [[i, 1, 1, 1, 1, 100.0, i + 1] for i in range(120)]
    b0 = RP.volume_proxy_bucket(rows)
    later = rows + [[999, 1, 1, 1, 1, 1e9, 1000]]
    assert RP.volume_proxy_bucket(later[:120]) == b0  # a future bar outside the snapshot is never read
    assert RP.volume_proxy_bucket(rows[:-1] + [[119, 1, 1, 1, 1, 1000.0, 120]]) == "HIGH"


def test_htf_candles_are_closed_at_or_before_the_decision():
    from app.replay.historical_provider import HistoricalClock, HistoricalMarketDataProvider
    from _cert import T0, ohlcv

    base = ohlcv(3, 400)
    htf = ohlcv(4, 100, bar=3_600_000)
    clock = HistoricalClock(T0)
    prov = HistoricalMarketDataProvider({"BTCUSDT": {"15m": base, "1h": htf}}, clock, source="t")
    for t in prov.step("BTCUSDT", "15m", start_ms=T0 + 50 * 900_000, end_ms=T0 + 60 * 900_000):  # clock advances per step
        snap = prov.build_snapshot("BTCUSDT", "15m", higher_timeframe="1h")
        assert int(snap.higher_timeframe_candles[-1][6]) <= t and int(snap.candles[-1][6]) <= t


def test_same_bar_tp_and_sl_is_resolved_against_the_trade():
    from test_forecast import _candidate, _flat_history_rows, _ms_and_regime

    from app.replay.cost_model import BINANCE_FUTURES_STANDARD
    from app.trading_intelligence.forecast.labels import label_candidate

    ms, _ = _ms_and_regime(_flat_history_rows(6))
    c = _candidate(ms, trigger=100.0, invalidation=95.0, target=110.0)
    future = [[c.decision_time + 900_000, 101, 112, 94, 101, 1000, c.decision_time + 1_799_999, 0, 0, 0, 0, 0]]
    label = label_candidate(c, future, cost_model=BINANCE_FUTURES_STANDARD, horizon_bars=1)
    assert label.terminal_outcome == "STOP_BEFORE_TARGET" and label.gross_R == -1.0  # never the favorable path
    assert "label_candidate(" in inspect.getsource(RP.run_replay)


# ============================== COST STRESS / NEIGHBORS ==============================
def test_cost_stress_reprices_costs_only_and_never_double_counts(replayed):
    for r in replayed.records:
        s15, s2 = r["stress"]["1.5"], r["stress"]["2.0"]
        assert r["net_R"] == pytest.approx(r["gross_R"] - r["cost_R"])
        assert s15["net_R"] == pytest.approx(r["gross_R"] - s15["cost_R"])   # same market outcome
        assert s2["net_R"] == pytest.approx(r["gross_R"] - s2["cost_R"])
        assert 0 < r["cost_R"] < s15["cost_R"] < s2["cost_R"]
        assert s2["cost_R"] <= 2.0 * r["cost_R"] * 1.25  # scaled, not stacked (uncertainty is not re-multiplied)


def test_parameter_neighbors_are_diagnostics_only(replayed):
    from app.trading_intelligence.economics.policy import default_admission_policy

    before = default_admission_policy().policy_hash
    names = {n for n, *_ in RP.NEIGHBOR_PLAN}
    assert all(set(r["neighbors"]) == names for r in replayed.records)
    m = E.stage_metrics(replayed.records, default_certification_policy())
    nb = m["parameter_neighbors"]
    assert set(nb["variants"]) == names and "no neighbor replaces the frozen policy" in nb["note"]
    assert default_admission_policy().policy_hash == before


# ============================== STAGE METRICS ==============================
def test_stage_metrics_have_every_required_section(replayed):
    m = E.stage_metrics(replayed.records, default_certification_policy())
    assert set(m["populations"]) == {"APPROVED", "ADMISSIBLE", "ALL"}
    allp = m["populations"]["ALL"]
    assert allp["net"]["status"] == "OK" and allp["net"]["ci_low"] <= allp["net"]["ci_high"]
    assert {"p_expectancy_positive", "median_R", "outcome_counts", "win_rate"} <= set(allp["net"])
    assert {"max_drawdown_R", "worst_trade_R", "max_loss_streak"} <= set(allp["drawdown_tail"])
    strata = m["stratification"]["strata"]
    assert set(E.STRATA) | {"asset_class", "venue"} <= set(strata)
    assert set(m["concentration"]["by"]) == set(E.CONCENTRATION_KEYS)
    assert m["calibration"]["status"] == "OK" and len(m["calibration"]["buckets"]) == 10
    assert set(m["cost_stress"]) == {"1.0", "1.5", "2.0"}
    assert m["counts"]["executed"] == "NOT_APPLICABLE_REPLAY"
    assert m["overfitting"]["pbo_cscv"]["status"] in ("OK", "NOT_APPLICABLE")
    assert m["overfitting"]["deflated_sharpe"]["population"] in ("APPROVED", "ADMISSIBLE")


def test_stress_windows_are_predeclared_and_deterministic(data):
    s, _ = data
    a, b = E.stress_windows(s, "15m", lookback_days=2), E.stress_windows(s, "15m", lookback_days=2)
    assert a == b and set(a["rules"]) == {"HIGH_VOLATILITY", "CRASH", "SHARP_REVERSAL"}
    assert "EVENT_WINDOWS" in a["unavailable"]  # a stale calendar never produces fabricated event windows


# ============================== PIPELINE ==============================
@pytest.fixture(scope="module")
def pipeline_env(tmp_path_factory, data):
    root = tmp_path_factory.mktemp("cert")
    db = SqliteResearchStore(str(root / "research.db"))
    s, m = data
    rep = certify(s, m, cfg=CFG, data_sources=SYNTHETIC_SOURCES, source_provider="synthetic",
                  dataset_identity="synthetic", research_db=db, artifact_dir=root / "art")
    return root, db, rep


def test_synthetic_data_cannot_certify_and_the_report_is_deterministic(pipeline_env, data):
    root, db, rep = pipeline_env
    assert rep.overall_status == "BLOCKED_BY_DATA" and not rep.ready_for_forward_demo
    for stage in ("FAST", "MEDIUM", "STRESS", "FULL"):
        assert rep.stages[stage].status == "BLOCKED_DATA"
        assert "SYNTHETIC_SOURCE_CANNOT_CERTIFY" in rep.stages[stage].reason_codes
    assert rep.stages["HOLDOUT"].status == "NOT_RUN" and rep.stages["FORWARD_DEMO"].status == "INSUFFICIENT_EVIDENCE"
    assert rep.library_recertification["current_flag"] == "RESEARCH_ONLY"
    assert rep.library_recertification["flag_changed_by_certification"] is False
    s, m = data
    again = certify(s, m, cfg=CFG, data_sources=SYNTHETIC_SOURCES, source_provider="synthetic",
                    dataset_identity="synthetic", research_db=db, artifact_dir=root / "art")
    assert again.report_hash == rep.report_hash and again.operational["reused_cached_replay"] is True
    assert ExperimentRegistry(db).trial_count() == 1        # the identical baseline is one experiment
    assert len(CertificationRunStore(db).runs()) == 6       # six stages, stored once each


def test_holdout_is_reserved_before_replay_and_stays_untouched(pipeline_env):
    _root, db, rep = pipeline_env
    hid = rep.replay["holdout_id"]
    st = HoldoutRegistry(db).status(hid)
    assert st["status"] == "RESERVED" and st["first_opened_at"] is None


def test_holdout_requires_a_certifiable_freeze_and_passed_preceding_stages(pipeline_env, data):
    root, db, rep = pipeline_env
    s, m = data
    dirty = certify(s, m, cfg=CFG, data_sources=SYNTHETIC_SOURCES, source_provider="synthetic",
                    dataset_identity="synthetic", research_db=db, artifact_dir=root / "art", open_holdout=True,
                    freeze=build_policy_freeze(certification_policy=default_certification_policy(),
                                               source_commit="abc", source_tree_dirty=True))
    assert "HOLDOUT_REQUIRES_POLICY_FREEZE" in dirty.stages["HOLDOUT"].reason_codes
    clean = certify(s, m, cfg=CFG, data_sources=SYNTHETIC_SOURCES, source_provider="synthetic",
                    dataset_identity="synthetic", research_db=db, artifact_dir=root / "art", open_holdout=True,
                    freeze=build_policy_freeze(certification_policy=default_certification_policy(),
                                               source_commit="abc", source_tree_dirty=False))
    assert "PRECEDING_STAGES_INSUFFICIENT" in clean.stages["HOLDOUT"].reason_codes
    assert HoldoutRegistry(db).status(rep.replay["holdout_id"])["status"] == "RESERVED"  # never consumed prematurely


def test_certification_artifacts_hold_no_secrets(pipeline_env):
    root, _db, rep = pipeline_env
    jpath, mpath = rep.write(root / "out")
    text = jpath.read_text() + mpath.read_text()
    for needle in ("api_key", "apiKey", "secret=", "BINANCE_API", "Bearer ", "password"):
        assert needle not in text
    body = json.loads(jpath.read_text())
    assert body["promotion"] == {"cati_active_execution_enabled": False, "section_25_owns_promotion": True}
    for word in ("guaranteed", "is profitable", "is safe"):
        assert word not in mpath.read_text().lower()


# ============================== CLI ==============================
def _candles_db(path, s):
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE historical_candles (symbol TEXT, interval TEXT, market_type TEXT, open_time INTEGER, "
                 "open REAL, high REAL, low REAL, close REAL, volume REAL, quote_volume REAL, trades INTEGER, "
                 "base_currency TEXT, quote_currency TEXT, data_source TEXT, data_version TEXT)")
    for sym, tfs in s.items():
        for tf, rows in tfs.items():
            conn.executemany("INSERT INTO historical_candles VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                             [(sym, tf, "crypto", r[0], r[1], r[2], r[3], r[4], r[5], r[7], r[8], sym[:-4], "USDT",
                               "synthetic_test", "v1") for r in rows])
    conn.commit()
    conn.close()


def test_cli_plan_reports_blocked_data_without_a_database(tmp_path, capsys):
    from app.trading_intelligence.research.certification.cli import main

    rc = main(["plan", "--db", str(tmp_path / "missing.db"), "--symbols", "BTCUSDT", "--artifacts", str(tmp_path / "a")])
    out = json.loads(capsys.readouterr().out)
    assert rc == 2 and out["status"] == "BLOCKED_DATA" and "backfill_historical_candles.py" in out["backfill"]
    assert out["overall"] == "BLOCKED_BY_DATA" and out["policy_hash"]


def test_cli_plan_status_and_report(tmp_path, capsys, data, pipeline_env):
    from app.trading_intelligence.research.certification.cli import main

    s, _ = data
    _candles_db(tmp_path / "c.db", s)
    assert main(["plan", "--db", str(tmp_path / "c.db"), "--symbols", "BTCUSDT,ETHUSDT",
                 "--label-horizon-bars", "24"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["coverage"]["BTCUSDT"]["rows"] == N and plan["data_sources"]["data_sources"] == ["synthetic_test"]
    assert plan["feasible"]["FULL"] is False and plan["chronology"]["method"] == "CHRONOLOGICAL_WALK_FORWARD_NO_SHUFFLE"
    root, _db, rep = pipeline_env
    rep.write(root / "rep")
    assert main(["report", "--artifacts", str(root / "rep")]) == 0
    assert json.loads(capsys.readouterr().out)["overall"] == "BLOCKED_BY_DATA"
    assert main(["status", "--research-db", str(root / "research.db")]) == 0
    st = json.loads(capsys.readouterr().out)
    assert st["holdout_events"] and st["experiments"] and len(st["certification_runs"]) >= 6
