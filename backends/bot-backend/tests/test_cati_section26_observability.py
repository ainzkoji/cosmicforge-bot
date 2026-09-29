"""CATI Section 26: structured events, metrics, alerts on the existing alert store, persisted reason codes, lineage,
redaction, and the deterministic research boundary."""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone

import pytest

from test_phase2b_permissions_transfers import (  # noqa: F401  (db is a fixture)
    SAFE_EVIDENCE, FakeAdapter, _account, _intent, _service, db,
)

from app.ops import multi_asset_alerts as A


def _iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _stage_events(caplog):
    return [json.loads(r.getMessage().split("[CATI_STAGE] ", 1)[1]) for r in caplog.records
            if "[CATI_STAGE]" in r.getMessage()]


# ---------------------------------------------------------------- 26.1 / 26.2 / 26.5 events + lineage + reasons
def test_transfer_plan_and_unknown_emit_structured_events_with_lineage(db, caplog):
    caplog.set_level(logging.INFO)
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)

    def timeout(**k):
        raise TimeoutError("read timed out ?signature=deadbeefcafebabe1234")
    svc = _service(db, FakeAdapter(submit=timeout))
    from decimal import Decimal
    svc.plan_transfer(user_id="alice", account_id="acc_a", asset="USDT", amount=Decimal("5"), source_wallet="FUND",
                      destination_wallet="CONTRACT")
    row = svc.request_transfer(_intent(key="evt-0001"))
    ev = _stage_events(caplog)
    plan = next(e for e in ev if e["component"] == "multi_asset.transfer_plan")
    unknown = next(e for e in ev if e["component"] == "multi_asset.transfer" and e["status"] == "UNKNOWN")
    for e in (plan, unknown):
        assert e["user_id"] == "alice" and e["broker_account_id"] == "acc_a"
    assert unknown["reason_codes"] == ["SUBMIT_OUTCOME_UNKNOWN"] and unknown["extra"]["transfer_id"] == row["id"]
    assert "deadbeefcafebabe1234" not in json.dumps(ev)
    with db.connect() as c:  # the reason is persisted, not only logged
        assert c.execute("SELECT failure_reason FROM broker_transfer_requests WHERE id=?", (row["id"],)).fetchone()[0] \
            == "SUBMIT_OUTCOME_UNKNOWN"


# ---------------------------------------------------------------- 26.4 alerts
def _seed_alert_conditions(db, now_ms):
    old = _iso(now_ms - 60 * 60_000)
    with db.connect() as c:
        c.execute("INSERT INTO broker_accounts (id,user_id,broker_id,market_type,label,status,environment,created_at,"
                  "updated_at,active_credential_version) VALUES ('acc_x','ux','bybit','crypto','t','connected','demo',?,?,1)",
                  (old, old))
        c.execute("INSERT INTO broker_transfer_requests (id,user_id,broker_account_id,broker,environment,asset,amount,"
                  "source_wallet,destination_wallet,idempotency_key,status,requested_at,created_at,updated_at) VALUES "
                  "('t-stuck','ux','acc_x','bybit','DEMO','USDT','5','FUND','CONTRACT','k-stuck','UNKNOWN',?,?,?)",
                  (old, old, old))
        c.execute("INSERT INTO broker_transfer_reconciliations (id,user_id,broker_account_id,broker,run_at,still_unresolved,"
                  "status) VALUES ('r1','ux','acc_x','bybit',?,1,'OK')", (old,))
        c.execute("INSERT INTO cati_portfolio_reservations (reservation_id, broker_account_id, bot_instance_id, cycle_id, "
                  "selected_candidate_ids, selected_instruments, status, mode, created_at, expires_at, updated_at, "
                  "reservation_version, resolution_deadline) VALUES ('resv-1','acc_x','b','c','[]','[]',"
                  "'RESOLUTION_PENDING','SHADOW',1,2,1,'v',?)", (now_ms - 1,))


def test_alert_conditions_fire_once_with_reason_and_lineage(db, monkeypatch, tmp_path):
    from app.trading_intelligence.observability.metrics import METRICS

    monkeypatch.setenv("CATI_RESEARCH_DATA_DIR", str(tmp_path / "none"))
    now = int(time.time() * 1000)
    _seed_alert_conditions(db, now)
    alerts = A.evaluate(db, now_ms=now)
    kinds = {a.alert_type for a in alerts}
    assert {A.TRANSFER_RESOLUTION_STUCK, A.ORDER_RESOLUTION_STUCK, A.RECONCILIATION_MISMATCH,
            A.INSTRUMENT_METADATA_STALE} <= kinds
    stuck = next(a for a in alerts if a.alert_type == A.TRANSFER_RESOLUTION_STUCK)
    assert stuck.user_id == "ux" and stuck.broker_account_id == "acc_x" and stuck.reason_code == "TRANSFER_UNKNOWN_UNRESOLVED"
    before = METRICS.counter("cati_multi_asset_alert_total", component=A.TRANSFER_RESOLUTION_STUCK, status="HIGH")
    written = A.emit(db, alerts)
    assert written == len(alerts) and A.emit(db, A.evaluate(db, now_ms=now)) == 0  # de-duplicated, no flood
    assert METRICS.counter("cati_multi_asset_alert_total", component=A.TRANSFER_RESOLUTION_STUCK, status="HIGH") == before + 1
    with db.connect() as c:
        rows = c.execute("SELECT alert_type, severity, details_json FROM alerts WHERE alert_type=?",
                         (A.TRANSFER_RESOLUTION_STUCK,)).fetchall()
    assert len(rows) == 1 and json.loads(rows[0][2])["reason_code"] == "TRANSFER_UNKNOWN_UNRESOLVED"


def test_capability_regression_alert_for_held_instrument(db, monkeypatch, tmp_path):
    from app.exchange.instruments import DiscoveredInstrument, InstrumentCatalog

    monkeypatch.setenv("CATI_RESEARCH_DATA_DIR", str(tmp_path / "none"))
    now = int(time.time() * 1000)
    with db.connect() as c:
        c.execute("INSERT INTO broker_accounts (id,user_id,broker_id,market_type,label,status,environment,created_at,"
                  "updated_at) VALUES ('acc_h','uh','bybit','crypto','t','connected','demo','x','x')")
        c.execute("INSERT INTO bot_instances (id,user_id,broker_account_id,market_type,strategy_id,mode,status,created_at,"
                  "updated_at) VALUES ('bot_h','uh','acc_h','crypto','s','paper','active','x','x')")
        c.execute("INSERT INTO positions (position_id,bot_instance_id,symbol,side,original_qty,remaining_qty,entry_price,"
                  "status,opened_at) VALUES ('p1','bot_h','GONEUSDT','LONG',1,1,10,'OPEN','x')")
    ins = lambda s: DiscoveredInstrument("bybit_linear", s, "CRYPTO", "PERPETUAL", f"{s[:-4]}/USDT:PERP", s[:-4],  # noqa: E731
                                         "USDT", "USDT", "PERPETUAL", "Trading", True, 0.1, 0.001, 0.001, 100.0, 5.0, 10.0)
    cat = InstrumentCatalog(db)
    cat.upsert("bybit_linear", "DEMO", [ins("GONEUSDT"), ins("BTCUSDT")], now - 10)
    cat.upsert("bybit_linear", "DEMO", [ins("BTCUSDT")], now)  # GONEUSDT delisted while held
    reg = [a for a in A.evaluate(db, now_ms=now) if a.alert_type == A.CAPABILITY_REGRESSION]
    assert len(reg) == 1 and reg[0].symbol == "GONEUSDT" and reg[0].reason_code == "INSTRUMENT_DELISTED"
    assert reg[0].broker_account_id == "acc_h" and reg[0].user_id == "uh"


def test_research_data_gap_and_stall_alerts(monkeypatch, tmp_path):
    """A temporary FX research DB (never the live one): FAILED periods = unexplained gap; no recent progress = stall."""
    from app.market_data import research_status as rs

    d = tmp_path / "research"
    d.mkdir()
    con = sqlite3.connect(d / "fx_reference_dukascopy.db")
    con.execute("CREATE TABLE fx_reference_ingest_log (provider TEXT, pair TEXT, timeframe TEXT, period TEXT, side TEXT, "
                "status TEXT, rows INTEGER, reason TEXT, recorded_at INTEGER)")
    con.execute("CREATE TABLE fx_reference_quotes (provider TEXT, pair TEXT, timeframe TEXT, open_time INTEGER)")
    stale = int(time.time() * 1000) - 12 * 3_600_000
    con.executemany("INSERT INTO fx_reference_ingest_log VALUES (?,?,?,?,?,?,?,?,?)", [
        ("dukascopy", "EURUSD", "1m", "2024-08-01", "BID", "FETCHED", 1, None, stale),
        ("dukascopy", "EURUSD", "1m", "2024-08-02", "BID", "FAILED", 0, "FETCH_FAILED", stale)])
    con.commit()
    con.close()
    monkeypatch.setenv("CATI_RESEARCH_DATA_DIR", str(d))
    rs.reset_cache()
    alerts = A._research_alerts(int(time.time() * 1000), A.AlertPolicy())
    kinds = {a.alert_type: a for a in alerts}
    assert kinds[A.UNEXPLAINED_DATA_GAP].details["failed_periods"] == 1
    assert A.DATASET_ACQUISITION_STALLED in kinds and kinds[A.DATASET_ACQUISITION_STALLED].user_id is None  # global


def test_reference_venue_divergence_alert():
    assert A.divergence_alert("bybit_linear", "EURUSDT", 12.0) is None
    a = A.divergence_alert("bybit_linear", "EURUSDT", -80.0)
    assert a.alert_type == A.REFERENCE_VENUE_DIVERGENCE and a.reason_code.endswith("ABOVE_THRESHOLD")


# ---------------------------------------------------------------- 26.3 metrics: bounded labels only
def test_metrics_refuse_id_like_labels():
    from app.trading_intelligence.observability.metrics import METRICS, MetricLabelError

    with pytest.raises(MetricLabelError):
        METRICS.inc("cati_multi_asset_alert_total", broker_account_id="acc_123456789")
    METRICS.inc("broker_transfer_total", venue="bybit", status="UNKNOWN", reason_family="SUBMIT")


# ---------------------------------------------------------------- 26.6 deterministic research boundary
def test_operational_timestamps_never_change_research_identity(monkeypatch):
    from pathlib import Path

    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parent / "trading_intelligence"))
    from test_cati_section13_closure import audit, frozen

    from app.trading_intelligence.market_state.global_state import build_global_market_state
    from app.trading_intelligence.research.certification.holdout_guard import pre_holdout_readiness

    a = frozen(created_at="2026-01-01T00:00:00Z")["manifest_hash"]
    p1 = audit()["partition_hash"]
    g1 = build_global_market_state([], decision_time=1_000, timeframe="15m").state_hash
    r1 = pre_holdout_readiness(dataset_manifest=None, acquisition_state="ACQUIRING")["readiness_hash"]
    monkeypatch.setattr(time, "time", lambda: 9_999_999_999.0)  # a completely different wall clock
    assert frozen(created_at="2099-12-31T23:59:59Z")["manifest_hash"] == a
    assert audit()["partition_hash"] == p1
    assert build_global_market_state([], decision_time=1_000, timeframe="15m").state_hash == g1
    assert pre_holdout_readiness(dataset_manifest=None, acquisition_state="ACQUIRING")["readiness_hash"] == r1
