"""Phase 6: security certification (adversarial tenant isolation), required
fail-closed behaviours, governance (CATI execution stays gated, no phase
auto-advance), shadow capital-routing evidence, metric label safety and
performance guards for 100+ symbol workloads."""
from __future__ import annotations

import inspect
import json
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from shared_lib.broker import BrokerResolverError, resolve_broker_auth
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

NOW = "2026-09-25T00:00:00Z"
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + (tmp_path / "p6.db").as_posix())
    d = DB()
    migrate(d)
    return d


def _account(db, acc, user, broker="bybit", env="demo", perms=None, status="connected"):
    from shared_lib.core.security.broker_security import encrypt_credentials

    with db.connect() as c:
        c.execute("INSERT INTO broker_accounts (id,user_id,broker_id,market_type,label,status,environment,created_at,"
                  "updated_at,active_credential_version) VALUES (?,?,?,?,?,?,?,?,?,1)",
                  (acc, user, broker, "crypto", "t", status, env, NOW, NOW))
        c.execute("INSERT INTO broker_credentials_v2 (account_id,version,status,encrypted_blob,created_at,updated_at,"
                  "permissions_json) VALUES (?,?,?,?,?,?,?)",
                  (acc, 1, "active", encrypt_credentials({"api_key": f"{user}-secret-key", "api_secret": "s"}), NOW, NOW,
                   json.dumps(perms) if perms else None))


# ── 6I adversarial isolation ───────────────────────────────────────────────

def test_user_a_cannot_resolve_trade_or_transfer_on_user_b_account(db):
    from app.core.broker_capability_gate import BrokerCapabilityGateError, assert_broker_execution_capability
    from app.transfers.models import TransferIntent
    from app.transfers.service import InternalTransferService, TransferAccessError

    _account(db, "acc_bob", "bob")
    with pytest.raises(BrokerResolverError) as e:  # read balance / credentials
        resolve_broker_auth("acc_bob", "alice", db)
    assert e.value.reason_code == BrokerResolverError.REASON_ACCESS_DENIED
    with pytest.raises(BrokerCapabilityGateError):  # trade (start a bot) on bob's account
        assert_broker_execution_capability(db, user_id="alice", broker_account_id="acc_bob")
    svc = InternalTransferService(db, adapter_factory=lambda a: pytest.fail("adapter must never be built"))
    with pytest.raises(TransferAccessError):  # move bob's funds
        svc.request_transfer(TransferIntent("alice", "acc_bob", "USDT", Decimal("1"), "FUND", "UNIFIED", "k-00000001"))
    with pytest.raises(TransferAccessError):
        svc.wallets(user_id="alice", account_id="acc_bob")


def test_bot_instance_routes_check_ownership():
    src = inspect.getsource(__import__("app.api.bot_instances", fromlist=["x"]))
    for fn in ("start_bot_instance", "pause_bot_instance"):
        body = src[src.index(f"def {fn}("):]
        body = body[: body.index("\n@router")] if "\n@router" in body else body
        assert 'instance.user_id != user["id"]' in body, fn  # cancel/stop another user's bot is refused


def test_credentials_never_leave_in_api_or_logs(db, caplog):
    from app.transfers.service import InternalTransferService

    _account(db, "acc_a", "alice", perms={"permissions": {"INTERNAL_TRANSFER": True, "WITHDRAW": False},
                                          "inspected": True, "broker": "bybit", "source": "t"})
    svc = InternalTransferService(db, adapter_factory=lambda a: None)
    caps = svc.capabilities(user_id="alice", account_id="acc_a")
    assert "alice-secret-key" not in json.dumps(caps, default=str)
    import logging
    with caplog.at_level(logging.DEBUG):
        logging.getLogger("x").warning("req url https://api.bybit.com/v5/x?api_key=alice-secret-key&sign=abc")
    from shared_lib.core.security.redaction import install_log_redaction
    install_log_redaction(logging.getLogger("x"))
    with caplog.at_level(logging.DEBUG):
        caplog.clear()
        logging.getLogger("x").warning("req url https://api.bybit.com/v5/x?api_key=alice-secret-key&sign=abc")
    assert "alice-secret-key" not in caplog.text


# ── 6K required fail-closed behaviour ──────────────────────────────────────

def test_permission_change_stops_new_entries(db):
    from app.core.broker_capability_gate import readiness_for_account

    _account(db, "acc_a", "alice", broker="binance", env="demo")
    assert readiness_for_account(db, user_id="alice", broker_account_id="acc_a").permitted
    with db.connect() as c:  # re-validation discovered withdraw permission on the active key
        c.execute("UPDATE broker_credentials_v2 SET permissions_json=? WHERE account_id='acc_a'",
                  (json.dumps({"permissions": {"WITHDRAW": True, "TRADE": True}}),))
    r = readiness_for_account(db, user_id="alice", broker_account_id="acc_a")
    assert not r.permitted and r.reason_code == "API_KEY_WITHDRAW_PERMISSION_PRESENT"
    with db.connect() as c:
        c.execute("UPDATE broker_credentials_v2 SET permissions_json=? WHERE account_id='acc_a'",
                  (json.dumps({"permissions": {"TRADE": False}}),))
    assert not readiness_for_account(db, user_id="alice", broker_account_id="acc_a").permitted


def test_invalid_credentials_and_unknown_transfer_and_missing_fx_reference_fail_closed(db):
    from app.trading_intelligence.capital.planner import (
        AccountCapitalState, CapitalSettings, is_fundable, plan_capital,
    )
    from app.trading_intelligence.contracts.instrument import from_fx_pair
    from app.trading_intelligence.fx.context import build_fx_context
    from shared_lib.broker.wallets import topology_for

    _account(db, "acc_rev", "alice", status="revoked")
    with pytest.raises(BrokerResolverError):
        resolve_broker_auth("acc_rev", "alice", db)
    plan = plan_capital(state=AccountCapitalState("a", "USDT", topology_for("binance"),
                                                  {"UMFUTURE": Decimal("0"), "FUNDING": Decimal("1000")},
                                                  transfer_capability_usable=True),
                        product="CRYPTO_PERPETUAL", required=Decimal("100"), settings=CapitalSettings(), plan_key="x")
    for status in (None, "SUBMITTED", "CONFIRMATION_PENDING", "UNKNOWN", "RECONCILIATION_REQUIRED", "FAILED"):
        assert not is_fundable(plan, status)  # no dependent trade until COMPLETED
    key = from_fx_pair(venue="bybit_linear", venue_symbol="EURUSDT", base="EUR", quote="USD")
    ctx = build_fx_context(instrument_key=key, as_of_ms=int(time.time() * 1000), reference=None)
    assert not ctx.tradable  # FX reference unavailable -> no FX entry


def test_existing_positions_keep_protection_when_new_entries_are_refused():
    """The capability gate only refuses NEW bots/entries: the runner's quarantine
    path keeps the bot row active so managed positions stay reconciled."""
    src = (ROOT / "app/runner/multi_runner.py").read_text()
    gate = src[src.index("A'. Execution-capability gate"):src.index("# Build credentials dict")]
    assert "quarantine_bot" in gate and "status = 'stopped'" not in gate and "close_position" not in gate


# ── 6H governance: nothing here grants runtime authority ───────────────────

def test_cati_execution_remains_disabled_and_phases_do_not_auto_advance(monkeypatch):
    from app.trading_intelligence.execution.config import CATIExecutionConfig, is_active_execution_enabled

    monkeypatch.delenv("CATI_ACTIVE_EXECUTION_ENABLED", raising=False)
    assert not is_active_execution_enabled() and CATIExecutionConfig().active_execution_enabled is False
    new_code = "\n".join(p.read_text() for p in [
        *(ROOT / "app/transfers").glob("*.py"), *(ROOT / "app/market_data").glob("*.py"),
        *(ROOT / "app/trading_intelligence/capital").glob("*.py"), ROOT / "app/trading_intelligence/fx/context.py",
        ROOT / "app/trading_intelligence/economics/fx_perp.py", ROOT / "app/ops/multi_asset_metrics.py",
        ROOT / "app/exchange/instruments.py", ROOT / "app/exchange/contract.py",
        ROOT / "app/trading_intelligence/research/certification/scopes.py",
        ROOT / "app/trading_intelligence/research/certification/portfolio_sim.py"])
    import re

    # no new code reads, sets or imports the active-execution switch
    assert not re.search(r"environ\[?[^\n]*CATI_ACTIVE_EXECUTION_ENABLED|setenv\([^\n]*CATI_ACTIVE|ENV_ACTIVE_EXECUTION|"
                         r"is_active_execution_enabled|active_execution_enabled\s*=\s*True", new_code)
    assert not re.search(r"HoldoutRegistry\(|\.open\(\s*holdout|holdouts\.open", new_code)  # no holdout is opened
    from app.trading_intelligence.venue.registry import ADAPTER_STATUS_REGISTRY

    assert set(ADAPTER_STATUS_REGISTRY) == {("binance_usdm", "DEMO"), ("binance_usdm", "TESTNET"), ("binance_usdm", "REAL")}


# ── 6F shadow capital-routing evidence ─────────────────────────────────────

def test_shadow_capital_plan_evidence_is_account_scoped_and_append_only(db):
    import sqlite3

    from app.trading_intelligence.capital.evidence import CapitalPlanEvidenceStore
    from app.trading_intelligence.capital.planner import AccountCapitalState, CapitalSettings, plan_capital
    from shared_lib.broker.wallets import topology_for

    plan = plan_capital(state=AccountCapitalState("acc_a", "USDT", topology_for("binance"),
                                                  {"UMFUTURE": Decimal("0"), "FUNDING": Decimal("1000")},
                                                  transfer_capability_usable=True),
                        product="CRYPTO_PERPETUAL", required=Decimal("100"),
                        settings=CapitalSettings(mode="AUTOMATED_INTERNAL_REALLOCATION", auto_rebalance_enabled=True,
                                                 authorized=True), plan_key="op1")
    store = CapitalPlanEvidenceStore(db)
    row = store.record(plan, user_id="alice", bot_instance_id="b", cycle_id="c", opportunity_id="op1", seed=1,
                       transferable_at_plan=1000)
    assert row["mode"] == "SHADOW" and json.loads(row["simulated_transfer_json"])["final_status"] in ("COMPLETED", "FAILED")
    assert store.list(user_id="mallory", broker_account_id="acc_a") == []
    with db.connect() as c, pytest.raises(sqlite3.IntegrityError):
        c.execute("DELETE FROM cati_capital_plan_evidence")


# ── 6J metrics ─────────────────────────────────────────────────────────────

def test_multi_asset_metrics_never_carry_identifiers():
    from app.ops import multi_asset_metrics as mm
    from app.trading_intelligence.observability.metrics import METRICS

    mm.transfer("bybit_linear", "BLOCKED", "WITHDRAW_PERMISSION_PRESENT")
    mm.capability_block("bingx_swap", "BROKER_EXECUTION_UNVALIDATED_FOR_LIVE")
    mm.transfer("0e7d1b8e-0000-4000-8000-000000000001", "COMPLETED")  # id-like: dropped, never raises
    snap = json.dumps(METRICS.snapshot())
    assert "broker_transfer_total" in snap and "0e7d1b8e" not in snap


# ── 6L performance guards ──────────────────────────────────────────────────

def test_multi_symbol_workloads_stay_linear():
    from app.exchange.instruments import parse_binance_symbol
    from app.market_data.resample import resample_all
    from app.market_data.universe import select_universe

    t0 = 1_767_225_600_000
    rows = [[t0 + i * 60_000, "1", "2", "0.5", "1.5", "1", t0 + (i + 1) * 60_000 - 1] for i in range(20_160)]  # 14d 1m
    start = time.perf_counter()
    out = resample_all(rows, ["5m", "15m", "1h", "4h"])
    assert len(out["15m"]["rows"]) == 1344 and time.perf_counter() - start < 5.0
    symbols = [{"symbol": f"C{i}USDT", "baseAsset": f"C{i}", "quoteAsset": "USDT", "status": "TRADING",
                "contractType": "PERPETUAL", "onboardDate": 1, "filters": []} for i in range(600)]
    ins = [parse_binance_symbol(s) for s in symbols]
    stats = {i.venue_symbol: SimpleNamespace(quote_volume_24h=1e8, spread_bps=1.0) for i in ins}
    start = time.perf_counter()
    sel = select_universe(ins, stats, venue="binance_usdm", as_of_ms=t0)
    assert len(sel.selected) == 150 and time.perf_counter() - start < 1.0


def test_certification_future_row_lookup_still_uses_binary_search():
    import inspect as _i

    from app.research import dataset

    # commit 9316a86: future_rows() locates the decision bar with bisect_right
    # instead of scanning the whole history per decision (O(N^2) replay).
    assert "bisect_right" in _i.getsource(dataset.future_rows)
    rows = [[i * 60_000, 1, 1, 1, 1, 1, (i + 1) * 60_000 - 1] for i in range(200_000)]
    start = time.perf_counter()
    for t in range(0, 200_000 * 60_000, 60_000 * 997):
        dataset.future_rows(rows, t, 4)
    assert time.perf_counter() - start < 2.0


def test_capital_routing_shadow_hook_is_off_by_default_and_evidence_only(db, monkeypatch):
    from shared_lib.broker.wallets import topology_for

    from app.trading_intelligence.capital import shadow_hook as sh

    _account(db, "acc_a", "alice", broker="binance", perms={"permissions": {"INTERNAL_TRANSFER": True, "WITHDRAW": False},
                                                            "inspected": True, "broker": "binance", "source": "t"})
    decision = SimpleNamespace(broker_account_id="acc_a", selected_opportunity_ids=("opp1",), cycle_id="cyc",
                               bot_instance_id="bot1")
    runner = SimpleNamespace(db=db, context=SimpleNamespace(user_id="alice", trade_usdt_per_order=500, max_leverage=5,
                                                             broker_type="binance"))
    monkeypatch.delenv(sh.ENV_FLAG, raising=False)
    assert sh.shadow_capital_routing(runner, SimpleNamespace(decision=decision), {}) == []
    monkeypatch.setenv(sh.ENV_FLAG, "1")
    adapter = MagicMock()
    adapter.topology.return_value = topology_for("binance")
    adapter.transferable.side_effect = lambda w, a: {"UMFUTURE": Decimal("10"), "FUNDING": Decimal("1000")}.get(w.native_type)
    monkeypatch.setattr("app.transfers.adapters.adapter_for", lambda auth: adapter)
    rows = sh.shadow_capital_routing(runner, SimpleNamespace(decision=decision), {})
    assert len(rows) == 1 and rows[0]["outcome"] == "PHYSICAL_INTERNAL_TRANSFER_REQUIRED" and rows[0]["mode"] == "SHADOW"
    assert not adapter.submit.called  # evidence only: nothing is ever submitted
