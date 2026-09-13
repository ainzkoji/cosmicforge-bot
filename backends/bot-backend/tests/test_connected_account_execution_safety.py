"""KYC and real-capital readiness are decided by the CONNECTED ACCOUNT, not the bot's mode.

``execution_mode = live`` means "execute through the connected broker account".
Whether real money is at stake is a property of that account
(``broker_accounts.environment``), resolved through the broker resolver. Both
gates are real-capital protections:

    live account          -> KYC evaluated, readiness approval mandatory, fail closed
    demo / testnet account -> KYC NOT_REQUIRED, readiness NOT_REQUIRED_FOR_DEMO_EXECUTION
    unknown environment    -> treated as live

And the KYC source is explicit about failure: a missing KYC schema is
UNAVAILABLE, never "rejected". The schema itself was dropped in the 2026-05-02
repository cleanup and is restored by the canonical migrations.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.product_safety.execution_safety import (
    ReadinessGateState,
    classify_connected_account,
    evaluate_execution_kyc,
    evaluate_execution_readiness,
)
from app.product_safety.readiness_gate import ReadinessStatus
from shared_lib.broker.environment import BrokerEnvironment, resolve_base_url
from shared_lib.core.policy.kyc_policy import (
    KYCAction,
    KYCGateStatus,
    check_kyc_gate,
    evaluate_kyc_gate,
)
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

USER = "user-exec-safety"
BOT = "bot_exec_safety"
ACCOUNT = "brk_exec_safety"
LIVE_TRADING = KYCAction.START_LIVE_TRADING.value


def _must_not_run(*_args, **_kwargs):
    raise AssertionError("a real-capital check ran for a demo/test account")


@pytest.fixture
def migrated_db(tmp_path, monkeypatch):
    """A database built by the canonical migrations (KYC schema included)."""
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + (tmp_path / "migrated.db").as_posix())
    db = DB()
    migrate(db)
    return db


@pytest.fixture
def pre_fix_db(tmp_path, monkeypatch):
    """The schema the runtime actually had: base tables, no KYC tables."""
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + (tmp_path / "pre_fix.db").as_posix())
    db = DB()
    with db.connect() as conn:
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "kyc_requirements_config" not in names
    return db


def _kyc_case(db, status: str) -> None:
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO kyc_cases (id, user_id, status, created_at, updated_at) "
            "VALUES (?, ?, ?, '2026-01-01', '2026-01-01')",
            (f"case_{status}", USER, status),
        )


def _runner(environment, *, db=None, extra_context: dict | None = None):
    """A PaperRunner with just the context the execution-safety decision reads."""
    from app.runner.runner import PaperRunner

    runner = object.__new__(PaperRunner)
    runner.context = SimpleNamespace(
        execution_mode="broker", broker_environment=environment,
        user_id=USER, bot_instance_id=BOT, broker_account_id=ACCOUNT,
        **(extra_context or {}),
    )
    runner.db = db
    return runner


# ── The restored KYC schema ─────────────────────────────────────────────────


def test_the_migration_restores_the_kyc_schema_with_requirements_and_no_approvals(migrated_db):
    with migrated_db.connect() as conn:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        requirement = conn.execute(
            "SELECT requires_kyc, required_status, description FROM kyc_requirements_config "
            "WHERE action_name='start_live_trading'"
        ).fetchone()
        cases = conn.execute("SELECT COUNT(*) FROM kyc_cases").fetchone()[0]
    assert {"kyc_cases", "kyc_requirements_config", "kyc_documents", "kyc_audit_log"} <= tables
    assert tuple(requirement) == (1, "approved", "Required for live trading with real funds")
    assert cases == 0, "the migration seeds requirement policy, never a KYC case or approval"


# ── KYC ─────────────────────────────────────────────────────────────────────


def test_1_kyc_required_and_approved_passes(migrated_db):
    _kyc_case(migrated_db, "approved")
    result = evaluate_kyc_gate(USER, LIVE_TRADING)
    assert result.status == KYCGateStatus.APPROVED
    assert result.allowed is True
    assert check_kyc_gate(USER, LIVE_TRADING) == (True, "OK")
    decision = evaluate_execution_kyc(user_id=USER, broker_environment="live")
    assert (decision.state, decision.allowed) == ("APPROVED", True)


@pytest.mark.parametrize("case_status", [None, "in_progress", "rejected"])
def test_2_kyc_required_and_not_approved_blocks(migrated_db, case_status):
    if case_status:
        _kyc_case(migrated_db, case_status)
    result = evaluate_kyc_gate(USER, LIVE_TRADING)
    assert result.status == KYCGateStatus.REQUIRED_NOT_APPROVED
    assert result.allowed is False
    decision = evaluate_execution_kyc(user_id=USER, broker_environment="live")
    assert (decision.state, decision.allowed) == ("REQUIRED_NOT_APPROVED", False)


def test_3_kyc_required_but_source_unavailable_fails_closed(pre_fix_db):
    result = evaluate_kyc_gate(USER, LIVE_TRADING)
    assert result.status == KYCGateStatus.UNAVAILABLE
    assert result.allowed is False
    allowed, message = check_kyc_gate(USER, LIVE_TRADING)
    assert allowed is False and message.startswith("KYC_UNAVAILABLE")
    decision = evaluate_execution_kyc(user_id=USER, broker_environment="live")
    assert (decision.state, decision.allowed) == ("UNAVAILABLE", False)


def test_4_kyc_not_required_is_not_required(migrated_db):
    result = evaluate_kyc_gate(USER, KYCAction.DEVELOPER_API_ACCESS.value)
    assert result.status == KYCGateStatus.NOT_REQUIRED
    assert result.allowed is True


def test_4b_a_demo_account_is_not_required_and_never_reads_kyc(pre_fix_db):
    decision = evaluate_execution_kyc(
        user_id=USER, broker_environment="demo", kyc_evaluator=_must_not_run,
    )
    assert decision.state == "NOT_REQUIRED"
    assert decision.state != "APPROVED", "demo execution is NOT_REQUIRED, not APPROVED"
    assert decision.allowed is True
    assert decision.real_capital is False


def test_5_a_missing_kyc_table_is_unavailable_not_rejected(pre_fix_db):
    result = evaluate_kyc_gate(USER, LIVE_TRADING)
    assert result.status == KYCGateStatus.UNAVAILABLE
    assert result.status != KYCGateStatus.REQUIRED_NOT_APPROVED

    engine = _safety_engine()
    unavailable = engine.check_pre_trade(
        "cfg", "BTCUSDT", 0.9, 1, 1000, 0, is_live_mode=True,
        user_kyc_approved=False, live_readiness_approved=True, kyc_status="UNAVAILABLE",
    )
    rejected = engine.check_pre_trade(
        "cfg", "BTCUSDT", 0.9, 1, 1000, 0, is_live_mode=True,
        user_kyc_approved=False, live_readiness_approved=True, kyc_status="REQUIRED_NOT_APPROVED",
    )
    assert unavailable.allowed is False and unavailable.block_reason.name == "KYC_UNAVAILABLE"
    assert rejected.allowed is False and rejected.block_reason.name == "KYC_REQUIRED"


# ── Real-capital readiness ──────────────────────────────────────────────────


@pytest.mark.parametrize("environment", ["demo", "testnet"])
def test_6_7_a_demo_or_testnet_account_does_not_need_production_readiness(migrated_db, environment):
    decision = evaluate_execution_readiness(
        db=migrated_db, bot_instance_id=BOT, broker_environment=environment,
    )
    assert decision.state == ReadinessGateState.NOT_REQUIRED_FOR_DEMO_EXECUTION
    assert decision.state == ReadinessStatus.NOT_REQUIRED_FOR_DEMO_EXECUTION.value
    assert decision.allowed is True
    assert decision.real_capital is False


def test_8_a_real_money_account_without_approval_is_blocked(migrated_db):
    decision = evaluate_execution_readiness(
        db=migrated_db, bot_instance_id=BOT, broker_environment="live",
    )
    assert decision.allowed is False
    assert decision.state == ReadinessGateState.NOT_MET
    assert "USER_CAPITAL_READINESS_NOT_MET" in decision.reason


def test_9_a_real_money_account_with_approval_passes():
    decision = evaluate_execution_readiness(
        db=object(), bot_instance_id=BOT, broker_environment="live",
        asserter=lambda **_: None,
    )
    assert (decision.state, decision.allowed) == (ReadinessGateState.APPROVED_FOR_CONTROLLED_BETA, True)


@pytest.mark.parametrize("environment", [None, "", "somewhere-else"])
def test_an_unknown_account_environment_is_treated_as_real_capital(environment):
    assert classify_connected_account(environment).real_capital is True
    readiness = evaluate_execution_readiness(
        db=None, bot_instance_id=BOT, broker_environment=environment,
    )
    assert readiness.allowed is False


# ── The connected account is the source of truth ────────────────────────────


def test_10_the_environment_comes_from_broker_account_resolution():
    from app.runner.bot_context import BotRunContext

    source = Path("app/runner/multi_runner.py").read_text(encoding="utf-8")
    assert "resolve_broker_auth_for_bot(" in source
    assert "broker_environment=auth.environment.value" in source

    from app.core.bot_instance_service import BotInstanceService
    from app.models.bot_instance_models import BotInstance
    from app.runner.effective_policy import resolve_effective_bot_policy

    instance = BotInstance(
        id=BOT, user_id=USER, broker_account_id=ACCOUNT, market_type="CRYPTO",
        strategy_id="master_ensemble", strategy_version="1.0.0", risk_level="balanced",
        symbols=["BTCUSDT", "ETHUSDT"], timeframes=["15m"], allocation_type="fixed_amount",
        allocation_value=120.0, mode="live", capital_allocation=120.0,
        capital_allocation_type="fixed_amount",
    )
    policy = resolve_effective_bot_policy(
        instance=instance,
        broker_environment="demo",  # what the resolver read from broker_accounts
        risk_params=BotInstanceService.get_risk_profile_preset("balanced"),
    )
    assert policy.execution_mode == "broker"
    assert policy.broker_environment == "demo"
    context = BotRunContext.from_effective_policy(policy, {})
    assert context.broker_environment == "demo"
    assert resolve_base_url("binance", BrokerEnvironment.DEMO) == "https://demo-fapi.binance.com"


def test_11_the_bot_has_no_environment_of_its_own(migrated_db):
    from app.models.bot_instance_models import BotInstance

    fields = set(getattr(BotInstance, "model_fields", {}) or getattr(BotInstance, "__dataclass_fields__", {}))
    with migrated_db.connect() as conn:
        columns = {r[1] for r in conn.execute("PRAGMA table_info(bot_instances)")}
    for forbidden in ("environment", "demo_mode", "testnet_mode", "mainnet_mode"):
        assert forbidden not in fields
        assert forbidden not in columns

    # Even a stray environment-looking attribute on the context is ignored:
    # only the resolved connected-account environment is read.
    runner = _runner("demo", extra_context={"environment": "live", "mode": "live"})
    snapshot = runner._execution_safety()
    assert snapshot["account_environment"] == "demo"
    assert snapshot["kyc"]["state"] == "NOT_REQUIRED"


def test_12_global_settings_cannot_override_the_connected_account(monkeypatch):
    from app.core.config import settings

    monkeypatch.setattr(settings, "BINANCE_ENV", "live", raising=False)
    monkeypatch.setattr(settings, "BINANCE_FAPI_BASE_URL", "https://fapi.binance.com", raising=False)
    monkeypatch.setenv("BINANCE_ENV", "live")

    demo = _runner("demo")._execution_safety()
    assert demo["real_capital"] is False
    assert demo["kyc"]["state"] == "NOT_REQUIRED"
    assert demo["readiness"]["state"] == "NOT_REQUIRED_FOR_DEMO_EXECUTION"

    monkeypatch.setattr(settings, "BINANCE_ENV", "testnet", raising=False)
    live = _runner("live", db=None)._execution_safety()
    assert live["real_capital"] is True
    assert live["readiness"]["allowed"] is False

    from shared_lib.broker.client_factory import build_client_from_auth
    from shared_lib.broker.resolver import BrokerAuth

    auth = BrokerAuth(
        account_id=ACCOUNT, user_id=USER, broker_type="binance",
        environment=BrokerEnvironment.DEMO,
        base_url=resolve_base_url("binance", BrokerEnvironment.DEMO),
        api_key="k" * 16, api_secret="s" * 16, credential_version=1, key_fingerprint="kkkk",
    )
    client = build_client_from_auth(auth)
    assert "demo-fapi.binance.com" in str(getattr(client, "base_url", ""))


def test_paper_execution_is_unaffected():
    runner = _runner("live")
    runner.context.execution_mode = "paper"
    snapshot = runner._execution_safety()
    assert snapshot["scope"] == "PAPER"
    assert runner._runtime_kyc_allowed() is True
    assert runner._runtime_live_readiness_allowed() is True


# ── End to end: approved decision -> KYC -> readiness -> capital -> executor ─


def _safety_engine():
    from app.risk.safety_engine import SafetyConfig, SafetyEngine

    db = MagicMock()
    conn = MagicMock()
    db.connect.return_value.__enter__.return_value = conn

    def execute(query, *args, **kwargs):
        cursor = MagicMock()
        text = str(query)
        if "daily_trade_counts" in text:
            cursor.fetchone.return_value = {"trade_count": 0}
        elif "order_failures" in text:
            cursor.fetchone.return_value = {"paused_until": None}
        else:
            cursor.fetchone.return_value = None
        cursor.fetchall.return_value = []
        return cursor

    conn.execute.side_effect = execute
    return SafetyEngine(db, MagicMock(), MagicMock(), SafetyConfig())


def _pre_trade(snapshot):
    return _safety_engine().check_pre_trade(
        "cfg", "BTCUSDT", 0.9, 1, 1000, 0, is_live_mode=True,
        user_kyc_approved=bool(snapshot["kyc"]["allowed"]),
        live_readiness_approved=bool(snapshot["readiness"]["allowed"]),
        kyc_status=snapshot["kyc"]["state"],
        live_readiness_status=snapshot["readiness"]["state"],
    )


def _live_executor(db, client):
    from app.execution.executor import BinanceExecutor

    executor = BinanceExecutor(
        client=client, execution_mode="live", live_symbols=["BTCUSDT"],
        bot_instance_id=BOT, db=db,
    )
    executor.run_id = "run-e2e"
    executor._capital_budget = 120.0
    executor._allocation_type = "fixed_amount"
    executor._allocation_value = 100.0
    executor._max_notional_per_symbol = 100.0
    executor._allow_scale_in = False
    executor._allow_hedge_mode = False
    return executor


def _connected_demo_adapter():
    """The connected broker adapter, at the network boundary."""
    entry = SimpleNamespace(
        broker_order_id="DEMO-ENTRY-1", avg_fill_price=50_000.0, qty_filled=0.002,
        model_dump=lambda: {"orderId": "DEMO-ENTRY-1", "status": "FILLED",
                            "executedQty": "0.002", "avgPrice": "50000.0",
                            "updateTime": 1700000000000},
    )
    client = MagicMock()
    client.get_position_info.return_value = {"positionAmt": "0.0"}
    client.account.return_value = {
        "availableBalance": "1000.0", "totalWalletBalance": "1000.0",
        "totalMaintMargin": "0.0", "totalInitialMargin": "0.0",
    }
    client.get_prices.return_value = {"BTCUSDT": 50_000.0}
    client.get_klines.return_value = [[0, 0, 0, 0, 0, 0, 1_700_000_000_000]]
    client.place_order.return_value = entry
    client.place_protection.return_value = SimpleNamespace(
        status="success", sl_order_id="SL-1", tp_order_id="TP-1",
        model_dump=lambda: {"status": "success", "sl_order_id": "SL-1", "tp_order_id": "TP-1"},
    )
    return client


def test_e2e_a_connected_demo_account_reaches_the_broker_adapter(migrated_db):
    snapshot = _runner("demo", db=migrated_db)._execution_safety()
    assert snapshot["kyc"]["state"] == "NOT_REQUIRED"
    assert snapshot["readiness"]["state"] == "NOT_REQUIRED_FOR_DEMO_EXECUTION"

    decision = _pre_trade(snapshot)
    assert decision.allowed is True, decision.message

    client = _connected_demo_adapter()
    executor = _live_executor(migrated_db, client)
    executor._size_qty = MagicMock(return_value=(0.0018, {"price": 50_000.0, "leverage": 1}))
    with patch("app.execution.executor.time.time", return_value=1_700_000_000.0):
        result = executor.execute_signal(
            "BTCUSDT", "BUY", 120.0, sl_price=49_000.0, tp_price=53_000.0,
            current_equity=1_000.0,
        )
    assert result.status == "ORDER_PLACED", result.error
    client.place_order.assert_called_once()
    # The capital ledger still governed the entry, with the per-trade allocation.
    verdict = executor._authorize_capital("BTCUSDT", 1.0, 1, 5.0)
    assert verdict.allocation_scope == "PER_TRADE"
    assert verdict.per_trade_allocation == 100.0


def test_e2e_a_real_money_account_without_approval_never_reaches_the_broker(migrated_db):
    snapshot = _runner("live", db=migrated_db)._execution_safety()
    assert snapshot["kyc"]["state"] == "REQUIRED_NOT_APPROVED"
    assert snapshot["readiness"]["state"] == ReadinessGateState.NOT_MET

    decision = _pre_trade(snapshot)
    assert decision.allowed is False
    assert decision.block_reason.name in {"KYC_REQUIRED", "LIVE_READINESS_REQUIRED"}


def test_e2e_open_committed_margin_does_not_exhaust_next_fixed_allocation(migrated_db):
    """Open committed margin does not replace per-trade allocation semantics."""
    with migrated_db.connect() as conn:
        conn.execute(
            "INSERT INTO positions (position_id, bot_instance_id, symbol, side, original_qty, "
            "remaining_qty, status, opened_at, committed_margin, leverage) "
            "VALUES ('p1', ?, 'ETHUSDT', 'LONG', 1, 1, 'OPEN', '2026-01-01', 120.0, 1)",
            (BOT,),
        )
    client = _connected_demo_adapter()
    executor = _live_executor(migrated_db, client)
    executor._size_qty = MagicMock(return_value=(0.0018, {"price": 50_000.0, "leverage": 1}))
    with patch("app.execution.executor.time.time", return_value=1_700_000_000.0):
        result = executor.execute_signal("BTCUSDT", "BUY", 120.0, leverage_override=1)
    assert result.status == "ORDER_PLACED", result.error
    client.place_order.assert_called_once()
