"""Build the Phase 12 validation identity and a real PaperRunner around it.

Everything here is the production construction path: the same
``resolve_effective_bot_policy``, the same ``BotRunContext``, the same
``PaperRunner``, the same ``TradingOrchestrator``. Only two things are
substituted, and both are substituted *outside* the trading logic:

* the exchange client, by a deterministic read-only one that raises on any
  order call, and
* the ensemble's market interpretation, by the controlled opportunity.
"""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone

VALIDATION_BOT = "bot_phase12_live"
VALIDATION_USER = "user_phase12_live"
BROKER_ACCOUNT = "brk_phase12_live"
SYMBOL = "BTCUSDT"
TIMEFRAME = "1m"
HIGHER_TIMEFRAME = "15m"

#: Deliberately not 'active': the canonical scheduler selects WHERE
#: status='active', so this identity can never be picked up and driven with a
#: real broker client by a running production runtime.
VALIDATION_STATUS = "validation"

CAPITAL_BUDGET = 10_000.0
POSITION_ALLOCATION = 1_000.0

TF_MS = 60 * 1000


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Identity ────────────────────────────────────────────────────────────────


def ensure_validation_identity(db) -> None:
    """Create the user, broker account and bot row this harness drives."""
    from scripts.phase12.bootstrap import FORBIDDEN_BOTS

    assert VALIDATION_BOT not in FORBIDDEN_BOTS
    now = utc_now()
    with db.connect() as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(users)")}
        if cols:
            payload = {"id": VALIDATION_USER, "email": "phase12@validation.local",
                       "full_name": "Phase 12 validation", "is_active": 1,
                       "status": "active", "created_at": now, "updated_at": now}
            usable = {k: v for k, v in payload.items() if k in cols}
            conn.execute(
                f"INSERT OR IGNORE INTO users ({','.join(usable)}) "
                f"VALUES ({','.join('?' * len(usable))})",
                tuple(usable.values()),
            )

        cols = {r[1] for r in conn.execute("PRAGMA table_info(broker_accounts)")}
        payload = {
            "id": BROKER_ACCOUNT, "user_id": VALIDATION_USER,
            "broker_id": "binance", "market_type": "crypto",
            "label": "phase12-controlled", "status": "connected",
            "environment": "demo", "active_credential_version": 1,
            "created_at": now, "updated_at": now,
        }
        usable = {k: v for k, v in payload.items() if k in cols}
        conn.execute(
            f"INSERT OR REPLACE INTO broker_accounts ({','.join(usable)}) "
            f"VALUES ({','.join('?' * len(usable))})",
            tuple(usable.values()),
        )

        cols = {r[1] for r in conn.execute("PRAGMA table_info(bot_instances)")}
        payload = {
            "id": VALIDATION_BOT,
            "user_id": VALIDATION_USER,
            "broker_account_id": BROKER_ACCOUNT,
            "market_type": "CRYPTO",
            "strategy_id": "master_ensemble",
            "strategy_version": "1.0.0",
            "config_id": "__auto_pilot__",
            "risk_profile_id": "__auto_pilot__",
            "symbols_json": json.dumps([SYMBOL]),
            "timeframes_json": json.dumps([TIMEFRAME]),
            "allocation_type": "fixed_amount",
            "allocation_value": POSITION_ALLOCATION,
            "capital_allocation": CAPITAL_BUDGET,
            "capital_allocation_type": "fixed_amount",
            "mode": "paper",
            "status": VALIDATION_STATUS,
            "risk_level": "balanced",
            "created_at": now,
            "updated_at": now,
            "started_at": now,
        }
        usable = {k: v for k, v in payload.items() if k in cols}
        conn.execute(
            f"INSERT OR REPLACE INTO bot_instances ({','.join(usable)}) "
            f"VALUES ({','.join('?' * len(usable))})",
            tuple(usable.values()),
        )


# ── Runtime construction ────────────────────────────────────────────────────


def open_session(db) -> str:
    from app.evidence.writers import open_runtime_session

    return open_runtime_session(
        db,
        database_role="research",
        database_path=db.path,
        process_execution_mode="paper",
        environment_name="phase12-live-validation",
        code_revision=_code_revision(),
    )


def _code_revision() -> str | None:
    import subprocess

    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, timeout=10,
        )
        return (out.stdout or "").strip() or None
    except Exception:
        return None


def build_runner(db, client, *, run_id: str | None = None, controlled: bool = True,
                 confidence: float = 0.92):
    """Construct the real PaperRunner for the validation bot."""
    from app.core.bot_instance_service import BotInstanceService
    from app.runner.bot_context import BotRunContext
    from app.runner.effective_policy import resolve_effective_bot_policy
    from app.runner.runner import PaperRunner

    service = BotInstanceService(db)
    instance = service.get_bot_instance(VALIDATION_BOT)
    if instance is None:
        raise SystemExit(f"validation bot {VALIDATION_BOT} not found in {db.path}")

    risk_params = BotInstanceService.get_risk_profile_preset(instance.risk_level)
    policy = resolve_effective_bot_policy(
        instance=instance,
        broker_environment="demo",
        risk_params=risk_params,
        monitor_interval_seconds=10,
    )
    creds = {
        "api_key": "phase12-controlled",
        "api_secret": "phase12-controlled",
        "broker_type": "binance",
        "environment": "demo",
        "base_url": "controlled://phase12",
    }
    context = BotRunContext.from_effective_policy(policy, creds)
    if run_id:
        context.run_id = run_id
    context.higher_timeframe = HIGHER_TIMEFRAME

    from app.exchange.registry import get_instrument_registry

    get_instrument_registry().refresh(broker_id="binance", client=client, force=True)

    runner = PaperRunner(client, context=context, effective_policy=policy)
    runner.runtime_session_id = os.environ.get("PHASE12_RUNTIME_SESSION_ID")

    if controlled:
        from scripts.phase12.controlled import install_controlled_opportunity

        # The orchestrator wraps the ensemble in a LegacyStrategyAdapter, which
        # delegates to the same object -- so installing on runner.strategy
        # covers both call sites, and the adapter needs nothing done to it.
        install_controlled_opportunity(runner.strategy, confidence=confidence)

    return runner, policy, context


def open_run(db, runner, policy, *, runtime_session_id: str):
    from app.evidence.writers import open_bot_run
    from shared_lib.persistence.evidence_schema import PAPER_FORWARD_VALIDATION

    open_bot_run(
        db,
        run_id=runner.run_id,
        bot_instance_id=VALIDATION_BOT,
        runtime_session_id=runtime_session_id,
        user_id=VALIDATION_USER,
        policy_hash=policy.policy_hash,
        provenance=PAPER_FORWARD_VALIDATION,
        execution_mode="paper",
        broker_environment="demo",
    )


def new_run_id() -> str:
    return uuid.uuid4().hex
