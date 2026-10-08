"""Deployment preview and deploy (Step 1.2 / 1.3) -- one validation and money
calculation service behind both endpoints.

Everything about the account is derived from the server's own records: the
broker account row (ownership, status, broker, ENVIRONMENT), the engine's
latest persisted account snapshot or, failing that, one authoritative read of
the exchange, the versioned risk profile library, the shared risk-sizing
formula and the venue's instrument filters. The client contributes a budget, a
risk level, optional advanced settings, the acknowledgement flag and an
idempotency key -- nothing else is trusted.

Deploy revalidates everything (the account may have changed since the
preview), creates the bot under the database write lock so two concurrent
requests cannot both succeed (the second sees ACCOUNT_ALREADY_HAS_BOT), records
the consent durably with its identity and version, and persists the request
identity so a repeated request with the same identity resolves to the same
deployment.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from decimal import Decimal
from typing import Any, Dict, List, Optional

from shared_lib import risk_levels
from shared_lib.broker.environment import normalize_environment
from shared_lib.deployment import contract as C
from shared_lib.persistence.db import utc_now_iso

from app.trading_intelligence.execution import risk_sizing

logger = logging.getLogger(__name__)

SUPPORTED_BROKERS = frozenset({"binance", "bybit", "bingx"})
#: A persisted engine snapshot older than this is not used for the preview.
SNAPSHOT_FRESH_MS = 120_000
#: Fallback exchange minimum notional (Binance USDⓈ-M) when the catalog has no instrument.
DEFAULT_MIN_NOTIONAL = Decimal("5")
#: Fallback widest structural stop (the system maximum) when no decision history exists.
DEFAULT_WIDEST_STOP_PCT = Decimal("15")
#: Bot states that occupy an account.
OCCUPYING_STATUSES = ("active", "paused")
CONSENT_TABLE = "deployment_consents"

STATUS_MAP = {"active": "running", "paused": "paused", "stopped": "stopped", "archived": "stopped",
              "deleted": "stopped", "error": "stopped"}


class DeploymentRefused(Exception):
    """A deploy that cannot proceed: ``status`` is the HTTP status, ``body`` the response."""

    def __init__(self, status: int, body: Dict[str, Any]):
        super().__init__(body.get("blockers", [{}])[0].get("code", "DEPLOYMENT_REFUSED") if body.get("blockers") else "DEPLOYMENT_REFUSED")
        self.status = status
        self.body = body


def ensure_schema(conn) -> None:
    conn.execute(f"""CREATE TABLE IF NOT EXISTS {CONSENT_TABLE} (
        consent_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, bot_instance_id TEXT NOT NULL,
        broker_account_id TEXT NOT NULL, risk_level TEXT NOT NULL, risk_profile_version TEXT NOT NULL,
        consent_version TEXT NOT NULL, acknowledged_at TEXT NOT NULL, request_id TEXT NOT NULL,
        preview_json TEXT NOT NULL)""")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{CONSENT_TABLE}_bot ON {CONSENT_TABLE}(bot_instance_id)")


# ── account facts ────────────────────────────────────────────────────────────

def load_account(db, user_id: str, broker_account_id: str) -> Optional[Dict[str, Any]]:
    """The user's broker account row, or None when it is not theirs / does not exist."""
    with db.connect() as c:
        row = c.execute("SELECT id, user_id, broker_id, environment, status, market_type, label FROM broker_accounts "
                        "WHERE id=? AND user_id=?", (broker_account_id, user_id)).fetchone()
    return dict(row) if row else None


def persisted_account_state(db, account_id: str, now_ms: int) -> Optional[Dict[str, Any]]:
    """The engine's latest persisted account document when fresh enough."""
    with db.connect() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_production_state'").fetchone():
            return None
        row = c.execute("SELECT observed_at, document FROM cati_production_state WHERE account_id=?", (account_id,)).fetchone()
    if row is None or now_ms - int(row[0]) > SNAPSHOT_FRESH_MS:
        return None
    document = json.loads(row[1])
    raw = document.get("account")
    if not isinstance(raw, dict) or "totalMarginBalance" not in raw:
        balance = document.get("balance") or {}
        if not balance or balance.get("equity") is None:
            return None
        raw = {"totalMarginBalance": balance.get("equity"), "totalWalletBalance": balance.get("wallet", balance.get("equity")),
               "availableBalance": balance.get("available", balance.get("equity"))}
    return {"equity": Decimal(str(raw["totalMarginBalance"])), "available": Decimal(str(raw.get("availableBalance", raw["totalMarginBalance"]))),
            "wallet": Decimal(str(raw.get("totalWalletBalance", raw["totalMarginBalance"]))),
            "source": "ENGINE_SNAPSHOT", "observed_at": int(row[0])}


def exchange_account_state(db, account: Dict[str, Any], now_ms: int) -> Dict[str, Any]:
    """One authoritative read of the exchange through the canonical resolver and client factory."""
    from shared_lib.broker.client_factory import build_client_from_auth
    from shared_lib.broker.resolver import resolve_broker_auth
    auth = resolve_broker_auth(account["id"], account["user_id"], db)
    client = build_client_from_auth(auth)
    raw = client.account()
    if not isinstance(raw, dict) or "totalMarginBalance" not in raw:
        raise ValueError(C.ACCOUNT_BALANCE_UNAVAILABLE)
    return {"equity": Decimal(str(raw["totalMarginBalance"])), "available": Decimal(str(raw.get("availableBalance", raw["totalMarginBalance"]))),
            "wallet": Decimal(str(raw.get("totalWalletBalance", raw["totalMarginBalance"]))),
            "source": "EXCHANGE_READ", "observed_at": now_ms}


def default_account_state_reader(db, account: Dict[str, Any], now_ms: int) -> Dict[str, Any]:
    state = persisted_account_state(db, account["id"], now_ms)
    return state if state is not None else exchange_account_state(db, account, now_ms)


def occupying_bot(db, account_id: str) -> Optional[Dict[str, Any]]:
    with db.connect() as c:
        row = c.execute(f"SELECT id, status, deploy_request_id, user_id FROM bot_instances WHERE broker_account_id=? "
                        f"AND status IN ({','.join('?' for _ in OCCUPYING_STATUSES)}) ORDER BY created_at DESC LIMIT 1",
                        (account_id, *OCCUPYING_STATUSES)).fetchone()
    return dict(row) if row else None


def exchange_minimum_notional(db, account: Dict[str, Any]) -> Decimal:
    """The venue's minimum order notional (largest over the catalog's listed instruments), or the default."""
    try:
        from app.exchange.catalog_refresh import VENUE_KEY
        from app.exchange.instruments import InstrumentCatalog
        venue = VENUE_KEY.get(str(account["broker_id"]).lower())
        if venue is None:
            return DEFAULT_MIN_NOTIONAL
        environment = normalize_environment(account["environment"] or "live").value.upper()
        values = [Decimal(str(i.min_notional)) for i in InstrumentCatalog(db).list(venue, environment)
                  if getattr(i, "min_notional", None)]
        return max(values) if values else DEFAULT_MIN_NOTIONAL
    except Exception:
        return DEFAULT_MIN_NOTIONAL


def stop_distance_assumptions(db) -> Dict[str, Any]:
    """Tightest / typical / widest structural stop of the frozen strategy's own
    decisions, in percent, when enough decisions exist. Otherwise the system
    maximum stop is the widest and no typical range is claimed."""
    with db.connect() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_residual_decisions'").fetchone():
            rows = []
        else:
            rows = c.execute("SELECT entry_reference, stop FROM cati_residual_decisions WHERE selected_symbol IS NOT NULL "
                             "AND entry_reference > 0 AND stop > 0 ORDER BY decision_time DESC LIMIT 500").fetchall()
    distances = sorted(abs(Decimal(str(r[0])) - Decimal(str(r[1]))) / Decimal(str(r[0])) * 100 for r in rows)
    if len(distances) < 20:
        return {"source": "SYSTEM_MAXIMUM_STOP", "decisions": len(distances), "tightest_pct": None, "typical_pct": None,
                "widest_pct": DEFAULT_WIDEST_STOP_PCT}
    q = lambda p: distances[min(len(distances) - 1, int(p * (len(distances) - 1)))]
    return {"source": "FROZEN_STRATEGY_DECISIONS", "decisions": len(distances),
            "tightest_pct": q(0.10).quantize(Decimal("0.01")), "typical_pct": q(0.50).quantize(Decimal("0.01")),
            "widest_pct": q(0.90).quantize(Decimal("0.01"))}


# ── the shared evaluation ───────────────────────────────────────────────────

def evaluate(db, user_id: str, request: C.DeploymentRequest, *, now_ms: Optional[int] = None,
             account_state_reader=None, for_deploy: bool = False) -> Dict[str, Any]:
    """Everything the preview shows and the deploy enforces, computed once."""
    account_state_reader = account_state_reader or default_account_state_reader
    now = int(time.time() * 1000) if now_ms is None else int(now_ms)
    blockers: List[Dict[str, Any]] = []
    requirements: List[str] = []
    out: Dict[str, Any] = {"schema_version": C.DEPLOYMENT_SCHEMA_VERSION, "request_id": request.request_id,
                           "broker_account_id": request.broker_account_id, "evaluated_at": now}
    try:
        request.budget.validated()
    except ValueError as exc:
        blockers.append(C.blocker(C.INVALID_ADVANCED_SETTINGS, field="budget", detail=str(exc)))

    account = load_account(db, user_id, request.broker_account_id)
    if account is None or str(account.get("status") or "").lower() not in {"connected", "active"}:
        blockers.append(C.blocker(C.ACCOUNT_NOT_CONNECTED))
        out.update(blockers=blockers, requirements=requirements, can_deploy=False)
        return out
    broker = str(account["broker_id"]).lower()
    environment = normalize_environment(account["environment"] or "live").value.upper()
    out.update(exchange=broker.upper(), environment=environment, account_label=account.get("label"))
    if broker not in SUPPORTED_BROKERS:
        blockers.append(C.blocker(C.BROKER_NOT_SUPPORTED, exchange=broker.upper()))
    if environment != "DEMO":
        # Step 1: the customer path deploys demo accounts only; live orders stay impossible.
        blockers.append(C.blocker(C.LIVE_NOT_AVAILABLE, environment=environment))

    # authoritative account state (never the client's numbers)
    state = None
    try:
        state = account_state_reader(db, account, now)
    except Exception as exc:
        logger.warning("deployment preview: account state unavailable for %s: %s", account["id"], type(exc).__name__)
        blockers.append(C.blocker(C.ACCOUNT_BALANCE_UNAVAILABLE, reason=type(exc).__name__))
    if state is not None:
        out["account"] = {"equity": str(state["equity"]), "available_balance": str(state["available"]),
                          "wallet": str(state["wallet"]), "source": state["source"], "observed_at": state["observed_at"]}

    # the money view from the single risk-profile library
    profile = risk_levels.get_profile(request.risk_level)
    budget = None
    if state is not None:
        try:
            budget = risk_sizing.effective_budget(budget_type=request.budget.type, budget_value=request.budget.value,
                                                  account_equity=state["equity"])
        except ValueError as exc:
            blockers.append(C.blocker(C.INVALID_ADVANCED_SETTINGS, field="budget", detail=str(exc)))
    elif request.budget.type == "fixed_amount":
        budget = Decimal(request.budget.value)
    if budget is not None:
        money = risk_levels.money_view(request.risk_level, budget)
        assumptions = stop_distance_assumptions(db)
        min_notional = exchange_minimum_notional(db, account)
        minimum_budget = risk_levels.minimum_deployable_budget(request.risk_level, exchange_min_notional=min_notional,
                                                               widest_stop_distance_pct=assumptions["widest_pct"])
        out.update(effective_budget=str(budget), budget={"type": request.budget.type, "value": request.budget.value},
                   risk_profile=profile.as_dict(), money=risk_levels.as_json(money),
                   minimum_deployable_budget=str(minimum_budget),
                   exchange_minimum_notional=str(min_notional), stop_distance_assumptions=risk_levels.as_json(assumptions),
                   ceiling_conflict={"system_per_trade_risk_ceiling_pct": str(risk_levels.SYSTEM_PER_TRADE_RISK_CEILING_PCT),
                                     "ceiling_applied": money["ceiling_applied"],
                                     "note": "The engine applies the stricter 0.4 % per-trade ceiling until the project owner "
                                             "approves widening it; the approved profile value is shown for comparison."}
                   if money["ceiling_applied"] else None)
        if state is not None and budget > state["equity"]:
            blockers.append(C.blocker(C.BUDGET_EXCEEDS_BALANCE, budget=str(budget), account_equity=str(state["equity"])))
        if budget < minimum_budget:
            blockers.append(C.blocker(C.BUDGET_TOO_SMALL_FOR_LEVEL, minimum_deployable_budget=str(minimum_budget)))
        # the typical position range, through the same sizing the engine runs
        if assumptions["typical_pct"] is not None:
            stops = {"tight": assumptions["tightest_pct"] / 100, "typical": assumptions["typical_pct"] / 100,
                     "wide": assumptions["widest_pct"] / 100}
            sized = risk_sizing.preview_range(budget_usdt=budget, risk_fraction=money["effective_per_trade_risk_pct"] / 100,
                                              price=Decimal("1"), leverage_ceiling=profile.leverage_ceiling,
                                              stop_distances=stops, min_notional=min_notional,
                                              max_position_usdt=request.advanced.max_position_usdt,
                                              max_open_risk_fraction=profile.fraction("max_open_risk_pct"))
            out["typical_position"] = {k: {"notional_usdt": v["notional_usdt"], "margin_usdt": v["margin_usdt"],
                                           "leverage": v["leverage"], "approved": v["approved"], "reason": v["reason"]}
                                       for k, v in sized.items()}
            if not sized["typical"]["approved"] and sized["typical"]["reason"] == risk_sizing.RISK_SIZE_BELOW_EXCHANGE_MINIMUM:
                blockers.append(C.blocker(C.RISK_SIZE_BELOW_EXCHANGE_MINIMUM))
        else:
            out["typical_position"] = None
            out["typical_position_note"] = "STOP_DISTANCE_ASSUMPTION_REQUIRED"
        if request.advanced.max_position_usdt is not None and Decimal(request.advanced.max_position_usdt) < min_notional:
            blockers.append(C.blocker(C.INVALID_ADVANCED_SETTINGS, field="max_position_usdt",
                                      detail=f"below the exchange minimum notional {min_notional}"))
        if request.advanced.daily_loss_limit_pct is not None and \
                Decimal(request.advanced.daily_loss_limit_pct) > profile.daily_loss_pause_pct:
            blockers.append(C.blocker(C.INVALID_ADVANCED_SETTINGS, field="daily_loss_limit_pct",
                                      detail=f"may only tighten the profile's {profile.daily_loss_pause_pct} % daily pause"))

    # one active deployment per account (an idempotent replay of the same request is not a conflict)
    existing = occupying_bot(db, account["id"])
    if existing is not None and not (existing.get("deploy_request_id") == request.request_id and existing["user_id"] == user_id):
        blockers.append(C.blocker(C.ACCOUNT_ALREADY_HAS_BOT, bot_id=existing["id"], bot_status=STATUS_MAP.get(existing["status"], existing["status"])))
    if not request.risk_acknowledged:
        requirements.append(C.RISK_NOT_ACKNOWLEDGED)
        if for_deploy:
            blockers.append(C.blocker(C.RISK_NOT_ACKNOWLEDGED))

    out.update(blockers=blockers, requirements=requirements, can_deploy=not blockers,
               consent={"version": C.CONSENT_VERSION, "text": C.CONSENT_TEXT},
               disclaimer="Estimated stop-loss risk is before fees and slippage; actual losses can exceed it because of "
                          "slippage, exchange outages or price gaps. Nothing here is a projection of earnings.")
    return out


def preview(db, user_id: str, request: C.DeploymentRequest, **kw) -> Dict[str, Any]:
    return evaluate(db, user_id, request, for_deploy=False, **kw)


# ── deploy ──────────────────────────────────────────────────────────────────

def _existing_for_request(db, user_id: str, request_id: str) -> Optional[Dict[str, Any]]:
    with db.connect() as c:
        row = c.execute("SELECT * FROM bot_instances WHERE deploy_request_id=? AND user_id=? ORDER BY created_at LIMIT 1",
                        (request_id, user_id)).fetchone()
        if row is None:
            return None
        bot = dict(row)
        consent = c.execute(f"SELECT preview_json FROM {CONSENT_TABLE} WHERE bot_instance_id=?", (bot["id"],)).fetchone()
    bot["_consent_preview"] = json.loads(consent[0]) if consent else None
    return bot


def deploy(db, user_id: str, request: C.DeploymentRequest, *, service=None, now_ms: Optional[int] = None,
           account_state_reader=None) -> Dict[str, Any]:
    """Create the deployment or raise ``DeploymentRefused`` with the blockers."""
    account_state_reader = account_state_reader or default_account_state_reader
    from app.core.bot_instance_service import BotInstanceService
    from app.core.broker_capability_gate import BrokerCapabilityGateError
    from app.models.bot_instance_models import CreateBotInstanceRequest
    service = service or BotInstanceService(db=db)
    now = int(time.time() * 1000) if now_ms is None else int(now_ms)

    # idempotency: the same identity resolves to the same deployment
    replay = _existing_for_request(db, user_id, request.request_id)
    if replay is not None:
        stored = (replay.get("_consent_preview") or {}).get("request_fingerprint")
        if stored and stored != request.fingerprint():
            raise DeploymentRefused(409, {"blockers": [C.blocker(C.REQUEST_ID_REUSED)], "bot_id": replay["id"]})
        return {"bot": bot_payload(db, service.get_bot_instance(replay["id"])), "idempotent_replay": True,
                "preview": replay.get("_consent_preview")}

    evaluation = evaluate(db, user_id, request, now_ms=now, account_state_reader=account_state_reader, for_deploy=True)
    if not evaluation["can_deploy"]:
        codes = {b["code"] for b in evaluation["blockers"]}
        status = 409 if C.ACCOUNT_ALREADY_HAS_BOT in codes else 422
        raise DeploymentRefused(status, evaluation)

    account = load_account(db, user_id, request.broker_account_id)
    profile = risk_levels.get_profile(request.risk_level)
    budget_value = float(Decimal(request.budget.value))
    daily = request.advanced.daily_loss_limit_pct
    create = CreateBotInstanceRequest(
        user_id=user_id, broker_account_id=account["id"], market_type="CRYPTO", strategy_id="cati", strategy_version="1.0.0",
        risk_level=profile.level, symbols=list(request.advanced.symbols), timeframes=["15m"],
        universe_mode="ALLOWLIST" if request.advanced.symbols else "BROKER",
        allocation_type="risk_based", allocation_value=float(profile.per_trade_risk_pct), mode="live",
        capital_allocation=budget_value, capital_allocation_type=request.budget.type,
        daily_loss_limit_pct=float(Decimal(daily) / 100) if daily is not None else None,
        risk_profile_version=profile.version,
        max_position_usdt=float(Decimal(request.advanced.max_position_usdt)) if request.advanced.max_position_usdt else None,
        risk_acknowledged_at=utc_now_iso(), deploy_request_id=request.request_id,
        environment=evaluation["environment"])
    acknowledged_at = create.risk_acknowledged_at

    # Atomic invariant: one occupying bot per account. The write lock is taken
    # BEFORE the re-check so a concurrent deploy waits and then sees the row.
    scope = getattr(db, "scope", None)
    from contextlib import nullcontext
    with (scope() if callable(scope) else nullcontext()):
        with db.connect() as c:
            c.execute("BEGIN IMMEDIATE")
            ensure_schema(c)
            if c.execute(f"SELECT 1 FROM bot_instances WHERE broker_account_id=? AND status IN ({','.join('?' for _ in OCCUPYING_STATUSES)})",
                         (account["id"], *OCCUPYING_STATUSES)).fetchone():
                raise DeploymentRefused(409, {**evaluation, "can_deploy": False,
                                              "blockers": [C.blocker(C.ACCOUNT_ALREADY_HAS_BOT)]})
            try:
                instance = service.create_bot_instance(create)
            except BrokerCapabilityGateError as exc:
                raise DeploymentRefused(422, {**evaluation, "can_deploy": False,
                                              "blockers": [C.blocker(C.BROKER_CAPABILITY_INCOMPLETE, reason=exc.reason_code)]})
            except sqlite3.IntegrityError:
                raise DeploymentRefused(409, {**evaluation, "can_deploy": False,
                                              "blockers": [C.blocker(C.ACCOUNT_ALREADY_HAS_BOT)]})
            consent_id = f"consent_{uuid.uuid4().hex[:16]}"
            c.execute(f"INSERT INTO {CONSENT_TABLE} VALUES(?,?,?,?,?,?,?,?,?,?)",
                      (consent_id, user_id, instance.id, account["id"], profile.level, profile.version, C.CONSENT_VERSION,
                       acknowledged_at, request.request_id,
                       json.dumps({**evaluation, "request_fingerprint": request.fingerprint()}, default=str)))
    try:
        from app.observability import account_recorder
        account_state = evaluation.get("account") or {}
        account_recorder.record_deployment(db, user_id=user_id, account_id=account["id"], bot_instance_id=instance.id,
                                           figures={"equity": account_state.get("equity"), "wallet": account_state.get("wallet"),
                                                    "available": account_state.get("available_balance"),
                                                    "source": account_state.get("source")}, now_ms=now)
    except Exception:  # never fails a deployment
        logger.exception("deployment equity snapshot not recorded for %s", instance.id)
    logger.info("deployment created bot=%s account=%s user=%s level=%s budget=%s", instance.id, account["id"], user_id,
                profile.level, request.budget.value)
    return {"bot": bot_payload(db, service.get_bot_instance(instance.id)), "idempotent_replay": False,
            "consent": {"consent_id": consent_id, "version": C.CONSENT_VERSION, "acknowledged_at": acknowledged_at},
            "preview": evaluation}


# ── the bot payload every bot response exposes (Step 1.3) ───────────────────

def bot_status(db, instance) -> Dict[str, Any]:
    """running / paused / stopped / deploying, mapped from the engine state machine."""
    raw = str(getattr(instance, "status", "") or "").lower()
    status = STATUS_MAP.get(raw, raw or "stopped")
    reason = getattr(instance, "stopped_reason", None)
    if raw == "active":
        # "deploying" until the engine has evaluated the account with THIS bot:
        # the latest persisted evaluation names the bot, or it was observed
        # after the bot was created.
        seen = False
        with db.connect() as c:
            if c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_production_state'").fetchone():
                row = c.execute("SELECT observed_at, document FROM cati_production_state WHERE account_id=?",
                                (instance.broker_account_id,)).fetchone()
                if row is not None:
                    execution = (json.loads(row[1]) or {}).get("execution") or {}
                    seen = execution.get("bot_instance_id") == instance.id or int(row[0]) >= _iso_to_ms(instance.created_at)
        if not seen:
            status = "deploying"
    if raw in ("archived", "deleted") and not reason:
        reason = raw.upper()
    return {"status": status, "stopped_reason": reason}


def _iso_to_ms(value: Any) -> int:
    from datetime import datetime, timezone
    try:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    except Exception:
        return 0


def bot_payload(db, instance) -> Dict[str, Any]:
    with db.connect() as c:
        account = c.execute("SELECT broker_id, environment, label FROM broker_accounts WHERE id=?",
                            (instance.broker_account_id,)).fetchone()
    broker = (account["broker_id"] if account else "unknown")
    environment = getattr(instance, "environment", None) or (
        normalize_environment(account["environment"] or "live").value.upper() if account else None)
    risk_based = str(getattr(instance, "allocation_type", "") or "").lower() == "risk_based"
    money = None
    budget = None
    if risk_based and instance.capital_allocation:
        budget = {"type": instance.capital_allocation_type, "value": str(instance.capital_allocation)}
        if instance.capital_allocation_type == "fixed_amount":
            money = risk_levels.as_json(risk_levels.money_view(instance.risk_level, str(instance.capital_allocation),
                                                               version=instance.risk_profile_version))
    elif instance.capital_allocation:
        budget = {"type": instance.capital_allocation_type, "value": str(instance.capital_allocation),
                  "legacy_allocation": {"type": instance.allocation_type, "value": str(instance.allocation_value)}}
    state = bot_status(db, instance)
    return {
        "id": instance.id,
        "name": f"CATI {str(instance.risk_level).capitalize()} on {str(broker).capitalize()}" + (f" ({account['label']})" if account and account["label"] else ""),
        "exchange": str(broker).upper(),
        "environment": environment,
        "broker_account_id": instance.broker_account_id,
        "risk_level": instance.risk_level,
        "risk_profile_version": getattr(instance, "risk_profile_version", None),
        "allocation_type": instance.allocation_type,
        "budget": budget,
        "money": money,
        "max_position_usdt": getattr(instance, "max_position_usdt", None),
        "status": state["status"],
        "engine_status": instance.status,
        "stopped_reason": state["stopped_reason"],
        "created_at": instance.created_at,
        "risk_acknowledged_at": getattr(instance, "risk_acknowledged_at", None),
    }


__all__ = ["DeploymentRefused", "SUPPORTED_BROKERS", "OCCUPYING_STATUSES", "CONSENT_TABLE", "ensure_schema", "load_account",
           "persisted_account_state", "exchange_account_state", "default_account_state_reader", "occupying_bot",
           "stop_distance_assumptions", "evaluate", "preview", "deploy", "bot_status", "bot_payload"]
