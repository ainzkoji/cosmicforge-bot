"""The customer's trading dashboard: one compact, truthful view per connected exchange account.

``production_runtime.status`` is the operator / engine read model (it carries the configuration matrix, every
position row the venue returns and the raw account document). The portal used to render parts of it directly,
which showed "LIVE" (the engine's CATI mode) next to a DEMO account, and a raw configuration dump. This view
answers only the customer's questions, each from the engine's own records:

* which exchange, DEMO or LIVE (the ACCOUNT's environment -- never the bot's ``mode``, which only says the bot
  trades at a broker rather than on paper), and whether real money is involved at all;
* is the connection healthy, is the engine running for this account, when was the strategy last evaluated, what
  was the latest signal, may a new entry be placed now and, if not, the exact reason and what to do;
* open positions with their protective-stop state, and the recent trades;
* whether the activity is EXPERIMENTAL DEMO activity (``governance.experimental_demo``).

Reason codes are explained by ``reason_registry.describe``; nothing here invents a reassuring default.
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from .reason_registry import describe

#: The broker snapshot is refreshed about every 30 s; older than this, the engine is not running for the account.
ENGINE_FRESH_SECONDS = 120
RECENT_TRADES = 10


def _table(conn: Any, name: str) -> bool:
    return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE name=?", (name,)).fetchone())


def _explained(code: Optional[str]) -> Dict[str, Any]:
    entry = describe(code)
    return {"code": entry["code"], "severity": entry["severity"], "message": entry["message"],
            "action": entry["action"]}


def _connection(state: Dict[str, Any]) -> Dict[str, Any]:
    status = str(state.get("status") or "UNKNOWN")
    if status == "SYNCED":
        health, code = "CONNECTED", None
    elif status == "AWAITING_FIRST_BROKER_SYNC":
        health, code = "CONNECTING", "AWAITING_FIRST_BROKER_SYNC"
    elif status == "STALE":
        health, code = "STALE", "BROKER_SNAPSHOT_STALE"
    elif status == "ENVIRONMENT_CHANGED":
        health, code = "ERROR", "BROKER_ENVIRONMENT_MISMATCH"
    else:
        health, code = "ERROR", state.get("reason") or (state.get("execution") or {}).get("reason") or status
    out = {"health": health, "status": status, "last_read_age_seconds": state.get("age_seconds")}
    out["problem"] = _explained(code) if code else None
    return out


def _strategy_authority(db: Any, state: Dict[str, Any], experimental: Optional[Dict[str, Any]],
                        signal_symbol: Optional[str]) -> Dict[str, Any]:
    """Would a qualifying signal actually be allowed to reach the exchange from this account? The execution
    boundary decides only when a signal arrives; this asks the same authorities now, so the customer sees the
    blockers BEFORE a signal is refused: the strategy's own authority (research certification and stage) or an
    active experimental DEMO grant, the venue (adapter) certification, and the market-entry policy."""
    from .residual_prospective import FAMILY
    environment = str(state.get("environment") or "").upper()
    blockers: List[Dict[str, Any]] = []
    now = int(time.time() * 1000)
    try:
        from app.trading_intelligence.pipeline.family_registry import CERTIFIED, FamilyRegistry
        entry = FamilyRegistry(db).entry(FAMILY)
        own = entry is not None and entry.certification_status == CERTIFIED and environment in entry.allowed_modes
        granted = environment == "DEMO" and bool(experimental) and experimental.get("state") == "ACTIVE"
        if entry is None or not entry.registered:
            blockers.append(_explained("FAMILY_NOT_REGISTERED"))
        elif entry.kill_switch_state == "ON":
            blockers.append(_explained("FAMILY_KILL_SWITCH"))
        elif not own and not granted:
            code = {"FAILED": "FAMILY_FAILED_RESEARCH", "REVOKED": "FAMILY_CERTIFICATION_REVOKED"}.get(
                str(entry.certification_status), "FAMILY_NOT_CERTIFIED")
            blockers.append({**_explained(code), "detail": f"research {entry.certification_status}, stage "
                             f"{entry.governance_stage}" + ("; an operator can grant a time-limited experimental "
                                                            "DEMO authorization" if environment == "DEMO" else "")})
        authority = "CERTIFIED" if own else "EXPERIMENTAL_DEMO" if granted else "NONE"
    except Exception as exc:  # noqa: BLE001 -- unreadable is a blocker, never a pass
        blockers.append(_explained("FAMILY_GATE_UNAVAILABLE") | {"detail": type(exc).__name__})
        authority = "UNKNOWN"
    try:
        from app.exchange.platform_adapter.certification import adapter_certified
        ok, why = adapter_certified(db, str(state.get("broker") or "").lower(), environment, now)
        if not ok:
            blockers.append({**_explained("VENUE_ADAPTER_NOT_CERTIFIED"), "detail": why})
    except Exception as exc:  # noqa: BLE001
        blockers.append({**_explained("VENUE_ADAPTER_NOT_CERTIFIED"), "detail": type(exc).__name__})
    try:
        from app.trading_intelligence.scale.market_entry_policy import MarketEntryPolicyLedger
        from app.trading_intelligence.scale.spread_gate import platform_database
        venue = {"binance": "BINANCE_USDM", "bybit": "BYBIT_LINEAR", "bingx": "BINGX_SWAP"}.get(
            str(state.get("broker") or "").lower(), str(state.get("broker") or "").upper())
        MarketEntryPolicyLedger(platform_database()).resolve(
            venue=venue, symbol=signal_symbol or "*", environment=environment, account_id=state["account_id"])
    except Exception as exc:  # noqa: BLE001 -- MarketPolicyError / SpreadRefused carry their code
        code = getattr(exc, "code", None) or type(exc).__name__
        blockers.append({**_explained(code), "detail": getattr(exc, "detail", "") or ""})
    return {"authority": authority, "blockers": blockers, "orders_possible": not blockers}


def _entry_permission(state: Dict[str, Any], experimental: Optional[Dict[str, Any]],
                      authority: Dict[str, Any]) -> Dict[str, Any]:
    execution = state.get("execution") or {}
    permission = str(state.get("execution_permission") or execution.get("execution_permission") or "BLOCKED_ACCOUNT")
    code = execution.get("reason") or state.get("reason")
    allowed = permission in ("WAITING_SIGNAL", "ORDER_ACTIVE") and authority["orders_possible"]
    if permission == "ORDER_ACTIVE":
        summary = "An order from this bot is active."
    elif permission == "WAITING_SIGNAL" and allowed:
        summary = "Ready: an order is placed when a qualifying signal arrives."
    elif permission == "WAITING_SIGNAL":
        summary = "Waiting for a signal, but a signal would be blocked: see the blockers."
    else:
        summary = "New entries are blocked."
    out = {"state": permission if allowed or permission != "WAITING_SIGNAL" else "BLOCKED_STRATEGY_AUTHORITY",
           "entries_allowed": allowed, "summary": summary,
           "reason": _explained(code) if code else None, "blockers": authority["blockers"],
           "strategy_authority": authority["authority"]}
    before_gate = execution.get("block_reason_before_order_gate")
    if before_gate:
        out["reason_before_order_switch"] = _explained(before_gate)
    if experimental and experimental.get("state") == "ACTIVE":
        out["experimental_demo"] = True
    return out


def _latest_evaluation(conn: Any, account_id: str) -> Optional[Dict[str, Any]]:
    if not _table(conn, "cati_production_decisions"):
        return None
    row = conn.execute("SELECT observed_at, document FROM cati_production_decisions WHERE account_id=? "
                       "ORDER BY observed_at DESC LIMIT 1", (account_id,)).fetchone()
    if not row:
        return None
    doc = json.loads(row[1])
    eligibility = doc.get("eligibility") or {}
    return {"evaluated_at_ms": int(row[0]), "stage": doc.get("stage"), "result": doc.get("final_result"),
            "symbol": doc.get("symbol"), "side": doc.get("side"), "score": doc.get("score"),
            "signal_eligible": eligibility.get("eligible"),
            "reason": _explained(doc.get("reason")) if doc.get("reason") else None,
            "orders_submitted": bool(doc.get("orders_submitted")),
            "experimental_demo": doc.get("experimental_demo"),
            # A size refusal (never resized): the order's margin, leverage, notional and planned risk at the
            # stop against the per-trade ceiling, and that the user's configured amount was not changed.
            "refusal_explanation": doc.get("refusal_explanation")}


def _latest_signal(db: Any) -> Dict[str, Any]:
    """The strategy's latest decision. ``NO_SIGNAL`` when it selected nothing; never a manufactured trade."""
    from .production_execution import latest_decision, eligibility
    row = latest_decision(db)
    if row is None:
        return {"state": "NO_SIGNAL", "reason": _explained("AWAITING_NATURAL_CATI_DECISION")}
    risk_state = json.loads(row.get("risk_state_json") or "{}")
    reason = risk_state.get("reason")
    now = int(time.time() * 1000)
    why = eligibility(row, now)
    if reason != "SELECTED_TOP1":
        return {"state": "NO_SIGNAL", "decision_time_ms": row.get("decision_time"),
                "reason": _explained(reason or "NO_ELIGIBLE_TOP1")}
    return {"state": "SIGNAL", "decision_time_ms": row.get("decision_time"), "symbol": row.get("selected_symbol"),
            "side": row.get("side"), "score": row.get("score"), "entry_reference": row.get("entry_reference"),
            "stop": row.get("stop"), "target": row.get("target"),
            "tradable_now": why is None, "reason": _explained(why) if why else None}


def _protective_stops(conn: Any, account_id: str) -> Dict[str, Dict[str, Any]]:
    """symbol -> latest recorded STOP_MARKET protection request of the account (what the engine placed)."""
    out: Dict[str, Dict[str, Any]] = {}
    if not _table(conn, "cati_production_protection"):
        return out
    for (document,) in conn.execute("SELECT document FROM cati_production_protection WHERE account_id=?",
                                    (account_id,)):
        try:
            doc = json.loads(document)
        except (TypeError, ValueError):
            continue
        if str(doc.get("type")) != "STOP_MARKET":
            continue
        symbol, at = doc.get("symbol"), int(doc.get("_requested_at") or 0)
        if symbol and at >= int(out.get(symbol, {}).get("requested_at_ms") or -1):
            out[symbol] = {"trigger_price": doc.get("triggerPrice"), "requested_at_ms": at}
    return out


def _positions(state: Dict[str, Any], stops: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    uncertain = {str(p.get("symbol")) for p in (state.get("protection_uncertain") or []) if isinstance(p, dict)}
    rows = []
    for p in state.get("positions") or []:
        try:
            amount = float(p.get("positionAmt") or 0)
        except (TypeError, ValueError):
            continue
        if amount == 0:
            continue
        symbol = str(p.get("symbol"))
        if symbol in uncertain:
            stop = {"state": "UNVERIFIED", "detail": "The stop could not be confirmed at the exchange; re-checked "
                                                     "every cycle."}
        elif symbol in stops:
            stop = {"state": "PLACED", **stops[symbol]}
        else:
            stop = {"state": "NOT_RECORDED", "detail": "No engine stop is recorded for this position (opened "
                                                       "outside the bot?)."}
        rows.append({"symbol": symbol, "side": "LONG" if amount > 0 else "SHORT", "quantity": abs(amount),
                     "entry_price": p.get("entryPrice"), "mark_price": p.get("markPrice"),
                     "unrealized_pnl": p.get("unRealizedProfit"), "leverage": p.get("leverage"),
                     "liquidation_price": p.get("liquidationPrice"), "protective_stop": stop})
    return rows


def _recent_trades(conn: Any, account_id: str, experimental: Dict[str, str]) -> List[Dict[str, Any]]:
    if not _table(conn, "cati_production_fills"):
        return []
    out = []
    for (document,) in conn.execute("SELECT document FROM cati_production_fills WHERE account_id=?", (account_id,)):
        try:
            doc = json.loads(document)
        except (TypeError, ValueError):
            continue
        meta = doc.get("_execution") or {}
        plan_id = meta.get("trade_plan_id")
        purpose = meta.get("purpose")
        label = ("EXPERIMENTAL_DEMO" if plan_id in experimental else
                 "VENUE_CERTIFICATION_TEST" if purpose == "DEMO_CERTIFICATION" else "STRATEGY")
        out.append({"time_ms": doc.get("time"), "symbol": doc.get("symbol"), "side": doc.get("side"),
                    "price": doc.get("price"), "quantity": doc.get("qty"), "realized_pnl": doc.get("realizedPnl"),
                    "commission": doc.get("commission"), "leg": meta.get("leg"), "label": label})
    out.sort(key=lambda t: int(t.get("time_ms") or 0), reverse=True)
    return out[:RECENT_TRADES]


def customer_dashboard(db: Any, *, user_id: str) -> Dict[str, Any]:
    from .production_runtime import status
    from app.trading_intelligence.governance.experimental_demo import account_status, experimental_plans
    engine = status(db, user_id=user_id)
    signal = _latest_signal(db)
    accounts = []
    with db.connect() as conn:
        for state in engine["accounts"]:
            account_id = state["account_id"]
            experimental = account_status(db, account_id)
            authority = _strategy_authority(db, state, experimental,
                                            signal.get("symbol") if signal.get("state") == "SIGNAL" else None)
            bots = [dict(b) for b in conn.execute(
                "SELECT id, status, mode FROM bot_instances WHERE broker_account_id=? AND user_id=? "
                "AND status IN ('active','paused')", (account_id, user_id))] if _table(conn, "bot_instances") else []
            age = state.get("age_seconds")
            auto = (state.get("execution") or {}).get("auto_trading") or state.get("auto_trading") or {}
            accounts.append({
                "account_id": account_id, "exchange": state.get("broker"), "environment": state.get("environment"),
                "real_money": state.get("environment") == "LIVE",
                "connection": _connection(state),
                "bot": {"bots": bots, "running": bool(bots) and age is not None and age <= ENGINE_FRESH_SECONDS,
                        "auto_trading_enabled": bool(auto.get("enabled")),
                        "engine_last_cycle_age_seconds": age},
                "last_evaluation": _latest_evaluation(conn, account_id),
                "entry_permission": _entry_permission(state, experimental, authority),
                "experimental_demo": experimental,
                "open_positions": _positions(state, _protective_stops(conn, account_id)),
                "recent_trades": _recent_trades(conn, account_id, experimental_plans(db, account_id)),
                "order_switch": {"demo_orders_enabled": engine.get("demo_order_submission_enabled"),
                                 "live_orders_enabled": engine.get("live_order_submission_enabled")},
            })
    return {"strategy": engine.get("strategy"), "latest_signal": signal, "accounts": accounts,
            "status": engine.get("status"),
            "real_money_trading_possible": any(a["real_money"] and a["order_switch"]["live_orders_enabled"]
                                               for a in accounts),
            "generated_at_ms": int(time.time() * 1000)}


__all__ = ["customer_dashboard", "ENGINE_FRESH_SECONDS"]
