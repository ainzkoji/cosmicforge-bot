"""The authoritative CATI account read model (Step 1.6): what the engine knows,
read from the engine's own persisted records. No exchange call anywhere here.

Sources (all written by the production path, never by this module):

* ``cati_production_state``            the runtime's last account document per cycle
  (broker positions, the evaluation: reason, permission, kill switch, daily
  loss state, latest decision, execution history incl. protection state)
* ``cati_execution_attempts``          one row per attempt state (the latest is current)
* ``cati_trade_plans``                 the immutable plan (stop, target, side)
* ``cati_production_fills``            exchange fills, entry and exit legs, with realized PnL and commission
* ``cati_account_income``              the income ledger (funding fees per symbol)
* ``cati_production_protection_uncertainty`` positions whose protection is unknown (Step 1.0a)
* ``account_equity_snapshots`` / ``account_equity_daily``  the equity history (account_recorder)

Realized P&L is reported GROSS (the exchange's realizedPnl of the exit fills),
with fees and funding as separate figures and ``net_pnl = realized - fees +
funding``; nothing is double counted.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from app.observability import account_recorder
from app.trading_intelligence.integration.reason_registry import describe

STALE_AFTER_MS = 120_000
POSITION_EXISTS = {"FILLED", "PARTIALLY_FILLED", "RECONCILED_POSITION_EXISTS", "POSITION_OPEN"}
CLOSED = {"POSITION_CLOSED"}


def _now_ms(now_ms: Optional[int]) -> int:
    return int(time.time() * 1000) if now_ms is None else int(now_ms)


def _table(c, name: str) -> bool:
    return bool(c.execute("SELECT 1 FROM sqlite_master WHERE name=?", (name,)).fetchone())


def account_document(db, account_id: str) -> Optional[Dict[str, Any]]:
    with db.connect() as c:
        if not _table(c, "cati_production_state"):
            return None
        row = c.execute("SELECT observed_at, document FROM cati_production_state WHERE account_id=?", (account_id,)).fetchone()
    if row is None:
        return None
    document = json.loads(row[1])
    document["_observed_at"] = int(row[0])
    return document


# ── status ──────────────────────────────────────────────────────────────────

def bot_status(db, instance, *, runtime_progress: Optional[Dict[str, Any]] = None, now_ms: Optional[int] = None) -> Dict[str, Any]:
    from app.core.deployment_service import bot_payload
    from app.execution.protection_state import uncertain_positions
    now = _now_ms(now_ms)
    payload = bot_payload(db, instance)
    document = account_document(db, instance.broker_account_id)
    observed_at = document["_observed_at"] if document else None
    age_ms = (now - observed_at) if observed_at is not None else None
    stale = age_ms is None or age_ms > STALE_AFTER_MS
    execution = (document or {}).get("execution") or {}
    reason = execution.get("reason") or (document or {}).get("reason")
    permission = execution.get("execution_permission") or (document or {}).get("execution_permission")
    if document is None:
        reason, permission = "AWAITING_FIRST_BROKER_SYNC", "BLOCKED_ACCOUNT"
    elif stale and (document.get("status") == "SYNCED"):
        # Never show a stale heartbeat as a healthy running engine.
        reason, permission = "BROKER_SNAPSHOT_STALE", "BLOCKED_ACCOUNT"
    risk = execution.get("risk") or {}
    decision = execution.get("latest_cati_decision") or {}
    eligibility = execution.get("eligibility") or {}
    described = describe(reason)
    return {
        **payload,
        "engine": {
            "running": bool(document) and not stale and document.get("status") == "SYNCED",
            "account_status": (document or {}).get("status"),
            "last_cycle_at": observed_at,
            "last_cycle_age_seconds": round(age_ms / 1000, 1) if age_ms is not None else None,
            "stale": stale,
            "evaluation_scope": execution.get("evaluation_scope"),
            "process": runtime_progress,
        },
        "eligibility": {
            "eligible_to_enter": bool(eligibility.get("eligible")) and permission in ("WAITING_SIGNAL", "ORDER_ACTIVE") and not stale,
            "execution_permission": permission,
            "reason_code": reason,
            "reason": described["message"],
            "suggested_action": described["action"],
            "severity": described["severity"],
            "block_reason_before_order_gate": execution.get("block_reason_before_order_gate"),
        },
        "last_decision": {
            "decision_id": decision.get("decision_id"), "decision_time": decision.get("decision_time"),
            "symbol": decision.get("selected_symbol"), "side": decision.get("side"),
            "eligible": eligibility.get("eligible"), "reason_code": eligibility.get("reason"),
            "reason": describe(eligibility.get("reason"))["message"] if eligibility.get("reason") else None,
        } if decision or eligibility else None,
        "kill_switch": {"engaged": bool(execution.get("kill_switch")) or
                        bool((execution.get("risk_controls") or {}).get("kill_switch"))},
        "daily_loss": {
            "risk_date": risk.get("risk_date"), "latched": bool(risk.get("loss_latched")),
            "loss_usdt": risk.get("daily_loss_usage"), "remaining_usdt": risk.get("remaining_daily_risk"),
            "limit_fraction": risk.get("daily_loss_limit_fraction"), "realized_pnl": risk.get("realized_pnl"),
            "unrealized_pnl": risk.get("unrealized_pnl"), "equity": risk.get("equity"),
            "source": (risk.get("daily_loss_policy") or {}).get("source"),
        } if risk else None,
        "protection_uncertain": uncertain_positions(db, instance.broker_account_id),
        "order_submission_gate": (document or {}).get("order_submission_gate"),
    }


# ── positions ───────────────────────────────────────────────────────────────

def _plan_index(db, account_id: str) -> Dict[str, Dict[str, Any]]:
    with db.connect() as c:
        if not _table(c, "cati_trade_plans"):
            return {}
        rows = c.execute("SELECT trade_plan_id, bot_instance_id, side, payload FROM cati_trade_plans WHERE broker_account_id=?",
                         (account_id,)).fetchall()
    out = {}
    for r in rows:
        d = dict(zip(("trade_plan_id", "bot_instance_id", "side", "payload"), r))
        try:
            payload = json.loads(d.get("payload") or "{}")
        except Exception:
            payload = {}
        target = None
        zones = payload.get("target_zones") or []
        if zones and isinstance(zones[0], dict):
            target = zones[0].get("price_high") if (payload.get("side") or d.get("side")) == "LONG" else zones[0].get("price_low")
        out[d["trade_plan_id"]] = {"bot_instance_id": d.get("bot_instance_id") or payload.get("bot_instance_id"),
                                   "side": d.get("side") or payload.get("side"),
                                   "stop": d.get("structural_invalidation_price") or payload.get("structural_invalidation_price"),
                                   "target": target, "entry_reference": d.get("entry_reference") or payload.get("entry_reference"),
                                   "decision_time": d.get("decision_time") or payload.get("decision_time")}
    return out


def _latest_attempts(db, account_id: str) -> Dict[str, Dict[str, Any]]:
    from app.trading_intelligence.evidence.stores import ExecutionAttemptStore
    try:
        rows = ExecutionAttemptStore(db).for_account(account_id)
    except Exception:
        return {}
    latest: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        latest[row["execution_attempt_id"]] = row
    return latest


def _mark_prices(document: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    marks = {}
    for p in (document or {}).get("positions") or []:
        if isinstance(p, dict) and p.get("symbol"):
            marks[p["symbol"]] = {"mark_price": p.get("markPrice"), "unrealized": p.get("unRealizedProfit"),
                                  "position_amt": p.get("positionAmt"), "entry_price": p.get("entryPrice"),
                                  "liquidation_price": p.get("liquidationPrice")}
    return marks


def positions(db, instance, *, now_ms: Optional[int] = None) -> List[Dict[str, Any]]:
    """Open positions of the bot with their exchange-side protection state."""
    from app.execution.protection_state import uncertain_positions
    document = account_document(db, instance.broker_account_id)
    observed_at = document["_observed_at"] if document else None
    marks = _mark_prices(document)
    plans = _plan_index(db, instance.broker_account_id)
    uncertain = {u["symbol"]: u for u in uncertain_positions(db, instance.broker_account_id)}
    history = {i.get("trade_plan_id"): i for i in ((document or {}).get("execution") or {}).get("execution_history") or []}
    out = []
    for attempt in _latest_attempts(db, instance.broker_account_id).values():
        if attempt.get("bot_instance_id") != instance.id or attempt["status"] not in POSITION_EXISTS:
            continue
        payload = attempt.get("payload") or {}
        plan = plans.get(attempt["trade_plan_id"], {})
        symbol = payload.get("venue_symbol") or (payload.get("instrument_key") or {}).get("venue_symbol") \
            or (history.get(attempt["trade_plan_id"]) or {}).get("position", {}).get("venue_symbol")
        item = history.get(attempt["trade_plan_id"]) or {}
        protection = item.get("protection")
        if isinstance(protection, dict):
            state = protection.get("state") or ("CONFIRMED" if protection.get("status") == "success" else "UNCONFIRMED")
            protection_reason = protection.get("reason")
        elif isinstance(protection, str):
            state, protection_reason = ("NOT_REQUIRED_FLAT" if protection == "NOT_REQUIRED_FLAT" else "UNCONFIRMED"), protection
        else:
            state, protection_reason = "UNKNOWN", "NOT_YET_VERIFIED"
        if symbol in uncertain:
            state, protection_reason = "UNKNOWN", uncertain[symbol]["reason"]
        mark = marks.get(symbol, {})
        out.append({
            "trade_plan_id": attempt["trade_plan_id"], "execution_attempt_id": attempt["execution_attempt_id"],
            "symbol": symbol, "side": plan.get("side") or payload.get("side"),
            "quantity": payload.get("filled_quantity") or item.get("position", {}).get("quantity"),
            "entry_price": payload.get("filled_price") or item.get("position", {}).get("entry_price"),
            "mark_price": mark.get("mark_price"), "mark_observed_at": observed_at,
            "mark_fresh": observed_at is not None and _now_ms(now_ms) - observed_at <= STALE_AFTER_MS,
            "unrealized_pnl": mark.get("unrealized"),
            # never fabricated: the plan's stop / target are the engine's own geometry
            "stop_price": plan.get("stop"), "target_price": plan.get("target"),
            "protection": {"state": state, "reason": protection_reason,
                           "sl_order_id": protection.get("sl_order_id") if isinstance(protection, dict) else None,
                           "tp_order_id": protection.get("tp_order_id") if isinstance(protection, dict) else None,
                           "description": describe(protection_reason)["message"] if state != "CONFIRMED" and protection_reason else None},
            "opened_at": payload.get("acknowledged_at") or payload.get("submitted_at") or attempt.get("recorded_at"),
            "status": attempt["status"],
        })
    return out


# ── trades ──────────────────────────────────────────────────────────────────

def _fills(db, account_id: str) -> List[Dict[str, Any]]:
    with db.connect() as c:
        if not _table(c, "cati_production_fills"):
            return []
        return [json.loads(r[0]) for r in c.execute("SELECT document FROM cati_production_fills WHERE account_id=?", (account_id,))]


def _funding(db, account_id: str, symbol: Optional[str], start: Optional[int], end: Optional[int]) -> float:
    if not symbol or start is None:
        return 0.0
    from app.trading_intelligence.integration import income_ledger
    try:
        rows = income_ledger.rows(db, account_id, start, end if end is not None else start + 400 * 86_400_000)
    except Exception:
        return 0.0
    return float(sum(float(r.get("income", 0) or 0) for r in rows if r.get("incomeType") == "FUNDING_FEE" and r.get("symbol") == symbol))


def trades(db, instance, *, page: int = 1, page_size: int = 50, include_open: bool = True) -> Dict[str, Any]:
    """Paginated trade history: one record per trade plan with recorded fills."""
    account_id = instance.broker_account_id
    plans = _plan_index(db, account_id)
    attempts = _latest_attempts(db, account_id)
    by_plan: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for fill in _fills(db, account_id):
        meta = fill.get("_execution") or {}
        plan_id = meta.get("trade_plan_id")
        if not plan_id:
            continue
        by_plan.setdefault(plan_id, {"ENTRY": [], "EXIT": []})[meta.get("leg", "ENTRY")].append(fill)
    records = []
    for plan_id, legs in by_plan.items():
        plan = plans.get(plan_id, {})
        attempt = next((a for a in attempts.values() if a["trade_plan_id"] == plan_id), None)
        if plan.get("bot_instance_id") not in (None, instance.id) or (attempt and attempt.get("bot_instance_id") not in (None, instance.id)):
            continue
        if plan.get("bot_instance_id") is None and (attempt is None or attempt.get("bot_instance_id") != instance.id):
            continue
        entries, exits = legs["ENTRY"], legs["EXIT"]
        if not entries:
            continue
        qty_in = sum(float(f.get("qty", 0) or 0) for f in entries)
        qty_out = sum(float(f.get("qty", 0) or 0) for f in exits)
        entry_avg = sum(float(f["price"]) * float(f["qty"]) for f in entries) / qty_in if qty_in else None
        exit_avg = sum(float(f["price"]) * float(f["qty"]) for f in exits) / qty_out if qty_out else None
        symbol = entries[0].get("symbol")
        entry_time = min(int(f.get("time", 0) or 0) for f in entries)
        exit_time = max(int(f.get("time", 0) or 0) for f in exits) if exits else None
        realized = sum(float(f.get("realizedPnl", 0) or 0) for f in entries + exits)
        fees = sum(float(f.get("commission", 0) or 0) for f in entries + exits)
        funding = _funding(db, account_id, symbol, entry_time, exit_time)
        status = attempt["status"] if attempt else ("POSITION_CLOSED" if exits and qty_out >= qty_in - 1e-9 else "UNKNOWN")
        closed = status in CLOSED or (exits and qty_out >= qty_in - 1e-9)
        if not closed and not include_open:
            continue
        stop = plan.get("stop")
        risk_per_unit = abs(float(entry_avg) - float(stop)) if (entry_avg is not None and stop) else None
        r_multiple = (realized / (risk_per_unit * qty_in)) if (closed and risk_per_unit and qty_in) else None
        reason_codes = tuple((attempt or {}).get("payload", {}).get("reason_codes") or ())
        exit_reason = None
        if closed:
            exit_reason = "FAIL_SAFE_CLOSE" if any("PROTECTION" in c for c in reason_codes) else \
                "STOP_OR_TARGET" if "BROKER_CONFIRMED_FLAT_AND_EXIT_FILL" in reason_codes else "CLOSED"
        records.append({
            "trade_id": plan_id, "symbol": symbol, "side": plan.get("side") or (attempt or {}).get("payload", {}).get("side"),
            "state": "CLOSED" if closed else "OPEN", "entry_time": entry_time, "exit_time": exit_time,
            "entry_price": entry_avg, "exit_price": exit_avg, "quantity": qty_in, "exit_quantity": qty_out,
            "realized_pnl_gross": realized if closed else None, "fees": fees, "funding": funding,
            "net_pnl": (realized - fees + funding) if closed else None, "exit_reason": exit_reason,
            "r_multiple": r_multiple, "stop_price": stop, "target_price": plan.get("target"),
            "purpose": (entries[0].get("_execution") or {}).get("purpose"), "attempt_status": status,
        })
    records.sort(key=lambda r: (r["exit_time"] or r["entry_time"] or 0, r["trade_id"]), reverse=True)
    page = max(1, int(page))
    size = max(1, min(int(page_size), 200))
    start = (page - 1) * size
    return {"total": len(records), "page": page, "page_size": size, "has_more": start + size < len(records),
            "trades": records[start:start + size],
            "pnl_definition": "realized_pnl_gross is the exchange's realized PnL of the exit fills; fees (commission) and "
                              "funding are separate; net_pnl = realized_pnl_gross - fees + funding."}


# ── summary ─────────────────────────────────────────────────────────────────

def summary(db, instance, *, now_ms: Optional[int] = None, timezone_name: str = "UTC") -> Dict[str, Any]:
    now = _now_ms(now_ms)
    closed = [t for t in trades(db, instance, page=1, page_size=200, include_open=False)["trades"]]
    try:
        from zoneinfo import ZoneInfo
        zone = ZoneInfo(timezone_name)
    except Exception:
        zone = timezone.utc
    today_start = int(datetime.fromtimestamp(now / 1000, zone).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    windows = {"today": today_start, "seven_days": now - 7 * 86_400_000, "thirty_days": now - 30 * 86_400_000, "all_time": 0}
    document = account_document(db, instance.broker_account_id)
    figures = account_recorder.equity_from_document(document or {}) if document else None
    equity = figures["equity"] if figures else None
    out = {"windows": {}, "pnl_definition": "realized_pnl_gross is the exchange's realized PnL; fees and funding are separate; "
                                           "net_pnl = realized_pnl_gross - fees + funding (nothing counted twice).",
           "equity": {"current": equity, "observed_at": document["_observed_at"] if document else None,
                      **account_recorder.peak_and_drawdown(db, instance.broker_account_id, equity)}}
    for name, start in windows.items():
        rows = [t for t in closed if (t["exit_time"] or 0) >= start]
        nets = [t["net_pnl"] for t in rows if t["net_pnl"] is not None]
        out["windows"][name] = {
            "trades": len(rows), "wins": sum(1 for n in nets if n > 0), "losses": sum(1 for n in nets if n < 0),
            "realized_pnl_gross": sum(t["realized_pnl_gross"] or 0 for t in rows), "fees": sum(t["fees"] for t in rows),
            "funding": sum(t["funding"] for t in rows), "net_pnl": sum(nets),
            "largest_win": max(nets) if any(n > 0 for n in nets) else None,
            "largest_loss": min(nets) if any(n < 0 for n in nets) else None,
        }
    return out


# ── equity ──────────────────────────────────────────────────────────────────

def equity(db, instance, *, since_ms: Optional[int] = None, until_ms: Optional[int] = None, limit: int = 2000,
           now_ms: Optional[int] = None) -> Dict[str, Any]:
    now = _now_ms(now_ms)
    rows = account_recorder.series(db, instance.broker_account_id, since_ms=since_ms, until_ms=until_ms, limit=limit)
    document = account_document(db, instance.broker_account_id)
    latest = account_recorder.equity_from_document(document) if document else None
    return {
        "broker_account_id": instance.broker_account_id,
        "points": [{"observed_at": r["observed_at"], "equity": r["equity"], "wallet": r["wallet"], "available": r["available"],
                    "unrealized": r["unrealized"], "source": r["source"], "freshness_ms": r["freshness_ms"], "reason": r["reason"]}
                   for r in rows],
        "daily": account_recorder.daily(db, instance.broker_account_id),
        "latest": {**latest, "observed_at": document["_observed_at"],
                   "stale": now - document["_observed_at"] > STALE_AFTER_MS} if latest else None,
        "history_available": bool(rows),
        "note": None if rows else "No equity history has been recorded for this account yet; nothing is shown as zero.",
    }


__all__ = ["STALE_AFTER_MS", "account_document", "bot_status", "positions", "trades", "summary", "equity"]
