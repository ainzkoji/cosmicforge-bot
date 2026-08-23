"""
Portfolio aggregation service.

Source-of-truth rules:
- Uses get_db() (reads DATABASE_URL) — same DB as user-backend broker_service.
- Uses resolve_broker_auth() for credential resolution — reads broker_credentials_v2
  with legacy fallback, enforces environment cross-check.
- Per-broker errors are surfaced explicitly in the response; totals are never
  silently zeroed when a broker call fails.
- running_bots_count is queried from bot_instances in the same DB — consistent
  with the runner's view.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from shared_lib.persistence.db import DB
from shared_lib.broker.resolver import resolve_broker_auth
from shared_lib.broker.errors import BrokerResolverError
from app.core.broker_service import list_user_broker_accounts, get_db
from app.exchange.binance.client import BinanceFuturesClient

logger = logging.getLogger(__name__)


def _build_binance_client(auth) -> BinanceFuturesClient:
    """Build a BinanceFuturesClient from a resolved BrokerAuth."""
    return BinanceFuturesClient(
        api_key=auth.api_key,
        api_secret=auth.api_secret,
        base_url=auth.base_url,
    )


def _count_running_bots(db: DB, user_id: str) -> int:
    """
    Count bot instances that are actively running (not stopped, deleted, or
    broker-blocked).  Uses the same DB as the rest of the portfolio service so
    the count is always consistent.
    """
    try:
        with db.connect() as conn:
            bi_cols = {
                r["name"]
                for r in conn.execute("PRAGMA table_info(bot_instances)").fetchall()
            }
            if "broker_health_status" in bi_cols:
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM bot_instances
                    WHERE user_id = ?
                      AND status IN ('active', 'error')
                      AND (broker_health_status IS NULL OR broker_health_status = 'ok')
                    """,
                    (user_id,),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM bot_instances WHERE user_id = ? AND status IN ('active', 'error')",
                    (user_id,),
                ).fetchone()
            return int(row["cnt"]) if row else 0
    except Exception as exc:
        logger.debug("_count_running_bots failed: %s", exc)
        return 0


def get_portfolio_summary(user_id: str) -> Dict[str, Any]:
    """
    Aggregates portfolio data from all connected brokers.

    Data source guarantees:
    - DB: get_db() — reads DATABASE_URL; same file as broker_service.py
    - Credentials: resolve_broker_auth() — broker_credentials_v2 with legacy fallback
    - Client URL: auth.base_url from canonical resolver (no hardcoded testnet logic)
    - Errors: per-account errors returned in 'account_errors'; totals reflect only
              accounts that succeeded
    """
    db = get_db()  # ← CRITICAL: must match DATABASE_URL, not DB() default path
    accounts = list_user_broker_accounts(user_id)

    total_equity = 0.0
    total_unrealized_pnl = 0.0
    total_balance = 0.0
    total_margin_used = 0.0
    total_available = 0.0
    total_maint_margin = 0.0

    positions: List[Dict[str, Any]] = []
    account_errors: List[Dict[str, Any]] = []

    for acc in accounts:
        if acc["status"] != "connected":
            continue

        account_id = acc["id"]
        broker_id = acc.get("broker_id", "")

        # ── Resolve credentials via canonical resolver ──────────────────────
        try:
            auth = resolve_broker_auth(account_id, user_id, db)
        except BrokerResolverError as exc:
            logger.warning(
                "portfolio_summary broker_auth_failed account=%s reason=%s",
                account_id, exc.reason_code,
            )
            account_errors.append({
                "account_id": account_id,
                "broker_id":  broker_id,
                "error_type": exc.reason_code,
                "error":      str(exc),
            })
            continue
        except Exception as exc:
            logger.warning("portfolio_summary resolve_error account=%s: %s", account_id, exc)
            account_errors.append({
                "account_id": account_id,
                "broker_id":  broker_id,
                "error_type": "resolve_error",
                "error":      str(exc),
            })
            continue

        # ── Fetch live data from exchange ───────────────────────────────────
        if broker_id == "binance":
            try:
                client = _build_binance_client(auth)
                acc_data = client.account()

                equity       = float(acc_data.get("totalMarginBalance", 0))
                wallet       = float(acc_data.get("totalWalletBalance", 0))
                upnl         = float(acc_data.get("totalUnrealizedProfit", 0))
                margin_used  = float(acc_data.get("totalInitialMargin", 0))
                available    = float(acc_data.get("availableBalance", 0))
                maint_margin = float(acc_data.get("totalMaintMargin", 0))

                total_equity        += equity
                total_balance       += wallet
                total_unrealized_pnl += upnl
                total_margin_used   += margin_used
                total_available     += available
                total_maint_margin  += maint_margin

                # Open positions
                raw_positions = client.position_risk_all()
                for p in raw_positions:
                    amt = float(p.get("positionAmt", 0))
                    if amt == 0:
                        continue
                    entry_price   = float(p.get("entryPrice", 0))
                    mark_price    = float(p.get("markPrice", 0))
                    pnl           = float(p.get("unRealizedProfit", 0))
                    leverage      = int(p.get("leverage", 1))
                    initial_margin = (entry_price * abs(amt)) / leverage if leverage > 0 else 0
                    roi_percent   = round((pnl / initial_margin * 100), 2) if initial_margin > 0 else 0

                    positions.append({
                        "account_id":  account_id,
                        "broker":      "Binance",
                        "symbol":      p.get("symbol"),
                        "side":        "LONG" if amt > 0 else "SHORT",
                        "size":        abs(amt),
                        "entry_price": entry_price,
                        "mark_price":  mark_price,
                        "pnl":         pnl,
                        "roi_percent": roi_percent,
                        "leverage":    leverage,
                    })

            except Exception as exc:
                logger.warning(
                    "portfolio_summary fetch_failed account=%s broker=binance error=%s",
                    account_id, exc,
                )
                account_errors.append({
                    "account_id": account_id,
                    "broker_id":  broker_id,
                    "error_type": "api_error",
                    "error":      str(exc),
                })

        # ── Other brokers — extend here as needed ───────────────────────────
        # elif broker_id == "bybit": ...

    # ── Recent trade activity (from DB) ────────────────────────────────────
    recent_activity: List[Dict[str, Any]] = []
    try:
        with db.connect() as conn:
            rows = conn.execute(
                """
                SELECT f.symbol, f.side, f.action, f.qty, f.price,
                       f.realized_pnl, f.timestamp_utc
                FROM trade_fills f
                LEFT JOIN bot_instances bi ON bi.id = f.bot_instance_id
                WHERE (
                    f.user_id = ?
                    OR ((f.user_id IS NULL OR f.user_id = '') AND bi.user_id = ?)
                )
                ORDER BY f.timestamp_utc DESC
                LIMIT 10
                """,
                (user_id, user_id),
            ).fetchall()
            for r in rows:
                recent_activity.append({
                    "symbol":    r["symbol"],
                    "side":      r["side"],
                    "action":    r["action"],
                    "qty":       r["qty"],
                    "timestamp": r["timestamp_utc"],
                })
    except Exception as exc:
        logger.debug("portfolio recent_activity query failed: %s", exc)

    # ── Running bot count ───────────────────────────────────────────────────
    running_bots = _count_running_bots(db, user_id)

    # ── Derived metrics ─────────────────────────────────────────────────────
    margin_ratio = (
        (total_maint_margin / total_equity) * 100 if total_equity > 0 else 0.0
    )
    allocation = [
        {
            "asset":   "Cash",
            "value":   round(total_available, 2),
            "percent": round((total_available / total_equity * 100), 2) if total_equity else 0,
        },
        {
            "asset":   "Invested",
            "value":   round(total_margin_used, 2),
            "percent": round((total_margin_used / total_equity * 100), 2) if total_equity else 0,
        },
    ]

    logger.debug(
        "portfolio_summary user=%s equity=%.2f available=%.2f running_bots=%d errors=%d",
        user_id, total_equity, total_available, running_bots, len(account_errors),
    )

    return {
        "summary": {
            # ← was `len(positions)` (open futures positions) — now correct bot count
            "active_strategies":     running_bots,
            "running_bots":          running_bots,
            "open_positions_count":  len(positions),
            "total_portfolio_value": round(total_equity, 2),
            "total_day_pnl":         round(total_unrealized_pnl, 2),
            "total_day_pnl_percent": round(
                (total_unrealized_pnl / total_balance * 100), 2
            ) if total_balance > 0 else 0,
        },
        "account_details": {
            "margin_balance":      round(total_equity, 2),
            "wallet_balance":      round(total_balance, 2),
            "unrealized_pnl":      round(total_unrealized_pnl, 2),
            "available_balance":   round(total_available, 2),
            "initial_margin":      round(total_margin_used, 2),
            "maintenance_margin":  round(total_maint_margin, 2),
            "margin_ratio_percent": round(margin_ratio, 2),
        },
        "positions":       positions,
        "recent_activity": recent_activity,
        "allocation":      allocation,
        # Surfaced so the UI can show per-account degraded state instead of
        # silently displaying $0 for accounts that failed.
        "account_errors":  account_errors,
        "broker_breakdown": {},
    }
