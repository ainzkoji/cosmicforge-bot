"""Account equity history (Step 1.6): ``account_equity_snapshots``.

The production engine knows the account's equity on every cycle (the venue
account document it already reads) but kept only the latest value. This
recorder keeps a time series the customer can see:

* one HOURLY sample per account (the first cycle of each hour),
* one sample per FILL the engine recorded (keyed by the fill id, so a replay
  of the same fill is a no-op),
* a DEPLOY sample when a bot is created (the equity the deployment started from).

Every row carries its data source and freshness (how old the venue document
was when sampled) so a reader can tell fresh from stale. Retention: hourly rows
older than ``RETENTION_DAYS`` are rolled up into ``account_equity_daily``
(open / high / low / close / samples per day) before they are deleted, so
long-range summaries stay accurate. Recording and pruning never raise: a
recorder failure is logged and the trading cycle continues.
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

TABLE = "account_equity_snapshots"
DAILY_TABLE = "account_equity_daily"
RETENTION_DAYS = 90
HOUR_MS = 3_600_000
DAY_MS = 86_400_000

DDL = (
    f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        snapshot_id TEXT PRIMARY KEY, user_id TEXT, broker_account_id TEXT NOT NULL, bot_instance_id TEXT,
        observed_at INTEGER NOT NULL, asset TEXT NOT NULL DEFAULT 'USDT',
        equity REAL NOT NULL, wallet REAL, available REAL, unrealized REAL,
        source TEXT NOT NULL, freshness_ms INTEGER, reason TEXT NOT NULL, dedupe_key TEXT NOT NULL UNIQUE)""",
    f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_account_time ON {TABLE}(broker_account_id, observed_at)",
    f"""CREATE TABLE IF NOT EXISTS {DAILY_TABLE} (
        broker_account_id TEXT NOT NULL, day TEXT NOT NULL, asset TEXT NOT NULL DEFAULT 'USDT',
        open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
        samples INTEGER NOT NULL, first_observed_at INTEGER NOT NULL, last_observed_at INTEGER NOT NULL,
        PRIMARY KEY(broker_account_id, day, asset))""",
)


def initialize(conn) -> None:
    for statement in DDL:
        conn.execute(statement)


def _f(value: Any) -> Optional[float]:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def equity_from_document(document: Dict[str, Any]) -> Optional[Dict[str, Optional[float]]]:
    """The equity figures of a persisted account document (venue account first, balance view second)."""
    raw = document.get("account")
    if isinstance(raw, dict) and raw.get("totalMarginBalance") is not None:
        return {"equity": _f(raw.get("totalMarginBalance")), "wallet": _f(raw.get("totalWalletBalance")),
                "available": _f(raw.get("availableBalance")), "unrealized": _f(raw.get("totalUnrealizedProfit"))}
    balance = document.get("balance")
    if isinstance(balance, dict) and balance.get("equity") is not None:
        return {"equity": _f(balance.get("equity")), "wallet": _f(balance.get("wallet")),
                "available": _f(balance.get("available")), "unrealized": None}
    return None


def _insert(db, *, user_id, account_id, bot_instance_id, observed_at, figures, source, freshness_ms, reason, dedupe_key) -> bool:
    if figures is None or figures.get("equity") is None:
        return False
    snapshot_id = "eq_" + hashlib.sha256(dedupe_key.encode()).hexdigest()[:24]
    row = (snapshot_id, user_id, account_id, bot_instance_id, int(observed_at), "USDT",
           figures["equity"], figures.get("wallet"), figures.get("available"), figures.get("unrealized"),
           source, freshness_ms, reason, dedupe_key)
    import sqlite3
    with db.connect() as c:
        try:
            inserted = c.execute(f"INSERT OR IGNORE INTO {TABLE} VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row).rowcount
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc):
                raise
            initialize(c)                                      # first use of this database only
            inserted = c.execute(f"INSERT OR IGNORE INTO {TABLE} VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row).rowcount
    return inserted > 0


def record_cycle(db, account: Dict[str, Any], document: Dict[str, Any], now_ms: int, *, bot_instance_id: Optional[str] = None,
                 fills: Iterable[Dict[str, Any]] = ()) -> Dict[str, Any]:
    """Called once per account cycle with the document the runtime persisted.
    Never raises."""
    written = {"hourly": False, "fills": 0}
    try:
        figures = equity_from_document(document)
        if figures is None:
            return written
        observed = int(document.get("observed_at") or now_ms)
        freshness = max(0, int(now_ms) - observed)
        hour = observed // HOUR_MS
        written["hourly"] = _insert(db, user_id=account.get("user_id"), account_id=account["id"], bot_instance_id=bot_instance_id,
                                    observed_at=observed, figures=figures, source="ENGINE_CYCLE", freshness_ms=freshness,
                                    reason="HOURLY", dedupe_key=f"hourly:{account['id']}:{hour}")
        recorded = list(fills)
        # A fill recorded outside this cycle's own history (an operator close, the
        # certification harness, a close finished just before a restart) is in the
        # fills table but not in ``fills``. The wallet moving is the cheap signal
        # that one may exist: only then is the table consulted, so an idle cycle
        # costs nothing extra. Seen on the connected demo account: the entry got
        # its snapshot, the exit did not.
        wallet = figures.get("wallet")
        if _last_wallet.get(account["id"], object()) != wallet:
            recorded += _recent_fills(db, account["id"])
            _last_wallet[account["id"]] = wallet
        seen = set()
        for fill in recorded:
            fill_id = fill.get("id")
            if fill_id is None or (fill.get("symbol"), str(fill_id)) in seen:
                continue
            seen.add((fill.get("symbol"), str(fill_id)))
            if _insert(db, user_id=account.get("user_id"), account_id=account["id"], bot_instance_id=bot_instance_id,
                       observed_at=observed, figures=figures, source="ENGINE_CYCLE", freshness_ms=freshness, reason="FILL",
                       dedupe_key=f"fill:{account['id']}:{fill.get('symbol')}:{fill_id}"):
                written["fills"] += 1
    except Exception:  # the recorder must never interrupt the cycle
        logger.exception("[ACCOUNT_RECORDER] equity snapshot could not be recorded for account %s", account.get("id"))
    return written


#: wallet balance at the last recorded cycle, per account (process memory only)
_last_wallet: Dict[str, Any] = {}
RECENT_FILLS = 20
#: a fill may be recorded a little before the cycle whose snapshot precedes it
FILL_LOOKBACK_MS = 60_000


def _recent_fills(db, account_id: str) -> List[Dict[str, Any]]:
    """Recorded fills of the account that happened since its last snapshot and
    have none of their own. An older fill is never given today's equity."""
    try:
        with db.connect() as c:
            last = c.execute(f"SELECT MAX(observed_at) FROM {TABLE} WHERE broker_account_id=?", (account_id,)).fetchone()[0]
            rows = c.execute(
                "SELECT f.trade_id, f.symbol, f.document FROM cati_production_fills f WHERE f.account_id=? AND NOT EXISTS ("
                f"SELECT 1 FROM {TABLE} s WHERE s.dedupe_key = 'fill:' || f.account_id || ':' || f.symbol || ':' || f.trade_id) "
                "ORDER BY f.rowid DESC LIMIT ?", (account_id, RECENT_FILLS)).fetchall()
    except sqlite3.OperationalError:
        return []                                             # no fills table yet: nothing to record
    since = (int(last) if last is not None else int(time.time() * 1000) - HOUR_MS) - FILL_LOOKBACK_MS
    out = []
    for trade_id, symbol, document in rows:
        try:
            filled_at = int(json.loads(document).get("time") or 0)
        except Exception:
            filled_at = 0
        if filled_at >= since:
            out.append({"id": trade_id, "symbol": symbol})
    return out


def record_deployment(db, *, user_id: str, account_id: str, bot_instance_id: str, figures: Dict[str, Any], now_ms: int) -> bool:
    """The equity a deployment started from. Never raises."""
    try:
        return _insert(db, user_id=user_id, account_id=account_id, bot_instance_id=bot_instance_id, observed_at=now_ms,
                       figures={k: _f(figures.get(k)) for k in ("equity", "wallet", "available", "unrealized")},
                       source=str(figures.get("source") or "DEPLOYMENT"), freshness_ms=0, reason="DEPLOY",
                       dedupe_key=f"deploy:{bot_instance_id}")
    except Exception:
        logger.exception("[ACCOUNT_RECORDER] deployment snapshot could not be recorded for bot %s", bot_instance_id)
        return False


def fills_in_document(document: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Every fill the cycle's execution history carries (entry and exit legs)."""
    out = []
    for item in (document.get("execution") or {}).get("execution_history") or []:
        for fill in (item.get("fills") or []) + (item.get("exit_fills") or []):
            if isinstance(fill, dict):
                out.append(fill)
    return out


def series(db, account_id: str, *, since_ms: Optional[int] = None, until_ms: Optional[int] = None, limit: int = 2000) -> List[Dict[str, Any]]:
    """Snapshots of one account, oldest first (local read only)."""
    with db.connect() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name=?", (TABLE,)).fetchone():
            return []
        sql, args = f"SELECT * FROM {TABLE} WHERE broker_account_id=?", [account_id]
        if since_ms is not None:
            sql += " AND observed_at>=?"
            args.append(int(since_ms))
        if until_ms is not None:
            sql += " AND observed_at<=?"
            args.append(int(until_ms))
        sql += " ORDER BY observed_at LIMIT ?"
        args.append(int(limit))
        return [dict(r) for r in c.execute(sql, args)]


def daily(db, account_id: str, *, limit: int = 400) -> List[Dict[str, Any]]:
    with db.connect() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name=?", (DAILY_TABLE,)).fetchone():
            return []
        return [dict(r) for r in c.execute(f"SELECT * FROM {DAILY_TABLE} WHERE broker_account_id=? ORDER BY day LIMIT ?",
                                           (account_id, int(limit)))]


def prune(db, now_ms: int, *, retention_days: int = RETENTION_DAYS) -> Dict[str, int]:
    """Roll hourly rows older than the retention into daily rows, then delete them. Never raises."""
    cutoff = int(now_ms) - int(retention_days) * DAY_MS
    out = {"rolled_days": 0, "deleted": 0}
    try:
        with db.connect() as c:
            initialize(c)
            rows = c.execute(f"SELECT broker_account_id, asset, observed_at, equity FROM {TABLE} WHERE observed_at < ? "
                             "ORDER BY broker_account_id, asset, observed_at", (cutoff,)).fetchall()
            groups: Dict[tuple, List] = {}
            for account_id, asset, observed_at, equity in rows:
                day = time.strftime("%Y-%m-%d", time.gmtime(observed_at / 1000))
                groups.setdefault((account_id, asset, day), []).append((observed_at, equity))
            for (account_id, asset, day), samples in groups.items():
                samples.sort()
                equities = [e for _, e in samples]
                existing = c.execute(f"SELECT open, high, low, close, samples, first_observed_at, last_observed_at FROM {DAILY_TABLE} "
                                     "WHERE broker_account_id=? AND day=? AND asset=?", (account_id, day, asset)).fetchone()
                if existing:
                    open_, high, low, close, n, first, last = existing
                    merged = {"open": open_ if first <= samples[0][0] else equities[0], "high": max(high, *equities),
                              "low": min(low, *equities), "close": close if last >= samples[-1][0] else equities[-1],
                              "samples": n + len(samples), "first": min(first, samples[0][0]), "last": max(last, samples[-1][0])}
                else:
                    merged = {"open": equities[0], "high": max(equities), "low": min(equities), "close": equities[-1],
                              "samples": len(samples), "first": samples[0][0], "last": samples[-1][0]}
                c.execute(f"INSERT OR REPLACE INTO {DAILY_TABLE} VALUES(?,?,?,?,?,?,?,?,?,?)",
                          (account_id, day, asset, merged["open"], merged["high"], merged["low"], merged["close"],
                           merged["samples"], merged["first"], merged["last"]))
                out["rolled_days"] += 1
            out["deleted"] = c.execute(f"DELETE FROM {TABLE} WHERE observed_at < ?", (cutoff,)).rowcount
    except Exception:
        logger.exception("[ACCOUNT_RECORDER] pruning failed")
    return out


def peak_and_drawdown(db, account_id: str, current_equity: Optional[float]) -> Dict[str, Optional[float]]:
    """Peak equity over the retained history (hourly rows and daily rollups) and the current drawdown."""
    peaks = [r["equity"] for r in series(db, account_id)] + [r["high"] for r in daily(db, account_id)]
    if current_equity is not None:
        peaks.append(float(current_equity))
    if not peaks:
        return {"peak_equity": None, "current_drawdown": None, "current_drawdown_pct": None}
    peak = max(peaks)
    if current_equity is None or peak <= 0:
        return {"peak_equity": peak, "current_drawdown": None, "current_drawdown_pct": None}
    drawdown = max(0.0, peak - float(current_equity))
    return {"peak_equity": peak, "current_drawdown": drawdown, "current_drawdown_pct": drawdown / peak * 100}


__all__ = ["TABLE", "DAILY_TABLE", "RETENTION_DAYS", "initialize", "equity_from_document", "record_cycle",
           "record_deployment", "fills_in_document", "series", "daily", "prune", "peak_and_drawdown"]
