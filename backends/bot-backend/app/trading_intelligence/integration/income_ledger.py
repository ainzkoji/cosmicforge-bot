"""Incremental, durable ledger of a broker account's income history.

The daily-loss latch and the weekly / monthly drawdown baselines need the
account's complete income (realized PnL, commission, funding, transfers) since
local midnight and since the start of the week / month. That used to be
downloaded from the venue in full on every 30-second cycle (one 30-weight
``/fapi/v1/income`` page per seven-day window: 150+ weight per idle account).

Here the venue is asked only for what the local ledger does not cover yet:

* a backfill of ``[start, covered_from)`` when an earlier period is needed,
* a refresh of ``[covered_to - OVERLAP_MS, now]`` -- the overlap re-reads the
  recent window so a record the venue publishes late (the income ledger lags
  the wallet) is still picked up; a refresh happens when the caller forces it
  (first computation of a risk day, an entry about to be evaluated, an open
  position), whenever the account WALLET differs from the one seen at the last
  refresh (a wallet move means income events happened), and otherwise at most
  every ``REFRESH_INTERVAL_MS``.

Every page goes through the same bounded, bisecting ``complete_income`` read
as before, so a full page is never mistaken for complete history, and nothing
is recorded -- the cursor does not move -- when a window could not be read
completely. Records are keyed by the venue's transaction id, so replaying a
window after a restart, or re-reading the overlap, is idempotent.
"""
from __future__ import annotations

import json
import math

TABLE = "cati_account_income"
CURSOR = "cati_account_income_cursor"
#: How far back a refresh re-reads before the covered end (late records).
OVERLAP_MS = 6 * 3600 * 1000
#: Unforced refresh cadence (the latch is forced current whenever it matters).
REFRESH_INTERVAL_MS = 300_000
#: Venue-supported window of one income query.
WINDOW_MS = 7 * 86_400_000

_SCHEMA = (
    f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        account_id TEXT NOT NULL, record_id TEXT NOT NULL, income_type TEXT NOT NULL,
        income REAL NOT NULL, time INTEGER NOT NULL, trade_id TEXT, symbol TEXT, document TEXT NOT NULL,
        PRIMARY KEY(account_id, record_id))""",
    f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_time ON {TABLE}(account_id, time)",
    f"""CREATE TABLE IF NOT EXISTS {CURSOR} (
        account_id TEXT PRIMARY KEY, covered_from INTEGER NOT NULL, covered_to INTEGER NOT NULL,
        refreshed_at INTEGER NOT NULL, pages INTEGER NOT NULL DEFAULT 0, wallet TEXT)""",
)


def initialize(conn) -> None:
    for statement in _SCHEMA:
        conn.execute(statement)


def complete_income(client, start, now):
    """Bounded complete history in venue-supported seven-day windows.

    Returns ``(records, pages)``-free list for compatibility: the page count is
    reported through ``ensure_covered``."""
    return _complete_income(client, start, now)[0]


def _complete_income(client, start, now):
    windows, history = [], []
    while start <= now:
        end = min(now, start + WINDOW_MS - 1)
        windows.append((start, end))
        start = end + 1
    pages = 0
    while windows:
        pages += 1
        if pages > 512:
            raise ValueError("BROKER_INCOME_HISTORY_INCOMPLETE")
        start, end = windows.pop()
        page = client.income_history(start_time_ms=start, end_time_ms=end, limit=1000)
        if not isinstance(page, list):
            raise ValueError("BROKER_INCOME_HISTORY_UNAVAILABLE")
        if any(not isinstance(p, dict) or not start <= int(p["time"]) <= end or
               not math.isfinite(float(p["income"])) or p.get("asset", "USDT") != "USDT" for p in page):
            raise ValueError("BROKER_INCOME_HISTORY_INVALID")
        if len(page) < 1000:
            history.extend(page)
        else:
            if start == end:
                raise ValueError("BROKER_INCOME_HISTORY_AMBIGUOUS")
            middle = (start + end) // 2
            windows.extend(((start, middle), (middle + 1, end)))
    return history, pages


def record_id(record: dict) -> str:
    """The venue's transaction id, or a deterministic key for a venue without one."""
    tran = record.get("tranId")
    if tran not in (None, ""):
        return str(tran)
    return "|".join(str(record.get(k, "")) for k in ("incomeType", "time", "tradeId", "symbol", "income"))


def _schema(db):
    """The ledger tables exist (created once per database by production_schema)."""
    from app.execution.production_schema import ensure
    ensure(db)


def _store(db, account_id, records) -> int:
    inserted = 0
    _schema(db)
    with db.connect() as c:
        for r in records:
            inserted += c.execute(f"INSERT OR IGNORE INTO {TABLE} VALUES(?,?,?,?,?,?,?,?)",
                                  (account_id, record_id(r), str(r.get("incomeType", "")), float(r["income"]), int(r["time"]),
                                   str(r["tradeId"]) if r.get("tradeId") not in (None, "") else None,
                                   r.get("symbol"), json.dumps(r, sort_keys=True))).rowcount
    return inserted


def cursor(db, account_id):
    with db.connect() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name=?", (CURSOR,)).fetchone():
            return None
        row = c.execute(f"SELECT * FROM {CURSOR} WHERE account_id=?", (account_id,)).fetchone()
    return dict(row) if row else None


def ensure_covered(db, client, account_id, start, now, *, force=False, wallet=None) -> dict:
    """Make the local ledger complete for ``[start, now]``, reading from the
    venue only what is not covered (plus the overlap on a refresh). Returns the
    cursor row plus ``pages`` (venue pages read by THIS call) and ``refreshed``.
    ``wallet`` is the account's current wallet balance: a change since the last
    refresh means income events happened and forces the refresh. A window the
    venue could not deliver completely raises and leaves the cursor untouched
    (the caller fails closed, as before)."""
    start, now = int(start), int(now)
    row = cursor(db, account_id)
    pages = 0
    refreshed = False
    wallet_text = None if wallet is None else str(wallet)
    if row is None:
        records, pages = _complete_income(client, start, now)
        _store(db, account_id, records)
        row = {"account_id": account_id, "covered_from": start, "covered_to": now, "refreshed_at": now, "pages": pages,
               "wallet": wallet_text}
        refreshed = True
    else:
        if start < row["covered_from"]:
            records, n = _complete_income(client, start, row["covered_from"] - 1)
            _store(db, account_id, records)
            row["covered_from"], pages = start, pages + n
        wallet_moved = wallet_text is not None and row.get("wallet") != wallet_text
        if force or wallet_moved or now - int(row["refreshed_at"]) >= REFRESH_INTERVAL_MS or now < row["covered_to"]:
            since = max(row["covered_from"], int(row["covered_to"]) - OVERLAP_MS + 1)
            records, n = _complete_income(client, since, now)
            _store(db, account_id, records)
            row.update(covered_to=now, refreshed_at=now,
                       wallet=wallet_text if wallet_text is not None else row.get("wallet"))
            pages, refreshed = pages + n, True
    _schema(db)
    with db.connect() as c:
        c.execute(f"INSERT INTO {CURSOR} VALUES(?,?,?,?,?,?) ON CONFLICT(account_id) DO UPDATE SET covered_from=excluded.covered_from,"
                  " covered_to=excluded.covered_to, refreshed_at=excluded.refreshed_at, pages=pages+excluded.pages,"
                  " wallet=excluded.wallet",
                  (account_id, row["covered_from"], row["covered_to"], row["refreshed_at"], pages, row.get("wallet")))
    return {**row, "pages": pages, "refreshed": refreshed}


def rows(db, account_id, start, end) -> list:
    """The venue records with ``start <= time <= end``, as the venue returned them."""
    with db.connect() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name=?", (TABLE,)).fetchone():
            return []
        return [json.loads(r[0]) for r in c.execute(
            f"SELECT document FROM {TABLE} WHERE account_id=? AND time BETWEEN ? AND ? ORDER BY time, record_id",
            (account_id, int(start), int(end)))]


def forget(db, account_id) -> None:
    """Drop an account's ledger and cursor (operator repair / tests)."""
    with db.connect() as c:
        for table in (TABLE, CURSOR):
            if c.execute("SELECT 1 FROM sqlite_master WHERE name=?", (table,)).fetchone():
                c.execute(f"DELETE FROM {table} WHERE account_id=?", (account_id,))


__all__ = ["TABLE", "CURSOR", "OVERLAP_MS", "REFRESH_INTERVAL_MS", "initialize", "complete_income", "record_id",
           "cursor", "ensure_covered", "rows", "forget"]
