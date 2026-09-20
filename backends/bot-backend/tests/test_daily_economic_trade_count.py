from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from shared_lib.persistence.db import DB
from shared_lib.persistence.state_store import StateStore


def _store(tmp_path, name: str) -> StateStore:
    db = DB(str(tmp_path / f"{name}.db"))
    with db.connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_daily_state (
                bot_instance_id TEXT NOT NULL,
                day TEXT NOT NULL,
                realized_pnl REAL DEFAULT 0,
                kill INTEGER DEFAULT 0,
                trade_count INTEGER DEFAULT 0,
                consecutive_losses INTEGER DEFAULT 0,
                consec_loss_cooldown_until_ms INTEGER DEFAULT 0,
                last_updated_at TEXT,
                PRIMARY KEY(bot_instance_id, day)
            )
            """
        )
        daily_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(bot_daily_state)").fetchall()
        }
        for column, definition in (
            ("trade_count", "INTEGER DEFAULT 0"),
            ("consecutive_losses", "INTEGER DEFAULT 0"),
            ("consec_loss_cooldown_until_ms", "INTEGER DEFAULT 0"),
        ):
            if column not in daily_columns:
                conn.execute(f"ALTER TABLE bot_daily_state ADD COLUMN {column} {definition}")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS positions (
                position_id TEXT PRIMARY KEY,
                bot_instance_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                status TEXT NOT NULL,
                opened_at TEXT NOT NULL,
                closed_at TEXT,
                remaining_qty REAL DEFAULT 0,
                execution_attempt_id TEXT,
                decision_id TEXT,
                broker_account_id TEXT
            )
            """
        )
    return StateStore(db, bot_instance_id="bot-test")


def _insert_position(
    store: StateStore,
    position_id: str,
    symbol: str = "BTCUSDT",
    opened_at: str = "2026-09-17T08:00:00+00:00",
    status: str = "OPEN",
) -> None:
    with store.db.connect() as conn:
        conn.execute(
            """
            INSERT INTO positions(
                position_id, bot_instance_id, symbol, side, status, opened_at,
                remaining_qty, execution_attempt_id, decision_id, broker_account_id
            )
            VALUES (?, ?, ?, 'LONG', ?, ?, 1, 'attempt-1', 'decision-1', 'acct-1')
            """,
            (position_id, store.bot_instance_id, symbol, status, opened_at),
        )


def test_first_entry_counts_once_and_duplicate_processing_stays_idempotent(tmp_path):
    store = _store(tmp_path, "first-entry")
    _insert_position(store, "pos-1")

    count, evidence = store.reconstruct_daily_trade_count(
        date(2026, 9, 17), timezone_name="Europe/Rome"
    )
    assert count == 1
    assert evidence == ["pos-1"]

    count, evidence = store.reconcile_daily_trade_count(
        date(2026, 9, 17),
        realized_pnl=0.0,
        kill=False,
        timezone_name="Europe/Rome",
    )
    assert count == 1
    assert evidence == ["pos-1"]

    count, evidence = store.reconcile_daily_trade_count(
        date(2026, 9, 17),
        realized_pnl=0.0,
        kill=False,
        timezone_name="Europe/Rome",
    )
    assert count == 1
    assert evidence == ["pos-1"]


def test_partial_fill_retry_protection_and_close_do_not_increment(tmp_path):
    store = _store(tmp_path, "non-entry-events")
    _insert_position(store, "pos-1")
    with store.db.connect() as conn:
        conn.execute(
            "UPDATE positions SET status='CLOSED', remaining_qty=0, closed_at=? WHERE position_id='pos-1'",
            ("2026-09-17T09:00:00+00:00",),
        )

    count, _ = store.reconstruct_daily_trade_count(date(2026, 9, 17), timezone_name="Europe/Rome")
    assert count == 1


def test_same_symbol_reentry_after_flat_counts_new_economic_trade(tmp_path):
    store = _store(tmp_path, "same-symbol-reentry")
    _insert_position(store, "pos-1", "BTCUSDT")
    _insert_position(store, "pos-2", "BTCUSDT", opened_at="2026-09-17T12:00:00+00:00")

    count, evidence = store.reconstruct_daily_trade_count(
        date(2026, 9, 17), timezone_name="Europe/Rome"
    )

    assert count == 2
    assert evidence == ["pos-1", "pos-2"]


def test_restart_reconciliation_preserves_count_and_new_day_resets(tmp_path):
    store = _store(tmp_path, "restart")
    _insert_position(store, "pos-1")
    _insert_position(store, "pos-2", opened_at="2026-09-17T14:00:00+00:00")
    _insert_position(store, "pos-3", opened_at="2026-09-17T18:00:00+00:00")

    first, _ = store.reconcile_daily_trade_count(
        date(2026, 9, 17), realized_pnl=1.0, kill=False, timezone_name="Europe/Rome"
    )
    restarted, _ = store.reconcile_daily_trade_count(
        date(2026, 9, 17), realized_pnl=1.0, kill=False, timezone_name="Europe/Rome"
    )
    next_day, _ = store.reconstruct_daily_trade_count(
        date(2026, 9, 18), timezone_name="Europe/Rome"
    )

    assert first == 3
    assert restarted == 3
    assert next_day == 0


def test_concurrent_duplicate_safety_uses_atomic_evidence_not_incrementing_memory(tmp_path):
    store = _store(tmp_path, "concurrency")
    _insert_position(store, "pos-1")

    def reconcile_duplicate(_):
        return store.reconcile_daily_trade_count(
            date(2026, 9, 17), realized_pnl=0.0, kill=False, timezone_name="Europe/Rome"
        )[0]

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(reconcile_duplicate, range(2)))

    assert results == [1, 1]
    with store.db.connect() as conn:
        row = conn.execute(
            "SELECT trade_count FROM bot_daily_state WHERE bot_instance_id='bot-test' AND day='2026-09-17'"
        ).fetchone()
    assert row["trade_count"] == 1


def test_europe_rome_dst_day_uses_local_midnight_boundaries(tmp_path):
    store = _store(tmp_path, "dst-boundary")
    _insert_position(store, "before", opened_at="2026-10-24T21:59:59+00:00")
    _insert_position(store, "start", opened_at="2026-10-24T22:00:00+00:00")
    _insert_position(store, "end", opened_at="2026-10-25T22:59:59+00:00")
    _insert_position(store, "after", opened_at="2026-10-25T23:00:00+00:00")

    count, evidence = store.reconstruct_daily_trade_count(
        date(2026, 10, 25), timezone_name="Europe/Rome"
    )

    assert count == 2
    assert evidence == ["start", "end"]


def test_realized_r_today_is_summed_from_authoritative_close_fills(tmp_path):
    from app.runner.runner import PaperRunner

    store = _store(tmp_path, "realized-r")
    with store.db.connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_instance_id TEXT,
                action TEXT,
                timestamp_utc TEXT,
                r_multiple REAL
            )
            """
        )
        insert_sql = (
            "INSERT INTO trade_fills("
            "bot_instance_id,symbol,side,action,qty,price,timestamp_utc,r_multiple"
            ") VALUES (?,'BTCUSDT','LONG',?,1,1,?,?)"
        )
        conn.execute(
            insert_sql,
            ("bot-test", "CLOSE", "2026-09-17T03:02:18+00:00", 0.654415755602081),
        )
        conn.execute(
            insert_sql,
            ("bot-test", "PARTIAL_CLOSE", "2026-09-17T03:06:24+00:00", 0.642308544379651),
        )
        conn.execute(
            insert_sql,
            ("bot-test", "CLOSE", "2026-09-16T20:00:00+00:00", 99.0),
        )

    fake_runner = SimpleNamespace(
        db=store.db,
        context=SimpleNamespace(bot_instance_id="bot-test"),
        daily_budget_engine=SimpleNamespace(
            timezone=ZoneInfo("Europe/Rome"),
            risk_date_for=lambda _: date(2026, 9, 17),
        ),
        _now_utc=lambda: datetime(2026, 9, 17, 12, tzinfo=timezone.utc),
    )

    assert PaperRunner._realized_r_today(fake_runner) == 1.296724299981732
