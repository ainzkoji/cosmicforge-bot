"""Persistent entry-intent protection for duplicate-open prevention."""
from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class EntryState(str, Enum):
    PENDING_OPEN = "PENDING_OPEN"
    OPEN_CONFIRMED = "OPEN_CONFIRMED"
    OPEN_FAILED = "OPEN_FAILED"


class SubmitState(str, Enum):
    NOT_SUBMITTED = "NOT_SUBMITTED"
    SUBMIT_CONFIRMED = "SUBMIT_CONFIRMED"
    SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN"


class AcquireStatus(str, Enum):
    ACQUIRED = "ACQUIRED"
    REUSED = "REUSED"
    BLOCKED = "BLOCKED"


@dataclass
class AcquireResult:
    status: AcquireStatus
    entry: Optional[dict]
    reason: str


class EntryProtection:
    PRE_SUBMIT_STALE_SECONDS = 10
    UNCERTAIN_TTL_SECONDS = 90
    FLAT_CONFIRMATIONS_REQUIRED = 3

    def __init__(self, db) -> None:
        self.db = db
        self._ensure_table()

    def _ensure_table(self) -> None:
        desired_columns = {
            "submit_state": "TEXT NOT NULL DEFAULT 'NOT_SUBMITTED'",
            "intent_key": "TEXT",
            "flat_confirmations": "INTEGER NOT NULL DEFAULT 0",
            "broker_order_id": "TEXT",
            "last_reconcile_at_ms": "INTEGER",
            "sized_notional": "REAL",
            "sized_qty": "REAL",
            "submitted_notional": "REAL",
            "submitted_qty": "REAL",
            "filled_notional": "REAL",
            "filled_qty": "REAL",
            "reference_price": "REAL",
        }
        with self.db.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_entries (
                    id                   TEXT PRIMARY KEY,
                    bot_id               TEXT NOT NULL,
                    symbol               TEXT NOT NULL,
                    side                 TEXT NOT NULL,
                    state                TEXT NOT NULL DEFAULT 'PENDING_OPEN',
                    submit_state         TEXT NOT NULL DEFAULT 'NOT_SUBMITTED',
                    client_order_id      TEXT NOT NULL,
                    broker_order_id      TEXT,
                    intent_key           TEXT,
                    intended_notional    REAL NOT NULL DEFAULT 0.0,
                    sized_notional       REAL,
                    sized_qty            REAL,
                    submitted_notional   REAL,
                    submitted_qty        REAL,
                    filled_notional      REAL,
                    filled_qty           REAL,
                    reference_price      REAL,
                    submitted_at_ms      INTEGER NOT NULL DEFAULT 0,
                    confirmed_at_ms      INTEGER,
                    last_reconcile_at_ms INTEGER,
                    flat_confirmations   INTEGER NOT NULL DEFAULT 0,
                    cycle_id             TEXT,
                    error_reason         TEXT,
                    created_at           TEXT DEFAULT (datetime('now')),
                    updated_at           TEXT DEFAULT (datetime('now')),
                    UNIQUE(bot_id, symbol, side)
                )
                """
            )
            columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(pending_entries)").fetchall()
            }
            for column_name, column_sql in desired_columns.items():
                if column_name not in columns:
                    conn.execute(
                        f"ALTER TABLE pending_entries ADD COLUMN {column_name} {column_sql}"
                    )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pe_bot_symbol
                ON pending_entries(bot_id, symbol)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS entry_protection_events (
                    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_ms              INTEGER NOT NULL,
                    event_type         TEXT NOT NULL,
                    bot_id             TEXT NOT NULL,
                    symbol             TEXT NOT NULL,
                    side               TEXT NOT NULL,
                    state              TEXT,
                    submit_state       TEXT,
                    client_order_id    TEXT,
                    intent_key         TEXT,
                    intended_notional  REAL,
                    sized_notional     REAL,
                    submitted_notional REAL,
                    filled_notional    REAL,
                    reference_price    REAL,
                    protected_exposure REAL,
                    max_exposure_limit REAL,
                    reason             TEXT,
                    cycle_id           TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_epe_bot_symbol_ts
                ON entry_protection_events(bot_id, symbol, ts_ms)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_epe_event_ts
                ON entry_protection_events(event_type, ts_ms)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_epe_client_ts
                ON entry_protection_events(client_order_id, ts_ms)
                """
            )

    def _load_entry(self, bot_id: str, symbol: str, side: str) -> Optional[dict]:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM pending_entries
                WHERE bot_id=? AND symbol=? AND side=?
                """,
                (bot_id, symbol.upper(), side.upper()),
            ).fetchone()
        return dict(row) if row else None

    def _load_entry_conn(self, conn, bot_id: str, symbol: str, side: str) -> Optional[dict]:
        row = conn.execute(
            """
            SELECT *
            FROM pending_entries
            WHERE bot_id=? AND symbol=? AND side=?
            """,
            (bot_id, symbol.upper(), side.upper()),
        ).fetchone()
        return dict(row) if row else None

    def _load_symbol_entries(self, bot_id: str, symbol: str) -> list[dict]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM pending_entries
                WHERE bot_id=? AND symbol=?
                ORDER BY created_at ASC
                """,
                (bot_id, symbol.upper()),
            ).fetchall()
        return [dict(row) for row in rows]

    def _compute_effective_exposure_conn(self, conn, bot_id: str, symbol: str) -> float:
        row = conn.execute(
            """
            SELECT COALESCE(
                SUM(
                    CASE
                        WHEN state = ? AND filled_notional IS NOT NULL AND filled_notional > 0 THEN filled_notional
                        WHEN submitted_notional IS NOT NULL AND submitted_notional > 0 THEN submitted_notional
                        WHEN sized_notional IS NOT NULL AND sized_notional > 0 THEN sized_notional
                        ELSE intended_notional
                    END
                ),
                0.0
            )
            FROM pending_entries
            WHERE bot_id=? AND symbol=?
              AND state IN (?, ?)
            """,
            (
                EntryState.OPEN_CONFIRMED.value,
                bot_id,
                symbol.upper(),
                EntryState.PENDING_OPEN.value,
                EntryState.OPEN_CONFIRMED.value,
            ),
        ).fetchone()
        return float(row[0] if row else 0.0)

    def _append_event(
        self,
        event_type: str,
        bot_id: str,
        symbol: str,
        side: str,
        *,
        conn=None,
        entry: Optional[dict] = None,
        reason: str | None = None,
        protected_exposure: float | None = None,
        max_exposure_limit: float | None = None,
        overrides: Optional[dict] = None,
    ) -> None:
        if conn is None:
            with self.db.connect() as own_conn:
                self._append_event(
                    event_type,
                    bot_id,
                    symbol,
                    side,
                    conn=own_conn,
                    entry=entry,
                    reason=reason,
                    protected_exposure=protected_exposure,
                    max_exposure_limit=max_exposure_limit,
                    overrides=overrides,
                )
            return
        snapshot = dict(entry or {})
        if not snapshot:
            row = conn.execute(
                """
                SELECT *
                FROM pending_entries
                WHERE bot_id=? AND symbol=? AND side=?
                """,
                (bot_id, symbol.upper(), side.upper()),
            ).fetchone()
            if row:
                snapshot = dict(row)
        if overrides:
            snapshot.update(overrides)
        if protected_exposure is None:
            protected_exposure = self._compute_effective_exposure_conn(conn, bot_id, symbol)
        conn.execute(
            """
            INSERT INTO entry_protection_events (
                ts_ms, event_type, bot_id, symbol, side, state, submit_state,
                client_order_id, intent_key, intended_notional, sized_notional,
                submitted_notional, filled_notional, reference_price,
                protected_exposure, max_exposure_limit, reason, cycle_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(time.time() * 1000),
                event_type,
                bot_id,
                symbol.upper(),
                side.upper(),
                snapshot.get("state"),
                snapshot.get("submit_state"),
                snapshot.get("client_order_id"),
                snapshot.get("intent_key"),
                snapshot.get("intended_notional"),
                snapshot.get("sized_notional"),
                snapshot.get("submitted_notional"),
                snapshot.get("filled_notional"),
                snapshot.get("reference_price"),
                protected_exposure,
                max_exposure_limit,
                reason,
                snapshot.get("cycle_id"),
            ),
        )

    def acquire_intent(
        self,
        bot_id: str,
        symbol: str,
        side: str,
        intended_notional: float,
        client_order_id: str,
        intent_key: str,
        cycle_id: Optional[str] = None,
        allow_hedge: bool = False,
    ) -> AcquireResult:
        now_ms = int(time.time() * 1000)
        symbol_up = symbol.upper()
        side_up = side.upper()
        entry_id = str(uuid.uuid4())

        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing_rows = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT *
                    FROM pending_entries
                    WHERE bot_id=? AND symbol=?
                    ORDER BY created_at ASC
                    """,
                    (bot_id, symbol_up),
                ).fetchall()
            ]

            if existing_rows:
                for existing in existing_rows:
                    if (
                        existing.get("side") == side_up
                        and existing.get("intent_key") == intent_key
                    ):
                        logger.warning(
                            "[ENTRY_PROTECTION] INTENT REUSED bot=%s sym=%s side=%s state=%s submit=%s cid=%s",
                            bot_id,
                            symbol_up,
                            side_up,
                            existing.get("state"),
                            existing.get("submit_state"),
                            existing.get("client_order_id"),
                        )
                        self._append_event(
                            "INTENT_REUSED",
                            bot_id,
                            symbol_up,
                            side_up,
                            conn=conn,
                            entry=existing,
                            reason="same_intent_already_locked",
                        )
                        return AcquireResult(
                            status=AcquireStatus.REUSED,
                            entry=existing,
                            reason="same_intent_already_locked",
                        )
                if not allow_hedge:
                    existing = existing_rows[0]
                    logger.warning(
                        "[ENTRY_PROTECTION] SYMBOL BLOCKED bot=%s sym=%s requested_side=%s existing_side=%s state=%s cid=%s",
                        bot_id,
                        symbol_up,
                        side_up,
                        existing.get("side"),
                        existing.get("state"),
                        existing.get("client_order_id"),
                    )
                    self._append_event(
                        "INTENT_BLOCKED",
                        bot_id,
                        symbol_up,
                        side_up,
                        conn=conn,
                        entry=existing,
                        reason="symbol_already_has_active_intent",
                    )
                    return AcquireResult(
                        status=AcquireStatus.BLOCKED,
                        entry=existing,
                        reason="symbol_already_has_active_intent",
                    )
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO pending_entries (
                    id, bot_id, symbol, side, state, submit_state, client_order_id,
                    broker_order_id, intent_key, intended_notional, sized_notional,
                    sized_qty, submitted_notional, submitted_qty, filled_notional,
                    filled_qty, reference_price, submitted_at_ms,
                    confirmed_at_ms, last_reconcile_at_ms, flat_confirmations,
                    cycle_id, error_reason, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                """,
                (
                    entry_id,
                    bot_id,
                    symbol_up,
                    side_up,
                    EntryState.PENDING_OPEN.value,
                    SubmitState.NOT_SUBMITTED.value,
                    client_order_id,
                    None,
                    intent_key,
                    float(intended_notional),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    now_ms,
                    None,
                    None,
                    0,
                    cycle_id,
                    None,
                ),
            )
            if cursor.rowcount == 1:
                created_entry = dict(
                    conn.execute(
                        """
                        SELECT *
                        FROM pending_entries
                        WHERE bot_id=? AND symbol=? AND side=?
                        """,
                        (bot_id, symbol_up, side_up),
                    ).fetchone()
                )
                self._append_event(
                    "INTENT_ACQUIRED",
                    bot_id,
                    symbol_up,
                    side_up,
                    conn=conn,
                    entry=created_entry,
                    reason="new_intent_locked",
                )
                logger.info(
                    "[ENTRY_PROTECTION] LOCK ACQUIRED bot=%s sym=%s side=%s cid=%s intent=%s",
                    bot_id,
                    symbol_up,
                    side_up,
                    client_order_id,
                    intent_key,
                )
                return AcquireResult(
                    status=AcquireStatus.ACQUIRED,
                    entry=created_entry,
                    reason="new_intent_locked",
                )
        existing_rows = self._load_symbol_entries(bot_id, symbol_up)
        existing = existing_rows[0] if existing_rows else None
        logger.warning(
            "[ENTRY_PROTECTION] LOCK BLOCKED bot=%s sym=%s side=%s existing_state=%s existing_cid=%s",
            bot_id,
            symbol_up,
            side_up,
            existing.get("state") if existing else None,
            existing.get("client_order_id") if existing else None,
        )
        if existing:
            self._append_event(
                "INTENT_BLOCKED",
                bot_id,
                symbol_up,
                side_up,
                entry=existing,
                reason="different_active_intent_exists",
            )
        return AcquireResult(
            status=AcquireStatus.BLOCKED,
            entry=existing,
            reason="different_active_intent_exists",
        )

    def mark_submit_confirmed(
        self,
        bot_id: str,
        symbol: str,
        side: str,
        broker_order_id: Optional[str] = None,
        max_exposure_limit: float | None = None,
    ) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE pending_entries
                SET submit_state=?, broker_order_id=COALESCE(?, broker_order_id),
                    error_reason=NULL, updated_at=datetime('now')
                WHERE bot_id=? AND symbol=? AND side=?
                """,
                (
                    SubmitState.SUBMIT_CONFIRMED.value,
                    broker_order_id,
                    bot_id,
                    symbol.upper(),
                    side.upper(),
                ),
            )
            entry = self._load_entry_conn(conn, bot_id, symbol, side)
            if entry:
                self._append_event(
                    "SUBMIT_CONFIRMED",
                    bot_id,
                    symbol,
                    side,
                    conn=conn,
                    entry=entry,
                    max_exposure_limit=max_exposure_limit,
                )

    def mark_sized(
        self,
        bot_id: str,
        symbol: str,
        side: str,
        sized_notional: float,
        sized_qty: float,
        reference_price: float,
        max_exposure_limit: float | None = None,
    ) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE pending_entries
                SET sized_notional=?, sized_qty=?, reference_price=?, updated_at=datetime('now')
                WHERE bot_id=? AND symbol=? AND side=?
                """,
                (
                    float(sized_notional),
                    float(sized_qty),
                    float(reference_price),
                    bot_id,
                    symbol.upper(),
                    side.upper(),
                ),
            )
            entry = self._load_entry_conn(conn, bot_id, symbol, side)
            if entry:
                self._append_event(
                    "SIZED",
                    bot_id,
                    symbol,
                    side,
                    conn=conn,
                    entry=entry,
                    max_exposure_limit=max_exposure_limit,
                )

    def mark_submit_prepared(
        self,
        bot_id: str,
        symbol: str,
        side: str,
        submitted_notional: float,
        submitted_qty: float,
        reference_price: float,
        max_exposure_limit: float | None = None,
    ) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE pending_entries
                SET submitted_notional=?, submitted_qty=?, reference_price=?, updated_at=datetime('now')
                WHERE bot_id=? AND symbol=? AND side=?
                """,
                (
                    float(submitted_notional),
                    float(submitted_qty),
                    float(reference_price),
                    bot_id,
                    symbol.upper(),
                    side.upper(),
                ),
            )
            entry = self._load_entry_conn(conn, bot_id, symbol, side)
            if entry:
                self._append_event(
                    "SUBMIT_PREPARED",
                    bot_id,
                    symbol,
                    side,
                    conn=conn,
                    entry=entry,
                    max_exposure_limit=max_exposure_limit,
                )

    def mark_submit_unknown(
        self,
        bot_id: str,
        symbol: str,
        side: str,
        reason: str,
        max_exposure_limit: float | None = None,
    ) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE pending_entries
                SET submit_state=?, error_reason=?, updated_at=datetime('now')
                WHERE bot_id=? AND symbol=? AND side=?
                """,
                (
                    SubmitState.SUBMIT_UNKNOWN.value,
                    reason,
                    bot_id,
                    symbol.upper(),
                    side.upper(),
                ),
            )
            entry = self._load_entry_conn(conn, bot_id, symbol, side)
            if entry:
                self._append_event(
                    "SUBMIT_UNKNOWN",
                    bot_id,
                    symbol,
                    side,
                    conn=conn,
                    entry=entry,
                    reason=reason,
                    max_exposure_limit=max_exposure_limit,
                )
        logger.warning(
            "[ENTRY_PROTECTION] SUBMIT UNKNOWN bot=%s sym=%s side=%s reason=%s",
            bot_id,
            symbol.upper(),
            side.upper(),
            reason,
        )

    def mark_confirmed(
        self,
        bot_id: str,
        symbol: str,
        side: str,
        filled_notional: Optional[float] = None,
        filled_qty: Optional[float] = None,
        max_exposure_limit: float | None = None,
    ) -> None:
        now_ms = int(time.time() * 1000)
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE pending_entries
                SET state=?, submit_state=?, confirmed_at_ms=?, flat_confirmations=0,
                    filled_notional=COALESCE(?, filled_notional),
                    filled_qty=COALESCE(?, filled_qty),
                    last_reconcile_at_ms=?, updated_at=datetime('now')
                WHERE bot_id=? AND symbol=? AND side=?
                """,
                (
                    EntryState.OPEN_CONFIRMED.value,
                    SubmitState.SUBMIT_CONFIRMED.value,
                    now_ms,
                    filled_notional,
                    filled_qty,
                    now_ms,
                    bot_id,
                    symbol.upper(),
                    side.upper(),
                ),
            )
            entry = self._load_entry_conn(conn, bot_id, symbol, side)
            if entry:
                self._append_event(
                    "OPEN_CONFIRMED",
                    bot_id,
                    symbol,
                    side,
                    conn=conn,
                    entry=entry,
                    max_exposure_limit=max_exposure_limit,
                )
        logger.info(
            "[ENTRY_PROTECTION] OPEN CONFIRMED bot=%s sym=%s side=%s",
            bot_id,
            symbol.upper(),
            side.upper(),
        )

    def _extract_fill_metrics(self, entry: dict, order_snapshot: Optional[dict]) -> tuple[Optional[float], Optional[float]]:
        if not order_snapshot:
            return None, None
        try:
            filled_qty = float(
                order_snapshot.get("executedQty")
                or order_snapshot.get("qty_filled")
                or order_snapshot.get("filledQty")
                or 0.0
            )
        except Exception:
            filled_qty = 0.0
        try:
            avg_price = float(
                order_snapshot.get("avgPrice")
                or order_snapshot.get("avg_fill_price")
                or order_snapshot.get("price")
                or entry.get("reference_price")
                or 0.0
            )
        except Exception:
            avg_price = 0.0
        if filled_qty <= 0 or avg_price <= 0:
            return None, None
        return filled_qty * avg_price, filled_qty

    def mark_failed(self, bot_id: str, symbol: str, side: str, reason: str) -> None:
        symbol_up = symbol.upper()
        side_up = side.upper()
        with self.db.connect() as conn:
            entry = self._load_entry_conn(conn, bot_id, symbol_up, side_up)
            conn.execute(
                """
                UPDATE pending_entries
                SET state=?, error_reason=?, updated_at=datetime('now')
                WHERE bot_id=? AND symbol=? AND side=?
                """,
                (EntryState.OPEN_FAILED.value, reason, bot_id, symbol_up, side_up),
            )
            if entry:
                failed_snapshot = dict(entry)
                failed_snapshot["state"] = EntryState.OPEN_FAILED.value
                failed_snapshot["error_reason"] = reason
                self._append_event(
                    "RELEASED",
                    bot_id,
                    symbol_up,
                    side_up,
                    conn=conn,
                    entry=failed_snapshot,
                    reason=reason,
                )
            conn.execute(
                "DELETE FROM pending_entries WHERE bot_id=? AND symbol=? AND side=?",
                (bot_id, symbol_up, side_up),
            )
        logger.info(
            "[ENTRY_PROTECTION] FAILED -> FLAT bot=%s sym=%s side=%s reason=%s",
            bot_id,
            symbol_up,
            side_up,
            reason,
        )

    def mark_closed(self, bot_id: str, symbol: str, side: str) -> None:
        with self.db.connect() as conn:
            entry = self._load_entry_conn(conn, bot_id, symbol, side)
            if entry:
                self._append_event(
                    "MARK_CLOSED",
                    bot_id,
                    symbol,
                    side,
                    conn=conn,
                    entry=entry,
                )
            conn.execute(
                "DELETE FROM pending_entries WHERE bot_id=? AND symbol=? AND side=?",
                (bot_id, symbol.upper(), side.upper()),
            )
        logger.info(
            "[ENTRY_PROTECTION] CLOSED -> FLAT bot=%s sym=%s side=%s",
            bot_id,
            symbol.upper(),
            side.upper(),
        )

    def reconcile_entry(
        self,
        bot_id: str,
        symbol: str,
        side: str,
        exchange_pos_amt: float,
        order_evidence: Optional[str] = None,
        order_snapshot: Optional[dict] = None,
    ) -> str:
        entry = self._load_entry(bot_id, symbol, side)
        if not entry:
            return "NOT_FOUND"

        now_ms = int(time.time() * 1000)
        symbol_up = symbol.upper()
        side_up = side.upper()
        pos_matches = (
            (side_up == "LONG" and exchange_pos_amt > 1e-9)
            or (side_up == "SHORT" and exchange_pos_amt < -1e-9)
        )
        if pos_matches:
            filled_notional, filled_qty = self._extract_fill_metrics(entry, order_snapshot)
            self.mark_confirmed(
                bot_id,
                symbol_up,
                side_up,
                filled_notional=filled_notional,
                filled_qty=filled_qty,
            )
            return "CONFIRMED"

        evidence = (order_evidence or "").upper()
        # "FILLED" means the entry order completed — the position may no longer be open.
        # Historical FILLED orders are always returned by Binance, so including FILLED here
        # would reset flat_confirmations=0 on every cycle, making mark_closed() unreachable.
        # Only genuinely active/pending order states count as live-position evidence.
        _ACTIVE_ORDER_STATUSES = {"OPEN", "FOUND", "PARTIALLY_FILLED", "NEW", "PENDING_CANCEL"}
        if evidence in _ACTIVE_ORDER_STATUSES:
            filled_notional, filled_qty = self._extract_fill_metrics(entry, order_snapshot)
            with self.db.connect() as conn:
                conn.execute(
                    """
                    UPDATE pending_entries
                    SET submit_state=?, flat_confirmations=0,
                        filled_notional=COALESCE(?, filled_notional),
                        filled_qty=COALESCE(?, filled_qty),
                        last_reconcile_at_ms=?, updated_at=datetime('now')
                    WHERE bot_id=? AND symbol=? AND side=?
                    """,
                    (
                        SubmitState.SUBMIT_CONFIRMED.value,
                        filled_notional,
                        filled_qty,
                        now_ms,
                        bot_id,
                        symbol_up,
                        side_up,
                    ),
                )
                held_entry = self._load_entry_conn(conn, bot_id, symbol_up, side_up)
                if held_entry and (
                    str(entry.get("submit_state") or "") != SubmitState.SUBMIT_CONFIRMED.value
                    or int(entry.get("flat_confirmations") or 0) > 0
                ):
                    self._append_event(
                        "RECONCILE_HELD",
                        bot_id,
                        symbol_up,
                        side_up,
                        conn=conn,
                        entry=held_entry,
                        reason=f"order_evidence:{evidence}",
                    )
            return "ORDER_EVIDENCE_HELD"

        flat_confirmations = int(entry.get("flat_confirmations") or 0) + 1
        submitted_at_ms = int(entry.get("submitted_at_ms") or now_ms)
        submit_state = SubmitState(
            entry.get("submit_state") or SubmitState.NOT_SUBMITTED.value
        )
        ttl_seconds = (
            self.PRE_SUBMIT_STALE_SECONDS
            if submit_state == SubmitState.NOT_SUBMITTED
            else self.UNCERTAIN_TTL_SECONDS
        )
        age_s = (now_ms - submitted_at_ms) / 1000.0

        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE pending_entries
                SET flat_confirmations=?, last_reconcile_at_ms=?, updated_at=datetime('now')
                WHERE bot_id=? AND symbol=? AND side=?
                """,
                (
                    flat_confirmations,
                    now_ms,
                    bot_id,
                    symbol_up,
                    side_up,
                ),
            )

        if entry.get("state") == EntryState.OPEN_CONFIRMED.value:
            if flat_confirmations >= self.FLAT_CONFIRMATIONS_REQUIRED:
                self.mark_closed(bot_id, symbol_up, side_up)
                return "CLOSED"
            # ── Safety-net force-clear for entries confirmed >30 min ago but still flat ──
            # Guards against any future evidence source that may still reset flat_confirmations.
            # 30 min = 2 full 15m candles — long enough to rule out transient API zeros.
            _FORCE_CLEAR_AGE_MS = 30 * 60 * 1000
            _confirmed_at = int(entry.get("confirmed_at_ms") or entry.get("submitted_at_ms") or now_ms)
            if (now_ms - _confirmed_at) > _FORCE_CLEAR_AGE_MS and flat_confirmations >= 1:
                logger.warning(
                    "[ENTRY_PROTECTION] FORCE_CLEAR bot=%s sym=%s side=%s: "
                    "OPEN_CONFIRMED for >30 min with exchange flat. Clearing stale lock.",
                    bot_id, symbol_up, side_up,
                )
                self.mark_closed(bot_id, symbol_up, side_up)
                return "CLOSED"
            if flat_confirmations == 1:
                self._append_event(
                    "RECONCILE_HELD",
                    bot_id,
                    symbol_up,
                    side_up,
                    entry=self._load_entry(bot_id, symbol_up, side_up) or entry,
                    reason="waiting_stronger_flat_proof",
                )
            return "WAITING_STRONGER_FLAT_PROOF"

        if flat_confirmations >= self.FLAT_CONFIRMATIONS_REQUIRED and age_s >= ttl_seconds:
            self.mark_failed(
                bot_id,
                symbol_up,
                side_up,
                reason=f"reconcile_cleared_after_{flat_confirmations}_flat_reads_{age_s:.0f}s",
            )
            return "CLEARED"
        if flat_confirmations == 1:
            self._append_event(
                "RECONCILE_HELD",
                bot_id,
                symbol_up,
                side_up,
                entry=self._load_entry(bot_id, symbol_up, side_up) or entry,
                reason="waiting_stronger_flat_proof",
            )
        return "WAITING_STRONGER_FLAT_PROOF"

    def get_effective_exposure(self, bot_id: str, symbol: str) -> float:
        with self.db.connect() as conn:
            return self._compute_effective_exposure_conn(conn, bot_id, symbol)

    def record_exposure_blocked(
        self,
        bot_id: str,
        symbol: str,
        side: str,
        *,
        reason: str,
        max_exposure_limit: float,
        protected_exposure: float,
        intended_notional: float | None = None,
        sized_notional: float | None = None,
        submitted_notional: float | None = None,
        filled_notional: float | None = None,
        reference_price: float | None = None,
        client_order_id: str | None = None,
        intent_key: str | None = None,
        cycle_id: str | None = None,
        state: str | None = None,
        submit_state: str | None = None,
    ) -> None:
        self._append_event(
            "EXPOSURE_BLOCKED",
            bot_id,
            symbol,
            side,
            reason=reason,
            protected_exposure=protected_exposure,
            max_exposure_limit=max_exposure_limit,
            overrides={
                "state": state or EntryState.PENDING_OPEN.value,
                "submit_state": submit_state or SubmitState.NOT_SUBMITTED.value,
                "client_order_id": client_order_id,
                "intent_key": intent_key,
                "intended_notional": intended_notional,
                "sized_notional": sized_notional,
                "submitted_notional": submitted_notional,
                "filled_notional": filled_notional,
                "reference_price": reference_price,
                "cycle_id": cycle_id,
            },
        )

    def get_entry(self, bot_id: str, symbol: str, side: str) -> Optional[dict]:
        return self._load_entry(bot_id, symbol, side)

    def list_entries(self, bot_id: str, symbol: Optional[str] = None) -> list[dict]:
        query = "SELECT * FROM pending_entries WHERE bot_id=?"
        params: list[object] = [bot_id]
        if symbol:
            query += " AND symbol=?"
            params.append(symbol.upper())
        with self.db.connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def list_events(
        self,
        bot_id: Optional[str] = None,
        symbol: Optional[str] = None,
        limit: int = 200,
    ) -> list[dict]:
        query = "SELECT * FROM entry_protection_events WHERE 1=1"
        params: list[object] = []
        if bot_id:
            query += " AND bot_id=?"
            params.append(bot_id)
        if symbol:
            query += " AND symbol=?"
            params.append(symbol.upper())
        query += " ORDER BY ts_ms DESC, id DESC LIMIT ?"
        params.append(int(limit))
        with self.db.connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def is_locked(self, bot_id: str, symbol: str, side: str) -> bool:
        return self._load_entry(bot_id, symbol, side) is not None

    def clear_all_for_bot(self, bot_id: str) -> None:
        with self.db.connect() as conn:
            conn.execute("DELETE FROM pending_entries WHERE bot_id=?", (bot_id,))
        logger.warning("[ENTRY_PROTECTION] ALL entries cleared for bot=%s", bot_id)

    def release_entry(
        self,
        bot_id: str,
        symbol: str,
        side: str,
        reason: str,
    ) -> Optional[dict]:
        entry = self._load_entry(bot_id, symbol, side)
        if not entry:
            return None
        self.mark_failed(bot_id, symbol, side, reason=reason)
        return entry


_registry: Dict[int, EntryProtection] = {}


def get_entry_protection(db) -> EntryProtection:
    key = id(db)
    if key not in _registry:
        _registry[key] = EntryProtection(db)
    return _registry[key]
