"""Section H: tables of the per-account multi-position portfolio, the reservation ledger and the risk brakes (Steps 3.7, 3.10).

Part of the CATI platform schema; see ``cati_platform_schema`` for the rules every statement here follows
(additive, idempotent, ``IF NOT EXISTS``, no existing table rewritten).

Two groups of tables, both new. Nothing here reads, alters or replaces a table of an older build: the five
earlier reservation stores keep their tables, their rows and their own code.

**The reservation ledger** (``cati_reservation_ledger`` and its event log) is the one place a NEW portfolio
reservation is written: one row per intent of one account, holding its position slot, its risk at the stop and
its margin. Open claims of the two durable legacy stores are mirrored into it (``source`` says where a row
comes from), so one query answers "what is this account committed to". The database itself keeps the rules
that must not depend on a caller being careful:

* an order whose outcome is unknown can leave ``UNKNOWN`` only with resolution evidence, and never to ``EXPIRED``;
* a submitted order can be released only with evidence, and never expires;
* ``FILLED``, ``RELEASED`` and ``EXPIRED`` are final;
* a row that is still open cannot be deleted;
* every insert and every state change appends one row to the event log, which accepts no UPDATE or DELETE;
* one account and book holds at most one open pipeline reservation per symbol.

**The brake state** (``cati_brake_state``, its event log and the restart records) is the per-account risk-day
state of Step 3.10. A terminal stop is a column of a persisted row, so it survives a restart of the process;
the database refuses to clear it unless the row names a pending restart record made for that very stop, and
refuses to delete a row that is stopped.
"""
from __future__ import annotations

from typing import Any, Tuple

from shared_lib.persistence.cati_platform_schema import append_only_triggers

LEDGER_TABLE = "cati_reservation_ledger"
LEDGER_EVENT_TABLE = "cati_reservation_ledger_events"
BRAKE_STATE_TABLE = "cati_brake_state"
BRAKE_EVENT_TABLE = "cati_brake_events"
BRAKE_RESTART_TABLE = "cati_brake_restarts"

#: a reservation that still holds capacity (``FILLED`` holds it only until a newer account snapshot shows the position)
LEDGER_OPEN_STATES: Tuple[str, ...] = ("RESERVED", "SUBMITTED", "UNKNOWN")
LEDGER_FINAL_STATES: Tuple[str, ...] = ("FILLED", "RELEASED", "EXPIRED")
LEDGER_STATES: Tuple[str, ...] = LEDGER_OPEN_STATES + LEDGER_FINAL_STATES
LEDGER_SOURCES: Tuple[str, ...] = ("PIPELINE", "LEGACY_CATI_RESERVATION", "LEGACY_PENDING_ENTRY")
LEDGER_BOOKS: Tuple[str, ...] = ("EXECUTION", "SHADOW")


def _in(values: Tuple[str, ...]) -> str:
    return "(" + ", ".join(f"'{v}'" for v in values) + ")"


DDL: Tuple[str, ...] = (
    # ---------------------------------------------------------------------------------- reservation ledger (3.7)
    f"""CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} (
        reservation_id TEXT PRIMARY KEY,
        account_id TEXT NOT NULL,
        user_id TEXT,
        book TEXT NOT NULL CHECK (book IN {_in(LEDGER_BOOKS)}),
        mode TEXT NOT NULL,
        source TEXT NOT NULL CHECK (source IN {_in(LEDGER_SOURCES)}),
        legacy_ref TEXT,
        legacy_generation INTEGER NOT NULL DEFAULT 0,
        intent_key TEXT NOT NULL,
        family_id TEXT NOT NULL,
        decision_id TEXT NOT NULL,
        decision_hash TEXT,
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,
        kind TEXT NOT NULL,
        quantity REAL,
        reference_price REAL,
        stop_price REAL,
        risk_amount REAL,
        notional REAL,
        margin REAL,
        leverage REAL,
        awaits_releases TEXT NOT NULL DEFAULT '[]',
        state TEXT NOT NULL CHECK (state IN {_in(LEDGER_STATES)}),
        created_at INTEGER NOT NULL,
        expires_at INTEGER,
        updated_at INTEGER NOT NULL,
        submitted_at INTEGER,
        client_order_id TEXT,
        unknown_since INTEGER,
        resolution_deadline INTEGER,
        filled_at INTEGER,
        filled_quantity REAL,
        filled_price REAL,
        resolution_evidence TEXT,
        last_actor TEXT NOT NULL DEFAULT '',
        last_detail TEXT NOT NULL DEFAULT '',
        snapshot_hash TEXT,
        limits_hash TEXT,
        authorization_hash TEXT,
        UNIQUE(account_id, book, intent_key))""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_rled_account_state ON {LEDGER_TABLE}(account_id, book, state)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_rled_state_expiry ON {LEDGER_TABLE}(state, expires_at)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_rled_decision ON {LEDGER_TABLE}(decision_id)",
    # one open pipeline claim per symbol of an account: a second concurrent entry cannot be inserted at all
    f"""CREATE UNIQUE INDEX IF NOT EXISTS uq_cati_rled_open_symbol ON {LEDGER_TABLE}(account_id, book, symbol)
        WHERE source = 'PIPELINE' AND state IN {_in(LEDGER_OPEN_STATES)}""",
    # a legacy row is mirrored once per life: a legacy id that is reserved again after it ended is a new generation
    f"""CREATE UNIQUE INDEX IF NOT EXISTS uq_cati_rled_legacy ON {LEDGER_TABLE}(source, legacy_ref, legacy_generation)
        WHERE legacy_ref IS NOT NULL""",
    f"""CREATE TABLE IF NOT EXISTS {LEDGER_EVENT_TABLE} (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        reservation_id TEXT NOT NULL,
        account_id TEXT NOT NULL,
        from_state TEXT,
        to_state TEXT NOT NULL,
        recorded_at INTEGER NOT NULL,
        actor TEXT NOT NULL,
        detail TEXT NOT NULL)""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_rlev_reservation ON {LEDGER_EVENT_TABLE}(reservation_id, seq)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_rlev_account ON {LEDGER_EVENT_TABLE}(account_id, seq)",
    *append_only_triggers(LEDGER_EVENT_TABLE, "cati_rlev"),
    # the history is written by the database, so no code path can change a state without leaving a record
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_rled_event_insert AFTER INSERT ON {LEDGER_TABLE}
        BEGIN INSERT INTO {LEDGER_EVENT_TABLE} (reservation_id, account_id, from_state, to_state, recorded_at, actor, detail)
              VALUES (NEW.reservation_id, NEW.account_id, NULL, NEW.state, NEW.updated_at, NEW.last_actor, NEW.last_detail);
        END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_rled_event_state AFTER UPDATE OF state ON {LEDGER_TABLE}
        WHEN OLD.state <> NEW.state
        BEGIN INSERT INTO {LEDGER_EVENT_TABLE} (reservation_id, account_id, from_state, to_state, recorded_at, actor, detail)
              VALUES (NEW.reservation_id, NEW.account_id, OLD.state, NEW.state, NEW.updated_at, NEW.last_actor, NEW.last_detail);
        END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_rled_final BEFORE UPDATE OF state ON {LEDGER_TABLE}
        WHEN OLD.state IN {_in(LEDGER_FINAL_STATES)} AND NEW.state <> OLD.state
        BEGIN SELECT RAISE(ABORT, '{LEDGER_TABLE}: a final reservation does not change state'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_rled_unknown BEFORE UPDATE OF state ON {LEDGER_TABLE}
        WHEN OLD.state = 'UNKNOWN' AND NEW.state <> 'UNKNOWN'
             AND (NEW.state NOT IN ('FILLED', 'RELEASED') OR trim(COALESCE(NEW.resolution_evidence, '')) = '')
        BEGIN SELECT RAISE(ABORT, '{LEDGER_TABLE}: an unknown outcome is resolved only with evidence'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_rled_submitted BEFORE UPDATE OF state ON {LEDGER_TABLE}
        WHEN OLD.state = 'SUBMITTED' AND (NEW.state IN ('RESERVED', 'EXPIRED')
             OR (NEW.state = 'RELEASED' AND trim(COALESCE(NEW.resolution_evidence, '')) = ''))
        BEGIN SELECT RAISE(ABORT, '{LEDGER_TABLE}: a submitted order is released only with evidence'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_rled_identity BEFORE UPDATE OF account_id, book, intent_key, symbol,
            direction, kind, source ON {LEDGER_TABLE}
        BEGIN SELECT RAISE(ABORT, '{LEDGER_TABLE}: the identity of a reservation is immutable'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_rled_no_open_delete BEFORE DELETE ON {LEDGER_TABLE}
        WHEN OLD.state IN {_in(LEDGER_OPEN_STATES)}
        BEGIN SELECT RAISE(ABORT, '{LEDGER_TABLE}: an open reservation is never deleted'); END""",
    # ---------------------------------------------------------------------------------------- risk brakes (3.10)
    f"""CREATE TABLE IF NOT EXISTS {BRAKE_STATE_TABLE} (
        account_id TEXT NOT NULL,
        scope TEXT NOT NULL,
        risk_level TEXT NOT NULL,
        profile_version TEXT NOT NULL,
        daily_pause REAL NOT NULL,
        halve_drawdown REAL NOT NULL,
        stop_drawdown REAL NOT NULL,
        day_definition TEXT NOT NULL,
        first_day TEXT NOT NULL,
        risk_day TEXT NOT NULL,
        opening_equity REAL NOT NULL,
        closing_equity REAL NOT NULL,
        highest_closing_equity REAL NOT NULL,
        daily_return REAL NOT NULL,
        daily_loss REAL NOT NULL,
        drawdown REAL NOT NULL,
        paused INTEGER NOT NULL DEFAULT 0,
        halve INTEGER NOT NULL DEFAULT 0,
        halted INTEGER NOT NULL DEFAULT 0,
        active_brake TEXT NOT NULL DEFAULT 'NONE',
        reason TEXT,
        triggered_at INTEGER,
        triggered_day TEXT,
        stop_seq INTEGER,
        restart_id TEXT,
        restart_pending INTEGER NOT NULL DEFAULT 0,
        restart_by TEXT,
        restart_role TEXT,
        restart_reference TEXT,
        restart_at INTEGER,
        restart_applied_day TEXT,
        evaluations INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY(account_id, scope))""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_brk_halted ON {BRAKE_STATE_TABLE}(halted, account_id)",
    f"""CREATE TABLE IF NOT EXISTS {BRAKE_EVENT_TABLE} (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        account_id TEXT NOT NULL,
        scope TEXT NOT NULL,
        event_type TEXT NOT NULL,
        risk_day TEXT,
        recorded_at INTEGER NOT NULL,
        actor TEXT NOT NULL,
        payload TEXT NOT NULL)""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_brkev_account ON {BRAKE_EVENT_TABLE}(account_id, scope, seq)",
    *append_only_triggers(BRAKE_EVENT_TABLE, "cati_brkev"),
    f"""CREATE TABLE IF NOT EXISTS {BRAKE_RESTART_TABLE} (
        restart_id TEXT PRIMARY KEY,
        account_id TEXT NOT NULL,
        scope TEXT NOT NULL,
        stop_seq INTEGER NOT NULL,
        authorized_by TEXT NOT NULL CHECK (length(trim(authorized_by)) > 0),
        authorized_role TEXT NOT NULL CHECK (authorized_role IN ('USER', 'OPERATOR')),
        reference TEXT NOT NULL CHECK (length(trim(reference)) > 0),
        authorized_at INTEGER NOT NULL,
        document TEXT NOT NULL)""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_brkrs_account ON {BRAKE_RESTART_TABLE}(account_id, scope, stop_seq)",
    *append_only_triggers(BRAKE_RESTART_TABLE, "cati_brkrs"),
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_brk_latch BEFORE UPDATE OF halted ON {BRAKE_STATE_TABLE}
        WHEN OLD.halted = 1 AND NEW.halted <> 1 AND (OLD.restart_pending <> 1 OR NOT EXISTS (
            SELECT 1 FROM {BRAKE_RESTART_TABLE} r WHERE r.restart_id = NEW.restart_id AND r.account_id = OLD.account_id
               AND r.scope = OLD.scope AND r.stop_seq = OLD.stop_seq))
        BEGIN SELECT RAISE(ABORT, '{BRAKE_STATE_TABLE}: a terminal stop is cleared only by a restart record'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_brk_identity BEFORE UPDATE OF account_id, scope ON {BRAKE_STATE_TABLE}
        BEGIN SELECT RAISE(ABORT, '{BRAKE_STATE_TABLE}: the identity of a brake state is immutable'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_brk_no_stopped_delete BEFORE DELETE ON {BRAKE_STATE_TABLE}
        WHEN OLD.halted = 1
        BEGIN SELECT RAISE(ABORT, '{BRAKE_STATE_TABLE}: a stopped account is never deleted'); END""",
)


def upgrade(conn: Any) -> None:
    """Additive column changes of this module's tables. None yet."""


__all__ = ["DDL", "upgrade", "LEDGER_TABLE", "LEDGER_EVENT_TABLE", "BRAKE_STATE_TABLE", "BRAKE_EVENT_TABLE",
           "BRAKE_RESTART_TABLE", "LEDGER_OPEN_STATES", "LEDGER_FINAL_STATES", "LEDGER_STATES", "LEDGER_SOURCES",
           "LEDGER_BOOKS"]
