"""Section H: tables of multi-position execution and durable stop replacement (Steps 3.8, 3.9).

Part of the CATI platform schema; see ``cati_platform_schema`` for the rules every statement here follows
(additive, idempotent, ``IF NOT EXISTS``, no existing table rewritten).

Six tables, all new. They hold what a process must still know after it died between two calls to an exchange,
so the database keeps the rules that must not depend on a caller being careful:

* ``cati_execution_intents`` is the intent state machine: one row per ORDER of one intent of one decision of
  one account (``sequence`` 0 is the intent's first order; a market fallback, a superseding order or a
  fail-safe close takes the next number). The row exists, with its client order id, before the order leaves.
  A final row never changes state; an order that may have left never returns to "not sent"; an unknown
  outcome leaves ``UNKNOWN`` only with evidence; an open row is never deleted; its identity never changes.
* ``cati_execution_events`` is the append-only journal. The database itself appends one row for every insert
  and every state change of an intent, and the code appends one row BEFORE and one AFTER every call that can
  change something at a venue. A ``VENUE_BEFORE`` without its ``VENUE_AFTER`` is a call whose outcome is not
  known: recovery asks the venue, it never assumes.
* ``cati_protection_state`` says, per account and symbol, which exchange-side stop protects the position and
  how sure that is. A row whose protection is pending, uncertain or awaiting retirement cannot be deleted.
* ``cati_protection_replacements`` is one row per stop replacement, advanced one step at a time and written
  before the step is taken. A finished replacement never changes again, so a repeated request changes nothing.
* ``cati_execution_blocks`` holds the durable "no new entries" blocks of an account. A block is cleared only
  with evidence, a cleared block is final, and an active block cannot be deleted.
* ``cati_execution_backoff`` holds the time before which a venue told this account not to ask again. It only
  moves forward.

No statement here reads or changes a table of an older build.
"""
from __future__ import annotations

from typing import Any, Tuple

from shared_lib.persistence.cati_platform_schema import append_only_triggers

INTENT_TABLE = "cati_execution_intents"
EVENT_TABLE = "cati_execution_events"
PROTECTION_TABLE = "cati_protection_state"
REPLACEMENT_TABLE = "cati_protection_replacements"
BLOCK_TABLE = "cati_execution_blocks"
BACKOFF_TABLE = "cati_execution_backoff"

#: an intent order that is not finished. ``PLANNED`` and ``DEFERRED`` have no order at the venue (never sent,
#: or definitively refused for a reason that allows the SAME request later); the others may have one.
INTENT_UNSENT_STATES: Tuple[str, ...] = ("PLANNED", "DEFERRED")
INTENT_SENT_STATES: Tuple[str, ...] = ("SUBMITTING", "OPEN", "CANCELING", "UNKNOWN")
INTENT_OPEN_STATES: Tuple[str, ...] = INTENT_UNSENT_STATES + INTENT_SENT_STATES
INTENT_FINAL_STATES: Tuple[str, ...] = ("FILLED", "PARTIAL", "CANCELED", "REJECTED", "ABSENT", "REFUSED", "EXPIRED",
                                        "DONE")
INTENT_STATES: Tuple[str, ...] = INTENT_OPEN_STATES + INTENT_FINAL_STATES

PROTECTION_STATUSES: Tuple[str, ...] = ("PENDING", "PROTECTED", "UNCERTAIN", "UNDER_PROTECTED", "ABSENT",
                                        "RETIRE_PENDING", "FLAT")
#: protection that still needs work or an answer: such a row is never deleted
PROTECTION_UNSETTLED: Tuple[str, ...] = ("PENDING", "UNCERTAIN", "UNDER_PROTECTED", "ABSENT", "RETIRE_PENDING")

REPLACEMENT_STEPS: Tuple[str, ...] = ("READ", "BUILT", "ESTABLISHING", "ESTABLISHED", "CONFIRMED", "RETIRING",
                                      "RECONCILED", "PERSISTED")
REPLACEMENT_OPEN, REPLACEMENT_DONE, REPLACEMENT_REFUSED, REPLACEMENT_UNWOUND = "OPEN", "DONE", "REFUSED", "UNWOUND"
REPLACEMENT_FINAL_STATES: Tuple[str, ...] = (REPLACEMENT_DONE, REPLACEMENT_REFUSED, REPLACEMENT_UNWOUND)
REPLACEMENT_STATES: Tuple[str, ...] = (REPLACEMENT_OPEN,) + REPLACEMENT_FINAL_STATES

BLOCK_ACTIVE, BLOCK_CLEARED = "ACTIVE", "CLEARED"
BLOCK_SCOPES: Tuple[str, ...] = ("ACCOUNT", "SYMBOL")
EVENT_TYPES: Tuple[str, ...] = ("STATE", "VENUE_BEFORE", "VENUE_AFTER", "GATE", "BLOCK", "PROTECTION", "NOTE")


def _in(values: Tuple[str, ...]) -> str:
    return "(" + ", ".join(f"'{v}'" for v in values) + ")"


DDL: Tuple[str, ...] = (
    # -------------------------------------------------------------------------------- intent state machine (3.8)
    f"""CREATE TABLE IF NOT EXISTS {INTENT_TABLE} (
        intent_id TEXT PRIMARY KEY,
        account_id TEXT NOT NULL,
        user_id TEXT,
        venue TEXT NOT NULL,
        environment TEXT NOT NULL,
        family_id TEXT NOT NULL,
        decision_id TEXT NOT NULL,
        decision_hash TEXT NOT NULL,
        batch_hash TEXT NOT NULL,
        symbol TEXT NOT NULL,
        kind TEXT NOT NULL,
        direction TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        role TEXT NOT NULL,
        increases_risk INTEGER NOT NULL,
        client_order_id TEXT,
        venue_order_id TEXT,
        side TEXT,
        order_type TEXT,
        reduce_only INTEGER NOT NULL DEFAULT 0,
        quantity REAL,
        limit_price REAL,
        reference_price REAL,
        stop_price REAL,
        max_slippage_bps REAL,
        reservation_id TEXT,
        state TEXT NOT NULL CHECK (state IN {_in(INTENT_STATES)}),
        filled_quantity REAL NOT NULL DEFAULT 0,
        average_price REAL,
        reason TEXT NOT NULL DEFAULT '',
        evidence TEXT,
        intent_document TEXT NOT NULL,
        expires_at INTEGER NOT NULL,
        created_at INTEGER NOT NULL,
        submitted_at INTEGER,
        updated_at INTEGER NOT NULL,
        UNIQUE(account_id, decision_id, symbol, kind, sequence))""",
    # a client order id names one order of one account, for ever
    f"""CREATE UNIQUE INDEX IF NOT EXISTS uq_cati_xint_client_order ON {INTENT_TABLE}(client_order_id)
        WHERE client_order_id IS NOT NULL""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_xint_account_state ON {INTENT_TABLE}(account_id, state)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_xint_account_symbol ON {INTENT_TABLE}(account_id, symbol, state)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_xint_decision ON {INTENT_TABLE}(decision_id)",
    f"""CREATE TABLE IF NOT EXISTS {EVENT_TABLE} (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        account_id TEXT NOT NULL,
        symbol TEXT,
        intent_id TEXT,
        replacement_id TEXT,
        event_type TEXT NOT NULL CHECK (event_type IN {_in(EVENT_TYPES)}),
        method TEXT,
        client_order_id TEXT,
        from_state TEXT,
        to_state TEXT,
        outcome TEXT,
        recorded_at INTEGER NOT NULL,
        detail TEXT NOT NULL DEFAULT '{{}}')""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_xev_account ON {EVENT_TABLE}(account_id, seq)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_xev_intent ON {EVENT_TABLE}(intent_id, seq)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_xev_replacement ON {EVENT_TABLE}(replacement_id, seq)",
    *append_only_triggers(EVENT_TABLE, "cati_xev"),
    # the history of an intent is written by the database: no code path changes a state without a record
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_xint_event_insert AFTER INSERT ON {INTENT_TABLE}
        BEGIN INSERT INTO {EVENT_TABLE} (account_id, symbol, intent_id, event_type, client_order_id, from_state,
                                         to_state, outcome, recorded_at)
              VALUES (NEW.account_id, NEW.symbol, NEW.intent_id, 'STATE', NEW.client_order_id, NULL, NEW.state,
                      NEW.reason, NEW.updated_at);
        END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_xint_event_state AFTER UPDATE OF state ON {INTENT_TABLE}
        WHEN OLD.state <> NEW.state
        BEGIN INSERT INTO {EVENT_TABLE} (account_id, symbol, intent_id, event_type, client_order_id, from_state,
                                         to_state, outcome, recorded_at)
              VALUES (NEW.account_id, NEW.symbol, NEW.intent_id, 'STATE', NEW.client_order_id, OLD.state, NEW.state,
                      NEW.reason, NEW.updated_at);
        END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_xint_final BEFORE UPDATE OF state ON {INTENT_TABLE}
        WHEN OLD.state IN {_in(INTENT_FINAL_STATES)} AND NEW.state <> OLD.state
        BEGIN SELECT RAISE(ABORT, '{INTENT_TABLE}: a final intent does not change state'); END""",
    # once an order may have left, the row can never again say that nothing was sent
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_xint_sent BEFORE UPDATE OF state ON {INTENT_TABLE}
        WHEN OLD.state IN {_in(INTENT_SENT_STATES)} AND NEW.state IN ('PLANNED', 'REFUSED', 'EXPIRED')
        BEGIN SELECT RAISE(ABORT, '{INTENT_TABLE}: an order that may have left is never unsent'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_xint_unknown BEFORE UPDATE OF state ON {INTENT_TABLE}
        WHEN OLD.state = 'UNKNOWN' AND NEW.state <> 'UNKNOWN' AND trim(COALESCE(NEW.evidence, '')) = ''
        BEGIN SELECT RAISE(ABORT, '{INTENT_TABLE}: an unknown outcome is resolved only with evidence'); END""",
    # an order is identified before it is sent
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_xint_identified BEFORE UPDATE OF state ON {INTENT_TABLE}
        WHEN NEW.state IN {_in(INTENT_SENT_STATES)} AND NEW.role <> 'STOP_REPLACE'
             AND trim(COALESCE(NEW.client_order_id, '')) = ''
        BEGIN SELECT RAISE(ABORT, '{INTENT_TABLE}: an order is identified before it is sent'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_xint_identity BEFORE UPDATE OF intent_id, account_id, decision_id, symbol,
            kind, direction, sequence, role, batch_hash ON {INTENT_TABLE}
        BEGIN SELECT RAISE(ABORT, '{INTENT_TABLE}: the identity of an intent is immutable'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_xint_client_order BEFORE UPDATE OF client_order_id ON {INTENT_TABLE}
        WHEN OLD.client_order_id IS NOT NULL AND NEW.client_order_id IS NOT OLD.client_order_id
        BEGIN SELECT RAISE(ABORT, '{INTENT_TABLE}: a client order id is never reassigned'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_xint_no_open_delete BEFORE DELETE ON {INTENT_TABLE}
        WHEN OLD.state IN {_in(INTENT_OPEN_STATES)}
        BEGIN SELECT RAISE(ABORT, '{INTENT_TABLE}: an open intent is never deleted'); END""",
    # ------------------------------------------------------------------------------------- protection state (3.9)
    f"""CREATE TABLE IF NOT EXISTS {PROTECTION_TABLE} (
        account_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,
        intended_stop_price REAL NOT NULL,
        status TEXT NOT NULL CHECK (status IN {_in(PROTECTION_STATUSES)}),
        client_order_id TEXT,
        venue_order_id TEXT,
        resting_stop_price REAL,
        form TEXT,
        covered_quantity REAL,
        position_quantity REAL,
        generation INTEGER NOT NULL DEFAULT 0,
        attempts INTEGER NOT NULL DEFAULT 0,
        requested_at INTEGER,
        retry_not_before INTEGER,
        superseded TEXT NOT NULL DEFAULT '[]',
        reason TEXT NOT NULL DEFAULT '',
        origin TEXT NOT NULL DEFAULT '',
        created_at INTEGER NOT NULL,
        confirmed_at INTEGER,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY(account_id, symbol))""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_prot_status ON {PROTECTION_TABLE}(account_id, status)",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_prot_no_unsettled_delete BEFORE DELETE ON {PROTECTION_TABLE}
        WHEN OLD.status IN {_in(PROTECTION_UNSETTLED)}
        BEGIN SELECT RAISE(ABORT, '{PROTECTION_TABLE}: unsettled protection is never deleted'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_prot_identity BEFORE UPDATE OF account_id, symbol ON {PROTECTION_TABLE}
        BEGIN SELECT RAISE(ABORT, '{PROTECTION_TABLE}: the identity of a protection record is immutable'); END""",
    f"""CREATE TABLE IF NOT EXISTS {REPLACEMENT_TABLE} (
        replacement_id TEXT PRIMARY KEY,
        account_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,
        request_key TEXT NOT NULL,
        reason TEXT NOT NULL,
        requested_stop_price REAL NOT NULL,
        protocol TEXT,
        step TEXT NOT NULL CHECK (step IN {_in(REPLACEMENT_STEPS)}),
        state TEXT NOT NULL CHECK (state IN {_in(REPLACEMENT_STATES)}),
        outcome TEXT NOT NULL DEFAULT '',
        old_client_order_id TEXT,
        old_venue_order_id TEXT,
        old_stop_price REAL,
        new_client_order_id TEXT,
        new_venue_order_id TEXT,
        new_stop_price REAL,
        new_quantity REAL,
        attempt INTEGER NOT NULL DEFAULT 0,
        retire_pending INTEGER NOT NULL DEFAULT 0,
        superseded TEXT NOT NULL DEFAULT '[]',
        retry_not_before INTEGER,
        detail TEXT NOT NULL DEFAULT '',
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        UNIQUE(account_id, symbol, request_key))""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_prep_open ON {REPLACEMENT_TABLE}(account_id, state, created_at)",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_prep_final BEFORE UPDATE ON {REPLACEMENT_TABLE}
        WHEN OLD.state IN {_in(REPLACEMENT_FINAL_STATES)}
        BEGIN SELECT RAISE(ABORT, '{REPLACEMENT_TABLE}: a finished replacement never changes'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_prep_identity BEFORE UPDATE OF replacement_id, account_id, symbol,
            direction, request_key, requested_stop_price ON {REPLACEMENT_TABLE}
        BEGIN SELECT RAISE(ABORT, '{REPLACEMENT_TABLE}: the identity of a replacement is immutable'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_prep_no_open_delete BEFORE DELETE ON {REPLACEMENT_TABLE}
        WHEN OLD.state = '{REPLACEMENT_OPEN}'
        BEGIN SELECT RAISE(ABORT, '{REPLACEMENT_TABLE}: an open replacement is never deleted'); END""",
    # ------------------------------------------------------------------------- entry blocks and venue back-off
    f"""CREATE TABLE IF NOT EXISTS {BLOCK_TABLE} (
        block_id TEXT PRIMARY KEY,
        account_id TEXT NOT NULL,
        scope TEXT NOT NULL CHECK (scope IN {_in(BLOCK_SCOPES)}),
        symbol TEXT NOT NULL DEFAULT '',
        reason TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('{BLOCK_ACTIVE}', '{BLOCK_CLEARED}')),
        operator_only INTEGER NOT NULL DEFAULT 0,
        detail TEXT NOT NULL DEFAULT '',
        created_at INTEGER NOT NULL,
        cleared_at INTEGER,
        cleared_by TEXT,
        evidence TEXT)""",
    f"""CREATE UNIQUE INDEX IF NOT EXISTS uq_cati_xblk_active ON {BLOCK_TABLE}(account_id, symbol, reason)
        WHERE state = '{BLOCK_ACTIVE}'""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_xblk_account ON {BLOCK_TABLE}(account_id, state)",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_xblk_clear BEFORE UPDATE OF state ON {BLOCK_TABLE}
        WHEN OLD.state = '{BLOCK_ACTIVE}' AND NEW.state = '{BLOCK_CLEARED}'
             AND (trim(COALESCE(NEW.evidence, '')) = '' OR trim(COALESCE(NEW.cleared_by, '')) = '')
        BEGIN SELECT RAISE(ABORT, '{BLOCK_TABLE}: a block is cleared only with evidence and an actor'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_xblk_final BEFORE UPDATE ON {BLOCK_TABLE}
        WHEN OLD.state = '{BLOCK_CLEARED}'
        BEGIN SELECT RAISE(ABORT, '{BLOCK_TABLE}: a cleared block never changes'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_xblk_identity BEFORE UPDATE OF block_id, account_id, scope, symbol,
            reason, operator_only ON {BLOCK_TABLE}
        BEGIN SELECT RAISE(ABORT, '{BLOCK_TABLE}: the identity of a block is immutable'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_xblk_no_active_delete BEFORE DELETE ON {BLOCK_TABLE}
        WHEN OLD.state = '{BLOCK_ACTIVE}'
        BEGIN SELECT RAISE(ABORT, '{BLOCK_TABLE}: an active block is never deleted'); END""",
    f"""CREATE TABLE IF NOT EXISTS {BACKOFF_TABLE} (
        account_id TEXT NOT NULL,
        venue TEXT NOT NULL,
        environment TEXT NOT NULL,
        retry_not_before INTEGER NOT NULL,
        code TEXT NOT NULL,
        method TEXT NOT NULL DEFAULT '',
        recorded_at INTEGER NOT NULL,
        PRIMARY KEY(account_id, venue, environment))""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_xbko_forward BEFORE UPDATE OF retry_not_before ON {BACKOFF_TABLE}
        WHEN NEW.retry_not_before < OLD.retry_not_before
        BEGIN SELECT RAISE(ABORT, '{BACKOFF_TABLE}: a back-off ends by time only'); END""",
)


def upgrade(conn: Any) -> None:
    """Additive column changes of this module's tables. None yet."""


__all__ = ["DDL", "upgrade", "INTENT_TABLE", "EVENT_TABLE", "PROTECTION_TABLE", "REPLACEMENT_TABLE", "BLOCK_TABLE",
           "BACKOFF_TABLE", "INTENT_UNSENT_STATES", "INTENT_SENT_STATES", "INTENT_OPEN_STATES", "INTENT_FINAL_STATES",
           "INTENT_STATES", "PROTECTION_STATUSES", "PROTECTION_UNSETTLED", "REPLACEMENT_STEPS", "REPLACEMENT_OPEN",
           "REPLACEMENT_DONE", "REPLACEMENT_REFUSED", "REPLACEMENT_UNWOUND", "REPLACEMENT_FINAL_STATES",
           "REPLACEMENT_STATES", "BLOCK_ACTIVE", "BLOCK_CLEARED", "BLOCK_SCOPES", "EVENT_TYPES"]
