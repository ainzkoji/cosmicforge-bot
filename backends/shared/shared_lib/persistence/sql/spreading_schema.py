"""Order-spreading integration tables (Section H, Step 5.5, gate EXT-LIQ-01).

Reserved migration range 60-69 (``platform_schema.MIGRATION_MODULES``). Every migration here is additive (new
tables and indexes) and reversible by dropping what it created.

``platform_spread_decisions``  one row per evaluation of the platform order budget by the execution boundary
                               for one ENTRY attempt: either the slot it RESERVED (and, later, how that slot was
                               settled -- FILLED with the actual filled notional, RELEASED or EXPIRED) or the
                               explicit refusal code. It is the audit link between a platform slot
                               (``platform_order_slots``) and the account's own execution attempt and client
                               order id, which stay the boundary's / executor's.
``platform_spread_observations`` calibration evidence (D4). Per entry attempt (and per shadow measurement where a
                               shadow book supplies depth): the depth the decision was taken on (digest, age,
                               spread, band notional within the worst price), the intended size, and what the
                               venue actually did (fill price, filled quantity, partial fill, realized slippage).
                               ``impact_estimate_bps`` is a mechanical walk of the VISIBLE book for the intended
                               quantity, labelled ``impact_model`` and stored with ``impact_calibrated = 0``: it is
                               not an impact model, and the schema refuses any row that claims otherwise. Nothing
                               here calibrates a policy; a calibrated policy still needs an owner approval
                               reference (``platform_symbol_budgets`` CHECK).

``platform_market_entry_policy_events`` (61, master plan L-08) the governed single-account market-entry policy
                               ledger: append-only (triggers refuse UPDATE and DELETE on both dialects), hash-chained
                               (``prev_hash`` -> ``hash``), one ``POLICY_RECORDED`` and at most one ``POLICY_REVOKED``
                               per ``policy_id``. The schema itself refuses a record without an approval reference,
                               a record that claims calibration (``calibrated = 0`` only) and any scope other than
                               ``SINGLE_ACCOUNT``. Written only by ``scale.market_entry_policy`` through the operator
                               interface; no code path writes a record of its own accord.

Money, prices, quantities and basis points are exact decimals (``{MONEY}``), written with ``decimal_in``.
"""
from __future__ import annotations

from typing import List

from .launch_schema import _drop_with_functions, _with_append_only
from .migrations import Migration
from .platform_schema import render

DECISION_OUTCOMES = ("RESERVED", "REFUSED")
SETTLED_STATES = ("FILLED", "RELEASED", "EXPIRED")
OBSERVATION_SOURCES = ("ENTRY_ATTEMPT", "SHADOW")
MARKET_POLICY_EVENT_TYPES = ("POLICY_RECORDED", "POLICY_REVOKED")
MARKET_POLICY_SCOPES = ("SINGLE_ACCOUNT",)
MARKET_POLICY_ENVIRONMENTS = ("DEMO", "LIVE")


def _in(*values: str) -> str:
    return "(" + ",".join(f"'{v}'" for v in values) + ")"


V60 = [
    "CREATE TABLE platform_spread_decisions (decision_id TEXT PRIMARY KEY, venue TEXT NOT NULL, symbol TEXT NOT NULL, "
    "side TEXT NOT NULL CHECK (side IN ('BUY','SELL')), signal_id TEXT NOT NULL, account_id TEXT NOT NULL, "
    "tenant_id TEXT, environment TEXT, execution_attempt_id TEXT, trade_plan_id TEXT, client_order_id TEXT, "
    f"outcome TEXT NOT NULL CHECK (outcome IN {_in(*DECISION_OUTCOMES)}), code TEXT, budget_id TEXT, slot_id TEXT, "
    "requested_notional {MONEY}, granted_notional {MONEY}, calibrated INTEGER NOT NULL CHECK (calibrated IN (0,1)), "
    "policy_version TEXT NOT NULL, order_style TEXT, depth_hash TEXT, created_at {BIGINT} NOT NULL, "
    f"settled_status TEXT CHECK (settled_status IS NULL OR settled_status IN {_in(*SETTLED_STATES)}), "
    "settled_at {BIGINT}, settled_notional {MONEY}, settle_reason TEXT, "
    "CHECK (outcome = 'REFUSED' OR (slot_id IS NOT NULL AND budget_id IS NOT NULL)), "
    "CHECK (outcome = 'RESERVED' OR code IS NOT NULL))",
    "CREATE INDEX ix_platform_spread_decisions_open ON platform_spread_decisions (account_id, outcome, settled_at)",
    "CREATE INDEX ix_platform_spread_decisions_attempt ON platform_spread_decisions (execution_attempt_id)",
    "CREATE INDEX ix_platform_spread_decisions_code ON platform_spread_decisions (outcome, code)",
    "CREATE TABLE platform_spread_observations (observation_id TEXT PRIMARY KEY, "
    f"source TEXT NOT NULL CHECK (source IN {_in(*OBSERVATION_SOURCES)}), venue TEXT NOT NULL, symbol TEXT NOT NULL, "
    "side TEXT NOT NULL CHECK (side IN ('BUY','SELL')), account_id TEXT, tenant_id TEXT, reference_id TEXT NOT NULL, "
    "budget_id TEXT, slot_id TEXT, intended_quantity {MONEY}, intended_notional {MONEY}, depth_hash TEXT, "
    "depth_observed_at {BIGINT}, depth_age_ms {BIGINT}, depth_levels INTEGER, best_bid {MONEY}, best_ask {MONEY}, "
    "spread_bps {MONEY}, band_notional {MONEY}, worst_price {MONEY}, reference_price {MONEY}, fill_price {MONEY}, "
    "filled_quantity {MONEY}, partial_fill INTEGER CHECK (partial_fill IS NULL OR partial_fill IN (0,1)), "
    "realized_slippage_bps {MONEY}, impact_estimate_bps {MONEY}, impact_model TEXT NOT NULL, "
    "impact_calibrated INTEGER NOT NULL DEFAULT 0 CHECK (impact_calibrated = 0), entry_status TEXT, "
    "recorded_at {BIGINT} NOT NULL, UNIQUE (source, reference_id))",
    "CREATE INDEX ix_platform_spread_observations_symbol ON platform_spread_observations (venue, symbol, side, recorded_at)",
]

V61 = [
    "CREATE TABLE platform_market_entry_policy_events (seq {ID}, event_id TEXT NOT NULL UNIQUE, "
    f"event_type TEXT NOT NULL CHECK (event_type IN {_in(*MARKET_POLICY_EVENT_TYPES)}), policy_id TEXT NOT NULL, "
    "venue TEXT NOT NULL, symbol TEXT NOT NULL, "
    f"environment TEXT NOT NULL CHECK (environment IN {_in(*MARKET_POLICY_ENVIRONMENTS)}), account_id TEXT, "
    f"scope TEXT NOT NULL CHECK (scope IN {_in(*MARKET_POLICY_SCOPES)}), "
    "calibrated INTEGER NOT NULL DEFAULT 0 CHECK (calibrated = 0), approval_ref TEXT, reason TEXT NOT NULL, "
    "actor_ref TEXT NOT NULL, terms {JSON} NOT NULL, recorded_at {BIGINT} NOT NULL, prev_hash TEXT NOT NULL, "
    "hash TEXT NOT NULL UNIQUE, UNIQUE (policy_id, event_type), "
    "CHECK (event_type <> 'POLICY_RECORDED' OR (approval_ref IS NOT NULL AND LENGTH(TRIM(approval_ref)) >= 3)), "
    "CHECK (LENGTH(TRIM(reason)) >= 3), CHECK (LENGTH(TRIM(actor_ref)) > 0))",
    "CREATE INDEX ix_platform_market_entry_policy_scope ON platform_market_entry_policy_events "
    "(venue, symbol, environment, seq)",
]

MIGRATIONS: List[Migration] = [
    Migration(60, "order_spreading_boundary_integration", render(V60),
              down=("DROP TABLE IF EXISTS platform_spread_observations",
                    "DROP TABLE IF EXISTS platform_spread_decisions")),
    Migration(61, "single_account_market_entry_policy",
              _with_append_only(V61, ["platform_market_entry_policy_events"]),
              down=_drop_with_functions("platform_market_entry_policy_events")),
]
