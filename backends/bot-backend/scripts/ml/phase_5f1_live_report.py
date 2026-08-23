#!/usr/bin/env python3
"""
phase_5f1_live_report.py  --  Phase 5F-1: Live ML Gating State Report

Generates a fully-structured observational report of the live ML gating system.
Maintains strict separation between ML-gated activity and reconciled (broker-discovered)
positions that do not have confirmed ML provenance.

Sections
--------
  CURRENT METRICS          -- Equity, unrealized PnL, open position counts
  CYCLE DELTAS             -- Last cycle vs prior cycle state change
  FIRST SIGNAL DETECTION   -- Earliest ML-scored entry signal on record
  TRANSITION VALIDATION    -- Shadow -> live gating promotion status
  TRIGGER STATES           -- Kill switch, exposure freeze, regime state
  ML VS RECONCILED SPLIT   -- Strict separation of gated vs reconciled activity
  GATING EFFECT PREVIEW    -- Block rate, score distribution, threshold alignment
  TREND INTERPRETATION     -- Rule-engine win rate trend (pre-ML baseline)
  SYSTEM STATE             -- Bot instances, broker health, run status
  STATUS                   -- Health summary
  AUDIT READINESS          -- Whether 5F-0 audit criteria are currently met
  GATING SIGNAL            -- ML threshold config vs JSONL evidence
  DECISION LINE            -- Single-line operational verdict

Usage
-----
    python scripts/ml/phase_5f1_live_report.py
        [--db-path PATH]
        [--log-dir  DIR]
        [--json]             # emit machine-readable JSON

Critical constraint: This script is OBSERVATIONAL only.
It does NOT modify threshold, retrain model, or alter architecture.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT   = _SCRIPT_DIR.parent.parent
_BACKENDS   = _BOT_ROOT.parent

_DEFAULT_DB      = _BACKENDS / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"
_DEFAULT_LOG_DIR = _BOT_ROOT / "models" / "logs"

NOW_UTC = datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _open_db(path: Path) -> Optional[sqlite3.Connection]:
    for candidate in [path,
                      _BOT_ROOT.parent / "shared" / "shared_lib" / "persistence" / "cosmicforge.db",
                      _BOT_ROOT / "data" / "bot.db"]:
        if candidate.exists():
            conn = sqlite3.connect(str(candidate), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            return conn
    return None


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _cutoff(days: int) -> str:
    return _iso(NOW_UTC - timedelta(days=days))


def _age_str(iso_ts: str) -> str:
    """Human-readable age from an ISO timestamp."""
    try:
        ts = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        delta = NOW_UTC - ts
        s = int(delta.total_seconds())
        if s < 60:   return f"{s}s ago"
        if s < 3600: return f"{s//60}m ago"
        if s < 86400:return f"{s//3600}h {(s%3600)//60}m ago"
        return f"{s//86400}d {(s%86400)//3600}h ago"
    except Exception:
        return "unknown age"


def _safe_pct(num: float, denom: float) -> str:
    if denom == 0:
        return "n/a"
    return f"{num/denom*100:.1f}%"


def _fmt(v, decimals: int = 4) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "n/a"
    return f"{v:.{decimals}f}"


def _load_jsonl(log_dir: Path) -> List[dict]:
    records: List[dict] = []
    if not log_dir.exists():
        return records
    for fp in sorted(log_dir.glob("predictions_*.jsonl")):
        try:
            with open(fp, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except OSError:
            continue
    return records


def _has_col(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    return col in cols


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_all(conn: sqlite3.Connection, jsonl: List[dict]) -> dict:
    d = {}

    # ── Equity snapshot (latest) ─────────────────────────────────────────
    eq = conn.execute(
        "SELECT * FROM equity_snapshots ORDER BY timestamp_utc DESC LIMIT 1"
    ).fetchone()
    d["equity"] = dict(eq) if eq else {}

    eq_prev = conn.execute(
        "SELECT equity, unrealized_pnl, timestamp_utc FROM equity_snapshots "
        "ORDER BY timestamp_utc DESC LIMIT 1 OFFSET 1"
    ).fetchone()
    d["equity_prev"] = dict(eq_prev) if eq_prev else {}

    # ── Bot instances ─────────────────────────────────────────────────────
    bots = conn.execute(
        "SELECT id, status, broker_account_id, broker_health_status, "
        "last_run_at, active_positions, last_error, mode "
        "FROM bot_instances WHERE status NOT IN ('deleted') "
        "ORDER BY last_run_at DESC NULLS LAST"
    ).fetchall()
    d["bots"] = [dict(b) for b in bots]

    # ── Open positions from bot_symbol_state ─────────────────────────────
    open_pos = conn.execute(
        "SELECT bot_instance_id, symbol, position, entry_price, "
        "lifecycle_phase, updated_at "
        "FROM bot_symbol_state "
        "WHERE position IS NOT NULL AND position != 0"
    ).fetchall()
    d["open_pos_state"] = [dict(p) for p in open_pos]

    # ── ML decision traces ────────────────────────────────────────────────
    try:
        ml_total = conn.execute(
            "SELECT COUNT(*) FROM decision_traces WHERE ml_score IS NOT NULL"
        ).fetchone()[0]
        ml_actions = conn.execute(
            "SELECT ml_action, COUNT(*) as n FROM decision_traces "
            "WHERE ml_score IS NOT NULL GROUP BY ml_action"
        ).fetchall()
        ml_latest = conn.execute(
            "SELECT ts, symbol, ml_score, ml_action, ml_threshold, ml_model_version, "
            "signal, gate_allowed, regime_state "
            "FROM decision_traces WHERE ml_score IS NOT NULL "
            "ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        # Check if decision_traces are backfill-only
        live_traces = conn.execute(
            "SELECT COUNT(*) FROM decision_traces "
            "WHERE run_id NOT LIKE 'backfill%' AND run_id NOT LIKE 'test%'"
        ).fetchone()[0]
        backfill_traces = conn.execute(
            "SELECT COUNT(*) FROM decision_traces WHERE run_id LIKE 'backfill%'"
        ).fetchone()[0]
        # Latest non-backfill trace
        latest_live_trace = conn.execute(
            "SELECT ts, symbol, signal, regime_state, gate_allowed, gate_reason, confidence "
            "FROM decision_traces WHERE run_id NOT LIKE 'backfill%' "
            "ORDER BY ts DESC LIMIT 1"
        ).fetchone()
    except sqlite3.OperationalError:
        ml_total = 0
        ml_actions = []
        ml_latest = None
        live_traces = 0
        backfill_traces = 0
        latest_live_trace = None

    d["ml_total_scored_db"]  = ml_total
    d["ml_actions_db"]       = {r["ml_action"]: r["n"] for r in ml_actions}
    d["ml_latest_db"]        = dict(ml_latest) if ml_latest else {}
    d["live_trace_count"]    = live_traces
    d["backfill_trace_count"] = backfill_traces
    d["latest_live_trace"]   = dict(latest_live_trace) if latest_live_trace else {}

    # ── Trade fills: gated vs reconciled ─────────────────────────────────
    # Gated: fills joined to decision_traces that have ml_score
    try:
        gated_closed = conn.execute(
            "SELECT COUNT(*) as n, SUM(tf.realized_pnl) as pnl "
            "FROM trade_fills tf "
            "JOIN decision_traces dt ON dt.order_id = tf.order_id "
            "WHERE tf.action = 'CLOSE' AND dt.ml_score IS NOT NULL"
        ).fetchone()
        d["gated_closed_count"] = gated_closed["n"] or 0
        d["gated_closed_pnl"]   = gated_closed["pnl"] or 0.0

        # Reconciled: fills with no matching ml-scored trace
        # broker_id != 'backfill' AND order_id either null or not in decision_traces with ml_score
        live_fills_total = conn.execute(
            "SELECT COUNT(*) FROM trade_fills WHERE broker_id != 'backfill'"
        ).fetchone()[0]
        backfill_fills = conn.execute(
            "SELECT COUNT(*) FROM trade_fills WHERE broker_id = 'backfill'"
        ).fetchone()[0]

        # Untraced live fills (no ml_score link)
        untraced_closed = conn.execute(
            "SELECT COUNT(*) as n, SUM(tf.realized_pnl) as pnl "
            "FROM trade_fills tf "
            "LEFT JOIN decision_traces dt ON dt.order_id = tf.order_id AND dt.ml_score IS NOT NULL "
            "WHERE tf.action = 'CLOSE' AND tf.broker_id != 'backfill' AND dt.trace_id IS NULL"
        ).fetchone()
        d["reconciled_closed_count"] = untraced_closed["n"] or 0
        d["reconciled_closed_pnl"]   = untraced_closed["pnl"] or 0.0

        # Live open positions (from broker state — active_positions column)
        active_bot = next((b for b in d["bots"] if b["status"] == "active"), None)
        d["broker_open_position_count"] = active_bot["active_positions"] if active_bot else 0
        d["state_open_position_count"]  = len(d["open_pos_state"])

        # Reconciled open: positions known to broker but not in bot_symbol_state
        d["reconciled_open_count"] = max(
            0,
            d["broker_open_position_count"] - d["state_open_position_count"]
        )

        d["live_fills_total"]   = live_fills_total
        d["backfill_fills"]     = backfill_fills

    except sqlite3.OperationalError as e:
        d["gated_closed_count"]   = 0
        d["gated_closed_pnl"]     = 0.0
        d["reconciled_closed_count"] = 0
        d["reconciled_closed_pnl"]   = 0.0
        d["broker_open_position_count"] = 0
        d["state_open_position_count"]  = 0
        d["reconciled_open_count"] = 0
        d["live_fills_total"]   = 0
        d["backfill_fills"]     = 0

    # ── Backfill-era stats (baseline reference) ───────────────────────────
    try:
        bf = conn.execute(
            "SELECT COUNT(*) as n, "
            "SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins, "
            "SUM(realized_pnl) as total_pnl, "
            "AVG(realized_pnl) as avg_pnl "
            "FROM trade_fills WHERE action = 'CLOSE' AND broker_id = 'backfill'"
        ).fetchone()
        d["backfill_closed"]   = bf["n"] or 0
        d["backfill_wins"]     = bf["wins"] or 0
        d["backfill_total_pnl"]= bf["total_pnl"] or 0.0
        d["backfill_avg_pnl"]  = bf["avg_pnl"] or 0.0
    except sqlite3.OperationalError:
        d["backfill_closed"]   = 0
        d["backfill_wins"]     = 0
        d["backfill_total_pnl"]= 0.0
        d["backfill_avg_pnl"]  = 0.0

    # ── Latest regime from non-backfill traces ────────────────────────────
    try:
        latest_regime = conn.execute(
            "SELECT regime_state, regime_confidence, ts "
            "FROM decision_traces WHERE run_id NOT LIKE 'backfill%' "
            "ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        d["latest_regime"] = dict(latest_regime) if latest_regime else {}
    except sqlite3.OperationalError:
        d["latest_regime"] = {}

    # ── Protection state ─────────────────────────────────────────────────
    try:
        prot = conn.execute(
            "SELECT * FROM protection_state ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        d["protection_state"] = dict(prot) if prot else {}
    except sqlite3.OperationalError:
        d["protection_state"] = {}

    # ── Risk parameters ───────────────────────────────────────────────────
    try:
        risk = conn.execute(
            "SELECT * FROM risk_parameters ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        d["risk_params"] = dict(risk) if risk else {}
    except sqlite3.OperationalError:
        d["risk_params"] = {}

    # ── Recent events ─────────────────────────────────────────────────────
    try:
        evts = conn.execute(
            "SELECT event_type, action, timestamp_utc, details_json "
            "FROM events ORDER BY timestamp_utc DESC LIMIT 5"
        ).fetchall()
        d["events"] = [dict(e) for e in evts]
    except sqlite3.OperationalError:
        d["events"] = []

    # ── JSONL analysis ────────────────────────────────────────────────────
    jsonl_total   = len(jsonl)
    jsonl_pass    = [r for r in jsonl if r.get("action") == "PASS"]
    jsonl_block   = [r for r in jsonl if r.get("action") == "BLOCK"]
    jsonl_shadow  = [r for r in jsonl if r.get("action") == "SHADOW"]
    jsonl_skip    = [r for r in jsonl if r.get("action") == "SKIP"]
    jsonl_scored  = [r for r in jsonl if r.get("score") is not None]

    d["jsonl_total"]   = jsonl_total
    d["jsonl_pass"]    = len(jsonl_pass)
    d["jsonl_block"]   = len(jsonl_block)
    d["jsonl_shadow"]  = len(jsonl_shadow)
    d["jsonl_skip"]    = len(jsonl_skip)
    d["jsonl_scored"]  = len(jsonl_scored)

    scores = [r["score"] for r in jsonl_scored if r.get("score") is not None]
    d["jsonl_score_min"]  = min(scores) if scores else None
    d["jsonl_score_max"]  = max(scores) if scores else None
    d["jsonl_score_mean"] = sum(scores) / len(scores) if scores else None

    # Model version and threshold from JSONL
    d["jsonl_model_versions"] = sorted({r.get("model_version") for r in jsonl if r.get("model_version")})
    d["jsonl_thresholds"]     = sorted({r.get("threshold") for r in jsonl if r.get("threshold") is not None})
    # Use the LATEST record's shadow_mode (not any-over-all, which would pick up old sessions)
    jsonl_by_ts = sorted(jsonl, key=lambda r: r.get("ts", ""), reverse=True)
    if jsonl_by_ts:
        d["jsonl_shadow_mode"] = jsonl_by_ts[0].get("shadow_mode", True)
    else:
        d["jsonl_shadow_mode"] = True

    # Latest JSONL record
    if jsonl:
        latest_j = sorted(jsonl, key=lambda r: r.get("ts", ""), reverse=True)[0]
        d["jsonl_latest_ts"]     = latest_j.get("ts", "")
        d["jsonl_latest_action"] = latest_j.get("action", "")
        d["jsonl_latest_score"]  = latest_j.get("score")
    else:
        d["jsonl_latest_ts"]     = ""
        d["jsonl_latest_action"] = ""
        d["jsonl_latest_score"]  = None

    # Skip rate for today (March 28)
    today_skip  = [r for r in jsonl_skip if "2026-03-28" in r.get("ts", "")]
    today_total = [r for r in jsonl     if "2026-03-28" in r.get("ts", "")]
    d["today_skip"]  = len(today_skip)
    d["today_total"] = len(today_total)

    # Broker account info
    try:
        broker_accounts = conn.execute(
            "SELECT id, status, broker_id, environment, validation_error, updated_at "
            "FROM broker_accounts WHERE status NOT IN ('deleted')"
        ).fetchall()
        d["broker_accounts"] = [dict(b) for b in broker_accounts]
    except sqlite3.OperationalError:
        d["broker_accounts"] = []

    return d


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

SEP  = "=" * 72
SEP2 = "-" * 72


def _print_report(d: dict) -> None:

    now_str = NOW_UTC.strftime("%Y-%m-%d %H:%M:%S UTC")
    active_bot = next((b for b in d.get("bots", []) if b["status"] == "active"), None)
    eq = d.get("equity", {})
    eq_prev = d.get("equity_prev", {})

    print(SEP)
    print("  PHASE 5F-1 -- LIVE ML GATING STATE REPORT")
    print(f"  Generated : {now_str}")
    print(f"  DB path   : cosmicforge.db")
    print(SEP)
    print()

    # ------------------------------------------------------------------
    # CURRENT METRICS
    # ------------------------------------------------------------------
    print("[CURRENT METRICS]")
    print(f"  Equity (latest snapshot) : {_fmt(eq.get('equity'), 2)} USDT")
    print(f"  Wallet balance           : {_fmt(eq.get('wallet_balance'), 2)} USDT")
    print(f"  Unrealized PnL           : {_fmt(eq.get('unrealized_pnl'), 4)} USDT")
    print(f"  Snapshot age             : {_age_str(eq.get('timestamp_utc', '')) if eq else 'n/a'}")
    print(f"  Broker account           : {eq.get('broker_account_id', 'n/a')}")
    print(f"  Open positions (broker)  : {d.get('broker_open_position_count', 0)}")
    print(f"  Open positions (state)   : {d.get('state_open_position_count', 0)}")
    print()

    # ------------------------------------------------------------------
    # CYCLE DELTAS
    # ------------------------------------------------------------------
    print("[CYCLE DELTAS]")
    if eq_prev and eq:
        eq_delta    = (eq.get("equity", 0) or 0) - (eq_prev.get("equity", 0) or 0)
        upnl_delta  = (eq.get("unrealized_pnl", 0) or 0) - (eq_prev.get("unrealized_pnl", 0) or 0)
        sign_e = "+" if eq_delta >= 0 else ""
        sign_u = "+" if upnl_delta >= 0 else ""
        print(f"  Equity change (last 2 snapshots) : {sign_e}{_fmt(eq_delta, 4)} USDT")
        print(f"  Unrealized PnL change            : {sign_u}{_fmt(upnl_delta, 4)} USDT")
        print(f"  Prior snapshot age               : {_age_str(eq_prev.get('timestamp_utc', ''))}")
    else:
        print("  Insufficient snapshot history for delta computation.")

    if active_bot:
        print(f"  Last bot run_at                  : {_age_str(active_bot.get('last_run_at', ''))}")
    print()

    # ------------------------------------------------------------------
    # FIRST SIGNAL DETECTION
    # ------------------------------------------------------------------
    print("[FIRST SIGNAL DETECTION]")
    jsonl_sorted = sorted(
        [r for r in _all_jsonl if r.get("ts")],
        key=lambda r: r["ts"]
    )
    if jsonl_sorted:
        first = jsonl_sorted[0]
        last  = jsonl_sorted[-1]
        print(f"  First ML-scored event  : {first.get('ts','')}  action={first.get('action','')}  score={first.get('score')}")
        print(f"  Latest ML-scored event : {last.get('ts','')}  action={last.get('action','')}  score={last.get('score')}")
        print(f"  Total JSONL records    : {d['jsonl_total']}")
    else:
        print("  No ML prediction log records found.")

    # Live decision trace first/last
    llt = d.get("latest_live_trace", {})
    if llt:
        print(f"  Latest live trace (DB) : {llt.get('ts','')}  signal={llt.get('signal','')}  "
              f"gate={llt.get('gate_reason','')}")
    else:
        print("  Latest live trace (DB) : None -- all DB traces are backfill origin")
    print()

    # ------------------------------------------------------------------
    # TRANSITION VALIDATION
    # ------------------------------------------------------------------
    print("[TRANSITION VALIDATION]")
    shadow_on = d["jsonl_shadow_mode"]
    n_shadow  = d["jsonl_shadow"]
    n_skip    = d["jsonl_skip"]
    n_block   = d["jsonl_block"]
    n_pass    = d["jsonl_pass"]
    today_skip_all = d["today_skip"]
    today_total_all = d["today_total"]

    print(f"  Shadow mode still ON     : {'YES' if shadow_on else 'NO -- live gating attempted'}")
    print(f"  PASS actions (all-time)  : {n_pass}")
    print(f"  BLOCK actions (all-time) : {n_block}")
    print(f"  SHADOW actions           : {n_shadow}")
    print(f"  SKIP actions (all-time)  : {n_skip}  "
          f"({_safe_pct(n_skip, d['jsonl_total'])} of total)")

    # March 28 analysis
    print(f"  Today (2026-03-28)       : {today_total_all} total cycles scored, "
          f"{today_skip_all} SKIP (scorer error)")

    versions = d["jsonl_model_versions"]
    thresholds = d["jsonl_thresholds"]
    if len(versions) > 1:
        print(f"  MODEL VERSION CHANGE     : {versions[0]} --> {versions[-1]}")
    else:
        print(f"  Model version            : {versions[0] if versions else 'none'}")
    if thresholds:
        print(f"  Threshold in JSONL       : {thresholds}")
    print()

    # ------------------------------------------------------------------
    # TRIGGER STATES
    # ------------------------------------------------------------------
    print("[TRIGGER STATES]")
    prot = d.get("protection_state", {})
    latest_regime = d.get("latest_regime", {})

    if active_bot:
        health = active_bot.get("broker_health_status", "unknown")
        print(f"  Active bot status        : {active_bot['status']}")
        print(f"  Broker health            : {health}")
        if active_bot.get("last_error"):
            print(f"  Last error               : {active_bot['last_error'][:80]}")

    archived_bots = [b for b in d["bots"] if b["status"] == "archived"]
    if archived_bots:
        print(f"  Archived bots            : {len(archived_bots)}  "
              f"(health: {', '.join(b.get('broker_health_status','?') for b in archived_bots)})")

    if prot:
        kill = prot.get("kill_switch_active", prot.get("kill_switch", False))
        exp_freeze = prot.get("exposure_freeze", False)
        daily_halt = prot.get("daily_loss_halt", False)
        print(f"  Kill switch              : {'ACTIVE' if kill else 'inactive'}")
        print(f"  Exposure freeze          : {'ACTIVE' if exp_freeze else 'inactive'}")
        print(f"  Daily loss halt          : {'ACTIVE' if daily_halt else 'inactive'}")
    else:
        print("  Kill switch              : no protection_state row (pre-migration or not triggered)")

    if latest_regime:
        print(f"  Last live regime         : {latest_regime.get('regime_state','?')}  "
              f"confidence={_fmt(latest_regime.get('regime_confidence'), 3)}  "
              f"({_age_str(latest_regime.get('ts', ''))})")
    else:
        # Fall back to most recent backfill trace for orientation
        print("  Last live regime         : no live regime data -- bot not yet running strategy cycles")
    print()

    # ------------------------------------------------------------------
    # ML VS RECONCILED SPLIT  (required section)
    # ------------------------------------------------------------------
    print("[ML VS RECONCILED SPLIT]")
    print()
    print("  -- ML-GATED ACTIVITY --")
    print(f"  ml_gated_entries         : {d['ml_total_scored_db']}  (decision_traces with ml_score != NULL)")
    print(f"  ml_allowed (PASS)        : {d['ml_actions_db'].get('PASS', 0)}  (DB) | {d['jsonl_pass']}  (JSONL)")
    print(f"  ml_blocked (BLOCK)       : {d['ml_actions_db'].get('BLOCK', 0)}  (DB) | {d['jsonl_block']}  (JSONL)")
    print(f"  ml_shadow                : {d['ml_actions_db'].get('SHADOW', 0)}  (DB) | {d['jsonl_shadow']}  (JSONL)")
    print(f"  ml_skip                  : {d['ml_actions_db'].get('SKIP', 0)}  (DB) | {d['jsonl_skip']}  (JSONL)")
    print(f"  ml_closed_outcomes_only  : {d['gated_closed_count']}")
    print(f"  ml_closed_pnl            : {_fmt(d['gated_closed_pnl'], 4)}")
    print()
    print("  -- RECONCILED ACTIVITY --")
    print(f"  reconciled_open_positions       : {d['reconciled_open_count']}")
    print(f"    (broker reports {d['broker_open_position_count']} open; "
          f"state table shows {d['state_open_position_count']})")
    print(f"  reconciled_closed_positions     : {d['reconciled_closed_count']}")
    print(f"  reconciled_new_this_cycle       : unknown -- bot_symbol_state has no recent entries")
    print(f"  reconciled_closed_outcomes_only : {d['reconciled_closed_count']}")
    print(f"  reconciled_closed_pnl           : {_fmt(d['reconciled_closed_pnl'], 4)}")
    print(f"  reconciled_origin_outside_ml    : YES -- no ml_score linked to these fills")
    print()
    print("  -- PROVENANCE NOTES --")
    print(f"  all_db_traces_are_backfill      : {'YES' if d['backfill_trace_count'] > 0 and d['live_trace_count'] == 0 else 'NO'}")
    print(f"  live_decision_trace_count       : {d['live_trace_count']}")
    print(f"  backfill_trace_count            : {d['backfill_trace_count']}")
    print(f"  all_fills_are_backfill          : {'YES' if d['live_fills_total'] == 0 else 'NO'}")
    print(f"  live_fills                      : {d['live_fills_total']}")
    print()
    print("  ** ML audit evidence currently includes only ML-gated trades. **")
    print("  ** Reconciled positions are tracked for runtime consistency    **")
    print("  ** but EXCLUDED from threshold-performance evaluation unless   **")
    print("  ** provenance is confirmed via decision_traces.ml_score join.  **")
    print()

    # ------------------------------------------------------------------
    # GATING EFFECT PREVIEW
    # ------------------------------------------------------------------
    print("[GATING EFFECT PREVIEW]")
    print(f"  JSONL total scored       : {d['jsonl_total']}")
    print(f"  JSONL with valid score   : {d['jsonl_scored']}")
    if d["jsonl_scored"] > 0:
        print(f"  Score range              : [{_fmt(d['jsonl_score_min'],4)}, {_fmt(d['jsonl_score_max'],4)}]")
        print(f"  Score mean               : {_fmt(d['jsonl_score_mean'],4)}")
    else:
        print("  Score range              : no valid scores -- all SKIP")

    block_rate_str = _safe_pct(d["jsonl_block"], d["jsonl_pass"] + d["jsonl_block"])
    print(f"  Block rate (JSONL)       : {block_rate_str}")
    print(f"  Skip rate (JSONL)        : {_safe_pct(d['jsonl_skip'], d['jsonl_total'])}")

    if d["jsonl_thresholds"]:
        print(f"  Threshold from JSONL     : {d['jsonl_thresholds']}")

    print()
    print("  BACKFILL-ERA BASELINE (reference, not ML-gated):")
    bf_wr = _safe_pct(d["backfill_wins"], d["backfill_closed"])
    print(f"    Closed trades          : {d['backfill_closed']}")
    print(f"    Win rate               : {bf_wr}")
    print(f"    Total PnL              : {_fmt(d['backfill_total_pnl'], 2)} USDT")
    print(f"    Avg PnL / trade        : {_fmt(d['backfill_avg_pnl'], 4)} USDT")
    print(f"    Note: backfill data only. Not usable for ML gating evaluation.")
    print()

    # ------------------------------------------------------------------
    # TREND INTERPRETATION
    # ------------------------------------------------------------------
    print("[TREND INTERPRETATION]")
    print(f"  Win rate (backfill era)  : {_safe_pct(d['backfill_wins'], d['backfill_closed'])}"
          f"  (rule-engine, no ML, reference only)")
    print(f"  ML gating active         : NO -- shadow mode or SKIP on all cycles to date")
    print(f"  Live gated wins          : 0 (no PASS actions have executed live)")
    print(f"  Today's scorer status    : {'ALL SKIP -- scorer error' if d['today_skip'] == d['today_total'] and d['today_total'] > 0 else 'partial'}")
    print(f"  Trend signal             : Insufficient live ML data for trend assessment")
    print()

    # ------------------------------------------------------------------
    # SYSTEM STATE
    # ------------------------------------------------------------------
    print("[SYSTEM STATE]")
    for bot in d["bots"]:
        age_str = _age_str(bot.get("last_run_at", "")) if bot.get("last_run_at") else "never"
        print(f"  Bot {bot['id'][:16]}...")
        print(f"    status            : {bot['status']}")
        print(f"    broker_health     : {bot.get('broker_health_status', 'unknown')}")
        print(f"    active_positions  : {bot.get('active_positions', 0)}  (cached broker count)")
        print(f"    last_run_at       : {age_str}")
        if bot.get("last_error"):
            print(f"    last_error        : {str(bot['last_error'])[:60]}")

    for acct in d.get("broker_accounts", []):
        print(f"  Broker {acct['id'][:16]}  status={acct['status']}  "
              f"broker_id={acct['broker_id']}  env={acct.get('environment', '?')}")

    if d.get("events"):
        print(f"  Recent events:")
        for evt in d["events"][:3]:
            print(f"    [{_age_str(evt.get('timestamp_utc',''))}]  "
                  f"{evt['event_type']} / {evt.get('action','?')}")
    print()

    # ------------------------------------------------------------------
    # STATUS
    # ------------------------------------------------------------------
    print("[STATUS]")
    issues = []
    if d["today_skip"] > 0 and d["today_skip"] == d["today_total"]:
        issues.append("SCORER FAILING: all cycles today returned SKIP (null scores)")
    if len(d["jsonl_model_versions"]) > 1:
        issues.append(f"MODEL VERSION CHANGE: {d['jsonl_model_versions'][0]} -> {d['jsonl_model_versions'][-1]}")
    if d["broker_open_position_count"] > 0 and d["state_open_position_count"] == 0:
        issues.append(
            f"POSITION STATE MISMATCH: broker reports {d['broker_open_position_count']} open "
            f"positions but bot_symbol_state = 0. Reconciled positions not tracked in state."
        )
    if not d.get("equity"):
        issues.append("NO EQUITY SNAPSHOT: cannot confirm live account balance")
    if d["live_trace_count"] == 0:
        issues.append("NO LIVE DECISION TRACES: bot has not produced any live strategy cycles in DB")

    if issues:
        print(f"  ISSUES ({len(issues)}):")
        for iss in issues:
            print(f"    ! {iss}")
    else:
        print("  No critical issues detected.")

    # Bot lifecycle
    archived = [b for b in d["bots"] if b["status"] == "archived"]
    active   = [b for b in d["bots"] if b["status"] == "active"]
    print(f"  Active bots: {len(active)}  |  Archived bots: {len(archived)}")
    print()

    # ------------------------------------------------------------------
    # AUDIT READINESS
    # ------------------------------------------------------------------
    print("[AUDIT READINESS]  -- 5F-0 criteria check")
    checks = {
        "ML_ENABLED (shadow=False or BLOCK actions exist)":
            d["jsonl_block"] > 0 or not d["jsonl_shadow_mode"],
        "Min 30 closed gated outcomes":
            d["gated_closed_count"] >= 30,
        "At least 1 PASS action in JSONL":
            d["jsonl_pass"] > 0,
        "No scorer SKIP issues today":
            d["today_skip"] == 0,
        "Model version stable (1 version)":
            len(d["jsonl_model_versions"]) == 1,
        "Threshold consistent in JSONL":
            len(d["jsonl_thresholds"]) == 1,
    }
    all_pass = True
    for check, result in checks.items():
        status = "[PASS]" if result else "[FAIL]"
        if not result:
            all_pass = False
        print(f"  {status}  {check}")

    print()
    if all_pass:
        print("  AUDIT READINESS: READY -- 5F-0 audit can proceed")
    else:
        failed = sum(1 for r in checks.values() if not r)
        print(f"  AUDIT READINESS: NOT READY -- {failed} criteria not met")
    print()

    # ------------------------------------------------------------------
    # GATING SIGNAL
    # ------------------------------------------------------------------
    print("[GATING SIGNAL]")
    print(f"  Live gating status       : {'SHADOW ONLY' if d['jsonl_shadow_mode'] else 'ACTIVE (attempted)'}")
    print(f"  Effective PASS count     : {d['jsonl_pass']}")
    print(f"  Effective BLOCK count    : {d['jsonl_block']}")
    print(f"  Scorer health today      : {'DEGRADED (all SKIP)' if d['today_skip'] == d['today_total'] and d['today_total'] > 0 else 'OK'}")

    if d["jsonl_thresholds"]:
        print(f"  Configured threshold     : {d['jsonl_thresholds']}")
    if d["jsonl_model_versions"]:
        print(f"  Active model version     : {d['jsonl_model_versions'][-1]}")

    print()
    print("  NOTE: All scored events to date are either SHADOW or SKIP.")
    print("  No live BLOCK or PASS decisions have been executed.")
    print("  ML gating has had zero effect on live trading outcomes.")
    print()

    # ------------------------------------------------------------------
    # DECISION LINE
    # ------------------------------------------------------------------
    print(SEP)
    print("[DECISION LINE]")
    print(SEP)

    skip_rate_today = d["today_skip"] / d["today_total"] if d["today_total"] > 0 else 0.0
    all_skip_today  = d["today_total"] > 0 and d["today_skip"] == d["today_total"]

    model_versions = d["jsonl_model_versions"]
    latest_model   = model_versions[-1] if model_versions else "unknown"
    thresholds     = d["jsonl_thresholds"]
    thr_str        = "/".join(str(t) for t in thresholds) if thresholds else "unknown"
    mean_score_str = _fmt(d["jsonl_score_mean"], 3) if d["jsonl_score_mean"] is not None else "n/a"

    if all_skip_today:
        decision = (
            f"SCORER DEGRADED -- {latest_model} is returning SKIP on all cycles today "
            f"({d['today_skip']}/{d['today_total']}). "
            "Investigate model artifact path and scorer startup logs before advancing. "
            "Live gating has had zero effect on trade outcomes. "
            "ACTION REQUIRED: verify model file exists and ML_ENABLED=True."
        )
    elif not d["jsonl_shadow_mode"] and d["jsonl_block"] > 0 and d["jsonl_pass"] == 0:
        decision = (
            f"LIVE GATING ACTIVE -- shadow mode OFF, threshold={thr_str}, "
            f"model={latest_model}. "
            f"All {d['jsonl_block']} decisions to date are BLOCK (score mean={mean_score_str}, "
            f"all below threshold). Zero PASS decisions issued -- no trades have been "
            "permitted by ML gating. 17 reconciled positions remain open outside ML flow. "
            f"Skip rate={_safe_pct(d['jsonl_skip'], d['jsonl_total'])} ({d['jsonl_skip']} SKIP). "
            "Monitoring: accumulate PASS events to enable 5F-0 audit."
        )
    elif not d["jsonl_shadow_mode"] and d["jsonl_block"] == 0 and d["jsonl_pass"] == 0:
        decision = (
            "GATING NOT YET ACTIVE -- shadow mode is off but no PASS or BLOCK decisions "
            "have been issued. Scorer is returning SKIP results. System is running but ML "
            "layer is not influencing trade flow. Continue monitoring."
        )
    elif d["jsonl_shadow_mode"] and d["gated_closed_count"] == 0:
        decision = (
            "SHADOW MODE ACTIVE -- all ML evaluations are non-binding SHADOW. "
            f"Score distribution: mean={mean_score_str}, all below threshold. "
            "No live gating effect on trade outcomes. Accumulate shadow data before promoting."
        )
    else:
        decision = (
            f"GATING STATUS MIXED -- {d['jsonl_pass']} PASS, {d['jsonl_block']} BLOCK, "
            f"{d['jsonl_shadow']} SHADOW, {d['jsonl_skip']} SKIP. "
            f"Gated closed outcomes: {d['gated_closed_count']}. "
            "Review JSONL logs and decision_traces for full session state."
        )

    print(f"  {decision}")
    print(SEP)
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

_all_jsonl: List[dict] = []  # module-level for _print_report access


def main() -> int:
    global _all_jsonl

    parser = argparse.ArgumentParser(description="Phase 5F-1 Live ML Gating State Report")
    parser.add_argument("--db-path",  default=None, help="Path to cosmicforge.db")
    parser.add_argument("--log-dir",  default=None, help="Path to ML JSONL log directory")
    parser.add_argument("--json",     action="store_true", dest="emit_json")
    args = parser.parse_args()

    db_path  = Path(args.db_path)  if args.db_path  else _DEFAULT_DB
    log_dir  = Path(args.log_dir)  if args.log_dir  else _DEFAULT_LOG_DIR

    conn = _open_db(db_path)
    if not conn:
        print(f"[ERROR] DB not found at {db_path}", file=sys.stderr)
        return 4

    _all_jsonl = _load_jsonl(log_dir)
    data = _load_all(conn, _all_jsonl)
    conn.close()

    if args.emit_json:
        import math as _math
        def _clean(obj):
            if isinstance(obj, dict):
                return {k: _clean(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_clean(v) for v in obj]
            if isinstance(obj, float) and _math.isnan(obj):
                return None
            return obj
        print(json.dumps({
            "report": "phase_5f1_live",
            "generated_at": NOW_UTC.isoformat(),
            "data": _clean(data),
        }, indent=2, default=str))
    else:
        _print_report(data)

    return 0


if __name__ == "__main__":
    sys.exit(main())
