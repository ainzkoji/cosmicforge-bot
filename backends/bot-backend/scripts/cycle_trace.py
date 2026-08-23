"""
cycle_trace.py — per-cycle signal trace viewer

Usage:
    python scripts/cycle_trace.py               # latest completed cycle, once
    python scripts/cycle_trace.py --watch        # refresh every 10s
    python scripts/cycle_trace.py --watch --interval 5
    python scripts/cycle_trace.py --cycle <cycle_id>
    python scripts/cycle_trace.py --bot <bot_instance_id>
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


# ── DB path (same resolution as DB class) ──────────────────────────────────
def _resolve_db() -> str:
    try:
        import dotenv
        dotenv.load_dotenv(Path(__file__).parent.parent / ".env")
    except Exception:
        pass

    url = os.environ.get("DATABASE_URL", "")
    if url.startswith("sqlite:///"):
        rel = url[len("sqlite:///"):]
        if not os.path.isabs(rel):
            base = Path(__file__).resolve().parent.parent.parent  # backends/
            return str((base / "bot-backend" / rel).resolve())
        return rel

    # fallback: walk up looking for the shared DB
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"
        if candidate.exists():
            return str(candidate)
    raise FileNotFoundError("Cannot locate cosmicforge.db")


# ── Outcome classification ──────────────────────────────────────────────────
def classify_outcome(row: dict) -> str:
    if row.get("event_blocked"):
        return "BLOCKED_BY_EVENT"
    if row.get("kill_switch_state") not in (None, "NORMAL", "None"):
        return "BLOCKED_BY_RISK"
    if row.get("execution_status") in ("ALREADY_OPEN",):
        return "ALREADY_OPEN"
    if row.get("position_opened"):
        return "EXECUTED"
    if row.get("execution_status") == "FILLED":
        return "EXECUTED"
    gate_reason = (row.get("gate_reason") or "").upper()
    regime = (row.get("regime_state") or "").upper()
    if "LOW_VOL" in gate_reason or "CHOP" in gate_reason or "LOW_VOL" in regime:
        return "HOLD_LOW_VOL_CHOP"
    ml_action = row.get("ml_action") or ""
    if ml_action and ml_action.upper() in ("BLOCK", "SKIP", "REJECT"):
        return "BLOCKED_BY_ML"
    rejection = (row.get("rejection_reason") or "").upper()
    if "ALREADY_OPEN" in rejection or "EXISTING" in rejection:
        return "ALREADY_OPEN"
    if "STALE" in rejection or "STALE" in gate_reason:
        return "STALE_DATA"
    signal = (row.get("signal") or "").lower()
    if signal in ("hold", ""):
        buy  = row.get("buy_score") or 0.0
        sell = row.get("sell_score") or 0.0
        thr  = row.get("threshold") or 0.0
        if buy < thr and sell < thr:
            return "HOLD_BELOW_THRESHOLD"
        return "HOLD_OTHER"
    return "OTHER"


# ── ANSI colours (suppressed on Windows if not supported) ──────────────────
USE_COLOR = sys.platform != "win32" or os.environ.get("TERM") == "xterm-256color"

def _c(code: str, text: str) -> str:
    if not USE_COLOR:
        return text
    codes = {
        "green":  "\033[92m",
        "red":    "\033[91m",
        "yellow": "\033[93m",
        "cyan":   "\033[96m",
        "dim":    "\033[2m",
        "bold":   "\033[1m",
        "reset":  "\033[0m",
    }
    return f"{codes.get(code, '')}{text}{codes['reset']}"


# ── Query helpers ───────────────────────────────────────────────────────────
def get_latest_cycle(conn: sqlite3.Connection, bot_id: str | None) -> str | None:
    q = "SELECT cycle_id FROM decision_traces"
    params: list = []
    if bot_id:
        q += " WHERE bot_instance_id = ?"
        params.append(bot_id)
    q += " ORDER BY rowid DESC LIMIT 1"
    row = conn.execute(q, params).fetchone()
    return row[0] if row else None


def get_cycle_rows(conn: sqlite3.Connection, cycle_id: str) -> list[dict]:
    cur = conn.execute(
        """
        SELECT trace_id, symbol, regime_state, regime_confidence,
               buy_score, sell_score, confidence, threshold,
               signal, gate_reason, gate_details_json,
               active_strategy_count, strategy_signals_json,
               ml_score, ml_action,
               event_blocked, event_block_reason,
               kill_switch_state, rejection_reason,
               execution_status, position_opened,
               intended_action, final_state_change,
               ts, equity, open_positions_count,
               adx, atr_pct, htf_opposed, reason_codes
        FROM decision_traces
        WHERE cycle_id = ?
        ORDER BY symbol
        """,
        (cycle_id,),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# ── Formatting ──────────────────────────────────────────────────────────────
def fmt_score(val, threshold=None) -> str:
    if val is None:
        return "  ---  "
    s = f"{val:6.3f}"
    if threshold is not None and val >= threshold:
        return _c("green", s)
    return s


def fmt_gap(gap: float) -> str:
    s = f"{gap:+6.3f}"
    if gap >= 0:
        return _c("green", s)
    if gap >= -0.05:
        return _c("yellow", s)
    return _c("dim", s)


def fmt_outcome(outcome: str) -> str:
    colors = {
        "EXECUTED":             "green",
        "ALREADY_OPEN":         "cyan",
        "HOLD_BELOW_THRESHOLD": "dim",
        "HOLD_LOW_VOL_CHOP":    "dim",
        "HOLD_OTHER":           "dim",
        "BLOCKED_BY_ML":        "yellow",
        "BLOCKED_BY_RISK":      "red",
        "BLOCKED_BY_EVENT":     "yellow",
        "STALE_DATA":           "yellow",
        "OTHER":                "dim",
    }
    return _c(colors.get(outcome, "dim"), f"{outcome:<26}")


def fmt_signal(sig: str) -> str:
    s = (sig or "hold").upper()
    if s == "BUY":
        return _c("green", f"{'BUY':4}")
    if s == "SELL":
        return _c("red", f"{'SELL':4}")
    return _c("dim", f"{'HOLD':4}")


def regime_short(r: str) -> str:
    map_ = {
        "STRONG_TREND":         "STRONG_TREND",
        "WEAK_TREND":           "WEAK_TREND  ",
        "RANGE":                "RANGE       ",
        "LOW_VOLATILITY_CHOP":  "LOW_VOL_CHOP",
    }
    return map_.get(r or "", (r or "?")[:12].ljust(12))


def parse_strategy_votes(row: dict) -> str:
    raw = row.get("strategy_signals_json") or "[]"
    try:
        items = json.loads(raw)
    except Exception:
        return ""
    parts = []
    for it in items:
        name = it.get("strategy", "?")
        sig  = (it.get("signal") or "?").upper()
        conf = it.get("confidence")
        cf   = f"({conf:.2f})" if conf is not None else ""
        parts.append(f"{name}:{sig}{cf}")
    return ", ".join(parts)


def parse_gate_details(row: dict) -> str:
    raw = row.get("gate_details_json") or "{}"
    try:
        d = json.loads(raw)
    except Exception:
        return ""
    parts = []
    if "dynamic_threshold" in d:
        parts.append(f"dyn_thr={d['dynamic_threshold']:.3f}")
    if "confidence_gap" in d:
        parts.append(f"gap={d['confidence_gap']:+.3f}")
    return " ".join(parts)


# ── Main print function ──────────────────────────────────────────────────────
def print_cycle(rows: list[dict], cycle_id: str, bot_id: str | None = None) -> None:
    if not rows:
        print("  (no rows for this cycle)")
        return

    ts_str  = (rows[0].get("ts") or "")[:19].replace("T", " ")
    equity  = rows[0].get("equity") or 0.0
    n_open  = rows[0].get("open_positions_count") or 0

    hdr = (
        f"\n{'='*120}\n"
        f"  CYCLE TRACE  |  cycle_id={cycle_id[:8]}…  |  {ts_str} UTC"
        f"  |  symbols={len(rows)}"
        f"  |  equity=${equity:,.2f}"
        f"  |  open_positions={n_open}"
    )
    if bot_id:
        hdr += f"  |  bot={bot_id}"
    print(_c("bold", hdr))
    print("=" * 120)

    # column header
    print(
        f"  {'SYMBOL':<12} {'REGIME':<13} {'BUY':>6} {'SELL':>6} {'CONF':>6}"
        f" {'THRESH':>6} {'GAP':>7}  {'SIG':4}  {'ACT':3}  {'OUTCOME':<26}  REASON"
    )
    print("-" * 120)

    outcome_counts: dict[str, int] = {}
    for row in rows:
        symbol  = (row.get("symbol") or "?")[:12].ljust(12)
        regime  = regime_short(row.get("regime_state") or "")
        buy     = row.get("buy_score") or 0.0
        sell    = row.get("sell_score") or 0.0
        conf    = row.get("confidence") or 0.0
        thr     = row.get("threshold") or 0.0
        gap     = conf - thr
        sig     = row.get("signal") or "hold"
        n_strat = row.get("active_strategy_count") or 0
        outcome = classify_outcome(row)
        outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1

        reason = row.get("reason_codes") or row.get("gate_reason") or ""
        if row.get("event_block_reason"):
            reason = f"EVENT: {row['event_block_reason']}"
        elif row.get("rejection_reason") and row["rejection_reason"] not in ("None", reason):
            reason = row["rejection_reason"]

        print(
            f"  {symbol} {_c('dim', regime)} "
            f"{fmt_score(buy, thr)} {fmt_score(sell, thr)} {fmt_score(conf, thr)}"
            f" {thr:6.3f} {fmt_gap(gap)}  {fmt_signal(sig)}  {n_strat:3d}"
            f"  {fmt_outcome(outcome)}  {_c('dim', str(reason)[:50])}"
        )

    # summary bar
    print("=" * 120)
    total = len(rows)
    executed = outcome_counts.get("EXECUTED", 0)
    hold_thr = outcome_counts.get("HOLD_BELOW_THRESHOLD", 0)
    hold_chop = outcome_counts.get("HOLD_LOW_VOL_CHOP", 0)
    already = outcome_counts.get("ALREADY_OPEN", 0)
    blocked = sum(v for k, v in outcome_counts.items() if k.startswith("BLOCKED"))
    other = total - executed - hold_thr - hold_chop - already - blocked

    parts = [
        _c("green",  f"EXECUTED={executed}"),
        _c("cyan",   f"ALREADY_OPEN={already}"),
        _c("dim",    f"HOLD_BELOW_THRESHOLD={hold_thr}"),
        _c("dim",    f"HOLD_LOW_VOL_CHOP={hold_chop}"),
        _c("yellow", f"BLOCKED={blocked}"),
    ]
    if other > 0:
        parts.append(_c("dim", f"OTHER={other}"))
    print(f"  SUMMARY: {' | '.join(parts)}")
    print()

    # detail section for any non-HOLD rows
    interesting = [r for r in rows if classify_outcome(r) not in (
        "HOLD_BELOW_THRESHOLD", "HOLD_LOW_VOL_CHOP"
    )]
    if interesting:
        print(_c("bold", "  DETAIL (non-HOLD rows):"))
        for row in interesting:
            outcome = classify_outcome(row)
            sym = row.get("symbol", "?")
            votes = parse_strategy_votes(row)
            gate_d = parse_gate_details(row)
            ml_a = row.get("ml_action") or "none"
            ml_s = row.get("ml_score")
            ml_str = f"{ml_a}" + (f" score={ml_s:.3f}" if ml_s is not None else "")
            print(
                f"    {sym:<12} outcome={outcome:<26}"
                f" | ml={ml_str:<20}"
                f" | votes={votes}"
            )
            if gate_d:
                print(f"    {'':12} gate_details: {gate_d}")
            if row.get("execution_status") and row["execution_status"] not in ("None", None):
                print(f"    {'':12} execution_status={row['execution_status']}")
        print()


# ── Entry point ──────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="CosmicForge per-cycle signal trace viewer")
    parser.add_argument("--watch",    action="store_true", help="Auto-refresh (default interval 10s)")
    parser.add_argument("--interval", type=int, default=10, help="Refresh interval in seconds (--watch mode)")
    parser.add_argument("--cycle",    default=None, help="Show a specific cycle_id")
    parser.add_argument("--bot",      default=None, help="Filter by bot_instance_id")
    args = parser.parse_args()

    db_path = _resolve_db()
    print(f"  DB: {db_path}")

    last_cycle: str | None = None

    while True:
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row

            cycle_id = args.cycle or get_latest_cycle(conn, args.bot)
            if not cycle_id:
                print("  No cycles found yet.")
            elif cycle_id != last_cycle or args.cycle:
                rows = get_cycle_rows(conn, cycle_id)
                print_cycle(rows, cycle_id, bot_id=args.bot)
                last_cycle = cycle_id
            else:
                ts_now = datetime.now(timezone.utc).strftime("%H:%M:%S")
                print(f"\r  [{ts_now}] Waiting for new cycle (last={last_cycle[:8]}…)…", end="", flush=True)

            conn.close()

        except KeyboardInterrupt:
            print("\n  Stopped.")
            break
        except Exception as exc:
            print(f"  Error: {exc}")

        if not args.watch:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
