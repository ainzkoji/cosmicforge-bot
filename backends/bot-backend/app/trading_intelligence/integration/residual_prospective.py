"""Frozen Mandate 003 residual evidence, independent of every order/model path.

Recent public candles warm up nuisance inputs only. Decisions are enrolled at
future hour boundaries; a decision is committed before its outcome is fetched.
Overlapping top1 counterfactuals have outcomes but are never portfolio trades.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
from pathlib import Path
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import requests

H, Q, WINDOW = 3600000, 900000, 672
FAMILY = "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1"
REGISTRY_HASH = "f3cac76976590fb59ae9f83252092dc484cf89e59b3880945288bc92458ce36f"
SOURCE = "BINANCE_PUBLIC_RESIDUAL_PROSPECTIVE_V1"
ROOT = Path(__file__).resolve().parents[5]
logger = logging.getLogger(__name__)
_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="cati-residual-observe")
_lock = threading.Lock()
_future = None
_last = 0.


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def frozen_definition():
    raw = (ROOT / "docs/research/cati_alpha_root_cause/next_edge_registry.json").read_bytes()
    if hashlib.sha256(raw).hexdigest() != REGISTRY_HASH:
        raise ValueError("FROZEN_RESIDUAL_REGISTRY_CHANGED")
    registry = json.loads(raw)
    family = registry["families"][0]
    if family["family"] != FAMILY:
        raise ValueError("FROZEN_RESIDUAL_FAMILY_CHANGED")
    return family, registry["cost_policy"]["rates"]


def hourly_window(native, decision):
    """Exactly 673 complete hours, including decision close; no filling gaps."""
    expected = np.arange(decision + 1 - (WINDOW + 1) * H, decision + 1, Q)
    rows = np.asarray(native, dtype=float)
    if rows.size == 0:
        raise ValueError("INSUFFICIENT_CLOSED_INPUTS")
    rows = rows[(rows[:, 0] >= expected[0]) & (rows[:, 0] + Q - 1 <= decision)]
    if len(rows) != len(expected) or not np.array_equal(rows[:, 0], expected):
        raise ValueError("INPUT_GAP_OR_DUPLICATE")
    if not np.isfinite(rows).all() or (rows[:, 1:5] <= 0).any() or (rows[:, 5] < 0).any():
        raise ValueError("INVALID_OHLCV")
    if (rows[:, 2] < rows[:, [1, 3, 4]].max(axis=1)).any() or (rows[:, 3] > rows[:, [1, 2, 4]].min(axis=1)).any():
        raise ValueError("INVALID_OHLC_GEOMETRY")
    groups = rows.reshape(WINDOW + 1, 4, 6)
    return np.column_stack((groups[:, 0, 1], groups[:, :, 2].max(axis=1),
                            groups[:, :, 3].min(axis=1), groups[:, -1, 4], groups[:, :, 5].sum(axis=1)))


def decision_snapshot(native, decision, universe):
    """Same OLS/residual/ATR/swing/top1 definitions as the frozen evaluator."""
    complete, missing = {}, {}
    for symbol in ("BTCUSDT", "ETHUSDT", *universe):
        try:
            complete[symbol] = hourly_window(native.get(symbol, []), decision)
        except ValueError as exc:
            missing[symbol] = str(exc)
    snapshot = {"eligible_universe": [], "registered_universe": list(universe),
                "missing_inputs": missing, "candidate": None, "reason": "INSUFFICIENT_COMPLETE_ASSETS"}
    if "BTCUSDT" not in complete or "ETHUSDT" not in complete:
        snapshot["reason"] = "FACTOR_INPUTS_UNAVAILABLE"
        return snapshot
    factors = np.column_stack((np.ones(WINDOW),
        np.log(complete["BTCUSDT"][1:, 3] / complete["BTCUSDT"][:-1, 3]),
        np.log(complete["ETHUSDT"][1:, 3] / complete["ETHUSDT"][:-1, 3])))
    if np.linalg.matrix_rank(factors) != 3:
        snapshot["reason"] = "FACTOR_FIT_NOT_FULL_RANK"
        return snapshot
    ranked = []
    for symbol in universe:
        if symbol not in complete:
            continue
        bars = complete[symbol]
        returns = np.log(bars[1:, 3] / bars[:-1, 3])
        beta = np.linalg.lstsq(factors, returns, rcond=None)[0]
        residual = returns - factors @ beta
        sd = float(np.std(residual, ddof=1))
        if not math.isfinite(sd) or sd <= 0:
            missing[symbol] = "INVALID_RESIDUAL_VOLATILITY"
            continue
        score = float(residual[-24:].sum() / (sd * math.sqrt(24)))
        snapshot["eligible_universe"].append(symbol)
        if abs(score) < 2:
            continue
        sign = 1 if score > 0 else -1
        previous = bars[:-1, 3]
        tr = np.maximum(bars[1:, 1] - bars[1:, 2],
                        np.maximum(abs(bars[1:, 1] - previous), abs(bars[1:, 2] - previous)))
        atr = float(tr[-14:].mean())
        swing = float(bars[-25:-1, 2].min() if sign > 0 else bars[-25:-1, 1].max())
        close = float(bars[-1, 3])
        risk = max(sign * (close - swing) + .25 * atr, 2 * atr, .003 * close)
        ranked.append({"symbol": symbol, "side": "LONG" if sign > 0 else "SHORT", "sign": sign,
                       "score": score, "entry_reference": close, "risk": float(risk),
                       "stop": close - sign * risk, "target": close + sign * 2.5 * risk,
                       "atr14": atr, "prior24h_swing": swing,
                       "beta_btc": float(sign * beta[1]), "beta_eth": float(sign * beta[2])})
    if len(snapshot["eligible_universe"]) < 30:
        return snapshot
    if not ranked:
        snapshot["reason"] = "NO_SCORE_AT_LEAST_2"
        return snapshot
    ranked.sort(key=lambda row: (-abs(row["score"]), row["symbol"]))
    snapshot["candidate"] = ranked[0]
    c = ranked[0]
    snapshot["reason"] = "SELECTED_TOP1" if min(c["stop"], c["target"]) > 0 else "INVALID_GEOMETRY"
    # Never fall through to a lower-ranked symbol after invalid geometry.
    return snapshot


def cost_parts(entry, exit_price, risk, rates):
    turnover = 1 + exit_price / entry
    fraction = risk / entry
    parts = {k: turnover * rates[k] / fraction for k in ("fee", "half_spread", "slippage")}
    parts["funding_buffer"] = rates["funding_per_8h"] * math.ceil(48 / 8) / fraction
    return parts


class Tracker:
    def __init__(self, db, now_ms=None):
        self.db = db
        self.family, self.rates = frozen_definition()
        now = int(time.time() * 1000) if now_ms is None else now_ms
        with db.connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS cati_residual_tracker (
                    registry_hash TEXT PRIMARY KEY, activated_at INTEGER NOT NULL,
                    first_decision_time INTEGER NOT NULL, last_decision_time INTEGER,
                    heartbeat_at INTEGER, status TEXT NOT NULL, detail_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS cati_residual_inputs (
                    symbol TEXT NOT NULL, open_time INTEGER NOT NULL,
                    open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL,
                    close REAL NOT NULL, volume REAL NOT NULL, received_at INTEGER NOT NULL,
                    source TEXT NOT NULL, PRIMARY KEY(symbol,open_time));
                CREATE TABLE IF NOT EXISTS cati_residual_decisions (
                    decision_id TEXT PRIMARY KEY, registry_hash TEXT NOT NULL,
                    decision_time INTEGER NOT NULL, recorded_at INTEGER NOT NULL,
                    eligible_universe_json TEXT NOT NULL, selected_symbol TEXT,
                    side TEXT, score REAL, entry_reference REAL, stop REAL, target REAL,
                    risk REAL, modeled_costs_json TEXT NOT NULL, risk_state_json TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL, portfolio_selected INTEGER NOT NULL,
                    lifecycle TEXT NOT NULL, entry_time INTEGER, entry_price REAL,
                    last_bar_open INTEGER, outcome TEXT, outcome_time INTEGER,
                    gross_R REAL, cost_R REAL, net_R REAL, outcome_json TEXT,
                    UNIQUE(registry_hash,decision_time));
                CREATE UNIQUE INDEX IF NOT EXISTS cati_residual_one_portfolio_position
                    ON cati_residual_decisions(registry_hash)
                    WHERE portfolio_selected=1 AND lifecycle IN ('PENDING_ENTRY','OPEN');
            """)
            conn.execute("INSERT OR IGNORE INTO cati_residual_tracker VALUES (?,?,?,NULL,?,'WARMING_UP',?)",
                (REGISTRY_HASH, now, (now // H + 1) * H - 1, now,
                 encoded({"source": SOURCE, "mode": "OBSERVE", "order_authority": "BLOCKED"})))

    def state(self):
        with self.db.connect() as conn:
            return dict(conn.execute("SELECT * FROM cati_residual_tracker WHERE registry_hash=?", (REGISTRY_HASH,)).fetchone())

    def heartbeat(self, now, status, detail):
        with self.db.connect() as conn:
            conn.execute("UPDATE cati_residual_tracker SET heartbeat_at=?,status=?,detail_json=? WHERE registry_hash=?",
                         (now, status, encoded(detail), REGISTRY_HASH))

    def commit_decision(self, decision, snapshot, now):
        state = self.state()
        if decision < state["first_decision_time"] or decision >= now or decision % H != H - 1:
            raise ValueError("PROSPECTIVE_HOUR_BOUNDARY_REQUIRED")
        c = snapshot["candidate"] or {}
        reason = snapshot["reason"]
        if now >= decision + Q:
            reason = "MISSED_PROSPECTIVE_BOUNDARY"
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            active = conn.execute("SELECT decision_id FROM cati_residual_decisions WHERE registry_hash=? AND portfolio_selected=1 AND lifecycle IN ('PENDING_ENTRY','OPEN')", (REGISTRY_HASH,)).fetchone()
            observe = reason == "SELECTED_TOP1"
            selected = observe and active is None
            risk_state = {"mode": "OBSERVE", "entry_authority": "BLOCKED", "orders_sent": False,
                "source": SOURCE, "reference_venue_only": True, "reason": reason,
                "portfolio_selected": selected, "overlap_rejected": observe and active is not None,
                "active_portfolio_decision_id": active[0] if active else None,
                "decision_recording_latency_ms": now - decision,
                "entry_reference_kind": "DECISION_CLOSE_NOT_A_FILL", "nuisance_beta_only": True}
            estimate = cost_parts(c["entry_reference"], c["entry_reference"], c["risk"], self.rates) if c and c["risk"] > 0 else None
            costs = {"rates": self.rates, "horizon_hours": 48, "parts_estimated_at_decision": estimate,
                     "actual_next_open_and_exit_notional_used_at_outcome": True}
            identity = hashlib.sha256((REGISTRY_HASH + ":" + str(decision)).encode()).hexdigest()
            changed = conn.execute("""INSERT OR IGNORE INTO cati_residual_decisions
                (decision_id,registry_hash,decision_time,recorded_at,eligible_universe_json,
                 selected_symbol,side,score,entry_reference,stop,target,risk,modeled_costs_json,
                 risk_state_json,snapshot_json,portfolio_selected,lifecycle,entry_time)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (identity, REGISTRY_HASH, decision, now, encoded(snapshot["eligible_universe"]),
                 c.get("symbol"), c.get("side"), c.get("score"), c.get("entry_reference"),
                 c.get("stop"), c.get("target"), c.get("risk"), encoded(costs), encoded(risk_state),
                 encoded(snapshot), int(selected), "PENDING_ENTRY" if observe else "SKIPPED",
                 decision + 1 if observe else None)).rowcount
            conn.execute("UPDATE cati_residual_tracker SET last_decision_time=MAX(COALESCE(last_decision_time,0),?) WHERE registry_hash=?", (decision, REGISTRY_HASH))
        return identity, bool(changed)

    def pending(self):
        with self.db.connect() as conn:
            return [dict(row) for row in conn.execute("SELECT * FROM cati_residual_decisions WHERE registry_hash=? AND lifecycle IN ('PENDING_ENTRY','OPEN') ORDER BY decision_time", (REGISTRY_HASH,))]

    def record_entry(self, identity, open_time, price, received_at):
        if not math.isfinite(price) or price <= 0:
            raise ValueError("INVALID_ENTRY_REFERENCE")
        with self.db.connect() as conn:
            row = conn.execute("SELECT * FROM cati_residual_decisions WHERE decision_id=?", (identity,)).fetchone()
            if not row or row["lifecycle"] != "PENDING_ENTRY":
                return
            if open_time != row["entry_time"] or received_at < row["recorded_at"]:
                raise ValueError("ENTRY_TIME_OR_PROVENANCE_MISMATCH")
            if row["side"] == "LONG":
                executable = row["stop"] < price < row["target"]
            else:
                executable = row["target"] < price < row["stop"]
            detail = {"entry_received_at": received_at, "entry_reference_kind": "NEXT_NATIVE15M_OPEN_REFERENCE_NOT_A_FILL"}
            conn.execute("UPDATE cati_residual_decisions SET lifecycle=?,entry_price=?,outcome_json=? WHERE decision_id=?",
                ("OPEN" if executable else "NON_EXECUTABLE_GAP", price, encoded(detail), identity))

    def observe_bar(self, identity, bar, received_at):
        """Consume a later closed bar exactly once, in order; no full-path replay."""
        op_time, op, hi, lo, close, volume = bar[:6]
        if op_time % Q or not all(math.isfinite(x) for x in bar[:6]) or min(op, hi, lo, close) <= 0 or volume < 0 or hi < max(op, lo, close) or lo > min(op, hi, close):
            raise ValueError("INVALID_OUTCOME_BAR")
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM cati_residual_decisions WHERE decision_id=?", (identity,)).fetchone()
            if not row or row["lifecycle"] != "OPEN":
                return
            expected = row["entry_time"] if row["last_bar_open"] is None else row["last_bar_open"] + Q
            if op_time < expected:
                return
            if op_time != expected or op_time + Q - 1 >= received_at or op_time + Q - 1 <= row["recorded_at"]:
                raise ValueError("OUTCOME_GAP_FUTURE_OR_PRE_ENROLLMENT")
            sign = 1 if row["side"] == "LONG" else -1
            stop = lo <= row["stop"] if sign > 0 else hi >= row["stop"]
            target = hi >= row["target"] if sign > 0 else lo <= row["target"]
            outcome = None
            exit_price = close
            if stop:
                outcome = "STOP"
                exit_price = min(op, row["stop"]) if sign > 0 else max(op, row["stop"])
            elif target:
                outcome, exit_price = "TARGET", row["target"]
            elif op_time + Q == row["entry_time"] + 48 * H:
                outcome = "TIMEOUT"
            if outcome is None:
                conn.execute("UPDATE cati_residual_decisions SET last_bar_open=? WHERE decision_id=?", (op_time, identity))
                return
            parts = cost_parts(row["entry_price"], exit_price, row["risk"], self.rates)
            cost = sum(parts.values())
            gross = sign * (exit_price - row["entry_price"]) / row["risk"]
            detail = json.loads(row["outcome_json"] or "{}")
            detail.update({"source": SOURCE, "reference_venue_only": True, "not_executed": True,
                "exit_reference": exit_price, "cost_parts": parts, "outcome_received_at": received_at,
                "ambiguous_stop_priority": bool(stop and target), "terminal_bar": list(bar[:6]),
                "net_1_5x_cost_R": gross - 1.5 * cost, "net_2x_cost_R": gross - 2 * cost})
            conn.execute("""UPDATE cati_residual_decisions SET lifecycle='CLOSED',last_bar_open=?,outcome=?,
                outcome_time=?,gross_R=?,cost_R=?,net_R=?,outcome_json=? WHERE decision_id=?""",
                (op_time, outcome, int(op_time + Q - 1), gross, cost, gross - cost, encoded(detail), identity))


class PublicCandles:
    """GET-only public reference client; bounded pace, no credentials/executor."""
    def __init__(self):
        self.session = requests.Session()
        self.last_request = 0.

    def __call__(self, symbol, start, end, limit=1000):
        time.sleep(max(0., .5 - (time.monotonic() - self.last_request)))
        self.last_request = time.monotonic()
        response = self.session.get("https://fapi.binance.com/fapi/v1/klines", timeout=15,
            params={"symbol": symbol, "interval": "15m", "startTime": int(start), "endTime": int(end), "limit": limit})
        if response.status_code in (418, 429):
            raise RateLimited("PUBLIC_REFERENCE_RATE_LIMIT")
        response.raise_for_status()
        rows = response.json()
        if not isinstance(rows, list):
            raise ValueError("PUBLIC_CANDLES_NOT_A_LIST")
        return rows


class RateLimited(RuntimeError):
    pass


def refresh_inputs(tracker, symbol, decision, fetch):
    start = decision + 1 - (WINDOW + 1) * H
    with tracker.db.connect() as conn:
        cached = conn.execute("SELECT open_time FROM cati_residual_inputs WHERE symbol=? AND open_time>=? AND open_time<=? ORDER BY open_time", (symbol, start, decision)).fetchall()
    stamps = {row[0] for row in cached}
    expected = range(start, decision + 1, Q)
    cursor = next((t for t in expected if t not in stamps), decision + 1)
    while cursor <= decision:
        rows = fetch(symbol, cursor, decision)
        if not rows:
            break
        received = int(time.time() * 1000)
        valid = []
        for raw in rows:
            stamp = int(raw[0])
            values = [float(x) for x in raw[1:6]]
            if stamp < cursor or stamp > decision or stamp % Q or int(raw[6]) != stamp + Q - 1 or stamp + Q - 1 > decision or stamp + Q - 1 >= received:
                raise ValueError("INPUT_TIMESTAMP_DEFECT")
            if not all(math.isfinite(x) for x in values) or min(values[:4]) <= 0 or values[4] < 0 or values[1] < max(values[0], values[2], values[3]) or values[2] > min(values[0], values[1], values[3]):
                raise ValueError("INPUT_VALUE_DEFECT")
            valid.append((symbol, stamp, *values, received, SOURCE))
        if len({x[1] for x in valid}) != len(valid) or any(a[1] >= b[1] for a,b in zip(valid,valid[1:])):
            raise ValueError("INPUT_DUPLICATE_OR_UNORDERED")
        with tracker.db.connect() as conn:
            conn.executemany("INSERT OR IGNORE INTO cati_residual_inputs VALUES (?,?,?,?,?,?,?,?,?)", valid)
        cursor = valid[-1][1] + Q
        if len(rows) < 1000:
            break
    with tracker.db.connect() as conn:
        return [list(row) for row in conn.execute("SELECT open_time,open,high,low,close,volume FROM cati_residual_inputs WHERE symbol=? AND open_time>=? AND open_time<=? ORDER BY open_time", (symbol, start, decision))]


def owner_current(db):
    from app.ops.runtime_ownership import current_owner, holder_is_alive, lease_is_stale
    owner = current_owner(db, db.path)
    # Written by THIS process: a lease left by an earlier process that happened
    # to have the same PID (a reboot) is not ours.
    return bool(owner and owner["pid"] == os.getpid() and not lease_is_stale(owner["heartbeat_at"])
                and holder_is_alive(owner["pid"], owner["started_at"]))


def settle_pending(tracker, fetch, now_ms):
    errors = {}
    for row in tracker.pending():
        if not owner_current(tracker.db):
            break
        try:
            if row["lifecycle"] == "PENDING_ENTRY":
                entry_rows = fetch(row["selected_symbol"], row["entry_time"], row["entry_time"] + Q - 1, limit=1)
                if not entry_rows or int(entry_rows[0][0]) != row["entry_time"]:
                    raise ValueError("NEXT_OPEN_UNAVAILABLE")
                # Only the opening reference is read, never this partial bar's H/L/C.
                tracker.record_entry(row["decision_id"], row["entry_time"], float(entry_rows[0][1]), int(time.time() * 1000))
            start = row["entry_time"] if row["last_bar_open"] is None else row["last_bar_open"] + Q
            end = min(now_ms // Q * Q - 1, row["entry_time"] + 48 * H - 1)
            if start > end:
                continue
            for raw in fetch(row["selected_symbol"], start, end):
                if int(raw[6]) != int(raw[0]) + Q - 1 or int(raw[6]) > end:
                    raise ValueError("OUTCOME_CLOSE_TIMESTAMP_DEFECT")
                tracker.observe_bar(row["decision_id"], [int(raw[0]), *map(float, raw[1:6])], int(time.time() * 1000))
        except RateLimited:
            raise
        except Exception as exc:
            errors[row["decision_id"]] = type(exc).__name__ + ":" + str(exc)[:160]
    return errors


def collect(db):
    if not owner_current(db):
        return
    tracker = Tracker(db)
    fetch = PublicCandles()
    now = int(time.time() * 1000)
    try:
        errors = settle_pending(tracker, fetch, now)
        state = tracker.state()
        latest = now // H * H - 1
        next_decision = (state["last_decision_time"] + H) if state["last_decision_time"] is not None else state["first_decision_time"]
        # Missed boundaries are explicit skips, never retrospectively evaluated.
        while next_decision < latest:
            tracker.commit_decision(next_decision, {"candidate": None, "eligible_universe": [],
                "registered_universe": tracker.family["universe"], "reason": "MISSED_PROSPECTIVE_BOUNDARY"}, now)
            next_decision += H
        due = next_decision == latest
        # Once inputs are warm, do not refetch 136 unchanged series each minute.
        with db.connect() as conn:
            last_input = conn.execute("SELECT MAX(open_time) FROM cati_residual_inputs").fetchone()[0]
        if not due and last_input is not None and last_input >= latest - Q + 1:
            tracker.heartbeat(now, "COLLECTING", {"next_decision_time": next_decision, "pending": len(tracker.pending()), "outcome_errors": errors})
            return
        native, input_errors = {}, {}
        for symbol in ("BTCUSDT", "ETHUSDT", *tracker.family["universe"]):
            if not owner_current(db):
                return
            try:
                native[symbol] = refresh_inputs(tracker, symbol, latest, fetch)
            except RateLimited:
                raise
            except Exception as exc:
                input_errors[symbol] = type(exc).__name__ + ":" + str(exc)[:160]
        snapshot = decision_snapshot(native, latest, tracker.family["universe"])
        snapshot["collection_errors"] = input_errors
        snapshot["input_population_sha256"] = hashlib.sha256(encoded(native).encode()).hexdigest()
        recorded = int(time.time() * 1000)
        if due and owner_current(db):
            tracker.commit_decision(latest, snapshot, recorded)
            # Commit above precedes every request for entry and subsequent bars.
            errors.update(settle_pending(tracker, fetch, recorded))
        with db.connect() as conn:
            conn.execute("DELETE FROM cati_residual_inputs WHERE open_time<?", (latest - (WINDOW + 8) * H,))
        tracker.heartbeat(recorded, "COLLECTING", {"eligible_count": len(snapshot["eligible_universe"]),
            "next_decision_time": next_decision + H if due else next_decision,
            "reason": snapshot["reason"], "input_errors": input_errors, "outcome_errors": errors,
            "source": SOURCE, "no_orders": True})
        logger.info("[CATI_RESIDUAL_PROSPECTIVE] eligible=%s due=%s decision=%s reason=%s no_orders=True", len(snapshot["eligible_universe"]), due, latest, snapshot["reason"])
    except RateLimited as exc:
        tracker.heartbeat(int(time.time() * 1000), "RATE_LIMIT_BACKOFF", {"reason": str(exc), "retry_after_seconds": 300})
        raise


def schedule(runner):
    global _future, _last
    if os.environ.get("COSMICFORGE_TEST_MODE") == "1" or not owner_current(runner.db):
        return
    with _lock:
        if _future is not None and not _future.done():
            return
        if time.monotonic() - _last < 60:
            return
        _last = time.monotonic()
        _future = _pool.submit(collect, runner.db)
        _future.add_done_callback(_completed)


def _completed(future):
    global _last
    try:
        future.result()
    except RateLimited:
        _last = time.monotonic() + 240
        logger.warning("[CATI_RESIDUAL_PROSPECTIVE] public reference rate limit; five-minute backoff")
    except Exception:
        logger.exception("[CATI_RESIDUAL_PROSPECTIVE] collection failed; runtime observation continues")
