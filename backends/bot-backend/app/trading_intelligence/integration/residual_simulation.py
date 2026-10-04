"""Durable, local CATI simulation over the frozen prospective decision stream.

No broker client, credentials, training or historical result store is accepted.
Fills are explicitly next-native-open *model* fills, acknowledged at wall time;
they are never a claim of executable historical or broker prices. Native closed
15m bars retain the frozen ambiguous-bar stop priority. Live prices mark equity
and enforce emergency risk independently of that strategy exit clock.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN
import json
import logging
import math
import os
import subprocess
import sys
import time
import uuid

from app.risk.system_limits import SystemLimits
from app.trading_intelligence.governance.promotion import PromotionGovernance
from .residual_prospective import (
    FAMILY, REGISTRY_HASH, H, Q, ROOT, PublicCandles, RateLimited,
    cost_parts, encoded, frozen_definition, owner_current,
)

logger = logging.getLogger(__name__)
MODE = {"mode": "LIVE_MARKET", "trading": "ACTIVE", "execution": "SIMULATED/TEST",
        "real_money": "DISABLED", "real_broker_live_orders": "DISABLED"}


def periods(now):
    dt = datetime.fromtimestamp(now / 1000, timezone.utc)
    return dt.date().isoformat(), (dt.date() - timedelta(days=dt.weekday())).isoformat()


def identity(kind):
    return f"cati_sim_{kind}_{uuid.uuid4().hex}"


class Book:
    def __init__(self, db):
        self.db = db
        self.family, self.rates = frozen_definition()
        self.limits = SystemLimits()
        with db.connect() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS cati_sim_account (
                    id INTEGER PRIMARY KEY CHECK(id=1), document TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS cati_sim_positions (
                    id TEXT PRIMARY KEY, decision_id TEXT UNIQUE NOT NULL,
                    status TEXT NOT NULL, document TEXT NOT NULL);
                CREATE UNIQUE INDEX IF NOT EXISTS cati_sim_one_position
                    ON cati_sim_positions(status) WHERE status='OPEN';
                CREATE TABLE IF NOT EXISTS cati_sim_orders (
                    id TEXT PRIMARY KEY, position_id TEXT NOT NULL,
                    status TEXT NOT NULL, document TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS cati_sim_fills (
                    id TEXT PRIMARY KEY, order_id TEXT UNIQUE NOT NULL,
                    position_id TEXT NOT NULL, document TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS cati_sim_decisions (
                    decision_id TEXT PRIMARY KEY, processed_at INTEGER NOT NULL,
                    reason TEXT NOT NULL, document TEXT NOT NULL);
            """)

    def activate(self, owner, now, capital=10000.):
        if not owner or not math.isfinite(capital) or capital <= 0:
            raise ValueError("VALID_OWNER_AND_VIRTUAL_CAPITAL_REQUIRED")
        day, week = periods(now)
        with self.db.connect() as c:
            c.execute("INSERT OR IGNORE INTO cati_sim_account VALUES (1,?)", (encoded({
                **MODE, "owner_user_id": owner, "activated_at": now,
                "first_decision_time": (now // H + 1) * H - 1,
                "registry_hash": REGISTRY_HASH, "strategy": FAMILY,
                "initial_virtual_capital": capital, "cash": capital,
                "equity": capital, "day": day, "day_start_equity": capital,
                "week": week, "week_start_equity": capital,
                "consecutive_losses": 0, "day_entries": 0, "daily_halted": False,
                "persistent_halt": None, "heartbeat_at": None,
                "market_received_at": None, "market_symbols": 0,
                "status": "AWAITING_LIVE_MARKET", "error": None,
                "daily_hard_loss_fraction": min(.025, self.limits.max_daily_loss_pct),
                "risk_per_entry_fraction": min(.0025, self.limits.max_risk_per_trade_ceiling),
                "execution_reference": "NEXT_NATIVE15M_OPEN_MODEL_FILL",
                "leverage": 1., "broker_submission_attempts": 0,
            }),))

    @staticmethod
    def account(c):
        row = c.execute("SELECT document FROM cati_sim_account WHERE id=1").fetchone()
        return json.loads(row[0]) if row else None

    @staticmethod
    def positions(c, status=None):
        sql = "SELECT document FROM cati_sim_positions"
        args = ()
        if status:
            sql += " WHERE status=?"
            args = (status,)
        return [json.loads(r[0]) for r in c.execute(sql + " ORDER BY rowid DESC", args)]

    @staticmethod
    def save_account(c, account):
        c.execute("UPDATE cati_sim_account SET document=? WHERE id=1", (encoded(account),))

    @staticmethod
    def save_position(c, position):
        c.execute("UPDATE cati_sim_positions SET status=?,document=? WHERE id=?",
                  (position["status"], encoded(position), position["id"]))

    def roll(self, a, now):
        day, week = periods(now)
        if day != a["day"]:
            a.update(day=day, day_start_equity=a["equity"], day_entries=0, daily_halted=False)
        if week != a["week"]:
            a.update(week=week, week_start_equity=a["equity"])
            if a["persistent_halt"] == "WEEKLY_DRAWDOWN":
                a["persistent_halt"] = None

    def risk_halt(self, a):
        if a["equity"] <= a["day_start_equity"] * (1 - min(.025, self.limits.max_daily_loss_pct)):
            a["daily_halted"] = True
        if a["equity"] <= a["initial_virtual_capital"] * (1 - self.limits.emergency_drawdown_halt_pct):
            a["persistent_halt"] = "EMERGENCY_DRAWDOWN"
        elif a["equity"] <= a["week_start_equity"] * (1 - self.limits.max_weekly_drawdown_pct):
            a["persistent_halt"] = "WEEKLY_DRAWDOWN"
        elif a["consecutive_losses"] >= self.limits.max_consecutive_losses:
            a["persistent_halt"] = "CONSECUTIVE_LOSSES"
        return a["persistent_halt"] or ("DAILY_2_5_PERCENT_CAP" if a["daily_halted"] else None)

    def admit(self, a, row, price, instrument):
        """Reject conflicting geometry; never clamp it or select runner-up."""
        snapshot = json.loads(row["snapshot_json"])
        if PromotionGovernance(self.db).kill_switch_on():
            return "CATI_NEW_ENTRY_KILL_SWITCH", 0.
        candidate = snapshot.get("candidate") or {}
        if snapshot.get("reason") != "SELECTED_TOP1" or row["lifecycle"] == "SKIPPED":
            return snapshot.get("reason", "NO_SIGNAL"), 0.
        if row["selected_symbol"] not in self.family["universe"]:
            return "OUTSIDE_FROZEN_UNIVERSE", 0.
        if not all(math.isfinite(float(row[k])) for k in ("risk", "stop", "target", "entry_reference", "score")) or not math.isfinite(price) or price <= 0:
            return "INVALID_GEOMETRY", 0.
        if not min(row["stop"], row["target"]) < price < max(row["stop"], row["target"]):
            return "NON_EXECUTABLE_GAP", 0.
        stop_fraction = abs(price - row["stop"]) / price
        atr = candidate.get("atr14", 0)
        if not self.limits.min_stop_loss_pct <= stop_fraction <= self.limits.max_stop_loss_pct:
            return "HARD_STOP_DISTANCE_LIMIT", 0.
        if not atr or not self.limits.min_stop_loss_atr_multiplier <= row["risk"] / atr <= self.limits.max_stop_loss_atr_multiplier:
            return "HARD_STOP_ATR_LIMIT", 0.
        halt = self.risk_halt(a)
        if halt:
            return halt, 0.
        if a["day_entries"] >= self.limits.max_trades_per_day:
            return "DAILY_RUNAWAY_ENTRY_GUARD", 0.
        if not instrument or instrument.get("status") != "TRADING":
            return "INSTRUMENT_NOT_TRADING", 0.
        # Full frozen costs reserved even for an early exit. No cost discount.
        cost_bound = ((price + max(price, row["target"], row["stop"])) *
                      sum(self.rates[k] for k in ("fee", "half_spread", "slippage")) +
                      price * self.rates["funding_per_8h"] * 6)
        unit_loss = max(row["risk"], abs(price - row["stop"])) + cost_bound
        remaining = a["equity"] - a["day_start_equity"] * (1 - min(.025, self.limits.max_daily_loss_pct))
        risk_cash = min(a["equity"] * min(.0025, self.limits.max_risk_per_trade_ceiling), remaining)
        # 1x leverage, one position, below symbol, correlated and total ceilings;
        # preserve at least the system's 35% free-margin buffer.
        exposure = min(self.limits.max_exposure_per_symbol_pct,
                       self.limits.max_correlation_exposure_pct,
                       self.limits.max_total_exposure_mult,
                       2 - self.limits.emergency_margin_ratio_threshold)
        qty = min(risk_cash / unit_loss, a["equity"] * exposure / price,
                  float(instrument["maxQty"]))
        step = Decimal(str(instrument["stepSize"]))
        qty = float((Decimal(str(qty)) / step).to_integral_value(rounding=ROUND_DOWN) * step)
        if qty <= 0 or qty < float(instrument["minQty"]) or qty * price < float(instrument["minNotional"]):
            return "INSTRUMENT_MINIMUM_OR_RISK_BUDGET", 0.
        return "APPROVED_SIMULATED", qty

    def order(self, c, p, kind, status, now, price):
        o = {"id": identity("order"), "position_id": p["id"], "symbol": p["symbol"],
             "side": p["side"] if kind == "ENTRY" else ("SHORT" if p["side"] == "LONG" else "LONG"),
             "kind": kind, "status": status, "quantity": p["quantity"],
             "price": price, "created_at": now, "execution": "SIMULATED/TEST",
             "broker_order_id": None, "reduce_only": kind != "ENTRY"}
        c.execute("INSERT INTO cati_sim_orders VALUES (?,?,?,?)", (o["id"], p["id"], status, encoded(o)))
        return o

    def fill(self, c, o, now, reference_at, costs):
        f = {"id": identity("fill"), "order_id": o["id"], "position_id": o["position_id"],
             "symbol": o["symbol"], "side": o["side"], "quantity": o["quantity"],
             "price": o["price"], "created_at": now, "reference_at": reference_at,
             "costs": costs, "execution": "SIMULATED/TEST", "broker_fill_id": None,
             "price_kind": "PUBLIC_REFERENCE_MODEL_FILL_NOT_BROKER_EXECUTION"}
        c.execute("INSERT INTO cati_sim_fills VALUES (?,?,?,?)", (f["id"], o["id"], o["position_id"], encoded(f)))

    def consider(self, row, entry_bar, received_at, instrument):
        """Entry must belong to a NEW enrolled boundary, before its first close."""
        with self.db.connect() as c:
            c.execute("BEGIN IMMEDIATE")
            a = self.account(c)
            if not a or row["decision_time"] < a["first_decision_time"] or row["recorded_at"] < a["activated_at"]:
                return
            if c.execute("SELECT 1 FROM cati_sim_decisions WHERE decision_id=?", (row["decision_id"],)).fetchone():
                return
            self.roll(a, received_at)
            reason, qty = "MISSED_SIMULATION_ENTRY_WINDOW", 0.
            price = None
            if received_at >= row["recorded_at"] and row["recorded_at"] > row["decision_time"] and received_at < row["decision_time"] + 1 + Q:
                if row["lifecycle"] == "SKIPPED":
                    reason = json.loads(row["risk_state_json"])["reason"]
                elif entry_bar and int(entry_bar[0]) == row["decision_time"] + 1:
                    price = float(entry_bar[1])
                    reason, qty = self.admit(a, row, price, instrument)
                else:
                    reason = "NEXT_OPEN_UNAVAILABLE"
            if self.positions(c, "OPEN"):
                reason, qty = "PORTFOLIO_ONE_POSITION_OVERLAP", 0.
            detail = {"reason": reason, "source_decision": row["decision_id"],
                      "decision_time": row["decision_time"], "decision_recorded_at": row["recorded_at"],
                      "entry_reference": price, "quantity": qty, "risk_equity": a["equity"],
                      "snapshot": json.loads(row["snapshot_json"]), "modeled_costs": json.loads(row["modeled_costs_json"]),
                      "risk_state": {k: a[k] for k in ("day", "day_start_equity", "week", "week_start_equity", "daily_halted", "persistent_halt", "day_entries", "consecutive_losses", "daily_hard_loss_fraction", "risk_per_entry_fraction")}}
            c.execute("INSERT INTO cati_sim_decisions VALUES (?,?,?,?)", (row["decision_id"], received_at, reason, encoded(detail)))
            if qty:
                sign = 1 if row["side"] == "LONG" else -1
                entry_costs = {k: qty * price * self.rates[k] for k in ("fee", "half_spread", "slippage")}
                entry_costs["funding_buffer"] = qty * price * self.rates["funding_per_8h"] * 6
                p = {"id": identity("position"), "decision_id": row["decision_id"], "status": "OPEN",
                     "symbol": row["selected_symbol"], "side": row["side"], "sign": sign,
                     "score": row["score"], "quantity": qty, "entry_price": price,
                     "entry_time": row["decision_time"] + 1, "created_at": received_at,
                     "stop": row["stop"], "target": row["target"], "risk": row["risk"],
                     "timeout_at": row["decision_time"] + 1 + 48 * H, "last_bar_open": None,
                     "mark": price, "mark_received_at": received_at, "unrealized_gross_pnl": 0.,
                     "entry_costs": entry_costs, "realized_gross_pnl": 0., "realized_net_pnl": 0.,
                     "execution": "SIMULATED/TEST", "registry_hash": REGISTRY_HASH,
                     "reference_received_at": received_at}
                c.execute("INSERT INTO cati_sim_positions VALUES (?,?,?,?)", (p["id"], p["decision_id"], "OPEN", encoded(p)))
                o = self.order(c, p, "ENTRY", "FILLED", received_at, price)
                self.fill(c, o, received_at, p["entry_time"], entry_costs)
                for kind, reference in (("STOP", p["stop"]), ("TARGET", p["target"]), ("TIMEOUT", None)):
                    self.order(c, p, kind, "PENDING", received_at, reference)
                a["cash"] -= sum(entry_costs.values())
                a["equity"] = a["cash"]
                a["day_entries"] += 1
            a["last_decision"] = detail
            self.save_account(c, a)

    def close(self, c, a, p, price, outcome, now, reference_at):
        parts = cost_parts(p["entry_price"], price, p["risk"], self.rates)
        risk_cash = p["quantity"] * p["risk"]
        costs = {k: v * risk_cash for k, v in parts.items()}
        exit_costs = {k: p["quantity"] * price * self.rates[k] for k in ("fee", "half_spread", "slippage")}
        gross = p["sign"] * p["quantity"] * (price - p["entry_price"])
        a["cash"] += gross - sum(exit_costs.values())
        net = gross - sum(costs.values())
        p.update(status="CLOSED", outcome=outcome, exit_price=price, closed_at=now,
                 outcome_reference_at=reference_at, realized_gross_pnl=gross,
                 realized_net_pnl=net, costs=costs, gross_R=gross / risk_cash,
                 cost_R=sum(parts.values()), net_R=net / risk_cash,
                 unrealized_gross_pnl=0.)
        matching = None
        for r in c.execute("SELECT document FROM cati_sim_orders WHERE position_id=? AND status='PENDING'", (p["id"],)).fetchall():
            o = json.loads(r[0])
            o["status"] = "FILLED" if o["kind"] == outcome else "CANCELED"
            o["updated_at"] = now
            if o["status"] == "FILLED":
                o["price"] = price
                matching = o
            c.execute("UPDATE cati_sim_orders SET status=?,document=? WHERE id=?", (o["status"], encoded(o), o["id"]))
        if matching is None:
            matching = self.order(c, p, outcome, "FILLED", now, price)
        self.fill(c, matching, now, reference_at, exit_costs)
        a["consecutive_losses"] = a["consecutive_losses"] + 1 if net < 0 else 0
        self.save_position(c, p)

    def observe_bar(self, position_id, bar, received_at):
        op_time, op, hi, lo, close, volume = map(float, bar[:6])
        if op_time % Q or op_time + Q > received_at or not all(math.isfinite(x) for x in (op_time, op, hi, lo, close, volume)) or min(op, hi, lo, close) <= 0 or volume < 0 or hi < max(op, lo, close) or lo > min(op, hi, close):
            raise ValueError("INVALID_OR_FUTURE_SIMULATION_BAR")
        with self.db.connect() as c:
            c.execute("BEGIN IMMEDIATE")
            p = next((p for p in self.positions(c, "OPEN") if p["id"] == position_id), None)
            if not p:
                return
            expected = p["entry_time"] if p["last_bar_open"] is None else p["last_bar_open"] + Q
            if op_time < expected:
                return
            if op_time != expected or op_time + Q <= p["created_at"]:
                raise ValueError("SIMULATION_OUTCOME_GAP_OR_PRE_ACTIVATION")
            a = self.account(c)
            self.roll(a, received_at)
            stop = lo <= p["stop"] if p["sign"] > 0 else hi >= p["stop"]
            target = hi >= p["target"] if p["sign"] > 0 else lo <= p["target"]
            outcome = None
            if stop:
                outcome = "STOP"
                close = min(op, p["stop"]) if p["sign"] > 0 else max(op, p["stop"])
            elif target:
                outcome, close = "TARGET", p["target"]
            elif op_time + Q == p["timeout_at"]:
                outcome = "TIMEOUT"
            p["last_bar_open"] = int(op_time)
            if outcome:
                self.close(c, a, p, close, outcome, received_at, int(op_time + Q - 1))
                a["equity"] = a["cash"]
                self.risk_halt(a)
            else:
                self.save_position(c, p)
            self.save_account(c, a)

    def mark(self, quotes, received_at):
        with self.db.connect() as c:
            c.execute("BEGIN IMMEDIATE")
            a = self.account(c)
            if not a:
                return
            self.roll(a, received_at)
            positions = self.positions(c, "OPEN")
            for p in positions:
                quote = quotes.get(p["symbol"])
                if not quote or not math.isfinite(quote[0]) or quote[0] <= 0 or not 0 <= received_at - quote[1] <= 30000:
                    a.update(status="MARKET_DATA_STALE", error="FRESH_POSITION_PRICE_REQUIRED", heartbeat_at=received_at)
                    self.save_account(c, a)
                    return
                p.update(mark=quote[0], mark_received_at=received_at,
                         unrealized_gross_pnl=p["sign"] * p["quantity"] * (quote[0] - p["entry_price"]))
                self.save_position(c, p)
            a["equity"] = a["cash"] + sum(p["unrealized_gross_pnl"] for p in positions)
            # Include estimated exit fees/spread/slippage in the hard-loss check.
            exit_reserve = sum(p["quantity"] * p["mark"] * sum(self.rates[k] for k in ("fee", "half_spread", "slippage")) for p in positions)
            conservative = dict(a, equity=a["equity"] - exit_reserve)
            halt = self.risk_halt(conservative)
            a.update(daily_halted=conservative["daily_halted"], persistent_halt=conservative["persistent_halt"])
            if halt:
                for p in positions:
                    self.close(c, a, p, p["mark"], "RISK_HALT", received_at, received_at)
                a["equity"] = a["cash"]
            a.update(heartbeat_at=received_at, market_received_at=received_at,
                     market_symbols=len(quotes), status="RISK_HALTED" if halt else "RUNNING", error=None)
            self.save_account(c, a)

    def error(self, error, now):
        with self.db.connect() as c:
            a = self.account(c)
            if a:
                a.update(status="DATA_ERROR_ENTRIES_BLOCKED", error=str(error)[:250], heartbeat_at=now)
                self.save_account(c, a)

    def status(self):
        with self.db.connect() as c:
            a = self.account(c)
            positions = self.positions(c)
            orders = [json.loads(r[0]) for r in c.execute("SELECT document FROM cati_sim_orders ORDER BY rowid DESC LIMIT 200")]
            fills = [json.loads(r[0]) for r in c.execute("SELECT document FROM cati_sim_fills ORDER BY rowid DESC LIMIT 200")]
            reasons = {r[0]: r[1] for r in c.execute("SELECT reason,COUNT(*) FROM cati_sim_decisions GROUP BY reason")}
            totals = {name: c.execute(f"SELECT COUNT(*) FROM cati_sim_{name}").fetchone()[0] for name in ("orders", "fills", "decisions")}
        if not a:
            return {"status": "NOT_ACTIVATED", "positions": [], "orders": [], "fills": []}
        closed = [p for p in positions if p["status"] == "CLOSED"]
        opened = [p for p in positions if p["status"] == "OPEN"]
        return {**a, "positions": positions, "orders": orders, "fills": fills,
                "risk": {"daily_cap_fraction": min(.025, self.limits.max_daily_loss_pct),
                         "daily_loss_cash": max(0., a["day_start_equity"] - a["equity"]),
                         "daily_budget_cash": a["day_start_equity"] * min(.025, self.limits.max_daily_loss_pct),
                         "daily_halted": a["daily_halted"], "persistent_halt": a["persistent_halt"],
                         "hard_limits": vars(self.limits), "portfolio_position_limit": 1},
                "pnl": {"realized_gross": sum(p["realized_gross_pnl"] for p in closed),
                        "realized_net": sum(p["realized_net_pnl"] for p in closed),
                        "unrealized_gross": sum(p["unrealized_gross_pnl"] for p in opened),
                        "unrealized_net": sum(p["unrealized_gross_pnl"] - sum(p["entry_costs"].values()) for p in opened),
                        "total_net": a["equity"] - a["initial_virtual_capital"]},
                "decision_reasons": reasons, "open_positions": len(opened), "closed_positions": len(closed),
                "order_count": totals["orders"], "fill_count": totals["fills"], "decision_count": totals["decisions"],
                "simulated_execution_attempts": totals["fills"], "real_execution_attempts": 0,
                "market_age_seconds": (int(time.time() * 1000) - a["market_received_at"]) / 1000 if a["market_received_at"] else None}


class PublicMarket:
    """Only allowlisted unauthenticated public GETs; no broker adapter exists."""
    def __init__(self):
        self.candles = PublicCandles()
        self.instruments = {}
        self.instrument_time = 0.

    def get(self, endpoint):
        if endpoint not in {"/fapi/v2/ticker/price", "/fapi/v1/exchangeInfo"}:
            raise ValueError("SIMULATION_PUBLIC_ENDPOINT_FORBIDDEN")
        response = self.candles.session.get("https://fapi.binance.com" + endpoint, timeout=15)
        if response.status_code in (418, 429):
            raise RateLimited("SIMULATION_PUBLIC_RATE_LIMIT")
        response.raise_for_status()
        return response.json()

    def quotes(self):
        return {r["symbol"]: (float(r["price"]), int(r["time"])) for r in self.get("/fapi/v2/ticker/price")}

    def instrument(self, symbol):
        if time.monotonic() - self.instrument_time > 3600 or not self.instruments:
            instruments = {}
            for row in self.get("/fapi/v1/exchangeInfo")["symbols"]:
                filters = {f["filterType"]: f for f in row["filters"]}
                lot = filters.get("MARKET_LOT_SIZE", filters.get("LOT_SIZE", {}))
                if not lot or float(lot["stepSize"]) <= 0:
                    continue
                instruments[row["symbol"]] = {"status": row["status"], **lot,
                    "minNotional": filters.get("MIN_NOTIONAL", {}).get("notional", 5.)}
            self.instruments = instruments
            self.instrument_time = time.monotonic()
        return self.instruments.get(symbol)


def tick(book, market):
    if not owner_current(book.db):
        return
    state = book.status()
    if state["status"] == "NOT_ACTIVATED":
        return
    quotes = market.quotes()
    if not quotes:
        raise ValueError("NO_LIVE_MARKET_QUOTES")
    if not owner_current(book.db):
        return
    book.mark(quotes, int(time.time() * 1000))
    state = book.status()
    now = int(time.time() * 1000)
    # Reconcile only already-created simulation positions, never old observer
    # positions or counterfactual outcomes. Cursor survives every restart.
    for p in state["positions"]:
        if p["status"] != "OPEN":
            continue
        start = p["entry_time"] if p["last_bar_open"] is None else p["last_bar_open"] + Q
        end = min(now // Q * Q - 1, p["timeout_at"] - 1)
        if start <= end:
            bars = market.candles(p["symbol"], start, end)
            if not bars or int(bars[0][0]) != start or int(bars[-1][0]) + Q - 1 != end:
                raise ValueError("SIMULATION_OUTCOME_PATH_INCOMPLETE")
            for bar in bars:
                if int(bar[6]) != int(bar[0]) + Q - 1 or int(bar[6]) > end:
                    raise ValueError("SIMULATION_BAR_CLOSE_DEFECT")
                if not owner_current(book.db):
                    return
                book.observe_bar(p["id"], [int(bar[0]), *map(float, bar[1:6])], int(time.time() * 1000))
    state = book.status()
    if state["status"] != "RUNNING":
        return
    with book.db.connect() as c:
        exists = c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_residual_decisions'").fetchone()
        rows = [dict(r) for r in c.execute("""SELECT d.* FROM cati_residual_decisions d
            LEFT JOIN cati_sim_decisions s ON s.decision_id=d.decision_id
            WHERE s.decision_id IS NULL AND d.registry_hash=? AND d.decision_time>=?
            ORDER BY d.decision_time""", (REGISTRY_HASH, state["first_decision_time"]))] if exists else []
    for row in rows:
        if not owner_current(book.db):
            return
        now = int(time.time() * 1000)
        bars, instrument = [], None
        if row["selected_symbol"] and row["lifecycle"] != "SKIPPED" and now < row["decision_time"] + 1 + Q and not state["open_positions"]:
            # Current quote is required even though the explicitly modeled fill
            # retains the frozen next-open price. Never replay an expired entry.
            quote = quotes.get(row["selected_symbol"])
            if not quote or not 0 <= now - quote[1] <= 30000:
                continue
            instrument = market.instrument(row["selected_symbol"])
            bars = market.candles(row["selected_symbol"], row["decision_time"] + 1, row["decision_time"] + Q, limit=1)
        if not owner_current(book.db):
            return
        book.consider(row, bars[0] if bars else None, int(time.time() * 1000), instrument)
        state = book.status()


async def run(db):
    book, market = Book(db), PublicMarket()
    fx_check = 0.
    while True:
        delay = 5
        try:
            if os.environ.get("COSMICFORGE_TEST_MODE") == "1":
                return
            if owner_current(db) and book.status()["status"] != "NOT_ACTIVATED" and time.monotonic() - fx_check > 60:
                try:
                    await asyncio.to_thread(ensure_fx_watcher)
                except Exception:
                    logger.exception("[CATI_SIMULATION] FX watcher supervision failed; retry next minute")
                fx_check = time.monotonic()
            await asyncio.to_thread(tick, book, market)
        except asyncio.CancelledError:
            raise
        except RateLimited as exc:
            await asyncio.to_thread(book.error, exc, int(time.time() * 1000))
            delay = 300
        except Exception as exc:
            logger.exception("[CATI_SIMULATION] failed closed; no broker routing")
            await asyncio.to_thread(book.error, exc, int(time.time() * 1000))
            delay = 15
        await asyncio.sleep(delay)


def ensure_fx_watcher():
    """Supervise the existing strict finalization independently of trade calls."""
    if not (ROOT / "data/research/fx_reference_dukascopy.db").is_file():
        return
    import psutil
    script = ROOT / "scripts/fx_finalization_watch.py"
    for proc in psutil.process_iter(["cmdline"]):
        try:
            if any(str(script).lower() == arg.lower() for arg in proc.info["cmdline"] or []):
                return
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue
    output = ROOT / "data/research/fx_finalization"
    output.mkdir(parents=True, exist_ok=True)
    with (output / "watcher.log").open("a") as log:
        subprocess.Popen([sys.executable, str(script)], cwd=ROOT,
                         stdout=log, stderr=subprocess.STDOUT,
                         creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)


def health(db):
    """Public runtime flags only; account and ledger remain authenticated."""
    with db.connect() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_sim_account'").fetchone():
            return {"status": "NOT_ACTIVATED"}
        a = Book.account(c)
    if not a:
        return {"status": "NOT_ACTIVATED"}
    return {k: a.get(k) for k in (*MODE, "status", "heartbeat_at", "market_received_at", "strategy", "registry_hash", "daily_hard_loss_fraction", "error")}
