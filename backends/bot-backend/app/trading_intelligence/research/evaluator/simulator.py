"""Daily portfolio simulator for the daily trend family (Section H, Step 2.4).

One account, long-or-flat, USDT-margined perpetuals, daily bars. The strategy rules are NOT here: signals come
from ``families.daily_trend.rules`` and targets from ``families.daily_trend.targets``; this module only moves
an account through time and keeps the books.

Order of events on day t (decisions were made at the close of t-1):

1. a held coin with no bar today is closed at its last close with the stress cost (data gap / contract ended);
2. the 00:00 funding event is settled on the quantity held BEFORE today's fills;
3. yesterday's orders fill at today's open (fee and slippage on the traded amount; exchange minimums apply);
4. stops: if today's low is at or below the stop, the position is closed at the stop, or at the open when the
   day opens below it, less slippage -- including a position opened this morning;
5. later funding events are settled on the quantity held after the fills (on a stop-out day costs are charged
   and income is not credited, because the hour of the stop is unknown);
6. positions are marked to the close; equity, drawdown and the brakes are updated;
7. the decision for tomorrow is made from what is known at this close; stops ratchet up.

Costs are never netted into prices: price P&L, fees, slippage and funding are four separate ledgers and
``equity = initial + price P&L - fees - slippage - funding``. "Gross" always means price P&L of the positions
actually held, before all three costs.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence

import numpy as np

from app.trading_intelligence.families.daily_trend import rules, targets as T
from app.trading_intelligence.families.daily_trend.spec import INTERPRETATION, SPECIFICATION

SIMULATOR_VERSION = "daily-portfolio-simulator-1"
EXIT_SIGNAL, EXIT_STOP, EXIT_STOP_GAP, EXIT_DATA_GAP, EXIT_BRAKE, EXIT_END = (
    "TARGET_ZERO", "STOP", "STOP_GAP_THROUGH", "NO_BAR_DATA_GAP_OR_CONTRACT_ENDED", "DRAWDOWN_STOP", "OPEN_AT_PERIOD_END")


@dataclass
class Features:
    """Everything the simulator reads, as day x symbol matrices. Row t was computed from rows 0..t only."""

    days: np.ndarray
    symbols: List[str]
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    quote_volume: np.ndarray
    strength: np.ndarray
    stop_distance: np.ndarray
    member: np.ndarray
    volume: np.ndarray
    funding_midnight: np.ndarray
    funding_later_positive: np.ndarray
    funding_later_negative: np.ndarray
    funding_hours: np.ndarray
    overlay: np.ndarray
    in_scope: np.ndarray
    step: np.ndarray
    min_notional: np.ndarray
    notes: Dict[str, Any] = field(default_factory=dict)


def in_scope_mask(symbols: Sequence[str], metadata: Mapping[str, Any]) -> np.ndarray:
    """Interpretation I2: stablecoin pairs and non-crypto contracts are out; unlisted symbols count as crypto."""
    stable = set(INTERPRETATION["stablecoin_bases"])
    out = []
    for s in symbols:
        base = s.replace("SETTLED", "")[:-4]
        meta = (metadata.get("symbols") or {}).get(s.replace("SETTLED", ""))
        tradfi = bool(meta) and meta.get("contract_type") == "TRADIFI_PERPETUAL"
        out.append(base not in stable and not tradfi)
    return np.array(out, dtype=bool)


def apply_relisting_rule(symbols: Sequence[str], fields: Mapping[str, np.ndarray]) -> int:
    """Interpretation I1: where two archive symbols with the same base have a bar on the same day, the one with
    fewer SETTLED suffixes is used. Returns the number of bar-days removed from the longer-suffixed symbol."""
    index = {s: i for i, s in enumerate(symbols)}
    removed = 0
    for s, i in index.items():
        shorter = s
        while shorter.endswith("SETTLED"):
            shorter = shorter[:-7]
            j = index.get(shorter)
            if j is None:
                continue
            both = ~np.isnan(fields["close"][:, i]) & ~np.isnan(fields["close"][:, j])
            if both.any():
                removed += int(both.sum())
                for name in ("open", "high", "low", "close", "quote_volume"):
                    fields[name][both, i] = np.nan
    return removed


def prepare(panel: Any, metadata: Mapping[str, Any]) -> Features:
    """Signals, stop distances and the point-in-time universe for a panel. Pure and causal."""
    f = {k: np.array(v, dtype=np.float64, copy=True) for k, v in panel.fields.items()}
    symbols = list(panel.symbols)
    removed = apply_relisting_rule(symbols, f)
    scope = in_scope_mask(symbols, metadata)
    member, volume = rules.universe(f["close"], f["quote_volume"], scope)
    total_funding = f["funding_midnight"] + f["funding_later_positive"] + f["funding_later_negative"]
    meta = metadata.get("symbols") or {}

    def number(symbol: str, key: str, default: float) -> float:
        value = (meta.get(symbol.replace("SETTLED", "")) or {}).get(key) if not symbol.endswith("SETTLED") else None
        try:
            return float(value) if value is not None else default
        except (TypeError, ValueError):
            return default

    return Features(
        days=np.asarray(panel.days), symbols=symbols, open=f["open"], high=f["high"], low=f["low"], close=f["close"],
        quote_volume=f["quote_volume"], strength=rules.strength(f["close"]),
        stop_distance=rules.stop_distance(f["high"], f["low"], f["close"]), member=member, volume=volume,
        funding_midnight=f["funding_midnight"], funding_later_positive=f["funding_later_positive"],
        funding_later_negative=f["funding_later_negative"], funding_hours=f["funding_hours"],
        overlay=rules.funding_overlay_factor(rules.funding_annualised(total_funding)), in_scope=scope,
        step=np.array([number(s, "market_step_size", 0.0) for s in symbols]),
        min_notional=np.array([number(s, "min_notional", INTERPRETATION["min_notional_without_metadata_usdt"])
                               for s in symbols]),
        notes={"relisting_bar_days_removed": removed, "symbols_out_of_scope": int((~scope).sum()),
               "symbols_without_exchange_metadata": sum(1 for s in symbols if s.replace("SETTLED", "") not in meta
                                                        or s.endswith("SETTLED"))})


@dataclass(frozen=True)
class RunConfig:
    level: T.RiskLevel
    start_day: int                      # first day of the account (day index); it may trade at this day's open
    end_day: int
    cost_multiple: float = 1.0          # 1 = base cost, 2 = stress
    drawdown_brakes: bool = True        # False = "without the brake" (daily pause stays on)
    funding_overlay: bool = False       # secondary test
    initial_equity: float = INTERPRETATION["initial_equity_usdt"]

    @property
    def label(self) -> str:
        return "|".join([self.level.policy, self.level.name, "base" if self.cost_multiple == 1.0 else
                         f"cost_x{self.cost_multiple:g}", "brakes_on" if self.drawdown_brakes else "brakes_off",
                         "overlay" if self.funding_overlay else "no_overlay"])


@dataclass
class _Position:
    qty: float
    stop: float
    mark: float
    trade: Dict[str, Any]
    stopped: bool = False


@dataclass
class SimResult:
    config: RunConfig
    days: np.ndarray
    daily: Dict[str, np.ndarray]
    trades: List[Dict[str, Any]]
    fills: List[Dict[str, Any]]
    rejections: List[Dict[str, Any]]
    entry_decisions: List[Dict[str, Any]]
    targets: List[Dict[str, Any]]
    events: List[Dict[str, Any]]
    totals: Dict[str, float]


def simulate(feat: Features, cfg: RunConfig) -> SimResult:
    spec, level = SPECIFICATION, cfg.level
    fee_rate = spec["taker_fee"] * cfg.cost_multiple
    slip_rate = spec["slippage"] * cfg.cost_multiple
    gap_multiple = spec["stress_cost_multiple"] / 1.0            # a data-gap exit always pays the stress cost
    missing_rate = INTERPRETATION["missing_funding_rate_per_8h"]
    days = feat.days
    i0, i1 = int(np.searchsorted(days, cfg.start_day)), int(np.searchsorted(days, cfg.end_day, side="right")) - 1
    if i0 > i1:
        raise ValueError("the run period holds no day of the panel")
    n = i1 - i0 + 1
    daily = {k: np.zeros(n) for k in ("equity", "return", "pnl_price", "fees", "slippage", "funding", "gross_notional",
                                      "positions", "drawdown", "paused", "halved", "halted", "funding_events",
                                      "funding_events_imputed")}
    sym = feat.symbols
    equity = peak = float(cfg.initial_equity)
    positions: Dict[int, _Position] = {}
    trades, fills, rejections, entry_decisions, target_log, events = [], [], [], [], [], []
    halted = halve = paused = False
    totals = {"pnl_price": 0.0, "fees": 0.0, "slippage": 0.0, "funding": 0.0, "turnover": 0.0, "max_participation": 0.0}

    def book(row: int, key: str, amount: float) -> None:
        nonlocal equity
        totals[key] += amount
        daily[key][row] += amount
        equity += amount if key == "pnl_price" else -amount

    def trade_cost(row: int, pos_trade: Optional[Dict[str, Any]], signed_qty: float, price: float,
                   multiple: float = 1.0) -> float:
        """Slippage is adverse movement on the traded amount; the fee is charged on the filled value. Returns
        the fill price (above the reference for a buy, below it for a sell)."""
        fill = price * (1.0 + math.copysign(slip_rate * multiple, signed_qty))
        slip = abs(signed_qty) * price * slip_rate * multiple
        fee = abs(signed_qty) * fill * fee_rate * multiple
        book(row, "slippage", slip)
        book(row, "fees", fee)
        totals["turnover"] += abs(signed_qty) * price
        if pos_trade is not None:
            pos_trade["fees"] += fee
            pos_trade["slippage"] += slip
        return fill

    def charge_funding(row: int, pos: _Position, amount: float, events: float, imputed: float = 0.0) -> None:
        daily["funding_events"][row] += events
        daily["funding_events_imputed"][row] += imputed
        if amount:
            book(row, "funding", amount)
            pos.trade["funding"] += amount

    def close_position(row: int, i: int, j: int, price: float, reason: str, multiple: float = 1.0) -> None:
        pos = positions.pop(j)
        pnl = pos.qty * (price - pos.mark)
        book(row, "pnl_price", pnl)
        pos.trade["pnl_price"] += pnl
        fill_price = trade_cost(row, pos.trade, -pos.qty, price, multiple)
        pos.trade.update(exit_day=int(days[i]), exit_price=fill_price, exit_reason=reason,
                         net_pnl=pos.trade["pnl_price"] - pos.trade["fees"] - pos.trade["slippage"] - pos.trade["funding"],
                         days_held=int(days[i]) - pos.trade["entry_day"])
        trades.append(pos.trade)
        fills.append({"day": int(days[i]), "symbol": sym[j], "kind": reason, "quantity": -pos.qty, "price": fill_price})

    def decide(i: int) -> List[T.Order]:
        """The decision at the close of row i, from row i only (the features are causal)."""
        members = np.flatnonzero(feat.member[i])
        cands = [T.Candidate(sym[j], float(feat.strength[i, j]), float(feat.stop_distance[i, j]), float(feat.volume[i, j]),
                             float(feat.overlay[i, j]) if cfg.funding_overlay else 1.0) for j in members]
        held_notional = {sym[j]: p.qty * feat.close[i, j] for j, p in positions.items()}
        decision = T.decide_targets(equity=equity, candidates=cands, held=held_notional, level=level, halve=halve,
                                    halted=halted, paused=paused)
        close = {sym[j]: float(feat.close[i, j]) for j in set(members) | set(positions)}
        orders = T.orders_from_targets(decision, held_quantity={sym[j]: p.qty for j, p in positions.items()}, close=close)
        day = int(days[i])
        for r in decision.rejected:
            rejections.append({"day": day, "stage": "DECISION", **r})
        why = {r["symbol"]: r["reason"] for r in decision.rejected if r["reason"] != T.REJECT_POSITION_LIMIT}
        for c in cands:                      # every NEW position the rule selected, whether or not it was allowed
            if held_notional.get(c.symbol, 0.0) <= 0 and (c.symbol in decision.targets or c.symbol in why):
                entry_decisions.append({"day": day, "symbol": c.symbol, "strength": c.strength,
                                        "stop_distance": c.stop_distance, "accepted": c.symbol in decision.targets,
                                        "reason": why.get(c.symbol)})
        target_log.append({"day": day, "equity": equity, "universe": int(len(members)), "qualified": decision.qualified,
                           "scale": dict(decision.scale), "open_risk": decision.open_risk,
                           "targets": {s: {"notional": v, "fraction_of_equity": v / equity if equity > 0 else 0.0,
                                           "strength": decision.strength.get(s), "stop_distance": decision.stop_distance[s]}
                                       for s, v in sorted(decision.targets.items())}})
        return orders

    index = {s: j for j, s in enumerate(sym)}
    pending: List[T.Order] = decide(i0 - 1) if i0 > 0 else []      # a fresh account may trade at its first open

    for i in range(i0, i1 + 1):
        row = i - i0
        day = int(days[i])
        start_equity = equity
        # 1. held coins without a bar ------------------------------------------------------------
        for j in [j for j in sorted(positions) if math.isnan(feat.open[i, j])]:
            events.append({"day": day, "symbol": sym[j], "event": EXIT_DATA_GAP, "price": positions[j].mark})
            close_position(row, i, j, positions[j].mark, EXIT_DATA_GAP, gap_multiple / max(cfg.cost_multiple, 1e-12))
        # 2. the 00:00 funding event, on what was held coming into the day; then mark to the open ----
        for j, pos in positions.items():
            price, known = feat.open[i, j], feat.funding_hours[i, j] > 0
            rate = feat.funding_midnight[i, j] if known else missing_rate       # no record at all: never zero
            charge_funding(row, pos, pos.qty * price * rate, 1.0, 0.0 if known else 1.0)
            pnl = pos.qty * (price - pos.mark)
            book(row, "pnl_price", pnl)
            pos.trade["pnl_price"] += pnl
            pos.mark = price
        # 3. yesterday's orders fill at the open --------------------------------------------------
        for order in pending:
            j = index[order.symbol]
            price = feat.open[i, j]
            if math.isnan(price):
                if order.kind != T.EXIT:
                    rejections.append({"day": day, "stage": "FILL", "symbol": order.symbol, "reason": T.REJECT_NO_BAR})
                continue
            pos = positions.get(j)
            if order.kind != T.ENTRY and pos is None:
                continue                                       # the position was closed by a gap exit above
            qty, reason = T.apply_exchange_filters(order, price=price, step=float(feat.step[j]) or None,
                                                   min_notional=float(feat.min_notional[j]) or None)
            if reason:
                rejections.append({"day": day, "stage": "FILL", "symbol": order.symbol, "reason": reason})
                continue
            if order.kind == T.EXIT:
                close_position(row, i, j, price, EXIT_BRAKE if halted else EXIT_SIGNAL)
                continue
            volume_today = feat.quote_volume[i, j]
            if volume_today > 0:
                totals["max_participation"] = max(totals["max_participation"], abs(qty) * price / volume_today)
            if order.kind == T.ENTRY:
                trade = {"symbol": order.symbol, "entry_day": day, "entry_price": 0.0, "quantity": qty,
                         "max_quantity": qty, "entry_stop_distance": order.stop_distance, "pnl_price": 0.0,
                         "fees": 0.0, "slippage": 0.0, "funding": 0.0, "adjustments": 0}
                fill = trade_cost(row, trade, qty, price)
                trade["entry_price"] = fill
                positions[j] = _Position(qty=qty, stop=fill * (1.0 - order.stop_distance), mark=price, trade=trade)
            else:
                fill = trade_cost(row, pos.trade, qty, price)
                pos.qty += qty
                pos.trade["adjustments"] += 1
                pos.trade["max_quantity"] = max(pos.trade["max_quantity"], pos.qty)
            fills.append({"day": day, "symbol": order.symbol, "kind": order.kind, "quantity": qty, "price": fill})
        # 4./5. stops and the later funding events ---------------------------------------------------
        for j in sorted(positions):
            pos = positions[j]
            price = feat.open[i, j]
            hours = min(24.0, float(feat.funding_hours[i, j]))
            missing = (24.0 - hours) / 8.0 if hours > 0 else 2.0       # 8h-equivalents the archive lacks after 00:00
            imputed = pos.qty * price * missing_rate * missing
            if feat.low[i, j] <= pos.stop:
                gap = price < pos.stop
                events.append({"day": day, "symbol": sym[j], "event": EXIT_STOP_GAP if gap else EXIT_STOP,
                               "stop": pos.stop, "open": float(price), "low": float(feat.low[i, j])})
                # the hour of the stop is unknown: later funding is charged when a cost, never credited
                charge_funding(row, pos, pos.qty * price * feat.funding_later_positive[i, j] + imputed, 2.0, missing)
                close_position(row, i, j, price if gap else pos.stop, EXIT_STOP_GAP if gap else EXIT_STOP)
            else:
                later = feat.funding_later_positive[i, j] + feat.funding_later_negative[i, j]
                charge_funding(row, pos, pos.qty * price * later + imputed, 2.0, missing)
        # 6. mark to the close; equity and brakes ---------------------------------------------------
        gross = 0.0
        for j, pos in positions.items():
            pnl = pos.qty * (feat.close[i, j] - pos.mark)
            book(row, "pnl_price", pnl)
            pos.trade["pnl_price"] += pnl
            pos.mark = feat.close[i, j]
            gross += pos.qty * pos.mark
        ret = equity / start_equity - 1.0 if start_equity > 0 else 0.0
        peak = max(peak, equity)
        drawdown = equity / peak - 1.0 if peak > 0 else 0.0
        paused = ret <= -level.daily_pause
        if cfg.drawdown_brakes:
            halve = drawdown <= -level.halve_drawdown
            if not halted and drawdown <= -level.stop_drawdown:
                halted = True
                events.append({"day": day, "event": "DRAWDOWN_STOP_FIRED", "drawdown": drawdown})
        if equity <= 0 and not halted:
            halted = True
            events.append({"day": day, "event": "ACCOUNT_EQUITY_EXHAUSTED"})
        daily["equity"][row], daily["return"][row], daily["drawdown"][row] = equity, ret, drawdown
        daily["gross_notional"][row], daily["positions"][row] = gross, len(positions)
        daily["paused"][row], daily["halved"][row], daily["halted"][row] = paused, halve, halted
        # 7. stops ratchet up; the decision for tomorrow --------------------------------------------
        for j, pos in positions.items():
            d = feat.stop_distance[i, j]
            if math.isfinite(d):
                pos.stop = max(pos.stop, feat.close[i, j] * (1.0 - d))
        pending = decide(i)

    for j, pos in sorted(positions.items()):                    # still open at the end: marked, not closed
        t = dict(pos.trade)
        t.update(exit_day=None, exit_price=None, exit_reason=EXIT_END, days_held=int(days[i1]) - t["entry_day"],
                 net_pnl=t["pnl_price"] - t["fees"] - t["slippage"] - t["funding"])
        trades.append(t)
    return SimResult(config=cfg, days=days[i0:i1 + 1], daily=daily, trades=trades, fills=fills, rejections=rejections,
                     entry_decisions=entry_decisions, targets=target_log, events=events, totals=totals)


__all__ = ["SIMULATOR_VERSION", "Features", "RunConfig", "SimResult", "prepare", "simulate", "in_scope_mask",
           "apply_relisting_rule", "EXIT_SIGNAL", "EXIT_STOP", "EXIT_STOP_GAP", "EXIT_DATA_GAP", "EXIT_BRAKE", "EXIT_END"]
