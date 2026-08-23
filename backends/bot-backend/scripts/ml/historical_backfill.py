"""
Historical Trace & Trade Backfill — Phase 3 Step 5E-3

PURPOSE:
  Replay historical Binance kline data through a standalone indicator engine
  to populate decision_traces and trade_fills in the live cosmicforge.db.

  This unblocks ML dataset building, model training, and shadow analysis.

DESIGN:
  - Fetches Binance public klines (no auth required) via requests
  - Standalone indicator engine: ADX, ATR%, MA slope, buy_score, sell_score
  - Simple but realistic BUY/SELL/HOLD decision logic
  - Simulates TP/SL position lifecycle with MFE/MAE tracking
  - Writes decision_traces via TraceRecorder (same path as live runner)
  - Writes trade_fills via record_fill() (same path as live runner)

USAGE:
  python scripts/ml/historical_backfill.py [--days 90] [--db auto] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# ---------------------------------------------------------------------------
# Path setup  (run from bot-backend/)
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).resolve().parents[2]
_SHARED = _ROOT.parent / "shared"
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_SHARED))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
)
logger = logging.getLogger("backfill")

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
INTERVAL = "15m"
INTERVAL_MS = 15 * 60 * 1000
LOOKBACK = 100          # candles needed to warm up indicators
CANDLE_LIMIT = 1500     # Binance max per request
BINANCE_BASE = "https://fapi.binance.com"

TP_ATR_MULT = 2.0       # take-profit = 2× ATR from entry
SL_ATR_MULT = 1.0       # stop-loss   = 1× ATR from entry
MIN_ADX = 18.0          # minimum ADX to allow directional trades
MIN_CONFIDENCE = 0.45   # minimum signal confidence to open a position
FEE_BPS = 6.0           # 0.06% round-trip fee per leg

# ---------------------------------------------------------------------------
# Indicator computation (pure numpy-free, stdlib only)
# ---------------------------------------------------------------------------

def _ema(values: List[float], period: int) -> List[float]:
    k = 2.0 / (period + 1)
    out: List[float] = []
    ema_val: float = values[0]
    for v in values:
        ema_val = v * k + ema_val * (1 - k)
        out.append(ema_val)
    return out


def compute_indicators(candles: List[List]) -> Dict[str, float]:
    """
    Given a window of candles (each: [open_time, open, high, low, close, volume, ...])
    returns a dict of indicator values at the LAST candle.
    """
    n = len(candles)
    if n < 20:
        return {}

    highs  = [float(c[2]) for c in candles]
    lows   = [float(c[3]) for c in candles]
    closes = [float(c[4]) for c in candles]

    # ---- ATR ------------------------------------------------------------------
    trues = []
    for i in range(1, n):
        hl    = highs[i] - lows[i]
        hpc   = abs(highs[i] - closes[i - 1])
        lpc   = abs(lows[i]  - closes[i - 1])
        trues.append(max(hl, hpc, lpc))

    period14 = 14
    atr_vals: List[float] = []
    if len(trues) >= period14:
        atr_rma = sum(trues[:period14]) / period14
        atr_vals.append(atr_rma)
        for tr in trues[period14:]:
            atr_rma = (atr_rma * (period14 - 1) + tr) / period14
            atr_vals.append(atr_rma)
    atr = atr_vals[-1] if atr_vals else 0.0
    atr_pct = atr / closes[-1] if closes[-1] > 0 else 0.0

    # ---- ADX ------------------------------------------------------------------
    up_moves   = [max(highs[i] - highs[i-1], 0) for i in range(1, n)]
    down_moves = [max(lows[i-1] - lows[i], 0)   for i in range(1, n)]
    dm_plus  = [u if u > d else 0.0 for u, d in zip(up_moves, down_moves)]
    dm_minus = [d if d > u else 0.0 for d, u in zip(down_moves, up_moves)]

    adx = 0.0
    if len(trues) >= period14 and len(dm_plus) >= period14:
        def rma_series(vals: List[float]) -> List[float]:
            result = []
            v = sum(vals[:period14]) / period14
            result.append(v)
            for x in vals[period14:]:
                v = (v * (period14 - 1) + x) / period14
                result.append(v)
            return result

        atr_s   = rma_series(trues)
        dmp_s   = rma_series(dm_plus)
        dmm_s   = rma_series(dm_minus)

        di_diff = [abs(p - m) / (a + 1e-10) for p, m, a in zip(dmp_s, dmm_s, atr_s)]
        di_sum  = [(p + m) / (a + 1e-10) for p, m, a in zip(dmp_s, dmm_s, atr_s)]
        dx_list = [100 * d / (s + 1e-10) for d, s in zip(di_diff, di_sum)]

        if len(dx_list) >= period14:
            adx_rma = sum(dx_list[:period14]) / period14
            for dx in dx_list[period14:]:
                adx_rma = (adx_rma * (period14 - 1) + dx) / period14
            adx = adx_rma

        # DI values (last point)
        dip = 100 * dmp_s[-1] / (atr_s[-1] + 1e-10)
        dim = 100 * dmm_s[-1] / (atr_s[-1] + 1e-10)
    else:
        dip = dim = 0.0

    # ---- MA slope -------------------------------------------------------------
    ma20 = _ema(closes, 20)
    ma50 = _ema(closes, 50) if n >= 50 else ma20
    slope20 = (ma20[-1] - ma20[-5]) / (ma20[-5] + 1e-10) if len(ma20) >= 5 else 0.0

    # ---- Momentum / buy-sell score --------------------------------------------
    # Simple: normalised DI spread drives scores
    di_spread = (dip - dim) / (adx + 1e-10) if adx > 0 else 0.0
    buy_score  = max(0.0, min(1.0, 0.5 + di_spread * 0.5))
    sell_score = max(0.0, min(1.0, 0.5 - di_spread * 0.5))

    # ---- Regime ---------------------------------------------------------------
    if adx >= 30:
        regime = "TRENDING"
    elif adx >= 18:
        regime = "RANGING"
    else:
        regime = "CHOPPY"

    # ---- Compression (BB width proxy) ----------------------------------------
    hi20 = max(highs[-20:])
    lo20 = min(lows[-20:])
    compression = (hi20 - lo20) / (closes[-1] + 1e-10)

    # ---- Breakout pressure (candle body / ATR) --------------------------------
    body = abs(closes[-1] - float(candles[-1][1]))
    breakout_pressure = body / (atr + 1e-10) if atr > 0 else 0.0

    return {
        "atr":               atr,
        "atr_pct":           atr_pct,
        "adx":               adx,
        "dip":               dip,
        "dim":               dim,
        "ma_slope":          slope20,
        "buy_score":         buy_score,
        "sell_score":        sell_score,
        "regime_state":      regime,
        "compression_ratio": compression,
        "breakout_pressure": breakout_pressure,
        "close":             closes[-1],
        "ma20":              ma20[-1],
        "ma50":              ma50[-1],
    }


def generate_signal(ind: Dict[str, float]) -> Tuple[str, float]:
    """
    Returns (signal, confidence) where signal ∈ {BUY, SELL, HOLD}.
    
    Rules:
    - ADX < MIN_ADX → HOLD (choppy market)
    - buy_score > 0.60 AND ma_slope > 0 AND close > ma20 → BUY
    - sell_score > 0.60 AND ma_slope < 0 AND close < ma20 → SELL
    - else HOLD
    """
    adx        = ind.get("adx", 0.0)
    buy_score  = ind.get("buy_score", 0.0)
    sell_score = ind.get("sell_score", 0.0)
    ma_slope   = ind.get("ma_slope", 0.0)
    close      = ind.get("close", 0.0)
    ma20       = ind.get("ma20", 0.0)

    if adx < MIN_ADX:
        return "HOLD", 0.0

    if buy_score > 0.58 and ma_slope > 0 and close > ma20:
        # Map buy_score [0.58, 1.0] → confidence [0.45, 0.95]
        confidence = MIN_CONFIDENCE + (buy_score - 0.58) / 0.42 * 0.50
        return "BUY", min(0.95, confidence)

    if sell_score > 0.58 and ma_slope < 0 and close < ma20:
        confidence = MIN_CONFIDENCE + (sell_score - 0.58) / 0.42 * 0.50
        return "SELL", min(0.95, confidence)

    return "HOLD", 0.0


# ---------------------------------------------------------------------------
# Binance public kline fetch
# ---------------------------------------------------------------------------

def fetch_klines(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> List[List]:
    """Fetch klines from Binance FAPI public endpoint with pagination."""
    all_klines: List[List] = []
    current = start_ms

    while current < end_ms:
        url = f"{BINANCE_BASE}/fapi/v1/klines"
        params = {
            "symbol":    symbol,
            "interval":  interval,
            "startTime": current,
            "endTime":   end_ms,
            "limit":     CANDLE_LIMIT,
        }
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            batch = resp.json()
        except Exception as exc:
            logger.error("Kline fetch failed for %s: %s", symbol, exc)
            break

        if not batch:
            break

        all_klines.extend(batch)
        last_open_time = int(batch[-1][0])

        if last_open_time >= end_ms - INTERVAL_MS:
            break

        current = last_open_time + INTERVAL_MS
        time.sleep(0.12)  # Respect rate limits

    return all_klines


# ---------------------------------------------------------------------------
# Backfill engine
# ---------------------------------------------------------------------------

class BackfillEngine:
    """
    Drives historical klines through the indicator + signal pipeline
    and writes decision_traces + trade_fills to the live database.
    """

    def __init__(self, db_path: str, dry_run: bool = False):
        self.db_path = db_path
        self.dry_run = dry_run
        self.run_id  = f"backfill_{uuid.uuid4().hex[:12]}"

        # Import live write-path dependencies
        from shared_lib.persistence.trace_recorder import TraceRecorder, StrategySignal
        from shared_lib.persistence.db import DB

        self.DB_cls = DB
        self.StrategySignal = StrategySignal

        if not dry_run:
            self.recorder = TraceRecorder(db_path=db_path)
            self.db = DB(path=db_path)
        else:
            self.recorder = None
            self.db = None

        # Counters
        self.total_traces  = 0
        self.total_opens   = 0
        self.total_closes  = 0
        self.total_blocked = 0

    def _write_trade_fill(
        self,
        symbol: str,
        side: str,
        action: str,
        qty: float,
        price: float,
        cycle_id: str,
        strategy: str,
        confidence: float,
        position_id: str,
        fee: float = 0.0,
        realized_pnl: Optional[float] = None,
        exit_reason: Optional[str] = None,
        mfe_pct: Optional[float] = None,
        mae_pct: Optional[float] = None,
        timestamp_utc: Optional[str] = None,
    ) -> None:
        """Write a trade fill using the same record_fill() path as the live runner."""
        if self.dry_run:
            return

        from app.ops.context import set_run_id, set_cycle_id
        from shared_lib.persistence.trade_fills import record_fill

        set_run_id(self.run_id)
        set_cycle_id(cycle_id)

        try:
            record_fill(
                db=self.db,
                symbol=symbol,
                side=side,
                action=action,
                qty=qty,
                price=price,
                fee=fee,
                realized_pnl=realized_pnl,
                strategy=strategy,
                strategy_version="backfill_v1",
                broker_id="backfill",
                account_id="backfill",
                asset_class="CRYPTO",
                timeframe=INTERVAL,
                confidence=confidence,
                position_id=position_id,
                exit_reason=exit_reason,
                mfe_pct=mfe_pct,
                mae_pct=mae_pct,
                slippage_pct=0.001,
            )
        except Exception as exc:
            logger.warning("record_fill failed for %s %s: %s", action, symbol, exc)

    def _write_trace(
        self,
        symbol: str,
        cycle_id: str,
        timestamp_utc: str,
        ind: Dict[str, float],
        signal: str,
        confidence: float,
        gate_allowed: bool,
        gate_reason: str,
        position: str,
        entry_price: Optional[float],
        equity: float,
        intended_action: str,
        sl_plan: Optional[float],
        tp_plan: Optional[float],
        execution_status: str,
        fill_price: Optional[float],
        final_position: str,
        order_id: Optional[str] = None,
    ) -> None:
        """Write a decision_traces row via the live TraceRecorder path."""
        if self.dry_run:
            return

        from app.ops.context import set_run_id, set_cycle_id
        set_run_id(self.run_id)
        set_cycle_id(cycle_id)

        trace_id = self.recorder.start_trace(
            run_id=self.run_id,
            cycle_id=cycle_id,
            symbol=symbol,
            account_id="backfill",
            environment="backfill",
            timeframe=INTERVAL,
        )

        # Override the ts to match the historical candle time
        with self.recorder._lock:
            if trace_id in self.recorder._traces:
                self.recorder._traces[trace_id].ts = timestamp_utc

        self.recorder.record_market(
            trace_id=trace_id,
            last_price=ind.get("close", 0.0),
            equity=equity,
            regime_state=ind.get("regime_state", "UNKNOWN"),
        )

        sig_obj = self.StrategySignal(
            strategy_name="backfill_ensemble",
            signal=signal,
            confidence=confidence,
            reason=f"adx={ind.get('adx', 0):.1f} slope={ind.get('ma_slope', 0):.4f}",
        )

        # Derive side from signal
        if signal == "BUY":
            side_val = 1
        elif signal == "SELL":
            side_val = -1
        else:
            side_val = 0

        self.recorder.record_strategies(
            trace_id=trace_id,
            signals=[sig_obj],
            chosen_strategy="backfill_ensemble",
            final_signal=signal,
            final_confidence=confidence,
            reason_codes=gate_reason or "NONE",
            adx=ind.get("adx"),
            atr_pct=ind.get("atr_pct"),
            ma_slope=ind.get("ma_slope"),
            compression_ratio=ind.get("compression_ratio"),
            breakout_pressure=ind.get("breakout_pressure"),
            buy_score=ind.get("buy_score"),
            sell_score=ind.get("sell_score"),
            threshold=MIN_CONFIDENCE,
            active_strategy_count=1,
            htf_opposed=False,
        )

        self.recorder.record_gate(
            trace_id=trace_id,
            allowed=gate_allowed,
            reason_code=gate_reason or "ALLOWED",
            reason=gate_reason or "",
        )

        self.recorder.record_intent(
            trace_id=trace_id,
            action=intended_action,
            sizing={"qty": 1.0, "usdt": 100.0},
            sl_plan=sl_plan,
            tp_plan=tp_plan,
        )

        self.recorder.record_execution(
            trace_id=trace_id,
            status=execution_status,
            order_id=order_id,
            fill_price=fill_price,
        )

        # Patch in the side value directly (column that build_dataset uses)
        with self.recorder._lock:
            if trace_id in self.recorder._traces:
                # We inject side via the signal field for ML labelling
                pass  # signal already encodes direction

        self.recorder.finalize(
            trace_id=trace_id,
            state_change=intended_action,
            final_position=final_position,
        )

        self.total_traces += 1

    def run_symbol(self, symbol: str, klines: List[List], equity: float = 10000.0) -> Dict:
        """Simulate trading for one symbol across all historical candles."""
        logger.info("  Processing %s: %d candles", symbol, len(klines))

        position: Optional[str] = None       # None, "LONG", "SHORT"
        entry_price: float = 0.0
        entry_qty:   float = 0.0
        position_id: Optional[str] = None
        sl_price:    float = 0.0
        tp_price:    float = 0.0
        entry_ts:    str = ""
        open_cycle_id: str = ""
        mfe:         float = 0.0
        mae:         float = 0.0

        stats = {"opens": 0, "closes": 0, "blocked": 0, "traces": 0}

        for i in range(LOOKBACK, len(klines)):
            candle  = klines[i]
            window  = klines[i - LOOKBACK: i + 1]

            open_time_ms = int(candle[0])
            close_price  = float(candle[4])
            high_price   = float(candle[2])
            low_price    = float(candle[3])

            ts_dt = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)
            ts_iso = ts_dt.isoformat()
            cycle_id = f"{self.run_id}_{symbol}_{i}"

            ind = compute_indicators(window)
            if not ind:
                continue

            atr = ind.get("atr", 0.0)

            # ---------------------------------------------------------------
            # 1. Check TP / SL on open position
            # ---------------------------------------------------------------
            if position is not None:
                hit_tp = hit_sl = False

                if position == "LONG":
                    current_pnl = (close_price - entry_price) / entry_price
                    mfe = max(mfe, (high_price - entry_price) / entry_price)
                    mae = min(mae, (low_price  - entry_price) / entry_price)
                    if low_price  <= sl_price:
                        hit_sl = True
                        exit_price = sl_price
                        exit_reason = "SL"
                    elif high_price >= tp_price:
                        hit_tp = True
                        exit_price = tp_price
                        exit_reason = "TP1"
                    else:
                        exit_price = None

                else:  # SHORT
                    current_pnl = (entry_price - close_price) / entry_price
                    mfe = max(mfe, (entry_price - low_price)  / entry_price)
                    mae = min(mae, (entry_price - high_price) / entry_price)
                    if high_price >= sl_price:
                        hit_sl = True
                        exit_price = sl_price
                        exit_reason = "SL"
                    elif low_price <= tp_price:
                        hit_tp = True
                        exit_price = tp_price
                        exit_reason = "TP1"
                    else:
                        exit_price = None

                if hit_tp or hit_sl:
                    # Gross PnL
                    if position == "LONG":
                        gross = (exit_price - entry_price) * entry_qty
                    else:
                        gross = (entry_price - exit_price) * entry_qty

                    fees = entry_price * entry_qty * (FEE_BPS / 10000.0) + \
                           exit_price  * entry_qty * (FEE_BPS / 10000.0)
                    net_pnl = gross - fees
                    equity += net_pnl

                    # Write CLOSE fill
                    self._write_trade_fill(
                        symbol=symbol,
                        side=position,
                        action="CLOSE",
                        qty=entry_qty,
                        price=exit_price,
                        cycle_id=cycle_id,
                        strategy="backfill_ensemble",
                        confidence=0.0,
                        position_id=position_id,
                        fee=fees / 2,
                        realized_pnl=net_pnl,
                        exit_reason=exit_reason,
                        mfe_pct=round(mfe * 100, 4),
                        mae_pct=round(mae * 100, 4),
                        timestamp_utc=ts_iso,
                    )

                    # Write CLOSE trace
                    self._write_trace(
                        symbol=symbol,
                        cycle_id=cycle_id,
                        timestamp_utc=ts_iso,
                        ind=ind,
                        signal="CLOSE",
                        confidence=0.0,
                        gate_allowed=True,
                        gate_reason="EXIT_" + exit_reason,
                        position=position,
                        entry_price=entry_price,
                        equity=equity,
                        intended_action="CLOSE",
                        sl_plan=sl_price,
                        tp_plan=tp_price,
                        execution_status="CLOSED",
                        fill_price=exit_price,
                        final_position="NONE",
                        order_id=f"close_{position_id}",
                    )
                    stats["traces"] += 1
                    stats["closes"] += 1

                    position = None
                    entry_price = entry_qty = sl_price = tp_price = 0.0
                    mfe = mae = 0.0
                    position_id = None
                    continue

                # Ongoing position — write monitoring trace (every 4th candle)
                if i % 4 == 0:
                    self._write_trace(
                        symbol=symbol,
                        cycle_id=cycle_id,
                        timestamp_utc=ts_iso,
                        ind=ind,
                        signal="HOLD",
                        confidence=0.0,
                        gate_allowed=False,
                        gate_reason="IN_POSITION",
                        position=position,
                        entry_price=entry_price,
                        equity=equity,
                        intended_action="HOLD",
                        sl_plan=sl_price,
                        tp_plan=tp_price,
                        execution_status="None",
                        fill_price=None,
                        final_position=position,
                    )
                    stats["traces"] += 1
                continue  # Don't open new while in position

            # ---------------------------------------------------------------
            # 2. Generate signal for new entry
            # ---------------------------------------------------------------
            signal, confidence = generate_signal(ind)

            if signal == "HOLD" or confidence < MIN_CONFIDENCE:
                # Write HOLD trace (1 in 5 to avoid DB bloat)
                if i % 5 == 0:
                    self._write_trace(
                        symbol=symbol,
                        cycle_id=cycle_id,
                        timestamp_utc=ts_iso,
                        ind=ind,
                        signal="HOLD",
                        confidence=confidence,
                        gate_allowed=False,
                        gate_reason="HOLD_SIGNAL",
                        position=None,
                        entry_price=None,
                        equity=equity,
                        intended_action="HOLD",
                        sl_plan=None,
                        tp_plan=None,
                        execution_status="None",
                        fill_price=None,
                        final_position="NONE",
                    )
                    stats["traces"] += 1
                    stats["blocked"] += 1
                continue

            # ---------------------------------------------------------------
            # 3. Open new position
            # ---------------------------------------------------------------
            if atr <= 0:
                continue

            side_str = "LONG" if signal == "BUY" else "SHORT"
            usdt = 100.0
            qty  = usdt / close_price
            fee  = usdt * (FEE_BPS / 10000.0)

            if side_str == "LONG":
                sl = close_price - atr * SL_ATR_MULT
                tp = close_price + atr * TP_ATR_MULT
            else:
                sl = close_price + atr * SL_ATR_MULT
                tp = close_price - atr * TP_ATR_MULT

            position    = side_str
            entry_price = close_price
            entry_qty   = qty
            sl_price    = sl
            tp_price    = tp
            mfe = mae   = 0.0
            position_id = str(uuid.uuid4())
            entry_ts    = ts_iso
            open_cycle_id = cycle_id

            equity -= fee

            # Write OPEN fill
            self._write_trade_fill(
                symbol=symbol,
                side=side_str,
                action="OPEN",
                qty=qty,
                price=close_price,
                cycle_id=cycle_id,
                strategy="backfill_ensemble",
                confidence=confidence,
                position_id=position_id,
                fee=fee,
                timestamp_utc=ts_iso,
            )

            # Write OPEN trace
            self._write_trace(
                symbol=symbol,
                cycle_id=cycle_id,
                timestamp_utc=ts_iso,
                ind=ind,
                signal=signal,
                confidence=confidence,
                gate_allowed=True,
                gate_reason="OPEN",
                position=side_str,
                entry_price=close_price,
                equity=equity,
                intended_action="OPEN",
                sl_plan=sl,
                tp_plan=tp,
                execution_status="ORDER_PLACED",
                fill_price=close_price,
                final_position=side_str,
                order_id=position_id,
            )
            stats["traces"] += 1
            stats["opens"]  += 1

        # Force-close any open position at last bar
        if position is not None and len(klines) > LOOKBACK:
            last_candle = klines[-1]
            exit_price  = float(last_candle[4])
            ts_iso = datetime.fromtimestamp(
                int(last_candle[0]) / 1000, tz=timezone.utc
            ).isoformat()
            cycle_id = f"{self.run_id}_{symbol}_final"

            if position == "LONG":
                gross = (exit_price - entry_price) * entry_qty
            else:
                gross = (entry_price - exit_price) * entry_qty
            fees = (entry_price + exit_price) * entry_qty * (FEE_BPS / 10000.0)
            net_pnl = gross - fees
            equity += net_pnl

            self._write_trade_fill(
                symbol=symbol, side=position, action="CLOSE",
                qty=entry_qty, price=exit_price, cycle_id=cycle_id,
                strategy="backfill_ensemble", confidence=0.0,
                position_id=position_id, fee=fees / 2,
                realized_pnl=net_pnl, exit_reason="TIME_EXIT",
                mfe_pct=round(mfe * 100, 4), mae_pct=round(mae * 100, 4),
                timestamp_utc=ts_iso,
            )
            stats["closes"] += 1

        self.total_traces  += stats["traces"]
        self.total_opens   += stats["opens"]
        self.total_closes  += stats["closes"]
        self.total_blocked += stats["blocked"]

        logger.info(
            "  %s done: traces=%d opens=%d closes=%d",
            symbol, stats["traces"], stats["opens"], stats["closes"]
        )
        return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Historical trace & trade backfill")
    parser.add_argument("--days",    type=int,   default=90,    help="History window in days")
    parser.add_argument("--db",      type=str,   default="auto", help="Path to cosmicforge.db, or 'auto'")
    parser.add_argument("--dry-run", action="store_true",        help="Simulate without writing to DB")
    parser.add_argument("--symbols", type=str,   default=",".join(SYMBOLS))
    args = parser.parse_args()

    # Resolve DB path
    if args.db == "auto":
        db_path = str(_SHARED / "shared_lib" / "persistence" / "cosmicforge.db")
    else:
        db_path = args.db
    db_path = str(Path(db_path).resolve())
    logger.info("Database: %s", db_path)
    if args.dry_run:
        logger.info("DRY RUN — no writes to database")

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    # Calculate time range
    now_ms   = int(time.time() * 1000)
    start_ms = now_ms - args.days * 24 * 60 * 60 * 1000

    logger.info(
        "Backfill: %d days of %s klines for %s",
        args.days, INTERVAL, symbols,
    )

    engine = BackfillEngine(db_path=db_path, dry_run=args.dry_run)

    total_equity = 10000.0
    for symbol in symbols:
        logger.info("Fetching klines for %s ...", symbol)
        klines = fetch_klines(symbol, INTERVAL, start_ms, now_ms)

        if not klines:
            logger.warning("No klines for %s — skipping", symbol)
            continue

        logger.info("Fetched %d candles for %s", len(klines), symbol)
        engine.run_symbol(symbol, klines, equity=total_equity)

    logger.info("=" * 60)
    logger.info("BACKFILL COMPLETE")
    logger.info("  decision_traces written : %d", engine.total_traces)
    logger.info("  trade_fills OPEN        : %d", engine.total_opens)
    logger.info("  trade_fills CLOSE       : %d", engine.total_closes)
    logger.info("  HOLD/blocked traces     : %d", engine.total_blocked)
    logger.info("=" * 60)

    if engine.total_closes < 500:
        logger.warning(
            "⚠️  Only %d closed trades generated — target is 500+. "
            "Consider increasing --days or adjusting signal thresholds.",
            engine.total_closes,
        )
    else:
        logger.info("✅ Target met: %d closed trades", engine.total_closes)


if __name__ == "__main__":
    main()
