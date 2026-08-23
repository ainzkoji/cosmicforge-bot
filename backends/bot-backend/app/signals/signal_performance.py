from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.signals.crypto_signal_engine import BinanceKlineMarketData, normalize_candles
from app.signals.signal_expiry import ensure_signal_performance, parse_utc
from shared_lib.persistence.db import DB, utc_now_iso
from shared_lib.persistence.signals import (
    PERFORMANCE_RESULT_AMBIGUOUS,
    PERFORMANCE_RESULT_EXPIRED,
    PERFORMANCE_RESULT_INVALIDATED,
    PERFORMANCE_RESULT_LOSS,
    PERFORMANCE_RESULT_WIN,
    SIGNAL_STATUS_ACTIVE,
    SIGNAL_STATUS_EXPIRED,
    SIGNAL_STATUS_INVALIDATED,
    SIGNAL_STATUS_PENDING_ENTRY,
    SIGNAL_STATUS_SL_HIT,
    SIGNAL_STATUS_TP1_HIT,
    SIGNAL_STATUS_TP2_HIT,
    SIGNAL_STATUS_TP3_HIT,
    ensure_signals_schema,
    get_signal_performance,
    update_signal_performance,
    update_trading_signal,
)


TRACKABLE_STATUSES = {
    SIGNAL_STATUS_PENDING_ENTRY,
    SIGNAL_STATUS_ACTIVE,
    SIGNAL_STATUS_TP1_HIT,
    SIGNAL_STATUS_TP2_HIT,
}
TERMINAL_STATUSES = {
    SIGNAL_STATUS_EXPIRED,
    SIGNAL_STATUS_SL_HIT,
    "CANCELLED",
    "INVALIDATED",
    SIGNAL_STATUS_TP3_HIT,
}


class SignalPerformanceUpdater:
    def __init__(self, db: DB | None = None, market_data: Any | None = None, candle_limit: int = 120):
        self.db = db or DB()
        self.market_data = market_data
        self.candle_limit = int(candle_limit)
        ensure_signals_schema(self.db)

    def update_signal_performance(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now(timezone.utc)
        summary = {
            "checked": 0,
            "expired": 0,
            "entry_triggered": 0,
            "tp1_hit": 0,
            "tp2_hit": 0,
            "tp3_hit": 0,
            "sl_hit": 0,
            "ambiguous": 0,
            "invalidated": 0,
            "errors": [],
        }
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM trading_signals
                WHERE is_published = 1
                  AND status IN (?, ?, ?, ?)
                ORDER BY created_at ASC
                """,
                (SIGNAL_STATUS_PENDING_ENTRY, SIGNAL_STATUS_ACTIVE, SIGNAL_STATUS_TP1_HIT, SIGNAL_STATUS_TP2_HIT),
            ).fetchall()
        for row in rows:
            signal = dict(row)
            summary["checked"] += 1
            try:
                result = self.evaluate_signal(signal, now=now)
                for key in ("expired", "entry_triggered", "tp1_hit", "tp2_hit", "tp3_hit", "sl_hit", "ambiguous", "invalidated"):
                    summary[key] += int(result.get(key, 0) or 0)
            except Exception as exc:
                summary["errors"].append({"signal_id": signal.get("id"), "reason": str(exc)})
        return summary

    def evaluate_signal(self, signal: dict[str, Any], now: datetime | None = None) -> dict[str, int]:
        now = now or datetime.now(timezone.utc)
        if str(signal.get("status") or "").upper() in TERMINAL_STATUSES:
            return {}
        expires_at = parse_utc(signal.get("expires_at"))
        if not expires_at:
            return {}

        perf = ensure_signal_performance(signal, self.db)
        if signal.get("status") == SIGNAL_STATUS_PENDING_ENTRY and expires_at <= now:
            self._update_perf(
                signal["id"],
                {"expired": 1, "result": PERFORMANCE_RESULT_EXPIRED, "closed_at": utc_now_iso()},
            )
            update_trading_signal(signal["id"], {"status": SIGNAL_STATUS_EXPIRED}, db=self.db)
            return {"expired": 1}

        market_data = self.market_data or BinanceKlineMarketData()
        candles = normalize_candles(market_data.fetch_candles(signal["symbol"], signal.get("timeframe") or "1h", self.candle_limit))
        if not candles:
            return {}

        changes = {
            "expired": 0,
            "entry_triggered": 0,
            "tp1_hit": 0,
            "tp2_hit": 0,
            "tp3_hit": 0,
            "sl_hit": 0,
            "ambiguous": 0,
            "invalidated": 0,
        }
        entry_seen = bool(perf.get("entry_triggered")) or signal.get("status") in {
            SIGNAL_STATUS_ACTIVE,
            SIGNAL_STATUS_TP1_HIT,
            SIGNAL_STATUS_TP2_HIT,
        }
        hit_state = {
            1: bool(perf.get("tp1_hit")) or signal.get("status") in {SIGNAL_STATUS_TP1_HIT, SIGNAL_STATUS_TP2_HIT, SIGNAL_STATUS_TP3_HIT},
            2: bool(perf.get("tp2_hit")) or signal.get("status") in {SIGNAL_STATUS_TP2_HIT, SIGNAL_STATUS_TP3_HIT},
            3: bool(perf.get("tp3_hit")) or signal.get("status") == SIGNAL_STATUS_TP3_HIT,
        }
        max_favorable, max_adverse = _movement_from_entry(signal, candles)

        for candle in candles:
            if not entry_seen:
                pre_entry_result = _pending_entry_invalidation(signal, candle, candles)
                if pre_entry_result:
                    return self._close_pending_signal(signal, pre_entry_result, max_favorable=max_favorable, max_adverse=max_adverse)
            if not entry_seen and _entry_touched(signal, candle):
                entry_seen = True
                changes["entry_triggered"] = 1
                self._update_perf(signal["id"], {"entry_triggered": 1, "max_favorable_move": max_favorable, "max_adverse_move": max_adverse})
                update_trading_signal(signal["id"], {"status": SIGNAL_STATUS_ACTIVE}, db=self.db)
            if not entry_seen:
                continue

            tp_hits = _tp_hits(signal, candle)
            sl_hit = _sl_hit(signal, candle)
            if sl_hit and tp_hits:
                # Conservative rule: if TP and SL touch inside the same candle, order is unknown.
                # We record AMBIGUOUS instead of presenting a clean win.
                changes["ambiguous"] = 1
                self._update_perf(
                    signal["id"],
                    {
                        "sl_hit": 1,
                        "result": PERFORMANCE_RESULT_AMBIGUOUS,
                        "closed_at": utc_now_iso(),
                        "max_favorable_move": max_favorable,
                        "max_adverse_move": max_adverse,
                    },
                )
                update_trading_signal(signal["id"], {"status": SIGNAL_STATUS_SL_HIT, "sl_hit_at": utc_now_iso()}, db=self.db)
                return changes
            if sl_hit:
                changes["sl_hit"] = 1
                self._update_perf(
                    signal["id"],
                    {
                        "sl_hit": 1,
                        "result": PERFORMANCE_RESULT_LOSS,
                        "closed_at": utc_now_iso(),
                        "max_favorable_move": max_favorable,
                        "max_adverse_move": max_adverse,
                    },
                )
                update_trading_signal(signal["id"], {"status": SIGNAL_STATUS_SL_HIT, "sl_hit_at": utc_now_iso()}, db=self.db)
                return changes
            if tp_hits:
                highest = max(tp_hits)
                for level in sorted(tp_hits):
                    if not hit_state[level]:
                        changes[f"tp{level}_hit"] = 1
                        hit_state[level] = True
                self._mark_tp(signal["id"], highest, max_favorable=max_favorable, max_adverse=max_adverse)
                if highest == 3:
                    return changes

        if expires_at <= now:
            changes["expired"] = 1
            if hit_state[2]:
                # TP2 is the minimum threshold for a full WIN. TP1 is partial progress only.
                self._update_perf(
                    signal["id"],
                    {"expired": 1, "result": PERFORMANCE_RESULT_WIN, "closed_at": utc_now_iso()},
                )
            else:
                # TP1-only by expiry is not a full win. Keep TP1_HIT status when present,
                # but close performance as EXPIRED because PARTIAL_WIN is not in the V1 enum.
                self._update_perf(
                    signal["id"],
                    {"expired": 1, "result": PERFORMANCE_RESULT_EXPIRED, "closed_at": utc_now_iso()},
                )
                if not hit_state[1]:
                    update_trading_signal(signal["id"], {"status": SIGNAL_STATUS_EXPIRED}, db=self.db)
        return changes

    def _close_pending_signal(
        self,
        signal: dict[str, Any],
        reason: str,
        *,
        max_favorable: float | None,
        max_adverse: float | None,
    ) -> dict[str, int]:
        now = utc_now_iso()
        if reason == "PRE_ENTRY_STOP_LOSS":
            self._update_perf(
                signal["id"],
                {
                    "sl_hit": 1,
                    "result": PERFORMANCE_RESULT_LOSS,
                    "closed_at": now,
                    "max_favorable_move": max_favorable,
                    "max_adverse_move": max_adverse,
                },
            )
            update_trading_signal(
                signal["id"],
                {
                    "status": SIGNAL_STATUS_SL_HIT,
                    "sl_hit_at": now,
                    "signal_reason": _append_lifecycle_note(signal, "Pre-entry stop loss was touched; signal closed."),
                },
                db=self.db,
            )
            return {"sl_hit": 1}

        self._update_perf(
            signal["id"],
            {
                "invalidated": 1,
                "result": PERFORMANCE_RESULT_INVALIDATED,
                "closed_at": now,
                "max_favorable_move": max_favorable,
                "max_adverse_move": max_adverse,
            },
        )
        update_trading_signal(
            signal["id"],
            {
                "status": SIGNAL_STATUS_INVALIDATED,
                "invalidated_at": now,
                "signal_reason": _append_lifecycle_note(signal, f"{reason}; entry opportunity is no longer valid."),
            },
            db=self.db,
        )
        return {"invalidated": 1}

    def _mark_tp(self, signal_id: str, highest_level: int, *, max_favorable: float | None, max_adverse: float | None) -> None:
        now = utc_now_iso()
        with self.db.connect() as conn:
            signal = conn.execute("SELECT * FROM trading_signals WHERE id = ?", (signal_id,)).fetchone()
        perf_updates: dict[str, Any] = {
            "max_favorable_move": max_favorable,
            "max_adverse_move": max_adverse,
        }
        signal_updates: dict[str, Any] = {"status": f"TP{highest_level}_HIT"}
        for level in range(1, highest_level + 1):
            perf_updates[f"tp{level}_hit"] = 1
            timestamp_field = f"tp{level}_hit_at"
            if not signal or not signal[timestamp_field]:
                signal_updates[timestamp_field] = now
        if highest_level == 3:
            perf_updates.update({"result": PERFORMANCE_RESULT_WIN, "closed_at": now})
        self._update_perf(signal_id, perf_updates)
        update_trading_signal(signal_id, signal_updates, db=self.db)

    def _update_perf(self, signal_id: str, updates: dict[str, Any]) -> None:
        current = get_signal_performance(signal_id, db=self.db)
        safe_updates = dict(updates)
        for field in ("tp1_hit_at", "tp2_hit_at", "tp3_hit_at", "sl_hit_at"):
            if field in safe_updates and current and current.get(field):
                safe_updates.pop(field, None)
        update_signal_performance(signal_id, safe_updates, db=self.db)


def _entry_touched(signal: dict[str, Any], candle: dict[str, Any]) -> bool:
    low = float(candle["low"])
    high = float(candle["high"])
    zone_low = signal.get("entry_zone_low")
    zone_high = signal.get("entry_zone_high")
    if zone_low is not None and zone_high is not None:
        return high >= float(zone_low) and low <= float(zone_high)
    entry = float(signal["entry_price"])
    return low <= entry <= high


def _tp_hits(signal: dict[str, Any], candle: dict[str, Any]) -> list[int]:
    side = str(signal["side"]).upper()
    high = float(candle["high"])
    low = float(candle["low"])
    hits: list[int] = []
    for level in (1, 2, 3):
        target = signal.get(f"take_profit_{level}")
        if target is None:
            continue
        target_f = float(target)
        if side == "BUY" and high >= target_f:
            hits.append(level)
        if side == "SELL" and low <= target_f:
            hits.append(level)
    return hits


def _sl_hit(signal: dict[str, Any], candle: dict[str, Any]) -> bool:
    side = str(signal["side"]).upper()
    stop = float(signal["stop_loss"])
    if side == "BUY":
        return float(candle["low"]) <= stop
    return float(candle["high"]) >= stop


def _pending_entry_invalidation(signal: dict[str, Any], candle: dict[str, Any], candles: list[dict[str, Any]]) -> str | None:
    if _sl_hit(signal, candle):
        return "PRE_ENTRY_STOP_LOSS"
    if _tp_hits(signal, candle):
        return "PRE_ENTRY_TP_ALREADY_HIT"
    if _volatility_spike(signal, candle, candles):
        return "VOLATILITY_SPIKE"
    if _entry_drift_too_far(signal, candle, candles):
        return "ENTRY_DRIFT_TOO_FAR"
    return None


def _entry_drift_too_far(signal: dict[str, Any], candle: dict[str, Any], candles: list[dict[str, Any]]) -> bool:
    entry = float(signal.get("entry_price") or 0)
    stop = float(signal.get("stop_loss") or 0)
    if entry <= 0 or stop <= 0:
        return False
    risk = abs(entry - stop)
    atr = _average_range(candles)
    max_drift = min(0.5 * risk, 0.75 * atr) if atr > 0 else 0.5 * risk
    if max_drift <= 0:
        return False
    close = float(candle["close"])
    side = str(signal["side"]).upper()
    if side == "BUY":
        return close > entry + max_drift
    return close < entry - max_drift


def _volatility_spike(signal: dict[str, Any], candle: dict[str, Any], candles: list[dict[str, Any]]) -> bool:
    close = float(candle.get("close") or 0)
    if close <= 0:
        return False
    latest_range_pct = (float(candle["high"]) - float(candle["low"])) / close
    previous_candles = [c for c in candles if c is not candle]
    average_range = _average_range(previous_candles)
    entry = float(signal.get("entry_price") or close)
    average_range_pct = average_range / entry if entry > 0 and average_range > 0 else 0.0
    if average_range_pct <= 0:
        return latest_range_pct > 0.05
    return latest_range_pct > max(0.05, 2.5 * average_range_pct)


def _average_range(candles: list[dict[str, Any]], lookback: int = 14) -> float:
    sample = candles[-lookback:] if len(candles) >= lookback else candles
    if not sample:
        return 0.0
    return sum(float(c["high"]) - float(c["low"]) for c in sample) / len(sample)


def _append_lifecycle_note(signal: dict[str, Any], note: str) -> str:
    existing = str(signal.get("signal_reason") or "").strip()
    suffix = f" Lifecycle update: {note}"
    return f"{existing}{suffix}" if existing else suffix.strip()


def _movement_from_entry(signal: dict[str, Any], candles: list[dict[str, Any]]) -> tuple[float | None, float | None]:
    entry = float(signal.get("entry_price") or 0)
    if entry <= 0 or not candles:
        return None, None
    side = str(signal["side"]).upper()
    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]
    if side == "BUY":
        favorable = max(highs) - entry
        adverse = entry - min(lows)
    else:
        favorable = entry - min(lows)
        adverse = max(highs) - entry
    return round(favorable, 8), round(adverse, 8)


def update_signal_performance_statuses(db: DB | None = None, market_data: Any | None = None) -> dict[str, Any]:
    return SignalPerformanceUpdater(db=db, market_data=market_data).update_signal_performance()
