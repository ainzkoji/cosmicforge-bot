from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from app.core.config import settings
from app.exchange.binance.client import BinanceFuturesClient
from app.signals.pair_discovery import DEFAULT_SEED_CRYPTO_SYMBOLS, TIER_1_CRYPTO_SYMBOLS, TIER_2_CRYPTO_SYMBOLS
from app.signals.signal_repository import SignalRepository
from app.signals.signal_scheduler_config import DEFAULT_MAX_ACTIVE_SIGNALS, DEFAULT_MAX_PUBLISHED_PER_SCAN

logger = logging.getLogger(__name__)
from app.signals.signal_risk import (
    calculate_entry_zone,
    calculate_dynamic_validity_minutes,
    calculate_risk_reward,
    calculate_stop_loss,
    calculate_take_profits,
    validate_signal_quality,
    validate_signal_prices,
)
from app.signals.signal_scoring import calculate_market_features, score_signal
from shared_lib.persistence.db import utc_now_iso
from shared_lib.persistence.signals import (
    SIGNAL_STATUS_PENDING_ENTRY,
    SOURCE_INTERNAL_SIGNAL_ENGINE,
)

SOURCE_DEV_MOCK_SIGNAL_ENGINE = "dev_mock_signal_engine"


ALLOWED_CRYPTO_SYMBOLS = DEFAULT_SEED_CRYPTO_SYMBOLS
SUPPORTED_TIMEFRAMES = {"1m", "5m", "15m", "30m", "1h", "4h"}
MIN_CANDLES = 50
DEFAULT_CANDLE_LIMIT = 120
MIN_CONFIDENCE = 70.0
MIN_RISK_REWARD = 1.8


class MarketDataClient(Protocol):
    def klines(self, symbol: str, interval: str = "1m", limit: int = 100) -> list[Any]:
        ...


class BinanceKlineMarketData:
    """Read-only candle adapter. It never places or manages orders."""

    def __init__(self, client: MarketDataClient | None = None):
        self.client = client or BinanceFuturesClient(
            api_key=settings.BINANCE_API_KEY,
            api_secret=settings.BINANCE_API_SECRET,
            base_url=settings.BINANCE_FAPI_BASE_URL,
            recv_window=settings.BINANCE_RECV_WINDOW,
        )

    def fetch_candles(self, symbol: str, timeframe: str, limit: int) -> list[Any]:
        return self.client.klines(symbol=symbol, interval=timeframe, limit=limit)


class MarketDataRunCache:
    """Small in-memory candle cache for a single generation run."""

    def __init__(self, market_data: Any | None = None):
        self.market_data = market_data or BinanceKlineMarketData()
        self._klines: dict[tuple[str, str, int], list[Any]] = {}

    def fetch_candles(self, symbol: str, timeframe: str, limit: int) -> list[Any]:
        key = (symbol.upper(), timeframe, int(limit))
        if key not in self._klines:
            self._klines[key] = self.market_data.fetch_candles(symbol, timeframe, limit)
        return self._klines[key]

    def clear(self) -> None:
        self._klines.clear()


class CryptoSignalEngine:
    def __init__(
        self,
        *,
        repository: SignalRepository | None = None,
        market_data: Any | None = None,
        allowed_symbols: list[str] | tuple[str, ...] | None = None,
        timeframe: str = "1h",
        candle_limit: int = DEFAULT_CANDLE_LIMIT,
        dev_mode: bool = False,
        defer_publish: bool = False,
        max_published_per_scan: int = DEFAULT_MAX_PUBLISHED_PER_SCAN,
        max_active_signals: int = DEFAULT_MAX_ACTIVE_SIGNALS,
    ):
        self.repository = repository or SignalRepository()
        self.market_data = market_data or BinanceKlineMarketData()
        self.allowed_symbols = tuple(symbol.upper() for symbol in (allowed_symbols or ALLOWED_CRYPTO_SYMBOLS))
        self.timeframe = timeframe
        self.candle_limit = int(candle_limit)
        self.dev_mode = bool(dev_mode)
        self.defer_publish = bool(defer_publish)
        self.max_published_per_scan = int(max_published_per_scan)
        self.max_active_signals = int(max_active_signals)

    def generate_crypto_signals(self, symbols: list[str] | tuple[str, ...] | None = None) -> dict[str, Any]:
        selected_symbols = tuple(symbol.upper() for symbol in (symbols or self.allowed_symbols))
        summary = {
            "scanned_symbols": 0,
            "candidates_created": 0,
            "accepted": 0,
            "rejected": 0,
            "signals_created": 0,
            "published": 0,
            "skipped_active_limit": 0,
            "skipped_published_limit": 0,
            "errors": [],
        }
        logger.info(
            "[SIGNAL_GENERATION_JOB_STARTED] SIGNAL_SCAN_MAX_PUBLISHED_LIMIT=%d SIGNAL_ACTIVE_LIMIT=%d symbols=%d",
            self.max_published_per_scan,
            self.max_active_signals,
            len(selected_symbols),
        )
        for symbol in selected_symbols:
            active_count = self.repository.count_active_signals()
            if active_count >= self.max_active_signals:
                logger.info(
                    "[SIGNAL_SKIPPED_MAX_ACTIVE_LIMIT] active=%d limit=%d — stopping scan",
                    active_count,
                    self.max_active_signals,
                )
                summary["skipped_active_limit"] += len(selected_symbols) - summary["scanned_symbols"]
                break
            if summary["published"] >= self.max_published_per_scan:
                logger.info(
                    "[SIGNAL_SKIPPED_MAX_PUBLISHED_LIMIT] published_this_scan=%d limit=%d — stopping scan",
                    summary["published"],
                    self.max_published_per_scan,
                )
                summary["skipped_published_limit"] += len(selected_symbols) - summary["scanned_symbols"]
                break
            summary["scanned_symbols"] += 1
            try:
                result = self.scan_symbol(symbol)
            except Exception as exc:
                logger.warning("[SIGNAL_GENERATION_JOB_FAILED] symbol=%s error=%s", symbol, exc)
                summary["errors"].append({"symbol": symbol, "error": str(exc)})
                continue
            for item in result:
                summary["candidates_created"] += 1
                if item.get("accepted"):
                    summary["accepted"] += 1
                else:
                    summary["rejected"] += 1
                if item.get("signal_id"):
                    summary["signals_created"] += 1
                if item.get("auto_published"):
                    summary["published"] += 1
        logger.info(
            "[SIGNAL_GENERATION_JOB_STARTED] scan complete: scanned=%d published=%d accepted=%d rejected=%d errors=%d",
            summary["scanned_symbols"],
            summary["published"],
            summary["accepted"],
            summary["rejected"],
            len(summary["errors"]),
        )
        return summary

    def scan_symbol(self, symbol: str) -> list[dict[str, Any]]:
        symbol = symbol.upper()
        if symbol not in self.allowed_symbols:
            return [self._reject_stub(symbol=symbol, side="BUY", reason="UNSUPPORTED_SYMBOL")]
        if self.timeframe not in SUPPORTED_TIMEFRAMES:
            return [self._reject_stub(symbol=symbol, side="BUY", reason="UNSUPPORTED_TIMEFRAME")]
        candles = self.fetch_candles(symbol)
        return [
            self.generate_candidate_for_side(symbol, "BUY", candles),
            self.generate_candidate_for_side(symbol, "SELL", candles),
        ]

    def fetch_candles(self, symbol: str) -> list[dict[str, Any]]:
        raw = self.market_data.fetch_candles(symbol, self.timeframe, self.candle_limit)
        return normalize_candles(raw)

    def generate_candidate_for_side(
        self,
        symbol: str,
        side: str,
        candles: list[dict[str, Any]] | list[Any],
    ) -> dict[str, Any]:
        symbol = symbol.upper()
        side = side.upper()
        base = {
            "asset_class": "crypto",
            "symbol": symbol,
            "side": side,
            "timeframe": self.timeframe,
            "strategy_name": "crypto_signal_center_v1",
            "source": SOURCE_DEV_MOCK_SIGNAL_ENGINE if self.dev_mode else SOURCE_INTERNAL_SIGNAL_ENGINE,
            "dev_mode": int(self.dev_mode),
        }
        if symbol not in self.allowed_symbols:
            return self._save_rejected(base, "UNSUPPORTED_SYMBOL")
        if side not in {"BUY", "SELL"}:
            return self._save_rejected(base, "INVALID_SIDE")
        if self.timeframe not in SUPPORTED_TIMEFRAMES:
            return self._save_rejected(base, "UNSUPPORTED_TIMEFRAME")

        normalized = normalize_candles(candles)
        if len(normalized) < MIN_CANDLES:
            return self._save_rejected(base, "INSUFFICIENT_MARKET_DATA")
        if not is_market_data_fresh(normalized[-1], self.timeframe):
            return self._save_rejected(base, "STALE_MARKET_DATA")

        features = calculate_market_features(normalized)
        if features["atr_pct"] > 0.10:
            return self._save_rejected(
                {
                    **base,
                    **_partial_candidate_from_features(features),
                    "signal_reason": (
                        f"Rejected because ATR volatility was {features['atr_pct'] * 100:.2f}%, "
                        "above the maximum allowed range for a clean manual signal."
                    ),
                },
                "VOLATILITY_TOO_HIGH",
            )

        try:
            candidate = self._build_candidate(base, features)
        except ValueError as exc:
            return self._save_rejected(base, str(exc))

        valid_prices, price_reason = validate_signal_prices(candidate, atr=features["atr"])
        if not valid_prices:
            candidate["signal_reason"] = f"Rejected because price validation failed: {price_reason or 'INVALID_TP'}."
            return self._save_rejected(candidate, price_reason or "INVALID_TP")
        if candidate["risk_reward"] < MIN_RISK_REWARD:
            candidate["signal_reason"] = (
                f"Rejected because risk/reward was {candidate['risk_reward']:.2f}, "
                f"below the required {MIN_RISK_REWARD:.2f} minimum."
            )
            return self._save_rejected(candidate, "RISK_REWARD_TOO_LOW")

        confidence_score, score_details = score_signal(
            side=side,
            features=features,
            risk_reward=candidate["risk_reward"],
        )
        candidate["confidence_score"] = confidence_score
        candidate["signal_reason"] = build_signal_reason(side=side, features=features, score_details=score_details)
        if candidate.get("validity_reason"):
            candidate["signal_reason"] = f"{candidate['signal_reason']} Entry validity: {candidate['validity_reason']}"
        if self.dev_mode:
            candidate["signal_reason"] = f"DEV/TEST signal for UI/backend validation only. {candidate['signal_reason']}"
        quality = validate_signal_quality(
            candidate,
            atr=features["atr"],
            atr_pct=features["atr_pct"],
            min_confidence=MIN_CONFIDENCE,
            min_risk_reward=MIN_RISK_REWARD,
        )
        if not quality["valid"]:
            reason = quality["reason"]
            if reason == "LOW_CONFIDENCE" and score_details["trend_alignment"] < 20:
                reason = "NO_CLEAR_TREND"
            candidate["signal_reason"] = (
                f"Rejected because {reason}: "
                + " ".join(quality.get("notes") or [])
            )
            return self._save_rejected(candidate, reason)
        if quality.get("notes"):
            intraday_notes = " ".join(note for note in quality["notes"] if "Estimated TP2 duration" in note)
            if intraday_notes:
                candidate["signal_reason"] = f"{candidate['signal_reason']} Intraday duration: {intraday_notes}"

        if self.repository.has_duplicate_open_signal(symbol, side, asset_class="crypto"):
            candidate["signal_reason"] = f"Rejected because an open unpublished or active {side} signal already exists for {symbol}."
            return self._save_rejected(candidate, "DUPLICATE_SIGNAL")

        candidate_id = self.repository.save_accepted_candidate(candidate)
        if self.dev_mode or self.defer_publish or int(candidate.get("dev_mode", 0) or 0) == 1:
            signal_id = self.repository.create_unpublished_signal_from_candidate(candidate, candidate_id=candidate_id)
            auto_published = False
        else:
            signal_id = self.repository.create_published_signal_from_candidate(candidate, candidate_id=candidate_id)
            auto_published = True
        return {
            "symbol": symbol,
            "side": side,
            "accepted": True,
            "candidate_id": candidate_id,
            "signal_id": signal_id,
            "auto_published": auto_published,
            "confidence_score": confidence_score,
            "risk_reward": candidate["risk_reward"],
        }

    def _build_candidate(self, base: dict[str, Any], features: dict[str, Any]) -> dict[str, Any]:
        entry = round(float(features["close"]), 8)
        zone_low, zone_high = calculate_entry_zone(entry, features["atr"])
        stop = calculate_stop_loss(
            side=base["side"],
            entry_price=entry,
            recent_swing_low=float(features["recent_swing_low"]),
            recent_swing_high=float(features["recent_swing_high"]),
            atr=float(features["atr"]),
        )
        tp1, tp2, tp3 = calculate_take_profits(side=base["side"], entry_price=entry, stop_loss=stop)
        rr = calculate_risk_reward(entry_price=entry, stop_loss=stop, take_profit_2=tp2)
        now = datetime.now(timezone.utc)
        validity = calculate_dynamic_validity_minutes(
            timeframe=base.get("timeframe"),
            atr=float(features["atr"]),
            entry_price=entry,
            entry_zone_low=zone_low,
            entry_zone_high=zone_high,
            stop_loss=stop,
            recent_volatility=float(features.get("atr_pct") or 0.0),
            trend_strength=abs(float(features.get("ema_fast", entry)) - float(features.get("ema_slow", entry))) / entry
            if entry > 0
            else None,
        )
        validity_minutes = int(validity["validity_minutes"])
        return {
            **base,
            "entry_price": entry,
            "entry_zone_low": zone_low,
            "entry_zone_high": zone_high,
            "stop_loss": stop,
            "take_profit_1": tp1,
            "take_profit_2": tp2,
            "take_profit_3": tp3,
            "risk_reward": rr,
            "confidence_score": 0.0,
            "status": "CANDIDATE",
            "signal_status": SIGNAL_STATUS_PENDING_ENTRY,
            "signal_validity_minutes": validity_minutes,
            "validity_reason": validity["reason"],
            "expires_at": (now + timedelta(minutes=validity_minutes)).isoformat(),
        }

    def _save_rejected(self, candidate: dict[str, Any], reason: str) -> dict[str, Any]:
        candidate_id = self.repository.save_rejected_candidate(candidate, reason)
        return {
            "symbol": candidate.get("symbol"),
            "side": candidate.get("side"),
            "accepted": False,
            "candidate_id": candidate_id,
            "rejection_reason": reason,
        }

    def _reject_stub(self, *, symbol: str, side: str, reason: str) -> dict[str, Any]:
        return self._save_rejected(
            {
                "asset_class": "crypto",
                "symbol": symbol,
                "side": side,
                "timeframe": self.timeframe,
                "strategy_name": "crypto_signal_center_v1",
                "source": SOURCE_DEV_MOCK_SIGNAL_ENGINE if self.dev_mode else SOURCE_INTERNAL_SIGNAL_ENGINE,
                "dev_mode": int(self.dev_mode),
            },
            reason,
        )


def generate_crypto_signals(**kwargs: Any) -> dict[str, Any]:
    scheduled_time_utc = kwargs.pop("scheduled_time_utc", None)
    scheduler_source = kwargs.pop("scheduler_source", None)

    # Section F-1 readiness evidence: when called by the scheduler, emit audit proof events.
    if scheduled_time_utc or scheduler_source:
        from shared_lib.persistence.db import DB
        from shared_lib.persistence.audit import Audit

        db = DB()
        audit = Audit(db=db)
        try:
            audit.event(
                event_type="SIGNAL_GENERATION_STARTED",
                details={"scheduled_time_utc": scheduled_time_utc, "source": scheduler_source or "unknown"},
            )
        except Exception:
            pass

        try:
            result = CryptoSignalEngine(**kwargs).generate_crypto_signals()
            try:
                audit.event(
                    event_type="SIGNAL_GENERATION_COMPLETED",
                    details={
                        "scheduled_time_utc": scheduled_time_utc,
                        "source": scheduler_source or "unknown",
                        "result_keys": list(result.keys()) if isinstance(result, dict) else None,
                    },
                )
            except Exception:
                pass
            return result
        except Exception as exc:
            try:
                audit.event(
                    event_type="SIGNAL_GENERATION_FAILED",
                    details={
                        "scheduled_time_utc": scheduled_time_utc,
                        "source": scheduler_source or "unknown",
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                )
            except Exception:
                pass
            raise

    return CryptoSignalEngine(**kwargs).generate_crypto_signals()


def normalize_candles(raw: list[Any]) -> list[dict[str, Any]]:
    candles: list[dict[str, Any]] = []
    for item in raw or []:
        if isinstance(item, dict):
            timestamp = item.get("timestamp") or item.get("open_time") or item.get("openTime") or item.get("time")
            candles.append(
                {
                    "timestamp": timestamp,
                    "open": float(item["open"]),
                    "high": float(item["high"]),
                    "low": float(item["low"]),
                    "close": float(item["close"]),
                    "volume": float(item.get("volume", 0.0)),
                }
            )
        else:
            candles.append(
                {
                    "timestamp": item[0] if len(item) > 0 else None,
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]) if len(item) > 5 else 0.0,
                }
            )
    return candles


def is_market_data_fresh(candle: dict[str, Any], timeframe: str) -> bool:
    timestamp = candle.get("timestamp")
    if timestamp is None:
        return True
    try:
        if isinstance(timestamp, (int, float)):
            ts = float(timestamp)
            candle_time = datetime.fromtimestamp(ts / 1000 if ts > 10_000_000_000 else ts, tz=timezone.utc)
        else:
            candle_time = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
            if candle_time.tzinfo is None:
                candle_time = candle_time.replace(tzinfo=timezone.utc)
    except Exception:
        return False
    max_age_seconds = _timeframe_seconds(timeframe) * 5
    return (datetime.now(timezone.utc) - candle_time.astimezone(timezone.utc)).total_seconds() <= max_age_seconds


def build_signal_reason(*, side: str, features: dict[str, Any], score_details: dict[str, Any]) -> str:
    direction = "bullish" if side == "BUY" else "bearish"
    component_summary = (
        f"score={score_details['confidence_score']:.2f} "
        f"(trend={score_details['trend_alignment']}, structure={score_details['market_structure']}, "
        f"momentum={score_details['momentum_confirmation']}, volatility={score_details['volatility_suitability']}, "
        f"risk_reward={score_details['risk_reward_quality']}, event_news={score_details['event_news_safety']}, "
        f"liquidity={score_details['spread_liquidity_quality']})"
    )
    return (
        f"{direction.title()} setup from EMA trend, market structure, momentum, and ATR suitability. "
        f"RSI={features['rsi']:.2f}, ATR%={features['atr_pct'] * 100:.2f}. "
        f"{component_summary}. "
        f"{score_details['event_news_note']} {score_details['liquidity_note']}"
    )


def _timeframe_seconds(timeframe: str) -> int:
    unit = timeframe[-1].lower()
    value = int(timeframe[:-1])
    if unit == "m":
        return value * 60
    if unit == "h":
        return value * 3600
    if unit == "d":
        return value * 86400
    return 60


def _partial_candidate_from_features(features: dict[str, Any]) -> dict[str, Any]:
    return {
        "entry_price": round(float(features.get("close", 0.0)), 8) if features.get("close") else None,
        "confidence_score": 0.0,
        "signal_reason": "Rejected before full candidate build due to unsuitable volatility.",
    }
