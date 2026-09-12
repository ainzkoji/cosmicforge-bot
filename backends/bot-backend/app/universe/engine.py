"""UniverseEngine -- broker instruments -> eligible -> ranked -> active shortlist.

Two stages, so breadth never costs a per-symbol request:

* **A. Discovery.** Instrument metadata from the adapter, cached for
  ``metadata_ttl_seconds`` (an hour by default). Never fetched per cycle.
* **B. Cheap ranking.** Batched market statistics (two requests for the whole
  venue), cached for ``stats_ttl_seconds``. Hard eligibility, then market
  quality, then a deterministic rank; the top ``active_limit`` become new-entry
  candidates.

The full Master Ensemble runs only on that shortlist, and only when a symbol has
a genuinely new closed candle -- that part lives in the runner.

Failure is never silent and never blocking. A failed or rate-limited refresh
backs off exponentially and serves the last good data while it is still usable,
flagged ``stale``; with nothing usable the engine returns no candidates, which
stops new entries and nothing else. Open positions are managed by the runner
whatever the universe says.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Mapping

from app.universe.adapters import UniverseAdapter
from app.universe.contracts import (
    Exclusion,
    InstrumentMeta,
    MarketStats,
    Product,
    UniverseMember,
    UniverseMode,
    UniverseSnapshot,
)

logger = logging.getLogger(__name__)

_DAY_MS = 86_400_000


@dataclass(frozen=True)
class UniverseConfig:
    active_limit: int = 100
    min_quote_volume_usdt: float = 50_000_000.0
    max_spread_bps: float = 10.0
    #: The 4h higher-timeframe EMA200 needs 200 closed 4h candles (33.3 days).
    min_listing_days: float = 35.0
    #: A 24h ticker whose last trade is older than this marks a halted market.
    max_stats_age_seconds: int = 900
    metadata_ttl_seconds: int = 3600
    stats_ttl_seconds: int = 900
    #: Serve cached statistics up to this many TTLs old when a refresh fails.
    stale_stats_multiplier: float = 4.0
    backoff_initial_seconds: int = 30
    backoff_max_seconds: int = 900
    settlement_assets: tuple[str, ...] = ("USDT",)
    allowed_underlying_types: tuple[str, ...] = ("COIN",)
    #: Largest notional one position of this bot can carry; None = not checked.
    max_position_notional: float | None = None

    @classmethod
    def from_settings(cls, settings: Any, *, max_position_notional: float | None = None) -> "UniverseConfig":
        def g(name: str, default: Any) -> Any:
            value = getattr(settings, name, default)
            return default if value is None or value == "" else value

        return cls(
            active_limit=int(g("UNIVERSE_ACTIVE_LIMIT", cls.active_limit)),
            min_quote_volume_usdt=float(g("UNIVERSE_MIN_QUOTE_VOLUME_USDT", cls.min_quote_volume_usdt)),
            max_spread_bps=float(g("UNIVERSE_MAX_SPREAD_BPS", cls.max_spread_bps)),
            min_listing_days=float(g("UNIVERSE_MIN_LISTING_DAYS", cls.min_listing_days)),
            max_stats_age_seconds=int(g("UNIVERSE_MAX_STATS_AGE_SECONDS", cls.max_stats_age_seconds)),
            metadata_ttl_seconds=int(g("UNIVERSE_METADATA_TTL_SECONDS", cls.metadata_ttl_seconds)),
            stats_ttl_seconds=int(g("UNIVERSE_STATS_TTL_SECONDS", cls.stats_ttl_seconds)),
            backoff_initial_seconds=int(g("UNIVERSE_BACKOFF_INITIAL_SECONDS", cls.backoff_initial_seconds)),
            backoff_max_seconds=int(g("UNIVERSE_BACKOFF_MAX_SECONDS", cls.backoff_max_seconds)),
            settlement_assets=tuple(
                a.strip().upper() for a in str(g("UNIVERSE_SETTLEMENT_ASSETS", "USDT")).split(",") if a.strip()
            ),
            allowed_underlying_types=tuple(
                a.strip().upper() for a in str(g("UNIVERSE_UNDERLYING_TYPES", "COIN")).split(",") if a.strip()
            ),
            max_position_notional=max_position_notional,
        )

    def to_dict(self) -> dict[str, Any]:
        from dataclasses import asdict

        return asdict(self)


def _is_rate_limit(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in (" 429", "429:", "http 429", " 418", "418:", "-1003", "too many requests", "rate limit"))


def hard_exclusion(
    meta: InstrumentMeta, stats: MarketStats | None, now_ms: int, config: UniverseConfig
) -> str | None:
    """Why this instrument cannot be traded by this bot at all, or ``None``."""
    if meta.product == Product.SPOT:
        return Exclusion.NOT_FUTURES
    if meta.product != Product.PERPETUAL:
        return Exclusion.UNSUPPORTED_CONTRACT
    if not meta.tradable:
        return Exclusion.NOT_TRADING
    if meta.margin_asset not in config.settlement_assets or meta.quote_asset not in config.settlement_assets:
        return Exclusion.QUOTE_UNSUPPORTED
    if config.allowed_underlying_types and (meta.underlying_type or "") not in config.allowed_underlying_types:
        return Exclusion.UNSUPPORTED_CONTRACT
    if not meta.tick_size or not meta.step_size or not meta.min_qty:
        return Exclusion.INVALID_INSTRUMENT_FILTERS
    if meta.listed_at_ms and now_ms - int(meta.listed_at_ms) < config.min_listing_days * _DAY_MS:
        return Exclusion.INSUFFICIENT_HISTORY
    if stats is not None:
        if stats.last_price is None or stats.last_price <= 0:
            return Exclusion.INVALID_PRICE
        if stats.stats_time_ms and now_ms - int(stats.stats_time_ms) > config.max_stats_age_seconds * 1000:
            return Exclusion.STALE_DATA
        cap = config.max_position_notional
        if cap and cap > 0:
            if meta.min_notional and meta.min_notional > cap:
                return Exclusion.MIN_NOTIONAL_UNSUPPORTED
            if meta.min_qty * stats.last_price > cap:
                return Exclusion.MIN_NOTIONAL_UNSUPPORTED
    return None


def quality_exclusion(stats: MarketStats | None, config: UniverseConfig) -> str | None:
    """Why a tradable instrument is not worth an entry today, or ``None``."""
    if stats is None or stats.quote_volume_24h is None:
        return Exclusion.MARKET_STATS_UNKNOWN
    if stats.quote_volume_24h < config.min_quote_volume_usdt:
        return Exclusion.LOW_LIQUIDITY
    if config.max_spread_bps > 0:
        if stats.spread_bps is None:
            return Exclusion.MARKET_STATS_UNKNOWN
        if stats.spread_bps > config.max_spread_bps:
            return Exclusion.SPREAD_TOO_WIDE
    return None


def rank_key(meta: InstrumentMeta, stats: MarketStats) -> tuple:
    """Deterministic: most traded first, tighter spread next, symbol last."""
    return (
        -(stats.quote_volume_24h or 0.0),
        stats.spread_bps if stats.spread_bps is not None else float("inf"),
        meta.venue_symbol,
    )


class UniverseEngine:
    """Cached discovery + eligibility + ranking for one broker account."""

    def __init__(
        self,
        adapter: UniverseAdapter,
        config: UniverseConfig,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.adapter = adapter
        self.config = config
        self._clock = clock
        self._instruments: dict[str, InstrumentMeta] | None = None
        self._instruments_at = 0.0
        self._stats: dict[str, MarketStats] | None = None
        self._stats_at = 0.0
        self._failures = 0
        self._next_attempt_at = 0.0
        self._last_error: str | None = None
        self._rate_limited = False
        self.request_count = 0

    def instrument(self, symbol: str) -> InstrumentMeta | None:
        return (self._instruments or {}).get(str(symbol or "").upper())

    def _load(self, now: float, need_stats: bool) -> None:
        if now < self._next_attempt_at:
            return
        try:
            if self._instruments is None or now - self._instruments_at >= self.config.metadata_ttl_seconds:
                self.request_count += 1
                self._instruments = {m.venue_symbol: m for m in self.adapter.instruments()}
                self._instruments_at = now
            if need_stats and (self._stats is None or now - self._stats_at >= self.config.stats_ttl_seconds):
                self.request_count += 2
                self._stats = dict(self.adapter.market_stats())
                self._stats_at = now
            self._failures = 0
            self._last_error = None
            self._rate_limited = False
        except Exception as exc:
            self._failures += 1
            delay = min(
                float(self.config.backoff_max_seconds),
                float(self.config.backoff_initial_seconds) * (2 ** (self._failures - 1)),
            )
            self._next_attempt_at = now + delay
            text = " ".join(f"{type(exc).__name__}: {exc}".split())[:300]
            self._last_error = text
            self._rate_limited = _is_rate_limit(" " + text)
            logger.warning(
                "[UNIVERSE] refresh failed (attempt %d, next in %.0fs, rate_limited=%s): %s",
                self._failures, delay, self._rate_limited, text,
            )

    def refresh(
        self,
        *,
        broker_account_id: str,
        mode: str = UniverseMode.BROKER,
        allowlist: Iterable[str] = (),
    ) -> UniverseSnapshot:
        mode = UniverseMode.normalize(mode) or UniverseMode.BROKER
        now = self._clock()
        now_ms = int(now * 1000)
        self._load(now, need_stats=True)
        generated_at = datetime.fromtimestamp(now, tz=timezone.utc).isoformat()
        caps = dict(self.adapter.capabilities())

        instruments = self._instruments or {}
        if not instruments:
            return UniverseSnapshot(
                broker_account_id=broker_account_id, venue=self.adapter.venue, mode=mode,
                generated_at=generated_at, discovered_count=0, eligible_count=0, ranked_count=0,
                active=(), excluded={}, stale=True,
                error=self._last_error or "INSTRUMENT_METADATA_UNAVAILABLE", capabilities=caps,
            )

        stats_age = (now - self._stats_at) if self._stats is not None else None
        stats_usable = (
            self._stats is not None
            and stats_age is not None
            and stats_age <= self.config.stats_ttl_seconds * self.config.stale_stats_multiplier
        )
        stats: Mapping[str, MarketStats] = self._stats if stats_usable else {}
        stale = bool(self._last_error) or (
            stats_age is not None and stats_age > self.config.stats_ttl_seconds
        ) or not stats_usable
        # A refresh failure may use the last good batch for the explicitly
        # bounded stale-cache window.  Validate each ticker as of the time the
        # batch was received; validating it against ``now`` would immediately
        # turn every member into STALE_DATA and defeat the fallback.  The
        # snapshot remains ``stale=True`` and carries the refresh error.
        eligibility_now_ms = (
            int(self._stats_at * 1000)
            if self._last_error and stats_usable and self._stats is not None
            else now_ms
        )
        unknown_reason = Exclusion.RATE_LIMIT_DEFERRED if self._rate_limited else Exclusion.MARKET_STATS_UNKNOWN

        allow = [str(s).strip().upper() for s in allowlist if str(s).strip()]
        allow_set = set(allow)
        excluded: dict[str, str] = {}
        eligible: list[InstrumentMeta] = []
        for symbol in sorted(instruments):
            meta = instruments[symbol]
            if mode == UniverseMode.ALLOWLIST and symbol not in allow_set:
                excluded[symbol] = Exclusion.USER_ALLOWLIST_EXCLUDED
                continue
            reason = hard_exclusion(meta, stats.get(symbol), eligibility_now_ms, self.config)
            if reason:
                excluded[symbol] = reason
            else:
                eligible.append(meta)
        for symbol in allow:
            if symbol not in instruments:
                excluded[symbol] = Exclusion.NOT_TRADING

        active: list[UniverseMember] = []
        ranked_count = 0
        if mode == UniverseMode.ALLOWLIST:
            by_symbol = {m.venue_symbol: m for m in eligible}
            ordered = [by_symbol[s] for s in allow if s in by_symbol]
            ranked_count = len(ordered)
            for i, meta in enumerate(ordered, start=1):
                active.append(self._member(meta, stats.get(meta.venue_symbol), i, "USER_ALLOWLIST"))
        else:
            ranked: list[tuple[InstrumentMeta, MarketStats]] = []
            for meta in eligible:
                st = stats.get(meta.venue_symbol)
                reason = quality_exclusion(st, self.config) if stats_usable else unknown_reason
                if reason:
                    excluded[meta.venue_symbol] = reason
                else:
                    ranked.append((meta, st))
            ranked.sort(key=lambda pair: rank_key(*pair))
            ranked_count = len(ranked)
            limit = max(0, int(self.config.active_limit))
            for i, (meta, st) in enumerate(ranked, start=1):
                if i <= limit:
                    active.append(self._member(meta, st, i, "RANKED_BY_QUOTE_VOLUME"))
                else:
                    excluded[meta.venue_symbol] = Exclusion.RANK_BELOW_ACTIVE_LIMIT

        return UniverseSnapshot(
            broker_account_id=broker_account_id,
            venue=self.adapter.venue,
            mode=mode,
            generated_at=generated_at,
            discovered_count=len(instruments),
            eligible_count=len(eligible),
            ranked_count=ranked_count,
            active=tuple(active),
            excluded=dict(sorted(excluded.items())),
            stale=stale,
            error=self._last_error,
            metadata_age_seconds=round(now - self._instruments_at, 3) if self._instruments is not None else None,
            stats_age_seconds=round(stats_age, 3) if stats_age is not None else None,
            capabilities=caps,
        )

    @staticmethod
    def _member(meta: InstrumentMeta, stats: MarketStats | None, rank: int, reason: str) -> UniverseMember:
        return UniverseMember(
            symbol=meta.venue_symbol,
            rank=rank,
            canonical_id=meta.canonical.canonical_id,
            underlying=meta.canonical.underlying,
            quote_volume_24h=stats.quote_volume_24h if stats else None,
            spread_bps=round(stats.spread_bps, 6) if (stats and stats.spread_bps is not None) else None,
            trade_count_24h=stats.trade_count_24h if stats else None,
            last_price=stats.last_price if stats else None,
            selection_reason=reason,
        )


__all__ = ["UniverseConfig", "UniverseEngine", "hard_exclusion", "quality_exclusion", "rank_key"]
