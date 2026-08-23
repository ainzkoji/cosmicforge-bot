from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
BOT_BACKEND = ROOT / "backends" / "bot-backend"
SHARED = ROOT / "backends" / "shared"
sys.path.insert(0, str(BOT_BACKEND))
sys.path.insert(0, str(SHARED))

from app.signals.crypto_signal_engine import ALLOWED_CRYPTO_SYMBOLS, CryptoSignalEngine, MarketDataRunCache  # noqa: E402
from app.signals.pair_discovery import PairDiscoveryService  # noqa: E402
from app.signals.signal_scheduler_config import (  # noqa: E402
    DEFAULT_MAX_ACTIVE_SIGNALS,
    DEFAULT_MAX_PUBLISHED_PER_SCAN,
    DEFAULT_MAX_SPREAD,
    DEFAULT_MAX_SYMBOLS,
    DEFAULT_MIN_VOLUME,
    DEFAULT_ROLLOUT_MODE,
    DEFAULT_TIERS,
    LOCK_SIGNAL_GENERATION,
    ROLLOUT_TIER_1_TIER_2_TIER_3,
    ROLLOUT_V1_SEED_ONLY,
    SETTING_ROLLOUT_MODE,
    SETTING_TIER_3_ENABLED,
    SIGNAL_GENERATION_LOCK_TTL_SECONDS,
    tiers_for_rollout_mode,
)
from app.signals.signal_ranking import rank_signal_candidates, select_top_candidates  # noqa: E402
from app.signals.signal_repository import SOURCE_DEV_MOCK_SIGNAL_ENGINE, SignalRepository  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402
from shared_lib.persistence.signals import (  # noqa: E402
    SIGNAL_STATUS_PENDING_ENTRY,
    acquire_signal_operation_lock,
    complete_signal_scan_run,
    create_signal_scan_result,
    create_signal_scan_run,
    fail_signal_scan_run,
    get_eligible_signal_symbols,
    get_signal_setting,
    get_signal_pair,
    get_signal_pair_metrics,
    is_signal_generation_paused,
    release_signal_operation_lock,
)


SUMMARY_FIELDS = (
    "scan_run_id",
    "scanned_symbols",
    "eligible_symbols",
    "skipped_symbols",
    "candidates_created",
    "accepted",
    "rejected",
    "ranked",
    "signals_created",
    "published",
    "not_published_due_to_limits",
    "errors",
)

DEV_SIGNAL_MODE_ENV = "DEV_SIGNAL_MODE"


class DryRunSignalRepository:
    """Repository shim for calculation-only runs; it never writes to the database."""

    def __init__(self):
        self._counter = 0

    def _next_id(self, prefix: str) -> str:
        self._counter += 1
        return f"dry_run_{prefix}_{self._counter}"

    def save_candidate(self, data: dict[str, Any]) -> str:
        return self._next_id("candidate")

    def save_rejected_candidate(self, data: dict[str, Any], rejection_reason: str) -> str:
        return self._next_id("rejected_candidate")

    def save_accepted_candidate(self, data: dict[str, Any]) -> str:
        return self._next_id("accepted_candidate")

    def create_unpublished_signal_from_candidate(
        self,
        candidate_data: dict[str, Any],
        candidate_id: str | None = None,
    ) -> str:
        return self._next_id("unpublished_signal")

    def create_published_signal_from_candidate(
        self,
        candidate_data: dict[str, Any],
        candidate_id: str | None = None,
    ) -> str:
        return self._next_id("published_signal")

    def has_duplicate_open_signal(self, symbol: str, side: str, asset_class: str = "crypto") -> bool:
        return False

    def publish_signal(self, signal_id: str) -> None:
        return None

    def count_active_signals(self, asset_class: str = "crypto") -> int:
        return 0

    def has_active_signal_for_symbol(self, symbol: str, asset_class: str = "crypto") -> bool:
        return False

    def count_published_signals_for_symbol_since(
        self,
        symbol: str,
        since_iso: str,
        asset_class: str = "crypto",
    ) -> int:
        return 0

    def list_recent_generated_signals(self, **kwargs: Any) -> list[dict[str, Any]]:
        return []


def load_dotenv(path: Path = BOT_BACKEND / ".env") -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def parse_symbol_list(value: str | None) -> list[str] | None:
    if not value:
        return None
    symbols = [item.strip().upper() for item in value.split(",") if item.strip()]
    return symbols or None


def is_dev_signal_mode_enabled() -> bool:
    return str(os.getenv(DEV_SIGNAL_MODE_ENV, "")).strip().lower() in {"1", "true", "yes", "on"}


def ensure_dev_signal_mode_allowed(*, requested: bool) -> None:
    if requested and not is_dev_signal_mode_enabled():
        raise RuntimeError("DEV_SIGNAL_MODE must be true before dev/mock signal generation is allowed.")


def normalize_summary(summary: dict[str, Any], *, dry_run: bool = False) -> dict[str, Any]:
    normalized = {
        "scan_run_id": summary.get("scan_run_id"),
        "scanned_symbols": int(summary.get("scanned_symbols", 0) or 0),
        "eligible_symbols": int(summary.get("eligible_symbols", summary.get("scanned_symbols", 0)) or 0),
        "skipped_symbols": int(summary.get("skipped_symbols", 0) or 0),
        "candidates_created": int(summary.get("candidates_created", 0) or 0),
        "accepted": int(summary.get("accepted", 0) or 0),
        "rejected": int(summary.get("rejected", 0) or 0),
        "ranked": int(summary.get("ranked", 0) or 0),
        "signals_created": int(summary.get("signals_created", 0) or 0),
        "published": int(summary.get("published", 0) or 0),
        "not_published_due_to_limits": int(summary.get("not_published_due_to_limits", 0) or 0),
        "errors": list(summary.get("errors", []) or []),
    }
    if dry_run:
        normalized["dry_run"] = True
        normalized["database_writes"] = 0
        normalized["published"] = 0
        normalized["would_publish"] = int(summary.get("published", summary.get("would_publish", 0)) or 0)
    if "ranked_candidates" in summary:
        normalized["ranked_candidates"] = summary["ranked_candidates"]
    for key in ("paused", "lock_not_acquired", "rollout_mode", "scheduled"):
        if key in summary:
            normalized[key] = summary[key]
    return normalized


def _mock_candidate(symbol: str, side: str, *, now: datetime) -> dict[str, Any]:
    symbol = symbol.upper()
    side = side.upper()
    base_prices = {"BTCUSDT": 100000.0, "ETHUSDT": 4000.0}
    entry = base_prices.get(symbol, 100.0)
    risk = entry * 0.01
    if side == "BUY":
        stop = entry - risk
        tp1, tp2, tp3 = entry + (1.5 * risk), entry + (2 * risk), entry + (3 * risk)
    else:
        stop = entry + risk
        tp1, tp2, tp3 = entry - (1.5 * risk), entry - (2 * risk), entry - (3 * risk)
    return {
        "asset_class": "crypto",
        "symbol": symbol,
        "side": side,
        "timeframe": "1h",
        "strategy_name": "crypto_signal_center_dev_mock_v1",
        "entry_price": round(entry, 8),
        "entry_zone_low": round(entry * 0.999, 8),
        "entry_zone_high": round(entry * 1.001, 8),
        "stop_loss": round(stop, 8),
        "take_profit_1": round(tp1, 8),
        "take_profit_2": round(tp2, 8),
        "take_profit_3": round(tp3, 8),
        "risk_reward": 2.0,
        "confidence_score": 80.0,
        "signal_reason": "DEV/TEST signal for UI/backend validation only. Deterministic mock record; not a real market recommendation.",
        "source": SOURCE_DEV_MOCK_SIGNAL_ENGINE,
        "dev_mode": 1,
        "signal_status": SIGNAL_STATUS_PENDING_ENTRY,
        "expires_at": (now + timedelta(hours=4)).isoformat(),
    }


def create_mock_dev_signals(
    *,
    db_path: str | None = None,
    symbols: list[str] | tuple[str, ...] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    ensure_dev_signal_mode_allowed(requested=True)
    selected_symbols = tuple(symbol.upper() for symbol in (symbols or ("BTCUSDT", "ETHUSDT")))
    repository: Any
    db: DB | None = None
    if dry_run:
        repository = DryRunSignalRepository()
    elif db_path:
        migrate(db_path)
        db = DB(path=db_path)
        repository = SignalRepository(db)
    else:
        migrate()
        db = DB()
        repository = SignalRepository(db)

    summary = {
        "scan_run_id": None,
        "scanned_symbols": len(selected_symbols),
        "eligible_symbols": len(selected_symbols),
        "skipped_symbols": 0,
        "candidates_created": 0,
        "accepted": 0,
        "rejected": 0,
        "signals_created": 0,
        "published": 0,
        "errors": [],
    }
    scan_run_id = None
    if not dry_run and db is not None:
        scan_run_id = create_signal_scan_run(
            "SIGNAL_GENERATION",
            data={"errors": "DEV_MOCK_SIGNAL_RUN"},
            db=db,
        )
        summary["scan_run_id"] = scan_run_id
    now = datetime.now(timezone.utc)
    for symbol in selected_symbols:
        if symbol not in {"BTCUSDT", "ETHUSDT"}:
            summary["errors"].append({"symbol": symbol, "error": "Unsupported dev mock symbol"})
            summary["skipped_symbols"] += 1
            if scan_run_id:
                create_signal_scan_result(
                    {
                        "scan_run_id": scan_run_id,
                        "symbol": symbol,
                        "was_skipped": 1,
                        "skip_reason": "SYMBOL_NOT_ELIGIBLE",
                        "error": "Unsupported dev mock symbol",
                    },
                    db=db,
                )
            continue
        candidate = _mock_candidate(symbol, "BUY", now=now)
        summary["candidates_created"] += 1
        if repository.has_duplicate_open_signal(symbol, "BUY", asset_class="crypto"):
            repository.save_rejected_candidate(candidate, "DUPLICATE_SIGNAL")
            summary["rejected"] += 1
            if scan_run_id:
                create_signal_scan_result(
                    {
                        "scan_run_id": scan_run_id,
                        "symbol": symbol,
                        "was_scanned": 1,
                        "candidate_count": 1,
                        "rejected_count": 1,
                    },
                    db=db,
                )
            continue
        candidate_id = repository.save_accepted_candidate(candidate)
        repository.create_unpublished_signal_from_candidate(candidate, candidate_id=candidate_id)
        summary["accepted"] += 1
        summary["signals_created"] += 1
        if scan_run_id:
            create_signal_scan_result(
                {
                    "scan_run_id": scan_run_id,
                    "symbol": symbol,
                    "was_scanned": 1,
                    "candidate_count": 1,
                    "accepted_count": 1,
                    "published_count": 0,
                },
                db=db,
            )
    if scan_run_id:
        complete_signal_scan_run(
            scan_run_id,
            {
                "status": "PARTIAL" if summary["errors"] else "COMPLETED",
                "symbols_eligible": summary["eligible_symbols"],
                "symbols_scanned": summary["scanned_symbols"] - summary["skipped_symbols"],
                "candidates_created": summary["candidates_created"],
                "signals_published": 0,
                "errors": json.dumps(summary["errors"]) if summary["errors"] else "DEV_MOCK_SIGNAL_RUN",
            },
            db=db,
        )
    return normalize_summary(summary, dry_run=dry_run)


def parse_tiers(value: str | None) -> tuple[str, ...] | None:
    if not value:
        return None
    tiers = tuple(item.strip().upper() for item in value.split(",") if item.strip())
    return tiers or None


def _db_for_run(db_path: str | None) -> DB:
    if db_path:
        migrate(db_path)
        return DB(path=db_path)
    migrate()
    return DB()


def _symbol_skip_reason(symbol: str, db: DB) -> str | None:
    symbol = symbol.upper()
    pair = get_signal_pair(symbol, db=db)
    metrics = get_signal_pair_metrics(symbol, db=db)
    if pair and int(pair.get("blacklisted") or 0):
        return "BLACKLISTED_SYMBOL"
    if metrics:
        if int(metrics.get("is_safe") or 0) != 1:
            return metrics.get("unsafe_reason") or "UNSAFE_PAIR"
        if metrics.get("quote_volume_24h") is not None and float(metrics["quote_volume_24h"]) < 50_000_000:
            return "LOW_VOLUME"
        if metrics.get("spread_percent") is not None and float(metrics["spread_percent"]) > 0.20:
            return "SPREAD_TOO_WIDE"
    if symbol in ALLOWED_CRYPTO_SYMBOLS:
        return None
    if not pair:
        return "SYMBOL_NOT_ELIGIBLE"
    if not metrics:
        return "UNSAFE_PAIR"
    return None


def _select_symbols(
    *,
    symbols: list[str] | tuple[str, ...] | None,
    db: DB | None,
    use_eligible_universe: bool,
    tiers: tuple[str, ...] | None,
    max_symbols: int | None,
    min_volume: float,
    max_spread: float,
) -> tuple[tuple[str, ...], list[dict[str, str]]]:
    if use_eligible_universe:
        if db is None:
            return (), []
        return (
            tuple(
                get_eligible_signal_symbols(
                    tiers=tiers,
                    min_quote_volume_24h=min_volume,
                    max_spread_percent=max_spread,
                    require_safe=True,
                    limit=max_symbols,
                    db=db,
                )
            ),
            [],
        )
    selected = tuple(symbol.upper() for symbol in symbols) if symbols else tuple(ALLOWED_CRYPTO_SYMBOLS)
    if max_symbols is not None:
        selected = selected[: int(max_symbols)]
    skipped: list[dict[str, str]] = []
    if db is not None:
        allowed: list[str] = []
        for symbol in selected:
            reason = _symbol_skip_reason(symbol, db)
            if reason:
                skipped.append({"symbol": symbol, "reason": reason})
            else:
                allowed.append(symbol)
        selected = tuple(allowed)
    return selected, skipped


def _chunks(items: tuple[str, ...], chunk_size: int) -> list[tuple[str, ...]]:
    size = max(1, int(chunk_size or 1))
    return [items[index : index + size] for index in range(0, len(items), size)]


def with_retry(operation, *, max_retries: int = 2, base_delay: float = 1.0, sleep_fn=time.sleep):
    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            return operation()
        except Exception as exc:
            last_exc = exc
            message = str(exc).lower()
            temporary = any(token in message for token in ("timeout", "temporar", "rate", "429", "connection", "api"))
            if not temporary or attempt >= max_retries:
                raise
            sleep_fn(float(base_delay) * (2**attempt))
    if last_exc:
        raise last_exc


def _load_pair_metrics(symbols: tuple[str, ...], db: DB | None) -> dict[str, dict[str, Any]]:
    if db is None:
        return {}
    metrics = {}
    for symbol in symbols:
        row = get_signal_pair_metrics(symbol, db=db)
        if row:
            metrics[symbol.upper()] = row
    return metrics


def _publish_limit_reason(
    candidate: dict[str, Any],
    repository: Any,
    *,
    published_this_scan: int,
    published_by_symbol: dict[str, int],
    max_published_per_scan: int,
    max_active_signals: int,
    max_signals_per_symbol_per_day: int,
) -> str | None:
    symbol = str(candidate.get("symbol") or "").upper()
    if published_this_scan >= max_published_per_scan:
        return "MAX_PUBLISHED_PER_SCAN_REACHED"
    if repository.count_active_signals(asset_class="crypto") >= max_active_signals:
        return "MAX_ACTIVE_SIGNALS_REACHED"
    if repository.has_active_signal_for_symbol(symbol, asset_class="crypto"):
        return "ACTIVE_SYMBOL_SIGNAL_EXISTS"
    day_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    already_today = repository.count_published_signals_for_symbol_since(symbol, day_start, asset_class="crypto")
    if already_today + published_by_symbol.get(symbol, 0) >= max_signals_per_symbol_per_day:
        return "SYMBOL_DAILY_LIMIT_REACHED"
    return None


def _publish_ranked_candidates(
    ranked: list[dict[str, Any]],
    repository: Any,
    *,
    dry_run: bool,
    max_published_per_scan: int,
    max_active_signals: int,
    max_signals_per_symbol_per_day: int,
) -> tuple[int, list[dict[str, Any]]]:
    published = 0
    published_by_symbol: dict[str, int] = {}
    decisions: list[dict[str, Any]] = []
    for candidate in ranked:
        reason = _publish_limit_reason(
            candidate,
            repository,
            published_this_scan=published,
            published_by_symbol=published_by_symbol,
            max_published_per_scan=max_published_per_scan,
            max_active_signals=max_active_signals,
            max_signals_per_symbol_per_day=max_signals_per_symbol_per_day,
        )
        if reason:
            decisions.append({**candidate, "published": False, "limit_reason": reason})
            continue
        if not dry_run and candidate.get("signal_id"):
            repository.publish_signal(str(candidate["signal_id"]))
        published += 1
        symbol = str(candidate.get("symbol") or "").upper()
        published_by_symbol[symbol] = published_by_symbol.get(symbol, 0) + 1
        decisions.append({**candidate, "published": True, "limit_reason": None})
    return published, decisions


def run_generation(
    *,
    symbols: list[str] | tuple[str, ...] | None = None,
    db_path: str | None = None,
    dry_run: bool = False,
    dev_mode: bool = False,
    timeframe: str = "1h",
    use_eligible_universe: bool = False,
    tiers: list[str] | tuple[str, ...] | None = None,
    max_symbols: int | None = None,
    min_volume: float = 50_000_000,
    max_spread: float = 0.20,
    refresh_universe: bool = False,
    chunk_size: int = 10,
    sleep_between_chunks: float = 1.0,
    max_retries: int = 2,
    retry_base_delay: float = 1.0,
    max_published_per_scan: int = 5,
    max_active_signals: int = 10,
    max_signals_per_symbol_per_day: int = 1,
    scheduled: bool = False,
    ignore_pause: bool = False,
    lock_ttl_seconds: int = SIGNAL_GENERATION_LOCK_TTL_SECONDS,
    rollout_mode: str | None = None,
    tier_3_enabled: bool | None = None,
    sleep_fn: Any = time.sleep,
    engine_factory: Any = CryptoSignalEngine,
) -> dict[str, Any]:
    ensure_dev_signal_mode_allowed(requested=dev_mode)
    db: DB | None = None
    if dry_run:
        repository: Any = DryRunSignalRepository()
    else:
        db = _db_for_run(db_path)
        repository = SignalRepository(db)

    if db is not None and scheduled and is_signal_generation_paused(db=db) and not ignore_pause:
        return normalize_summary(
            {
                "paused": True,
                "scheduled": True,
                "rollout_mode": rollout_mode,
                "errors": [],
            }
        )

    lock_acquired = False
    if db is not None and scheduled and not dry_run:
        lock_acquired = acquire_signal_operation_lock(
            LOCK_SIGNAL_GENERATION,
            lock_ttl_seconds,
            metadata={"script": "generate_daily_crypto_signals", "rollout_mode": rollout_mode},
            db=db,
        )
        if not lock_acquired:
            return normalize_summary(
                {
                    "lock_not_acquired": True,
                    "scheduled": True,
                    "rollout_mode": rollout_mode,
                    "errors": [],
                }
            )

    try:
        configured_rollout = str(
            rollout_mode
            or (get_signal_setting(SETTING_ROLLOUT_MODE, DEFAULT_ROLLOUT_MODE, db=db) if db is not None else DEFAULT_ROLLOUT_MODE)
        ).strip().upper()
        if (scheduled or rollout_mode) and configured_rollout != ROLLOUT_V1_SEED_ONLY and symbols is None:
            use_eligible_universe = True
            tiers = tuple(tiers) if tiers else tiers_for_rollout_mode(configured_rollout)
        if configured_rollout == ROLLOUT_TIER_1_TIER_2_TIER_3:
            setting_tier_3 = False
            if db is not None:
                setting_tier_3 = str(get_signal_setting(SETTING_TIER_3_ENABLED, "0", db=db)).strip().lower() in {
                    "1",
                    "true",
                    "yes",
                    "on",
                }
            if not (tier_3_enabled or setting_tier_3):
                raise RuntimeError("TIER_3 rollout requires explicit tier_3_enabled=true setting or flag.")

        if refresh_universe and not dry_run and db is not None:
            discovery = PairDiscoveryService(db=db)
            discovery_summary = discovery.discover_binance_futures_pairs(
                min_quote_volume_24h=min_volume,
                max_spread_percent=max_spread,
                validate_candles=True,
            )
            if discovery_summary.get("errors"):
                # Discovery problems are not fatal if an existing safe universe is present.
                pass

        selected_symbols, skipped_symbols = _select_symbols(
            symbols=symbols,
            db=db,
            use_eligible_universe=use_eligible_universe,
            tiers=tuple(tiers) if tiers else None,
            max_symbols=max_symbols,
            min_volume=min_volume,
            max_spread=max_spread,
        )
        scan_run_id = None if dry_run else create_signal_scan_run("SIGNAL_GENERATION", db=db)

        engine_kwargs = {
            "repository": repository,
            "allowed_symbols": selected_symbols,
            "timeframe": timeframe,
            "dev_mode": dev_mode,
            "defer_publish": True,
        }
        if engine_factory is CryptoSignalEngine:
            engine_kwargs["market_data"] = MarketDataRunCache()
        engine = engine_factory(
            **engine_kwargs,
        )
        if dry_run or not hasattr(engine, "scan_symbol"):
            summary = engine.generate_crypto_signals(symbols=selected_symbols)
            summary["scan_run_id"] = scan_run_id
            summary["eligible_symbols"] = len(selected_symbols)
            summary["skipped_symbols"] = len(skipped_symbols)
            if scan_run_id:
                complete_signal_scan_run(
                    scan_run_id,
                    {
                        "status": "PARTIAL" if summary.get("errors") or skipped_symbols else "COMPLETED",
                        "symbols_eligible": len(selected_symbols),
                        "symbols_scanned": int(summary.get("scanned_symbols", 0) or 0),
                        "candidates_created": int(summary.get("candidates_created", 0) or 0),
                        "signals_published": int(summary.get("published", 0) or 0),
                        "errors": json.dumps(summary.get("errors", [])) if summary.get("errors") else None,
                    },
                    db=db,
                )
            summary["rollout_mode"] = configured_rollout
            summary["scheduled"] = scheduled
            return normalize_summary(summary, dry_run=dry_run)

        summary = {
            "scan_run_id": scan_run_id,
            "scanned_symbols": 0,
            "eligible_symbols": len(selected_symbols),
            "skipped_symbols": len(skipped_symbols),
            "candidates_created": 0,
            "accepted": 0,
            "rejected": 0,
            "ranked": 0,
            "signals_created": 0,
            "published": 0,
            "not_published_due_to_limits": 0,
            "rollout_mode": configured_rollout,
            "scheduled": scheduled,
            "errors": [],
        }
        for skipped in skipped_symbols:
            if scan_run_id:
                create_signal_scan_result(
                    {
                        "scan_run_id": scan_run_id,
                        "symbol": skipped["symbol"],
                        "was_skipped": 1,
                        "skip_reason": skipped["reason"],
                    },
                    db=db,
                )
        per_symbol: dict[str, dict[str, Any]] = {}
        accepted_candidates: list[dict[str, Any]] = []
        chunks = _chunks(selected_symbols, chunk_size)
        for chunk_index, chunk in enumerate(chunks):
            for symbol in chunk:
                try:
                    result = with_retry(
                        lambda symbol=symbol: engine.scan_symbol(symbol),
                        max_retries=max_retries,
                        base_delay=retry_base_delay,
                        sleep_fn=sleep_fn,
                    )
                except Exception as exc:
                    summary["errors"].append({"symbol": symbol, "error": str(exc)})
                    per_symbol[symbol] = {
                        "scan_run_id": scan_run_id,
                        "symbol": symbol,
                        "was_skipped": 1,
                        "skip_reason": "API_ERROR",
                        "error": str(exc),
                    }
                    continue
                summary["scanned_symbols"] += 1
                candidate_count = len(result)
                accepted_items = [item for item in result if item.get("accepted")]
                accepted_count = len(accepted_items)
                rejected_count = candidate_count - accepted_count
                signal_count = sum(1 for item in result if item.get("signal_id"))
                summary["candidates_created"] += candidate_count
                summary["accepted"] += accepted_count
                summary["rejected"] += rejected_count
                summary["signals_created"] += signal_count
                accepted_candidates.extend(accepted_items)
                per_symbol[symbol] = {
                    "scan_run_id": scan_run_id,
                    "symbol": symbol,
                    "was_scanned": 1,
                    "candidate_count": candidate_count,
                    "accepted_count": accepted_count,
                    "rejected_count": rejected_count,
                    "published_count": 0,
                }
            if chunk_index < len(chunks) - 1 and sleep_between_chunks > 0:
                sleep_fn(float(sleep_between_chunks))

        pair_metrics = _load_pair_metrics(tuple(str(item.get("symbol") or "").upper() for item in accepted_candidates), db)
        ranked = rank_signal_candidates(accepted_candidates, pair_metrics=pair_metrics)
        summary["ranked"] = len(ranked)
        published_count, publish_decisions = _publish_ranked_candidates(
            ranked,
            repository,
            dry_run=dry_run,
            max_published_per_scan=max_published_per_scan,
            max_active_signals=max_active_signals,
            max_signals_per_symbol_per_day=max_signals_per_symbol_per_day,
        )
        summary["published"] = published_count
        summary["not_published_due_to_limits"] = max(0, len(ranked) - published_count)
        summary["ranked_candidates"] = [
            {
                "symbol": item.get("symbol"),
                "side": item.get("side"),
                "rank_position": item.get("rank_position"),
                "total_rank_score": item.get("total_rank_score"),
                "published": item.get("published"),
                "limit_reason": item.get("limit_reason"),
            }
            for item in publish_decisions
        ]
        for item in publish_decisions:
            if item.get("published"):
                symbol = str(item.get("symbol") or "").upper()
                if symbol in per_symbol:
                    per_symbol[symbol]["published_count"] = int(per_symbol[symbol].get("published_count") or 0) + 1
        if scan_run_id:
            for row in per_symbol.values():
                create_signal_scan_result(row, db=db)
            complete_signal_scan_run(
                scan_run_id,
                {
                    "status": "PARTIAL" if summary["errors"] or skipped_symbols else "COMPLETED",
                    "symbols_eligible": summary["eligible_symbols"],
                    "symbols_scanned": summary["scanned_symbols"],
                    "candidates_created": summary["candidates_created"],
                    "signals_published": summary["published"],
                    "errors": json.dumps(summary["errors"]) if summary["errors"] else None,
                },
                db=db,
            )
        return normalize_summary(summary, dry_run=dry_run)
    except Exception as exc:
        if "scan_run_id" in locals() and scan_run_id:
            fail_signal_scan_run(scan_run_id, str(exc), db=db)
        raise
    finally:
        if lock_acquired and db is not None:
            release_signal_operation_lock(LOCK_SIGNAL_GENERATION, db=db)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Generate Crypto Signal Center candidates and unpublished trading signals. "
            "Recommended future scheduler times: 07:00, 12:00, 16:00, and 20:00 UTC."
        )
    )
    parser.add_argument("--db-path", default=None, help="Optional SQLite database path for tests/manual runs.")
    parser.add_argument("--symbol", default=None, help="Scan one symbol, e.g. BTCUSDT.")
    parser.add_argument("--limit-symbols", default=None, help="Comma-separated symbols to scan.")
    parser.add_argument("--timeframe", default="1h", help="Signal timeframe. Default: 1h.")
    parser.add_argument("--dry-run", action="store_true", help="Calculate signals without database writes.")
    parser.add_argument("--dev-mode", action="store_true", help="Mark persisted records as dev/test records.")
    parser.add_argument("--create-mock-dev-signals", action="store_true", help="Create deterministic DEV/TEST mock signals. Requires DEV_SIGNAL_MODE=true.")
    parser.add_argument("--use-eligible-universe", action="store_true", help="Scan safe symbols from signal_pair_universe/signal_pair_metrics.")
    parser.add_argument("--max-symbols", type=int, default=None, help="Maximum symbols to scan.")
    parser.add_argument("--tiers", default=None, help="Comma-separated tiers, e.g. TIER_1,TIER_2.")
    parser.add_argument("--refresh-universe", action="store_true", help="Run read-only pair discovery before selecting eligible symbols.")
    parser.add_argument("--chunk-size", type=int, default=10, help="Number of symbols to scan before sleeping.")
    parser.add_argument("--sleep-between-chunks", type=float, default=1.0, help="Seconds to sleep between scan chunks.")
    parser.add_argument("--min-volume", type=float, default=50_000_000.0, help="Minimum 24h quote volume for eligible-universe mode.")
    parser.add_argument("--max-spread", type=float, default=0.20, help="Maximum spread percent for eligible-universe mode.")
    parser.add_argument("--max-published-per-scan", type=int, default=5)
    parser.add_argument("--max-active-signals", type=int, default=10)
    parser.add_argument("--max-signals-per-symbol-per-day", type=int, default=1)
    parser.add_argument("--scheduled", action="store_true", help="Run with scheduler-safe defaults, pause checks, and operation lock.")
    parser.add_argument("--ignore-pause", action="store_true", help="Explicitly bypass signal_generation_paused for manual/admin runs.")
    parser.add_argument("--lock-ttl-seconds", type=int, default=SIGNAL_GENERATION_LOCK_TTL_SECONDS)
    parser.add_argument("--rollout-mode", default=None, help="V1_SEED_ONLY, TIER_1_ONLY, TIER_1_TIER_2, or TIER_1_TIER_2_TIER_3.")
    parser.add_argument("--enable-tier-3", action="store_true", help="Explicitly allow Tier 3 when rollout mode requests it.")
    args = parser.parse_args()

    load_dotenv()
    symbols = [args.symbol.strip().upper()] if args.symbol else parse_symbol_list(args.limit_symbols)
    if args.scheduled:
        args.use_eligible_universe = True
        args.tiers = args.tiers or ",".join(DEFAULT_TIERS)
        args.max_symbols = args.max_symbols or DEFAULT_MAX_SYMBOLS
        args.max_published_per_scan = args.max_published_per_scan or DEFAULT_MAX_PUBLISHED_PER_SCAN
        args.max_active_signals = args.max_active_signals or DEFAULT_MAX_ACTIVE_SIGNALS
        args.min_volume = args.min_volume or DEFAULT_MIN_VOLUME
        args.max_spread = args.max_spread or DEFAULT_MAX_SPREAD
        args.rollout_mode = args.rollout_mode or DEFAULT_ROLLOUT_MODE
    try:
        if args.create_mock_dev_signals:
            summary = create_mock_dev_signals(symbols=symbols, db_path=args.db_path, dry_run=args.dry_run)
        else:
            summary = run_generation(
                symbols=symbols,
                db_path=args.db_path,
                dry_run=args.dry_run,
                dev_mode=args.dev_mode,
                timeframe=args.timeframe,
                use_eligible_universe=args.use_eligible_universe,
                tiers=parse_tiers(args.tiers),
                max_symbols=args.max_symbols,
                min_volume=args.min_volume,
                max_spread=args.max_spread,
                refresh_universe=args.refresh_universe,
                chunk_size=args.chunk_size,
                sleep_between_chunks=args.sleep_between_chunks,
                max_published_per_scan=args.max_published_per_scan,
                max_active_signals=args.max_active_signals,
                max_signals_per_symbol_per_day=args.max_signals_per_symbol_per_day,
                scheduled=args.scheduled,
                ignore_pause=args.ignore_pause,
                lock_ttl_seconds=args.lock_ttl_seconds,
                rollout_mode=args.rollout_mode,
                tier_3_enabled=args.enable_tier_3,
            )
    except RuntimeError as exc:
        summary = normalize_summary({"errors": [{"error": str(exc)}]})
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 1 if summary.get("errors") else 0


if __name__ == "__main__":
    raise SystemExit(main())
