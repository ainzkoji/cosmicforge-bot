"""UniverseRuntime -- one bot's universe, refreshed on a cadence, open positions first.

The universe decides which symbols may produce NEW entries. It never decides
which positions are managed: every symbol the bot holds a position (or an
in-flight entry) in is managed on every cycle, first, whether or not it still
ranks. Dropping out of the ranking changes nothing about SL/TP, TP1,
break-even, trailing, closes or reconciliation.

Concentration: a candidate whose underlying the bot already holds -- through
any symbol, quote or multiplier contract -- is not a new-entry candidate
(``UNDERLYING_ALREADY_OPEN``). Universe breadth is not position capacity; the
capital ledger, risk and the threshold engine still decide every entry.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping

from app.universe.contracts import Exclusion, UniverseMode, UniverseSnapshot
from app.universe.engine import UniverseEngine
from app.universe.evidence import record_universe_snapshot
from app.universe.identity import underlying_from_symbol

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UniverseResolution:
    snapshot: UniverseSnapshot
    open_symbols: tuple[str, ...]
    candidates: tuple[str, ...]
    managed: tuple[str, ...]
    exposure_excluded: Mapping[str, str] = field(default_factory=dict)
    refreshed: bool = False
    snapshot_id: str | None = None


def _ordered_unique(symbols: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for s in symbols:
        u = str(s or "").strip().upper()
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def resolve_managed(
    snapshot: UniverseSnapshot,
    open_symbols: Iterable[str],
    *,
    underlying_of: Callable[[str], str] = underlying_from_symbol,
) -> tuple[tuple[str, ...], tuple[str, ...], dict[str, str]]:
    """``(candidates, managed, exposure_excluded)``; managed = open first, then candidates."""
    held = _ordered_unique(open_symbols)
    held_underlyings = {underlying_of(s) for s in held}
    candidates: list[str] = []
    exposure_excluded: dict[str, str] = {}
    for member in snapshot.active:
        if member.symbol in held:
            continue
        if member.underlying in held_underlyings:
            exposure_excluded[member.symbol] = Exclusion.UNDERLYING_ALREADY_OPEN
            continue
        candidates.append(member.symbol)
    return tuple(candidates), tuple(held + candidates), exposure_excluded


class UniverseRuntime:
    def __init__(
        self,
        *,
        engine: UniverseEngine,
        broker_account_id: str,
        bot_instance_id: str,
        mode: str = UniverseMode.BROKER,
        allowlist: Iterable[str] = (),
        refresh_seconds: float = 900.0,
        db: Any = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.engine = engine
        self.broker_account_id = broker_account_id
        self.bot_instance_id = bot_instance_id
        self.mode = UniverseMode.normalize(mode) or UniverseMode.BROKER
        self.allowlist = tuple(_ordered_unique(allowlist))
        self.refresh_seconds = float(refresh_seconds)
        self._db = db
        self._clock = clock
        self._snapshot: UniverseSnapshot | None = None
        self._refreshed_at = 0.0

    @property
    def snapshot(self) -> UniverseSnapshot | None:
        return self._snapshot

    def due(self) -> bool:
        return self._snapshot is None or (self._clock() - self._refreshed_at) >= self.refresh_seconds

    def underlying_of(self, symbol: str) -> str:
        meta = self.engine.instrument(symbol)
        return meta.canonical.underlying if meta else underlying_from_symbol(symbol)

    def resolve(
        self,
        *,
        open_symbols: Iterable[str],
        run_id: str | None = None,
        runtime_session_id: str | None = None,
        force: bool = False,
    ) -> UniverseResolution:
        held = tuple(_ordered_unique(open_symbols))
        refreshed = False
        if force or self.due():
            self._snapshot = self.engine.refresh(
                broker_account_id=self.broker_account_id, mode=self.mode, allowlist=self.allowlist,
            )
            self._refreshed_at = self._clock()
            refreshed = True
        snapshot = self._snapshot
        candidates, managed, exposure = resolve_managed(snapshot, held, underlying_of=self.underlying_of)
        snapshot_id = None
        if refreshed:
            budget = None
            try:
                budget = self.engine.adapter.request_budget()
            except Exception:
                budget = None
            if self._db is not None:
                snapshot_id = record_universe_snapshot(
                    self._db, snapshot,
                    bot_instance_id=self.bot_instance_id, run_id=run_id,
                    runtime_session_id=runtime_session_id, open_symbols=held,
                    managed_symbols=managed, exposure_excluded=exposure,
                    config=self.engine.config.to_dict(),
                    request_weight_used=getattr(budget, "used", None),
                    request_weight_limit=getattr(budget, "limit", None),
                )
            logger.info(
                "[UNIVERSE] bot=%s venue=%s mode=%s discovered=%d eligible=%d ranked=%d active=%d "
                "open=%d candidates=%d managed=%d excluded=%s stale=%s error=%s",
                self.bot_instance_id, snapshot.venue, snapshot.mode, snapshot.discovered_count,
                snapshot.eligible_count, snapshot.ranked_count, snapshot.active_count, len(held),
                len(candidates), len(managed), snapshot.excluded_by_reason, snapshot.stale, snapshot.error,
            )
        return UniverseResolution(
            snapshot=snapshot,
            open_symbols=held,
            candidates=candidates,
            managed=managed,
            exposure_excluded=exposure,
            refreshed=refreshed,
            snapshot_id=snapshot_id,
        )


__all__ = ["UniverseResolution", "UniverseRuntime", "resolve_managed"]
