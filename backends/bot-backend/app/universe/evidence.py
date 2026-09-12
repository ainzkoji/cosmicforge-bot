"""Universe evidence -- what was discovered, what was excluded and why, what was chosen.

One ``universe_snapshots`` row per refresh (every ~15 minutes, not every
cycle), with the exclusions as a compact ``symbol -> reason`` map, and one
``universe_members`` row per active candidate with its rank and the quality
inputs that ranked it. No credentials are ever written.

Recording never raises into the trading path: losing an evidence row is a
reporting gap, stopping a cycle is a trading fault.
"""
from __future__ import annotations

import json
import logging
import uuid
from collections import Counter
from typing import Any, Iterable, Mapping

from app.universe.contracts import UniverseSnapshot

logger = logging.getLogger(__name__)


def record_universe_snapshot(
    db: Any,
    snapshot: UniverseSnapshot,
    *,
    bot_instance_id: str,
    run_id: str | None = None,
    runtime_session_id: str | None = None,
    open_symbols: Iterable[str] = (),
    managed_symbols: Iterable[str] = (),
    exposure_excluded: Mapping[str, str] | None = None,
    config: Mapping[str, Any] | None = None,
    request_weight_used: int | None = None,
    request_weight_limit: int | None = None,
) -> str | None:
    snapshot_id = f"unv_{uuid.uuid4().hex[:16]}"
    excluded = dict(snapshot.excluded)
    excluded.update(exposure_excluded or {})
    managed = list(managed_symbols)
    try:
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO universe_snapshots (
                    snapshot_id, bot_instance_id, broker_account_id, run_id, runtime_session_id,
                    venue, universe_mode, generated_at, discovered_count, eligible_count,
                    ranked_count, active_count, excluded_count, managed_count,
                    excluded_by_reason_json, excluded_json, active_symbols_json,
                    managed_symbols_json, open_symbols_json, stale, error, capabilities_json,
                    config_json, metadata_age_seconds, stats_age_seconds,
                    request_weight_used, request_weight_limit
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id, bot_instance_id, snapshot.broker_account_id, run_id, runtime_session_id,
                    snapshot.venue, snapshot.mode, snapshot.generated_at,
                    snapshot.discovered_count, snapshot.eligible_count, snapshot.ranked_count,
                    snapshot.active_count, len(excluded), len(managed),
                    json.dumps(dict(sorted(Counter(excluded.values()).items()))),
                    json.dumps(excluded, sort_keys=True),
                    json.dumps(list(snapshot.active_symbols)),
                    json.dumps(managed),
                    json.dumps(list(open_symbols)),
                    1 if snapshot.stale else 0,
                    snapshot.error,
                    json.dumps(dict(snapshot.capabilities), sort_keys=True),
                    json.dumps(dict(config or {}), sort_keys=True, default=str),
                    snapshot.metadata_age_seconds,
                    snapshot.stats_age_seconds,
                    request_weight_used,
                    request_weight_limit,
                ),
            )
            conn.executemany(
                """
                INSERT INTO universe_members (
                    snapshot_id, symbol, rank, canonical_id, underlying, quote_volume_24h,
                    spread_bps, trade_count_24h, last_price, selection_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        snapshot_id, m.symbol, m.rank, m.canonical_id, m.underlying,
                        m.quote_volume_24h, m.spread_bps, m.trade_count_24h, m.last_price,
                        m.selection_reason,
                    )
                    for m in snapshot.active
                ],
            )
        return snapshot_id
    except Exception as exc:
        logger.warning("[UNIVERSE_EVIDENCE] snapshot not recorded for %s: %s", bot_instance_id, exc)
        return None


__all__ = ["record_universe_snapshot"]
