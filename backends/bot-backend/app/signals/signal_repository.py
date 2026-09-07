from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from shared_lib.persistence.db import DB, utc_now_iso
from shared_lib.persistence.signals import (
    CANDIDATE_STATUS_ACCEPTED,
    CANDIDATE_STATUS_CANDIDATE,
    CANDIDATE_STATUS_REJECTED,
    SIGNAL_STATUS_ACTIVE,
    SIGNAL_STATUS_PENDING_ENTRY,
    SOURCE_INTERNAL_SIGNAL_ENGINE,
    create_signal_candidate,
    create_trading_signal,
    ensure_signals_schema,
    mark_candidate_rejected,
    publish_trading_signal,
    update_signal_candidate,
)

SOURCE_DEV_MOCK_SIGNAL_ENGINE = "dev_mock_signal_engine"


class SignalRepository:
    def __init__(self, db: DB | None = None):
        self.db = db or DB()
        ensure_signals_schema(self.db)

    def save_candidate(self, data: dict[str, Any]) -> str:
        payload = self._candidate_payload(data)
        payload.setdefault("status", CANDIDATE_STATUS_CANDIDATE)
        return create_signal_candidate(payload, db=self.db)

    def save_rejected_candidate(self, data: dict[str, Any], rejection_reason: str) -> str:
        payload = self._candidate_payload(data)
        payload["status"] = CANDIDATE_STATUS_REJECTED
        payload["rejection_reason"] = rejection_reason
        candidate_id = create_signal_candidate(payload, db=self.db)
        mark_candidate_rejected(candidate_id, rejection_reason, db=self.db)
        return candidate_id

    def save_accepted_candidate(self, data: dict[str, Any]) -> str:
        payload = self._candidate_payload(data)
        payload["status"] = CANDIDATE_STATUS_ACCEPTED
        candidate_id = create_signal_candidate(payload, db=self.db)
        update_signal_candidate(candidate_id, {"status": CANDIDATE_STATUS_ACCEPTED}, db=self.db)
        return candidate_id

    def create_unpublished_signal_from_candidate(
        self,
        candidate_data: dict[str, Any],
        candidate_id: str | None = None,
    ) -> str:
        return self._create_signal_from_candidate(candidate_data, candidate_id=candidate_id, publish=False)

    def create_published_signal_from_candidate(
        self,
        candidate_data: dict[str, Any],
        candidate_id: str | None = None,
    ) -> str:
        return self._create_signal_from_candidate(candidate_data, candidate_id=candidate_id, publish=True)

    def _create_signal_from_candidate(
        self,
        candidate_data: dict[str, Any],
        *,
        candidate_id: str | None = None,
        publish: bool = False,
    ) -> str:
        published_at = utc_now_iso() if publish else None
        expires_at = candidate_data["expires_at"]
        validity_minutes = candidate_data.get("signal_validity_minutes")
        if publish and validity_minutes:
            published_dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            expires_at = (published_dt + timedelta(minutes=int(validity_minutes))).isoformat()
        payload = {
            "candidate_id": candidate_id,
            "asset_class": candidate_data["asset_class"],
            "symbol": candidate_data["symbol"],
            "side": candidate_data["side"],
            "timeframe": candidate_data.get("timeframe"),
            "strategy_name": candidate_data.get("strategy_name"),
            "entry_price": candidate_data["entry_price"],
            "entry_zone_low": candidate_data.get("entry_zone_low"),
            "entry_zone_high": candidate_data.get("entry_zone_high"),
            "stop_loss": candidate_data["stop_loss"],
            "take_profit_1": candidate_data["take_profit_1"],
            "take_profit_2": candidate_data.get("take_profit_2"),
            "take_profit_3": candidate_data.get("take_profit_3"),
            "risk_reward": candidate_data["risk_reward"],
            "confidence_score": candidate_data["confidence_score"],
            "signal_reason": candidate_data.get("signal_reason"),
            "status": candidate_data.get("signal_status", SIGNAL_STATUS_PENDING_ENTRY),
            "is_published": 1 if publish else 0,
            "source": candidate_data.get("source") or SOURCE_INTERNAL_SIGNAL_ENGINE,
            "dev_mode": int(candidate_data.get("dev_mode", 0) or 0),
            "expires_at": expires_at,
            "published_at": published_at,
        }
        return create_trading_signal(payload, db=self.db)

    def has_duplicate_open_signal(self, symbol: str, side: str, asset_class: str = "crypto") -> bool:
        now = utc_now_iso()
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT id
                FROM trading_signals
                WHERE UPPER(symbol) = ?
                  AND UPPER(side) = ?
                  AND LOWER(asset_class) = ?
                  AND status IN (?, ?)
                  AND expires_at > ?
                LIMIT 1
                """,
                (symbol.upper(), side.upper(), asset_class.lower(), SIGNAL_STATUS_PENDING_ENTRY, SIGNAL_STATUS_ACTIVE, now),
            ).fetchone()
        return row is not None

    def publish_signal(self, signal_id: str) -> None:
        publish_trading_signal(signal_id, db=self.db)

    def count_active_signals(self, asset_class: str = "crypto") -> int:
        now = utc_now_iso()
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM trading_signals
                WHERE is_published = 1
                  AND LOWER(asset_class) = ?
                  AND status IN (?, ?)
                  AND expires_at > ?
                """,
                (asset_class.lower(), SIGNAL_STATUS_PENDING_ENTRY, SIGNAL_STATUS_ACTIVE, now),
            ).fetchone()
        return int(row["c"] if row else 0)

    def has_active_signal_for_symbol(self, symbol: str, asset_class: str = "crypto") -> bool:
        now = utc_now_iso()
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT id
                FROM trading_signals
                WHERE is_published = 1
                  AND UPPER(symbol) = ?
                  AND LOWER(asset_class) = ?
                  AND status IN (?, ?)
                  AND expires_at > ?
                LIMIT 1
                """,
                (symbol.upper(), asset_class.lower(), SIGNAL_STATUS_PENDING_ENTRY, SIGNAL_STATUS_ACTIVE, now),
            ).fetchone()
        return row is not None

    def count_published_signals_for_symbol_since(
        self,
        symbol: str,
        since_iso: str,
        asset_class: str = "crypto",
    ) -> int:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM trading_signals
                WHERE is_published = 1
                  AND UPPER(symbol) = ?
                  AND LOWER(asset_class) = ?
                  AND COALESCE(published_at, created_at) >= ?
                """,
                (symbol.upper(), asset_class.lower(), since_iso),
            ).fetchone()
        return int(row["c"] if row else 0)

    def list_recent_generated_signals(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        symbol: str | None = None,
        asset_class: str = "crypto",
    ) -> list[dict[str, Any]]:
        where = ["LOWER(asset_class) = ?", "source = ?"]
        params: list[Any] = [asset_class.lower(), SOURCE_INTERNAL_SIGNAL_ENGINE]
        if symbol:
            where.append("UPPER(symbol) = ?")
            params.append(symbol.upper())
        with self.db.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM trading_signals
                WHERE {' AND '.join(where)}
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
                """,
                (*params, int(limit), int(offset)),
            ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _candidate_payload(data: dict[str, Any]) -> dict[str, Any]:
        allowed_columns = {
            "id",
            "asset_class",
            "symbol",
            "side",
            "timeframe",
            "strategy_name",
            "entry_price",
            "entry_zone_low",
            "entry_zone_high",
            "stop_loss",
            "take_profit_1",
            "take_profit_2",
            "take_profit_3",
            "risk_reward",
            "confidence_score",
            "signal_reason",
            "rejection_reason",
            "source",
            "status",
            "dev_mode",
            "created_at",
            "updated_at",
        }
        payload = {key: value for key, value in dict(data).items() if key in allowed_columns}
        payload.setdefault("asset_class", "crypto")
        payload["asset_class"] = str(payload["asset_class"]).lower()
        payload["symbol"] = str(payload["symbol"]).upper()
        payload["side"] = str(payload["side"]).upper()
        payload.setdefault("source", SOURCE_INTERNAL_SIGNAL_ENGINE)
        payload.setdefault("dev_mode", 0)
        return payload
