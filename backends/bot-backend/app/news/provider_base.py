"""
Base contract for news providers and the shared RawNewsItem dataclass.
"""
from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional


@dataclass
class RawNewsItem:
    provider: str
    title: str
    published_utc: str
    ingested_utc: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    external_id: Optional[str] = None
    source_name: Optional[str] = None
    source_domain: Optional[str] = None
    source_url: Optional[str] = None
    author: Optional[str] = None
    body_snippet: Optional[str] = None
    raw_payload_json: Optional[str] = None
    latency_seconds: Optional[float] = None
    language: str = "en"

    def fingerprint(self) -> str:
        """Stable content hash for exact-duplicate detection."""
        text = f"{self.provider}|{self.external_id or ''}|{self.title.lower().strip()}"
        return hashlib.sha256(text.encode()).hexdigest()[:16]


class NewsProviderBase(ABC):
    """All provider clients implement this interface."""

    @property
    @abstractmethod
    def provider_name(self) -> str:
        ...

    @abstractmethod
    def fetch(self) -> List[RawNewsItem]:
        """
        Poll the provider and return new items since the last fetch.
        Must be safe to call repeatedly — deduplication happens upstream.
        Raises on unrecoverable errors; returns [] on empty / rate-limit.
        """
        ...

    def is_enabled(self) -> bool:
        return True
