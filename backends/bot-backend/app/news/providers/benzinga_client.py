"""
Benzinga real-time provider client (crypto/financial news).

Enhanced version with latency tracking and graceful key-missing disable.
"""
from __future__ import annotations

import json
import time
import logging
import urllib.request as urllib_request
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from urllib.error import HTTPError, URLError

from app.news.news_normalizer import NormalizedNewsItem, _compute_latency_seconds, _url_hash

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.benzinga.com/api/v2/news"
_PROVIDER = "benzinga"


class BenzingaRealtimeClient:
    def __init__(
        self,
        api_key: str,
        channels: str = "crypto",
        page_size: int = 50,
        timeout: int = 10,
        max_items: int = 100,
    ) -> None:
        self._api_key = api_key.strip()
        self._channels = channels
        self._page_size = min(page_size, max_items)
        self._timeout = timeout
        self._max_items = max_items
        self._backoff_until: float = 0.0
        self._last_fetch: float = 0.0

    @property
    def provider(self) -> str:
        return _PROVIDER

    def is_enabled(self) -> bool:
        return bool(self._api_key)

    def is_due(self, interval_seconds: int) -> bool:
        now = time.monotonic()
        if now < self._backoff_until:
            return False
        return (now - self._last_fetch) >= interval_seconds

    def fetch(self) -> Tuple[List[NormalizedNewsItem], Optional[str], float]:
        if not self._api_key:
            return [], "No API key configured", 0.0

        url = (
            f"{_BASE_URL}?token={self._api_key}"
            f"&channels={self._channels}"
            f"&pageSize={self._page_size}"
            f"&displayOutput=abstract"
        )

        t0 = time.time()
        now_ts = time.monotonic()
        error: Optional[str] = None
        items: List[NormalizedNewsItem] = []

        try:
            req = urllib_request.Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "CosmicForge-NewsBot/1.0",
                },
            )
            with urllib_request.urlopen(req, timeout=self._timeout) as resp:
                raw = resp.read().decode()
            latency = round(time.time() - t0, 3)
            data = json.loads(raw)
            ingested = datetime.now(timezone.utc).isoformat()

            posts = data if isinstance(data, list) else data.get("data", [])
            for post in posts[: self._max_items]:
                item = self._normalize(post, ingested, latency)
                if item:
                    items.append(item)

            self._last_fetch = now_ts
            self._backoff_until = 0.0
            logger.debug("[Benzinga] fetched %d items in %.2fs", len(items), latency)
            return items, None, latency

        except HTTPError as exc:
            error = f"HTTP {exc.code}: {exc.reason}"
            self._backoff_until = now_ts + (600 if exc.code == 429 else 120)
        except URLError as exc:
            error = f"URLError: {exc.reason}"
            self._backoff_until = now_ts + 120
        except Exception as exc:
            error = f"Unexpected: {exc}"
            self._backoff_until = now_ts + 120
            logger.exception("[Benzinga] unexpected error")

        self._last_fetch = now_ts
        latency = round(time.time() - t0, 3)
        return [], error, latency

    def _normalize(
        self, post: dict, ingested: str, latency: float
    ) -> Optional[NormalizedNewsItem]:
        title = (post.get("headline") or post.get("title") or "").strip()
        if not title:
            return None

        article_id = str(post.get("id", ""))
        created_raw = post.get("created") or post.get("updatedAt", "")
        try:
            published_utc = datetime.fromisoformat(
                created_raw.replace("Z", "+00:00")
            ).isoformat()
        except Exception:
            published_utc = ingested

        authors = post.get("authors") or []
        body_snippet = (
            (post.get("teaser") or post.get("body", ""))[:500] or ""
        )
        link = post.get("url", "")

        return NormalizedNewsItem(
            provider=_PROVIDER,
            source_name="Benzinga",
            source_domain="benzinga.com",
            source_url=link,
            title=title,
            body_snippet=body_snippet,
            published_utc=published_utc,
            ingested_utc=ingested,
            language="en",
            category="MARKET",
            raw_payload_json=json.dumps({"latency_seconds": latency, **post}),
            external_id=article_id or _url_hash(link or title),
            latency_seconds=_compute_latency_seconds(published_utc, ingested),
        )
