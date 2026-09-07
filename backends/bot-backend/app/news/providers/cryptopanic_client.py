"""
CryptoPanic real-time provider client.

Enhanced version with:
- per-fetch latency tracking
- graceful key-missing disable
- NormalizedNewsItem output
- rate-limit backoff
"""
from __future__ import annotations

import json
import time
import logging
import urllib.request as urllib_request
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from urllib.error import HTTPError, URLError

from app.news.news_normalizer import NormalizedNewsItem, _compute_latency_seconds

logger = logging.getLogger(__name__)

_BASE_URL = "https://cryptopanic.com/api/v1/posts/"
_PROVIDER = "cryptopanic"


class CryptoPanicRealtimeClient:
    def __init__(
        self,
        api_key: str,
        currencies: str = "",
        filter_: str = "hot",
        timeout: int = 10,
        max_items: int = 100,
    ) -> None:
        self._api_key = api_key.strip()
        self._currencies = currencies
        self._filter = filter_
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
        """
        Returns (items, error_message, latency_seconds).
        items=[] on failure. Caller must check is_enabled() first.
        """
        if not self._api_key:
            return [], "No API key configured", 0.0

        url = (
            f"{_BASE_URL}?auth_token={self._api_key}"
            f"&public=true&filter={self._filter}&kind=news"
        )
        if self._currencies:
            url += f"&currencies={self._currencies}"

        t0 = time.time()
        now_ts = time.monotonic()
        error: Optional[str] = None
        items: List[NormalizedNewsItem] = []

        try:
            req = urllib_request.Request(url, headers={"User-Agent": "CosmicForge-NewsBot/1.0"})
            with urllib_request.urlopen(req, timeout=self._timeout) as resp:
                data = json.loads(resp.read().decode())
            latency = round(time.time() - t0, 3)
            ingested = datetime.now(timezone.utc).isoformat()

            for post in data.get("results", [])[:self._max_items]:
                item = self._normalize(post, ingested, latency)
                if item:
                    items.append(item)

            self._last_fetch = now_ts
            self._backoff_until = 0.0
            logger.debug("[CryptoPanic] fetched %d items in %.2fs", len(items), latency)
            return items, None, latency

        except HTTPError as exc:
            error = f"HTTP {exc.code}: {exc.reason}"
            backoff = 600 if exc.code == 429 else 120
            self._backoff_until = now_ts + backoff
        except URLError as exc:
            error = f"URLError: {exc.reason}"
            self._backoff_until = now_ts + 120
        except Exception as exc:
            error = f"Unexpected: {exc}"
            self._backoff_until = now_ts + 120
            logger.exception("[CryptoPanic] unexpected error")

        self._last_fetch = now_ts
        latency = round(time.time() - t0, 3)
        return [], error, latency

    def _normalize(
        self, post: dict, ingested: str, latency: float
    ) -> Optional[NormalizedNewsItem]:
        title = (post.get("title") or "").strip()
        if not title:
            return None

        post_id = str(post.get("id", ""))
        published_raw = post.get("published_at") or post.get("created_at", "")
        try:
            published_utc = datetime.fromisoformat(
                published_raw.replace("Z", "+00:00")
            ).isoformat()
        except Exception:
            published_utc = ingested

        domain = post.get("source", {}).get("domain", "cryptopanic.com")
        source_name = post.get("source", {}).get("title", "CryptoPanic")
        url = post.get("url", "")

        from app.news.news_normalizer import _url_hash
        return NormalizedNewsItem(
            provider=_PROVIDER,
            source_name=source_name,
            source_domain=domain,
            source_url=url,
            title=title,
            body_snippet="",
            published_utc=published_utc,
            ingested_utc=ingested,
            language="en",
            category="CRYPTO",
            raw_payload_json=json.dumps({"latency_seconds": latency, **post}),
            external_id=post_id or _url_hash(url or title),
            latency_seconds=_compute_latency_seconds(published_utc, ingested),
        )
