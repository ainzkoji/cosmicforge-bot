"""
Generic JSON news API adapter.

Supports any provider that returns a JSON array or object with a list field.
Configure via:
  GENERIC_NEWS_API_KEY=...
  GENERIC_NEWS_API_URL=https://api.example.com/v1/news
  GENERIC_NEWS_API_ENABLED=true

The response must be one of:
  - a JSON array of article objects
  - a JSON object with a field named "articles", "data", "results", or "items"

Each article must have at least a `title` field.
Optional: `url`, `publishedAt`/`published_at`/`created_at`, `source.name`,
          `description`/`body`/`snippet`.
"""
from __future__ import annotations

import json
import time
import logging
import urllib.request as urllib_request
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from urllib.error import HTTPError, URLError

from app.news.news_normalizer import NormalizedNewsItem, _compute_latency_seconds, _url_hash, _extract_domain

logger = logging.getLogger(__name__)

_PROVIDER = "generic"
_LIST_KEYS = ("articles", "data", "results", "items", "news", "posts")


class GenericNewsApiClient:
    def __init__(
        self,
        api_key: str,
        api_url: str,
        category: str = "CRYPTO",
        timeout: int = 10,
        max_items: int = 100,
        extra_headers: Optional[dict] = None,
    ) -> None:
        self._api_key = api_key.strip()
        self._api_url = api_url.strip()
        self._category = category
        self._timeout = timeout
        self._max_items = max_items
        self._extra_headers = extra_headers or {}
        self._backoff_until: float = 0.0
        self._last_fetch: float = 0.0

    @property
    def provider(self) -> str:
        return _PROVIDER

    def is_enabled(self) -> bool:
        return bool(self._api_key and self._api_url)

    def is_due(self, interval_seconds: int) -> bool:
        now = time.monotonic()
        if now < self._backoff_until:
            return False
        return (now - self._last_fetch) >= interval_seconds

    def fetch(self) -> Tuple[List[NormalizedNewsItem], Optional[str], float]:
        if not self.is_enabled():
            return [], "No API key or URL configured", 0.0

        url = self._api_url
        if "?" in url:
            url += f"&apiKey={self._api_key}"
        else:
            url += f"?apiKey={self._api_key}"

        t0 = time.time()
        now_ts = time.monotonic()
        error: Optional[str] = None
        items: List[NormalizedNewsItem] = []

        headers = {
            "Accept": "application/json",
            "User-Agent": "CosmicForge-NewsBot/1.0",
            "Authorization": f"Bearer {self._api_key}",
            **self._extra_headers,
        }

        try:
            req = urllib_request.Request(url, headers=headers)
            with urllib_request.urlopen(req, timeout=self._timeout) as resp:
                data = json.loads(resp.read().decode())
            latency = round(time.time() - t0, 3)
            ingested = datetime.now(timezone.utc).isoformat()

            posts = data if isinstance(data, list) else None
            if posts is None:
                for key in _LIST_KEYS:
                    if isinstance(data.get(key), list):
                        posts = data[key]
                        break
            if posts is None:
                return [], "Unrecognised response format", latency

            for post in posts[: self._max_items]:
                item = self._normalize(post, ingested, latency)
                if item:
                    items.append(item)

            self._last_fetch = now_ts
            self._backoff_until = 0.0
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
            logger.exception("[GenericNewsApi] unexpected error")

        self._last_fetch = now_ts
        return [], error, round(time.time() - t0, 3)

    def _normalize(
        self, post: dict, ingested: str, latency: float
    ) -> Optional[NormalizedNewsItem]:
        title = (
            post.get("title") or post.get("headline") or post.get("name") or ""
        ).strip()
        if not title:
            return None

        link = post.get("url") or post.get("link") or post.get("web_url") or ""
        body_snippet = (
            post.get("description")
            or post.get("body")
            or post.get("snippet")
            or post.get("teaser")
            or ""
        )[:500]

        published_raw = (
            post.get("publishedAt")
            or post.get("published_at")
            or post.get("created_at")
            or post.get("updated_at")
            or ""
        )
        from app.news.news_normalizer import _to_utc_iso
        published_utc = _to_utc_iso(published_raw) if published_raw else ingested

        source = post.get("source") or {}
        source_name = (
            source.get("name") if isinstance(source, dict) else str(source)
        ) or "Generic"
        domain = _extract_domain(link) or "unknown"

        external_id = str(post.get("id") or post.get("uuid") or "") or _url_hash(
            link or title
        )

        return NormalizedNewsItem(
            provider=_PROVIDER,
            source_name=source_name,
            source_domain=domain,
            source_url=link,
            title=title,
            body_snippet=body_snippet,
            published_utc=published_utc,
            ingested_utc=ingested,
            language=post.get("language", "en"),
            category=self._category,
            raw_payload_json=json.dumps({"latency_seconds": latency, **post}),
            external_id=external_id,
            latency_seconds=_compute_latency_seconds(published_utc, ingested),
        )
