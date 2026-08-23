"""
Benzinga news provider client (crypto/financial news endpoint).
Docs: https://docs.benzinga.io/benzinga/newsfeed-v2.html
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import List, Optional
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

from app.news.provider_base import NewsProviderBase, RawNewsItem


_BASE_URL = "https://api.benzinga.com/api/v2/news"


class BenzingaClient(NewsProviderBase):
    def __init__(
        self,
        api_key: str,
        channels: str = "crypto",
        page_size: int = 50,
        timeout: int = 10,
    ) -> None:
        self._api_key = api_key
        self._channels = channels
        self._page_size = page_size
        self._timeout = timeout

    @property
    def provider_name(self) -> str:
        return "benzinga"

    def is_enabled(self) -> bool:
        return bool(self._api_key)

    def fetch(self) -> List[RawNewsItem]:
        if not self._api_key:
            return []

        url = (
            f"{_BASE_URL}?token={self._api_key}"
            f"&channels={self._channels}"
            f"&pageSize={self._page_size}"
            f"&displayOutput=abstract"
        )

        t0 = time.monotonic()
        try:
            req = Request(url, headers={"Accept": "application/json", "User-Agent": "CosmicForge-Bot/1.0"})
            with urlopen(req, timeout=self._timeout) as resp:
                data = json.loads(resp.read().decode())
        except (URLError, HTTPError, Exception):
            return []

        latency = time.monotonic() - t0
        ingested = datetime.now(timezone.utc).isoformat()
        items: List[RawNewsItem] = []

        # Benzinga returns a list directly
        posts = data if isinstance(data, list) else data.get("data", [])
        for post in posts:
            article_id = str(post.get("id", ""))
            created_raw = post.get("created") or post.get("updatedAt", "")
            try:
                published_utc = datetime.fromisoformat(
                    created_raw.replace("Z", "+00:00")
                ).isoformat()
            except Exception:
                published_utc = ingested

            # Extract first author
            authors = post.get("authors") or []
            author = authors[0].get("name") if authors else None

            # Body snippet from teaser/abstract
            body_snippet = post.get("teaser") or post.get("body", "")[:500] or None

            items.append(
                RawNewsItem(
                    provider=self.provider_name,
                    title=post.get("headline") or post.get("title", ""),
                    published_utc=published_utc,
                    ingested_utc=ingested,
                    external_id=article_id,
                    source_name="Benzinga",
                    source_domain="benzinga.com",
                    source_url=post.get("url"),
                    author=author,
                    body_snippet=body_snippet,
                    raw_payload_json=json.dumps(post),
                    latency_seconds=round(latency, 3),
                    language="en",
                )
            )

        return items
