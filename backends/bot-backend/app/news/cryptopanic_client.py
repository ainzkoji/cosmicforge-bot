"""
CryptoPanic news provider client.
Docs: https://cryptopanic.com/developers/api/
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import List, Optional
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

from app.news.provider_base import NewsProviderBase, RawNewsItem


_BASE_URL = "https://cryptopanic.com/api/v1/posts/"


class CryptoPanicClient(NewsProviderBase):
    def __init__(
        self,
        api_key: str,
        currencies: Optional[str] = None,
        filter_: str = "hot",
        timeout: int = 10,
    ) -> None:
        self._api_key = api_key
        self._currencies = currencies  # e.g. "BTC,ETH"
        self._filter = filter_
        self._timeout = timeout
        self._last_id: Optional[str] = None

    @property
    def provider_name(self) -> str:
        return "cryptopanic"

    def is_enabled(self) -> bool:
        return bool(self._api_key)

    def fetch(self) -> List[RawNewsItem]:
        if not self._api_key:
            return []

        url = f"{_BASE_URL}?auth_token={self._api_key}&public=true&filter={self._filter}&kind=news"
        if self._currencies:
            url += f"&currencies={self._currencies}"

        t0 = time.monotonic()
        try:
            req = Request(url, headers={"User-Agent": "CosmicForge-Bot/1.0"})
            with urlopen(req, timeout=self._timeout) as resp:
                data = json.loads(resp.read().decode())
        except (URLError, HTTPError, Exception):
            return []

        latency = time.monotonic() - t0
        ingested = datetime.now(timezone.utc).isoformat()
        items: List[RawNewsItem] = []

        for post in data.get("results", []):
            post_id = str(post.get("id", ""))
            published_raw = post.get("published_at") or post.get("created_at", "")
            try:
                published_utc = datetime.fromisoformat(
                    published_raw.replace("Z", "+00:00")
                ).isoformat()
            except Exception:
                published_utc = ingested

            currencies = post.get("currencies") or []
            domain = post.get("source", {}).get("domain", "")
            source_name = post.get("source", {}).get("title", "")

            items.append(
                RawNewsItem(
                    provider=self.provider_name,
                    title=post.get("title", ""),
                    published_utc=published_utc,
                    ingested_utc=ingested,
                    external_id=post_id,
                    source_name=source_name,
                    source_domain=domain,
                    source_url=post.get("url"),
                    body_snippet=None,
                    raw_payload_json=json.dumps(post),
                    latency_seconds=round(latency, 3),
                    language="en",
                )
            )

        return items
