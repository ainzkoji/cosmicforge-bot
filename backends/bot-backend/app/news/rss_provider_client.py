"""
RSS feed fetcher and XML parser.

Uses only stdlib (urllib + xml.etree) — no feedparser dependency.
Implements per-source rate limiting and safe retry with backoff.
"""
from __future__ import annotations

import logging
import time
import urllib.request as urllib_request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import List, Optional
from urllib.error import HTTPError, URLError

from app.news.news_normalizer import NormalizedNewsItem, normalize_rss_entry

logger = logging.getLogger(__name__)

# XML namespaces commonly used in RSS/Atom feeds
_NS = {
    "content": "http://purl.org/rss/1.0/modules/content/",
    "dc":      "http://purl.org/dc/elements/1.1/",
    "media":   "http://search.yahoo.com/mrss/",
    "atom":    "http://www.w3.org/2005/Atom",
}

_DEFAULT_HEADERS = {
    "User-Agent": "CosmicForge-NewsBot/1.0 (+https://cosmicforge.io/news-bot)",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}


def _elem_text(elem: Optional[ET.Element]) -> str:
    if elem is None:
        return ""
    return (elem.text or "").strip()


def _parse_rss_items(root: ET.Element) -> List[dict]:
    """Parse <rss> or <rdf:RDF> flavoured feeds."""
    items = []
    # Standard RSS 2.0
    for item in root.findall(".//item"):
        entry: dict = {}
        for tag in ("title", "link", "description", "pubDate", "author", "guid"):
            el = item.find(tag)
            if el is not None:
                entry[tag] = _elem_text(el)
        # content:encoded
        ce = item.find(f"{{{_NS['content']}}}encoded")
        if ce is not None:
            entry["content"] = _elem_text(ce)
        # dc:date fallback for pubDate
        dc_date = item.find(f"{{{_NS['dc']}}}date")
        if dc_date is not None and "pubDate" not in entry:
            entry["pubDate"] = _elem_text(dc_date)
        # Normalise keys to common names
        entry.setdefault("published", entry.get("pubDate", ""))
        entry.setdefault("summary", entry.get("description", ""))
        entry.setdefault("url", entry.get("link", ""))
        items.append(entry)
    return items


def _parse_atom_items(root: ET.Element) -> List[dict]:
    """Parse Atom 1.0 feeds."""
    ns = "http://www.w3.org/2005/Atom"
    items = []
    for entry in root.findall(f"{{{ns}}}entry"):
        e: dict = {}
        for tag in ("title", "summary", "updated", "published", "id"):
            el = entry.find(f"{{{ns}}}{tag}")
            if el is not None:
                e[tag] = _elem_text(el)
        # link href
        link_el = entry.find(f"{{{ns}}}link")
        if link_el is not None:
            e["link"] = link_el.get("href", "")
        e.setdefault("url", e.get("link", ""))
        e.setdefault("published", e.get("updated", ""))
        items.append(e)
    return items


def _parse_feed(xml_bytes: bytes) -> List[dict]:
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise ValueError(f"XML parse error: {exc}") from exc

    tag = root.tag.lower()
    if "rss" in tag or "rdf" in tag:
        return _parse_rss_items(root)
    if "feed" in tag:
        return _parse_atom_items(root)
    # Fallback: try both parsers
    items = _parse_rss_items(root)
    if not items:
        items = _parse_atom_items(root)
    return items


class RSSProviderClient:
    """
    Fetches and parses a single RSS feed URL.
    Thread-safe; no mutable state beyond the last-fetch timestamp.
    """

    def __init__(
        self,
        source_id: str,
        source_name: str,
        rss_url: str,
        category: str = "CRYPTO",
        timeout: int = 15,
        max_items: int = 50,
        fetch_interval_seconds: int = 300,
    ) -> None:
        self.source_id = source_id
        self.source_name = source_name
        self.rss_url = rss_url
        self.category = category
        self.timeout = timeout
        self.max_items = max_items
        self.fetch_interval_seconds = fetch_interval_seconds
        self._last_fetch_ts: float = 0.0
        self._backoff_until_ts: float = 0.0

    def is_due(self) -> bool:
        now = time.monotonic()
        if now < self._backoff_until_ts:
            return False
        return (now - self._last_fetch_ts) >= self.fetch_interval_seconds

    def fetch(self) -> tuple[List[NormalizedNewsItem], Optional[str]]:
        """
        Returns (items, error_message).
        items is [] on failure; error_message is None on success.
        """
        now_ts = time.monotonic()
        t0 = time.time()
        error: Optional[str] = None
        items: List[NormalizedNewsItem] = []

        try:
            req = urllib_request.Request(self.rss_url, headers=_DEFAULT_HEADERS)
            with urllib_request.urlopen(req, timeout=self.timeout) as resp:
                xml_bytes = resp.read()
            raw_entries = _parse_feed(xml_bytes)
            for entry in raw_entries[: self.max_items]:
                item = normalize_rss_entry(
                    entry,
                    source_name=self.source_name,
                    source_domain=self.source_id,
                    category=self.category,
                )
                if item:
                    items.append(item)
            self._last_fetch_ts = now_ts
            self._backoff_until_ts = 0.0
            logger.debug("[RSS:%s] fetched %d items in %.1fs",
                         self.source_id, len(items), time.time() - t0)

        except HTTPError as exc:
            error = f"HTTP {exc.code}: {exc.reason}"
            backoff = 600 if exc.code == 429 else self.fetch_interval_seconds * 2
            self._backoff_until_ts = now_ts + backoff
            logger.warning("[RSS:%s] %s — backing off %ds", self.source_id, error, backoff)

        except URLError as exc:
            error = f"URLError: {exc.reason}"
            self._backoff_until_ts = now_ts + self.fetch_interval_seconds * 2
            logger.warning("[RSS:%s] %s", self.source_id, error)

        except ValueError as exc:
            error = str(exc)
            self._backoff_until_ts = now_ts + self.fetch_interval_seconds * 2
            logger.warning("[RSS:%s] parse error: %s", self.source_id, error)

        except Exception as exc:
            error = f"Unexpected: {exc}"
            self._backoff_until_ts = now_ts + self.fetch_interval_seconds * 2
            logger.exception("[RSS:%s] unexpected error", self.source_id)

        self._last_fetch_ts = now_ts
        return items, error
