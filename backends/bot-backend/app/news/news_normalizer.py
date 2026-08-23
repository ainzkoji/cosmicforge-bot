"""
Normalizes raw provider data (RSS entries, API payloads, manual imports)
into a single NormalizedNewsItem dataclass that feeds raw_news_items.
"""
from __future__ import annotations

import hashlib
import html
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional
from urllib.parse import urlparse


@dataclass
class NormalizedNewsItem:
    provider: str           # e.g. "rss:coindesk", "api:cryptopanic", "manual"
    source_name: str        # human label e.g. "CoinDesk"
    source_domain: str      # e.g. "coindesk.com"
    source_url: str
    title: str
    body_snippet: str
    published_utc: str      # always UTC ISO-8601
    ingested_utc: str
    language: str = "en"
    category: str = "CRYPTO"   # CRYPTO / MACRO / MARKET / GENERAL
    raw_payload_json: str = "{}"
    external_id: Optional[str] = None   # URL-hash used for dedup
    latency_seconds: Optional[float] = None


_HTML_TAG_RE = re.compile(
    r"</?(?:a|article|aside|b|blockquote|br|code|dd|div|dl|dt|em|figcaption|figure|footer|h[1-6]|header|hr|i|img|li|main|ol|p|pre|section|small|source|span|strong|sub|sup|table|tbody|td|tfoot|th|thead|tr|u|ul|video)[^>]*>",
    re.IGNORECASE,
)


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = html.unescape(text or "")
    text = _HTML_TAG_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_domain(url: str) -> str:
    try:
        host = urlparse(url).hostname or ""
        return host.lstrip("www.")
    except Exception:
        return ""


def _to_utc_iso(raw: str) -> str:
    """
    Parse RFC2822 (RSS pubDate) or ISO8601 to UTC ISO string.
    Falls back to now() on any parse failure.
    """
    if not raw:
        return datetime.now(timezone.utc).isoformat()
    raw = raw.strip()
    # Try RFC2822 (standard RSS pubDate: "Mon, 01 Jan 2024 12:00:00 +0000")
    try:
        dt = parsedate_to_datetime(raw)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    # Try ISO8601 variants
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
        try:
            raw_clean = raw.replace("Z", "+00:00")
            dt = datetime.fromisoformat(raw_clean)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            continue
    return datetime.now(timezone.utc).isoformat()


def _url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:20]


def _compute_latency_seconds(published_utc: str, ingested_utc: str) -> Optional[float]:
    try:
        published = datetime.fromisoformat(published_utc.replace("Z", "+00:00"))
        ingested = datetime.fromisoformat(ingested_utc.replace("Z", "+00:00"))
        return max(0.0, (ingested - published).total_seconds())
    except Exception:
        return None


def normalize_rss_entry(
    entry: dict,
    source_name: str,
    source_domain: str,
    category: str = "CRYPTO",
) -> Optional[NormalizedNewsItem]:
    """
    Convert a parsed RSS entry dict (from rss_provider_client) to NormalizedNewsItem.
    Returns None if title is missing.
    """
    title = _strip_html(entry.get("title", ""))
    if not title:
        return None

    link = entry.get("link") or entry.get("url") or ""
    body_raw = entry.get("summary") or entry.get("description") or entry.get("content") or ""
    body = _strip_html(body_raw)[:500]

    published_raw = (
        entry.get("published")
        or entry.get("pubDate")
        or entry.get("updated")
        or ""
    )

    domain = _extract_domain(link) or source_domain
    ingested = datetime.now(timezone.utc).isoformat()

    return NormalizedNewsItem(
        provider=f"rss:{source_domain}",
        source_name=source_name,
        source_domain=domain,
        source_url=link,
        title=title,
        body_snippet=body,
        published_utc=_to_utc_iso(published_raw),
        ingested_utc=ingested,
        language="en",
        category=category,
        external_id=_url_hash(link) if link else _url_hash(title),
        latency_seconds=_compute_latency_seconds(_to_utc_iso(published_raw), ingested),
    )


def normalize_manual_entry(
    title: str,
    source_name: str,
    source_url: str,
    published_utc_raw: str,
    body_snippet: str = "",
    category: str = "CRYPTO",
) -> NormalizedNewsItem:
    """Convert a manually-entered news item to NormalizedNewsItem."""
    domain = _extract_domain(source_url) or "manual"
    ingested = datetime.now(timezone.utc).isoformat()
    link = source_url or ""
    return NormalizedNewsItem(
        provider="manual",
        source_name=source_name or "Manual Import",
        source_domain=domain,
        source_url=link,
        title=_strip_html(title),
        body_snippet=_strip_html(body_snippet)[:500],
        published_utc=_to_utc_iso(published_utc_raw),
        ingested_utc=ingested,
        language="en",
        category=category,
        external_id=_url_hash(link or title),
        latency_seconds=_compute_latency_seconds(_to_utc_iso(published_utc_raw), ingested),
    )
