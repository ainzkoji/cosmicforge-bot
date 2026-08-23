"""
Reuters-compatible provider placeholder.

Disabled by default. Returns [] without a valid API key.
When Reuters/Dow Jones/Bloomberg connectivity is available,
implement `fetch()` by targeting their respective REST endpoints.

Current status: DISABLED — awaiting enterprise API access.
"""
from __future__ import annotations

import logging
from typing import List, Optional, Tuple

from app.news.news_normalizer import NormalizedNewsItem

logger = logging.getLogger(__name__)

_PROVIDER = "reuters"


class ReutersClient:
    """
    Placeholder for Reuters / Dow Jones / Bloomberg-compatible provider.

    Design notes for future implementor:
    - Reuters Connect API: https://api.reutersconnect.com/content/v1
    - Dow Jones DNA: https://developer.dowjones.com/
    - Bloomberg Enterprise Data: requires enterprise agreement
    - Implement `fetch()` returning List[NormalizedNewsItem]
    - Use `NormalizedNewsItem.category = "MACRO"` for macro news
    """

    def __init__(self, api_key: str = "", timeout: int = 10, max_items: int = 100) -> None:
        self._api_key = api_key.strip()
        self._timeout = timeout
        self._max_items = max_items

    @property
    def provider(self) -> str:
        return _PROVIDER

    def is_enabled(self) -> bool:
        # Requires an explicit API key — never accidentally enabled
        return False  # hard-disabled until key + endpoint is configured

    def is_due(self, interval_seconds: int) -> bool:
        return False  # never due while disabled

    def fetch(self) -> Tuple[List[NormalizedNewsItem], Optional[str], float]:
        """Returns empty list — implementation pending enterprise API access."""
        if not self._api_key:
            logger.debug("[Reuters] disabled — no API key")
            return [], "Provider not configured (no API key)", 0.0
        # Future: implement Reuters Connect or Dow Jones DNA fetch here
        return [], "Reuters provider not yet implemented", 0.0
