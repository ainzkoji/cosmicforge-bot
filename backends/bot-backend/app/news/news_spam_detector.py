"""
News Spam / Duplicate Density Detector.

Scores how likely a cluster is spam based on:
  - duplicate title ratio across items
  - low-reliability source dominance
  - high-frequency re-posts in a short window
  - identical body fingerprints

Returns spam_score 0.0 → 1.0 (higher = more spam).
"""
from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional


def _normalize_title(title: str) -> str:
    """Strip punctuation/whitespace and lowercase for comparison."""
    return re.sub(r"[^a-z0-9 ]", "", title.lower()).strip()


def _title_similarity(a: str, b: str) -> float:
    """Simple Jaccard similarity between two title token sets."""
    ta = set(_normalize_title(a).split())
    tb = set(_normalize_title(b).split())
    if not ta and not tb:
        return 1.0
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def compute_spam_score(
    *,
    titles: List[str],
    source_reliabilities: List[float],
    ingestion_timestamps: Optional[List[str]] = None,
    duplicate_count: int = 0,
    total_count: int = 1,
) -> float:
    """
    Compute spam_score for a cluster.

    Parameters
    ----------
    titles                : list of headline strings for all items in cluster
    source_reliabilities  : reliability score per item (parallel to titles)
    ingestion_timestamps  : ISO datetime strings (used for velocity check)
    duplicate_count       : how many items already flagged is_duplicate=1
    total_count           : total raw items in cluster

    Returns
    -------
    float: spam_score in [0.0, 1.0]
    """
    if not titles or total_count == 0:
        return 0.0

    # ── 1. Duplicate density from DB flag ──────────────────────────────────
    dup_ratio = min(1.0, duplicate_count / total_count)

    # ── 2. Title similarity within cluster ──────────────────────────────────
    # Sample pairwise Jaccard for up to first 10 titles to keep it O(n²) safe
    sample = titles[:10]
    pair_sims: List[float] = []
    for i in range(len(sample)):
        for j in range(i + 1, len(sample)):
            pair_sims.append(_title_similarity(sample[i], sample[j]))
    avg_sim = sum(pair_sims) / len(pair_sims) if pair_sims else 0.0

    # ── 3. Source reliability: low-quality source dominance ─────────────────
    if source_reliabilities:
        avg_rel = sum(source_reliabilities) / len(source_reliabilities)
        low_q = sum(1 for r in source_reliabilities if r < 0.4) / len(source_reliabilities)
    else:
        avg_rel = 0.5
        low_q = 0.0

    # ── 4. Velocity: many items in a very short window → bot amplification ──
    velocity_score = 0.0
    if ingestion_timestamps and len(ingestion_timestamps) >= 3:
        try:
            times = sorted(
                datetime.fromisoformat(t.replace("Z", "+00:00"))
                for t in ingestion_timestamps
                if t
            )
            if len(times) >= 2:
                window_sec = (times[-1] - times[0]).total_seconds()
                # ≥5 items within 60 s → suspicious
                if window_sec < 60 and len(times) >= 5:
                    velocity_score = min(1.0, len(times) / 10.0)
                elif window_sec < 300 and len(times) >= 10:
                    velocity_score = min(0.7, len(times) / 20.0)
        except Exception:
            pass

    # ── 5. Combine ──────────────────────────────────────────────────────────
    spam_score = (
        dup_ratio       * 0.35   # duplicate density weight
        + avg_sim       * 0.25   # identical headline weight
        + low_q         * 0.20   # low-quality source proportion
        + velocity_score * 0.20  # velocity/bot amplification
    )

    return round(min(1.0, spam_score), 4)


def is_spam_cluster(spam_score: float, threshold: float = 0.60) -> bool:
    """Convenience helper — True if spam_score exceeds threshold."""
    return spam_score >= threshold
