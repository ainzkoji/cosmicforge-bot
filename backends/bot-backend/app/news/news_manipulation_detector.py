"""
News Manipulation Detection Engine.

Detects signs that a cluster may be manipulated / synthetic hype.

Flags:
    POSSIBLE_MANIPULATION   – general red flags present
    LOW_CONFIDENCE_EVENT    – most sources are unverified / untrusted
    RUMOR_ONLY              – all items are speculation/unverified
    BOT_AMPLIFICATION       – viral velocity from low-quality accounts

Returns a manipulation_flag string or None (clean).
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple


# Narrative types that imply speculation / rumor
_RUMOR_NARRATIVES = {"RUMOR_SPECULATION", "GENERAL_CRYPTO_NEWS"}

# Narrative types that are commonly co-opted in pumps
_HIGH_RISK_NARRATIVES = {
    "WHALE_MOVEMENT", "EXCHANGE_NEWS", "FUNDING_INVESTMENT",
    "PARTNERSHIP_ADOPTION", "MARKET_SENTIMENT",
}


def detect_manipulation(
    *,
    source_reliabilities: List[float],
    source_domains: List[str],
    spam_score: float,
    narrative_types: List[str],
    source_count: int,
    provider_count: int,
    velocity_items_per_minute: float = 0.0,
    is_manipulation_suspect: bool = False,
) -> Optional[str]:
    """
    Analyse cluster characteristics and return a manipulation flag or None.

    Parameters
    ----------
    source_reliabilities        : per-item reliability scores
    source_domains              : per-item source domain strings
    spam_score                  : output from compute_spam_score()
    narrative_types             : list of matched narrative_type strings
    source_count                : total sources in cluster
    provider_count              : distinct provider domains
    velocity_items_per_minute   : items ingested per minute (0 if unknown)
    is_manipulation_suspect     : pre-existing flag from clustering engine

    Returns
    -------
    str | None – e.g. "POSSIBLE_MANIPULATION", "RUMOR_ONLY", etc.
    """
    flags: List[Tuple[str, float]] = []  # (flag_name, score)

    avg_rel = (
        sum(source_reliabilities) / len(source_reliabilities)
        if source_reliabilities else 0.5
    )
    low_q_ratio = (
        sum(1 for r in source_reliabilities if r < 0.35) / len(source_reliabilities)
        if source_reliabilities else 0.0
    )

    unique_domains = len(set(d.lower() for d in source_domains if d))

    # ── Rule 1: Almost all sources are low-quality ──────────────────────────
    # Fixed lower score so more-specific flags (BOT_AMPLIFICATION, RUMOR_ONLY) win
    if avg_rel < 0.30 and source_count >= 2:
        flags.append(("LOW_CONFIDENCE_EVENT", 0.55))

    # ── Rule 2: Rumor-only narratives with no high-quality backing ───────────
    nar_set = set(narrative_types)
    all_rumor = nar_set and nar_set.issubset(_RUMOR_NARRATIVES)
    if all_rumor and avg_rel < 0.50:
        flags.append(("RUMOR_ONLY", 0.90))

    # ── Rule 3: Single-provider cluster with many items → bot amplification ─
    if provider_count == 1 and source_count >= 5 and avg_rel < 0.40:
        flags.append(("BOT_AMPLIFICATION", 0.85))

    # ── Rule 4: High velocity from low-quality domains ───────────────────────
    if velocity_items_per_minute > 3.0 and avg_rel < 0.45:
        flags.append(("BOT_AMPLIFICATION", min(1.0, velocity_items_per_minute / 10)))

    # ── Rule 5: High spam score + low unique domain diversity ────────────────
    if spam_score > 0.55 and unique_domains <= 2 and source_count >= 4:
        flags.append(("POSSIBLE_MANIPULATION", spam_score))

    # ── Rule 6: Pre-existing DB flag ─────────────────────────────────────────
    if is_manipulation_suspect:
        flags.append(("POSSIBLE_MANIPULATION", 0.80))

    # ── Rule 7: High-risk narrative with low-quality source surge ────────────
    high_risk_present = bool(nar_set & _HIGH_RISK_NARRATIVES)
    if high_risk_present and low_q_ratio > 0.70 and source_count >= 3:
        flags.append(("POSSIBLE_MANIPULATION", low_q_ratio * 0.90))

    if not flags:
        return None

    # Return the flag with the highest associated score
    flags.sort(key=lambda x: x[1], reverse=True)
    return flags[0][0]


# Priority order for combining flags when several fire at once
MANIPULATION_FLAG_SEVERITY = {
    "POSSIBLE_MANIPULATION": 4,
    "BOT_AMPLIFICATION": 3,
    "RUMOR_ONLY": 2,
    "LOW_CONFIDENCE_EVENT": 1,
}


def flag_severity(flag: Optional[str]) -> int:
    if flag is None:
        return 0
    return MANIPULATION_FLAG_SEVERITY.get(flag, 0)
