"""
Rule-based narrative classifier for news clusters.

18 narrative types, keyword matching only — no ML.
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

from shared_lib.persistence.db import DB
from shared_lib.persistence.news_intelligence import upsert_narrative


# (narrative_type, narrative_label, keywords, confidence_weight)
_NARRATIVE_RULES: List[Tuple[str, str, List[str], float]] = [
    ("REGULATORY_ACTION", "Regulatory action / legal ruling", [
        "sec", "cftc", "fca", "ban", "regulation", "lawsuit", "court", "fine",
        "sanction", "illegal", "probe", "investigation", "compliance", "enforcement",
    ], 0.85),
    ("HACK_EXPLOIT", "Security breach / exploit", [
        "hack", "exploit", "breach", "stolen", "theft", "vulnerability",
        "attack", "drained", "rug pull", "scam", "phishing", "malware",
    ], 0.90),
    ("ETF_APPROVAL", "ETF / institutional product approval", [
        "etf", "spot etf", "approved", "approval", "blackrock", "fidelity",
        "vanguard", "grayscale", "fund approval", "institutional product",
    ], 0.80),
    ("MACRO_ECONOMIC", "Macro-economic event / policy", [
        "fed", "federal reserve", "interest rate", "inflation", "cpi", "ppi",
        "gdp", "recession", "monetary policy", "quantitative", "fomc", "powell",
        "treasury", "yield", "dollar", "dxy",
    ], 0.75),
    ("WHALE_MOVEMENT", "Large wallet / whale movement", [
        "whale", "large transfer", "wallet", "moved", "cold storage",
        "exchange inflow", "exchange outflow", "large holder", "dormant",
    ], 0.70),
    ("EXCHANGE_NEWS", "Exchange listing / delisting / outage", [
        "listed on", "delisted", "listing", "binance lists", "coinbase adds",
        "trading suspended", "outage", "maintenance", "exchange down",
    ], 0.80),
    ("PROTOCOL_UPGRADE", "Protocol upgrade / hard fork", [
        "upgrade", "hard fork", "soft fork", "hardfork", "testnet", "mainnet launch",
        "protocol update", "v2", "v3", "migration", "merge",
    ], 0.80),
    ("PARTNERSHIP_ADOPTION", "Partnership / enterprise adoption", [
        "partnership", "collaboration", "integration", "adopted by", "deal with",
        "enterprise", "corporate", "agreement", "signed", "announced partnership",
    ], 0.70),
    ("MARKET_SENTIMENT", "General market sentiment shift", [
        "bullish", "bearish", "fear", "greed", "sentiment", "mood",
        "outlook", "market cap", "dominance", "recovery", "correction",
    ], 0.60),
    ("DEFI_EVENT", "DeFi protocol event", [
        "defi", "liquidity pool", "yield farming", "staking", "tvl",
        "protocol", "smart contract", "governance", "dao", "apy", "amm",
    ], 0.75),
    ("NFT_EVENT", "NFT market event", [
        "nft", "non-fungible", "opensea", "blur", "mint", "collection",
        "floor price", "royalty", "digital art", "metaverse",
    ], 0.70),
    ("STABLECOIN_EVENT", "Stablecoin / depeg event", [
        "stablecoin", "depeg", "usdt", "usdc", "busd", "dai",
        "tether", "circle", "peg", "collateral",
    ], 0.85),
    ("MINER_NETWORK", "Mining / network health event", [
        "miner", "mining", "hashrate", "hash rate", "difficulty",
        "block reward", "halving", "pool", "asic", "proof of work",
    ], 0.75),
    ("FUNDING_INVESTMENT", "Funding / investment round", [
        "raised", "funding", "investment", "venture", "series a", "series b",
        "vc", "capital", "seed round", "valuation", "acquisition",
    ], 0.70),
    ("GEOPOLITICAL", "Geopolitical / country-level event", [
        "china", "russia", "ukraine", "war", "sanctions", "geopolit",
        "government", "nation", "country", "central bank", "cbdc", "ban in",
    ], 0.75),
    ("TECHNICAL_ANALYSIS", "Technical analysis / price target", [
        "resistance", "support", "breakout", "breakdown", "rsi", "macd",
        "moving average", "fibonacci", "chart", "pattern", "target price",
    ], 0.55),
    ("RUMOR_SPECULATION", "Rumor / unverified speculation", [
        "rumor", "reportedly", "sources say", "unconfirmed", "speculation",
        "could be", "might be", "possibly", "alleged", "claim",
    ], 0.50),
    ("GENERAL_CRYPTO_NEWS", "General crypto industry news", [
        "crypto", "bitcoin", "blockchain", "web3", "digital asset",
        "cryptocurrency", "token", "coin",
    ], 0.40),
]


def _match_score(text: str, keywords: List[str]) -> Tuple[float, List[str]]:
    text_lower = text.lower()
    matched = [kw for kw in keywords if re.search(r"\b" + re.escape(kw) + r"\b", text_lower)]
    if not matched:
        return 0.0, []
    ratio = len(matched) / max(1, min(len(keywords), 5))
    return min(1.0, ratio), matched


class NewsNarrativeClassifier:
    def __init__(self, db: DB) -> None:
        self._db = db

    def classify_and_store(
        self,
        cluster_id: int,
        canonical_title: str,
        body_text: str = "",
        source_count: int = 1,
    ) -> List[Dict]:
        """
        Returns list of matched narrative dicts, persists top results to DB.
        """
        text = f"{canonical_title} {body_text}"
        results = []

        for narrative_type, label, keywords, base_conf in _NARRATIVE_RULES:
            raw_score, matched_kws = _match_score(text, keywords)
            if raw_score == 0.0:
                continue

            # Confidence = base weight * match ratio, boosted by source count
            source_boost = min(0.15, (source_count - 1) * 0.03)
            confidence = min(0.95, base_conf * raw_score + source_boost)

            is_manipulation_related = narrative_type in (
                "HACK_EXPLOIT", "RUMOR_SPECULATION"
            )

            upsert_narrative(
                self._db,
                cluster_id=cluster_id,
                narrative_type=narrative_type,
                narrative_confidence=confidence,
                matched_keywords=",".join(matched_kws),
            )
            results.append({
                "narrative_type": narrative_type,
                "label": label,
                "confidence": confidence,
                "keywords_matched": matched_kws,
                "is_manipulation_related": is_manipulation_related,
            })

        results.sort(key=lambda x: x["confidence"], reverse=True)
        return results

    def top_narrative(self, narratives: List[Dict]) -> Optional[str]:
        """Returns the highest-confidence narrative_type, or None."""
        if not narratives:
            return None
        return narratives[0]["narrative_type"]
