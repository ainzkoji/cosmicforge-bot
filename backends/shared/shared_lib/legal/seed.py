"""Seed documents: DRAFTS ONLY, clearly labelled, never counsel-approved.

The only seeded text is the product's existing Risk Disclosure (the content of
``frontends/user-frontend/src/pages/legal/RiskDisclosure.tsx``), published as
version 1 of ``TRADING_RISK_DISCLOSURE`` with ``review_status =
DRAFT_PENDING_COUNSEL`` and a DRAFT notice at the top of its body.

No Terms of Service, Privacy Policy or other legal text is invented here:
those categories stay unpublished until real (counsel-reviewed) text is
published through the admin API. Seeding happens only when the category has
no document at all, so it never adds a version on top of published ones.
"""
from __future__ import annotations

from datetime import datetime, timezone

DRAFT_NOTICE = (
    "DRAFT - PENDING COUNSEL REVIEW. This text has not been reviewed or approved by legal counsel "
    "and is not a final legal document. It will be replaced by a reviewed version."
)

RISK_DISCLOSURE_TITLE = "Risk Disclosure (Draft - pending counsel review)"

RISK_DISCLOSURE_BODY = f"""{DRAFT_NOTICE}

Please read this page before you use CosmicForge. It explains what the product does and the risks you take when you use it. If you do not understand or accept these risks, do not use the product with real money.

## What the product does

- CosmicForge is software that automates order placement on your own exchange account. It connects through API keys that you create at your exchange and provide to us.
- We do not hold customer funds. Your money stays in your account at your exchange, and deposits and withdrawals are made by you at the exchange.
- The software follows rules and the settings you choose (for example capital budget, position size and daily loss limit). It does not know your personal circumstances.

## Risks

- Loss of capital. You can lose some or all of the money you trade with.
- Leverage and liquidation. Futures and other leveraged products multiply losses as well as gains. A small price move against a leveraged position can lead to liquidation by the exchange and the loss of your margin.
- Software, connectivity and exchange failures. Our software, your connection, third-party services or the exchange can fail, be slow or be unavailable. Orders may be delayed, rejected, partly filled or filled at a different price than expected, and protective orders may not execute. Positions can remain open while the software is not running.
- The strategy may lose money. No trading strategy works in all market conditions. A strategy that performed well in the past can lose money in the future.
- No guarantee of profit. We do not promise or guarantee any profit, return, win rate or maximum loss. Risk settings and loss limits reduce exposure but do not cap your loss.
- Simulated results differ from live results. Paper trading, demo accounts and backtests do not reflect real fills, fees, slippage, liquidity or your own behaviour under pressure. Past or simulated performance does not indicate future results.

## Your responsibilities

- API keys. Create keys with trading permission only. Never enable withdrawal permission on a key you give to us or to anyone else. Restrict keys by IP address where your exchange allows it, and revoke a key at the exchange if you think it has been exposed.
- Monitoring. Check your bots, open positions and exchange account regularly. Stopping a bot does not close positions that are already open; close them at your exchange if you want them closed.
- Your own decisions. You decide whether to trade, how much capital to use and which settings to choose. Only use money you can afford to lose.
- Taxes and rules. You are responsible for your own tax reporting and for checking that using this product and trading these instruments is permitted where you live.

## Not investment advice

Nothing in the product or on this website is investment, financial, legal or tax advice, or a recommendation to buy or sell any instrument. Signals, statistics and educational content are provided for information only. If you are unsure, speak to an independent, qualified adviser.
"""

SEED_DOCUMENTS = (
    ("TRADING_RISK_DISCLOSURE", "en", RISK_DISCLOSURE_TITLE, RISK_DISCLOSURE_BODY),
)


def seed_draft_documents(conn) -> int:
    """Insert each seed draft whose category has no document yet. Returns the count added."""
    from shared_lib.legal.documents import DRAFT, content_sha256

    added = 0
    now_iso = datetime.now(timezone.utc).isoformat()
    for category, locale, title, body in SEED_DOCUMENTS:
        if conn.execute("SELECT 1 FROM legal_documents WHERE category = ? LIMIT 1", (category,)).fetchone():
            continue
        doc_id = f"legal_{category.lower()}_{locale}_v1_seed"
        conn.execute(
            """INSERT OR IGNORE INTO legal_documents (doc_id, category, version, locale, title, body, content_sha256,
                   published_at, published_by, material_change, change_summary, review_status)
               VALUES (?, ?, 1, ?, ?, ?, ?, ?, 'seed', 1, ?, ?)""",
            (doc_id, category, locale, title, body, content_sha256(title, body), now_iso,
             "Draft seeded from the product's existing risk disclosure page", DRAFT),
        )
        conn.execute(
            "INSERT INTO legal_audit_log (occurred_at, actor_type, actor_id, action, doc_id, category, details_json)"
            " VALUES (?, 'system', 'seed', 'draft_document_seeded', ?, ?, ?)",
            (now_iso, doc_id, category, '{"review_status": "DRAFT_PENDING_COUNSEL"}'),
        )
        added += 1
    return added


__all__ = ["DRAFT_NOTICE", "SEED_DOCUMENTS", "seed_draft_documents"]
