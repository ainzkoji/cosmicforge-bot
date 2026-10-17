"""Step 7.2 -- the market availability guard, the versioned definition files, and the per-family market
declarations (kept outside FamilyDefinition so no definition hash moves)."""
from __future__ import annotations

import copy
import json

import pytest

import _step7 as F
from app.markets import guard as G
from app.markets.calendar import local_ms, utc_ms
from app.markets.contracts import ContractSpec
from app.markets.registry import DATA_DIR, MarketRegistry, MarketRegistryError, default_registry
from app.trading_intelligence.pipeline import family_markets as FM
from app.trading_intelligence.pipeline.family_registry import definitions

CHI, NY = "America/Chicago", "America/New_York"
#: DAILY_TREND's definition hash before Step 7 (computed 2026-10-17 at 22a80bc, before any Step 7 change).
DAILY_TREND_DEFINITION_HASH = "09d33c15dc08f8ebefb0cc42d122116fe1523207f3a5d43391a6649664481527"


@pytest.fixture
def guard():
    return G.MarketAvailabilityGuard(F.registry())


# ── the guard ──────────────────────────────────────────────────────────────

def test_crypto_is_never_refused_for_calendar_reasons(guard):
    for ts in (utc_ms(2026, 10, 17, 12), utc_ms(2026, 12, 25), utc_ms(2026, 3, 8, 8)):
        assert guard.refuse_reason("CRYPTO_BINANCE_USDT_PERPETUAL", ts) is None
    shipped = G.MarketAvailabilityGuard()
    for market in default_registry().crypto_markets():
        assert shipped.refuse_reason(market.market_id, utc_ms(2026, 10, 18, 3)) is None


def test_a_closed_market_is_refused_with_its_next_open(guard):
    saturday = local_ms(CHI, 2026, 10, 17, 12)
    out = guard.evaluate("FIX_FUTURES", saturday)
    assert out["reason"] == G.MARKET_CLOSED and out["next_open_ms"] == local_ms(CHI, 2026, 10, 18, 17)
    assert guard.refuse_reason("FIX_FUTURES", local_ms(CHI, 2026, 12, 24, 20)) == G.MARKET_CLOSED     # holiday
    assert guard.refuse_reason("FIX_FUTURES", local_ms(CHI, 2026, 10, 20, 3, 5)) == G.MARKET_CLOSED   # maintenance
    assert guard.refuse_reason("FIX_FUTURES", local_ms(CHI, 2026, 10, 19, 20)) is None


def test_an_unverified_calendar_is_refused_even_when_it_would_say_open(guard):
    assert guard.refuse_reason("FIX_UNVERIFIED", local_ms(CHI, 2026, 10, 19, 20)) == G.CALENDAR_UNVERIFIED
    shipped = G.MarketAvailabilityGuard()
    for market in default_registry().tradfi_markets():
        assert shipped.refuse_reason(market.market_id, utc_ms(2026, 10, 20, 15)) == G.CALENDAR_UNVERIFIED


def test_outside_the_calendar_coverage_is_unverified(guard):
    assert guard.refuse_reason("FIX_FUTURES", local_ms(CHI, 2028, 3, 7, 10)) == G.CALENDAR_UNVERIFIED


def test_expired_and_unlisted_contracts_are_refused_before_the_calendar(guard):
    c = ContractSpec.from_plain(F.contract_doc("FX1-A", local_ms(CHI, 2026, 12, 18, 8, 30),
                                               listing_ms=local_ms(CHI, 2026, 6, 1)))
    assert guard.refuse_reason("FIX_FUTURES", local_ms(CHI, 2026, 5, 4, 10), c) == G.CONTRACT_NOT_LISTED
    assert guard.refuse_reason("FIX_FUTURES", local_ms(CHI, 2026, 12, 19, 12), c) == G.CONTRACT_EXPIRED   # Saturday too
    assert guard.refuse_reason("FIX_FUTURES", local_ms(CHI, 2026, 12, 18, 8, 29), c) is None


def test_data_staleness_counts_only_open_market_bars(guard):
    # hourly freshness, two missing bars allowed, New York cash session
    monday_1530 = local_ms(NY, 2026, 10, 19, 15, 30)
    assert guard.refuse_reason("FIX_CASH", monday_1530, last_bar_open_ms=utc_ms(2026, 10, 19, 17), check_data=True) is None
    assert guard.refuse_reason("FIX_CASH", monday_1530, last_bar_open_ms=utc_ms(2026, 10, 19, 14),
                               check_data=True) == G.DATA_STALE
    # Friday's last bar is not stale on Monday morning: the weekend is not counted
    monday_1000 = local_ms(NY, 2026, 10, 19, 10, 0)
    assert guard.refuse_reason("FIX_CASH", monday_1000, last_bar_open_ms=utc_ms(2026, 10, 16, 19), check_data=True) is None
    assert guard.refuse_reason("FIX_CASH", monday_1000, last_bar_open_ms=None, check_data=True) == G.DATA_STALE
    # crypto, daily: yesterday's bar is fresh, three days ago is stale
    ts = utc_ms(2026, 10, 17, 12)
    assert guard.refuse_reason("CRYPTO_BINANCE_USDT_PERPETUAL", ts, last_bar_open_ms=utc_ms(2026, 10, 16),
                               check_data=True) is None
    assert guard.refuse_reason("CRYPTO_BINANCE_USDT_PERPETUAL", ts, last_bar_open_ms=utc_ms(2026, 10, 14),
                               check_data=True) == G.DATA_STALE


def test_an_unknown_market_is_refused(guard):
    assert guard.refuse_reason("NOWHERE", utc_ms(2026, 1, 1)) == G.MARKET_UNKNOWN


# ── the definition files ───────────────────────────────────────────────────

def test_the_shipped_definitions_load_and_every_entry_names_source_and_status():
    reg = MarketRegistry.load()
    assert reg.data_versions == {"calendars": "2026-10-17.1", "markets": "2026-10-17.1", "contracts": "2026-10-17.1"}
    for name in ("calendars.json", "markets.json", "contracts.json"):
        doc = json.loads((DATA_DIR / name).read_text(encoding="utf-8"))
        entries = [e for key in ("calendars", "markets", "contracts", "series") for e in doc.get(key, [])]
        assert entries and all(e.get("source") and e.get("verification_status") for e in entries), name
    # every traditional-market definition shipped here is an UNVERIFIED example
    assert reg.tradfi_markets() and all(m.verification_status == "UNVERIFIED" for m in reg.tradfi_markets())
    assert all(reg.calendar(m.calendar_id).verification_status == "UNVERIFIED" for m in reg.tradfi_markets())
    assert all(c.verification_status == "UNVERIFIED" for c in reg.contracts.values())
    assert reg.calendar("CRYPTO_24_7").is_continuous
    assert {m.venue for m in reg.crypto_markets()} == {"binance", "bybit", "bingx"}


def _docs():
    load = lambda n: json.loads((DATA_DIR / n).read_text(encoding="utf-8"))  # noqa: E731
    return load("calendars.json"), load("markets.json"), load("contracts.json")


@pytest.mark.parametrize("mutate", [
    lambda c, m, k: m["markets"][0].update(calendar_id="NOPE"),
    lambda c, m, k: m["markets"].append(copy.deepcopy(m["markets"][0])),
    lambda c, m, k: m["markets"][0].pop("source"),
    lambda c, m, k: c["calendars"][1].update(verification_status="PLATFORM_CONVENTION"),   # only for continuous crypto
    lambda c, m, k: m["markets"][3].update(verification_status="PLATFORM_CONVENTION"),
    lambda c, m, k: c["calendars"][1].update(verification_status="VERIFIED"),              # VERIFIED needs coverage
    lambda c, m, k: k["series"][0]["contracts"].append("MISSING"),
    lambda c, m, k: c.update(schema_version="2"),
])
def test_inconsistent_definitions_are_refused(mutate):
    c, m, k = _docs()
    mutate(c, m, k)
    with pytest.raises((MarketRegistryError, ValueError)):
        MarketRegistry.from_documents(calendars=c, markets=m, contracts=k)


# ── family market declarations ─────────────────────────────────────────────

def test_every_registry_family_declares_its_markets_and_they_agree_with_its_definition():
    reg = default_registry()
    defs = definitions()
    assert set(defs) == set(FM.FAMILY_MARKETS), "every family needs a market declaration (and only real families)"
    for family_id, d in defs.items():
        decl = FM.FAMILY_MARKETS[family_id]
        declared = {(reg.market(m).venue, reg.market(m).product) for m in decl.markets}
        assert declared == set(d.supported_markets), family_id
        assert {reg.market(m).calendar_id for m in decl.markets} == set(decl.calendars)
        assert {reg.market(m).market_type for m in decl.markets} == set(decl.market_types)


def test_daily_trend_is_crypto_only_and_its_definition_hash_is_unchanged():
    decl = FM.declaration("DAILY_TREND")
    assert decl.markets == ("CRYPTO_BINANCE_USDT_PERPETUAL",) and decl.calendars == ("CRYPTO_24_7",)
    assert decl.market_types == ("CRYPTO_USDT_PERPETUAL",)
    assert definitions()["DAILY_TREND"].definition_hash == DAILY_TREND_DEFINITION_HASH
    assert not hasattr(definitions()["DAILY_TREND"], "market_scope")


def test_no_family_declares_a_traditional_market():
    for market in default_registry().tradfi_markets():
        assert FM.families_supporting(market.market_id) == ()
