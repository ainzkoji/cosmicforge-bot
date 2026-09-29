"""CATI Section 27: definition-of-done acceptance -- code-level invariants behind every DoD line. Data-completion
states are READ (never asserted complete here); the live research databases are never touched by this suite."""
from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]


def _coverage():
    return json.loads((REPO / "docs/research/coverage/crypto_broad_binance_v1.coverage.json").read_text(encoding="utf-8"))


# 27.1 crypto data
def test_at_least_100_validated_broad_crypto_instruments_with_lineage_and_young_symbol_policy():
    c = _coverage()
    agg = c["aggregate"]
    assert agg["complete_symbols"] >= 100 and agg["complete_symbols"] == agg["symbols"]
    assert agg["gaps"] == agg["invalid_rows"] == agg["duplicate_rows"] == 0 and agg["reconciles_to_db"] is True
    assert c["provenance"] and all(m.get("data_source") for m in c["members"])
    assert agg["young_symbols"] == 0 and "LISTING_TOO_RECENT" in agg["young_symbols_note"]  # explicitly classified
    from app.market_data.research_status import load_manifest

    assert load_manifest("CRYPTO_BROAD")["manifest"]["universe_hash"] == c["universe_hash"]


# 27.2 FX reference data
def test_fx_reference_ingestion_is_provider_neutral_and_needs_no_oanda():
    from app.market_data.research_status import BACKFILL_PROVIDERS, load_manifest

    fx = load_manifest("FX_REFERENCE")
    assert fx["status"] == "FROZEN" and fx["manifest"]["provider"] == "dukascopy"
    assert fx["manifest"]["price_kind"] == "REFERENCE_MARKET_PRICE" and fx["manifest"]["bid_ask_required"] is True
    assert "oanda" not in json.dumps(BACKFILL_PROVIDERS).lower()


# 27.3 / 27.5 dynamic discovery: every API-visible instrument mapped, or visibly unavailable with a reason
@pytest.mark.parametrize("row", [
    {"symbol": "BTCUSDT", "baseCoin": "BTC", "quoteCoin": "USDT", "contractType": "LinearPerpetual", "status": "Trading"},
    {"symbol": "EURUSDT", "baseCoin": "EUR", "quoteCoin": "USDT", "contractType": "LinearPerpetual", "status": "Trading",
     "symbolType": "forex"},
    {"symbol": "XAUUSDT", "baseCoin": "XAU", "quoteCoin": "USDT", "contractType": "LinearPerpetual", "status": "Trading",
     "symbolType": "commodity"},
    {"symbol": "BTCUSDT-26DEC25", "baseCoin": "BTC", "quoteCoin": "USDT", "contractType": "LinearFutures",
     "status": "Trading"},
    {"symbol": "OLDUSDT", "baseCoin": "OLD", "quoteCoin": "USDT", "contractType": "LinearPerpetual", "status": "Closed"},
])
def test_every_discovered_instrument_is_mapped_or_blocked_with_a_reason(row):
    from app.activation.market import instrument_capabilities
    from app.exchange.instruments import parse_bybit_instrument

    ins = parse_bybit_instrument(row)
    assert ins is not None and ins.canonical_symbol and ins.asset_class
    view = instrument_capabilities(ins, broker="bybit", environment="LIVE", permissions={"TRADE": True})
    assert view["lifecycle_stage"] != "CATI_EXECUTABLE"  # nothing becomes executable by being discovered
    assert view["next_blocker"] and view["next_blocker"]["reason"]  # a deterministic, visible reason


# 27.4 same broker multi-asset: CODE PATH READY is not PROVEN EXECUTION READY
def test_same_broker_crypto_and_fx_code_path_without_claiming_proof():
    from app.activation.market import account_market_status
    from app.exchange.instruments import parse_bybit_instrument
    from app.trading_intelligence.venue.registry import ADAPTER_STATUS_REGISTRY

    rows = [parse_bybit_instrument({"symbol": "BTCUSDT", "baseCoin": "BTC", "quoteCoin": "USDT",
                                    "contractType": "LinearPerpetual", "status": "Trading"}),
            parse_bybit_instrument({"symbol": "EURUSDT", "baseCoin": "EUR", "quoteCoin": "USDT", "symbolType": "forex",
                                    "contractType": "LinearPerpetual", "status": "Trading"})]
    demo = account_market_status(broker="bybit", environment="DEMO", permissions={"TRADE": True}, instruments=rows)
    assert demo["markets"]["CRYPTO"]["markets_api_tradable"] == 1 and demo["markets"]["FX"]["markets_api_tradable"] == 1
    assert demo["markets"]["CRYPTO"]["markets_cati_eligible"] == 0 and demo["markets"]["FX"]["markets_cati_eligible"] == 0
    live = account_market_status(broker="bybit", environment="LIVE", permissions={"TRADE": True}, instruments=rows)
    assert live["markets"]["FX"]["execution"]["reason"] == "BROKER_EXECUTION_UNVALIDATED_FOR_LIVE"
    assert not any(v.startswith("bybit") for v, _ in ADAPTER_STATUS_REGISTRY)  # no external validation claimed


# 27.6 / 27.7 capital
def test_unified_logical_segmented_official_route_no_withdrawal():
    from shared_lib.broker.wallets import topology_for

    from app.trading_intelligence.capital.planner import AccountCapitalState, CapitalSettings, plan_capital

    uni = plan_capital(state=AccountCapitalState("a", "USDT", topology_for("bybit", "UNIFIED"), {"UNIFIED": Decimal("500")}),
                       product="CRYPTO_PERPETUAL", required=Decimal("50"), settings=CapitalSettings(), plan_key="u")
    assert uni.outcome == "NO_ACTION_SHARED_COLLATERAL" and uni.transfer is None
    seg = plan_capital(state=AccountCapitalState("a", "USDT", topology_for("binance"),
                                                 {"UMFUTURE": Decimal("1"), "FUNDING": Decimal("500"), "MAIN": None},
                                                 transfer_capability_usable=True),
                       product="CRYPTO_PERPETUAL", required=Decimal("50"), settings=CapitalSettings(), plan_key="s")
    assert seg.needs_transfer and seg.transfer.source_wallet == "FUNDING" and seg.transfer.destination_wallet == "UMFUTURE"
    assert seg.transfer.auto_submit is False and "USER_ACTION_REQUIRED" in seg.reason_codes  # policy-controlled


def test_auto_capital_routing_is_policy_controlled_and_emergency_disabled_blocks():
    from types import SimpleNamespace

    from app.transfers.service import InternalTransferService

    svc = InternalTransferService.__new__(InternalTransferService)
    svc.db = None
    auth = SimpleNamespace(account_id="a")
    row = {"metadata": {}}
    assert svc._automation_block(auth, row, {"emergency_disabled": True}).reason == "AUTOMATION_EMERGENCY_DISABLED"
    assert svc._automation_block(auth, row, {"mode": "MANUAL_TRANSFER"}).reason == "AUTOMATION_NOT_AUTHORIZED"
    assert svc._automation_block(auth, row, {"mode": "AUTOMATED_INTERNAL_REALLOCATION", "authorized_at": "t"}
                                 ).reason == "AUTOMATION_DISABLED"


# 27.11 / 27.12 separate certification universes, holdouts closed, no automatic advancement
def test_certification_universes_separate_and_no_path_advances_authority(tmp_path):
    from app.market_data.research_status import holdout_state, load_manifest
    from app.trading_intelligence.governance.promotion import PromotionGovernance
    from app.trading_intelligence.research.certification.registry import SqliteResearchStore

    crypto, fx = load_manifest("CRYPTO_BROAD")["manifest"], load_manifest("FX_REFERENCE")["manifest"]
    assert crypto["universe_hash"] != fx["universe_hash"] and crypto["execution_authorized"] is False
    assert fx["execution_authorized"] is False and fx["holdout_opened"] is False
    store = SqliteResearchStore(str(tmp_path / "gov.db"))
    assert PromotionGovernance(store).current_phase() == "M0"
    assert holdout_state(store)["state"] == "CLOSED"
    import re

    app = Path(__file__).resolve().parents[1] / "app"
    writers = [p.relative_to(app).as_posix() for p in app.rglob("*.py")
               if re.search(r"INSERT INTO \{?(self\.)?(PHASES|SCOPES)\}?|cati_promotion_phase_history \(",
                            p.read_text(encoding="utf-8", errors="ignore"))]
    assert writers == ["trading_intelligence/governance/promotion.py"]  # only the explicit governance API writes phases
    api_code = "\n".join(p.read_text(encoding="utf-8", errors="ignore") for p in (app / "api").glob("*.py"))
    # no HTTP route can advance governance, grant a live scope, authorize or open a holdout
    for forbidden in ("PromotionGovernance", "grant_scope", "authorize_holdout", "holdouts.open", "HoldoutRegistry("):
        assert forbidden not in api_code, forbidden
