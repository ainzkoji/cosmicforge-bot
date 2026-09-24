"""Part A -- Section 6/7 architectural closure: AccountExposureSnapshot,
AccountPortfolioReservation, and the controller/service interfaces."""
from __future__ import annotations

import inspect

import pytest

from app.trading_intelligence.contracts.exposure import (
    AccountExposureSnapshot,
    ExposureRecord,
    ExposureStatus,
)
from app.trading_intelligence.contracts.instrument import CRYPTO, from_symbol_fallback
from app.trading_intelligence.contracts.portfolio import (
    AccountPortfolioReservation,
    ReservationStatus,
)
from app.trading_intelligence.controller.interfaces import (
    AccountPortfolioService,
    CATIControllerProtocol,
    PositionIntelligenceService,
)

INSTRUMENT = from_symbol_fallback(venue="binance", venue_symbol="BTCUSDT", asset_class=CRYPTO)


def test_account_exposure_snapshot_requires_broker_account_id():
    with pytest.raises(ValueError):
        AccountExposureSnapshot(exposure_snapshot_id="axs_1", broker_account_id="", as_of_time=1)


def test_account_exposure_snapshot_is_account_scoped_and_builds():
    record = ExposureRecord(
        bot_instance_id="bot_1",
        instrument_key=INSTRUMENT,
        side="LONG",
        quantity=1.0,
        notional=100.0,
        entry_reference=100.0,
        exposure_status=ExposureStatus.OPEN.value,
    )
    snap = AccountExposureSnapshot.build(
        broker_account_id="acct_1", as_of_time=1_700_000_000_000, open_exposures=(record,)
    )
    assert snap.broker_account_id == "acct_1"
    assert snap.open_exposures[0].bot_instance_id == "bot_1"
    assert snap.exposure_snapshot_id.startswith("axs_")


def test_exposure_record_rejects_unknown_side_and_status():
    with pytest.raises(ValueError):
        ExposureRecord(
            bot_instance_id="bot_1", instrument_key=INSTRUMENT, side="SIDEWAYS",
            quantity=1.0, notional=1.0, entry_reference=1.0, exposure_status=ExposureStatus.OPEN.value,
        )
    with pytest.raises(ValueError):
        ExposureRecord(
            bot_instance_id="bot_1", instrument_key=INSTRUMENT, side="LONG",
            quantity=1.0, notional=1.0, entry_reference=1.0, exposure_status="NOT_A_STATUS",
        )


def test_shared_market_state_never_depends_on_account_exposure():
    """Structural proof: nothing in the MarketState/engine call chain accepts
    an AccountExposureSnapshot."""
    import dataclasses

    from app.trading_intelligence.contracts.market_state import MarketState
    from app.trading_intelligence.market_state.engine import build_market_state

    for name in inspect.signature(build_market_state).parameters:
        assert "exposure" not in name.lower()
    for f in dataclasses.fields(MarketState):
        assert "exposure" not in f.name.lower()


def test_reservation_is_deterministic_shape_and_versioned():
    r = AccountPortfolioReservation(
        reservation_id="res_1", broker_account_id="acct_1", cycle_id="cycle_1",
        selected_candidate_ids=("sc_1", "sc_2"), created_at=1_000, expires_at=2_000,
    )
    assert r.status == ReservationStatus.RESERVED.value
    assert r.reservation_version
    consumed = r.with_status(ReservationStatus.CONSUMED)
    assert consumed.status == ReservationStatus.CONSUMED.value
    assert r.status == ReservationStatus.RESERVED.value  # original untouched (immutable)


def test_reservation_rejects_bad_expiry_and_status():
    with pytest.raises(ValueError):
        AccountPortfolioReservation(
            reservation_id="r", broker_account_id="a", cycle_id="c",
            selected_candidate_ids=(), created_at=1000, expires_at=500,
        )
    with pytest.raises(ValueError):
        AccountPortfolioReservation(
            reservation_id="r", broker_account_id="a", cycle_id="c",
            selected_candidate_ids=(), created_at=1000, expires_at=2000, status="BOGUS",
        )
    with pytest.raises(ValueError):
        AccountPortfolioReservation(
            reservation_id="r", broker_account_id="", cycle_id="c",
            selected_candidate_ids=(), created_at=1000, expires_at=2000,
        )


def test_interfaces_have_no_order_or_capital_authority():
    forbidden = ("order", "capital", "client", "execute", "broker_credential")
    for proto in (CATIControllerProtocol, AccountPortfolioService, PositionIntelligenceService):
        for method_name in ("run_cycle", "select", "assess"):
            method = getattr(proto, method_name, None)
            if method is None:
                continue
            sig = inspect.signature(method)
            for name in sig.parameters:
                lowered = name.lower()
                assert not any(f in lowered for f in forbidden), f"{proto.__name__}.{method_name} accepts {name}"


def test_no_premature_portfolio_optimization_logic_exists():
    """Section 16 portfolio optimization must not have been implemented --
    the portfolio contracts module must contain only data shapes and a
    Protocol, no concrete selection algorithm."""
    import app.trading_intelligence.contracts.portfolio as portfolio_module

    source = inspect.getsource(portfolio_module)
    for forbidden_term in ("correlation_matrix", "optimize_portfolio", "sharpe", "markowitz"):
        assert forbidden_term not in source.lower()
