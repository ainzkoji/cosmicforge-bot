import pytest
from app.trading_intelligence.portfolio.currency_exposure import ExposureItem,build_exposure,check_concentration


@pytest.mark.parametrize("notional,base,quote,side",[(None,"EUR","USD","LONG"),(float("nan"),"EUR","USD","LONG"),
    (float("inf"),"EUR","USD","LONG"),(100,"","USD","LONG"),(100,"EUR","","LONG"),(100,"EUR","USD","UNKNOWN")])
def test_unknown_exposure_blocks(notional,base,quote,side):
    p=build_exposure([ExposureItem("EURUSD","FX",base,quote,side,notional)])
    assert p.unknown_notional and not check_concentration(build_exposure([]),p)[0]


def test_size_weighted_cross_pairs_and_short_inversion():
    p=build_exposure([ExposureItem("EURUSD","FX","EUR","USD","LONG",10000),
                      ExposureItem("GBPUSD","FX","GBP","USD","LONG",20000)])
    assert p.by_code == {"EUR":10000,"GBP":20000,"USD":-30000}
    short=build_exposure([ExposureItem("EURJPY","FX","EUR","JPY","SHORT",5000)])
    assert short.by_code == {"EUR":-5000,"JPY":5000}


@pytest.mark.parametrize("settlement",["USDT","USDC","USD","FUTURE_ASSET"])
def test_collateral_is_gross_and_separate_from_direction(settlement):
    p=build_exposure([ExposureItem("BTCUSDT","CRYPTO","BTC","USDT","LONG",10000,settlement_asset=settlement),
                      ExposureItem("ETHUSDT","CRYPTO","ETH","USDT","SHORT",20000,settlement_asset=settlement)])
    assert p.by_code == {"BTC":10000,"ETH":-20000}
    assert p.settlement_by_code == {settlement:30000}


# -- account scope, notional sizing and fail-closed loading (15.3, 15.5, 15.9) ----------------------------------
from test_portfolio import NOW, add_bot, add_position, tdb  # noqa: F401  (tdb is a fixture)
from app.trading_intelligence.portfolio.exposure_builder import PortfolioDataError, build_account_exposure_snapshot


def test_account_currency_exposure_is_size_weighted_across_bots_and_account_scoped(tdb):
    for bot, acct in (("botA", "acct1"), ("botB", "acct1"), ("botC", "acct2")):
        add_bot(tdb, bot, acct)
    add_position(tdb, "botA", "BTCUSDT", qty=1.0, px=100.0)
    add_position(tdb, "botB", "ETHUSDT", "SHORT", qty=2.0, px=50.0)
    add_position(tdb, "botC", "SOLUSDT", qty=100.0, px=10.0)
    one = build_account_exposure_snapshot(tdb, "acct1", NOW).currency_exposure
    assert one.by_code_raw.get("BTC") == 100.0 and one.by_code_raw.get("ETH") == -100.0  # notional, not trade count
    assert one.settlement_by_code == {"USDT": 200.0}  # gross collateral concentration, direction-free
    assert "SOL/USDT:PERP" not in one.instruments  # account B never leaks into account A
    two = build_account_exposure_snapshot(tdb, "acct2", NOW).currency_exposure
    assert two.settlement_by_code == {"USDT": 1000.0}


@pytest.mark.parametrize("qty,px", [(0.0, 100.0), (1.0, None), (1.0, 0.0), (1.0, -5.0)])
def test_unknown_notional_is_never_zero_exposure(tdb, qty, px):
    add_bot(tdb, "botA", "acct1")
    add_position(tdb, "botA", "BTCUSDT", qty=qty, px=px)
    with pytest.raises(PortfolioDataError, match="NOTIONAL_UNKNOWN"):
        build_account_exposure_snapshot(tdb, "acct1", NOW)


def test_unknown_notional_blocks_portfolio_approval(tdb):
    from test_portfolio import context, ranked
    from app.trading_intelligence.portfolio.service import ShadowAccountPortfolioService
    add_bot(tdb, "botA", "acct1")
    add_position(tdb, "botA", "ETHUSDT", px=None)
    out = ShadowAccountPortfolioService(tdb).select_and_reserve(
        ranked=[ranked("BTCUSDT", 1.0)], broker_account_id="acct1", bot_instance_id="botA", cycle_id="c1",
        max_open_positions=3, context=context(), now_ms=NOW)
    assert out.decision.selected_opportunity_ids == ()
