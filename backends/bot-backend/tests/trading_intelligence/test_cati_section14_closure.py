from dataclasses import replace
import pytest
from _helpers import market_state_and_regime, flat_rows
from app.trading_intelligence.market_state.global_state import build_global_market_state
from app.trading_intelligence.market_state.crypto_context import build_crypto_context
from app.trading_intelligence.fx.context import build_fx_context
from app.trading_intelligence.contracts.instrument import from_fx_pair


def test_global_nested_immutability_and_owned_input():
    s=build_global_market_state([],decision_time=100,timeframe="1m")
    identity=s.state_hash
    with pytest.raises(TypeError):s.components["breadth"]=None
    with pytest.raises(TypeError):s.components["data_quality"].detail["valid"]=9
    with pytest.raises(TypeError):s.components |= {"foo":None}
    assert s.state_hash == identity


@pytest.mark.parametrize("stamp,reason",[(None,"TIMESTAMP"),(101,"NON_CAUSAL"),(-4_000_000,"STALE")])
def test_calendar_requires_causal_fresh_timestamp(stamp,reason):
    s=build_global_market_state([],decision_time=100,timeframe="1m",event_source_state="AVAILABLE",event_observed_at=stamp)
    assert s.component("event_risk").value is None and reason in s.component("event_risk").reason


def test_crypto_missing_features_are_explicit_and_future_state_unavailable():
    ms,_=market_state_and_regime(flat_rows(1))
    a=build_crypto_context(ms,as_of_ms=ms.decision_time)
    assert a.components["liquidations"].value is None
    assert a.components["liquidations"].reason
    assert a.context_hash == build_crypto_context(ms,as_of_ms=ms.decision_time).context_hash
    future=build_crypto_context(ms,as_of_ms=ms.latest_closed_candle_time-1)
    assert all(c.value is None for c in future.components.values())


def test_fx_reference_spread_never_drives_execution_context():
    key=from_fx_pair(venue="bybit",venue_symbol="EURUSDUSDT",base="EUR",quote="USD")
    ref={"open_time":0,"mid_close":1.1001,"bid_close":1.1,"ask_close":1.1002,"provider":"dukascopy"}
    a=build_fx_context(instrument_key=key,as_of_ms=60000,reference=ref,calendar_state="STALE")
    assert a.reference_spread_bps is not None and a.venue_spread_bps is None and a.spread_state == "UNAVAILABLE"
    assert a.reference_provider == "dukascopy" and a.calendar_availability == "STALE"



# -- shadow-only proof (14.6), wiring, evidence (14.11) --------------------------------------------------------
import ast
import copy
import json
from pathlib import Path
from types import SimpleNamespace

APP = Path(__file__).resolve().parents[2] / "app"
#: the ONLY modules allowed to reference GlobalMarketState / asset contexts. A selection, admission, ranking,
#: risk, sizing or execution module appearing here would be an ungoverned authority change.
SHADOW_ONLY_ALLOWLIST = {
    "activation/cati.py",                                  # the observe-only activation flag
    "trading_intelligence/contracts/global_market_state.py",
    "trading_intelligence/fx/context.py",
    "trading_intelligence/governance/destination_map.py",  # documentation map (strings only)
    "trading_intelligence/integration/cycle_shadow.py",    # the evidence stage
    "trading_intelligence/market_state/crypto_context.py",
    "trading_intelligence/market_state/global_state.py",
    "trading_intelligence/market_state/global_state_store.py",
    "trading_intelligence/economics/fx_perp.py",           # 16.16 overlay: FX context in, NO production caller
}


def test_no_decision_module_consumes_global_state_or_asset_contexts():
    needles = ("global_market_state", "market_state.global_state", "global_state_store", "crypto_context",
               "build_fx_context", "FXMarketContext", "CryptoMarketContext")
    users = {p.relative_to(APP).as_posix() for p in APP.rglob("*.py")
             if any(n in p.read_text(encoding="utf-8", errors="ignore") for n in needles)}
    assert users <= SHADOW_ONLY_ALLOWLIST, sorted(users - SHADOW_ONLY_ALLOWLIST)
    overlay_callers = {p.relative_to(APP).as_posix() for p in APP.rglob("*.py")
                       if "fx_perp_overlay" in p.read_text(encoding="utf-8", errors="ignore")}
    assert overlay_callers == {"trading_intelligence/economics/fx_perp.py"}  # not wired into certified costs


def test_cycle_discards_the_global_state_stage_result():
    tree = ast.parse((APP / "trading_intelligence/integration/cycle_shadow.py").read_text(encoding="utf-8"))
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and getattr(n.func, "id", None) == "_global_market_state_stage"]
    parents = {id(c): p for p in ast.walk(tree) for c in ast.iter_child_nodes(p)}
    assert calls and all(isinstance(parents[id(c)], ast.Expr) for c in calls)  # never assigned / passed on


def test_stage_leaves_the_cycle_result_untouched_and_persists_contexts(tmp_path, monkeypatch):
    from shared_lib.persistence.cati_schema import ensure_cati_schema
    from shared_lib.persistence.db import DB
    from app.trading_intelligence.integration import cycle_shadow as cs
    from app.trading_intelligence.market_state.global_state_store import GlobalMarketStateStore

    db = DB(path=str(tmp_path / "g.db"))
    ensure_cati_schema(db)
    monkeypatch.delenv("CATI_GLOBAL_MARKET_STATE_ENABLED", raising=False)
    ms, _ = market_state_and_regime(flat_rows(1))
    ranked = ("rk_1", "rk_2")
    result = SimpleNamespace(evaluations=[SimpleNamespace(market_state=ms, opportunities=())], ranked=ranked,
                             batch=SimpleNamespace(cycle_id="cyc", bot_instance_id="bot", broker_account_id="acc"))
    before = copy.deepcopy(result.__dict__)
    assert cs._global_market_state_stage(SimpleNamespace(db=db), result, ms.decision_time, "15m") is not None
    assert result.__dict__.keys() == before.keys() and result.ranked == before["ranked"]
    assert result.evaluations[0].market_state is ms and result.batch.__dict__ == before["batch"].__dict__
    row = GlobalMarketStateStore(db).latest()[0]
    payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
    assert ms.instrument_key.canonical_symbol in payload["asset_contexts"]
    keys = set()
    def walk(v):
        if isinstance(v, dict):
            keys.update(k.lower() for k in v)
            [walk(x) for x in v.values()]
        elif isinstance(v, list):
            [walk(x) for x in v]
    walk(payload)
    forbidden = {"user_id", "broker_account_id", "balance", "equity", "margin", "position", "positions", "order",
                 "orders", "order_id", "pnl", "reservation", "reservations", "transfer", "transfer_state"}
    assert not keys & forbidden, keys & forbidden  # tenant-neutral payload (the evidence ROW carries its scope)


def test_informational_values_change_identity_not_authority():
    a = build_global_market_state([], decision_time=100, timeframe="1m", event_source_state="AVAILABLE",
                                  event_observed_at=100)
    b = build_global_market_state([], decision_time=100, timeframe="1m", event_source_state="STALE")
    assert a.component("event_risk").status == "AVAILABLE" and b.component("event_risk").reason == "EVENT_CALENDAR_STALE"
    assert a.state_hash != b.state_hash  # recorded as different evidence ...
    assert not hasattr(a, "admission") and not hasattr(a, "veto") and not hasattr(a, "rank")  # ... carrying no authority
