"""Section 16.A-16.F: transfer / logical-allocation economics supplied at runtime through the Section 17
account stage (dry-run capital plan), never by moving money to find out."""
from dataclasses import dataclass
from decimal import Decimal as D
from datetime import datetime, timedelta, timezone

import pytest
from _plan import fresh_db, run_pipeline, venue_evaluated

from shared_lib.broker.wallets import topology_for

from app.trading_intelligence.capital.planner import AccountCapitalState, CapitalSettings
from app.trading_intelligence.capital.route_facts import ObservedRouteFacts, RouteFacts
from app.trading_intelligence.portfolio.account_capital import consider_account

CRYPTO_OK = {"CRYPTO": (True, None)}


@dataclass
class StubFacts:
    fee: float = None
    latency_ms: int = None
    calls: int = 0

    def facts(self, *, broker_account_id, source_wallet, destination_wallet, asset, now_ms):
        self.calls += 1
        return RouteFacts(self.fee, "USDT" if self.fee is not None else None,
                          "fixture:declared-route-fee" if self.fee is not None else None, self.latency_ms,
                          "fixture:observed" if self.latency_ms is not None else None, now_ms)


@pytest.fixture
def ranked(tmp_path):
    db = fresh_db(tmp_path)
    p = run_pipeline(db, [venue_evaluated("BTCUSDT", seed=999)])
    r = p["result"].ranked[0]
    return db, p["result"].ranked, p["evaluated"], p["evaluated"][r.setup_candidate_id].candidate


def unified(free="1000"):
    return AccountCapitalState("acct1", "USDT", topology_for("bybit", "UNIFIED"), {"UNIFIED": D(free), "FUND": D("0")})


def segmented(futures="10", funding="5000", usable=True):
    return AccountCapitalState("acct1", "USDT", topology_for("binance"),
                               {"UMFUTURE": D(futures), "FUNDING": D(funding), "MAIN": None},
                               transfer_capability_usable=usable)


def view(ranked, state, facts=None, *, margin="50", now=None, settings=None):
    db, rk, ev, cand = ranked
    return consider_account(rk, ev, user_id="u1", broker_account_id="acct1", state=state,
                            settings=settings or CapitalSettings(), per_trade_margin=D(margin), leverage=5.0,
                            family=CRYPTO_OK, route_facts=facts or StubFacts(), cycle_id="c1",
                            now_ms=now if now is not None else cand.decision_time + 1_000)


def only(v):
    (c,) = v.candidates.values()
    return c


def test_unified_account_is_logical_allocation_without_transfer_economics(ranked):
    facts = StubFacts()
    c = only(view(ranked, unified(), facts))
    assert c.viable and c.plan["outcome"] == "NO_ACTION_SHARED_COLLATERAL"
    # NOT a zero-fee transfer: there is no physical move, no fee, no delay
    assert c.transfer["status"] == "NOT_APPLICABLE" and c.transfer["fee_R"] is None and c.transfer["latency_ms"] is None
    assert facts.calls == 0  # no route facts are even requested for a logical allocation


def test_segmented_account_gets_dry_run_route_economics(ranked):
    c = only(view(ranked, segmented(), StubFacts(fee=0.1, latency_ms=5_000)))
    assert c.plan["outcome"] == "PHYSICAL_INTERNAL_TRANSFER_REQUIRED"
    t = c.plan["transfer"]
    assert (t["source_wallet"], t["destination_wallet"]) == ("FUNDING", "UMFUTURE") and D(t["amount"]) > 0
    assert c.viable and c.transfer["status"] == "KNOWN" and c.transfer["fee_R"] > 0 and c.transfer["latency_ms"] == 5_000
    # the known fee is subtracted from the edge ONCE, through the unchanged admission gates
    ev = ranked[2][c.setup_candidate_id].opportunity
    assert c.final_net_edge_r == pytest.approx(ev.ev_net_r - c.transfer["fee_R"])


@pytest.mark.parametrize("state,reason", [
    (AccountCapitalState("acct1", "USDT", None, {}), "ACCOUNT_TOPOLOGY_UNKNOWN"),
    (segmented(usable=False), "INTERNAL_TRANSFER_UNAVAILABLE"),
])
def test_unknown_topology_or_unsupported_route_is_unavailable(ranked, state, reason):
    c = only(view(ranked, state))
    assert not c.viable and "TRANSFER_ECONOMICS_UNAVAILABLE" in c.reason_codes and reason in c.reason_codes


def test_route_not_allowed_by_user_is_unavailable(ranked):
    c = only(view(ranked, segmented(), settings=CapitalSettings(allowed_routes=())))  # the user allows no route
    assert not c.viable and "INTERNAL_TRANSFER_ROUTE_UNAVAILABLE" in c.reason_codes


@pytest.mark.parametrize("facts,reason", [
    (StubFacts(fee=None, latency_ms=5_000), "TRANSFER_COST_UNAVAILABLE"),
    (StubFacts(fee=0.1, latency_ms=None), "TRANSFER_LATENCY_UNAVAILABLE"),
])
def test_unknown_fee_or_latency_blocks(ranked, facts, reason):
    c = only(view(ranked, segmented(), facts))
    assert not c.viable and reason in c.reason_codes and "TRANSFER_ECONOMICS_UNAVAILABLE" in c.reason_codes


def test_transfer_delay_vs_opportunity_lifetime(ranked):
    cand = ranked[3]
    now = cand.decision_time + 1_000
    left = cand.valid_until - now
    late = only(view(ranked, segmented(), StubFacts(fee=0.1, latency_ms=left + 1), now=now))
    assert not late.viable
    assert {"TRANSFER_DELAY_EXCEEDS_VALIDITY", "OPPORTUNITY_EXPIRED_BEFORE_CAPITAL_READY"} <= set(late.reason_codes)
    ok = only(view(ranked, segmented(), StubFacts(fee=0.1, latency_ms=left // 2), now=now))
    assert ok.viable and ok.transfer["capital_ready_by"] == now + left // 2


def test_prohibitive_known_fee_fails_final_economics(ranked):
    c = only(view(ranked, segmented(), StubFacts(fee=10_000.0, latency_ms=1_000)))
    assert not c.viable and any(r.startswith("FINAL_ECONOMICS_NOT_VIABLE") for r in c.reason_codes)


def test_no_physical_transfer_happens_during_economics(ranked):
    db = ranked[0]
    view(ranked, segmented(), StubFacts(fee=0.1, latency_ms=1_000))
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM broker_transfer_requests").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM broker_transfer_events").fetchone()[0] == 0


def test_observed_latency_needs_real_history_and_fee_is_never_invented(ranked):
    db = ranked[0]
    now = int(datetime(2026, 9, 28, tzinfo=timezone.utc).timestamp() * 1000)
    facts = ObservedRouteFacts(db)
    f = facts.facts(broker_account_id="acct1", source_wallet="FUNDING", destination_wallet="UMFUTURE", asset="USDT",
                    now_ms=now)
    assert f.fee is None and f.fee_source is None and f.latency_ms is None  # topology publishes no fee; no history
    base = datetime(2026, 9, 27, tzinfo=timezone.utc)
    with db.connect() as conn:
        for i, secs in enumerate((2, 7, 4)):
            sub = base + timedelta(minutes=i)
            conn.execute(
                "INSERT INTO broker_transfer_requests (id, user_id, broker_account_id, broker, environment, asset, amount,"
                " source_wallet, destination_wallet, idempotency_key, status, requested_at, submitted_at, confirmed_at,"
                " created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (f"t{i}", "u1", "acct1", "binance", "DEMO", "USDT", "10", "FUNDING", "UMFUTURE", f"k{i}", "COMPLETED",
                 sub.isoformat(), sub.isoformat(), (sub + timedelta(seconds=secs)).isoformat(), sub.isoformat(),
                 sub.isoformat()))
    f = facts.facts(broker_account_id="acct1", source_wallet="FUNDING", destination_wallet="UMFUTURE", asset="USDT",
                    now_ms=now)
    assert f.latency_ms == 7_000 and f.latency_source.startswith("OBSERVED_ACCOUNT_HISTORY")  # slowest observation
    assert f.fee is None  # observed latency never implies a fee
    other = facts.facts(broker_account_id="acct2", source_wallet="FUNDING", destination_wallet="UMFUTURE",
                        asset="USDT", now_ms=now)
    assert other.latency_ms is None  # another account's history is never used


def test_section22_and_frozen_cost_policy_hashes_unchanged():
    from test_cati_section16_closure import DEFAULT_VENUE_COST_POLICY_HASH, RESEARCH_DEFAULT_V1_HASH

    from app.trading_intelligence.research.certification.policy import research_default_v1
    from app.trading_intelligence.venue.policy import default_venue_cost_policy

    assert research_default_v1().policy_hash == RESEARCH_DEFAULT_V1_HASH
    assert default_venue_cost_policy().policy_hash == DEFAULT_VENUE_COST_POLICY_HASH
