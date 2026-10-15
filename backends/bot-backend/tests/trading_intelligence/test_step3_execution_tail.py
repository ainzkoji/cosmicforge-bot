"""Step 3.4: the unified execution tail.

Three things are proved here.

1. **Checked evidence.** The broker-health and capital-readiness verdicts of ``pipeline.readiness`` are pure
   functions of evidence, and no missing, unreadable, stale or future-dated piece of it ever counts in favour of
   an entry. The production tail (``production_execution.prepare_submission``, shared by the natural path and by
   the operator's certification harness) hands the final boundary those verdicts, and the boundary refuses the
   plan before any order when one of them fails.
2. **The plan mode.** A plan of the production tail carries the account's environment as its mode, which is
   what the real family gate demands; the previous constant would be refused on a demo account.
3. **The operator transport tool stays apart.** ``demo_transport_smoke`` cannot act inside a strategy
   submission, nor on an account whose strategy execution is unresolved.

The end-to-end tests drive the real ``process_account`` cycle, the real boundary, orchestrator and executor over
a mocked exchange client, with the REAL family gate. That gate refuses every real family, so the tests that need
an order to be reachable register the family in the test's temporary database on a synthetic research register
whose hypothesis passed, and promote it through the real governance operations. That says nothing about any
real family: ``test_the_real_gate_still_stops_the_natural_plan`` and ``test_step3_no_uncertified_orders`` show
the unregistered product state.
"""
from __future__ import annotations

import dataclasses
import inspect
import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

import _step3 as S
from test_demo_boundary_certification import broker  # noqa: F401  (fixture)
from test_production_demo_execution import demo  # noqa: F401  (fixture)
from test_production_execution import live  # noqa: F401  (fixture)
from test_production_research_separation import fresh, process  # noqa: F401  (fixture)

from app.exchange.binance import client as binance_client
from app.execution import demo_boundary_certification as cert
from app.execution import demo_transport_smoke as smoke
from app.trading_intelligence.capital import planner
from app.trading_intelligence.contracts.execution import ExecutionAttempt
from app.trading_intelligence.execution.boundary import BoundaryStatus
from app.trading_intelligence.execution.entry_permit import boundary_entry_permit
from app.trading_intelligence.integration import income_ledger
from app.trading_intelligence.integration import production_execution as production
from app.trading_intelligence.integration.residual_prospective import FAMILY, REGISTRY_HASH
from app.trading_intelligence.pipeline import provenance as P
from app.trading_intelligence.pipeline import readiness as R
from app.trading_intelligence.portfolio import reservation_store
from app.trading_intelligence.trade_plan.validation import verify_trade_plan_integrity
from shared_lib.broker.environment import BrokerEnvironment, resolve_base_url
from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema

NOW = 1_800_000_000_000
TOO_OLD = NOW - R.MAX_AUTHENTICATED_READ_AGE_MS - 1        # an authenticated read one millisecond past its bound
DEMO_ENDPOINT = resolve_base_url("binance", BrokerEnvironment.DEMO)


# ══════════════════════════════════════════════════════════════════════════ broker health (pure)
def broker_evidence(**over):
    base = dict(broker_account_id="acct-1", venue="BINANCE_USDM", plan_environment="DEMO", client_environment="DEMO",
                now_ms=NOW, equity=5000.0, wallet=5000.0, free_margin=4900.0, margin_used=100.0,
                authenticated_read_at_ms=NOW - 1_500, venue_price=100.0, venue_price_at_ms=NOW,
                rate_limit=R.RateLimitEvidence(0.0, 120, 2400, 0.8))
    base.update(over)
    return R.BrokerEvidence(**base)


def health(**over):
    return R.evaluate_broker_health(broker_evidence(**over))


def test_complete_fresh_evidence_is_healthy_and_says_where_it_came_from():
    context = health()
    assert context.status == "HEALTHY" and context.reason_codes == ()
    assert (context.broker_account_id, context.venue, context.environment) == ("acct-1", "BINANCE_USDM", "DEMO")
    assert context.observed_at == NOW and context.freshness_ms == 1_500 and context.source == R.SOURCE
    # a fully used account is still a complete snapshot: sufficiency is the capital gate's question
    assert health(free_margin=0.0, margin_used=0).status == "HEALTHY"
    # the client's own environment is what is reported, not a copy of the plan's
    assert health(client_environment="demo", plan_environment="demo").environment == "DEMO"


@pytest.mark.parametrize("field", [f.name for f in dataclasses.fields(R.BrokerEvidence)])
@pytest.mark.parametrize("missing", [None, "", object(), MagicMock(), float("nan"), float("inf"), True, "5000"])
def test_no_missing_or_unreadable_evidence_is_ever_healthy(field, missing):
    """The rule of the assignment, for every field and every way a value can fail to be evidence. (Text is
    evidence for the four text fields only, and only when it names something.)"""
    textual = field in ("broker_account_id", "venue", "plan_environment", "client_environment")
    if textual and missing == "5000" and field in ("broker_account_id", "venue"):
        pytest.skip("any non-empty text names an account or a venue")
    context = health(**{field: missing})
    assert context.status != "HEALTHY", (field, missing, context)
    assert context.reason_codes and set(context.reason_codes) <= set(R.BROKER_REASONS)


@pytest.mark.parametrize("over,status,reason", [
    (dict(broker_account_id=None), "UNKNOWN", R.BROKER_IDENTITY_MISSING),
    (dict(venue="  "), "UNKNOWN", R.BROKER_IDENTITY_MISSING),
    (dict(client_environment=None), "UNKNOWN", R.BROKER_ENVIRONMENT_UNREADABLE),
    (dict(plan_environment="PAPER"), "UNKNOWN", R.BROKER_ENVIRONMENT_UNREADABLE),
    (dict(client_environment="LIVE"), "UNAVAILABLE", R.BROKER_ENVIRONMENT_MISMATCH),
    (dict(equity=0.0), "UNKNOWN", R.BROKER_ACCOUNT_SNAPSHOT_INCOMPLETE),
    (dict(wallet=-1.0), "UNKNOWN", R.BROKER_ACCOUNT_SNAPSHOT_INCOMPLETE),
    (dict(free_margin=None), "UNKNOWN", R.BROKER_ACCOUNT_SNAPSHOT_INCOMPLETE),
    (dict(margin_used=float("nan")), "UNKNOWN", R.BROKER_ACCOUNT_SNAPSHOT_INCOMPLETE),
    (dict(authenticated_read_at_ms=None), "UNKNOWN", R.BROKER_AUTHENTICATED_READ_UNPROVEN),
    (dict(authenticated_read_at_ms=0), "UNKNOWN", R.BROKER_AUTHENTICATED_READ_UNPROVEN),
    (dict(authenticated_read_at_ms=NOW + 5_001), "UNKNOWN", R.BROKER_AUTHENTICATED_READ_FUTURE_DATED),
    (dict(authenticated_read_at_ms=TOO_OLD), "DEGRADED", R.BROKER_AUTHENTICATED_READ_STALE),
    (dict(venue_price=0.0), "UNKNOWN", R.BROKER_VENUE_READ_UNPROVEN),
    (dict(venue_price_at_ms=None), "UNKNOWN", R.BROKER_VENUE_READ_UNPROVEN),
    (dict(venue_price_at_ms=NOW - R.MAX_VENUE_READ_AGE_MS - 1), "UNKNOWN", R.BROKER_VENUE_READ_UNPROVEN),
    (dict(venue_price_at_ms=NOW + 5_001), "UNKNOWN", R.BROKER_VENUE_READ_UNPROVEN),
    (dict(rate_limit=None), "UNKNOWN", R.BROKER_RATE_LIMIT_STATE_UNREADABLE),
    (dict(rate_limit=R.RateLimitEvidence(None)), "UNKNOWN", R.BROKER_RATE_LIMIT_STATE_UNREADABLE),
    (dict(rate_limit=R.RateLimitEvidence(-1.0)), "UNKNOWN", R.BROKER_RATE_LIMIT_STATE_UNREADABLE),
    (dict(rate_limit=R.RateLimitEvidence(0.01)), "UNAVAILABLE", R.BROKER_RATE_LIMIT_BACKOFF),
    (dict(rate_limit=R.RateLimitEvidence(0.0, 1920, 2400, 0.8)), "DEGRADED", R.BROKER_REQUEST_WEIGHT_NEAR_LIMIT),
])
def test_each_failed_condition_has_its_status_and_reason(over, status, reason):
    context = health(**over)
    assert context.status == status and reason in context.reason_codes, context


def test_the_bounds_are_inclusive_and_small_clock_differences_are_tolerated():
    assert health(authenticated_read_at_ms=NOW - R.MAX_AUTHENTICATED_READ_AGE_MS).status == "HEALTHY"
    assert health(venue_price_at_ms=NOW - R.MAX_VENUE_READ_AGE_MS).status == "HEALTHY"
    ahead = health(authenticated_read_at_ms=NOW + 5_000, venue_price_at_ms=NOW + 5_000)
    assert ahead.status == "HEALTHY" and ahead.freshness_ms == 0
    assert R.evaluate_broker_health(broker_evidence(authenticated_read_at_ms=NOW - 61_000),
                                    max_authenticated_read_age_ms=60_000).status == "DEGRADED"


def test_the_request_weight_can_only_withdraw_health():
    """A venue that reports no weight proves nothing either way; neither does a weight without its budget."""
    for rate in (R.RateLimitEvidence(0.0), R.RateLimitEvidence(0.0, None, 2400, 0.8),
                 R.RateLimitEvidence(0.0, MagicMock(), 2400, 0.8), R.RateLimitEvidence(0.0, 2399, None, None),
                 R.RateLimitEvidence(0.0, 1919, 2400, 0.8)):
        assert health(rate_limit=rate).status == "HEALTHY", rate
    # ...and a comfortable weight never repairs anything else
    assert health(rate_limit=R.RateLimitEvidence(None, 1, 2400, 0.8)).status == "UNKNOWN"


def test_the_worst_status_wins_and_every_reason_is_reported():
    both = health(rate_limit=R.RateLimitEvidence(30.0, 2400, 2400, 0.8), authenticated_read_at_ms=TOO_OLD, equity=None)
    assert both.status == "UNAVAILABLE"
    assert both.reason_codes == (R.BROKER_ACCOUNT_SNAPSHOT_INCOMPLETE, R.BROKER_AUTHENTICATED_READ_STALE,
                                 R.BROKER_RATE_LIMIT_BACKOFF, R.BROKER_REQUEST_WEIGHT_NEAR_LIMIT)
    assert health(authenticated_read_at_ms=TOO_OLD, venue_price=None).status == "UNKNOWN"
    nothing = R.evaluate_broker_health(R.BrokerEvidence(*[None] * 13))
    assert nothing.status == "UNKNOWN" and nothing.observed_at is None and nothing.freshness_ms is None


# ══════════════════════════════════════════════════════════════════════ capital readiness (pure)
def reservation(**over):
    base = dict(reservation_id="resv-1", status="RESERVED", expires_at=NOW + 60_000, broker_account_id="acct-1",
                bot_instance_id="bot-1", mode="PRODUCTION")
    base.update(over)
    return SimpleNamespace(**base)


def capital_evidence(**over):
    base = dict(broker="binance", product="CRYPTO_PERPETUAL", broker_account_id="acct-1", plan_broker_account_id="acct-1",
                plan_bot_instance_id="bot-1", plan_reservation_id="resv-1", plan_key="tplan-1", now_ms=NOW,
                free_margin=5000.0, in_flight_margin=0.0, required_margin=Decimal("1"), unresolved_transfers=0,
                reservation=reservation())
    base.update(over)
    return R.CapitalEvidence(**base)


def capital(**over):
    return R.evaluate_capital_readiness(capital_evidence(**over))


def test_complete_evidence_is_ready_on_the_logical_path():
    assert capital() == planner.CapitalReadiness(True, "LOGICAL")
    assert capital(free_margin=1.0, required_margin=1).ready                       # exactly the floor is enough
    assert capital(free_margin=Decimal("2"), in_flight_margin=1.0).ready
    # a unified account whose mode WAS read is shared collateral: still the logical path, no transfer
    assert capital(broker="bybit", account_mode="UNIFIED") == planner.CapitalReadiness(True, "LOGICAL")


@pytest.mark.parametrize("field", ["broker", "product", "broker_account_id", "plan_broker_account_id",
                                   "plan_bot_instance_id", "plan_reservation_id", "now_ms", "free_margin",
                                   "in_flight_margin", "required_margin", "unresolved_transfers", "reservation"])
@pytest.mark.parametrize("missing", [None, "", object(), MagicMock(), float("nan"), True])
def test_no_missing_or_unreadable_evidence_is_ever_ready(field, missing):
    verdict = capital(**{field: missing})
    assert not verdict.ready and verdict.reason, (field, missing, verdict)


@pytest.mark.parametrize("over,path,reason,pending", [
    (dict(now_ms=None), "BLOCKED", R.CAPITAL_EVALUATION_TIME_MISSING, False),
    (dict(plan_broker_account_id="acct-2"), "BLOCKED", R.CAPITAL_ACCOUNT_MISMATCH, False),
    (dict(unresolved_transfers=None), "BLOCKED", R.TRANSFER_STATE_UNREADABLE, False),
    (dict(unresolved_transfers=1), "BLOCKED", planner.ACCOUNT_RECONCILIATION_REQUIRED, True),
    (dict(required_margin=None), "BLOCKED", R.CAPITAL_REQUIREMENT_UNKNOWN, False),
    (dict(required_margin=0), "BLOCKED", R.CAPITAL_REQUIREMENT_UNKNOWN, False),
    (dict(in_flight_margin=None), "BLOCKED", R.IN_FLIGHT_MARGIN_UNREADABLE, False),
    (dict(in_flight_margin=-0.1), "BLOCKED", R.IN_FLIGHT_MARGIN_UNREADABLE, False),
    (dict(broker="bybit"), "BLOCKED", planner.ACCOUNT_TOPOLOGY_UNKNOWN, False),       # account mode never read
    (dict(broker="kraken"), "BLOCKED", planner.ACCOUNT_TOPOLOGY_UNKNOWN, False),
    (dict(product="FX_PERPETUAL"), "BLOCKED", "NO_WALLET_COLLATERALISES_PRODUCT", False),
    (dict(free_margin=None), "BLOCKED", "TRADING_WALLET_BALANCE_UNKNOWN", False),
    (dict(free_margin=0.99), "BLOCKED", R.INSUFFICIENT_FREE_MARGIN, False),
    (dict(free_margin=-5.0), "BLOCKED", R.INSUFFICIENT_FREE_MARGIN, False),
    (dict(free_margin=1.5, in_flight_margin=1.0), "BLOCKED", R.INSUFFICIENT_FREE_MARGIN, False),
    (dict(reservation=None), "LOGICAL", "LOGICAL_RESERVATION_UNKNOWN", False),
    (dict(reservation=reservation(status="RELEASED")), "LOGICAL", "LOGICAL_RESERVATION_NOT_VALID", False),
    (dict(reservation=reservation(status="EXPIRED")), "LOGICAL", "LOGICAL_RESERVATION_NOT_VALID", False),
    (dict(reservation=reservation(status="RESOLUTION_PENDING")), "LOGICAL", "LOGICAL_RESERVATION_NOT_VALID", False),
    (dict(reservation=reservation(reservation_id="resv-2")), "LOGICAL", R.RESERVATION_PLAN_MISMATCH, False),
    (dict(reservation=reservation(expires_at=NOW)), "LOGICAL", R.RESERVATION_EXPIRED, False),
    (dict(reservation=reservation(expires_at=None)), "LOGICAL", R.RESERVATION_EXPIRED, False),
    (dict(reservation=reservation(broker_account_id="acct-2")), "LOGICAL", R.RESERVATION_ACCOUNT_MISMATCH, False),
    (dict(reservation=reservation(bot_instance_id="bot-2")), "LOGICAL", R.RESERVATION_BOT_MISMATCH, False),
    (dict(reservation=reservation(mode="SHADOW")), "LOGICAL", R.RESERVATION_NOT_EXECUTION_SCOPED, False),
])
def test_each_capital_refusal_has_its_reason(over, path, reason, pending):
    assert capital(**over) == planner.CapitalReadiness(False, path, reason, pending)


def test_the_requirement_is_the_smallest_order_never_a_guess():
    assert R.minimum_order_margin(5.0, 5) == Decimal("1")
    assert R.minimum_order_margin(Decimal("100"), 20.0) == Decimal("5")
    for notional, leverage in ((None, 5), (5, None), (0, 5), (-5, 5), (5, 0), (5, 0.5), ("5", 5), (5, True),
                               (float("nan"), 5), (5, float("inf")), (MagicMock(), 5)):
        assert R.minimum_order_margin(notional, leverage) is None, (notional, leverage)


def test_the_module_is_pure_and_tied_to_its_authorities():
    source = inspect.getsource(R)
    for forbidden in ("os.environ", "getenv", "app.core.config", "import time", ".connect(", "requests", "client."):
        assert forbidden not in source, forbidden
    assert R.EXECUTION_RESERVATION_MODE == reservation_store.MODE_PRODUCTION
    # the unforced refresh interval of the read whose age is measured must fit inside the bound
    assert R.MAX_AUTHENTICATED_READ_AGE_MS > income_ledger.REFRESH_INTERVAL_MS
    # the refusals of the planner are the planner's own codes: one implementation, not a copy
    assert capital(unresolved_transfers=3).reason == planner.ACCOUNT_RECONCILIATION_REQUIRED


def test_the_evidence_document_is_json_and_keeps_the_reason():
    broker_side = broker_evidence(rate_limit=R.RateLimitEvidence(12.5, 2000, 2400, 0.8))
    funds = capital_evidence(free_margin=0.5, required_margin=Decimal("1.25"))
    document = R.readiness_document(R.evaluate_broker_health(broker_side), R.evaluate_capital_readiness(funds),
                                    broker=broker_side, funds=funds)
    assert json.loads(json.dumps(document)) == document
    assert document["version"] == R.READINESS_VERSION
    recorded = document["broker_health"]
    assert recorded["status"] == "UNAVAILABLE"
    assert recorded["reason_codes"] == [R.BROKER_RATE_LIMIT_BACKOFF, R.BROKER_REQUEST_WEIGHT_NEAR_LIMIT]
    assert (recorded["rate_limit_backoff_s"], recorded["used_weight_1m"]) == (12.5, 2000)
    assert document["capital"] == {"ready": False, "path": "BLOCKED", "reason": R.INSUFFICIENT_FREE_MARGIN,
                                   "pending": False, "free_margin": 0.5, "in_flight_margin": 0.0,
                                   "required_margin": "1.25", "unresolved_transfers": 0, "reservation_status": "RESERVED"}


# ═════════════════════════════════════════════════════════════ the production tail, end to end
@pytest.fixture
def certified(fresh, tmp_path):  # noqa: F811
    """The plan's family registered on a SYNTHETIC passed hypothesis and promoted to M6 through the real
    governance operations, in this test's temporary database only, with a stand-in for the venue adapter's
    certification register. The boundary keeps the real gate class and every check of it (family
    authorization, decision verification, venue certification); the product registry is untouched and still
    refuses the family."""
    ensure_cati_platform_schema(fresh.db)
    definition = S.definition(family_id=FAMILY, rule_hash=REGISTRY_HASH,
                              implementation="app.trading_intelligence.integration.residual_prospective")
    registry = S.registry(fresh.db, S.research_register(tmp_path, status="RESEARCH_PASS"), definition)
    registry.register_all(actor_ref="system:test")
    S.promote(registry, definition.family_id, "M6", prereq=S.prerequisites(definition, certification="CERTIFIED"))
    fresh.gate = P.FamilyExecutionGate(fresh.db, registry=registry,
                                       venue_certified=lambda venue, environment, now_ms: (True, ""))
    fresh.boundary_for().family_gate = fresh.gate
    return fresh


def readiness_of(result):
    return result["risk_controls"]["execution_readiness"]


def stored_plan_mode(h, trade_plan_id):
    with h.db.connect() as c:
        return c.execute("SELECT mode, environment FROM cati_trade_plans WHERE trade_plan_id=?",
                         (trade_plan_id,)).fetchone()


def test_the_real_gate_still_stops_the_natural_plan(fresh):  # noqa: F811
    """Product state: the family is not registered, so nothing is sent. The evidence was still evaluated, and
    recorded with the cycle, before the boundary was asked."""
    result = process(fresh)
    assert result["boundary"]["status"] == BoundaryStatus.GOVERNANCE_NOT_AUTHORIZED, result["boundary"]
    fresh.client.place_order.assert_not_called()
    evidence = readiness_of(result)
    assert evidence["broker_health"]["status"] == "HEALTHY" and evidence["capital"]["ready"] is True


def test_a_certified_family_is_executed_once_on_checked_evidence(certified):
    result = process(certified)
    assert result["boundary"]["status"] == BoundaryStatus.EXECUTED, result["boundary"]
    assert certified.client.place_order.call_count == 1
    evidence = readiness_of(result)
    assert evidence["broker_health"] == {
        "status": "HEALTHY", "reason_codes": [], "source": R.SOURCE, "observed_at": certified.now,
        "environment": "DEMO", "authenticated_read_age_ms": 0, "rate_limit_backoff_s": 0.0, "used_weight_1m": None}
    # venue minimum notional 5 at the bot's maximum leverage 5; the account document's free margin; no
    # transfer; the cycle's own execution-scoped reservation
    assert Decimal(evidence["capital"].pop("required_margin")) == 1
    assert evidence["capital"] == {
        "ready": True, "path": "LOGICAL", "reason": None, "pending": False, "free_margin": 5000,
        "in_flight_margin": 0.0, "unresolved_transfers": 0, "reservation_status": "RESERVED"}
    evidence = readiness_of(result)
    assert tuple(stored_plan_mode(certified, result["trade_plan_id"])) == ("DEMO", "DEMO")
    assert json.loads(json.dumps(result["risk_controls"], default=str))["execution_readiness"] == evidence
    process(certified)
    assert certified.client.place_order.call_count == 1


def blocked_before_any_order(h, result, status, reason, evidence_reason=None, section="broker_health"):
    boundary = result["boundary"]
    assert boundary["status"] == status and boundary["reason_codes"][0] == reason, boundary
    h.client.place_order.assert_not_called()
    assert not result["orders_submitted"]
    with h.db.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM cati_execution_attempts").fetchone()[0] == 0
    if evidence_reason is not None:
        recorded = readiness_of(result)[section]
        assert evidence_reason in (recorded["reason_codes"] if section == "broker_health" else [recorded["reason"]])


def test_a_venue_backoff_stops_the_entry_that_the_client_would_have_sent(certified):
    """The client never delays a mutation, so only this check keeps an entry away from a venue that has just
    told this address to wait."""
    binance_client._note_backoff(DEMO_ENDPOINT, 30.0, 429)
    result = process(certified)
    blocked_before_any_order(certified, result, BoundaryStatus.RISK_REJECTED, "PREVALIDATION_BROKER_DEGRADED",
                             R.BROKER_RATE_LIMIT_BACKOFF)
    assert readiness_of(result)["broker_health"]["status"] == "UNAVAILABLE"
    # the state is per endpoint: a ban on the LIVE endpoint says nothing about this demo account
    binance_client.reset_rate_limit_state()
    binance_client._note_backoff(resolve_base_url("binance", BrokerEnvironment.LIVE), 30.0, 418)
    assert process(certified)["boundary"]["status"] == BoundaryStatus.EXECUTED


def test_a_request_weight_near_the_limit_stops_the_entry(certified):
    threshold = binance_client.REQUEST_WEIGHT_LIMIT_1M * binance_client.REQUEST_WEIGHT_PAUSE_FRACTION
    certified.client.last_used_weight_1m = int(threshold)   # the level at which the client itself starts pacing reads
    blocked_before_any_order(certified, process(certified), BoundaryStatus.RISK_REJECTED,
                             "PREVALIDATION_BROKER_DEGRADED", R.BROKER_REQUEST_WEIGHT_NEAR_LIMIT)


@pytest.mark.parametrize("defect,reason", [
    ("stale", R.BROKER_AUTHENTICATED_READ_STALE), ("absent", R.BROKER_AUTHENTICATED_READ_UNPROVEN),
    ("future", R.BROKER_AUTHENTICATED_READ_FUTURE_DATED), ("no_equity", R.BROKER_ACCOUNT_SNAPSHOT_INCOMPLETE)])
def test_account_evidence_that_is_stale_absent_or_incomplete_stops_the_entry(certified, monkeypatch, defect, reason):
    real = production.account_risk

    def tampered(*a, **k):
        risk = real(*a, **k)
        if defect == "stale":
            risk["income_ledger"]["refreshed_at"] = certified.now - R.MAX_AUTHENTICATED_READ_AGE_MS - 1
        elif defect == "future":
            risk["income_ledger"]["refreshed_at"] = certified.now + 60_000
        elif defect == "absent":
            del risk["income_ledger"]
        else:
            risk["wallet"] = float("nan")
        return risk

    monkeypatch.setattr(production, "account_risk", tampered)
    blocked_before_any_order(certified, process(certified), BoundaryStatus.RISK_REJECTED,
                             "PREVALIDATION_BROKER_DEGRADED", reason)


def add_transfer(h, status):
    with h.db.connect() as c:
        c.execute("INSERT INTO broker_transfer_requests (id, user_id, broker_account_id, broker, environment, asset,"
                  " amount, source_wallet, destination_wallet, idempotency_key, status, origin, requested_at,"
                  " metadata_json, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  (f"tr-{status}", h.account["user_id"], h.account["id"], "binance", "demo", "USDT", "250", "FUNDING",
                   "UMFUTURE", f"key-{status}", status, "MANUAL", "2026-10-09", "{}", "2026-10-09", "2026-10-09"))


@pytest.mark.parametrize("status", ["REQUESTED", "VALIDATING", "SUBMITTING", "SUBMITTED", "CONFIRMATION_PENDING",
                                    "UNKNOWN", "RECONCILIATION_REQUIRED"])
def test_a_transfer_that_is_planned_submitted_or_unknown_never_funds_an_entry(certified, status):
    add_transfer(certified, status)
    result = process(certified)
    blocked_before_any_order(certified, result, BoundaryStatus.CAPITAL_NOT_READY,
                             f"CAPITAL_NOT_READY:{planner.ACCOUNT_RECONCILIATION_REQUIRED}",
                             planner.ACCOUNT_RECONCILIATION_REQUIRED, section="capital")
    recorded = readiness_of(result)["capital"]
    assert recorded["pending"] is True and recorded["unresolved_transfers"] == 1


@pytest.mark.parametrize("status", ["COMPLETED", "FAILED", "BLOCKED"])
def test_a_resolved_transfer_or_another_accounts_transfer_does_not_block(certified, status):
    add_transfer(certified, status)
    with certified.db.connect() as c:                       # an unresolved transfer of ANOTHER account
        c.execute("INSERT OR IGNORE INTO broker_accounts (id,user_id,broker_id,environment,status) VALUES"
                  " ('another-account','u9','binance','demo','connected')")
        c.execute("INSERT INTO broker_transfer_requests (id, user_id, broker_account_id, broker, environment, asset,"
                  " amount, source_wallet, destination_wallet, idempotency_key, status, origin, requested_at,"
                  " metadata_json, created_at, updated_at) VALUES ('tr-other','u9','another-account','binance','demo',"
                  "'USDT','1','FUNDING','UMFUTURE','k','SUBMITTED','MANUAL','x','{}','x','x')")
    assert process(certified)["boundary"]["status"] == BoundaryStatus.EXECUTED


def test_an_unreadable_transfer_state_is_not_ready(certified):
    with certified.db.connect() as c:
        c.execute("ALTER TABLE broker_transfer_requests RENAME TO broker_transfer_requests_gone")
    blocked_before_any_order(certified, process(certified), BoundaryStatus.CAPITAL_NOT_READY,
                             f"CAPITAL_NOT_READY:{R.TRANSFER_STATE_UNREADABLE}", R.TRANSFER_STATE_UNREADABLE, "capital")


def test_free_margin_below_the_smallest_order_is_not_ready(certified):
    certified.client.account.return_value = dict(totalWalletBalance=5000, totalMarginBalance=5000, availableBalance=0.99,
                                                 totalInitialMargin=4999.01, totalUnrealizedProfit=0)
    blocked_before_any_order(certified, process(certified), BoundaryStatus.CAPITAL_NOT_READY,
                             f"CAPITAL_NOT_READY:{R.INSUFFICIENT_FREE_MARGIN}", R.INSUFFICIENT_FREE_MARGIN, "capital")


@pytest.mark.parametrize("defect,reason", [
    ("lost", "LOGICAL_RESERVATION_UNKNOWN"), ("unreadable", "LOGICAL_RESERVATION_UNKNOWN"),
    ("released", "LOGICAL_RESERVATION_NOT_VALID"), ("expired", R.RESERVATION_EXPIRED),
    ("other_bot", R.RESERVATION_BOT_MISMATCH), ("shadow", R.RESERVATION_NOT_EXECUTION_SCOPED)])
def test_the_plans_own_valid_reservation_is_required(certified, defect, reason):
    """The row is read back from the store by ``prepare_submission``; here the store answers something else."""
    reservations = certified.boundary_for().reservations
    real = reservations.get

    def get(reservation_id):
        row = real(reservation_id)
        if defect == "lost":
            return None
        if defect == "unreadable":
            raise RuntimeError("database is locked")
        return dataclasses.replace(row, **{
            "released": dict(status="RELEASED"), "other_bot": dict(bot_instance_id="another-bot"),
            "shadow": dict(mode="SHADOW"),
            # still marked RESERVED (the expiry sweep has not run), but its time ended at this very moment
            "expired": dict(created_at=certified.now - 60_000, expires_at=certified.now)}[defect])

    captured = {}
    real_prepare = production.prepare_submission

    def prepare(*a, **k):
        reservations.get = get
        try:
            captured.update(real_prepare(*a, **k))
        finally:
            reservations.get = real                            # the boundary reads the true row afterwards
        return dict(captured)

    production.prepare_submission = prepare
    try:
        result = process(certified)
    finally:
        production.prepare_submission = real_prepare
    assert captured["capital"] == planner.CapitalReadiness(False, "LOGICAL", reason)
    blocked_before_any_order(certified, result, BoundaryStatus.CAPITAL_NOT_READY, f"CAPITAL_NOT_READY:{reason}")


def test_readiness_asks_nothing_more_of_the_venue(certified):
    """The evidence is what the cycle already holds: the preparation makes the same four reads as before."""
    calls = []
    real_prepare = production.prepare_submission

    def prepare(db, account, bot, client, *a, **k):
        client.reset_mock()                                   # call records only; answers are kept
        prepared = real_prepare(db, account, bot, client, *a, **k)
        calls.extend(name for name, _, _ in client.method_calls)
        return prepared

    production.prepare_submission = prepare
    try:
        assert process(certified)["boundary"]["status"] == BoundaryStatus.EXECUTED
    finally:
        production.prepare_submission = real_prepare
    assert sorted(calls) == ["depth", "exchange_info_cached", "klines", "last_price"]


# ═══════════════════════════════════════════════════════════ the certification harness (same tail)
def test_the_certification_harness_is_given_the_same_checked_evidence(broker):  # noqa: F811
    h, state = broker
    report = cert.run(h.db, h.account["id"], "tail-ok", action="hold")
    assert report["status"] == "PROTECTED" and state["create_count"] == 1, report
    evidence = report["risk_controls"]["execution_readiness"]
    assert evidence["broker_health"]["status"] == "HEALTHY" and evidence["broker_health"]["reason_codes"] == []
    assert evidence["capital"]["ready"] is True and evidence["capital"]["reservation_status"] == "RESERVED"


def test_the_certification_harness_is_stopped_by_a_venue_backoff(broker):  # noqa: F811
    h, state = broker
    binance_client._note_backoff(DEMO_ENDPOINT, 30.0, 418)
    report = cert.run(h.db, h.account["id"], "tail-backoff", action="hold")
    assert report["status"] == "BLOCKED" and report["reason"] == "PREVALIDATION_BROKER_DEGRADED", report
    assert state["create_count"] == 0 and not state["legs"]
    recorded = report["risk_controls"]["execution_readiness"]["broker_health"]
    assert recorded["status"] == "UNAVAILABLE" and recorded["reason_codes"] == [R.BROKER_RATE_LIMIT_BACKOFF]


def test_the_certification_harness_is_stopped_by_an_unresolved_transfer(broker):  # noqa: F811
    h, state = broker
    add_transfer(h, "SUBMITTED")
    report = cert.run(h.db, h.account["id"], "tail-transfer", action="hold")
    assert report["status"] == "BLOCKED", report
    assert report["reason"] == f"CAPITAL_NOT_READY:{planner.ACCOUNT_RECONCILIATION_REQUIRED}"
    assert state["create_count"] == 0 and not state["legs"]


# ══════════════════════════════════════════════════════════════════════════════════ the plan mode
@pytest.mark.parametrize("environment,mode", [("live", "LIVE"), ("mainnet", "LIVE"), ("LIVE", "LIVE"), ("demo", "DEMO"),
                                              ("testnet", "DEMO"), ("paper", "DEMO"), ("DEMO", "DEMO")])
def test_the_plan_mode_is_the_account_environment(fresh, environment, mode):  # noqa: F811
    plan = production.build_plan(fresh.row, {**fresh.account, "environment": environment}, fresh.plan.bot_instance_id,
                                 fresh.instrument("ADAUSDT"), "resv-any")
    assert plan.mode == plan.environment == mode and plan.mode in P.C.EXECUTING_MODES
    assert verify_trade_plan_integrity(plan)


def test_an_unknown_environment_builds_no_plan(fresh):  # noqa: F811
    with pytest.raises(ValueError):
        production.build_plan(fresh.row, {**fresh.account, "environment": "moon"}, fresh.plan.bot_instance_id,
                              fresh.instrument("ADAUSDT"), "resv-any")


def test_the_real_gate_requires_the_new_mode_and_refuses_the_old_one(certified):
    plan = production.build_plan(certified.row, certified.account, certified.plan.bot_instance_id,
                                 certified.instrument("ADAUSDT"), "resv-any")
    assert plan.environment == "DEMO"
    assert certified.gate.authorize(plan, now_ms=certified.now) == (True, "FAMILY_AUTHORIZED")
    as_before = S.rebuilt(plan, mode="LIVE")                 # what build_plan produced on a demo account before
    assert certified.gate.authorize(as_before, now_ms=certified.now) == (False, P.FAMILY_ENVIRONMENT_NOT_AUTHORIZED)
    as_shadow = S.rebuilt(plan, mode="SHADOW")
    assert certified.gate.authorize(as_shadow, now_ms=certified.now) == (False, P.FAMILY_MODE_NOT_EXECUTABLE)
    # the mode is hashed: the two are different plans with different attempt identities
    assert as_before.trade_plan_hash != plan.trade_plan_hash and as_before.trade_plan_id != plan.trade_plan_id
    ids = {ExecutionAttempt.build_id(trade_plan_id=p.trade_plan_id, trade_plan_hash=p.trade_plan_hash,
                                     broker_account_id=p.broker_account_id) for p in (plan, as_before)}
    assert len(ids) == 2


def test_exactly_once_per_decision_does_not_rest_on_the_plan_identity(certified):
    """Because the mode changed the plan id of a demo decision, the attempt-id check of the boundary cannot be
    what stops a second order for a decision attempted under the other identity. The reservation does: its id
    comes from the account, the bot and the decision, and it is refused once that decision has an attempt."""
    result = process(certified)
    assert result["boundary"]["status"] == BoundaryStatus.EXECUTED
    boundary, row = certified.boundary_for(), certified.row
    ins = boundary.preflight.catalog.record(boundary.preflight.venue_key, "DEMO", "ADAUSDT")["instrument"]
    again = boundary.reservations.reserve(
        broker_account_id=certified.account["id"], bot_instance_id=certified.plan.bot_instance_id,
        cycle_id=row["decision_id"], selected=[(row["decision_id"], ins.canonical_symbol, boundary.adapter.venue,
                                                ins.venue_symbol, row["side"])],
        now_ms=certified.now, ttl_seconds=60, max_open_positions=1, mode="PRODUCTION",
        production_scope=(certified.account["user_id"], certified.account["id"]))
    assert not again.reserved and again.conflict_reason == "CATI_DECISION_ALREADY_ATTEMPTED"
    assert again.reservation is None and result["reservation_id"].startswith("resv")


# ═════════════════════════════════════════════════════════════════ the operator transport tool
def smoke_scope(monkeypatch):
    client = SimpleNamespace(broker_environment="demo", base_url=DEMO_ENDPOINT)
    monkeypatch.setattr("app.trading_intelligence.integration.residual_prospective.owner_current", lambda db: True)
    payload = {"newClientOrderId": "CFSMOKEabc", "symbol": "ADAUSDT", "side": "BUY", "type": "MARKET"}
    return client, payload, smoke._permit.set((client, object(), "CFSMOKEabc", "ADAUSDT"))


def test_the_smoke_permit_is_dead_inside_a_strategy_or_certification_submission(monkeypatch):
    client, payload, token = smoke_scope(monkeypatch)
    try:
        assert smoke.permitted(client, payload)                                   # the tool's own narrow permit
        request = SimpleNamespace(risk_decision_id="risk-1", trade_plan_id="tplan-1", trade_plan_hash="hash")
        with boundary_entry_permit(request):
            assert not smoke.permitted(client, payload)                           # a strategy entry is being submitted
        assert smoke.permitted(client, payload)
        harness = cert._permit.set(("db", "acct", "tplan", "hash"))
        try:
            assert not smoke.permitted(client, payload)                           # the certification harness is running
        finally:
            cert._permit.reset(harness)
    finally:
        smoke._permit.reset(token)
    assert not smoke.permitted(client, payload)


def test_the_smoke_refuses_to_start_inside_a_boundary_submission(demo, monkeypatch):  # noqa: F811
    factory = Mock()
    monkeypatch.setattr("shared_lib.broker.client_factory.build_client_from_auth", factory)
    request = SimpleNamespace(risk_decision_id="risk-1", trade_plan_id="tplan-1", trade_plan_hash="hash")
    with boundary_entry_permit(request):
        with pytest.raises(ValueError, match="DEMO_SMOKE_INSIDE_BOUNDARY_SUBMISSION"):
            smoke.run(demo.db, demo.account["id"])
    factory.assert_not_called()


def test_the_smoke_opens_nothing_while_strategy_execution_is_unresolved(demo, monkeypatch):  # noqa: F811
    from shared_lib.broker.auto_trading import set_authorization

    with demo.db.connect() as c:
        c.execute("UPDATE broker_accounts SET environment='demo' WHERE id=?", (demo.account["id"],))
        c.execute("UPDATE bot_instances SET status='stopped' WHERE broker_account_id=?", (demo.account["id"],))
        c.execute("UPDATE bot_instances SET status='active' WHERE id=?", (demo.plan.bot_instance_id,))
        set_authorization(c, account_id=demo.account["id"], user_id=demo.account["user_id"],
                          bot_instance_id=demo.plan.bot_instance_id, enabled=True, now="fixture")
    client = Mock(broker_environment="demo", base_url=DEMO_ENDPOINT, _broker_account_id=demo.account["id"])
    monkeypatch.setattr("shared_lib.broker.resolver.resolve_broker_auth", lambda *a: None)
    monkeypatch.setattr("shared_lib.broker.client_factory.build_client_from_auth", lambda auth: client)
    # an entry of a trade plan whose outcome is unknown: the reservation is held RESOLUTION_PENDING
    assert demo.reservations.mark_resolution_pending(
        demo.plan.portfolio_reservation_id, demo.now, trade_plan_id=demo.plan.trade_plan_id,
        execution_attempt_id="attempt-unknown", resolution_deadline=demo.now + 900_000, note="SUBMIT_UNKNOWN")
    assert production.open_lineage(demo.db, demo.account["id"])["bots"] == [demo.plan.bot_instance_id]
    with pytest.raises(ValueError, match="DEMO_SMOKE_STRATEGY_EXECUTION_UNRESOLVED"):
        smoke.run(demo.db, demo.account["id"])
    client._signed_post.assert_not_called()
    client.position_risk.assert_not_called()                 # refused before any venue read of a new smoke
    with demo.db.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM demo_transport_smoke").fetchone()[0] == 0


def test_the_smoke_says_what_it_is():
    assert "NOT A STRATEGY PATH" in smoke.__doc__ and "CATIExecutionBoundary" in smoke.__doc__
    source = inspect.getsource(smoke)
    for strategy_object in ("TradePlan", "process_trade_plan", "reservations.reserve", "ExecutionAttempt"):
        assert strategy_object not in source.replace(smoke.__doc__, ""), strategy_object
