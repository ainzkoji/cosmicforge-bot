"""Per-bot sizing configuration, from the saved deployment row to the order the production path would send.

Every bot instance carries its own deployment settings (budget, allocation mode and value, risk level,
leverage ceiling, optional maximum position). These tests drive the REAL production factory
(``production_execution.process_account`` -> ``boundary_for`` -> TradingOrchestrator -> BinanceExecutor), as
``test_natural_signal_replay`` does, with only the exchange transport mocked, and check that:

* the saved row is read as saved -- a fixed amount is the MARGIN per trade, a percentage allocation is that
  percentage of the bot's own budget, a risk-based bot is sized from budget x risk / stop -- and is never
  written back or resized;
* the owner's deployment (Binance DEMO, 417.58 USDT equity, Balanced, fixed 120 USDT, 1INCHUSDT LONG with a
  5.3 % structural stop) is approved by hard risk at its full 120 USDT margin and is then REFUSED -- by the
  experimental DEMO notional limit, and by the 0.40 % executable-price risk limit (L-08) -- with an explanation
  of the planned risk, never reduced;
* a percentage budget is resolved against the account equity, never read as an amount of USDT;
* two bots on one account keep their own configurations while the account-level rules (one execution owner,
  the strictest daily loss) still apply, and an existing position blocks a new entry.

Family authorization: the residual family FAILED its research gate, so -- exactly as ``conftest``'s
``_legacy_execution_tail_family_authorization`` does for ``test_natural_signal_replay`` -- the
family-authorization part of the gate is reduced to "the family is defined in the registry" inside this
module's fixture. The decision-verification part of the gate still runs for real.
"""
from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

import test_natural_signal_replay as replay
from _exec import cati_bot
from test_natural_signal_replay import deployed  # noqa: F401  (fixture)
from test_production_demo_execution import demo  # noqa: F401  (fixture)
from test_production_execution import live  # noqa: F401  (fixture)
from app.policy.effective_policy import resolve_effective_bot_policy
from app.core.bot_instance_service import BotInstanceService
from app.trading_intelligence.execution.leverage import resolve_cati_leverage
from app.trading_intelligence.execution.risk_sizing import RISK_SIZE_OPEN_RISK_CAP_REACHED, size_risk_based
from app.trading_intelligence.integration import production_execution as production
from app.trading_intelligence.integration import reason_registry

#: The owner's account and decision, as recorded (Binance DEMO account brk_c729454e6c98, bot bot_a8117dc719fc,
#: decision cc5d1dd3...: 1INCHUSDT LONG, reference 0.08288, structural stop 0.0784714, next open 0.08289).
OWNER_EQUITY = 417.58
OWNER_ROW = dict(id="bot_a8117dc719fc", user_id="owner", broker_account_id="brk_c729454e6c98", market_type="CRYPTO",
                 strategy_id="cati", strategy_version="1.0.0", risk_level="balanced", risk_profile_id="__auto_pilot__",
                 config_id="__auto_pilot__", allocation_type="fixed_amount", allocation_value=120.0,
                 capital_allocation=120.0, capital_allocation_type="fixed_amount", mode="live", symbols=[],
                 timeframes=["15m"], universe_mode="BROKER", daily_loss_limit_pct=None, max_position_usdt=None,
                 risk_profile_version=None)
ONE_INCH = ("1INCHUSDT", "LONG", 2.2936201370434137, 0.08288, 0.07847142857142857, 0.09390142857142855,
            0.004408571428571422, 0.0006742857142857146, 0.08289)
REAL_TICK = "0.0001"          # 1INCHUSDT's price tick: one tick is ~12 bps of the price
FINE_TICK = "0.000001"        # a hypothetical tick fine enough for the plan's own 2 bps spread bound

BOT_COLUMNS = ("allocation_type", "allocation_value", "capital_allocation", "capital_allocation_type", "risk_level",
               "mode", "max_position_usdt", "daily_loss_limit_pct", "risk_profile_version", "status")


def _family_defined_only(monkeypatch):
    from app.trading_intelligence.pipeline import contracts, family_registry, provenance

    def defined_family_only(self, plan):
        definition = family_registry.definitions().get(contracts.provenance_of(plan).get("family_id"))
        if definition is None:
            return None, provenance.FAMILY_UNKNOWN
        return SimpleNamespace(definition=definition), None

    monkeypatch.setattr(provenance.FamilyExecutionGate, "family_authorization", defined_family_only)
    monkeypatch.setattr(provenance.FamilyExecutionGate, "venue_certification", lambda self, plan, now_ms: None)


@pytest.fixture
def owner(deployed, monkeypatch):  # noqa: F811
    """The natural-replay deployment with the owner's equity and the recorded 1INCHUSDT decision available."""
    _family_defined_only(monkeypatch)
    monkeypatch.setattr(replay, "EQUITY", OWNER_EQUITY)
    monkeypatch.setitem(replay.RECORDED, "1INCH", ONE_INCH)
    monkeypatch.setitem(replay.VENUE, "1INCHUSDT", (REAL_TICK, "1", "5"))
    deployed.bot_id = deployed.plan.bot_instance_id
    return deployed


def configure(h, **row):
    """The deployment row of the harness bot, as the supported deployment workflow persists it."""
    base = dict(capital=120.0, allocation=120.0, allocation_type="fixed_amount")
    base.update({k: row.pop(k) for k in list(row) if k in base})
    cati_bot(h.db, h.bot_id, h.account["id"], user_id=h.plan.user_id, **base)
    with h.db.connect() as c:
        c.execute("UPDATE bot_instances SET mode='live' WHERE id=?", (h.bot_id,))
        for column, value in row.items():
            c.execute(f"UPDATE bot_instances SET {column}=? WHERE id=?", (value, h.bot_id))
    production._boundaries.clear()


def saved(h, bot_id=None):
    with h.db.connect() as c:
        row = c.execute(f"SELECT {','.join(BOT_COLUMNS)} FROM bot_instances WHERE id=?",
                        (bot_id or h.bot_id,)).fetchone()
    return tuple(row)


def governed_market_policy(h, monkeypatch, *, tick):
    """The production order budget: the code-constant default policy (market entries refused) plus an
    owner-approved, DEMO-only, single-account all-symbols market-entry record (L-08) -- the 0.40 % check at the
    executable price applies."""
    from app.trading_intelligence.pipeline.family_governance import OperatorAuthorization
    from app.trading_intelligence.scale import market_entry_policy as M
    from app.trading_intelligence.scale import spread_gate as G
    from app.trading_intelligence.scale.order_spreading import CONSERVATIVE_DEFAULT

    monkeypatch.setitem(G.PlatformSpreadGate.__init__.__kwdefaults__, "policy", CONSERVATIVE_DEFAULT)
    monkeypatch.setitem(replay.VENUE, "1INCHUSDT", (tick, "1", "5"))
    pdb = h.platform_db()
    with pdb.transaction(write=False) as conn:
        now = conn.now_ms()
    M.MarketEntryPolicyLedger(pdb).record(
        OperatorAuthorization(operator_id="operator-test", channel="test:sizing", permission="admin-emergency"),
        venue=h.plan.venue, symbol=M.ALL_SYMBOLS, environment="DEMO",
        terms=dict(max_participation="0.10", max_slippage_bps="25", max_spread_bps="25", max_depth_age_ms=5000,
                   min_levels=5, absolute_cap_notional="5000", expires_at_ms=now + 86_400_000),
        approval_ref="TEST-ONLY", reason="per-bot sizing test inside a disposable database",
        account_id=h.plan.broker_account_id)


def experimental_grant(h, monkeypatch, *, cap="100.0"):
    """An experimental DEMO authorization with a notional limit, as on the owner's account, recorded in the test
    database; the family gate reports it for the plan as the real gate does for a granted DEMO plan."""
    from app.trading_intelligence.governance import experimental_demo as X
    from app.trading_intelligence.pipeline import provenance

    ledger, now = X.ExperimentalDemoLedger(h.db), h.now
    scope = {"authorization_id": "xdemo_sizing_test", "account_id": h.account["id"], "user_id": h.plan.user_id,
             "family_id": "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1"}
    payload = {"family_rule_hash": "test", "research_status_at_grant": "FAILED", "stage_at_grant": "M0",
               "venue": "binance", "base_url": "https://demo-fapi.binance.com", "approval_ref": "TEST-ONLY",
               "reason": "sizing test", "duration_hours": 72, "max_entry_notional_usdt": cap,
               "expires_at": now + 72 * 3_600_000, "label": X.LABEL, "not_certification": True,
               "grants_live_authority": False}
    with h.db.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        X.ensure(conn)
        ledger._append(conn, X.AUTHORIZED, scope, "operator:test", payload, now)
    original = provenance.FamilyExecutionGate.authorize

    def granted(self, plan, *, now_ms):
        allowed, why = original(self, plan, now_ms=now_ms)
        return (True, f"{X.AUTHORIZED_REASON}:xdemo_sizing_test") if allowed else (allowed, why)

    monkeypatch.setattr(provenance.FamilyExecutionGate, "authorize", granted)


def risk_payload(h, result):
    return h.approved(result)


# ── 1. the saved row is the configuration ───────────────────────────────────────────────────────────────────

def test_the_owner_row_resolves_to_its_own_fixed_margin_and_is_not_rewritten():
    instance = SimpleNamespace(**OWNER_ROW)
    before = dict(vars(instance))
    policy = resolve_effective_bot_policy(instance=instance, broker_environment="demo",
                                          risk_params=BotInstanceService.get_risk_profile_preset("balanced"))
    assert (policy.capital_budget, policy.capital_allocation_type) == (120.0, "fixed_amount")
    assert (policy.position_allocation_type, policy.position_allocation_value) == ("fixed_amount", 120.0)
    assert policy.risk_level == "balanced" and policy.execution_mode == "broker"
    assert policy.max_position_usdt is None
    assert policy.max_daily_loss_pct == pytest.approx(0.025) and policy.daily_loss_source == "RISK_PROFILE_DEFAULT"
    assert policy.max_leverage == pytest.approx(10.0)          # BROKER universe: per-symbol ceilings per order
    assert vars(instance) == before                             # resolving never alters the saved settings


def test_the_production_factory_maps_a_fixed_amount_to_a_fixed_margin(owner):
    configure(owner)
    boundary = production.boundary_for(owner.db, owner.account, {"id": owner.bot_id}, owner.client)
    config = boundary.orchestrator.validated_config
    assert (config.use_fixed_size, config.fixed_size_usdt) == (True, 120.0)
    executor = boundary.adapter.executor
    assert (executor._allocation_type, executor._allocation_value, executor._capital_budget) == \
        ("fixed_amount", 120.0, 120.0)
    from app.risk.capital_ledger import per_trade_allocation_margin
    assert per_trade_allocation_margin(executor._allocation_type, executor._allocation_value,
                                       executor._capital_budget) == 120.0       # margin per trade, not notional


# ── 2. the owner's deployment, end to end ────────────────────────────────────────────────────────────────────

def test_the_owner_fixed_120_is_approved_at_full_margin_with_resolved_leverage(owner):
    """Hard risk keeps the fixed 120 USDT margin and resolves the leverage (balanced compound-risk limit:
    0.225 / 5.33 % -> 4x). Notional 480, loss at the stop ~25.6 USDT = ~6.1 % of 417.58 USDT. Under the test
    harness order budget (market entries allowed, no L-08 record) nothing refuses it -- the refusals come from
    the governed limits below."""
    configure(owner)
    before = saved(owner)
    owner.signal("1INCH")
    result = owner.process()
    risk = risk_payload(owner, result)
    assert risk["status"] == "APPROVED" and risk["resolved_leverage"] == 4
    assert risk["resolved_notional"] == pytest.approx(480.0)
    assert risk["resolved_notional"] / risk["resolved_leverage"] == pytest.approx(120.0)
    stop = ONE_INCH[4]
    loss = risk["resolved_quantity"] * (ONE_INCH[8] - stop)
    assert loss == pytest.approx(25.59, abs=0.05) and loss / OWNER_EQUITY == pytest.approx(0.0613, abs=0.001)
    assert saved(owner) == before


def test_no_leverage_lets_a_fixed_120_margin_fit_the_040_limit_at_a_53_percent_stop():
    stop = (ONE_INCH[8] - ONE_INCH[4]) / ONE_INCH[8]
    res = resolve_cati_leverage(stop_fraction=stop, user_max=10, asset_class_max=7, compound_risk_limit=0.225,
                                slippage_fraction=0.0002)
    assert (res.leverage, res.binding) == (4, "COMPOUND_RISK_LIMIT")
    ceiling = 0.004 * OWNER_EQUITY                                       # 1.67 USDT
    assert all(120.0 * lev * stop > ceiling for lev in range(1, 11))    # even 1x risks 6.40 USDT (1.53 %)
    assert 120.0 * 1 * 0.0139 <= ceiling < 120.0 * 1 * 0.0140           # the widest stop 120 USDT at 1x fits: ~1.39 %


def test_the_owner_trade_is_refused_by_the_experimental_demo_limit_with_an_explanation(owner, monkeypatch):
    configure(owner)
    before = saved(owner)
    experimental_grant(owner, monkeypatch, cap="100.0")
    owner.signal("1INCH")
    result = owner.process()
    assert result["boundary"]["status"] == "GOVERNANCE_NOT_AUTHORIZED"
    assert result["reason"] == "EXPERIMENTAL_DEMO_NOTIONAL_LIMIT_EXCEEDED:480.0>100.0"
    why = result["refusal_explanation"]
    assert (why["margin_usdt"], why["leverage"], why["notional_usdt"]) == (pytest.approx(120.0), 4.0, pytest.approx(480.0))
    assert why["configured_allocation_changed"] is False and why["order_resized"] is False
    assert "not changed" in why["message"] and "not resized" in why["message"]
    owner.client.place_order.assert_not_called()
    assert saved(owner) == before
    json.dumps(result, default=str)   # the evaluation record stays serialisable


def test_with_the_real_tick_the_governed_market_entry_refuses_on_the_plans_own_spread_bound(owner, monkeypatch):
    """1INCHUSDT's one-tick spread (~12 bps) is wider than the frozen plan's own 2 bps bound: under the governed
    market policy that refusal comes before the 0.40 % check is reached."""
    configure(owner)
    governed_market_policy(owner, monkeypatch, tick=REAL_TICK)
    owner.signal("1INCH", spread_ticks=1)
    result = owner.process()
    assert result["boundary"]["status"] == "ORDER_SPREADING_REFUSED"
    assert result["reason"] == "MARKET_SPREAD_EXCESSIVE"
    owner.client.place_order.assert_not_called()


def test_the_owner_trade_is_refused_at_the_executable_price_for_risking_more_than_040(owner, monkeypatch):
    configure(owner)
    before = saved(owner)
    governed_market_policy(owner, monkeypatch, tick=FINE_TICK)
    owner.signal("1INCH")
    result = owner.process()
    assert result["boundary"]["status"] == "ORDER_SPREADING_REFUSED"
    assert result["reason"] == "EXECUTABLE_PRICE_RISK_EXCEEDED"
    why = result["refusal_explanation"]
    assert why["margin_usdt"] == pytest.approx(120.0) and why["leverage"] == 4.0
    assert why["planned_risk_pct"] == pytest.approx(6.13, abs=0.05)
    assert why["per_trade_risk_ceiling_pct"] == 0.40
    assert why["max_risk_usdt_at_ceiling"] == pytest.approx(1.67, abs=0.01)
    assert "0.40%" in why["message"] and "not changed" in why["message"]
    entry = reason_registry.describe(result["reason"])
    assert entry["severity"] == "blocked" and "0.40%" in entry["message"] and "not changed" in entry["message"]
    owner.client.place_order.assert_not_called()
    assert saved(owner) == before


# ── 3. the other allocation modes ────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("capital,pct,basis", [(400.0, 25.0, 400.0),           # budget below equity: % of budget
                                                (1000.0, 25.0, OWNER_EQUITY)])  # equity below budget: % of equity
def test_a_percentage_allocation_is_that_percentage_of_the_usable_balance_as_notional(owner, capital, pct, basis):
    """"% of balance": the position notional is that percentage of the balance the bot may use, min(equity,
    budget). Before the fix the percentage never reached CATI sizing (the plan was sized as atr_risk)."""
    configure(owner, capital=capital, allocation=pct, allocation_type="percent_balance")
    before = saved(owner)
    owner.signal("ADA_7")
    result = owner.process()
    assert result["boundary"]["status"] == "EXECUTED", result["boundary"]
    risk = risk_payload(owner, result)
    assert risk["resolved_leverage"] == 3
    assert risk["resolved_notional"] == pytest.approx(basis * pct / 100.0)
    [order] = owner.seen["orders"]
    submitted = float(order.qty) * owner.client.last_price.return_value
    assert 0.99 * basis * pct / 100.0 <= submitted <= basis * pct / 100.0 + 1e-9   # never above, step rounding below
    assert saved(owner) == before


def test_risk_based_sizing_computes_the_size_instead_of_using_the_budget(owner, monkeypatch):
    """A risk-based bot with the same 120 USDT budget on the same signal: 120 x 0.40 % (the Balanced 0.5 % is
    clamped to the system ceiling) / 5.33 % -> ~9 USDT notional at the profile's 2x ceiling. It passes the 0.40 %
    check at the executable price where the fixed 120 USDT is refused."""
    configure(owner, capital=120.0, allocation=0.5, allocation_type="risk_based")
    governed_market_policy(owner, monkeypatch, tick=FINE_TICK)
    owner.signal("1INCH")
    result = owner.process()
    assert result["boundary"]["status"] == "EXECUTED", result["boundary"]
    risk = risk_payload(owner, result)
    assert risk["resolved_leverage"] == 2
    assert risk["resolved_notional"] == pytest.approx(108 * ONE_INCH[8])          # floor(9.0 / 0.08289) coins
    loss = risk["resolved_quantity"] * (ONE_INCH[8] - ONE_INCH[4])
    assert loss <= 120.0 * 0.004 + 1e-9 and loss / OWNER_EQUITY < 0.004


def test_risk_based_sizing_respects_existing_open_risk_and_the_user_maximum():
    common = dict(budget_usdt=1000, risk_fraction=0.004, stop_distance_fraction=0.05, price=10, leverage=5,
                  leverage_ceiling=5, qty_step=1, min_qty=1, min_notional=5)
    free = size_risk_based(**common)
    assert free.approved and free.notional_usdt == Decimal("80")                     # 1000 x 0.4 % / 5 %
    capped = size_risk_based(**common, max_position_usdt=50)
    assert capped.approved and capped.notional_usdt == Decimal("50") and capped.binding == "USER_MAX_POSITION"
    partly = size_risk_based(**common, open_risk_usdt=8, max_open_risk_fraction=0.01)   # 2 USDT of open risk left
    assert partly.approved and partly.notional_usdt == Decimal("40") and partly.binding == "OPEN_RISK_CAP"
    full = size_risk_based(**common, open_risk_usdt=10, max_open_risk_fraction=0.01)
    assert not full.approved and full.reason == RISK_SIZE_OPEN_RISK_CAP_REACHED


def test_a_percentage_budget_is_resolved_against_equity_never_read_as_usdt(owner):
    """``capital_allocation_type = percent_balance``: the saved 50 is 50 % of the account equity. Before the fix
    the executor's capital ledger took it as 50 USDT."""
    configure(owner, capital=50.0, allocation=0.5, allocation_type="risk_based",
              capital_allocation_type="percent_balance")
    boundary = production.boundary_for(owner.db, owner.account, {"id": owner.bot_id}, owner.client)
    executor = boundary.adapter.executor
    assert executor._capital_budget == 0.0                    # unresolved: the ledger refuses until the cycle's equity
    assert production.resolve_executor_capital_budget(boundary, OWNER_EQUITY) == pytest.approx(208.79)
    assert executor._capital_budget == pytest.approx(208.79)
    with pytest.raises(ValueError, match="ACCOUNT_CAPITAL_BUDGET_UNRESOLVED"):
        production.resolve_executor_capital_budget(boundary, None)
    assert executor._capital_budget == 0.0                    # an unusable equity leaves nothing authorised


# ── 4. several bots on one account ───────────────────────────────────────────────────────────────────────────

def second_bot(h, bot_id, **config):
    cati_bot(h.db, bot_id, h.account["id"], user_id=h.plan.user_id, **config)   # inserts an active CATI row
    with h.db.connect() as c:
        c.execute("UPDATE bot_instances SET mode='live', status='active' WHERE id=?", (bot_id,))


def test_two_bots_on_one_account_keep_their_own_configuration_and_share_the_account_rules(owner):
    configure(owner)                                                        # fixed 120, Balanced
    second_bot(owner, "bot_second_cfg", capital=300.0, allocation=0.25, allocation_type="risk_based")
    with owner.db.connect() as c:
        c.execute("UPDATE bot_instances SET risk_level='conservative', daily_loss_limit_pct=NULL WHERE id='bot_second_cfg'")
    before = (saved(owner), saved(owner, "bot_second_cfg"))
    service = BotInstanceService(db=owner.db)
    policies = {}
    for bot_id in (owner.bot_id, "bot_second_cfg"):
        instance = service.get_bot_instance(bot_id)
        policies[bot_id] = resolve_effective_bot_policy(instance=instance, broker_environment="demo",
                                                        risk_params=production.bot_risk_params(service, instance))
    a, b = policies[owner.bot_id], policies["bot_second_cfg"]
    assert (a.position_allocation_type, a.position_allocation_value, a.capital_budget) == ("fixed_amount", 120.0, 120.0)
    assert (b.position_allocation_type, b.capital_budget, b.risk_level) == ("risk_based", 300.0, "conservative")
    assert b.risk_per_trade == pytest.approx(0.0025) and a.risk_per_trade == pytest.approx(0.0025)
    assert a.policy_hash != b.policy_hash
    # The account-wide daily loss is the strictest bot's: the conservative risk-based bot's VERSIONED profile
    # (1 %), not the legacy preset's 2.5 % the account resolver used to read for every bot.
    bots = [{"id": owner.bot_id}, {"id": "bot_second_cfg"}]
    daily = production.account_daily_loss_policy(owner.db, owner.account, bots)
    assert daily == {"pct": pytest.approx(0.01), "source": "RISK_PROFILE_DEFAULT", "bot_instance_id": "bot_second_cfg"}
    # One execution owner per account: with two active bots no entry is taken by either.
    owner.signal("1INCH")
    result = owner.process()
    assert result["reason"] == "ACCOUNT_EXECUTION_OWNER_AMBIGUOUS" and not result["orders_submitted"]
    owner.client.place_order.assert_not_called()
    assert (saved(owner), saved(owner, "bot_second_cfg")) == before


def test_an_existing_position_on_the_account_blocks_a_new_entry_without_touching_the_configuration(owner):
    configure(owner)
    before = saved(owner)
    owner.signal("1INCH")
    manual = {"symbol": "BTCUSDT", "positionAmt": "0.01", "entryPrice": "60000", "positionSide": "BOTH"}
    result = owner.process(positions=[manual])
    assert result["reason"] == "ACCOUNT_WIDE_POSITION_OR_ORDER_ACTIVE" and not result["orders_submitted"]
    owner.client.place_order.assert_not_called()
    assert saved(owner) == before


def test_every_new_sizing_refusal_code_has_a_customer_message():
    for code in ("EXECUTABLE_PRICE_RISK_EXCEEDED", "EXECUTABLE_PRICE_RISK_UNVERIFIABLE",
                 "EXECUTABLE_STOP_DISTANCE_EXCEEDED", "MARKET_PREDICTED_SLIPPAGE_EXCEEDS_BOUND",
                 "MARKET_POLICY_BUDGET_MISMATCH", "DEPTH_INSUFFICIENT_FOR_QUANTITY",
                 "PERCENT_ALLOCATION_MARGIN_UNRESOLVED"):
        entry = reason_registry.describe(code)
        assert entry["severity"] != reason_registry.UNKNOWN and entry["message"], code
