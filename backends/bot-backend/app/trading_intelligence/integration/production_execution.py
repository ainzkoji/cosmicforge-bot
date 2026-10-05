"""Frozen residual decisions -> existing account-scoped execution boundary.

Driven only by production_runtime's canonical lease. Observation outcomes are
never fills. No legacy runner, alternate strategy, credentials or retry loop.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone, time as day_time
from zoneinfo import ZoneInfo
import json
import hashlib
import math
import time

from app.core.config import settings
from shared_lib.broker.environment import normalize_environment
from shared_lib.core.production import order_submission_gate
from .residual_prospective import FAMILY, REGISTRY_HASH, SOURCE, Q, owner_current, frozen_definition

_boundaries = {}
_TRADING_INCOME = {"REALIZED_PNL", "COMMISSION", "FUNDING_FEE", "INSURANCE_CLEAR", "COMMISSION_REBATE",
                   "DELIVERED_SETTELMENT", "DELIVERED_SETTLEMENT", "POSITION_LIMIT_INCREASE_FEE", "FEE_RETURN", "API_REBATE"}


def complete_income(client, start, now):
    """Bounded complete history in venue-supported seven-day windows."""
    windows, history = [], []
    while start <= now:
        end = min(now, start + 7*86400000 - 1)
        windows.append((start, end))
        start = end + 1
    pages = 0
    while windows:
        pages += 1
        if pages > 512:
            raise ValueError("BROKER_INCOME_HISTORY_INCOMPLETE")
        start, end = windows.pop()
        page = client.income_history(start_time_ms=start, end_time_ms=end, limit=1000)
        if not isinstance(page, list):
            raise ValueError("BROKER_INCOME_HISTORY_UNAVAILABLE")
        if any(not isinstance(p, dict) or not start <= int(p["time"]) <= end or
               not math.isfinite(float(p["income"])) or p.get("asset", "USDT") != "USDT" for p in page):
            raise ValueError("BROKER_INCOME_HISTORY_INVALID")
        if len(page) < 1000:
            history.extend(page)
        else:
            if start == end:
                raise ValueError("BROKER_INCOME_HISTORY_AMBIGUOUS")
            middle = (start+end)//2
            windows.extend(((start, middle), (middle+1, end)))
    return history


def account_periods(db, account, client, wallet, equity, risk_date, zone, now):
    """Reconstruct cash-ledger peaks, persist observed equity peaks per account.

    External cash transfers change capital, not trading drawdown. No legacy
    bot runner is required to create the weekly/monthly risk baseline.
    """
    from app.risk.state import get_week_start, get_month_start
    starts = {"weekly": get_week_start(risk_date), "monthly": get_month_start(risk_date)}
    earliest = int(datetime.combine(min(starts.values()), day_time.min, zone).timestamp()*1000)
    history = complete_income(client, earliest, now)
    output = {}
    with db.connect() as c:
        c.execute("CREATE TABLE IF NOT EXISTS cati_account_period_risk(account_id TEXT,period TEXT,start_date TEXT,peak_equity REAL NOT NULL,transfers REAL NOT NULL DEFAULT 0,PRIMARY KEY(account_id,period,start_date))")
        for period, date in starts.items():
            stamp = int(datetime.combine(date, day_time.min, zone).timestamp()*1000)
            rows = sorted((r for r in history if int(r["time"]) >= stamp), key=lambda r:(int(r["time"]), str(r.get("tranId", ""))))
            opening = wallet - sum(float(r["income"]) for r in rows)
            transfers = sum(float(r["income"]) for r in rows if r["incomeType"] not in _TRADING_INCOME)
            adjusted = equity
            running = peak = opening
            for r in rows:
                if r["incomeType"] in _TRADING_INCOME:
                    running += float(r["income"])
                else:
                    running += float(r["income"])
                    peak += float(r["income"])
                peak = max(peak, running)
            peak = max(peak, adjusted)
            if not all(math.isfinite(v) for v in (peak, adjusted)) or peak <= 0:
                raise ValueError("BROKER_PERIOD_RISK_BASIS_UNAVAILABLE")
            c.execute("INSERT INTO cati_account_period_risk VALUES(?,?,?,?,?) ON CONFLICT(account_id,period,start_date) DO UPDATE SET peak_equity=MAX(peak_equity+excluded.transfers-transfers,excluded.peak_equity),transfers=excluded.transfers", (account["id"],period,date.isoformat(),peak,transfers))
            durable = c.execute("SELECT peak_equity FROM cati_account_period_risk WHERE account_id=? AND period=? AND start_date=?", (account["id"],period,date.isoformat())).fetchone()[0]
            output[period] = {"peak_equity": durable, "adjusted_equity": adjusted,
                "drawdown_pct": max(0., (durable-adjusted)/durable*100), "source": "BROKER_CASH_LEDGER_AND_DURABLE_EQUITY"}
    return output


def initialize(db):
    with db.connect() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS cati_production_decisions (
            account_id TEXT NOT NULL, decision_id TEXT NOT NULL, bot_instance_id TEXT,
            observed_at INTEGER NOT NULL, document TEXT NOT NULL,
            PRIMARY KEY(account_id,decision_id));
        CREATE TABLE IF NOT EXISTS cati_production_daily_risk (
            account_id TEXT NOT NULL, day TEXT NOT NULL, opening_wallet REAL NOT NULL,
            loss_latched INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(account_id,day));
        CREATE TABLE IF NOT EXISTS cati_production_fills (
            account_id TEXT NOT NULL, trade_id TEXT NOT NULL, order_id TEXT NOT NULL,
            symbol TEXT NOT NULL, document TEXT NOT NULL, PRIMARY KEY(account_id,symbol,trade_id));
        """)


def latest_decision(db):
    with db.connect() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_residual_decisions'").fetchone():
            return None
        row = c.execute("SELECT * FROM cati_residual_decisions WHERE registry_hash=? ORDER BY decision_time DESC LIMIT 1",
                        (REGISTRY_HASH,)).fetchone()
    return dict(row) if row else None


def eligibility(row, now):
    if row is None:
        return "AWAITING_NATURAL_CATI_DECISION"
    state = json.loads(row["risk_state_json"])
    snapshot = json.loads(row["snapshot_json"])
    if row["registry_hash"] != REGISTRY_HASH or state.get("source") != SOURCE:
        return "FROZEN_RESIDUAL_PROVENANCE_REQUIRED"
    if row["decision_id"] != hashlib.sha256((REGISTRY_HASH+":"+str(row["decision_time"])).encode()).hexdigest():
        return "RESIDUAL_DECISION_ID_MISMATCH"
    if state.get("reason") != "SELECTED_TOP1" or snapshot.get("reason") != "SELECTED_TOP1":
        return state.get("reason", "CATI_NOT_ELIGIBLE")
    if not row["portfolio_selected"] or state.get("overlap_rejected"):
        return "RESIDUAL_PORTFOLIO_OVERLAP"
    if not row["decision_time"] < row["recorded_at"] <= now < row["decision_time"] + Q:
        return "PROSPECTIVE_ENTRY_WINDOW_EXPIRED"
    if row.get("entry_price") is None:
        return "NEXT_NATIVE_OPEN_REFERENCE_REQUIRED"
    entry = float(row["entry_price"])
    if not math.isfinite(entry) or not (min(float(row["stop"]), float(row["target"])) < entry <
                                      max(float(row["stop"]), float(row["target"]))):
        return "NON_EXECUTABLE_GAP"
    candidate = snapshot.get("candidate") or {}
    for key, field in (("symbol", "selected_symbol"), ("side", "side"), ("score", "score"),
                       ("entry_reference", "entry_reference"), ("stop", "stop"), ("target", "target"), ("risk", "risk")):
        if candidate.get(key) != row[field]:
            return "RESIDUAL_DECISION_CONTENT_MISMATCH"
    if abs(float(row["score"])) < 2 or len(snapshot.get("eligible_universe", [])) < 30:
        return "RESIDUAL_DECISION_NOT_QUALIFIED"
    family, _ = frozen_definition()
    if row["selected_symbol"] not in family["universe"]:
        return "RESIDUAL_SYMBOL_OUTSIDE_FROZEN_UNIVERSE"
    values = [float(row[k]) for k in ("entry_reference", "stop", "target", "risk")]
    if not all(math.isfinite(v) and v > 0 for v in values):
        return "INVALID_CATI_GEOMETRY"
    price, stop, target, distance = values
    sign = 1 if row["side"] == "LONG" else -1 if row["side"] == "SHORT" else 0
    if not sign or abs(sign*(price-stop)-distance) > 1e-8*price or abs(sign*(target-price)-2.5*distance) > 1e-8*price:
        return "INVALID_CATI_GEOMETRY"
    return None


def account_risk(db, account, client, positions, orders, bots, now):
    """Broker-wide risk, including manual and other bots' activity. Unknown
    history fails closed. Loss latch survives a restart and a later recovery.
    Existing adaptive/weekly/monthly/consecutive-loss gates still run below.
    """
    raw = client.account()
    equity = float(raw["totalMarginBalance"])
    wallet = float(raw["totalWalletBalance"])
    free = float(raw["availableBalance"])
    margin = float(raw["totalInitialMargin"])
    if not all(math.isfinite(v) for v in (equity, wallet, free, margin)) or min(equity, wallet) <= 0:
        raise ValueError("BROKER_ACCOUNT_RISK_UNAVAILABLE")
    risk_zone = ZoneInfo(settings.ADAPTIVE_DAILY_RISK_TIMEZONE)
    risk_date = datetime.fromtimestamp(now/1000, timezone.utc).astimezone(risk_zone).date()
    day_start = int(datetime.combine(risk_date, day_time.min, risk_zone).timestamp()*1000)
    # Pagination is mandatory; a full page is never mistaken for complete history.
    history = complete_income(client, day_start, now)
    # Wallet cashflow reconstructs midnight wallet; transfers affect capital,
    # never realized trading PnL. All broker trading losses/fees/funding count.
    trading = _TRADING_INCOME
    realized = sum(float(p["income"]) for p in history if p["incomeType"] in trading)
    from app.risk.daily_loss import DailyLossState
    streak = DailyLossState(risk_date)
    realized_events = sorted((p for p in history if p["incomeType"] == "REALIZED_PNL" and float(p["income"]) != 0),
                             key=lambda p: (int(p["time"]), str(p.get("tranId", ""))))
    for event in realized_events:
        streak.record_trade_result(float(event["income"]) > 0, soft_limit=settings.MAX_CONSECUTIVE_LOSSES_SOFT,
            hard_limit=settings.MAX_CONSECUTIVE_LOSSES_HARD, cooldown_minutes=settings.CONSECUTIVE_LOSS_COOLDOWN_MINUTES,
            now_ms=int(event["time"]))
    unrealized = float(raw["totalUnrealizedProfit"])
    opening = wallet - sum(float(p["income"]) for p in history)
    if not math.isfinite(opening) or opening <= 0 or not math.isfinite(realized + unrealized):
        raise ValueError("DAILY_RISK_BASIS_UNAVAILABLE")
    day = risk_date.isoformat()
    with db.connect() as c:
        c.execute("BEGIN IMMEDIATE")
        c.execute("INSERT OR IGNORE INTO cati_production_daily_risk VALUES(?,?,?,0)", (account["id"], day, opening))
        state = c.execute("SELECT * FROM cati_production_daily_risk WHERE account_id=? AND day=?", (account["id"], day)).fetchone()
        basis = float(state["opening_wallet"])
        loss = max(0., -realized - unrealized)
        limit = basis * min(.025, settings.ADAPTIVE_DAILY_RISK_MAX_DAILY_LOSS_PCT)
        latched = bool(state["loss_latched"]) or loss >= limit
        if latched:
            c.execute("UPDATE cati_production_daily_risk SET loss_latched=1 WHERE account_id=? AND day=?", (account["id"], day))
    if any(not math.isfinite(float(p["positionAmt"])) for p in positions):
        raise ValueError("BROKER_POSITION_EXPOSURE_UNAVAILABLE")
    active = [p for p in positions if abs(float(p["positionAmt"])) > 0]
    hedge = any(str(p.get("positionSide", "BOTH")).upper() != "BOTH" for p in positions)
    entry_orders = [o for o in orders if not o.get("reduceOnly") and not o.get("closePosition")]
    from app.risk.adaptive_daily_budget import AdaptiveDailyRiskBudgetEngine, AdaptiveDailyRiskInputs, AdaptiveDailyRiskPolicy
    # Read the same adaptive policy settings as the existing execution runtime.
    mapping = {"daily_r_budget": "R_BUDGET", "minimum_history_trades": "MIN_HISTORY_TRADES",
        "risk_lookback_trades": "LOOKBACK_TRADES", "risk_lookback_days": "LOOKBACK_DAYS",
        "minimum_budget_usdt": "MIN_BUDGET_USDT", "maximum_budget_usdt": "MAX_BUDGET_USDT",
        "caution_consumption_pct": "CAUTION_PCT", "defensive_consumption_pct": "DEFENSIVE_PCT",
        "performance_factor_min": "PERFORMANCE_FACTOR_MIN", "performance_factor_max": "PERFORMANCE_FACTOR_MAX",
        "volatility_factor_min": "VOLATILITY_FACTOR_MIN", "drawdown_factor_min": "DRAWDOWN_FACTOR_MIN"}
    policy_values = {field: getattr(settings, "ADAPTIVE_DAILY_RISK_"+suffix) for field, suffix in mapping.items()}
    adaptive = AdaptiveDailyRiskBudgetEngine(AdaptiveDailyRiskPolicy(**policy_values,
        max_daily_loss_pct=min(.025, settings.ADAPTIVE_DAILY_RISK_MAX_DAILY_LOSS_PCT),
        timezone_name=settings.ADAPTIVE_DAILY_RISK_TIMEZONE), db=db).evaluate(AdaptiveDailyRiskInputs(
            bot_instance_id=account["id"], risk_date=risk_date, day_open_equity=basis,
            current_equity=equity, realized_pnl_today=realized))
    # The frozen portfolio admits only one position. Any existing account
    # exposure, even on another bot/symbol, prevents a new residual entry.
    reason = "DAILY_HARD_LOSS_CAP_REACHED" if latched else "ACCOUNT_WIDE_POSITION_OR_ORDER_ACTIVE" if active or entry_orders else None
    if not active and orders and reason is None:
        reason = "ACCOUNT_WIDE_ORPHAN_ORDER_ACTIVE"
    if hedge and reason is None:
        reason = "NATIVE_PROTECTION_REQUIRES_ONE_WAY_ACCOUNT"
    return {"equity": equity, "wallet": wallet, "free_capital": free, "margin_used": margin,
            "opening_equity": basis, "realized_pnl": realized, "unrealized_pnl": unrealized,
            "daily_loss_usage": loss, "remaining_daily_risk": max(0., limit-loss),
            "daily_hard_loss_fraction": .025, "loss_latched": latched,
            "open_positions": len(active), "entry_orders": len(entry_orders),
            "bot_instance_ids": [b["id"] for b in bots], "reason": reason,
            "adaptive_daily_risk": adaptive.as_policy_context(), "risk_date": day,
            "consecutive_losses": streak.consecutive_losses,
            "consec_loss_day_paused": streak.consec_loss_day_paused,
            "consec_loss_cooldown_until_ms": streak.consec_loss_cooldown_until_ms,
            "periods": account_periods(db, account, client, wallet, equity, risk_date, risk_zone, now)}


def persisted_risk_controls(db, bot_id, risk, now):
    from shared_lib.persistence.state_store import StateStore
    from app.risk.state import get_week_start, get_month_start
    date = datetime.fromisoformat(risk["risk_date"]).date()
    store = StateStore(db, bot_instance_id=bot_id)
    daily = store.load_daily(date)
    if daily is None:
        raise ValueError("PERSISTED_RISK_STATE_REQUIRED")
    weekly_limit = float(settings.MAX_WEEKLY_DRAWDOWN_PCT)
    monthly_limit = float(settings.MAX_MONTHLY_DRAWDOWN_PCT)
    controls = {"kill_switch": bool(daily["kill"]), "daily_trade_count": daily["trade_count"],
        "daily_realized_pnl": risk["realized_pnl"], "adaptive_daily_risk": risk["adaptive_daily_risk"],
        "weekly_drawdown_pct": 0., "monthly_drawdown_pct": 0.,
        "max_weekly_drawdown_pct": weekly_limit, "max_monthly_drawdown_pct": monthly_limit,
        "consecutive_losses": max(daily["consecutive_losses"], risk["consecutive_losses"]),
        "consec_loss_cooldown_until_ms": max(daily["consec_loss_cooldown_until_ms"], risk["consec_loss_cooldown_until_ms"]),
        "consec_loss_day_paused": risk["consec_loss_day_paused"] or daily["consecutive_losses"] >= settings.MAX_CONSECUTIVE_LOSSES_HARD,
        "min_stop_atr_multiplier": settings.MIN_STOP_ATR_MULTIPLIER}
    for period, start, loader, limit in (("weekly", get_week_start(date), store.load_weekly_snapshot, weekly_limit),
                                       ("monthly", get_month_start(date), store.load_monthly_snapshot, monthly_limit)):
        snapshot = loader(start)
        broker_period = risk.get("periods", {}).get(period)
        if broker_period:
            controls[period+"_drawdown_pct"] = broker_period["drawdown_pct"]
        if limit > 0 and not broker_period and (snapshot is None or snapshot.peak_equity <= 0):
            raise ValueError("PERSISTED_"+period.upper()+"_RISK_BASIS_REQUIRED")
        if snapshot:
            controls[period+"_drawdown_pct"] = max(controls[period+"_drawdown_pct"], (snapshot.peak_equity-risk["equity"])/snapshot.peak_equity*100)
    return controls


def build_plan(row, account, bot_id, instrument, reservation_id):
    """Transport the frozen decision unchanged. Forecast fields are explicitly
    inapplicable: no prediction, confidence model, or alpha is manufactured.
    """
    from app.trading_intelligence.contracts.instrument import InstrumentKey
    from app.trading_intelligence.contracts.trade_plan import (
        TradePlan, AllowedEntryZone, TargetZone, ExpectedCosts, ExecutionPreferences)
    did, px = row["decision_id"], float(row["entry_price"])
    costs = json.loads(row["modeled_costs_json"])["parts_estimated_at_decision"]
    deadline = row["decision_time"] + Q
    lineage = {k: did for k in ("snapshot_id", "market_state_id", "regime_distribution_id", "source_candidate_id",
        "venue_observation_id", "cost_estimate_id", "economic_opportunity_id", "veto_decision_id",
        "ranking_batch_id", "ranked_opportunity_id", "portfolio_decision_id")}
    key = instrument.to_instrument_key()
    rates = json.loads(row["modeled_costs_json"])["rates"]
    bps = rates["slippage"] * 10000
    return TradePlan.build(**lineage, forecast_id="NOT_APPLICABLE_FROZEN_RESIDUAL",
        portfolio_reservation_id=reservation_id, user_id=account["user_id"], broker_account_id=account["id"],
        bot_instance_id=bot_id, run_id=None, cycle_id=did, instrument_key=key, venue=key.venue, environment=normalize_environment(account["environment"]).value.upper(),
        side=row["side"], setup_family=FAMILY, setup_version=REGISTRY_HASH,
        decision_time=row["decision_time"], entry_reference=px,
        allowed_entry_zone=AllowedEntryZone(px, px*(1-bps/10000), px*(1+bps/10000), bps, 0., deadline),
        structural_invalidation_price=float(row["stop"]), initial_risk_distance=float(row["risk"]),
        target_zones=(TargetZone(did, float(row["target"]), float(row["target"]), 2.5, "PRIMARY"),),
        expected_holding_time_ms=48*3600000, plan_expiry_time=deadline,
        expected_gross_R=0., expected_net_R=0., conservative_edge_R=0., p_net_profitable=0.,
        credible_interval_low=0., credible_interval_high=0., raw_support=0, ess=0., backoff_level=0,
        expected_costs=ExpectedCosts(did, did, costs["fee"], costs["half_spread"], costs["slippage"],
            costs["funding_buffer"], 0., sum(costs.values()), 0., "FROZEN_RESIDUAL", None, SOURCE),
        economic_size_assumption=None,
        execution_preferences=ExecutionPreferences("MARKET", (), bps, "NORMAL", "ALLOW_PARTIAL_FILL", "GTC",
            rates["half_spread"]*20000, 30_000), thesis_conditions=(), invalidation_conditions=(),
        reason_codes=("FROZEN_RESIDUAL_TOP1", "FORECAST_NOT_APPLICABLE"), versions=(("residual_registry", REGISTRY_HASH),),
        mode="LIVE", plan_created_at=row["recorded_at"])


def boundary_for(db, account, bot, client):
    """Build execution components directly, without constructing PaperRunner."""
    from app.core.bot_instance_service import BotInstanceService
    from app.core.broker_capability_gate import assert_broker_execution_capability, load_permission_evidence
    from app.runner.effective_policy import resolve_effective_bot_policy
    from app.risk.system_limits import UserConfigurableLimits, RiskLevel
    from app.core.trading_orchestrator import TradingOrchestrator
    from app.execution.executor import BinanceExecutor
    from app.exchange.instruments import InstrumentCatalog
    from app.exchange.catalog_refresh import VENUE_KEY
    from app.trading_intelligence.execution.preflight import SubmissionPreflight
    from app.trading_intelligence.execution.binance_adapter import executor_adapter_for
    from app.trading_intelligence.execution.boundary import CATIExecutionBoundary
    environment = normalize_environment(account["environment"]).value.upper()
    if normalize_environment(client.broker_environment).value.upper() != environment:
        raise ValueError("BROKER_ENVIRONMENT_MISMATCH")
    service = BotInstanceService(db=db)
    instance = service.get_bot_instance(bot["id"])
    if instance is None or instance.user_id != account["user_id"] or instance.broker_account_id != account["id"]:
        raise ValueError("BROKER_ACCOUNT_OWNERSHIP_MISMATCH")
    if instance.strategy_id not in ("cati", FAMILY):
        raise ValueError("PRODUCTION_REQUIRES_CATI_BOT")
    assert_broker_execution_capability(db, user_id=account["user_id"], broker_account_id=account["id"])
    policy = resolve_effective_bot_policy(instance=instance, broker_environment=environment.lower(),
        risk_params=service.get_risk_profile_preset(instance.risk_level))
    if policy.execution_mode != "broker":
        raise ValueError("PRODUCTION_REJECTS_PAPER_BOT")
    cache_key = (db.path, account["id"], bot["id"])
    cached = _boundaries.get(cache_key)
    if cached and cached[0] == policy.policy_hash:
        boundary = cached[1]
        boundary.adapter.executor.client = client
        client._production_db, client._production_account_id = db, account["id"]
        with db.connect() as c:
            boundary.preflight.permissions = load_permission_evidence(c, account["id"])
        return boundary
    symbols = list(policy.symbols)
    if policy.universe_mode == "BROKER":
        symbols = list(frozen_definition()[0]["universe"])
    if account["broker_id"].lower() == "bingx":
        symbols = [client._normalize_symbol(s) for s in symbols]
    levels = {"conservative": RiskLevel.LOW, "aggressive": RiskLevel.HIGH}
    limits = UserConfigurableLimits(risk_level=levels.get(policy.risk_level, RiskLevel.MEDIUM),
        max_daily_loss_pct=min(.025, policy.max_daily_loss/policy.capital_budget), max_open_positions=1,
        max_trades_per_day=policy.max_daily_trades, allowed_symbols=symbols,
        requested_leverage={s: int(policy.max_leverage) for s in symbols}, paper_mode=False,
        use_fixed_size=policy.position_allocation_type == "fixed_amount",
        fixed_size_usdt=policy.position_allocation_value if policy.position_allocation_type == "fixed_amount" else None)
    orchestrator = TradingOrchestrator(config_id=bot["id"], user_config=limits, strategy_id="cati",
                                      broker_id=account["id"], effective_policy=policy)
    # Dependency injection must bind every safety projection to this database.
    orchestrator.db = orchestrator.safety.db = orchestrator.protection.db = db
    executor = BinanceExecutor(client, execution_mode="live", live_symbols=symbols, bot_instance_id=bot["id"], db=db,
                               market_data_interval="15m")
    executor._broker_account_id = account["id"]
    executor._capital_budget = policy.capital_budget
    executor._allocation_type = policy.position_allocation_type
    executor._allocation_value = policy.position_allocation_value
    executor._max_open_positions = 1
    executor._max_notional_per_symbol = float(getattr(settings, "MAX_NOTIONAL_PER_SYMBOL", 0.) or 0.)
    adapter = executor_adapter_for(account["broker_id"], executor)
    client._production_db = db
    client._production_account_id = account["id"]
    preflight = SubmissionPreflight(catalog=InstrumentCatalog(db), broker=account["broker_id"],
        venue_key=VENUE_KEY[account["broker_id"].lower()], catalog_environment=environment, account_environment=environment.lower())
    with db.connect() as c:
        preflight.permissions = load_permission_evidence(c, account["id"])
    boundary = CATIExecutionBoundary(orchestrator=orchestrator, adapter=adapter, db=db,
        preflight=preflight, account_scope=(account["user_id"], account["id"]))
    _boundaries[cache_key] = (policy.policy_hash, boundary)
    return boundary


def process_account(db, account, client, snapshot, *, now_ms=None, boundary_factory=boundary_for):
    from app.trading_intelligence.execution.boundary import AccountState
    from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore
    from app.trading_intelligence.capital.planner import CapitalReadiness
    from app.trading_intelligence.trade_plan.validation import MarketReference
    from app.trading_intelligence.contracts.system_health import BrokerHealthContext
    from app.trading_intelligence.venue.binance import BinanceUsdmEconomicAdapter
    from app.trading_intelligence.venue.adapter import VenueRawSnapshot
    from app.product_safety.execution_safety import evaluate_execution_kyc, evaluate_execution_readiness
    now = int(time.time()*1000) if now_ms is None else now_ms
    initialize(db)
    if not settings.production:
        raise ValueError("CATI_PRODUCTION_PROFILE_REQUIRED")
    if not owner_current(db):
        raise ValueError("CANONICAL_RUNTIME_LEASE_REQUIRED")
    environment = normalize_environment(account["environment"]).value.upper()
    gate = order_submission_gate(environment)
    row = latest_decision(db)
    result = {"latest_cati_decision": row, "eligibility": None, "execution_permission": "BLOCKED_ACCOUNT",
              "reason": None, "broker_account_id": account["id"], "orders_submitted": False,
              "credential_version": getattr(client, "_production_credential_version", None),
              "environment": environment, "order_submission_gate": gate}
    with db.connect() as c:
        bots = [dict(b) for b in c.execute("SELECT * FROM bot_instances WHERE broker_account_id=? AND status='active'", (account["id"],))]
        from shared_lib.broker.auto_trading import authorization
        result["auto_trading"] = authorization(c, account, bots)
    if any(b.get("user_id") != account["user_id"] for b in bots):
        result["reason"] = "BROKER_ACCOUNT_OWNERSHIP_MISMATCH"
    elif account["broker_id"].lower() not in {"binance", "bybit", "bingx"}:
        result["reason"] = "DEMO_CAPABILITY_UNAVAILABLE" if environment == "DEMO" else "EXECUTION_ADAPTER_UNVALIDATED"
        result["missing_capabilities"] = ["COMPLETE_ACCOUNT_INCOME_HISTORY", "DURABLE_PROTECTION_READ_BACK"]
    else:
        # Multiple owners must never each claim the account. Resolve a unique
        # execution bot; risk still includes ALL broker and bot activity.
        if account["broker_id"].lower() == "binance":
            symbols = {p["symbol"] for p in snapshot["positions"] if abs(float(p["positionAmt"])) > 0}
            if row and row["selected_symbol"]:
                symbols.add(row["selected_symbol"])
            with db.connect() as c:
                if c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_production_protection'").fetchone():
                    symbols.update(json.loads(r[0])["symbol"] for r in c.execute(
                        "SELECT document FROM cati_production_protection WHERE account_id=?", (account["id"],)))
            for symbol in sorted(symbols):
                protective = client.get_algo_orders(symbol, raise_on_error=True)
                if not isinstance(protective, list):
                    raise ValueError("NATIVE_PROTECTION_READ_UNAVAILABLE")
                snapshot["orders"].extend(protective)
        result["risk"] = account_risk(db, account, client, snapshot["positions"], snapshot["orders"], bots, now)
        if len(bots) != 1:
            result["reason"] = "ACCOUNT_EXECUTION_OWNER_AMBIGUOUS" if bots else "AUTO_TRADING_DISABLED"
        else:
            boundary = boundary_factory(db, account, bots[0], client)
            result["recovery"] = [asdict(r) for r in boundary.recover_pending(now_ms=now)]
            result["execution_history"] = reconcile_executions(db, boundary, client, now)
            result["kill_switch"] = boundary.authority.gov.kill_switch_on(scope=account["id"])
            controls = persisted_risk_controls(db, bots[0]["id"], result["risk"], now)
            result["risk_controls"] = controls
            control_reason = ("CATI_NEW_ENTRY_KILL_SWITCH" if result["kill_switch"] or controls.get("kill_switch") else
                "CONSECUTIVE_LOSS_DAY_PAUSED" if controls.get("consec_loss_day_paused") else
                "CONSECUTIVE_LOSS_COOLDOWN" if controls.get("consec_loss_cooldown_until_ms", 0) > now else
                next((p.upper()+"_DRAWDOWN_LIMIT_REACHED" for p in ("weekly", "monthly")
                      if controls.get("max_"+p+"_drawdown_pct", 0) > 0 and
                      controls[p+"_drawdown_pct"] >= controls["max_"+p+"_drawdown_pct"]), None))
            why = eligibility(row, now)
            result["eligibility"] = {"eligible": why is None, "reason": why}
            if not result["auto_trading"]["enabled"]:
                result.update(reason=result["auto_trading"]["reason"], execution_permission="BLOCKED_ACCOUNT")
            elif control_reason:
                result.update(reason=control_reason, execution_permission="BLOCKED_RISK")
            elif result["risk"]["reason"]:
                result.update(reason=result["risk"]["reason"], execution_permission="BLOCKED_RISK")
            elif any(r["status"] == "STILL_UNKNOWN" for r in result["recovery"]):
                result.update(reason="ACCOUNT_SUBMIT_OUTCOME_UNRESOLVED", execution_permission="BLOCKED_RISK")
            elif why:
                result["reason"] = why
                result["execution_permission"] = ("WAITING_SIGNAL" if why in {
                    "AWAITING_NATURAL_CATI_DECISION", "PROSPECTIVE_ENTRY_WINDOW_EXPIRED",
                    "NEXT_NATIVE_OPEN_REFERENCE_REQUIRED", "RESIDUAL_PORTFOLIO_OVERLAP",
                    "NO_ELIGIBLE_TOP1", "MISSED_PROSPECTIVE_BOUNDARY"} and not result["risk"]["reason"]
                    and not result["kill_switch"] else "BLOCKED_RISK")
            else:
                venue_symbol = client._normalize_symbol(row["selected_symbol"]) if account["broker_id"].lower() == "bingx" else row["selected_symbol"]
                catalog = boundary.preflight.catalog.record(boundary.preflight.venue_key, environment, venue_symbol)
                if not catalog:
                    result["reason"] = "INSTRUMENT_UNKNOWN"
                else:
                    ins = catalog["instrument"]
                    reservation = boundary.reservations.reserve(broker_account_id=account["id"], bot_instance_id=bots[0]["id"],
                        cycle_id=row["decision_id"], selected=[(row["decision_id"], ins.canonical_symbol, boundary.adapter.venue,
                        ins.venue_symbol, row["side"])], now_ms=now, ttl_seconds=max(1, (row["decision_time"]+Q-now)//1000),
                        max_open_positions=1)
                    if not reservation.reserved:
                        result.update(reason=reservation.conflict_reason, execution_permission="BLOCKED_RISK")
                    else:
                        plan = build_plan(row, account, bots[0]["id"], ins, reservation.reservation.reservation_id)
                        plans = TradePlanEvidenceStore(db)
                        plans.append(plan)
                        if account["broker_id"].lower() == "binance":
                            info = client.exchange_info_cached()
                            symbol_info = next((s for s in info["symbols"] if s["symbol"] == ins.venue_symbol), None)
                            raw = VenueRawSnapshot(venue_symbol=ins.venue_symbol, captured_at=now,
                                payloads={"exchange_info_symbol": symbol_info, "exchange_info_as_of": now})
                            caps = BinanceUsdmEconomicAdapter().describe_execution_capabilities(None, raw)
                        else:
                            from app.trading_intelligence.venue.perpetual import BybitEconomicAdapter, BingXEconomicAdapter
                            economic = BybitEconomicAdapter() if account["broker_id"].lower() == "bybit" else BingXEconomicAdapter()
                            caps = economic.describe_execution_capabilities(None, VenueRawSnapshot(
                                venue_symbol=ins.venue_symbol, captured_at=now, payloads={"instrument": ins, "metadata_as_of": now}))
                        price = float(client.last_price(ins.venue_symbol))
                        price_observed_at = now if now_ms is not None else int(time.time()*1000)
                        klines = client.klines(symbol=ins.venue_symbol, interval="15m", limit=250)
                        kyc = evaluate_execution_kyc(user_id=account["user_id"], broker_environment=environment.lower())
                        readiness = evaluate_execution_readiness(db=db, bot_instance_id=bots[0]["id"], broker_environment=environment.lower())
                        risk = result["risk"]
                        controls = persisted_risk_controls(db, bots[0]["id"], risk, now)
                        result["risk_controls"] = controls
                        if not owner_current(db):
                            raise ValueError("CANONICAL_RUNTIME_LEASE_REQUIRED")
                        submission_time = now if now_ms is not None else int(time.time()*1000)
                        result["stage"] = "DECISION_ACCEPTED"
                        with db.connect() as c:
                            c.execute("INSERT OR REPLACE INTO cati_production_decisions VALUES(?,?,?,?,?)",
                                (account["id"], row["decision_id"], bots[0]["id"], submission_time, json.dumps(result, default=str)))
                        out = boundary.process_trade_plan(plan, market_reference=MarketReference(price, price_observed_at),
                            broker_health=BrokerHealthContext(account["id"], plan.venue, environment, "HEALTHY", now, "BROKER_SYNC"),
                            venue_capabilities=caps, account=AccountState(risk["equity"], risk["margin_used"], risk["free_capital"], risk["open_positions"]),
                            klines=klines, atr=json.loads(row["snapshot_json"])["candidate"]["atr14"], now_ms=submission_time,
                            capital=CapitalReadiness(True, "LOGICAL"), user_kyc_approved=kyc.allowed,
                            live_readiness_approved=readiness.allowed, kyc_status=kyc.state, live_readiness_status=readiness.state,
                            execution_mode="broker", market_type="CRYPTO", **controls)
                        result["boundary"] = asdict(out)
                        result["reason"] = out.reason_codes[0] if out.reason_codes else out.status
                        result["execution_permission"] = ("BLOCKED_"+environment+"_ORDER_GATE" if out.status == gate["reason"]
                            else "ORDER_ACTIVE" if out.status in ("EXECUTED", "SUBMIT_UNKNOWN_PENDING_RECONCILIATION")
                            else "BLOCKED_RISK")
                        result["orders_submitted"] = bool(out.attempt and out.attempt.broker_order_id)
    if not gate["enabled"]:
        result["execution_permission"] = "BLOCKED_"+environment+"_ORDER_GATE"
        result["block_reason_before_order_gate"] = result["reason"]
        result["reason"] = gate["reason"]
    if row:
        with db.connect() as c:
            c.execute("INSERT OR REPLACE INTO cati_production_decisions VALUES(?,?,?,?,?)", (account["id"], row["decision_id"],
                bots[0]["id"] if len(bots) == 1 else None, now, json.dumps(result, default=str)))
    return result


def reconcile_executions(db, boundary, client, now):
    """Order/fill/protection truth for persisted plans, independent of entry permission."""
    from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore
    from app.trading_intelligence.evidence.stores import ExecutionAttemptStore
    initialize(db)
    plans = TradePlanEvidenceStore(db)
    attempts = ExecutionAttemptStore(db)
    account = boundary.account_scope[1]
    latest = {}
    for attempt in attempts.for_account(account):
        if attempt["user_id"] == boundary.account_scope[0]:
            latest[attempt["execution_attempt_id"]] = attempt
    history = []
    for attempt in latest.values():
        plan = plans.load_plan(account, attempt["trade_plan_id"])
        if plan is None or (plan.user_id, plan.broker_account_id) != boundary.account_scope:
            continue
        payload = attempt["payload"]
        if not payload.get("client_order_id") and not payload.get("broker_order_id"):
            continue
        order = boundary.adapter.query_order(plan.instrument_key.venue_symbol,
            broker_order_id=payload.get("broker_order_id"), client_order_id=payload.get("client_order_id"))
        position = boundary.adapter.reconcile_position(plan.instrument_key.venue_symbol)
        fills = client.user_trades(symbol=plan.instrument_key.venue_symbol,
            start_time_ms=max(plan.decision_time, now-6*86_400_000), end_time_ms=now, limit=1000)
        if not isinstance(fills, list):
            raise ValueError("BROKER_FILL_HISTORY_UNAVAILABLE")
        # Only actual broker fills for this order belong to this intent.
        oid = order.broker_order_id or payload.get("broker_order_id")
        fills = [f for f in fills if str(f.get("orderId")) == str(oid)]
        with db.connect() as c:
            for fill in fills:
                if fill.get("id") is None:
                    raise ValueError("BROKER_FILL_ID_UNAVAILABLE")
                c.execute("INSERT OR IGNORE INTO cati_production_fills VALUES(?,?,?,?,?)",
                    (account, str(fill["id"]), str(oid), plan.instrument_key.venue_symbol, json.dumps(fill)))
            fills = [json.loads(r[0]) for r in c.execute(
                "SELECT document FROM cati_production_fills WHERE account_id=? AND symbol=? AND order_id=?",
                (account, plan.instrument_key.venue_symbol, str(oid)))]
        item = {"trade_plan_id": plan.trade_plan_id, "cati_decision_id": plan.source_candidate_id,
                "order": asdict(order), "position": asdict(position), "fills": fills,
                "protection": "NOT_REQUIRED_FLAT" if position.answered and position.quantity == 0 else "UNCONFIRMED"}
        if order.answered and order.executed_qty > 0 and position.answered and position.side == plan.side and position.quantity > 0 \
                and position.quantity <= order.executed_qty and position.entry_price and order.avg_price \
                and abs(position.entry_price-order.avg_price) <= 1e-8*order.avg_price:
            if order_submission_gate(plan.environment)["enabled"]:
                client._production_intent_identity = f"{plan.trade_plan_id}|{plan.trade_plan_hash}"
                try:
                    item["protection"] = boundary.adapter.submit_protection(plan.instrument_key.venue_symbol,
                        side=plan.side, quantity=min(position.quantity, order.executed_qty),
                        stop_price=plan.structural_invalidation_price, target_price=plan.target_zones[0].price_high)
                except Exception as exc:
                    item["protection"] = {"status": "UNCONFIRMED", "reason": type(exc).__name__}
            else:
                item["protection"] = order_submission_gate(plan.environment)["reason"]
        history.append(item)
    return history
