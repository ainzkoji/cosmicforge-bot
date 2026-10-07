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
import logging
import math
import time

from app.core.config import settings
from shared_lib.broker.environment import normalize_environment
from shared_lib.core.production import order_submission_gate
from .residual_prospective import FAMILY, REGISTRY_HASH, SOURCE, Q, owner_current, frozen_definition

logger = logging.getLogger(__name__)

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
        from .production_evidence import initialize as initialize_evaluations
        initialize_evaluations(c)


def latest_decision(db):
    with db.connect() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_residual_decisions'").fetchone():
            return None
        row = c.execute("SELECT * FROM cati_residual_decisions WHERE registry_hash=? ORDER BY decision_time DESC LIMIT 1",
                        (REGISTRY_HASH,)).fetchone()
    return dict(row) if row else None


def eligibility(row, now):
    """Execution signal provenance; research portfolio occupancy has no authority.

    The caller supplies only latest_decision(), then revalidates it before CREATE.
    Account occupancy is evaluated separately from broker/durable execution truth.
    """
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
    if not row["decision_time"] < row["recorded_at"] <= now < row["decision_time"] + Q:
        return "PROSPECTIVE_ENTRY_WINDOW_EXPIRED"
    if row.get("entry_price") is None:
        return "NEXT_NATIVE_OPEN_REFERENCE_REQUIRED"
    outcome = json.loads(row.get("outcome_json") or "{}")
    if (row.get("entry_time") != row["decision_time"] + 1
            or outcome.get("entry_reference_kind") != "NEXT_NATIVE15M_OPEN_REFERENCE_NOT_A_FILL"
            or not row["recorded_at"] <= outcome.get("entry_received_at", -1) <= now):
        return "NEXT_NATIVE_OPEN_PROVENANCE_REQUIRED"
    if row.get("lifecycle") == "NON_EXECUTABLE_GAP":
        return "NON_EXECUTABLE_GAP"
    if row.get("lifecycle") != "OPEN":
        return "PROSPECTIVE_ENTRY_NOT_OPEN"
    entry = float(row["entry_price"])
    if not math.isfinite(entry) or not (min(float(row["stop"]), float(row["target"])) < entry <
                                      max(float(row["stop"]), float(row["target"]))):
        return "NON_EXECUTABLE_GAP"
    candidate = snapshot.get("candidate") or {}
    if json.loads(row["eligible_universe_json"]) != snapshot.get("eligible_universe"):
        return "RESIDUAL_DECISION_CONTENT_MISMATCH"
    for key, field in (("symbol", "selected_symbol"), ("side", "side"), ("score", "score"),
                       ("entry_reference", "entry_reference"), ("stop", "stop"), ("target", "target"), ("risk", "risk")):
        if candidate.get(key) != row[field]:
            return "RESIDUAL_DECISION_CONTENT_MISMATCH"
    if (not math.isfinite(float(row["score"])) or abs(float(row["score"])) < 2
            or len(set(snapshot.get("eligible_universe", []))) < 30
            or row["selected_symbol"] not in snapshot.get("eligible_universe", [])):
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


def certification_pnl(db, account_id, start, now):
    """Fill ids and realized PnL net of fees of labelled DEMO_CERTIFICATION
    fills in [start, now]. Operational certification is account truth, never
    CATI strategy performance."""
    with db.connect() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_production_fills'").fetchone():
            return set(), 0.
        docs = [json.loads(r[0]) for r in c.execute("SELECT document FROM cati_production_fills WHERE account_id=?", (account_id,))]
    docs = [d for d in docs if (d.get("_execution") or {}).get("purpose") == "DEMO_CERTIFICATION"]
    pnl = sum(float(d.get("realizedPnl") or 0) - float(d.get("commission") or 0)
              for d in docs if start <= int(d.get("time", 0)) <= now)
    return {str(d.get("id")) for d in docs}, pnl


def account_daily_loss_policy(db, account, bots):
    """The daily loss policy enforced for a broker account: each active bot's
    resolved EffectiveBotPolicy, the most restrictive one when several bots
    share the account (losses are aggregated account-wide). None when no
    active bot resolves a policy -- the caller fails closed."""
    from app.core.bot_instance_service import BotInstanceService
    from app.runner.effective_policy import resolve_effective_bot_policy
    service = BotInstanceService(db=db)
    resolved = []
    for bot in bots:
        instance = service.get_bot_instance(bot["id"])
        if instance is None or instance.broker_account_id != account["id"]:
            continue
        policy = resolve_effective_bot_policy(instance=instance, broker_environment=str(account.get("environment") or ""),
            risk_params=service.get_risk_profile_preset(instance.risk_level))
        resolved.append({"pct": policy.max_daily_loss_pct, "source": policy.daily_loss_source,
                         "bot_instance_id": instance.id})
    return min(resolved, key=lambda p: p["pct"]) if resolved else None


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
    # Strategy accounting excludes labelled DEMO_CERTIFICATION fills; the
    # wallet, equity, opening basis and drawdowns remain broker account truth.
    cert_ids, cert_pnl = certification_pnl(db, account["id"], day_start, now)
    strategy = [p for p in history if str(p.get("tradeId") or "") not in cert_ids]
    # Wallet cashflow reconstructs midnight wallet; transfers affect capital,
    # never realized trading PnL. All broker trading losses/fees/funding count.
    trading = _TRADING_INCOME
    realized = sum(float(p["income"]) for p in strategy if p["incomeType"] in trading)
    fees = -sum(float(p['income']) for p in strategy if p['incomeType'] == 'COMMISSION')
    funding = sum(float(p['income']) for p in strategy if p['incomeType'] == 'FUNDING_FEE')
    flows = sum(float(p["income"]) for p in history if p["incomeType"] not in trading)
    from app.risk.daily_loss import DailyLossState
    streak = DailyLossState(risk_date)
    realized_events = sorted((p for p in strategy if p["incomeType"] == "REALIZED_PNL" and float(p["income"]) != 0),
                             key=lambda p: (int(p["time"]), str(p.get("tranId", ""))))
    for event in realized_events:
        streak.record_trade_result(float(event["income"]) > 0, soft_limit=settings.MAX_CONSECUTIVE_LOSSES_SOFT,
            hard_limit=settings.MAX_CONSECUTIVE_LOSSES_HARD, cooldown_minutes=settings.CONSECUTIVE_LOSS_COOLDOWN_MINUTES,
            now_ms=int(event["time"]), emit_events=False)
    unrealized = float(raw["totalUnrealizedProfit"])
    opening = wallet - sum(float(p["income"]) for p in history)
    if not math.isfinite(opening) or opening <= 0 or not math.isfinite(realized + unrealized):
        raise ValueError("DAILY_RISK_BASIS_UNAVAILABLE")
    policy = account_daily_loss_policy(db, account, bots)
    day = risk_date.isoformat()
    with db.connect() as c:
        c.execute("BEGIN IMMEDIATE")
        c.execute("INSERT OR IGNORE INTO cati_production_daily_risk VALUES(?,?,?,0)", (account["id"], day, opening))
        state = c.execute("SELECT * FROM cati_production_daily_risk WHERE account_id=? AND day=?", (account["id"], day)).fetchone()
        basis = float(state["opening_wallet"])
        account_realized = wallet - basis - flows
        # The broker income ledger can lag the wallet. Strategy PnL never reads
        # better than the wallet's own change since the day's opening basis,
        # net of capital flows and of labelled certification fills.
        realized = min(realized, account_realized - cert_pnl)
        loss = max(0., -realized - unrealized)
        limit = basis * policy["pct"] if policy else None
        latched = bool(state["loss_latched"]) or (limit is not None and loss >= limit)
        if latched:
            c.execute("UPDATE cati_production_daily_risk SET loss_latched=1 WHERE account_id=? AND day=?", (account["id"], day))
    if any(not math.isfinite(float(p["positionAmt"])) for p in positions):
        raise ValueError("BROKER_POSITION_EXPOSURE_UNAVAILABLE")
    active = [p for p in positions if abs(float(p["positionAmt"])) > 0]
    hedge = any(str(p.get("positionSide", "BOTH")).upper() != "BOTH" for p in positions)
    entry_orders = [o for o in orders if not o.get("reduceOnly") and not o.get("closePosition")]
    from app.risk.adaptive_daily_budget import AdaptiveDailyRiskBudgetEngine, AdaptiveDailyRiskInputs, AdaptiveDailyRiskPolicy
    # Adaptive tightening knobs only; the ceiling is the account's resolved
    # daily loss policy, never a global USDT or percentage constant.
    mapping = {"daily_r_budget": "R_BUDGET", "minimum_history_trades": "MIN_HISTORY_TRADES",
        "risk_lookback_trades": "LOOKBACK_TRADES", "risk_lookback_days": "LOOKBACK_DAYS",
        "caution_consumption_pct": "CAUTION_PCT", "defensive_consumption_pct": "DEFENSIVE_PCT",
        "performance_factor_min": "PERFORMANCE_FACTOR_MIN", "performance_factor_max": "PERFORMANCE_FACTOR_MAX",
        "volatility_factor_min": "VOLATILITY_FACTOR_MIN", "drawdown_factor_min": "DRAWDOWN_FACTOR_MIN"}
    policy_values = {field: getattr(settings, "ADAPTIVE_DAILY_RISK_"+suffix) for field, suffix in mapping.items()}
    adaptive = AdaptiveDailyRiskBudgetEngine(AdaptiveDailyRiskPolicy(**policy_values,
        max_daily_loss_pct=policy["pct"], minimum_budget_usdt=None, maximum_budget_usdt=None,
        timezone_name=settings.ADAPTIVE_DAILY_RISK_TIMEZONE), db=db).evaluate(AdaptiveDailyRiskInputs(
            bot_instance_id=account["id"], risk_date=risk_date, day_open_equity=basis,
            current_equity=equity, realized_pnl_today=realized, fees_today=fees, funding_today=funding)) if policy else None
    # The frozen portfolio admits only one position. Any existing account
    # exposure, even on another bot/symbol, prevents a new residual entry.
    reason = ("ACCOUNT_DAILY_LOSS_POLICY_UNAVAILABLE" if policy is None else "USER_DAILY_LOSS_LIMIT_REACHED" if latched
              else "ACCOUNT_WIDE_POSITION_OR_ORDER_ACTIVE" if active or entry_orders else None)
    if not active and orders and reason is None:
        reason = "ACCOUNT_WIDE_ORPHAN_ORDER_ACTIVE"
    if hedge and reason is None:
        reason = "NATIVE_PROTECTION_REQUIRES_ONE_WAY_ACCOUNT"
    return {"equity": equity, "wallet": wallet, "free_capital": free, "margin_used": margin,
            "opening_equity": basis, "realized_pnl": realized, "unrealized_pnl": unrealized,
            "fees": fees, "funding": funding,
            "account_realized_pnl": account_realized, "certification_pnl": cert_pnl,
            "daily_loss_usage": loss, "remaining_daily_risk": max(0., limit-loss) if limit is not None else 0.,
            "daily_loss_limit_fraction": policy["pct"] if policy else None, "daily_loss_policy": policy,
            "loss_latched": latched,
            "open_positions": len(active), "entry_orders": len(entry_orders),
            "bot_instance_ids": [b["id"] for b in bots], "reason": reason,
            "adaptive_daily_risk": adaptive.as_policy_context() if adaptive else None, "risk_date": day,
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
    from app.trading_intelligence.governance.account_authority import AccountExecutionAuthority
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
        max_daily_loss_pct=policy.max_daily_loss_pct, max_open_positions=1,
        max_trades_per_day=policy.max_daily_trades, allowed_symbols=symbols,
        requested_leverage={s: int(policy.max_leverage) for s in symbols}, paper_mode=False,
        use_fixed_size=policy.position_allocation_type == "fixed_amount",
        fixed_size_usdt=policy.position_allocation_value if policy.position_allocation_type == "fixed_amount" else None)
    orchestrator = TradingOrchestrator(config_id=bot["id"], user_config=limits, strategy_id="cati",
                                      broker_id=account["id"], effective_policy=policy)
    # Dependency injection must bind every safety projection to this database.
    orchestrator.db = orchestrator.safety.db = orchestrator.protection.db = db
    # The safety engine created its monitoring tables on the database it was
    # constructed with; the one it is bound to must have them as well.
    orchestrator.safety._init_monitoring_state()
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
        preflight=preflight, account_scope=(account["user_id"], account["id"]),
        authority=AccountExecutionAuthority(db, (account['user_id'], account['id'])))
    from app.exchange.instruments import sync_instruments
    preflight.refresh = lambda: sync_instruments(boundary.adapter.executor.client,catalog=preflight.catalog,
        venue=preflight.venue_key,environment=preflight.catalog_environment,now_ms=int(time.time()*1000))
    _boundaries[cache_key] = (policy.policy_hash, boundary)
    return boundary


def process_account(db, account, client, snapshot, *, now_ms=None, boundary_factory=boundary_for):
    """Preserve every evaluation, including exceptions before an attempt exists."""
    initialize(db)
    result = {}
    failure = None
    try:
        _process_account(db, account, client, snapshot, now_ms=now_ms,
                         boundary_factory=boundary_factory, evaluation=result)
    except Exception as exc:
        failure = exc
        code = getattr(exc, 'reason_code', None)
        if not code and isinstance(exc, ValueError) and str(exc).replace('_','').isalnum() and str(exc).upper() == str(exc):
            code = str(exc)
        result.update(reason=code or type(exc).__name__, execution_permission='BLOCKED_ACCOUNT', stage='EVALUATION_FAILED')
    finally:
        now = int(time.time()*1000) if now_ms is None else now_ms
        if result.get('reservation_id') and result.get('bot_instance_id'):
            from app.trading_intelligence.portfolio.reservation_store import CATIReservationStore
            CATIReservationStore(db).release_unsubmitted(result['reservation_id'],now,
                account_scope=(account['user_id'],account['id']),bot_instance_id=result['bot_instance_id'])
    from .production_evidence import record
    record(db, account, result, now)
    if failure is not None:
        failure.production_evaluation = result
        raise failure
    return result


def _native_protection_snapshot(db, account, client, snapshot, row):
    """Add the account's native conditional (protection) orders to the snapshot."""
    if account["broker_id"].lower() == "binance":
        symbols = {p["symbol"] for p in snapshot["positions"] if abs(float(p["positionAmt"])) > 0}
        if row and row["selected_symbol"]:
            symbols.add(row["selected_symbol"])
        with db.connect() as c:
            symbols.update(r[0] for r in c.execute("SELECT e.symbol FROM pending_entries e JOIN bot_instances b ON b.id=e.bot_id WHERE b.broker_account_id=? AND e.state!='OPEN_FAILED'", (account['id'],)))
            if c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_production_protection'").fetchone():
                symbols.update(json.loads(r[0])["symbol"] for r in c.execute(
                    "SELECT document FROM cati_production_protection WHERE account_id=?", (account["id"],)))
        for symbol in sorted(symbols):
            protective = client.get_algo_orders(symbol, raise_on_error=True)
            if not isinstance(protective, list):
                raise ValueError("NATIVE_PROTECTION_READ_UNAVAILABLE")
            snapshot["orders"].extend(protective)


def open_lineage(db, account_id):
    """Execution lineage of this account that still needs maintenance: the bots
    behind unresolved attempts / open positions / unresolved reservations, and
    whether a close is pending. Local reads only."""
    from app.trading_intelligence.contracts.execution import POSITION_EXISTS
    # A position may exist, or it is unknown whether one does.
    live = set(POSITION_EXISTS) | {'PENDING_SUBMIT', 'SUBMIT_UNKNOWN'}
    bots, pending_close = [], False
    with db.connect() as c:
        tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN "
            "('cati_execution_attempts','cati_production_closes','cati_portfolio_reservations')")}
        if 'cati_execution_attempts' in tables:
            latest = {}
            for r in c.execute("SELECT execution_attempt_id,status,bot_instance_id FROM cati_execution_attempts "
                               "WHERE broker_account_id=? ORDER BY recorded_at, sequence", (account_id,)):
                latest[r[0]] = (r[1], r[2])
            bots += [bot for status, bot in latest.values() if status in live]
        if 'cati_portfolio_reservations' in tables:
            bots += [r[0] for r in c.execute("SELECT bot_instance_id FROM cati_portfolio_reservations "
                                             "WHERE broker_account_id=? AND status='RESOLUTION_PENDING'", (account_id,))]
        if 'cati_production_closes' in tables:
            pending_close = bool(c.execute("SELECT 1 FROM cati_production_closes WHERE account_id=? AND status!='CLOSED' "
                                           "LIMIT 1", (account_id,)).fetchone())
    return {'bots': list(dict.fromkeys(b for b in bots if b)), 'pending_close': pending_close}


def maintain_account(db, account, client, bots, now, boundary_factory=boundary_for):
    """Position-safety maintenance of EXISTING lineage only: resolve unknown
    submits, repair protection, run the fail-safe / horizon close. It never
    evaluates or submits an entry and needs no entry precondition (no account
    risk, no auto-trading consent, no active bot). Returns None when the
    account has nothing to maintain; raises -- after doing everything it
    could -- when any part failed, so the caller fails closed."""
    lineage = open_lineage(db, account['id'])
    if not lineage['bots'] and not lineage['pending_close']:
        return None
    # The boundary only carries this account's adapter and scope: any bot of the
    # account builds it. Prefer the single active bot, then the lineage's own
    # bot (it may be paused or stopped -- its position is still ours to manage).
    candidates = [bots[0]] if len(bots) == 1 else []
    candidates += [{'id': b} for b in lineage['bots'] if b not in {c['id'] for c in candidates}]
    if not candidates:
        with db.connect() as c:
            candidates = [{'id': r[0]} for r in c.execute(
                "SELECT id FROM bot_instances WHERE broker_account_id=? ORDER BY status='active' DESC, id", (account['id'],))]
    boundary = bot_id = build_error = None
    for bot in candidates:
        try:
            boundary, bot_id = boundary_factory(db, account, bot, client), bot['id']
            break
        except Exception as exc:
            build_error = build_error or exc
    if boundary is None:
        raise build_error or ValueError('MAINTENANCE_BOUNDARY_UNAVAILABLE')
    done = {'boundary': boundary, 'bot_id': bot_id, 'recovery': [], 'execution_history': []}
    failure = None
    try:
        done['recovery'] = [asdict(r) for r in boundary.recover_pending(now_ms=now)]
    except Exception as exc:
        failure = exc  # positions are still reconciled below
    try:
        done['execution_history'] = reconcile_executions(db, boundary, client, now)
    except Exception as exc:
        done['execution_history'] = getattr(exc, 'execution_history', [])
        failure = failure or exc
    if failure is not None:
        failure.maintenance = done
        raise failure
    return done


def maintain_only(db, account, client, *, now_ms=None, boundary_factory=boundary_for):
    """Maintenance for a cycle that cannot evaluate entries at all (an auxiliary
    step of the account sync failed). Same authority checks as a full cycle."""
    initialize(db)
    if not settings.production:
        raise ValueError("CATI_PRODUCTION_PROFILE_REQUIRED")
    if not owner_current(db):
        raise ValueError("CANONICAL_RUNTIME_LEASE_REQUIRED")
    if account["broker_id"].lower() not in {"binance", "bybit", "bingx"}:
        return None
    with db.connect() as c:
        bots = [dict(b) for b in c.execute("SELECT * FROM bot_instances WHERE broker_account_id=? AND status='active'", (account["id"],))]
    return maintain_account(db, account, client, bots, int(time.time()*1000) if now_ms is None else now_ms, boundary_factory)


#: ``symbol`` of a flatten result row that describes a whole account, not one
#: position (every result field is always a string for API consumers).
ACCOUNT_LEVEL = "*"
#: ``detail`` prefix of every ``submitted`` flatten row: THIS request sent a
#: close order and the venue acknowledged it. The API counts a ``submitted``
#: row as success only with this evidence.
SUBMITTED_DETAIL_PREFIX = "CLOSE_ORDER_ACKNOWLEDGED"


def _reason(exc):
    """A stable reason code for an exception; never its (possibly signed) text."""
    code = getattr(exc, 'reason_code', None)
    if not code and isinstance(exc, ValueError) and str(exc).replace('_','').isalnum() and str(exc).upper() == str(exc):
        code = str(exc)
    return code or type(exc).__name__


def flatten_account(db, account, client, *, request_id, now_ms=None, boundary_factory=boundary_for):
    """Operator emergency flatten of ONE account: every open position is closed
    with the SAME durable reduce-only close the fail-safe uses (same gates, same
    read-back, same bounded retry), then the normal reconciliation cancels the
    protection legs of what is confirmed flat. Never submits an entry. The
    caller holds the runtime cycle lock and has already set the kill switch.

    Returns one result per open symbol:

    * ``closed``       the close order is FILLED and the broker reports flat
    * ``no_position``  the broker reports flat
    * ``submitted``    THIS request sent a close order and the venue acknowledged
                       it; only the fill / flat confirmation is outstanding
    * ``failed``       everything else, with a precise ``detail`` -- including a
                       close that an EARLIER request or cycle sent and that is
                       still unconfirmed: this request did nothing about it.

    Which durable close intent is used for a symbol, in this order:

    1. the lineage of the trade plan that opened the position (so this close IS
       the fail-safe close: same durable row, same client ids);
    2. any close intent of this account and symbol that is still open (an
       earlier emergency request, or a lineage whose plan cannot be read);
    3. a new ``EMERGENCY|<request id>`` intent.

    The next one is tried only when the previous one sent nothing AND its
    latest attempt is PROVEN not to be working at the venue (refused, absent,
    terminal, exhausted or waiting out a retry backoff). While an attempt may
    still be live, nothing further is sent, so closes are never stacked; at
    most one close order per symbol is sent by one request. (Stacking would
    still be exposure-safe -- every close is reduce-only -- but it is bounded.)"""
    from app.execution.production_close import close_position, EMERGENCY_IDENTITY_PREFIX
    from app.trading_intelligence.contracts.execution import NO_POSITION
    from app.trading_intelligence.evidence.stores import ExecutionAttemptStore
    from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore
    now = int(time.time()*1000) if now_ms is None else now_ms
    initialize(db)
    if not settings.production:
        raise ValueError("CATI_PRODUCTION_PROFILE_REQUIRED")
    if not owner_current(db):
        raise ValueError("CANONICAL_RUNTIME_LEASE_REQUIRED")
    environment = normalize_environment(account["environment"]).value.upper()
    if normalize_environment(client.broker_environment).value.upper() != environment:
        raise ValueError("BROKER_ENVIRONMENT_MISMATCH")
    positions = client.position_risk()
    if not isinstance(positions, list):
        raise ValueError("BROKER_READ_SHAPE_INVALID")
    symbols = list(dict.fromkeys(p["symbol"] for p in positions if abs(float(p["positionAmt"])) > 0))
    base = {"account_id": account["id"]}
    if not symbols:
        return [{**base, "symbol": ACCOUNT_LEVEL, "status": "no_position", "detail": "BROKER_REPORTS_FLAT"}]
    if account["broker_id"].lower() != "binance":
        # The durable close speaks this venue's order API only: an open position
        # elsewhere is reported as NOT closed, never silently skipped.
        return [{**base, "symbol": s, "status": "failed", "detail": "EMERGENCY_FLATTEN_UNSUPPORTED_BROKER"} for s in symbols]
    gate = order_submission_gate(environment)
    if not gate["enabled"]:
        # The close path's own gate; reported, never bypassed.
        return [{**base, "symbol": s, "status": "failed", "detail": gate["reason"]} for s in symbols]
    client._production_db, client._production_account_id = db, account["id"]
    # A position opened by a trade plan is closed under that plan's identity, so
    # this close IS the fail-safe close (same durable row, same client id).
    # Discovery is best effort per row: one unreadable attempt or plan must not
    # fail the whole account's flatten -- that symbol falls through to an open
    # close intent or to the emergency identity.
    lineage, plans, latest = {}, TradePlanEvidenceStore(db), {}
    try:
        for attempt in ExecutionAttemptStore(db).for_account(account["id"]):
            latest[attempt["execution_attempt_id"]] = attempt
    except Exception as exc:
        latest = {}
        logger.warning("[EMERGENCY_FLATTEN] account=%s execution lineage unreadable (%s); emergency identity is used",
                       account["id"], type(exc).__name__)
    for attempt in latest.values():
        try:
            if attempt["status"] in NO_POSITION or attempt["status"] == "POSITION_CLOSED":
                continue
            plan = plans.load_plan(account["id"], attempt["trade_plan_id"])
            if plan is not None and (plan.user_id, plan.broker_account_id) == (account["user_id"], account["id"]):
                lineage[plan.instrument_key.venue_symbol] = f"{plan.trade_plan_id}|{plan.trade_plan_hash}"
        except Exception as exc:
            logger.warning("[EMERGENCY_FLATTEN] account=%s lineage of attempt %s unreadable (%s); skipped",
                           account["id"], attempt.get("execution_attempt_id") if isinstance(attempt, dict) else "?",
                           type(exc).__name__)

    def open_intents(symbol):
        """Identities of this account's close intents for ``symbol`` that are not
        CLOSED, oldest first. Local read; unreadable means none."""
        try:
            with db.connect() as c:
                if not c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_production_closes'").fetchone():
                    return []
                return [r[0] for r in c.execute("SELECT identity FROM cati_production_closes WHERE account_id=? "
                                                "AND symbol=? AND status!='CLOSED' ORDER BY rowid", (account["id"], symbol))]
        except Exception as exc:
            logger.warning("[EMERGENCY_FLATTEN] account=%s open close intents unreadable (%s)", account["id"],
                           type(exc).__name__)
            return []

    def close_as(identity, symbol):
        """(status, detail, trace) of one durable close intent."""
        client._production_intent_identity = identity
        trace = {}
        try:
            order = close_position(client, symbol, trace=trace)
        except Exception as exc:
            code = _reason(exc)
            state = trace.get("state")
            detail = code if not state or state == code else f"{code}:{state}"
            # "submitted" is evidence about THIS request only: it sent a close
            # order, the venue acknowledged it, and that order is not known to
            # be dead. A close some earlier call sent is never reported as such.
            if trace.get("acknowledged") and not trace.get("dead"):
                return "submitted", f"{SUBMITTED_DETAIL_PREFIX}:{detail}", trace
            return "failed", detail, trace
        flat = isinstance(order, dict) and order.get("status") == "no_position"
        return ("no_position", "BROKER_REPORTS_FLAT", trace) if flat else ("closed", "CLOSE_FILLED_AND_FLAT_CONFIRMED", trace)

    results = []
    emergency = f"{EMERGENCY_IDENTITY_PREFIX}{request_id}"
    for symbol in symbols:
        candidates = list(dict.fromkeys(i for i in [lineage.get(symbol), *open_intents(symbol), emergency] if i))
        status = detail = None
        for identity in candidates:
            status, detail, trace = close_as(identity, symbol)
            # Move on to the next intent only when this one sent nothing and
            # cannot still be working at the venue (see the docstring).
            if status != "failed" or trace.get("posted") or not trace.get("not_working"):
                break
        results.append({**base, "symbol": symbol, "status": status, "detail": detail})
    # Cancel the now-orphaned protection legs the way the normal close path does.
    try:
        with db.connect() as c:
            bots = [dict(b) for b in c.execute("SELECT * FROM bot_instances WHERE broker_account_id=? AND status='active'", (account["id"],))]
        maintain_account(db, account, client, bots, now, boundary_factory)
    except Exception as exc:
        cleanup = str(exc) if isinstance(exc, ValueError) and str(exc).replace('_','').isalnum() else type(exc).__name__
        for item in results:
            if item["status"] == "closed":
                item["detail"] += f"; PROTECTION_CLEANUP_PENDING:{cleanup}"
    return results


def _process_account(db, account, client, snapshot, *, now_ms=None, boundary_factory=boundary_for, evaluation):
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
    result = evaluation
    result.update({"latest_cati_decision": row, "eligibility": None, "execution_permission": "BLOCKED_ACCOUNT",
              "reason": None, "broker_account_id": account["id"], "orders_submitted": False,
              "credential_version": getattr(client, "_production_credential_version", None),
              "environment": environment, "order_submission_gate": gate})
    research_state = json.loads(row['risk_state_json']) if row else {}
    result['research_observation_overlap'] = bool(research_state.get('overlap_rejected'))
    with db.connect() as c:
        result['research_portfolio_active'] = bool(c.execute("SELECT 1 FROM cati_residual_decisions WHERE registry_hash=? AND portfolio_selected=1 AND lifecycle IN ('PENDING_ENTRY','OPEN') LIMIT 1", (REGISTRY_HASH,)).fetchone()) if row else False
    with db.connect() as c:
        bots = [dict(b) for b in c.execute("SELECT * FROM bot_instances WHERE broker_account_id=? AND status='active'", (account["id"],))]
        from shared_lib.broker.auto_trading import authorization
        result["auto_trading"] = authorization(c, account, bots)
    result['bot_instance_id'] = bots[0]['id'] if len(bots) == 1 else None
    mismatch = any(b.get("user_id") != account["user_id"] for b in bots)
    supported = account["broker_id"].lower() in {"binance", "bybit", "bingx"}
    # ── Position safety first ────────────────────────────────────────────────
    # Recovery and reconciliation of EXISTING attempts/positions (protection
    # repair, fail-safe close, horizon close) used to run only after
    # account_risk() succeeded and only with exactly one active bot: a paused
    # bot, a second bot or one unreadable income page left an open position
    # unmanaged. They now run first, for any account that has such lineage, and
    # need nothing that gates an ENTRY. Entry evaluation below is unchanged.
    snapshot_error = maintenance = None
    if supported:
        if not mismatch:
            try:
                # The same pre-maintenance order snapshot entry evaluation always used.
                _native_protection_snapshot(db, account, client, snapshot, row)
            except Exception as exc:
                snapshot_error = exc  # raised below, after maintenance had its turn
        try:
            maintenance = maintain_account(db, account, client, bots, now, boundary_factory)
        except Exception as exc:
            done = getattr(exc, 'maintenance', None) or {}
            result.update(recovery=done.get('recovery', []), execution_history=done.get('execution_history', []))
            if not mismatch and snapshot_error is None:
                try:
                    # Keep the account-wide loss latch and risk evidence current.
                    result["risk"] = account_risk(db, account, client, snapshot["positions"], snapshot["orders"], bots, now)
                except Exception:
                    pass
            raise  # fail closed: no entry is evaluated after a maintenance failure
        if maintenance:
            result.update(recovery=maintenance['recovery'], execution_history=maintenance['execution_history'])
        if snapshot_error is not None:
            raise snapshot_error
    if mismatch:
        result["reason"] = "BROKER_ACCOUNT_OWNERSHIP_MISMATCH"
    elif not supported:
        result["reason"] = "DEMO_CAPABILITY_UNAVAILABLE" if environment == "DEMO" else "EXECUTION_ADAPTER_UNVALIDATED"
        result["missing_capabilities"] = ["COMPLETE_ACCOUNT_INCOME_HISTORY", "DURABLE_PROTECTION_READ_BACK"]
    else:
        # Multiple owners must never each claim the account. Resolve a unique
        # execution bot; risk still includes ALL broker and bot activity.
        result["risk"] = account_risk(db, account, client, snapshot["positions"], snapshot["orders"], bots, now)
        if len(bots) != 1:
            result["reason"] = "ACCOUNT_EXECUTION_OWNER_AMBIGUOUS" if bots else "AUTO_TRADING_DISABLED"
        else:
            boundary = (maintenance['boundary'] if maintenance and maintenance['bot_id'] == bots[0]['id']
                        else boundary_factory(db, account, bots[0], client))
            if not maintenance:
                # Nothing existed to maintain above; these are then no-ops kept
                # in their original place.
                result["recovery"] = [asdict(r) for r in boundary.recover_pending(now_ms=now)]
                result["execution_history"] = reconcile_executions(db, boundary, client, now)
            from .production_portfolio import execution_portfolio, reconcile_confirmed_intents
            result['intent_reconciliation'] = reconcile_confirmed_intents(db, account, snapshot)
            result['execution_portfolio'] = execution_portfolio(db, account, snapshot, result['execution_history'], now)
            result['execution_portfolio_active'] = result['execution_portfolio']['active']
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
            elif result['execution_portfolio']['active']:
                result.update(reason=result['execution_portfolio']['reason'], execution_permission='BLOCKED_RISK')
            elif why:
                result["reason"] = why
                result["execution_permission"] = ("WAITING_SIGNAL" if why in {
                    "AWAITING_NATURAL_CATI_DECISION", "PROSPECTIVE_ENTRY_WINDOW_EXPIRED",
                    "NEXT_NATIVE_OPEN_REFERENCE_REQUIRED",
                    "NO_ELIGIBLE_TOP1", "NO_SCORE_AT_LEAST_2", "MISSED_PROSPECTIVE_BOUNDARY"} and not result["risk"]["reason"]
                    and not result["kill_switch"] else "BLOCKED_RISK")
            else:
                venue_symbol = client._normalize_symbol(row["selected_symbol"]) if account["broker_id"].lower() == "bingx" else row["selected_symbol"]
                catalog = boundary.preflight.catalog.record(boundary.preflight.venue_key, environment, venue_symbol)
                if not boundary.preflight._fresh(catalog,now) and boundary.preflight.refresh is not None:
                    boundary.preflight.refresh()
                    catalog = boundary.preflight.catalog.record(boundary.preflight.venue_key, environment, venue_symbol)
                if not catalog:
                    result["reason"] = "INSTRUMENT_UNKNOWN"
                else:
                    ins = catalog["instrument"]
                    reservation = boundary.reservations.reserve(broker_account_id=account["id"], bot_instance_id=bots[0]["id"],
                        cycle_id=row["decision_id"], selected=[(row["decision_id"], ins.canonical_symbol, boundary.adapter.venue,
                        ins.venue_symbol, row["side"])], now_ms=now, ttl_seconds=max(1, (row["decision_time"]+Q-now)//1000),
                        max_open_positions=1, mode='PRODUCTION', production_scope=(account['user_id'], account['id']))
                    if not reservation.reserved:
                        result.update(reason=reservation.conflict_reason, execution_permission='WAITING_SIGNAL'
                            if reservation.conflict_reason=='CATI_DECISION_ALREADY_ATTEMPTED' else "BLOCKED_RISK")
                    else:
                        result['reservation_id'] = reservation.reservation.reservation_id
                        result['reservation_status'] = reservation.reservation.status
                        plan = build_plan(row, account, bots[0]["id"], ins, reservation.reservation.reservation_id)
                        result['trade_plan_id'] = plan.trade_plan_id
                        plans = TradePlanEvidenceStore(db)
                        plans.append(plan)
                        prepared = prepare_submission(db, account, bots[0], client, boundary, plan, ins,
                                                      result["risk"], now_ms=now_ms)
                        result["risk_controls"] = prepared.pop("controls_evidence")
                        if not owner_current(db):
                            raise ValueError("CANONICAL_RUNTIME_LEASE_REQUIRED")
                        from app.ops.runtime_shutdown import stop_requested
                        if stop_requested():
                            # A stopping runtime finishes what it has begun; it does
                            # not open a position it will not be there to manage.
                            raise ValueError("RUNTIME_SHUTDOWN_IN_PROGRESS")
                        submission_time = now if now_ms is not None else int(time.time()*1000)
                        current = latest_decision(db)
                        stale = ("CURRENT_CATI_DECISION_CHANGED" if not current or current['decision_id'] != row['decision_id']
                                 else eligibility(current, submission_time))
                        if stale:
                            boundary._release_unsubmitted(plan, submission_time)
                            result.update(reason=stale, execution_permission='WAITING_SIGNAL')
                            result['execution_portfolio'] = execution_portfolio(db, account, snapshot, result['execution_history'], submission_time)
                            result['execution_portfolio_active'] = result['execution_portfolio']['active']
                            return result
                        result["stage"] = "DECISION_ACCEPTED"
                        from .production_evidence import record
                        record(db, account, result, submission_time)
                        out = boundary.process_trade_plan(plan, **prepared)
                        result["boundary"] = asdict(out)
                        result['stage'] = 'BOUNDARY_RESULT'
                        result["reason"] = out.reason_codes[0] if out.reason_codes else out.status
                        result["execution_permission"] = ("BLOCKED_"+environment+"_ORDER_GATE" if out.status == gate["reason"]
                            else "ORDER_ACTIVE" if out.status in ("EXECUTED", "SUBMIT_UNKNOWN_PENDING_RECONCILIATION")
                            else "BLOCKED_RISK")
                        result["orders_submitted"] = bool(out.attempt and out.attempt.broker_order_id)
                        result['execution_portfolio'] = execution_portfolio(db, account, snapshot,
                            reconcile_executions(db, boundary, client, submission_time), submission_time)
                        result['execution_portfolio_active'] = result['execution_portfolio']['active']
    if not gate["enabled"]:
        result["execution_permission"] = "BLOCKED_"+environment+"_ORDER_GATE"
        result["block_reason_before_order_gate"] = result["reason"]
        result["reason"] = gate["reason"]
    return result


def residual_cost_rates(db, plan):
    """The frozen decision's modeled cost rates, which judge live entry
    economics for its plan. None when the decision is unavailable."""
    with db.connect() as c:
        row = c.execute('SELECT modeled_costs_json FROM cati_residual_decisions WHERE decision_id=?',
                        (plan.source_candidate_id,)).fetchone()
    return json.loads(row[0])['rates'] if row else None


def prepare_submission(db, account, bot, client, boundary, plan, instrument, risk, *, now_ms=None, atr=None):
    """Common production preparation for natural entries and labelled DEMO certification."""
    from app.trading_intelligence.execution.boundary import AccountState
    from app.trading_intelligence.capital.planner import CapitalReadiness
    from app.trading_intelligence.trade_plan.validation import MarketReference
    from app.trading_intelligence.contracts.system_health import BrokerHealthContext
    from app.trading_intelligence.venue.binance import BinanceUsdmEconomicAdapter
    from app.trading_intelligence.venue.adapter import VenueRawSnapshot
    from app.product_safety.execution_safety import evaluate_execution_kyc, evaluate_execution_readiness
    now = int(time.time()*1000) if now_ms is None else now_ms
    if account['broker_id'].lower() == 'binance':
        info = client.exchange_info_cached()
        metadata = next((s for s in info['symbols'] if s['symbol'] == instrument.venue_symbol), None)
        caps = BinanceUsdmEconomicAdapter().describe_execution_capabilities(None, VenueRawSnapshot(
            venue_symbol=instrument.venue_symbol, captured_at=now,
            payloads={'exchange_info_symbol':metadata, 'exchange_info_as_of':now}))
    else:
        from app.trading_intelligence.venue.perpetual import BybitEconomicAdapter, BingXEconomicAdapter
        economic = BybitEconomicAdapter() if account['broker_id'].lower() == 'bybit' else BingXEconomicAdapter()
        caps = economic.describe_execution_capabilities(None, VenueRawSnapshot(venue_symbol=instrument.venue_symbol,
            captured_at=now, payloads={'instrument':instrument, 'metadata_as_of':now}))
    klines = client.klines(symbol=instrument.venue_symbol, interval='15m', limit=250)
    kyc = evaluate_execution_kyc(user_id=account['user_id'], broker_environment=plan.environment.lower())
    readiness = evaluate_execution_readiness(db=db, bot_instance_id=bot['id'], broker_environment=plan.environment.lower())
    controls = persisted_risk_controls(db,bot['id'],risk,now)
    # Read the executable price last, after slower metadata/history work.
    price = float(client.last_price(instrument.venue_symbol))
    observed = int(time.time()*1000) if now_ms is None else now_ms
    rates = book = None
    if plan.mode != 'DEMO_CERTIFICATION':
        with db.connect() as c:
            row = c.execute('SELECT snapshot_json,modeled_costs_json FROM cati_residual_decisions WHERE decision_id=?',
                            (plan.source_candidate_id,)).fetchone()
        if row:
            atr = json.loads(row[0])['candidate']['atr14'] if atr is None else atr
        rates = residual_cost_rates(db, plan)
        # Executable depth for the pre-submit slippage/economics check. An
        # unreadable book is passed as unavailable: hard risk fails closed.
        try:
            book = client.depth(instrument.venue_symbol, limit=100)
        except Exception:
            book = None
        if not (isinstance(book, dict) and isinstance(book.get('bids'), list) and isinstance(book.get('asks'), list)):
            book = {'unavailable': True}
    return dict(market_reference=MarketReference(price,observed),
        broker_health=BrokerHealthContext(account['id'],plan.venue,plan.environment,'HEALTHY',now,'BROKER_SYNC'),
        venue_capabilities=caps, account=AccountState(risk['equity'],risk['margin_used'],risk['free_capital'],risk['open_positions']),
        klines=klines, atr=atr, now_ms=observed, capital=CapitalReadiness(True,'LOGICAL'),
        user_kyc_approved=kyc.allowed, live_readiness_approved=readiness.allowed,
        kyc_status=kyc.state,live_readiness_status=readiness.state,execution_mode='broker',market_type='CRYPTO',
        entry_cost_rates=rates, order_book=book, controls_evidence=controls, **controls)


def reconcile_executions(db, boundary, client, now):
    """Order/fill/protection truth for persisted plans, independent of entry permission."""
    from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore
    from app.trading_intelligence.evidence.stores import ExecutionAttemptStore
    initialize(db)
    plans = TradePlanEvidenceStore(db)
    attempts = ExecutionAttemptStore(db)
    account = boundary.account_scope[1]
    # Resolve a CLOSE whose acknowledgement was lost before considering new
    # capacity. The durable row makes close_position read-only on replay.
    with db.connect() as c:
        pending_closes = []
        if c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_production_closes'").fetchone():
            pending_closes = [dict(r) for r in c.execute(
                "SELECT * FROM cati_production_closes WHERE account_id=? AND status!='CLOSED'", (account,))]
    # One position's problem must not stop the rest of the account's maintenance:
    # every pending close and every attempt is processed, each failure is kept,
    # and the first is raised only at the end (the account still fails closed:
    # the caller evaluates no entry after an exception from here).
    failures = []
    from app.execution.production_close import close_position, EMERGENCY_IDENTITY_PREFIX
    for pending in pending_closes:
        try:
            # An operator emergency flatten carries no trade-plan lineage; its row
            # is already scoped to this account by the query above.
            if not pending['identity'].startswith(EMERGENCY_IDENTITY_PREFIX):
                plan = plans.load_plan(account,pending['identity'].split('|')[0])
                if plan is None or (plan.user_id,plan.broker_account_id) != boundary.account_scope:
                    raise ValueError('CLOSE_ACCOUNT_OWNERSHIP_UNCONFIRMED')
            client._production_intent_identity = pending['identity']
            close_position(client,pending['symbol'])
        except Exception as exc:
            failures.append(exc)
    latest = {}
    for attempt in attempts.for_account(account):
        if attempt["user_id"] == boundary.account_scope[0]:
            latest[attempt["execution_attempt_id"]] = attempt
    history = []
    for attempt in latest.values():
        from app.trading_intelligence.contracts.execution import NO_POSITION
        if attempt['status'] in NO_POSITION or attempt['status'] == 'POSITION_CLOSED':
            continue
        try:
            plan = plans.load_plan(account, attempt["trade_plan_id"])
        except Exception as exc:
            # One unreadable plan row: kept as this account's failure (it still
            # fails closed) without skipping every other position's maintenance.
            failures.append(exc)
            continue
        if plan is None or (plan.user_id, plan.broker_account_id) != boundary.account_scope:
            continue
        payload = attempt["payload"]
        if not payload.get("client_order_id") and not payload.get("broker_order_id"):
            continue
        try:
            _reconcile_attempt(db, boundary, client, now, plans, attempts, account, attempt, plan, payload, history)
        except Exception as exc:
            failures.append(exc)
    if failures:
        # What was reconciled stays available to the caller; the error is unchanged.
        failures[0].execution_history = history
        raise failures[0]
    return history


def _reconcile_attempt(db, boundary, client, now, plans, attempts, account, attempt, plan, payload, history):
    """Order/fill/protection truth of ONE persisted attempt (see reconcile_executions)."""
    if True:  # body kept at its original indentation so the change stays reviewable
        order = boundary.adapter.query_order(plan.instrument_key.venue_symbol,
            broker_order_id=payload.get("broker_order_id"), client_order_id=payload.get("client_order_id"))
        position = boundary.adapter.reconcile_position(plan.instrument_key.venue_symbol)
        all_fills = client.user_trades(symbol=plan.instrument_key.venue_symbol,
            start_time_ms=max(plan.decision_time, now-6*86_400_000), end_time_ms=now, limit=1000)
        if not isinstance(all_fills, list) or len(all_fills) >= 1000:
            raise ValueError("BROKER_FILL_HISTORY_UNAVAILABLE")
        # Only actual broker fills for this order belong to this intent.
        oid = order.broker_order_id or payload.get("broker_order_id")
        fills = [f for f in all_fills if str(f.get("orderId")) == str(oid)]
        with db.connect() as c:
            for fill in fills:
                if fill.get("id") is None:
                    raise ValueError("BROKER_FILL_ID_UNAVAILABLE")
                c.execute("INSERT OR IGNORE INTO cati_production_fills VALUES(?,?,?,?,?)",
                    (account, str(fill["id"]), str(oid), plan.instrument_key.venue_symbol, json.dumps({**fill,
                        '_execution': {'purpose':plan.mode,'trade_plan_id':plan.trade_plan_id,'leg':'ENTRY'}})))
            fills = [json.loads(r[0]) for r in c.execute(
                "SELECT document FROM cati_production_fills WHERE account_id=? AND symbol=? AND order_id=?",
                (account, plan.instrument_key.venue_symbol, str(oid)))]
        item = {"trade_plan_id": plan.trade_plan_id, "cati_decision_id": plan.source_candidate_id,
                "classification": 'DEMO_CERTIFICATION' if plan.mode == 'DEMO_CERTIFICATION' else 'NATURAL_CATI',
                "order": asdict(order), "position": asdict(position), "fills": fills,
                "protection": "NOT_REQUIRED_FLAT" if position.answered and position.quantity == 0 else "UNCONFIRMED"}
        # Binance prints avgPrice rounded; the order's own complete fills give
        # the exact average the position entryPrice is computed from.
        fill_qty = sum(float(f.get("qty", 0)) for f in fills)
        entry_avg = (sum(float(f["price"])*float(f["qty"]) for f in fills)/fill_qty
                     if fill_qty and abs(fill_qty-order.executed_qty) <= 1e-9*order.executed_qty else order.avg_price)
        if order.answered and order.executed_qty > 0 and position.answered and position.side == plan.side and position.quantity > 0 \
                and position.quantity <= order.executed_qty and position.entry_price and entry_avg \
                and abs(position.entry_price-entry_avg) <= 1e-8*entry_avg:
            if order_submission_gate(plan.environment)["enabled"]:
                client._production_intent_identity = f"{plan.trade_plan_id}|{plan.trade_plan_hash}"
                try:
                    item["protection"] = boundary.adapter.submit_protection(plan.instrument_key.venue_symbol,
                        side=plan.side, quantity=min(position.quantity, order.executed_qty),
                        stop_price=plan.structural_invalidation_price, target_price=plan.target_zones[0].price_high)
                except Exception as exc:
                    item["protection"] = {"status": "UNCONFIRMED", "reason": type(exc).__name__}
                    # An acknowledged live entry without proven native protection
                    # uses the same durable reduce-only fail-safe close.
                    item['fail_safe_close'] = boundary.adapter.submit_exit(plan.instrument_key.venue_symbol,
                        side=plan.side,quantity=position.quantity)
                if now >= plan.decision_time + 1 + plan.expected_holding_time_ms:
                    item['horizon_close'] = boundary.adapter.submit_exit(plan.instrument_key.venue_symbol,
                        side=plan.side,quantity=position.quantity)
            else:
                item["protection"] = order_submission_gate(plan.environment)["reason"]
        if order.answered and order.executed_qty > 0 and position.answered and position.quantity == 0:
            # A historical FILLED entry + flat snapshot alone can be eventual
            # consistency. Require an acknowledged close or actual exit fills.
            identity = f'{plan.trade_plan_id}|{plan.trade_plan_hash}'
            with db.connect() as c:
                closed = c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_production_closes'").fetchone()
                closed = closed and c.execute("SELECT document FROM cati_production_closes WHERE account_id=? AND identity=? AND status='CLOSED'",(account,identity)).fetchone()
            # A close intent retired because the account was already flat carries
            # no confirmed close order: it is not evidence of an exit by itself.
            closed = bool(closed and json.loads(closed[0]).get('order'))
            entry_time = max((int(f.get('time',now)) for f in fills),default=now)
            exits = [f for f in all_fills if int(f.get('time',0)) >= entry_time
                     and f.get('side') == ('SELL' if plan.side == 'LONG' else 'BUY')]
            if closed or (fills and sum(float(f.get('qty',0)) for f in exits) >= order.executed_qty):
                with db.connect() as c:
                    for fill in exits:
                        if fill.get('id') is None or fill.get('orderId') is None:
                            raise ValueError('BROKER_FILL_ID_UNAVAILABLE')
                        c.execute('INSERT OR IGNORE INTO cati_production_fills VALUES(?,?,?,?,?)',
                            (account,str(fill['id']),str(fill['orderId']),plan.instrument_key.venue_symbol,
                             json.dumps({**fill,'_execution':{'purpose':plan.mode,
                                 'trade_plan_id':plan.trade_plan_id,'leg':'EXIT'}})))
                from app.execution.production_protection import cancel_flat_protection
                cancel_flat_protection(client,identity)
                from app.execution.entry_protection import get_entry_protection
                get_entry_protection(db).mark_closed(plan.bot_instance_id,plan.instrument_key.venue_symbol,plan.side)
                from dataclasses import replace
                base = boundary._attempt_from_payload(plan,payload)
                records = attempts.history(account,attempt['execution_attempt_id'])
                attempts.append(replace(base,status='POSITION_CLOSED',resolved_at=now,recorded_at=now,
                    reason_codes=tuple(base.reason_codes)+('BROKER_CONFIRMED_FLAT_AND_EXIT_FILL',)),len(records))
                boundary._resolve(plan,'CONSUMED',now,'BROKER_CONFIRMED_ENTRY_AND_CLOSE')
                item.update(lifecycle='CLOSED',exit_fills=exits)
        history.append(item)
