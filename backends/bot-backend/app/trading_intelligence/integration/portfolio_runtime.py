"""Connected-account handoff to the single portfolio-intent execution tail.

The independent scheduler always remains shadow. This handoff obtains broker
positions separately, after family authorization, under the broker cycle's lease
and lock. A recorded shadow decision is never converted to an executable one.
"""
import math
import os
import time
from dataclasses import asdict, replace
from importlib import import_module

from app.trading_intelligence.hashing import short_id
from app.trading_intelligence.pipeline import brakes, contracts as C, execution as X
from app.trading_intelligence.pipeline.family_registry import FamilyRegistry
from app.trading_intelligence.pipeline.portfolio import AccountLimits
from app.trading_intelligence.pipeline.position_management import record_decision
from app.trading_intelligence.pipeline.provenance import DecisionStore

DAY_MS = 86_400_000
MAX_CLOSE_OBSERVATION_DELAY_MS = 60_000
_runtimes = {}


def close_brakes(db, account, client, market, risk_level, now_ms):
    """Use measured broker equity at consecutive UTC closes, never current equity
    relabelled as historical. Missing closes or incomplete cashflow refuse entry.
    """
    from . import income_ledger
    from .production_execution import _TRADING_INCOME

    close = int(market.freshness["decision_time_ms"])
    day = market.cycle_key
    current = brakes.current(db, account["id"], brakes.ACCOUNT_SCOPE)
    if current is not None and current.risk_day == day:
        return current.flags
    if current is not None:
        from datetime import date, timedelta
        if date.fromisoformat(day) - date.fromisoformat(current.risk_day) != timedelta(days=1):
            raise ValueError("ACCOUNT_BRAKE_CLOSE_GAP")
    with db.connect() as conn:
        def sample(at):
            row = conn.execute(
                "SELECT equity,observed_at FROM account_equity_snapshots WHERE broker_account_id=? AND user_id=? "
                "AND source='ENGINE_CYCLE' AND observed_at>=? AND observed_at<=? AND freshness_ms<=? "
                "ORDER BY observed_at LIMIT 1",
                (account["id"], account["user_id"], at, min(at + MAX_CLOSE_OBSERVATION_DELAY_MS, now_ms),
                 MAX_CLOSE_OBSERVATION_DELAY_MS)).fetchone()
            if row is None or not math.isfinite(float(row[0])) or float(row[0]) <= 0:
                raise ValueError("ACCOUNT_BROKER_CLOSE_EVIDENCE_REQUIRED")
            return float(row[0]), int(row[1])
        closing, observed = sample(close)
        opening, previous_observed = sample(close - DAY_MS)
    # The account ledger must cover exactly the two observed closes, including
    # all transfers. No inference from a change in wallet balance is used.
    income_ledger.ensure_covered(db, client, account["id"], previous_observed, observed, force=True)
    flows = sum(float(row["income"]) for row in income_ledger.rows(db, account["id"], previous_observed, observed)
                if row["incomeType"] not in _TRADING_INCOME and int(row["time"]) > previous_observed)
    return brakes.evaluate(
        db, account_id=account["id"], scope=brakes.ACCOUNT_SCOPE, risk_day=day,
        day_definition=brakes.UTC_DAY, closing_equity=closing, initial_equity=opening,
        net_transfers=flows, thresholds=brakes.BrakeThresholds.from_profile(risk_level),
        now_ms=now_ms, actor="broker-portfolio-runtime").flags


def boundary_for(db, account, client, bot, clock):
    from app.exchange.platform_adapter.certification import adapter_certified
    from app.exchange.platform_adapter.factory import adapter_for
    from app.exchange.platform_adapter.rate_budget import BudgetedVenueAdapter, RateBudget
    from app.exchange.platform_adapter.rules_cache import RulesCache
    from app.core.bot_instance_service import BotInstanceService
    from app.core.broker_capability_gate import assert_broker_execution_capability
    from app.core.risk_profile_params import risk_based_params
    from app.runner.effective_policy import resolve_effective_bot_policy
    from app.core.config import settings

    service = BotInstanceService(db=db)
    instance = service.get_bot_instance(bot["id"])
    if instance is None or (instance.user_id, instance.broker_account_id) != (account["user_id"], account["id"]):
        raise ValueError("BROKER_ACCOUNT_OWNERSHIP_MISMATCH")
    if instance.strategy_id != "cati":
        raise ValueError("PRODUCTION_REQUIRES_CATI_BOT")
    assert_broker_execution_capability(db, user_id=account["user_id"], broker_account_id=account["id"])
    params = (risk_based_params(instance) if str(instance.allocation_type).lower() == "risk_based"
              else service.get_risk_profile_preset(instance.risk_level))
    policy = resolve_effective_bot_policy(instance=instance, broker_environment=account["environment"].lower(), risk_params=params)
    if policy.execution_mode != "broker":
        raise ValueError("PRODUCTION_REJECTS_PAPER_BOT")
    max_notional = [float(policy.capital_budget) * float(policy.max_leverage)]
    for value in (policy.max_position_usdt, getattr(settings, "MAX_NOTIONAL_PER_SYMBOL", None)):
        if value is not None and float(value) > 0:
            max_notional.append(float(value))
    if policy.position_allocation_type == "fixed_amount":
        max_notional.append(float(policy.position_allocation_value))
    limits = AccountLimits.from_profile(bot["risk_level"], version=policy.risk_profile_version,
                                        max_position_notional=min(max_notional))
    limits = replace(limits, per_trade_risk=min(limits.per_trade_risk, policy.risk_per_trade),
                     max_open_risk=min(limits.max_open_risk, policy.max_open_risk_fraction or limits.max_open_risk),
                     max_positions=min(limits.max_positions, policy.max_open_positions),
                     max_leverage=min(limits.max_leverage, policy.max_leverage))

    adapter = adapter_for(client, account["broker_id"].lower(), account["environment"], clock=clock)
    client._production_db, client._production_account_id = db, account["id"]
    adapter = BudgetedVenueAdapter(adapter, RateBudget(db), account_ref=account["id"],
                                   server_address=os.environ.get("CATI_EGRESS_SCOPE", "shared-runtime-egress"), clock=clock)
    rules = RulesCache(db, worker="broker-portfolio-runtime")
    boundary = X.PortfolioExecutionBoundary(
        db=db, adapter=adapter, account=C.AccountRef(account["user_id"], account["id"],
                                                   account["broker_id"].lower(), account["environment"]),
        limits=limits, rules=rules,
        adapter_certification=lambda venue, env: adapter_certified(db, venue, env, clock(),
                                                                  adapter_version=adapter.adapter_version), clock=clock)
    boundary.effective_policy, boundary.capital_budget = policy, policy.capital_budget
    return boundary


def operating_controls(db, account, client, bots, snapshot, boundary, now):
    """Preserve the existing broker-wide intraday, period and loss-streak gates."""
    from .production_execution import account_risk, persisted_risk_controls
    if snapshot is None:
        raise ValueError("BROKER_ACCOUNT_RISK_SNAPSHOT_REQUIRED")
    risk = account_risk(db, account, client, snapshot["positions"], snapshot["orders"], bots, now,
                        account_state=snapshot.get("account"), income_force=True)
    controls = persisted_risk_controls(db, bots[0]["id"], risk, now)
    from datetime import datetime, time as day_time
    from zoneinfo import ZoneInfo
    from app.core.config import settings
    start = int(datetime.combine(datetime.fromisoformat(risk["risk_date"]).date(), day_time.min,
                                 ZoneInfo(settings.ADAPTIVE_DAILY_RISK_TIMEZONE)).timestamp() * 1000)
    with db.connect() as conn:
        pipeline_entries = conn.execute(
            "SELECT COUNT(*) FROM (SELECT decision_id,symbol FROM cati_execution_intents WHERE account_id=? "
            "AND kind='ENTRY' AND submitted_at>=? GROUP BY decision_id,symbol)", (account["id"], start)).fetchone()[0]
    controls["daily_trade_count"] += pipeline_entries
    controls["portfolio_entry_attempts"] = pipeline_entries
    reason = risk.get("reason")
    if controls.get("kill_switch"):
        reason = reason or "CATI_NEW_ENTRY_KILL_SWITCH"
    if controls.get("consec_loss_day_paused"):
        reason = reason or "CONSECUTIVE_LOSS_DAY_PAUSED"
    if controls.get("consec_loss_cooldown_until_ms", 0) > now:
        reason = reason or "CONSECUTIVE_LOSS_COOLDOWN"
    for period in ("weekly", "monthly"):
        limit = controls.get("max_" + period + "_drawdown_pct", 0)
        if limit > 0 and controls[period + "_drawdown_pct"] >= limit:
            reason = reason or period.upper() + "_DRAWDOWN_LIMIT_REACHED"
    policy = boundary.effective_policy
    if policy.daily_trade_cap_enabled and controls["daily_trade_count"] >= policy.max_daily_trades:
        reason = reason or "DAILY_TRADE_CAP_REACHED"
    if controls["daily_trade_count"] >= policy.hard_runaway_daily_entry_limit:
        reason = reason or "HARD_DAILY_ENTRY_LIMIT_REACHED"
    if risk.get("loss_latched") or risk.get("remaining_daily_risk", 0) <= 0:
        reason = reason or "ACCOUNT_DAILY_LOSS_LIMIT_REACHED"
    from app.transfers.models import TERMINAL
    terminal = sorted(status.value for status in TERMINAL)
    with db.connect() as conn:
        pending = conn.execute("SELECT COUNT(*) FROM broker_transfer_requests WHERE broker_account_id=? AND status "
                               "NOT IN (" + ",".join("?" for _ in terminal) + ")", (account["id"], *terminal)).fetchone()[0]
    if pending:
        reason = reason or "ACCOUNT_TRANSFER_UNRESOLVED"
    from app.risk.capital_ledger import ACCOUNT_RESERVATIONS
    if ACCOUNT_RESERVATIONS.pending_margin(str(account["id"])) > 0:
        reason = reason or "LEGACY_IN_FLIGHT_MARGIN_UNRESOLVED"
    return reason, risk, controls


def batch_from_record(record, definition):
    """Revalidate the persisted batch without changing mode, identity or sizing."""
    document = record["document"]
    runtime = document["runtime_batch"]
    return C.IntentBatch(
        provenance=C.DecisionProvenance(
            family_id=record["family_id"], family_version=record["family_version"],
            mandate_id=record["mandate_id"] or "NONE", hypothesis_id=definition.hypothesis_id or "NONE",
            rule_hash=record["rule_hash"], decision_id=record["decision_id"], decision_hash=record["decision_hash"],
            decision_time_ms=record["decision_time"], data_snapshot_id=runtime["data_snapshot_id"],
            timeframe=definition.timeframe, mode=record["mode"]),
        account=C.AccountRef(record["user_id"], record["account_id"], runtime["venue"], record["environment"]),
        intents=tuple(C.intent_from_plain(i) for i in document["intents"]),
        constraints=C.PortfolioConstraints(**runtime["constraints"]), instruments=frozenset(runtime["instruments"]),
        created_at_ms=runtime["created_at_ms"], expires_at_ms=record["expires_at"])


class ConnectedPortfolioRuntime:
    def __init__(self, db, *, registry=None, make_boundary=None, data=None, brake_source=None, clock=None):
        from app.core.config import settings
        if settings.production and any(value is not None for value in (registry, make_boundary, data, brake_source, clock)):
            raise ValueError("PRODUCTION_PORTFOLIO_DEPENDENCIES_MUST_BE_AUTHORITATIVE")
        self.db = db
        self.registry = registry if registry is not None else FamilyRegistry(db)
        self.make_boundary = make_boundary or boundary_for
        self.data, self.brake_source = data, brake_source or close_brakes
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.markets = {}

    @staticmethod
    def _may_manage(entry, environment):
        if environment in entry.allowed_modes:
            return True
        from app.trading_intelligence.governance.phases import phase_index
        from app.trading_intelligence.pipeline.family_governance import DEMO_STAGE, LIVE_STAGE
        return (entry.kill_switch_state == "ON"
                and set(entry.blockers) - {"FAMILY_KILL_SWITCH", "NO_PLATFORM_PIPELINE_ADAPTER"} == set()
                and entry.definition.activation_policy.get("kill_switch_position_policy") == "MANAGE_TO_EXIT"
                and phase_index(entry.governance_stage) >= phase_index(DEMO_STAGE if environment == C.DEMO else LIVE_STAGE))

    def _market(self, entry, now):
        from app.market_data.daily_feed import default_daily_bars
        from .platform_runtime import budgeted_archive_get
        definition = entry.definition
        if not definition.pipeline_adapter:
            raise ValueError("FAMILY_BROKER_PIPELINE_ADAPTER_REQUIRED")
        pipeline = import_module(definition.pipeline_adapter).PIPELINE
        if not callable(getattr(pipeline, "broker_account", None)):
            raise ValueError("FAMILY_BROKER_ACCOUNT_STAGE_REQUIRED")
        key = pipeline.due_key(now)
        identity = (definition.family_id, definition.definition_hash, key)
        if identity not in self.markets:
            pipeline.verify(self.registry._research())
            data = self.data
            if data is None:
                if definition.timeframe != "1d":
                    raise ValueError("FAMILY_TIMEFRAME_DATA_PROVIDER_REQUIRED")
                data = default_daily_bars(self.db, http_get=budgeted_archive_get(self.db))
            market = pipeline.market(data, key)
            self.markets = {k: v for k, v in self.markets.items() if k[0] != definition.family_id}
            self.markets[identity] = market
        return pipeline, self.markets[identity]

    def _owned_positions(self, snapshot, family_id, account):
        """Manual or other-family holdings count toward risk but aren't adopted.
        Ambiguous changes to an owned quantity require reconciliation first.
        """
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT symbol,SUM(CASE WHEN side='BUY' THEN filled_quantity ELSE -filled_quantity END) "
                "FROM cati_execution_intents WHERE account_id=? AND user_id=? AND venue=? AND environment=? "
                "AND family_id=? AND filled_quantity>0 GROUP BY symbol",
                (account["id"], account["user_id"], account["broker_id"].lower(), account["environment"], family_id)).fetchall()
        quantities = {r[0]: float(r[1]) for r in rows}
        owned = []
        for held in snapshot.positions:
            expected = quantities.get(held.symbol, 0)
            if expected <= 0:
                continue
            if held.direction != C.LONG or not math.isclose(expected, held.quantity, rel_tol=1e-8, abs_tol=1e-10):
                raise ValueError("PORTFOLIO_OWNED_POSITION_CHANGED_EXTERNALLY")
            owned.append(held)
        return replace(snapshot, positions=tuple(owned))

    def process(self, account, client, *, legacy_maintenance=None, broker_snapshot=None):
        from app.core.config import settings
        entries = [e for e in self.registry.entries() if self._may_manage(e, account["environment"])
                   and account["broker_id"].lower() in {v for v, _ in e.definition.supported_markets}]
        with self.db.connect() as conn:
            history = conn.execute("SELECT 1 FROM cati_execution_intents WHERE account_id=? LIMIT 1", (account["id"],)).fetchone()
        if not entries and not history:
            return None  # Existing rejected-family diagnostics remain visible.
        if settings.production:
            from .production_runtime import owner_current
            if not owner_current(self.db):
                raise ValueError("CANONICAL_RUNTIME_LEASE_REQUIRED")
        if legacy_maintenance is not None:
            legacy_maintenance()  # Compatibility recovery cannot produce a new family decision.
        with self.db.connect() as conn:
            bots = [dict(r) for r in conn.execute(
                "SELECT id,risk_level,status FROM bot_instances WHERE broker_account_id=? AND user_id=? "
                "ORDER BY status='active' DESC,created_at DESC", (account["id"], account["user_id"]))]
        active = [b for b in bots if b["status"] == "active"]
        if not bots:
            raise ValueError("ACCOUNT_PORTFOLIO_RISK_PROFILE_REQUIRED")
        boundary = self.make_boundary(self.db, account, client, bots[0], self.clock)
        recovery = boundary.recover(now_ms=self.clock())
        result = {"stage": "PORTFOLIO_EXECUTION", "recovery": asdict(recovery), "families": [],
                  "bot_instance_id": bots[0]["id"], "execution_permission": "PORTFOLIO_RECOVERY",
                  "reason": "NO_EXECUTABLE_FAMILY"}
        if recovery.status != X.BATCH_PROCESSED or len(active) != 1:
            result.update(execution_permission="BLOCKED_ACCOUNT",
                          reason=(recovery.reasons[0] if recovery.reasons else "ACCOUNT_EXECUTION_OWNER_AMBIGUOUS"))
            return result
        risk_block = None
        if settings.production:
            risk_block, risk, controls = operating_controls(self.db, account, client, active, broker_snapshot, boundary, self.clock())
            result.update(risk=risk, risk_controls=controls, risk_block=risk_block)
        for entry in entries:
            now = self.clock()
            if settings.production and result["families"]:
                refreshed = {**broker_snapshot, "account": None}
                risk_block, risk, controls = operating_controls(self.db, account, client, active, refreshed, boundary, now)
                result.update(risk=risk, risk_controls=controls, risk_block=risk_block)
            pipeline, market = self._market(entry, now)
            definition = entry.definition
            decision_time = pipeline.decision_time_ms(market.cycle_key)
            if now >= decision_time + definition.decision_ttl_ms:
                raise ValueError("FAMILY_BROKER_DECISION_STALE")
            cycle = short_id("broker-cycle", {"family": definition.family_id, "key": market.cycle_key,
                                               "mode": account["environment"], "venue": account["broker_id"].lower()})
            store = DecisionStore(self.db)
            record = store.get(store.decision_id(cycle, account["id"]))
            if record is None:
                flags = self.brake_source(self.db, account, client, market, active[0]["risk_level"], now)
                snapshot = self._owned_positions(boundary.account_snapshot(), definition.family_id, account)
                decision = pipeline.broker_account(market, snapshot, risk_level=active[0]["risk_level"], brake_flags=flags,
                                                    capital_budget=getattr(boundary, "capital_budget", None))
                policy = getattr(boundary, "effective_policy", None)
                if policy is not None and policy.universe_mode == "ALLOWLIST":
                    allowed = {str(symbol).upper().replace("-", "") for symbol in policy.symbols}
                    refused = [i for i in decision.intents if C.increases_risk(i) and i.symbol not in allowed]
                    decision.intents = tuple(i for i in decision.intents if i not in refused)
                    decision.document.setdefault("rejections", []).extend(
                        {"symbol": i.symbol, "reason": "ACCOUNT_INSTRUMENT_NOT_ALLOWED", "stage": "ACCOUNT_ELIGIBILITY"}
                        for i in refused)
                if policy is not None:
                    cap = policy.hard_runaway_daily_entry_limit
                    if policy.daily_trade_cap_enabled:
                        cap = min(cap, policy.max_daily_trades)
                    remaining = max(0, cap - result["risk_controls"]["daily_trade_count"])
                    admitted, refused = [], []
                    for intent in decision.intents:
                        if intent.kind == C.ENTRY:
                            if remaining == 0:
                                refused.append(intent)
                                continue
                            remaining -= 1
                        admitted.append(intent)
                    decision.intents = tuple(admitted)
                    decision.document.setdefault("rejections", []).extend(
                        {"symbol": i.symbol, "reason": "DAILY_ENTRY_CAP_REACHED", "stage": "ACCOUNT_ELIGIBILITY"}
                        for i in refused)
                constraints = boundary.limits.constraints(snapshot.equity)
                extra = {**decision.document, "runtime_brakes": asdict(flags),
                         "runtime_batch": {"venue": account["broker_id"].lower(), "constraints": C.to_plain(constraints),
                                           "instruments": list(decision.instruments), "created_at_ms": now,
                                           "data_snapshot_id": market.data_snapshot_id}}
                batch = record_decision(
                    self.db, definition=definition, account=boundary.account, intents=decision.intents,
                    constraints=constraints, instruments=decision.instruments, cycle_id=cycle,
                    decision_time_ms=decision_time, expires_at_ms=decision_time + definition.decision_ttl_ms,
                    data_snapshot_id=market.data_snapshot_id, mode=account["environment"], now_ms=now, extra=extra)
            else:
                if not store.intact(record):
                    raise ValueError("DECISION_TAMPERED")
                batch = batch_from_record(record, definition)
                flags = brakes.BrakeFlags(**record["document"]["runtime_brakes"])
            # Unknown/blocked operating risk withdraws every increase while the
            # unchanged, recorded reductions and stop management can still run.
            outcome = boundary.execute(batch, brake_state=None if risk_block else flags, now_ms=self.clock())
            result["families"].append({"family_id": definition.family_id, "decision_id": batch.provenance.decision_id,
                                       "execution": asdict(outcome)})
            result.update(execution_permission="PORTFOLIO_PROCESSED", reason=outcome.status)
        return result


def dispatch_account(db, account, client, *, legacy_maintenance, broker_snapshot=None):
    """Called by the canonical broker cycle; no separate order worker is started."""
    key = str(db.path)
    runtime = _runtimes.get(key)
    if runtime is None:
        runtime = ConnectedPortfolioRuntime(db)
        if len(_runtimes) >= 8:
            _runtimes.clear()
        _runtimes[key] = runtime
    return runtime.process(account, client, legacy_maintenance=legacy_maintenance, broker_snapshot=broker_snapshot)
