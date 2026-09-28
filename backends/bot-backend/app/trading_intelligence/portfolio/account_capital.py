"""Account-scoped capital consideration (Sections 17.3-17.7, 17.10; closes Section 16.C/16.D).

Runs AFTER whole-universe ranking and BEFORE account portfolio selection, in global rank order (the
ranking is never re-run or re-ordered by an account's capital or route):

    ranked opportunity
      -> account-wide block (kill switch)            ACCOUNT_KILL_SWITCH
      -> market-family eligibility                    FAMILY_BLOCKED:<reason>   (one family, not the account)
      -> provisional capital requirement              CAPITAL_REQUIREMENT_UNKNOWN
      -> DRY-RUN capital plan (Section 9 planner)     ACCOUNT_CAPITAL_STATE_UNAVAILABLE / planner reasons
      -> transfer / logical-allocation economics      economics/transfer.assess_transfer
      -> FINAL account economics (same admission      FINAL_ECONOMICS_NOT_VIABLE:<gate>
         gates, edge net of a KNOWN transfer fee)

Nothing here moves funds, reserves capital, sizes an order or submits anything. Provisional sizing is the
bot's configured per-trade allocation (``risk.capital_ledger.per_trade_allocation_margin``): an upper
bound of the margin ONE trade may use, never execution authority.

Unified collateral: family budgets (``AccountCapitalPolicy.family_max_fraction``) are POLICY constraints
over ONE capital basis (the trading wallet's broker-reported free collateral), never separate money --
a 10,000 basis with crypto 60% / FX 50% allows at most 10,000 in total, not 11,000.
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, FrozenSet, Mapping, Optional, Sequence, Tuple

from app.trading_intelligence.capital.planner import (
    AccountCapitalState, CapitalSettings, PHYSICAL_INTERNAL_TRANSFER_REQUIRED, plan_capital,
)
from app.trading_intelligence.economics.transfer import (
    LOGICAL_ROUTES, TransferAssessment, TransferEconomics, assess_transfer,
)
from app.trading_intelligence.hashing import stable_hash

ACCOUNT_CAPITAL_VERSION = "account-capital-consideration-v1"
PRODUCT_BY_ASSET_CLASS = {"CRYPTO": "CRYPTO_PERPETUAL", "FX": "FX_PERPETUAL", "COMMODITIES": "TRADFI_PERPETUAL",
                          "STOCK": "TRADFI_PERPETUAL", "INDEX": "TRADFI_PERPETUAL", "FUTURES": "TRADFI_PERPETUAL"}
CAPABILITY_BY_ASSET_CLASS = {"CRYPTO": "crypto_perpetuals", "FX": "fx_perpetuals", "COMMODITIES": "tradfi",
                             "STOCK": "tradfi", "INDEX": "tradfi"}

ACCOUNT_CAPITAL_STATE_UNAVAILABLE = "ACCOUNT_CAPITAL_STATE_UNAVAILABLE"
CAPITAL_REQUIREMENT_UNKNOWN = "CAPITAL_REQUIREMENT_UNKNOWN"
FINAL_ECONOMICS_NOT_VIABLE = "FINAL_ECONOMICS_NOT_VIABLE"
FAMILY_BLOCKED = "FAMILY_BLOCKED"
PRODUCT_TYPE_UNSUPPORTED = "PRODUCT_TYPE_UNSUPPORTED"
CAPITAL_BUDGET_EXCEEDED = "CAPITAL_BUDGET_EXCEEDED"
FAMILY_BUDGET_EXCEEDED = "FAMILY_BUDGET_EXCEEDED"
OPPORTUNITY_EXPIRED = "OPPORTUNITY_EXPIRED"


@dataclass(frozen=True)
class AccountCapitalPolicy:
    schema_version: str = "account-capital-policy-v1"
    #: logical family budgets: (asset_class, max fraction of the capital basis). Constraints, not money.
    family_max_fraction: Tuple[Tuple[str, float], ...] = ()
    #: how long dry-run transfer facts stay valid for final economics
    transfer_facts_ttl_ms: int = 60_000

    @property
    def policy_hash(self) -> str:
        return stable_hash(dataclasses.asdict(self))


@dataclass(frozen=True)
class CandidateCapital:
    ranked_opportunity_id: str
    setup_candidate_id: str
    asset_class: str
    product: Optional[str]
    required_margin: Optional[Decimal]
    provisional_notional: Optional[float]
    trading_wallet: Optional[str]
    plan: Optional[Mapping[str, Any]]
    transfer: Optional[Mapping[str, Any]]
    final_net_edge_r: Optional[float]
    final_conservative_edge_r: Optional[float]
    reason_codes: Tuple[str, ...] = ()
    #: the dry-run ``CapitalPlan`` itself (evidence recording); not part of equality
    capital_plan: Any = field(default=None, compare=False, repr=False)

    @property
    def viable(self) -> bool:
        return not self.reason_codes

    @property
    def physical(self) -> bool:
        return bool(self.plan) and self.plan.get("outcome") == PHYSICAL_INTERNAL_TRANSFER_REQUIRED

    @property
    def transfer_amount(self) -> Decimal:
        t = (self.plan or {}).get("transfer") or {}
        return Decimal(str(t.get("amount"))) if t.get("amount") else Decimal("0")


@dataclass(frozen=True)
class CapitalBudget:
    """Selection-time capital constraint (pure). ``available_by_wallet`` = basis - capital already reserved
    by active CATI reservations on the account (so two bots cannot both spend it)."""
    required: Mapping[str, float]
    wallet: Mapping[str, Optional[str]]
    family: Mapping[str, str]
    physical: FrozenSet[str]
    transfer_amount: Mapping[str, float]
    available_by_wallet: Mapping[str, float]
    family_limit: Mapping[str, float]        # absolute amounts: fraction x basis
    family_reserved: Mapping[str, float]

    def violation(self, rids: Sequence[str]) -> Optional[str]:
        if sum(1 for r in rids if r in self.physical) > 1:
            return f"{CAPITAL_BUDGET_EXCEEDED}:ONE_PHYSICAL_ROUTE_PER_SELECTION"
        by_wallet: Dict[str, float] = {}
        extra: Dict[str, float] = {}
        for r in rids:
            w = self.wallet.get(r) or "UNKNOWN"
            by_wallet[w] = by_wallet.get(w, 0.0) + self.required.get(r, float("inf"))
            if r in self.physical:
                extra[w] = extra.get(w, 0.0) + self.transfer_amount.get(r, 0.0)
        for w, need in by_wallet.items():
            if need > self.available_by_wallet.get(w, 0.0) + extra.get(w, 0.0) + 1e-9:
                return f"{CAPITAL_BUDGET_EXCEEDED}:{w}"
        fam: Dict[str, float] = {}
        for r in rids:
            ac = self.family.get(r, "UNKNOWN")
            fam[ac] = fam.get(ac, 0.0) + self.required.get(r, float("inf"))
        for ac, need in fam.items():
            if ac in self.family_limit and self.family_reserved.get(ac, 0.0) + need > self.family_limit[ac] + 1e-9:
                return f"{FAMILY_BUDGET_EXCEEDED}:{ac}"
        return None


@dataclass(frozen=True)
class AccountCapitalView:
    broker_account_id: str
    user_id: Optional[str]
    asset: str
    topology_class: str
    account_mode: Optional[str]
    capital_basis_by_wallet: Mapping[str, Optional[Decimal]]
    reserved_by_wallet: Mapping[str, Decimal]
    reserved_by_family: Mapping[str, Decimal]
    candidates: Mapping[str, CandidateCapital]
    policy: AccountCapitalPolicy = field(default_factory=AccountCapitalPolicy)
    version: str = ACCOUNT_CAPITAL_VERSION
    #: evidence: the broker topology and per-wallet balances (None = unknown, never 0) this view used
    topology: Optional[Mapping[str, Any]] = None
    balances: Mapping[str, Optional[str]] = field(default_factory=dict)

    def viable(self, rid: str) -> bool:
        c = self.candidates.get(rid)
        return c is not None and c.viable

    def budget(self) -> CapitalBudget:
        viable = {rid: c for rid, c in self.candidates.items() if c.viable}
        available = {w: float(b - self.reserved_by_wallet.get(w, Decimal("0")))
                     for w, b in self.capital_basis_by_wallet.items() if b is not None}
        basis = sum((b for b in self.capital_basis_by_wallet.values() if b is not None), Decimal("0"))
        return CapitalBudget(
            required={rid: float(c.required_margin) for rid, c in viable.items()},
            wallet={rid: c.trading_wallet for rid, c in viable.items()},
            family={rid: c.asset_class for rid, c in viable.items()},
            physical=frozenset(rid for rid, c in viable.items() if c.physical),
            transfer_amount={rid: float(c.transfer_amount) for rid, c in viable.items()},
            available_by_wallet=available,
            family_limit={ac: float(basis) * float(frac) for ac, frac in self.policy.family_max_fraction},
            family_reserved={ac: float(v) for ac, v in self.reserved_by_family.items()})

    def claim(self, selected_rids: Sequence[str]) -> Optional[Dict[str, Any]]:
        """The capital a reservation of ``selected_rids`` must hold, with its lineage (no secrets)."""
        chosen = [self.candidates[r] for r in selected_rids if r in self.candidates]
        if not chosen:
            return None
        wallets = sorted({c.trading_wallet or "UNKNOWN" for c in chosen})
        by_family: Dict[str, Decimal] = {}
        by_wallet: Dict[str, Decimal] = {}
        transfer_in: Dict[str, Decimal] = {}
        for c in chosen:
            need = c.required_margin or Decimal("0")
            w = c.trading_wallet or "UNKNOWN"
            by_family[c.asset_class] = by_family.get(c.asset_class, Decimal("0")) + need
            by_wallet[w] = by_wallet.get(w, Decimal("0")) + need
            if c.physical:  # capital that only exists in the wallet once the (confirmed) transfer lands
                transfer_in[w] = transfer_in.get(w, Decimal("0")) + c.transfer_amount
        basis = sum((b for b in self.capital_basis_by_wallet.values() if b is not None), Decimal("0"))
        return {"user_id": self.user_id, "asset": self.asset, "wallet": wallets[0] if len(wallets) == 1 else "MULTI",
                "amount": format(sum((c.required_margin or Decimal("0") for c in chosen), Decimal("0")), "f"),
                "by_family": {k: format(v, "f") for k, v in sorted(by_family.items())},
                "by_wallet": {k: format(v, "f") for k, v in sorted(by_wallet.items())},
                "transfer_in_by_wallet": {k: format(v, "f") for k, v in sorted(transfer_in.items())},
                "basis": {w: (format(b, "f") if b is not None else None)
                          for w, b in sorted(self.capital_basis_by_wallet.items())},
                "total_basis": format(basis, "f"),
                "family_limits": {ac: float(f) for ac, f in self.policy.family_max_fraction},
                "topology_class": self.topology_class,
                "plans": {c.ranked_opportunity_id: (c.plan or {}).get("outcome") for c in chosen},
                "transfer_keys": sorted(((c.plan or {}).get("transfer") or {}).get("idempotency_key")
                                        for c in chosen if c.physical),
                "policy_hash": self.policy.policy_hash, "version": self.version}

    def rejections(self) -> Tuple[Tuple[str, str, str, str], ...]:
        """(ranked_id, candidate_id, reason_code, detail) for every non-viable candidate, in rank order."""
        return tuple((c.ranked_opportunity_id, c.setup_candidate_id, c.reason_codes[0].split(":", 1)[0],
                      ";".join(c.reason_codes)) for c in self.candidates.values() if not c.viable)


def _dec(v: Any) -> Optional[Decimal]:
    try:
        d = Decimal(str(v))
        return d if d.is_finite() else None
    except Exception:
        return None


def family_eligibility(broker: str, environment: str, asset_classes: Sequence[str], *,
                       permissions: Optional[Mapping[str, Any]] = None) -> Dict[str, Tuple[bool, Optional[str]]]:
    """Per market family: may THIS account execute it? One blocked family never blocks another."""
    from shared_lib.broker.capabilities import Capability, execution_readiness

    out: Dict[str, Tuple[bool, Optional[str]]] = {}
    for ac in sorted(set(asset_classes)):
        cap = CAPABILITY_BY_ASSET_CLASS.get(ac)
        if cap is None:
            out[ac] = (False, PRODUCT_TYPE_UNSUPPORTED)
            continue
        r = execution_readiness(broker, environment, permissions=permissions, product=Capability(cap))
        out[ac] = (bool(r.permitted), None if r.permitted else (r.reason_code or "ACCOUNT_NOT_ELIGIBLE"))
    return out


def consider_account(
    ranked: Sequence[Any], evaluated_by_candidate_id: Mapping[str, Any], *, user_id: Optional[str],
    broker_account_id: str, state: Optional[AccountCapitalState], settings: CapitalSettings,
    per_trade_margin: Optional[Decimal], leverage: float, family: Mapping[str, Tuple[bool, Optional[str]]],
    route_facts: Any, now_ms: int, cycle_id: str, account_block: Optional[str] = None,
    reserved_by_wallet: Optional[Mapping[str, Decimal]] = None,
    reserved_by_family: Optional[Mapping[str, Decimal]] = None,
    policy: Optional[AccountCapitalPolicy] = None, admission_policy: Any = None,
    account_mode: Optional[str] = None,
) -> AccountCapitalView:
    """Pure given its inputs (``route_facts`` is a read-only provider). Global rank order is preserved."""
    from app.trading_intelligence.contracts.economics import AdmissionPolicy
    from app.trading_intelligence.economics.gates import conservative_edge_gate, net_edge_gate

    policy = policy or AccountCapitalPolicy()
    admission = admission_policy or AdmissionPolicy()
    reserved = {k: Decimal(str(v)) for k, v in (reserved_by_wallet or {}).items()}
    if state is not None:
        merged = dict(state.reserved_by_wallet)
        for w, v in reserved.items():
            merged[w] = merged.get(w, Decimal("0")) + v
        state = dataclasses.replace(state, reserved_by_wallet=merged)
    required = per_trade_margin if per_trade_margin is not None and per_trade_margin > 0 else None
    lev = max(float(leverage or 1.0), 1.0)
    out: Dict[str, CandidateCapital] = {}
    for r in sorted(ranked, key=lambda x: x.rank_position):
        ev = evaluated_by_candidate_id.get(r.setup_candidate_id)
        key = r.instrument_key
        ac = str(key.asset_class)
        product = PRODUCT_BY_ASSET_CLASS.get(ac)
        base = dict(ranked_opportunity_id=r.ranked_opportunity_id, setup_candidate_id=r.setup_candidate_id,
                    asset_class=ac, product=product, required_margin=required,
                    provisional_notional=float(required) * lev if required is not None else None,
                    trading_wallet=None, plan=None, transfer=None, final_net_edge_r=None,
                    final_conservative_edge_r=None)
        ok, why = family.get(ac, (False, "FAMILY_ELIGIBILITY_UNKNOWN"))
        if account_block:
            out[r.ranked_opportunity_id] = CandidateCapital(**base, reason_codes=(account_block,))
            continue
        if not ok:
            out[r.ranked_opportunity_id] = CandidateCapital(**base, reason_codes=(f"{FAMILY_BLOCKED}:{why}",))
            continue
        if product is None:
            out[r.ranked_opportunity_id] = CandidateCapital(**base, reason_codes=(PRODUCT_TYPE_UNSUPPORTED,))
            continue
        if required is None:
            out[r.ranked_opportunity_id] = CandidateCapital(**base, reason_codes=(CAPITAL_REQUIREMENT_UNKNOWN,))
            continue
        if state is None:
            out[r.ranked_opportunity_id] = CandidateCapital(**base, reason_codes=(ACCOUNT_CAPITAL_STATE_UNAVAILABLE,))
            continue
        if ev is None:
            out[r.ranked_opportunity_id] = CandidateCapital(**base, reason_codes=("EVALUATION_EVIDENCE_MISSING",))
            continue
        if ev.candidate.valid_until is None or now_ms >= ev.candidate.valid_until:
            # an expired (or validity-unknown) opportunity is never approved for capital, on ANY route
            out[r.ranked_opportunity_id] = CandidateCapital(**base, reason_codes=(OPPORTUNITY_EXPIRED,))
            continue
        # DRY RUN: a pure plan -- no transfer intent is created, nothing is submitted or reserved
        plan = plan_capital(state=state, product=product, required=required, settings=settings,
                            plan_key=f"{cycle_id}:{r.ranked_opportunity_id}")
        facts = None
        if plan.needs_transfer and plan.transfer is not None:
            facts = route_facts.facts(broker_account_id=broker_account_id, source_wallet=plan.transfer.source_wallet,
                                      destination_wallet=plan.transfer.destination_wallet,
                                      asset=plan.transfer.asset, now_ms=now_ms)
        t = plan.transfer
        transfer = TransferEconomics(
            plan.outcome, user_id or "", broker_account_id, now_ms, now_ms + policy.transfer_facts_ttl_ms,
            f"{plan.version}:DRY_RUN", fee_quote=facts.fee if facts else None,
            expected_latency_ms=facts.latency_ms if facts else None, fee_currency=facts.fee_currency if facts else None,
            fee_source=facts.fee_source if facts else None, latency_source=facts.latency_source if facts else None,
            source_wallet=t.source_wallet if t else None, destination_wallet=t.destination_wallet if t else None,
            asset=plan.asset, amount=format(t.amount, "f") if t else None, plan_reason_codes=tuple(plan.reason_codes))
        cand = ev.candidate
        entry, risk = float(cand.trigger_reference), float(cand.initial_structural_risk)
        risk_ccy = (float(required) * lev) * risk / entry if entry > 0 and risk > 0 else None
        a: TransferAssessment = assess_transfer(
            transfer, now=now_ms, opportunity_valid_until=cand.valid_until, user_id=user_id or "",
            broker_account_id=broker_account_id, risk_ccy=risk_ccy, settlement_currency=state.asset.upper())
        reasons = list(a.reason_codes)
        opp = ev.opportunity
        net = cons = None
        if a.viable:
            fee_r = a.fee_R or 0.0  # a KNOWN physical fee reduces the edge once; NOT_APPLICABLE reduces nothing
            net, cons = float(opp.ev_net_r) - fee_r, float(opp.conservative_edge_r) - fee_r
            for gate in (net_edge_gate(float(opp.ev_gross_r), net, admission), conservative_edge_gate(cons, admission)):
                if not gate.passed:
                    reasons.append(f"{FINAL_ECONOMICS_NOT_VIABLE}:{gate.reason_code}")
        cost = ev.cost_estimate
        mae = (getattr(cost, "native_costs", None) or {}).get("multi_asset_economics")
        if mae is not None and mae.get("availability") == "UNAVAILABLE_WITH_REASON":
            reasons.append("ECONOMICS_UNAVAILABLE")
        out[r.ranked_opportunity_id] = CandidateCapital(
            **{**base, "trading_wallet": plan.trading_wallet, "plan": plan.to_dict(), "transfer": a.to_dict(),
               "final_net_edge_r": net, "final_conservative_edge_r": cons},
            reason_codes=tuple(dict.fromkeys(reasons)), capital_plan=plan)
    topo = state.topology if state is not None else None
    basis: Dict[str, Optional[Decimal]] = {}
    if state is not None:
        for c in out.values():
            if c.trading_wallet and c.trading_wallet not in basis:
                basis[c.trading_wallet] = state.free_by_wallet.get(c.trading_wallet)
    return AccountCapitalView(
        broker_account_id=broker_account_id, user_id=user_id, asset=(state.asset if state else "UNKNOWN").upper(),
        topology_class=(topo.topology_class.value if topo is not None else "UNKNOWN"),
        account_mode=account_mode or (topo.account_mode if topo is not None else None),
        capital_basis_by_wallet=basis, reserved_by_wallet=reserved,
        reserved_by_family={k: Decimal(str(v)) for k, v in (reserved_by_family or {}).items()},
        candidates=out, policy=policy, topology=topo.to_dict() if topo is not None else None,
        balances={w: (format(v, "f") if v is not None else None)
                  for w, v in sorted((state.free_by_wallet if state is not None else {}).items())})


__all__ = ["ACCOUNT_CAPITAL_STATE_UNAVAILABLE", "ACCOUNT_CAPITAL_VERSION", "AccountCapitalPolicy",
           "AccountCapitalView", "CAPITAL_BUDGET_EXCEEDED", "CAPITAL_REQUIREMENT_UNKNOWN", "CandidateCapital",
           "CapitalBudget", "FAMILY_BLOCKED", "FAMILY_BUDGET_EXCEEDED", "FINAL_ECONOMICS_NOT_VIABLE",
           "OPPORTUNITY_EXPIRED", "PRODUCT_BY_ASSET_CLASS", "PRODUCT_TYPE_UNSUPPORTED", "consider_account", "family_eligibility"]
