"""CapitalAllocationPlanner (Phase 5E/5F).

Decides HOW an opportunity on one broker account could be funded -- it does
not move money by itself. Outcomes:

* NO_ACTION_SHARED_COLLATERAL        the product's wallet is shared/unified
                                     collateral and free margin covers it.
* LOGICAL_REALLOCATION               the product's own wallet already holds
                                     enough free capital; assignment is
                                     bookkeeping only.
* PHYSICAL_INTERNAL_TRANSFER_REQUIRED a wallet-to-wallet move INSIDE the same
                                     account is needed; a transfer intent is
                                     produced (and only auto-submitted when
                                     the user authorised automation).
* INSUFFICIENT_SAFE_CAPITAL          even with every safe internal move, not
                                     enough.
* TRANSFER_UNSUPPORTED               a move is needed but the account/broker
                                     cannot (capability, permission, route).
* ACCOUNT_RECONCILIATION_REQUIRED    a transfer on this account is unresolved:
                                     nothing is planned on unknown balances.

Capital from a transfer is NEVER counted before the broker confirms it:
``is_fundable`` stays False for a PHYSICAL plan until the linked transfer
is COMPLETED. There is no cross-broker path: only wallets of THIS account
are considered.

Blocked paths name their reason: ``ACCOUNT_TOPOLOGY_UNKNOWN`` (the account
mode was not read -- shared collateral is never guessed and no transfer is
tried to find out), ``INTERNAL_TRANSFER_ROUTE_UNAVAILABLE`` (no official
same-account route from any wallet, or none the user allows).

``capital_readiness`` is the execution gate (Section 9.14): a PHYSICAL plan
is ready only once its transfer is broker-COMPLETED (confirmed or reconciled);
a shared-collateral / logical plan only while its reservation is valid.
"""
from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Any, Dict, Mapping, Optional, Tuple

from shared_lib.broker.wallets import BrokerTopology, TopologyMode, WalletPurpose

NO_ACTION_SHARED_COLLATERAL = "NO_ACTION_SHARED_COLLATERAL"
LOGICAL_REALLOCATION = "LOGICAL_REALLOCATION"
PHYSICAL_INTERNAL_TRANSFER_REQUIRED = "PHYSICAL_INTERNAL_TRANSFER_REQUIRED"
INSUFFICIENT_SAFE_CAPITAL = "INSUFFICIENT_SAFE_CAPITAL"
TRANSFER_UNSUPPORTED = "TRANSFER_UNSUPPORTED"
ACCOUNT_RECONCILIATION_REQUIRED = "ACCOUNT_RECONCILIATION_REQUIRED"
ACCOUNT_TOPOLOGY_UNKNOWN = "ACCOUNT_TOPOLOGY_UNKNOWN"
INTERNAL_TRANSFER_ROUTE_UNAVAILABLE = "INTERNAL_TRANSFER_ROUTE_UNAVAILABLE"
PLANNER_VERSION = "capital-allocation-planner-v1"


@dataclass(frozen=True)
class CapitalSettings:
    mode: str = "MANUAL_TRANSFER"            # MANUAL_TRANSFER | AUTOMATED_INTERNAL_REALLOCATION
    auto_rebalance_enabled: bool = False
    authorized: bool = False
    max_transfer_amount: Optional[Decimal] = None
    min_funding_balance: Decimal = Decimal("0")
    min_derivatives_reserve: Decimal = Decimal("0")
    buffer_fraction: Decimal = Decimal("0.05")  # move shortfall x (1 + buffer)
    asset_allowlist: Optional[Tuple[str, ...]] = None
    wallet_allowlist: Optional[Tuple[str, ...]] = None
    allowed_routes: Optional[Tuple[str, ...]] = None       # "SRC->DST" (native type or purpose)
    manual_approval_threshold: Optional[Decimal] = None     # automated moves above it need the user
    emergency_disabled: bool = False

    @property
    def automated(self) -> bool:
        return (self.mode == "AUTOMATED_INTERNAL_REALLOCATION" and self.auto_rebalance_enabled and self.authorized
                and not self.emergency_disabled)

    def route_allowed(self, src: Any, dst: Any) -> bool:
        if self.allowed_routes is None:
            return True
        names = {f"{a}->{b}" for a in (src.native_type, src.purpose.value) for b in (dst.native_type, dst.purpose.value)}
        return bool(names & {str(r).strip().upper().replace(" ", "") for r in self.allowed_routes})

    @classmethod
    def from_store(cls, s: Mapping[str, Any]) -> "CapitalSettings":
        d = lambda v, default=None: Decimal(str(v)) if v not in (None, "") else default  # noqa: E731
        return cls(mode=s.get("mode") or "MANUAL_TRANSFER", auto_rebalance_enabled=bool(s.get("auto_rebalance_enabled")),
                   authorized=bool(s.get("authorized_at")), max_transfer_amount=d(s.get("max_transfer_amount")),
                   min_funding_balance=d(s.get("min_funding_balance"), Decimal("0")),
                   min_derivatives_reserve=d(s.get("min_derivatives_reserve"), Decimal("0")),
                   asset_allowlist=tuple(s["asset_allowlist"]) if s.get("asset_allowlist") else None,
                   wallet_allowlist=tuple(s["wallet_allowlist"]) if s.get("wallet_allowlist") else None,
                   allowed_routes=tuple(s["allowed_routes"]) if s.get("allowed_routes") is not None else None,
                   manual_approval_threshold=d(s.get("manual_approval_threshold")),
                   emergency_disabled=bool(s.get("emergency_disabled")))


@dataclass(frozen=True)
class AccountCapitalState:
    broker_account_id: str
    asset: str
    topology: Optional[BrokerTopology]
    #: native wallet -> broker-authoritative free/transferable amount (None = unknown)
    free_by_wallet: Mapping[str, Optional[Decimal]]
    #: capital already committed in each wallet (reservations + pending entries)
    reserved_by_wallet: Mapping[str, Decimal] = field(default_factory=dict)
    transfer_capability_usable: bool = False
    transfer_block_reason: Optional[str] = None
    unresolved_transfers: int = 0


@dataclass(frozen=True)
class TransferProposal:
    source_wallet: str
    destination_wallet: str
    asset: str
    amount: Decimal
    idempotency_key: str
    auto_submit: bool


@dataclass(frozen=True)
class CapitalPlan:
    outcome: str
    broker_account_id: str
    product: str
    asset: str
    required: Decimal
    trading_wallet: Optional[str]
    available_in_trading_wallet: Optional[Decimal]
    transfer: Optional[TransferProposal] = None
    reason_codes: Tuple[str, ...] = ()
    version: str = PLANNER_VERSION

    @property
    def needs_transfer(self) -> bool:
        return self.outcome == PHYSICAL_INTERNAL_TRANSFER_REQUIRED

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        for k in ("required", "available_in_trading_wallet"):
            d[k] = None if d[k] is None else format(d[k], "f")
        if d["transfer"]:
            d["transfer"]["amount"] = format(d["transfer"]["amount"], "f")
        return d


def is_fundable(plan: CapitalPlan, transfer_status: Optional[str] = None) -> bool:
    """May a reservation that depends on this plan become executable?"""
    if plan.outcome in (NO_ACTION_SHARED_COLLATERAL, LOGICAL_REALLOCATION):
        return True
    if plan.outcome == PHYSICAL_INTERNAL_TRANSFER_REQUIRED:
        return transfer_status == "COMPLETED"  # broker-confirmed only; never on SUBMITTED/UNKNOWN
    return False


def plan_capital(*, state: AccountCapitalState, product: str, required: Decimal, settings: CapitalSettings,
                 plan_key: str) -> CapitalPlan:
    asset = state.asset.upper()
    base = dict(broker_account_id=state.broker_account_id, product=product, asset=asset, required=required)
    if state.unresolved_transfers:
        return CapitalPlan(ACCOUNT_RECONCILIATION_REQUIRED, trading_wallet=None, available_in_trading_wallet=None,
                           reason_codes=("TRANSFER_UNRESOLVED",), **base)
    topo = state.topology
    if topo is None:
        # never guess shared collateral, never probe with a transfer
        return CapitalPlan(TRANSFER_UNSUPPORTED, trading_wallet=None, available_in_trading_wallet=None,
                           reason_codes=(ACCOUNT_TOPOLOGY_UNKNOWN,), **base)
    target = topo.wallet_for_product(product)
    if target is None:
        return CapitalPlan(TRANSFER_UNSUPPORTED, trading_wallet=None, available_in_trading_wallet=None,
                           reason_codes=("NO_WALLET_COLLATERALISES_PRODUCT",), **base)
    free_t = state.free_by_wallet.get(target.native_type)
    if free_t is None:
        return CapitalPlan(INSUFFICIENT_SAFE_CAPITAL, trading_wallet=target.native_type, available_in_trading_wallet=None,
                           reason_codes=("TRADING_WALLET_BALANCE_UNKNOWN",), **base)
    usable_t = free_t - state.reserved_by_wallet.get(target.native_type, Decimal("0"))
    if target.purpose in (WalletPurpose.DERIVATIVES, WalletPurpose.UNIFIED):
        usable_t -= settings.min_derivatives_reserve
    shared = any(target.purpose in g for g in topo.shared_purposes) or target.purpose == WalletPurpose.UNIFIED
    if usable_t >= required:
        outcome = NO_ACTION_SHARED_COLLATERAL if shared else LOGICAL_REALLOCATION
        return CapitalPlan(outcome, trading_wallet=target.native_type, available_in_trading_wallet=usable_t, **base)

    shortfall = required - max(usable_t, Decimal("0"))
    amount = (shortfall * (Decimal("1") + settings.buffer_fraction)).quantize(Decimal("0.00000001"))
    reasons = []
    if settings.asset_allowlist is not None and asset not in {a.upper() for a in settings.asset_allowlist}:
        return CapitalPlan(TRANSFER_UNSUPPORTED, trading_wallet=target.native_type, available_in_trading_wallet=usable_t,
                           reason_codes=("ASSET_NOT_ALLOWED",), **base)
    if not state.transfer_capability_usable:
        return CapitalPlan(TRANSFER_UNSUPPORTED, trading_wallet=target.native_type, available_in_trading_wallet=usable_t,
                           reason_codes=(state.transfer_block_reason or "INTERNAL_TRANSFER_UNAVAILABLE",), **base)
    if settings.max_transfer_amount is not None and amount > settings.max_transfer_amount:
        amount = settings.max_transfer_amount
        reasons.append("CAPPED_BY_MAX_TRANSFER_AMOUNT")
        if amount < shortfall:
            return CapitalPlan(INSUFFICIENT_SAFE_CAPITAL, trading_wallet=target.native_type,
                               available_in_trading_wallet=usable_t, reason_codes=tuple(reasons), **base)

    best, routable = None, 0
    for w in sorted(topo.wallets, key=lambda w: w.native_type):
        if w.native_type == target.native_type or not topo.route(w, target):
            continue
        if topo.mode_between(w.purpose, target.purpose) == TopologyMode.SHARED_COLLATERAL:
            continue
        if settings.wallet_allowlist is not None and w.native_type not in settings.wallet_allowlist \
                and w.purpose.value not in settings.wallet_allowlist:
            continue
        if not settings.route_allowed(w, target):
            continue
        routable += 1
        free = state.free_by_wallet.get(w.native_type)
        if free is None:
            continue  # unknown balance is never counted
        keep = settings.min_funding_balance if w.purpose == WalletPurpose.FUNDING else settings.min_derivatives_reserve
        spare = free - state.reserved_by_wallet.get(w.native_type, Decimal("0")) - keep
        if spare >= amount and (best is None or spare > best[1]):
            best = (w, spare)
    if best is None and routable == 0:
        # no official same-account route (or none the user allows): block -- never a withdrawal/deposit/
        # cross-broker fallback, none exists
        return CapitalPlan(TRANSFER_UNSUPPORTED, trading_wallet=target.native_type, available_in_trading_wallet=usable_t,
                           reason_codes=tuple(reasons + [INTERNAL_TRANSFER_ROUTE_UNAVAILABLE]), **base)
    if best is None:
        return CapitalPlan(INSUFFICIENT_SAFE_CAPITAL, trading_wallet=target.native_type, available_in_trading_wallet=usable_t,
                           reason_codes=tuple(reasons + ["NO_WALLET_WITH_SAFE_SPARE_CAPITAL"]), **base)
    src = best[0]
    key = "cap-" + hashlib.sha256(f"{plan_key}|{state.broker_account_id}|{src.native_type}|{target.native_type}|"
                                  f"{asset}|{amount}".encode()).hexdigest()[:40]
    auto = settings.automated
    if auto and settings.manual_approval_threshold is not None and amount > settings.manual_approval_threshold:
        auto = False
        reasons.append("MANUAL_APPROVAL_REQUIRED")
    if settings.emergency_disabled:
        reasons.append("AUTOMATION_EMERGENCY_DISABLED")
    proposal = TransferProposal(src.native_type, target.native_type, asset, amount, key, auto_submit=auto)
    return CapitalPlan(PHYSICAL_INTERNAL_TRANSFER_REQUIRED, trading_wallet=target.native_type,
                       available_in_trading_wallet=usable_t, transfer=proposal,
                       reason_codes=tuple(reasons + ([] if auto else ["USER_ACTION_REQUIRED"])), **base)


@dataclass(frozen=True)
class CapitalReadiness:
    """May an entry that depends on ``plan`` proceed to the execution boundary NOW?

    ``pending`` = not yet, but may become ready (transfer in flight / reconciliation); otherwise a
    not-ready result is definitive for this plan."""
    ready: bool
    path: str                 # LOGICAL | PHYSICAL | BLOCKED
    reason: Optional[str] = None
    pending: bool = False


def capital_readiness(plan: CapitalPlan, *, transfer_status: Optional[str] = None,
                      reservation_status: Optional[str] = None) -> CapitalReadiness:
    """Section 9.13/9.14: local intent is not money. Physical path -> the transfer must be broker-COMPLETED
    (confirmed or reconciled); logical path -> the reservation must be valid (RESERVED)."""
    if plan.outcome in (NO_ACTION_SHARED_COLLATERAL, LOGICAL_REALLOCATION):
        if reservation_status == "RESERVED":
            return CapitalReadiness(True, "LOGICAL")
        return CapitalReadiness(False, "LOGICAL", "LOGICAL_RESERVATION_NOT_VALID"
                                if reservation_status else "LOGICAL_RESERVATION_UNKNOWN")
    if plan.outcome == PHYSICAL_INTERNAL_TRANSFER_REQUIRED:
        st = str(transfer_status or "").upper()
        if st == "COMPLETED":
            return CapitalReadiness(True, "PHYSICAL")
        if st in ("REQUESTED", "VALIDATING", "SUBMITTING", "SUBMITTED", "CONFIRMATION_PENDING", "UNKNOWN",
                  "RECONCILIATION_REQUIRED"):
            return CapitalReadiness(False, "PHYSICAL", f"INTERNAL_TRANSFER_{st}", pending=True)
        return CapitalReadiness(False, "PHYSICAL", f"INTERNAL_TRANSFER_{st}" if st else "INTERNAL_TRANSFER_NOT_CONFIRMED")
    if plan.outcome == ACCOUNT_RECONCILIATION_REQUIRED:
        return CapitalReadiness(False, "BLOCKED", ACCOUNT_RECONCILIATION_REQUIRED, pending=True)
    return CapitalReadiness(False, "BLOCKED", (plan.reason_codes[0] if plan.reason_codes else plan.outcome))


def submit_if_authorised(plan: CapitalPlan, *, service: Any, user_id: str,
                         preconditions: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    """Hand an AUTHORISED automated proposal to the internal-transfer service (which re-validates
    everything, including every ``TRANSFER_PRECONDITIONS`` fact being positively True). Manual plans are
    never submitted; a transfer without the admitted opportunity it funds is refused by the service."""
    if not plan.needs_transfer or plan.transfer is None or not plan.transfer.auto_submit:
        return None
    from app.transfers.models import TRANSFER_PRECONDITIONS, TransferIntent, TransferOrigin

    t = plan.transfer
    facts = {k: preconditions.get(k) is True for k in TRANSFER_PRECONDITIONS}
    return service.request_transfer(TransferIntent(
        user_id=user_id, broker_account_id=plan.broker_account_id, asset=t.asset, amount=t.amount,
        source_wallet=t.source_wallet, destination_wallet=t.destination_wallet, idempotency_key=t.idempotency_key,
        origin=TransferOrigin.CAPITAL_PLANNER,
        metadata={"planner_version": plan.version, "product": plan.product, "preconditions": facts}))


__all__ = ["ACCOUNT_RECONCILIATION_REQUIRED", "ACCOUNT_TOPOLOGY_UNKNOWN", "INTERNAL_TRANSFER_ROUTE_UNAVAILABLE",
           "AccountCapitalState", "CapitalPlan", "CapitalReadiness", "CapitalSettings", "capital_readiness",
           "INSUFFICIENT_SAFE_CAPITAL", "LOGICAL_REALLOCATION", "NO_ACTION_SHARED_COLLATERAL",
           "PHYSICAL_INTERNAL_TRANSFER_REQUIRED", "TRANSFER_UNSUPPORTED", "TransferProposal", "is_fundable",
           "plan_capital", "submit_if_authorised"]
