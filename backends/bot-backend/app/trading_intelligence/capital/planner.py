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

    @property
    def automated(self) -> bool:
        return self.mode == "AUTOMATED_INTERNAL_REALLOCATION" and self.auto_rebalance_enabled and self.authorized

    @classmethod
    def from_store(cls, s: Mapping[str, Any]) -> "CapitalSettings":
        d = lambda v, default=None: Decimal(str(v)) if v not in (None, "") else default  # noqa: E731
        return cls(mode=s.get("mode") or "MANUAL_TRANSFER", auto_rebalance_enabled=bool(s.get("auto_rebalance_enabled")),
                   authorized=bool(s.get("authorized_at")), max_transfer_amount=d(s.get("max_transfer_amount")),
                   min_funding_balance=d(s.get("min_funding_balance"), Decimal("0")),
                   min_derivatives_reserve=d(s.get("min_derivatives_reserve"), Decimal("0")),
                   asset_allowlist=tuple(s["asset_allowlist"]) if s.get("asset_allowlist") else None,
                   wallet_allowlist=tuple(s["wallet_allowlist"]) if s.get("wallet_allowlist") else None)


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
    target = topo.wallet_for_product(product) if topo else None
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

    best = None
    for w in sorted(topo.wallets, key=lambda w: w.native_type):
        if w.native_type == target.native_type or not topo.route(w, target):
            continue
        if topo.mode_between(w.purpose, target.purpose) == TopologyMode.SHARED_COLLATERAL:
            continue
        if settings.wallet_allowlist is not None and w.native_type not in settings.wallet_allowlist \
                and w.purpose.value not in settings.wallet_allowlist:
            continue
        free = state.free_by_wallet.get(w.native_type)
        if free is None:
            continue  # unknown balance is never counted
        keep = settings.min_funding_balance if w.purpose == WalletPurpose.FUNDING else settings.min_derivatives_reserve
        spare = free - state.reserved_by_wallet.get(w.native_type, Decimal("0")) - keep
        if spare >= amount and (best is None or spare > best[1]):
            best = (w, spare)
    if best is None:
        return CapitalPlan(INSUFFICIENT_SAFE_CAPITAL, trading_wallet=target.native_type, available_in_trading_wallet=usable_t,
                           reason_codes=tuple(reasons + ["NO_WALLET_WITH_SAFE_SPARE_CAPITAL"]), **base)
    src = best[0]
    key = "cap-" + hashlib.sha256(f"{plan_key}|{state.broker_account_id}|{src.native_type}|{target.native_type}|"
                                  f"{asset}|{amount}".encode()).hexdigest()[:40]
    proposal = TransferProposal(src.native_type, target.native_type, asset, amount, key, auto_submit=settings.automated)
    return CapitalPlan(PHYSICAL_INTERNAL_TRANSFER_REQUIRED, trading_wallet=target.native_type,
                       available_in_trading_wallet=usable_t, transfer=proposal,
                       reason_codes=tuple(reasons + ([] if settings.automated else ["USER_ACTION_REQUIRED"])), **base)


def submit_if_authorised(plan: CapitalPlan, *, service: Any, user_id: str) -> Optional[Dict[str, Any]]:
    """Hand an AUTHORISED automated proposal to the internal-transfer service
    (which re-validates everything). Manual plans are never submitted."""
    if not plan.needs_transfer or plan.transfer is None or not plan.transfer.auto_submit:
        return None
    from app.transfers.models import TransferIntent, TransferOrigin

    t = plan.transfer
    return service.request_transfer(TransferIntent(
        user_id=user_id, broker_account_id=plan.broker_account_id, asset=t.asset, amount=t.amount,
        source_wallet=t.source_wallet, destination_wallet=t.destination_wallet, idempotency_key=t.idempotency_key,
        origin=TransferOrigin.CAPITAL_PLANNER, metadata={"planner_version": plan.version, "product": plan.product}))


__all__ = ["ACCOUNT_RECONCILIATION_REQUIRED", "AccountCapitalState", "CapitalPlan", "CapitalSettings",
           "INSUFFICIENT_SAFE_CAPITAL", "LOGICAL_REALLOCATION", "NO_ACTION_SHARED_COLLATERAL",
           "PHYSICAL_INTERNAL_TRANSFER_REQUIRED", "TRANSFER_UNSUPPORTED", "TransferProposal", "is_fundable",
           "plan_capital", "submit_if_authorised"]
