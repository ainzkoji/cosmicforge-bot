"""Broker-internal transfer service (Phase 2G).

    request -> REQUESTED -> VALIDATING -> BLOCKED (reason)            [terminal]
                                       -> SUBMITTING -> COMPLETED      [terminal]
                                                     -> FAILED         [terminal, broker refused before moving]
                                                     -> CONFIRMATION_PENDING / SUBMITTED
                                                     -> UNKNOWN        (dispatched, outcome not known)
    UNKNOWN / SUBMITTED / CONFIRMATION_PENDING -> reconciliation only
                                                  (NEVER re-submitted)

Validation before submission (every check fails closed with a reason code):
ownership + active credential (canonical resolver), INTERNAL_TRANSFER
capability for broker+environment, positive permission evidence with
WITHDRAW denied, both wallets exist in the account's live topology, a
native route exists and the wallets are not shared collateral (a unified
account needs NO physical transfer), asset/wallet allowlists, amount > 0,
per-transfer and 24h limits, broker-authoritative transferable balance,
minimum funding/derivatives/free-margin reserves, no pending CATI
reservation when collateral leaves a trading wallet, active-bot capital
allocations still covered, and no other transfer in flight on the account.
"""
from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional

from shared_lib.broker import BrokerResolverError, resolve_broker_auth
from shared_lib.broker.capabilities import Capability, CapabilityState, declared_profile
from shared_lib.broker.permissions import is_internal_transfer_permitted, load_evidence
from shared_lib.broker.resolver import BrokerAuth
from shared_lib.broker.wallets import BrokerTopology, TopologyMode, WalletPurpose
from shared_lib.core.security.redaction import redact_exception

from app.transfers.adapters import BrokerRejected, TransferAdapter, VenueApiUnavailable, adapter_for
from app.transfers.models import BlockReason as B
from app.transfers.models import TRANSFER_PRECONDITIONS, TransferIntent, TransferOrigin, TransferStatus as S
from app.transfers.store import TransferStore

logger = logging.getLogger(__name__)

_TRADING_PURPOSES = {WalletPurpose.DERIVATIVES, WalletPurpose.UNIFIED, WalletPurpose.FX, WalletPurpose.TRADFI,
                     WalletPurpose.STOCKS}
_account_locks: Dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _account_lock(account_id: str) -> threading.Lock:
    with _locks_guard:
        return _account_locks.setdefault(account_id, threading.Lock())


class TransferAccessError(PermissionError):
    """The account does not exist for this user (never reveals which)."""


@dataclass
class Validation:
    ok: bool
    reason: Optional[str] = None
    detail: str = ""
    route_code: Optional[str] = None
    source_native: Optional[str] = None
    destination_native: Optional[str] = None
    transferable: Optional[Decimal] = None


def _metric(venue: str, status: str, reason: Optional[str]) -> None:
    from app.ops import multi_asset_metrics

    multi_asset_metrics.transfer(venue, status, reason)


def _dec(v: Any) -> Optional[Decimal]:
    try:
        return Decimal(str(v)) if v not in (None, "") else None
    except (InvalidOperation, ValueError):
        return None


class InternalTransferService:
    def __init__(self, db: Any, *, adapter_factory: Callable[[BrokerAuth], Optional[TransferAdapter]] = adapter_for,
                 resolver: Callable[..., BrokerAuth] = resolve_broker_auth):
        self.db = db
        self.store = TransferStore(db)
        self._adapter_factory = adapter_factory
        self._resolve = resolver

    # -- account context ------------------------------------------------------------
    def _auth(self, user_id: str, account_id: str) -> BrokerAuth:
        try:
            return self._resolve(account_id, user_id, self.db)
        except BrokerResolverError as exc:
            if exc.reason_code in (BrokerResolverError.REASON_NOT_FOUND, BrokerResolverError.REASON_ACCESS_DENIED):
                raise TransferAccessError("broker account not found") from exc
            raise

    def _evidence(self, account_id: str, version: int):
        with self.db.connect() as conn:
            r = conn.execute("SELECT permissions_json FROM broker_credentials_v2 WHERE account_id=? AND version=?",
                             (account_id, version)).fetchone()
        return load_evidence(r[0] if r else None)

    def capabilities(self, *, user_id: str, account_id: str) -> Dict[str, Any]:
        """Account-scoped transfer capability + topology (GET transfer-capabilities)."""
        auth = self._auth(user_id, account_id)
        ev = self._evidence(account_id, auth.credential_version)
        profile = declared_profile(auth.broker_type)
        if ev is not None:
            profile = profile.for_account(dict(ev.permissions))
        perm_ok, perm_reason = is_internal_transfer_permitted(ev)
        adapter = self._adapter_factory(auth)
        topo, topo_error = None, None
        if adapter is not None:
            try:
                topo = adapter.topology()
            except Exception as exc:
                topo_error = redact_exception(exc)
        cap = profile.entry(Capability.INTERNAL_TRANSFER)
        usable = profile.usable(Capability.INTERNAL_TRANSFER, auth.environment) and perm_ok and topo is not None
        return {
            "broker_account_id": account_id,
            "broker": auth.broker_type,
            "environment": auth.environment.value,
            "withdrawals_supported_by_platform": False,
            "internal_transfer": {"state": cap.state.value, "reason_code": cap.reason_code, "usable": usable,
                                  "permission_ok": perm_ok, "permission_reason": None if perm_ok else perm_reason},
            "permission_evidence": ev.to_dict() if ev else None,
            "topology": topo.to_dict() if topo else None,
            "topology_error": topo_error or (None if topo else B.ACCOUNT_MODE_UNKNOWN.value),
        }

    def wallets(self, *, user_id: str, account_id: str, assets: Optional[List[str]] = None) -> Dict[str, Any]:
        auth = self._auth(user_id, account_id)
        adapter = self._adapter_factory(auth)
        if adapter is None:
            return {"broker": auth.broker_type, "wallets": [], "reason_code": B.INTERNAL_TRANSFER_UNSUPPORTED.value}
        topo = adapter.topology()
        if topo is None:
            return {"broker": auth.broker_type, "wallets": [], "reason_code": B.ACCOUNT_MODE_UNKNOWN.value}
        out = []
        for w in topo.wallets:
            balances = {}
            for asset in (assets or ["USDT"]):
                try:
                    val = adapter.transferable(w, asset)
                    balances[asset.upper()] = {"transferable": None if val is None else format(val, "f"),
                                               "status": "AVAILABLE" if val is not None else "UNAVAILABLE"}
                except VenueApiUnavailable:
                    balances[asset.upper()] = {"transferable": None, "status": "UNAVAILABLE",
                                               "reason_code": B.VENUE_API_UNAVAILABLE.value}
                except Exception as exc:
                    balances[asset.upper()] = {"transferable": None, "status": "UNAVAILABLE",
                                               "reason_code": "BROKER_ERROR", "detail": redact_exception(exc)[:160]}
            out.append({**w.to_dict(), "balances": balances})
        return {"broker": auth.broker_type, "account_mode": topo.account_mode, "wallets": out,
                "validation_status": topo.validation_status}

    # -- validation -----------------------------------------------------------------
    def _validate(self, auth: BrokerAuth, adapter: Optional[TransferAdapter], row: Dict[str, Any],
                  settings: Dict[str, Any]) -> Validation:
        profile = declared_profile(auth.broker_type)
        cap = profile.entry(Capability.INTERNAL_TRANSFER)
        if adapter is None or cap.state in (CapabilityState.UNSUPPORTED, CapabilityState.VENUE_API_UNAVAILABLE):
            return Validation(False, B.INTERNAL_TRANSFER_UNSUPPORTED.value, f"{auth.broker_type}: {cap.reason_code}")
        if not profile.usable(Capability.INTERNAL_TRANSFER, auth.environment):
            return Validation(False, B.INTERNAL_TRANSFER_UNSUPPORTED.value,
                              f"internal transfer is {cap.state.value} for {auth.environment.value}")
        ok, reason = is_internal_transfer_permitted(self._evidence(auth.account_id, auth.credential_version))
        if not ok:
            return Validation(False, reason)

        automated = row["origin"] != TransferOrigin.MANUAL.value
        if automated:
            blocked = self._automation_block(auth, row, settings)
            if blocked is not None:
                return blocked

        try:
            topo: Optional[BrokerTopology] = adapter.topology()
        except VenueApiUnavailable:
            return Validation(False, B.VENUE_API_UNAVAILABLE.value)
        if topo is None:
            return Validation(False, B.ACCOUNT_MODE_UNKNOWN.value)
        src, dst = topo.wallet(row["source_wallet"]), topo.wallet(row["destination_wallet"])
        if src is None:
            return Validation(False, B.UNKNOWN_SOURCE_WALLET.value, row["source_wallet"])
        if dst is None:
            return Validation(False, B.UNKNOWN_DESTINATION_WALLET.value, row["destination_wallet"])
        if src.native_type == dst.native_type:
            return Validation(False, B.SAME_WALLET.value)
        if topo.mode_between(src.purpose, dst.purpose) == TopologyMode.SHARED_COLLATERAL:
            return Validation(False, B.SHARED_COLLATERAL_NO_TRANSFER_NEEDED.value)
        route = topo.route(src, dst)
        if not route:
            return Validation(False, B.ROUTE_UNSUPPORTED.value, f"{src.native_type}->{dst.native_type}")
        allowed_routes = settings.get("allowed_routes")
        if allowed_routes is not None:
            names = {f"{a}->{b}" for a in (src.native_type, src.purpose.value)
                     for b in (dst.native_type, dst.purpose.value)}
            if not names & {str(r).strip().upper().replace(" ", "") for r in allowed_routes}:
                return Validation(False, B.ROUTE_NOT_ALLOWED.value, f"{src.native_type}->{dst.native_type}")

        asset = row["asset"].upper()
        amount = _dec(row["amount"])
        if amount is None or amount <= 0:
            return Validation(False, B.INVALID_AMOUNT.value)
        threshold = _dec(settings.get("manual_approval_threshold"))
        if automated and threshold is not None and amount > threshold:
            return Validation(False, B.MANUAL_APPROVAL_REQUIRED.value, f"{amount} > {threshold}")
        allow_assets = settings.get("asset_allowlist")
        if allow_assets is not None and asset not in {a.upper() for a in allow_assets}:
            return Validation(False, B.ASSET_NOT_ALLOWED.value, asset)
        allow_wallets = settings.get("wallet_allowlist")
        if allow_wallets is not None:
            allowed = {w.upper() for w in allow_wallets}
            for w in (src, dst):
                if w.native_type.upper() not in allowed and w.purpose.value not in allowed:
                    return Validation(False, B.WALLET_NOT_ALLOWED.value, w.native_type)
        max_amt = _dec(settings.get("max_transfer_amount"))
        if max_amt is not None and amount > max_amt:
            return Validation(False, B.MAX_TRANSFER_AMOUNT_EXCEEDED.value)
        daily = _dec(settings.get("daily_transfer_limit"))
        if daily is not None and self.store.completed_amount_since(auth.account_id, asset) + amount > daily:
            return Validation(False, B.DAILY_TRANSFER_LIMIT_EXCEEDED.value)

        if self.store.in_flight(auth.account_id, exclude_id=row["id"]):
            return Validation(False, B.TRANSFER_IN_FLIGHT.value, "another transfer on this account is unresolved")

        try:
            transferable = adapter.transferable(src, asset)
        except VenueApiUnavailable:
            return Validation(False, B.VENUE_API_UNAVAILABLE.value)
        except Exception as exc:
            return Validation(False, B.SOURCE_BALANCE_UNAVAILABLE.value, redact_exception(exc)[:160])
        if transferable is None:
            return Validation(False, B.SOURCE_BALANCE_UNAVAILABLE.value)
        if transferable < amount:
            return Validation(False, B.INSUFFICIENT_TRANSFERABLE_BALANCE.value,
                              f"transferable {transferable} < {amount}", transferable=transferable)
        max_pct = _dec(settings.get("max_transfer_pct"))
        if max_pct is not None and amount > transferable * max_pct:
            return Validation(False, B.MAX_TRANSFER_PCT_EXCEEDED.value, f"{amount} > {max_pct} x {transferable}")
        dest_cap = _dec(settings.get("max_destination_balance"))
        if dest_cap is not None:
            try:
                dest_now = adapter.transferable(dst, asset)
            except Exception:
                dest_now = None
            if dest_now is None:  # an unknown destination balance never satisfies a cap
                return Validation(False, B.DESTINATION_BALANCE_UNAVAILABLE.value)
            if dest_now + amount > dest_cap:
                return Validation(False, B.DESTINATION_CAP_EXCEEDED.value, f"{dest_now} + {amount} > {dest_cap}")
        remaining = transferable - amount
        min_fund = _dec(settings.get("min_funding_balance"))
        if src.purpose == WalletPurpose.FUNDING and min_fund is not None and remaining < min_fund:
            return Validation(False, B.MIN_FUNDING_BALANCE.value)
        if src.purpose in _TRADING_PURPOSES:
            reserve = _dec(settings.get("min_derivatives_reserve"))
            if reserve is not None and remaining < reserve:
                return Validation(False, B.MIN_DERIVATIVES_RESERVE.value)
            min_free = _dec(settings.get("min_free_margin"))
            if min_free is not None and remaining < min_free:
                return Validation(False, B.MIN_FREE_MARGIN.value)
            if self._pending_reservations(auth.account_id):
                return Validation(False, B.PENDING_RISK_RESERVATIONS.value)
            required = self._active_bot_allocations(auth.account_id)
            if required is not None and remaining < required:
                return Validation(False, B.BOT_ALLOCATION_CONSTRAINT.value,
                                  f"active bots allocate {required}; {remaining} would remain")
        return Validation(True, route_code=route, source_native=src.native_type, destination_native=dst.native_type,
                          transferable=transferable)

    def _automation_block(self, auth: BrokerAuth, row: Dict[str, Any], settings: Dict[str, Any]) -> Optional[Validation]:
        """Auto Capital Routing is separate from auto trading and from manual transfers: an automated
        move needs the user's explicit grant, the switch on, no emergency stop, the CATI new-entry kill
        switch off, and the admitted opportunity it funds (never a loss, never "more capital")."""
        if settings.get("emergency_disabled"):
            return Validation(False, B.AUTOMATION_EMERGENCY_DISABLED.value)
        if not (settings.get("mode") == "AUTOMATED_INTERNAL_REALLOCATION" and settings.get("authorized_at")):
            return Validation(False, B.AUTOMATION_NOT_AUTHORIZED.value)
        if not settings.get("auto_rebalance_enabled"):
            return Validation(False, B.AUTOMATION_DISABLED.value)
        try:
            from app.trading_intelligence.governance.promotion import PromotionGovernance

            kill = PromotionGovernance(self.db).kill_switch_on(scope=auth.account_id)
        except Exception:
            kill = None
        if kill is None:
            return Validation(False, B.GOVERNANCE_STATE_UNAVAILABLE.value)
        if kill:
            return Validation(False, B.CATI_KILL_SWITCH_ACTIVE.value)
        pre = (row.get("metadata") or {}).get("preconditions")
        missing = [k for k in TRANSFER_PRECONDITIONS if not (isinstance(pre, dict) and pre.get(k) is True)]
        if missing:
            return Validation(False, B.TRANSFER_PRECONDITIONS_NOT_ESTABLISHED.value, ",".join(missing))
        return None

    def _pending_reservations(self, account_id: str) -> int:
        try:
            with self.db.connect() as conn:
                r = conn.execute("SELECT COUNT(*) FROM cati_portfolio_reservations WHERE broker_account_id=? "
                                 "AND status IN ('RESERVED','RESOLUTION_PENDING')", (account_id,)).fetchone()
            return int(r[0] or 0)
        except Exception:
            return 0  # table absent: no CATI reservations exist

    def _active_bot_allocations(self, account_id: str) -> Optional[Decimal]:
        """Sum of fixed capital allocations of active bots on this account."""
        with self.db.connect() as conn:
            rows = conn.execute("SELECT capital_allocation, capital_allocation_type FROM bot_instances "
                                "WHERE broker_account_id=? AND status IN ('active','error')", (account_id,)).fetchall()
        total = Decimal("0")
        for r in rows:
            if str(r["capital_allocation_type"] or "").lower() in ("fixed_amount", "fixed", "usdt"):
                total += _dec(r["capital_allocation"]) or Decimal("0")
        return total if rows else None

    # -- request / submit -----------------------------------------------------------
    def request_transfer(self, intent: TransferIntent) -> Dict[str, Any]:
        auth = self._auth(intent.user_id, intent.broker_account_id)
        row, created = self.store.create_or_get(intent, broker=auth.broker_type, environment=auth.environment.value,
                                                credential_version=auth.credential_version)
        if not created:
            return row  # idempotent replay: same transfer, never a second submission
        with _account_lock(intent.broker_account_id):
            return self._run(auth, row)

    def _run(self, auth: BrokerAuth, row: Dict[str, Any]) -> Dict[str, Any]:
        tid = row["id"]
        self.store.transition(tid, expect=S.REQUESTED, to=S.VALIDATING, event="VALIDATION_STARTED")
        adapter = self._adapter_factory(auth)
        settings = self.store.settings(user_id=auth.user_id, broker_account_id=auth.account_id)
        try:
            v = self._validate(auth, adapter, row, settings)
        except Exception as exc:  # a validation error is a block, never a submission
            v = Validation(False, "VALIDATION_ERROR", redact_exception(exc)[:200])
        if not v.ok:
            self.store.transition(tid, expect=S.VALIDATING, to=S.BLOCKED, event="BLOCKED",
                                  detail={"reason": v.reason, "detail": v.detail}, failure_reason=v.reason)
            logger.info("internal_transfer_blocked id=%s account=%s reason=%s", tid, auth.account_id, v.reason)
            _metric(auth.broker_type, "BLOCKED", v.reason)
            return self.store.get_any(tid)

        meta = dict(row.get("metadata") or {})
        meta["route_code"] = v.route_code
        meta["pre_submit_transferable"] = format(v.transferable, "f") if v.transferable is not None else None
        from datetime import datetime, timezone

        submitted_at = datetime.now(timezone.utc).isoformat()
        self.store.transition(tid, expect=S.VALIDATING, to=S.SUBMITTING, event="SUBMITTING",
                              detail={"route_code": v.route_code}, source_venue_wallet=v.source_native,
                              destination_venue_wallet=v.destination_native, submitted_at=submitted_at,
                              metadata_json=json.dumps(meta, default=str))
        topo = adapter.topology()
        src, dst = topo.wallet(v.source_native), topo.wallet(v.destination_native)
        dispatched = False
        try:
            dispatched = True
            outcome = adapter.submit(request_id=tid, route_code=v.route_code, source=src, destination=dst,
                                     asset=row["asset"], amount=Decimal(row["amount"]))
        except BrokerRejected as exc:
            # The broker answered and refused: nothing moved.
            self.store.transition(tid, expect=S.SUBMITTING, to=S.FAILED, event="BROKER_REJECTED",
                                  detail={"error": redact_exception(exc)[:200]}, failure_reason="BROKER_REJECTED")
            _metric(auth.broker_type, "FAILED", "BROKER_REJECTED")
            return self.store.get_any(tid)
        except Exception as exc:
            # Dispatched and no answer (timeout, connection reset, 5xx): the
            # transfer MAY have happened. Never re-submit; reconcile.
            self.store.transition(tid, expect=S.SUBMITTING, to=S.UNKNOWN, event="SUBMIT_OUTCOME_UNKNOWN",
                                  detail={"error": redact_exception(exc)[:200], "dispatched": dispatched},
                                  failure_reason="SUBMIT_OUTCOME_UNKNOWN")
            logger.warning("internal_transfer_unknown id=%s account=%s", tid, auth.account_id)
            _metric(auth.broker_type, "UNKNOWN", "SUBMIT_OUTCOME_UNKNOWN")
            return self.store.get_any(tid)
        fields: Dict[str, Any] = {"broker_transfer_id": outcome.broker_transfer_id}
        if outcome.status == S.COMPLETED:
            fields["confirmed_at"] = datetime.now(timezone.utc).isoformat()
        if outcome.status == S.FAILED:
            fields["failure_reason"] = f"BROKER_STATUS_{outcome.raw_status or 'FAILED'}"
        self.store.transition(tid, expect=S.SUBMITTING, to=outcome.status, event="SUBMITTED",
                              detail={"raw_status": outcome.raw_status, "detail": outcome.detail}, **fields)
        # safe metadata only: ids, venue, wallets, asset, state, broker reference -- never credentials/headers
        logger.info("internal_transfer_submitted id=%s account=%s venue=%s route=%s->%s asset=%s status=%s "
                    "broker_ref=%s", tid, auth.account_id, auth.broker_type, v.source_native, v.destination_native,
                    row["asset"], outcome.status.value, outcome.broker_transfer_id)
        _metric(auth.broker_type, outcome.status.value, outcome.raw_status)
        return self.store.get_any(tid)


__all__ = ["InternalTransferService", "TransferAccessError"]
