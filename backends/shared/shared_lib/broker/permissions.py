"""API-key permission evidence and the trading-activation decision.

Every connected credential version carries PERMISSION EVIDENCE: what the
broker itself reported about the key, normalised to one vocabulary.

    READ_ACCOUNT  READ_POSITIONS  READ_ORDERS  TRADE  INTERNAL_TRANSFER  WITHDRAW

Each permission is ``True`` (broker says granted), ``False`` (broker says
not granted) or ``None`` (the broker was not asked / cannot be asked).

Decision (``evaluate_trading_permissions``):

* ``WITHDRAW is True``  -> REJECTED_WITHDRAW_PERMISSION. The platform never
  needs withdrawal permission; a withdrawal-capable key is never used for
  automated trading. The user must create a key without it.
* ``TRADE is False`` or a required read permission ``False`` ->
  REJECTED_MISSING_TRADING_PERMISSION.
* permission inspection unavailable for this broker/environment ->
  ACCEPTED_UNVERIFIED: trading may proceed (the broker still enforces the
  key), but anything that MOVES money (internal transfers) requires
  positive evidence and is refused.
* otherwise ACCEPTED.

Normalisers are pure functions over the broker's documented payloads so they
are testable without network access.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, Mapping, Optional


class Permission(str, Enum):
    READ_ACCOUNT = "READ_ACCOUNT"
    READ_POSITIONS = "READ_POSITIONS"
    READ_ORDERS = "READ_ORDERS"
    TRADE = "TRADE"
    INTERNAL_TRANSFER = "INTERNAL_TRANSFER"
    WITHDRAW = "WITHDRAW"


PERMISSIONS = tuple(p.value for p in Permission)
REQUIRED_FOR_TRADING = (Permission.READ_ACCOUNT, Permission.READ_POSITIONS, Permission.READ_ORDERS, Permission.TRADE)


class PermissionDecision(str, Enum):
    ACCEPTED = "ACCEPTED"
    ACCEPTED_UNVERIFIED = "ACCEPTED_UNVERIFIED"
    REJECTED_WITHDRAW_PERMISSION = "REJECTED_WITHDRAW_PERMISSION"
    REJECTED_MISSING_TRADING_PERMISSION = "REJECTED_MISSING_TRADING_PERMISSION"


WITHDRAW_REJECTION_MESSAGE = (
    "This API key has WITHDRAWAL permission. CosmicForge never needs it and will not trade with a "
    "withdrawal-capable key. Create a new API key with withdrawals DISABLED and reconnect."
)


@dataclass(frozen=True)
class PermissionEvidence:
    broker: str
    permissions: Mapping[str, Optional[bool]]
    source: str                       # e.g. "binance:/sapi/v1/account/apiRestrictions"
    inspected: bool                   # False = broker could not be asked
    probed_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    ip_restricted: Optional[bool] = None
    notes: tuple = ()

    def get(self, perm: Permission) -> Optional[bool]:
        return self.permissions.get(perm.value)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "broker": self.broker,
            "permissions": {p: self.permissions.get(p) for p in PERMISSIONS},
            "source": self.source,
            "inspected": self.inspected,
            "probed_at_ms": self.probed_at_ms,
            "ip_restricted": self.ip_restricted,
            "notes": list(self.notes),
            "decision": evaluate_trading_permissions(self)[0].value,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PermissionEvidence":
        return cls(broker=str(data.get("broker") or ""), permissions=dict(data.get("permissions") or {}),
                   source=str(data.get("source") or ""), inspected=bool(data.get("inspected")),
                   probed_at_ms=int(data.get("probed_at_ms") or 0), ip_restricted=data.get("ip_restricted"),
                   notes=tuple(data.get("notes") or ()))


def evaluate_trading_permissions(ev: PermissionEvidence) -> tuple[PermissionDecision, str]:
    if ev.get(Permission.WITHDRAW) is True:
        return PermissionDecision.REJECTED_WITHDRAW_PERMISSION, WITHDRAW_REJECTION_MESSAGE
    missing = [p.value for p in REQUIRED_FOR_TRADING if ev.get(p) is False]
    if missing:
        return (PermissionDecision.REJECTED_MISSING_TRADING_PERMISSION,
                f"API key lacks required permission(s): {', '.join(missing)}")
    if not ev.inspected or any(ev.get(p) is None for p in REQUIRED_FOR_TRADING) or ev.get(Permission.WITHDRAW) is None:
        return PermissionDecision.ACCEPTED_UNVERIFIED, "permissions could not be fully verified with the broker"
    return PermissionDecision.ACCEPTED, "ok"


def is_trading_permitted(ev: Optional[PermissionEvidence]) -> bool:
    if ev is None:
        return True  # legacy credential without evidence: the broker still enforces the key
    return evaluate_trading_permissions(ev)[0] in (PermissionDecision.ACCEPTED, PermissionDecision.ACCEPTED_UNVERIFIED)


def is_internal_transfer_permitted(ev: Optional[PermissionEvidence]) -> tuple[bool, str]:
    """Moving money needs POSITIVE evidence: inspected, transfer granted, withdraw denied."""
    if ev is None or not ev.inspected:
        return False, "PERMISSION_EVIDENCE_REQUIRED"
    if ev.get(Permission.WITHDRAW) is not False:
        return False, ("WITHDRAW_PERMISSION_PRESENT" if ev.get(Permission.WITHDRAW) else "WITHDRAW_PERMISSION_UNVERIFIED")
    if ev.get(Permission.INTERNAL_TRANSFER) is not True:
        return False, "INTERNAL_TRANSFER_PERMISSION_MISSING"
    return True, "ok"


def unverified(broker: str, source: str, *, read_account: Optional[bool] = None, note: str = "") -> PermissionEvidence:
    perms: Dict[str, Optional[bool]] = {p: None for p in PERMISSIONS}
    perms[Permission.READ_ACCOUNT.value] = read_account
    return PermissionEvidence(broker=broker, permissions=perms, source=source, inspected=False,
                              notes=(note,) if note else ())


# ── Normalisers ──────────────────────────────────────────────────────────────

def normalize_binance_api_restrictions(payload: Mapping[str, Any]) -> PermissionEvidence:
    """``GET /sapi/v1/account/apiRestrictions``.

    enableReading -> READ_*; enableFutures -> TRADE (USD-M futures);
    permitsUniversalTransfer -> INTERNAL_TRANSFER (wallet <-> futures);
    enableWithdrawals -> WITHDRAW. ``enableInternalTransfer`` is transfer
    between master/sub accounts, which this platform does not use.
    """
    reading = _bool(payload.get("enableReading"))
    perms = {
        Permission.READ_ACCOUNT.value: reading,
        Permission.READ_POSITIONS.value: reading,
        Permission.READ_ORDERS.value: reading,
        Permission.TRADE.value: _bool(payload.get("enableFutures")),
        Permission.INTERNAL_TRANSFER.value: _bool(payload.get("permitsUniversalTransfer")),
        Permission.WITHDRAW.value: _bool(payload.get("enableWithdrawals")),
    }
    return PermissionEvidence(broker="binance", permissions=perms, source="binance:/sapi/v1/account/apiRestrictions",
                              inspected=True, ip_restricted=_bool(payload.get("ipRestrict")))


def normalize_bybit_query_api(result: Mapping[str, Any]) -> PermissionEvidence:
    """``GET /v5/user/query-api`` ``result``.

    readOnly=1 means no trading at all. ``permissions`` groups:
    ContractTrade [Order, Position], Derivatives [DerivativesTrade],
    Wallet [AccountTransfer, SubMemberTransfer, Withdraw], ...
    """
    groups = result.get("permissions") or {}

    def has(group: str, item: str) -> bool:
        vals = groups.get(group) or []
        return item in vals if isinstance(vals, list) else False

    read_only = str(result.get("readOnly", "0")) in ("1", "true", "True")
    trade = (not read_only) and (has("ContractTrade", "Order") or has("Derivatives", "DerivativesTrade"))
    perms = {
        # Any valid key can read its own account/positions/orders on V5.
        Permission.READ_ACCOUNT.value: True,
        Permission.READ_POSITIONS.value: True,
        Permission.READ_ORDERS.value: True,
        Permission.TRADE.value: trade,
        Permission.INTERNAL_TRANSFER.value: (not read_only) and has("Wallet", "AccountTransfer"),
        Permission.WITHDRAW.value: has("Wallet", "Withdraw"),
    }
    ips = result.get("ips")
    ip_restricted = None if ips is None else not (ips == ["*"] or ips == "*" or ips == [])
    return PermissionEvidence(broker="bybit", permissions=perms, source="bybit:/v5/user/query-api", inspected=True,
                              ip_restricted=ip_restricted)


def _bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None


def permissions_payload(ev: PermissionEvidence) -> Dict[str, Any]:
    """The JSON persisted on broker_credentials_v2.permissions_json (no secrets)."""
    return ev.to_dict()


def load_evidence(raw_json: Optional[str]) -> Optional[PermissionEvidence]:
    import json

    if not raw_json:
        return None
    try:
        data = json.loads(raw_json)
    except (TypeError, ValueError):
        return None
    return PermissionEvidence.from_dict(data) if isinstance(data, dict) else None


def summarize(perms: Iterable[str]) -> str:
    return ",".join(sorted(perms))


__all__ = [
    "Permission", "PERMISSIONS", "PermissionDecision", "PermissionEvidence", "REQUIRED_FOR_TRADING",
    "WITHDRAW_REJECTION_MESSAGE", "evaluate_trading_permissions", "is_internal_transfer_permitted",
    "is_trading_permitted", "load_evidence", "normalize_binance_api_restrictions", "normalize_bybit_query_api",
    "permissions_payload", "unverified",
]
