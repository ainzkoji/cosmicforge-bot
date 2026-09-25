"""Portfolio cash-flow history for a user's connected broker accounts.

Credentials come only from the canonical resolver and clients only from the
canonical factory (no reads of the legacy ``broker_credentials`` table).

Classification (never guessed from the sign of an amount):

* Binance USD-M ``/fapi/v1/income`` ``incomeType=TRANSFER`` rows are moves
  between the user's own Binance wallets (spot/funding <-> futures), i.e.
  ``INTERNAL_TRANSFER`` with direction IN/OUT -- not deposits/withdrawals.
* Bybit UTA ``transaction-log`` ``TRANSFER_IN`` / ``TRANSFER_OUT`` rows are
  likewise inter-wallet moves. External deposits/withdrawals land in the
  Funding wallet and are not reported by this log.

External DEPOSIT / WITHDRAWAL rows are only ever produced from a venue's
explicit deposit/withdrawal history, which this service does not query.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from shared_lib.broker import BrokerResolverError, build_client_from_auth, resolve_broker_auth
from shared_lib.persistence.db import DB

from app.core.broker_service import list_user_broker_accounts

logger = logging.getLogger(__name__)

INTERNAL_TRANSFER = "INTERNAL_TRANSFER"
_BYBIT_TRANSFER_TYPES = {"TRANSFER_IN": "IN", "TRANSFER_OUT": "OUT"}


def classify_binance_income(item: Dict[str, Any]) -> Dict[str, Any] | None:
    if str(item.get("incomeType", "")).upper() != "TRANSFER":
        return None
    amount = float(item.get("income", 0) or 0)
    if amount == 0:
        return None
    return {
        "type": INTERNAL_TRANSFER,
        "direction": "IN" if amount > 0 else "OUT",
        "asset": item.get("asset") or "UNKNOWN",
        "amount": abs(amount),
        "status": "SUCCESS",
        "timestamp": int(item.get("time", 0) or 0),
        "tx_id": str(item.get("tranId", "") or ""),
    }


def classify_bybit_log(item: Dict[str, Any]) -> Dict[str, Any] | None:
    direction = _BYBIT_TRANSFER_TYPES.get(str(item.get("type", "")).upper())
    if direction is None:
        return None
    amount = abs(float(item.get("change", 0) or 0))
    if amount == 0:
        return None
    return {
        "type": INTERNAL_TRANSFER,
        "direction": direction,
        "asset": item.get("currency") or item.get("coin") or "UNKNOWN",
        "amount": amount,
        "status": "SUCCESS",
        "timestamp": int(item.get("transactionTime", 0) or 0),
        "tx_id": str(item.get("id") or item.get("transactionId") or ""),
    }


def get_portfolio_transactions(user_id: str, limit: int = 50) -> Dict[str, Any]:
    accounts = list_user_broker_accounts(user_id)
    db = DB()
    rows: List[Dict[str, Any]] = []
    errors: Dict[str, str] = {}

    for acc in accounts:
        if acc.get("status") != "connected":
            continue
        broker = acc.get("broker_id")
        if broker not in ("binance", "bybit"):
            continue
        try:
            auth = resolve_broker_auth(acc["id"], user_id, db)
            client = build_client_from_auth(auth)
        except BrokerResolverError as exc:
            errors[acc["id"]] = exc.reason_code
            continue
        try:
            if broker == "binance":
                raw = client._signed_get("/fapi/v1/income", {"incomeType": "TRANSFER", "limit": limit})
                items = [classify_binance_income(i) for i in (raw if isinstance(raw, list) else [])]
                label = "Binance"
            else:
                items = [classify_bybit_log(i) for i in client.transaction_log(limit=limit)]
                label = "ByBit"
        except Exception as exc:  # broker error: report it, never invent rows
            errors[acc["id"]] = type(exc).__name__
            logger.warning("[Transactions] %s account=%s fetch failed: %s", broker, acc["id"], type(exc).__name__)
            continue
        for item in items:
            if item is not None:
                rows.append({"account_id": acc["id"], "broker": label, **item})

    rows.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
    return {"transactions": rows[:limit], "errors": errors}
