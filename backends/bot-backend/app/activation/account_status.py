"""Per-account multi-asset status (the data behind the UI/admin broker panel).

Everything is read for ONE broker account of ONE user, through the canonical
resolver (ownership enforced, 404 semantics upstream). Output fields:

Connected Broker / Account / environment; Markets Available / API-Tradable /
CATI-Eligible per family; capability states with UI status + reason; Capital
Buckets (wallet topology); Transfer capability + current (in-flight)
transfer; Logical allocation policy; Reserved (CATI portfolio reservations);
Risk state. Balances are broker-authoritative only when a live read is
requested (``include_balances``) -- a failed read is UNAVAILABLE, never 0.

No credentials, keys or secrets appear in the output.
"""
from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional

VENUE_KEY = {"binance": "binance_usdm", "bybit": "bybit_linear", "bingx": "bingx_swap"}


def _catalog_env(environment: str) -> List[str]:
    e = str(environment).upper()
    return [e, "REAL"] if e == "LIVE" else [e]


def discovered_instruments(db: Any, broker: str, environment: str) -> Optional[list]:
    """The account environment's discovered catalog, or None when never synced."""
    from app.exchange.instruments import InstrumentCatalog

    venue = VENUE_KEY.get(broker)
    if venue is None:
        return None
    try:
        cat = InstrumentCatalog(db)
        for env in _catalog_env(environment):
            rows = cat.list(venue, env, tradable_only=False)
            if rows:
                return rows
    except Exception:
        return None
    return None


def sync_discovery(db: Any, auth: Any, *, client_factory: Optional[Callable[[Any], Any]] = None,
                   now_ms: Optional[int] = None) -> Dict[str, Any]:
    """Refresh the venue catalog from the venue's official discovery API (public instrument metadata)."""
    from shared_lib.broker.client_factory import build_client_from_auth

    from app.exchange.instruments import InstrumentCatalog, sync_instruments
    from app.ops import multi_asset_metrics as mm

    broker = str(auth.broker_type).lower()
    venue = VENUE_KEY.get(broker)
    if venue is None:
        return {"status": "UNSUPPORTED", "reason": "PLATFORM_ADAPTER_NOT_IMPLEMENTED"}
    client = (client_factory or build_client_from_auth)(auth)
    try:
        counts = sync_instruments(client, catalog=InstrumentCatalog(db), venue=venue,
                                  environment=str(auth.environment.value).upper(),
                                  now_ms=int(now_ms or time.time() * 1000))
        mm.instrument_sync(venue, "OK")
        return {"status": "SYNCED", "venue": venue, **counts}
    except Exception as exc:
        mm.instrument_sync(venue, "FAILED")
        from shared_lib.core.security.redaction import redact_exception

        return {"status": "FAILED", "venue": venue, "reason": "DISCOVERY_FAILED", "detail": redact_exception(exc)}


def _health(db: Any, account_id: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    try:
        with db.connect() as conn:
            r = conn.execute("SELECT COUNT(*) FROM bot_instances WHERE broker_account_id=? AND "
                             "broker_health_status='broker_blocked'", (account_id,)).fetchone()
        out["quarantined"] = bool(r and r[0])
    except Exception:
        pass  # unknown facts are simply not asserted
    return out


def _reserved(db: Any, account_id: str) -> Dict[str, Any]:
    try:
        from app.trading_intelligence.portfolio.reservation_store import CATIReservationStore

        rows = CATIReservationStore(db).active_reservations(account_id, int(time.time() * 1000))
        return {"status": "AVAILABLE", "active_reservations": len(rows),
                "note": "CATI portfolio reservations (slots); capital reservation is the executor's ledger"}
    except Exception:
        return {"status": "UNAVAILABLE", "reason": "RESERVATION_STORE_UNAVAILABLE"}


def account_status(db: Any, *, user_id: str, account_id: str, service: Any = None,
                   include_balances: bool = False) -> Dict[str, Any]:
    from shared_lib.broker.wallets import topology_for

    from app.activation.market import account_market_status
    from app.trading_intelligence.contracts.portfolio_intel import PortfolioPolicy
    from app.transfers.service import InternalTransferService

    svc = service or InternalTransferService(db)
    auth = svc._auth(user_id, account_id)            # ownership: raises TransferAccessError for others
    ev = svc._evidence(account_id, auth.credential_version)
    perms = dict(ev.permissions) if ev is not None else None
    broker = str(auth.broker_type).lower()
    env = str(auth.environment.value).upper()
    in_flight = svc.store.in_flight(account_id)
    instruments = discovered_instruments(db, broker, env)
    status = account_market_status(broker=broker, environment=env, permissions=perms, instruments=instruments,
                                   health=_health(db, account_id), transfers_in_flight=len(in_flight), db=db,
                                   broker_account_id=account_id)
    topo = topology_for(broker)
    policy = PortfolioPolicy()
    out = {
        "broker_account_id": account_id, "broker": broker, "environment": env,
        "discovery": {"status": "SYNCED" if instruments else "DATA_NOT_READY",
                      "instruments": len(instruments or [])},
        "permission_evidence": ({"inspected": ev.inspected, "permissions": dict(ev.permissions)}
                                if ev is not None else {"inspected": False, "reason": "PERMISSION_EVIDENCE_REQUIRED"}),
        **{k: status[k] for k in ("capabilities", "markets", "withdrawal_permission_required",
                                  "withdrawals_supported_by_platform", "withdraw_permission_present")},
        "capital_buckets": topo.to_dict() if topo else {"status": "UNSUPPORTED", "reason": "TOPOLOGY_UNKNOWN"},
        "current_transfers": [{k: t.get(k) for k in ("id", "status", "asset", "amount", "source_wallet",
                                                     "destination_wallet", "created_at")} for t in in_flight],
        "logical_allocation": {"asset_class_max_positions": dict(policy.asset_class_max_positions) or "UNLIMITED",
                               "max_net_currency_units": policy.max_net_currency_units,
                               "note": "risk allocations, not wallets; hard account risk stays superior"},
        "reserved": _reserved(db, account_id),
        "risk_state": {"quarantined": _health(db, account_id).get("quarantined"),
                       "hard_daily_loss_limit": "ENFORCED_BY_RISK_ENGINE"},
    }
    if include_balances:
        try:
            out["balances"] = svc.wallets(user_id=user_id, account_id=account_id, assets=["USDT", "USDC"])
        except Exception:
            out["balances"] = {"status": "UNAVAILABLE", "reason": "BALANCE_READ_FAILED"}
    return out


__all__ = ["VENUE_KEY", "account_status", "discovered_instruments", "sync_discovery"]
