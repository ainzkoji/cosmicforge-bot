"""Per-account multi-asset status (the data behind the UI/admin broker panel).

Everything is read for ONE broker account of ONE user, through the canonical
resolver (ownership enforced, 404 semantics upstream). Output fields:

Connected Broker / Account / environment; Markets Available / API-Tradable /
CATI-Eligible per family; capability states with UI status + reason; Capital
Buckets (wallet topology); Transfer capability + current (in-flight)
transfer; Logical allocation policy; Reserved (CATI portfolio reservations);
Risk state. Balances and the account mode (wallet topology) are
broker-authoritative only when a live read is requested (``include_balances``)
-- a failed read is UNAVAILABLE, never 0, and an unread Bybit account mode is
ACCOUNT_TOPOLOGY_UNKNOWN, never assumed unified.

Discovery freshness (Section 7.11): the venue catalog is refreshed from the
venue's public discovery API when it is missing, older than
``DISCOVERY_MAX_AGE_MS``, or an event requested it since the last sync
(``app.exchange.catalog_refresh``: an instrument-related venue API error, an
account-mode change) -- on market-status reads (``refresh_stale``) and in the
background ``discovery_refresh_loop`` -- at most once per
``REFRESH_MIN_INTERVAL_MS`` per venue/environment (no call storms). A catalog
that stays stale blocks execution capability (DISCOVERY_STALE); a pending
event request only makes the refresh happen sooner, it changes no decision.

No credentials, keys or secrets appear in the output: permission health is
abstract (VERIFIED / MISSING / UNVERIFIED).
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

VENUE_KEY = {"binance": "binance_usdm", "bybit": "bybit_linear", "bingx": "bingx_swap"}
DISCOVERY_MAX_AGE_MS = 6 * 3_600_000
REFRESH_MIN_INTERVAL_MS = 5 * 60_000
_refresh_attempts: Dict[Tuple[str, str], int] = {}
_refresh_guard = threading.Lock()
logger = logging.getLogger(__name__)


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


def discovery_freshness(db: Any, broker: str, environment: str, *, now_ms: Optional[int] = None) -> Dict[str, Any]:
    """SYNCED / STALE / DATA_NOT_READY for the account environment's catalog (never raises)."""
    from app.exchange.instruments import InstrumentCatalog

    venue = VENUE_KEY.get(broker)
    last = None
    if venue is not None:
        try:
            cat = InstrumentCatalog(db)
            for env in _catalog_env(environment):
                last = cat.last_synced_ms(venue, env)
                if last is not None:
                    break
        except Exception:
            last = None
    now = int(now_ms if now_ms is not None else time.time() * 1000)
    if last is None:
        return {"status": "DATA_NOT_READY", "last_synced_ms": None, "age_ms": None, "fresh": None}
    age = now - last
    fresh = age <= DISCOVERY_MAX_AGE_MS
    return {"status": "SYNCED" if fresh else "STALE", "last_synced_ms": last, "age_ms": age, "fresh": fresh,
            "max_age_ms": DISCOVERY_MAX_AGE_MS}


def refresh_if_stale(db: Any, auth: Any, *, client_factory: Optional[Callable[[Any], Any]] = None,
                     now_ms: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """Refresh the catalog when missing/stale, throttled per venue+environment. None = nothing attempted."""
    broker = str(auth.broker_type).lower()
    env = str(auth.environment.value).upper()
    venue = VENUE_KEY.get(broker)
    if venue is None:
        return None
    now = int(now_ms if now_ms is not None else time.time() * 1000)
    if not refresh_due(db, broker, env, now_ms=now):
        return None
    with _refresh_guard:
        last_try = _refresh_attempts.get((venue, env))
        if last_try is not None and now - last_try < REFRESH_MIN_INTERVAL_MS:
            return {"status": "THROTTLED", "venue": venue}
        _refresh_attempts[(venue, env)] = now
    return sync_discovery(db, auth, client_factory=client_factory, now_ms=now)


def refresh_due(db: Any, broker: str, environment: str, *, now_ms: Optional[int] = None) -> bool:
    """Missing / stale catalog, or an event requested a refresh after the last sync."""
    from app.exchange import catalog_refresh

    fresh = discovery_freshness(db, broker, environment, now_ms=now_ms)
    if fresh["status"] != "SYNCED":
        return True
    req = catalog_refresh.pending(VENUE_KEY.get(broker, ""), environment)
    return bool(req and req["requested_ms"] > (fresh["last_synced_ms"] or 0))


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
    env = str(auth.environment.value).upper()
    synced_ms = int(now_ms or time.time() * 1000)
    try:
        counts = sync_instruments(client, catalog=InstrumentCatalog(db), venue=venue, environment=env,
                                  now_ms=synced_ms)
        mm.instrument_sync(venue, "OK")
        from app.exchange import catalog_refresh

        catalog_refresh.clear(venue, env, synced_ms=synced_ms)
        return {"status": "SYNCED", "venue": venue, **counts}
    except Exception as exc:
        mm.instrument_sync(venue, "FAILED")
        from shared_lib.core.security.redaction import redact_exception

        return {"status": "FAILED", "venue": venue, "reason": "DISCOVERY_FAILED", "detail": redact_exception(exc)}


def _connected_pairs(db: Any) -> Dict[tuple, List[tuple]]:
    """(broker, ENV) -> [(account_id, user_id), ...] of connected accounts (most recently updated first)."""
    from shared_lib.broker.environment import normalize_environment

    out: Dict[tuple, List[tuple]] = {}
    with db.connect() as conn:
        rows = conn.execute("SELECT id, user_id, broker_id, environment FROM broker_accounts "
                            "ORDER BY updated_at DESC").fetchall()
    for acc, user, broker, env in rows:
        b = str(broker or "").lower()
        if b not in VENUE_KEY:
            continue
        try:
            e = normalize_environment(env).value.upper()
        except Exception:
            continue  # an environment we cannot name is never guessed
        out.setdefault((b, e), []).append((acc, user))
    return out


def refresh_due_catalogs(db: Any, *, resolver: Optional[Callable[..., Any]] = None,
                         client_factory: Optional[Callable[[Any], Any]] = None,
                         now_ms: Optional[int] = None, max_candidates: int = 5) -> Dict[str, Any]:
    """One background pass: refresh every venue/environment catalog that has a connected account AND is
    missing, stale or event-requested. Venue-global public metadata only; nothing account-scoped is read or
    written. A catalog that is current costs no venue call and no credential resolution."""
    from shared_lib.broker import BrokerResolverError, resolve_broker_auth

    resolve = resolver or resolve_broker_auth
    now = int(now_ms if now_ms is not None else time.time() * 1000)
    results: Dict[str, Any] = {}
    for (broker, env), candidates in sorted(_connected_pairs(db).items()):
        key = f"{VENUE_KEY[broker]}:{env}"
        if not refresh_due(db, broker, env, now_ms=now):
            results[key] = {"status": "CURRENT"}
            continue
        auth = None
        for acc, user in candidates[:max_candidates]:
            try:
                auth = resolve(acc, user, db)
                break
            except BrokerResolverError:
                continue
        if auth is None:
            results[key] = {"status": "NO_USABLE_ACCOUNT"}
            continue
        try:
            results[key] = refresh_if_stale(db, auth, client_factory=client_factory, now_ms=now) or {"status": "CURRENT"}
        except Exception:
            results[key] = {"status": "FAILED", "reason": "DISCOVERY_FAILED"}
    return results


async def discovery_refresh_loop(db: Any, *, interval_s: float = 300.0) -> None:
    """Background worker (Section 7.11): periodic + event-requested venue catalog refresh."""
    import asyncio

    while True:
        try:
            summary = await asyncio.to_thread(refresh_due_catalogs, db)
            done = {k: v.get("status") for k, v in summary.items() if v.get("status") != "CURRENT"}
            if done:
                logger.info("[CATALOG_REFRESH] pass %s", done)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            from shared_lib.core.security.redaction import redact_exception

            logger.error("[CATALOG_REFRESH] loop error=%s", redact_exception(exc))
        await asyncio.sleep(interval_s)


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


def _account_mode(svc: Any, auth: Any) -> Optional[str]:
    """The account mode READ from the broker (None when not readable -- never guessed)."""
    try:
        adapter = svc._adapter_factory(auth)
        return adapter.account_mode() if adapter is not None else None
    except Exception:
        return None


def account_status(db: Any, *, user_id: str, account_id: str, service: Any = None,
                   include_balances: bool = False, refresh_stale: bool = False,
                   instruments_family: Optional[str] = None,
                   client_factory: Optional[Callable[[Any], Any]] = None) -> Dict[str, Any]:
    from shared_lib.broker.wallets import ACCOUNT_MODE_DEPENDENT, topology_class, topology_for, topology_for_account

    from app.activation.market import FAMILIES, account_market_status, instrument_capabilities
    from app.trading_intelligence.contracts.portfolio_intel import PortfolioPolicy
    from app.transfers.service import InternalTransferService

    svc = service or InternalTransferService(db)
    auth = svc._auth(user_id, account_id)            # ownership: raises TransferAccessError for others
    ev = svc._evidence(account_id, auth.credential_version)
    perms = dict(ev.permissions) if ev is not None else None
    broker = str(auth.broker_type).lower()
    env = str(auth.environment.value).upper()
    in_flight = svc.store.in_flight(account_id)
    refresh = None
    if refresh_stale:
        try:
            refresh = refresh_if_stale(db, auth, client_factory=client_factory)
        except Exception:
            refresh = {"status": "FAILED", "reason": "DISCOVERY_FAILED"}
    freshness = discovery_freshness(db, broker, env)
    instruments = discovered_instruments(db, broker, env)
    account_mode = _account_mode(svc, auth) if include_balances else None
    status = account_market_status(broker=broker, environment=env, permissions=perms, instruments=instruments,
                                   account_mode=account_mode, health=_health(db, account_id),
                                   transfers_in_flight=len(in_flight), db=db, broker_account_id=account_id,
                                   discovery_fresh=freshness["fresh"] if instruments else None)
    topo = topology_for_account(broker, account_mode)
    policy = PortfolioPolicy()
    if topo is not None:
        buckets = {**topo.to_dict(), "account_mode_source": ("BROKER" if broker in ACCOUNT_MODE_DEPENDENT
                                                             else "SINGLE_ACCOUNT_MODEL")}
    elif topology_for(broker) is not None:
        buckets = {"status": "BLOCKED", "reason": "ACCOUNT_TOPOLOGY_UNKNOWN",
                   "topology_class": topology_class(broker, None).value,
                   "detail": "account mode not read from the broker; request include_balances=true"}
    else:
        buckets = {"status": "UNSUPPORTED", "reason": "TOPOLOGY_UNKNOWN", "topology_class": "UNSUPPORTED"}
    out = {
        "broker_account_id": account_id, "broker": broker, "environment": env,
        "discovery": {**freshness, "instruments": len(instruments or []),
                      **({"refresh": refresh} if refresh is not None else {})},
        "permission_evidence": ({"inspected": ev.inspected, "permissions": dict(ev.permissions)}
                                if ev is not None else {"inspected": False, "reason": "PERMISSION_EVIDENCE_REQUIRED"}),
        **{k: status[k] for k in ("capabilities", "markets", "withdrawal_permission_required",
                                  "withdrawals_supported_by_platform", "withdraw_permission_present",
                                  "permission_health", "topology_class")},
        "capital_buckets": buckets,
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
    fam = str(instruments_family or "").upper()
    if fam in FAMILIES:
        from app.activation.market import _cati_eligible

        cati = _cati_eligible(fam, broker, env, db, account_id, ())
        out["instruments"] = [instrument_capabilities(i, broker=broker, environment=env, permissions=perms,
                                                      cati_decision=cati)
                              for i in (instruments or []) if i.asset_class == fam]
    return out


__all__ = ["DISCOVERY_MAX_AGE_MS", "REFRESH_MIN_INTERVAL_MS", "VENUE_KEY", "account_status", "discovered_instruments",
           "discovery_freshness", "discovery_refresh_loop", "refresh_due", "refresh_due_catalogs", "refresh_if_stale",
           "sync_discovery"]
