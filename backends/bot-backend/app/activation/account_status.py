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
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

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


def request_sync(db: Any, auth: Any, *, user_id: str, client_factory: Optional[Callable[[Any], Any]] = None,
                 now_ms: Optional[int] = None) -> Dict[str, Any]:
    """User-requested catalog sync (Section 20.3): the SAME Section 7 discovery, behind the SAME per-venue cooldown
    as every other refresh -- a repeated request inside ``REFRESH_MIN_INTERVAL_MS`` is THROTTLED (deterministic,
    no venue call), so no user can storm a venue. Instrument metadata only; no history is downloaded."""
    from app.trading_intelligence.hashing import short_id

    broker = str(auth.broker_type).lower()
    env = str(auth.environment.value).upper()
    venue = VENUE_KEY.get(broker)
    now = int(now_ms if now_ms is not None else time.time() * 1000)
    lineage = {"request_id": short_id("isync", {"u": user_id, "a": auth.account_id, "v": venue, "t": now}),
               "user_id": user_id, "broker_account_id": auth.account_id, "venue": venue, "environment": env,
               "requested_at_ms": now}
    if venue is None:
        return {"status": "UNSUPPORTED", "reason": "PLATFORM_ADAPTER_NOT_IMPLEMENTED", "lineage": lineage}
    with _refresh_guard:
        last_try = _refresh_attempts.get((venue, env))
        if last_try is not None and now - last_try < REFRESH_MIN_INTERVAL_MS:
            logger.info("[CATALOG_SYNC] throttled request=%s account=%s venue=%s", lineage["request_id"],
                        auth.account_id, venue)
            return {"status": "THROTTLED", "reason": "SYNC_COOLDOWN", "venue": venue,
                    "retry_after_ms": REFRESH_MIN_INTERVAL_MS - (now - last_try), "lineage": lineage}
        _refresh_attempts[(venue, env)] = now
    out = sync_discovery(db, auth, client_factory=client_factory, now_ms=now)
    logger.info("[CATALOG_SYNC] request=%s account=%s venue=%s status=%s", lineage["request_id"], auth.account_id,
                venue, out.get("status"))
    return {**out, "lineage": lineage}


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
        from shared_lib.core.production import production_enabled
        if production_enabled() and e != "LIVE":
            continue
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


def topology_view(broker: str, account_mode: Optional[str], *, internal_transfer: Mapping[str, Any],
                  observed_at_ms: Optional[int]) -> Dict[str, Any]:
    """UNIFIED / SEGMENTED / UNKNOWN / UNSUPPORTED with the broker-native mode, wallets and routes (Section 20.5).
    Route availability follows THIS account's INTERNAL_TRANSFER capability; route fees are never assumed."""
    from shared_lib.broker.wallets import ACCOUNT_MODE_DEPENDENT, topology_class, topology_for, topology_for_account

    topo = topology_for_account(broker, account_mode)
    cls = topo.topology_class.value if topo is not None else topology_class(broker, account_mode).value
    available = internal_transfer.get("state") == "ACTIVE"
    out: Dict[str, Any] = {
        "class": cls, "account_mode": getattr(topo, "account_mode", None),
        "account_mode_source": ("BROKER" if broker in ACCOUNT_MODE_DEPENDENT else "SINGLE_ACCOUNT_MODEL")
        if topo is not None else None,
        "observed_at_ms": observed_at_ms if topo is not None and broker in ACCOUNT_MODE_DEPENDENT else None,
        "reason_code": None if topo is not None else ("ACCOUNT_TOPOLOGY_UNKNOWN" if topology_for(broker) is not None
                                                      else "TOPOLOGY_UNSUPPORTED"),
        "wallets": [w.to_dict() for w in topo.wallets] if topo is not None else [],
        "routes": [{**r, "available": available,
                    "reason_code": None if available else (internal_transfer.get("reason")
                                                           or "INTERNAL_TRANSFER_UNAVAILABLE")}
                   for r in (topo.to_dict()["routes"] if topo is not None else [])],
    }
    if cls == "UNIFIED":
        out["logical_allocation"] = ("one broker collateral pool: per-market allocations are POLICY constraints over "
                                     "it, not separate broker wallets, and moving between them moves no money")
    return out


def _instrument_readiness(item: Dict[str, Any], *, broker: str) -> Dict[str, Any]:
    """Four independent states (Section 20.4 / 21.8): market available != research ready != certification ready
    != execution authorized. Discovery alone never makes an instrument trade-ready."""
    from app.market_data import research_status as rs

    dims = item["dimensions"]
    venue, ac = item["venue"], item["asset_class"]
    research = {"state": "RESEARCH_ONLY", "reason": "INSTRUMENT_RESEARCH_ONLY"}
    if ac == "CRYPTO" and venue == "binance_usdm":
        members = rs.research_members("CRYPTO_BROAD")
        if members is not None and item["venue_symbol"] in members:
            research = {"state": "RESEARCH_READY", "reason": None, "dataset": "CRYPTO_BROAD"}
    elif ac == "FX":
        members = rs.research_members("FX_REFERENCE")
        legs = str(item["canonical_symbol"]).split(":")[0].replace("/", "")
        if members is not None and legs in members:
            acq = rs._cached("fx_acq", lambda: rs.fx_acquisition(rs.load_manifest("FX_REFERENCE")["manifest"]))
            research = ({"state": "RESEARCH_READY", "reason": None, "dataset": "FX_REFERENCE"}
                        if acq.get("state") == "COMPLETE" else
                        {"state": "DATASET_ACQUIRING", "reason": "DATASET_ACQUIRING", "dataset": "FX_REFERENCE"})
    cert = dims["CERTIFICATION_READY"]
    gov = dims["GOVERNANCE_AUTHORISED"]
    executable = item["lifecycle_stage"] == "CATI_EXECUTABLE"
    return {
        "market_available": {"state": dims["MARKET_EXISTS"]["state"] == "YES", "reason": dims["MARKET_EXISTS"]["reason"]},
        "research": research,
        "certification": {"state": "CERTIFICATION_READY" if cert["state"] == "YES" else "NOT_CERTIFIED",
                          "reason": None if cert["state"] == "YES" else "CERTIFICATION_NOT_READY"},
        "execution_authorized": {"state": executable,
                                 "reason": None if executable else ((item["next_blocker"] or {}).get("reason")
                                                                    or gov["reason"])},
    }


INSTRUMENT_FILTERS = ("product_type", "lifecycle_stage", "api_tradable", "research_state", "execution_authorized")


def _filter_instruments(rows: List[Dict[str, Any]], filters: Mapping[str, Any]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        if filters.get("product_type") and r["product_type"] != str(filters["product_type"]).upper():
            continue
        if filters.get("lifecycle_stage") and r["lifecycle_stage"] != str(filters["lifecycle_stage"]).upper():
            continue
        if filters.get("api_tradable") is not None and (
                (r["dimensions"]["API_EXECUTION_SUPPORTED"]["state"] == "YES") != bool(filters["api_tradable"])):
            continue
        if filters.get("research_state") and r["readiness"]["research"]["state"] != str(
                filters["research_state"]).upper():
            continue
        if filters.get("execution_authorized") is not None and (
                r["readiness"]["execution_authorized"]["state"] != bool(filters["execution_authorized"])):
            continue
        out.append(r)
    return out


def account_status(db: Any, *, user_id: str, account_id: str, service: Any = None,
                   include_balances: bool = False, refresh_stale: bool = False,
                   instruments_family: Optional[str] = None,
                   client_factory: Optional[Callable[[Any], Any]] = None,
                   instrument_filters: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
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
    now_ms = int(time.time() * 1000)
    out = {
        "broker_account_id": account_id, "broker": broker, "environment": env, "observed_at_ms": now_ms,
        "topology": topology_view(broker, account_mode, internal_transfer=status["capabilities"].get(
            "INTERNAL_TRANSFER", {}), observed_at_ms=now_ms if include_balances else None),
        "discovery": {**freshness, "instruments": len(instruments or []),
                      **({"refresh": refresh} if refresh is not None else {})},
        # Section 25.5 least privilege: read + trade (+ internal transfer only for physical routes); an IP allowlist
        # is operator GUIDANCE where the venue supports it, never a requirement the platform imposes
        "least_privilege": {"recommended_permissions": ["READ", "TRADE", "INTERNAL_TRANSFER (only for physical routes)"],
                            "withdrawal": "NEVER_REQUIRED",
                            "ip_allowlist": {"state": getattr(ev, "ip_restricted", None) if ev is not None else None,
                                             "guidance": "restrict the API key to this platform's egress IP where the "
                                                         "venue supports it"}},
        "permission_evidence": ({"inspected": ev.inspected, "permissions": dict(ev.permissions),
                                 "observed_at_ms": getattr(ev, "probed_at_ms", None)}
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
        rows = []
        for i in (instruments or []):
            if i.asset_class != fam:
                continue
            item = instrument_capabilities(i, broker=broker, environment=env, permissions=perms, cati_decision=cati)
            item["readiness"] = _instrument_readiness(item, broker=broker)
            rows.append(item)
        out["instruments"] = _filter_instruments(rows, instrument_filters or {})
    return out


__all__ = ["DISCOVERY_MAX_AGE_MS", "INSTRUMENT_FILTERS", "REFRESH_MIN_INTERVAL_MS", "VENUE_KEY", "account_status",
           "discovered_instruments", "request_sync", "topology_view",
           "discovery_freshness", "discovery_refresh_loop", "refresh_due", "refresh_due_catalogs", "refresh_if_stale",
           "sync_discovery"]
