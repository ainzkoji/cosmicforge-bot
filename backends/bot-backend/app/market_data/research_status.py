"""Research dataset / certification STATUS for operators (Sections 20.9-20.11, 21.9, 23, 27).

Read-only, derived from recorded evidence -- never an authority:

* the three frozen universe manifests in ``docs/research`` (re-verified on every load: a tampered file is
  reported INVALID, never trusted);
* the committed broad-crypto coverage artifact (``docs/research/coverage``);
* bounded aggregate queries on the research databases, opened READ-ONLY (``mode=ro``: WAL readers never block
  or corrupt the running acquisition writers). The "remaining" arithmetic mirrors the acquisition scripts' own
  ``--plan`` logic (FX: pairs x non-Saturday days in the manifest window, a period done when every logged side is
  FETCHED / NO_FILE / EMPTY; crypto deep: members x months from requested_start, done in FETCHED / EMPTY /
  NOT_LISTED / UNAVAILABLE).

Holdout state, certification state, governance phase and execution authority are separate fields: nothing here
collapses them into one "ready" flag, and nothing here can open a holdout or change any state. Paths are fixed
repository locations (optionally a data directory from ``CATI_RESEARCH_DATA_DIR``); no caller-supplied path,
URL or command is ever used.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

RESEARCH_STATUS_VERSION = "cati-research-status-v1"
REPO_ROOT = Path(__file__).resolve().parents[4]
MANIFESTS = {"CRYPTO_BROAD": "cati_crypto_universe_binance_v1.json",
             "CRYPTO_DEEP": "cati_crypto_deep_universe_binance_v1.json",
             "FX_REFERENCE": "cati_fx_universe_dukascopy_v1.json"}
DATABASES = {"CRYPTO_DEEP": "crypto_deep_binance.db", "FX_REFERENCE": "fx_reference_dukascopy.db"}
BROAD_COVERAGE = "coverage/crypto_broad_binance_v1.coverage.json"
FX_DONE = ("FETCHED", "NO_FILE", "EMPTY")
CRYPTO_DONE = ("FETCHED", "EMPTY", "NOT_LISTED", "UNAVAILABLE")
CACHE_TTL_S = 120.0
_cache: Dict[str, Tuple[float, Any]] = {}
_lock = threading.Lock()


def docs_dir() -> Path:
    return REPO_ROOT / "docs" / "research"


def data_dir() -> Path:
    return Path(os.environ.get("CATI_RESEARCH_DATA_DIR") or (REPO_ROOT / "data" / "research"))


def _ro(path: Path) -> Optional[sqlite3.Connection]:
    if not path.exists():
        return None
    conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=5)
    conn.execute("PRAGMA query_only=1")
    return conn


def _cached(key: str, fn):
    now = time.monotonic()
    with _lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < CACHE_TTL_S:
            return hit[1]
    value = fn()
    with _lock:
        _cache[key] = (now, value)
    return value


def reset_cache() -> None:
    with _lock:
        _cache.clear()


# -- frozen manifests -------------------------------------------------------------------------------------------
def load_manifest(name: str) -> Dict[str, Any]:
    """The frozen manifest, re-verified. {"status": FROZEN|INVALID|MISSING, "manifest": ..., "reason": ...}."""
    path = docs_dir() / MANIFESTS[name]
    if not path.exists():
        return {"status": "MISSING", "reason": "MANIFEST_NOT_FOUND", "manifest": None}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        from app.market_data import universe as U

        if data.get("schema_version") == U.FROZEN_UNIVERSE_SCHEMA_VERSION:
            U.verify_frozen_universe(data)
        elif data.get("schema_version") == U.DEEP_UNIVERSE_SCHEMA_VERSION:
            U.verify_deep_universe(data)
        else:
            from app.market_data.fx_universe import verify_fx_universe

            verify_fx_universe(data)
        return {"status": "FROZEN", "reason": None, "manifest": data}
    except Exception as exc:
        return {"status": "INVALID", "reason": f"MANIFEST_VERIFICATION_FAILED:{type(exc).__name__}", "manifest": None}


def _members(name: str, m: Mapping[str, Any]) -> List[str]:
    if name == "CRYPTO_BROAD":
        return list(m.get("selected_symbols") or ())
    if name == "CRYPTO_DEEP":
        return [x["venue_symbol"] for x in m.get("members") or ()]
    return [x["pair"] for x in m.get("members") or ()]


def research_members(name: str) -> Optional[frozenset]:
    """Instrument membership of a VERIFIED frozen universe (None when not verifiable)."""
    res = _cached(f"members:{name}", lambda: load_manifest(name))
    return frozenset(_members(name, res["manifest"])) if res["status"] == "FROZEN" else None


# -- acquisition state (bounded, read-only) ---------------------------------------------------------------------
def _fx_days(m: Mapping[str, Any]) -> List[str]:
    start = datetime.fromtimestamp(m["window_start_ms"] / 1000, timezone.utc).date()
    end = (datetime.fromtimestamp(m["window_end_ms"] / 1000, timezone.utc) - timedelta(days=1)).date()
    out, d = [], start
    while d <= end:
        if d.weekday() != 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def fx_acquisition(manifest: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if manifest is None:
        return {"state": "UNAVAILABLE", "reason": "FX_MANIFEST_NOT_VERIFIED"}
    conn = _ro(data_dir() / DATABASES["FX_REFERENCE"])
    if conn is None:
        return {"state": "UNAVAILABLE", "reason": "FX_RESEARCH_DATABASE_NOT_PRESENT"}
    try:
        pairs = _members("FX_REFERENCE", manifest)
        days = _fx_days(manifest)
        lo, hi = days[0], days[-1]
        marks = ",".join("?" for _ in FX_DONE)
        done = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT pair, period FROM fx_reference_ingest_log WHERE provider=? AND "
            f"timeframe='1m' AND period>=? AND period<=? GROUP BY pair, period "
            f"HAVING MIN(status IN ({marks}))=1)", ("dukascopy", lo, hi, *FX_DONE)).fetchone()[0]
        failed = conn.execute("SELECT COUNT(DISTINCT pair || period) FROM fx_reference_ingest_log WHERE provider=? "
                              "AND timeframe='1m' AND status='FAILED'", ("dukascopy",)).fetchone()[0]
        last = conn.execute("SELECT MAX(recorded_at) FROM fx_reference_ingest_log WHERE timeframe='1m'").fetchone()[0]
        hourly = dict(conn.execute("SELECT status, COUNT(*) FROM fx_reference_ingest_log WHERE provider=? AND "
                                   "timeframe='1h' GROUP BY status", ("dukascopy",)).fetchall())
        derived = {}
        for tf in ("5m", "15m", "4h"):
            present = sum(1 for p in pairs if conn.execute(
                "SELECT 1 FROM fx_reference_quotes WHERE pair=? AND timeframe=? LIMIT 1", (p, tf)).fetchone())
            derived[tf] = {"pairs_present": present, "pairs": len(pairs),
                           "state": "NOT_STARTED" if present == 0 else ("PRESENT" if present == len(pairs) else "PARTIAL")}
    finally:
        conn.close()
    expected = len(pairs) * len(days)
    state = "COMPLETE" if done == expected and not failed else ("NOT_STARTED" if done == 0 else "ACQUIRING")
    return {"state": state, "reason": None if state == "COMPLETE" else "DATASET_ACQUIRING",
            "1m": {"expected_periods": expected, "done_periods": done, "remaining_periods": expected - done,
                   "failed_retryable_periods": failed, "last_recorded_at_ms": last,
                   "unit": "pair x UTC market day (Saturdays excluded)"},
            "1h": {"by_status": dict(sorted(hourly.items()))}, "derived": derived}


def _months(start_ms: int, end_ms: int) -> Iterable[str]:
    d = datetime.fromtimestamp(start_ms / 1000, timezone.utc)
    y, m = d.year, d.month
    while int(datetime(y, m, 1, tzinfo=timezone.utc).timestamp() * 1000) < end_ms:
        yield f"{y}-{m:02d}"
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def crypto_deep_acquisition(manifest: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if manifest is None:
        return {"state": "UNAVAILABLE", "reason": "CRYPTO_DEEP_MANIFEST_NOT_VERIFIED"}
    conn = _ro(data_dir() / DATABASES["CRYPTO_DEEP"])
    if conn is None:
        return {"state": "UNAVAILABLE", "reason": "CRYPTO_DEEP_DATABASE_NOT_PRESENT"}
    try:
        status: Dict[Tuple[str, str], str] = {}
        for sym, period, st in conn.execute("SELECT venue_symbol, period, status FROM market_ingest_log WHERE "
                                            "dataset='klines' AND timeframe='1m'"):
            status[(sym, period)] = st
        five = dict(conn.execute("SELECT status, COUNT(*) FROM market_ingest_log WHERE dataset='klines' AND "
                                 "timeframe='5m' GROUP BY status").fetchall())
        features = {f"{d}:{s}": n for d, s, n in conn.execute(
            "SELECT dataset, status, COUNT(*) FROM market_ingest_log WHERE dataset!='klines' GROUP BY dataset, status")}
        last = conn.execute("SELECT MAX(recorded_at) FROM market_ingest_log").fetchone()[0]
    finally:
        conn.close()
    expected = done = failed = 0
    for mem in manifest.get("members") or ():
        for p in _months(int(mem["requested_start_ms"]), int(manifest["window_end_ms"])):
            expected += 1
            st = status.get((mem["venue_symbol"], p))
            done += int(st in CRYPTO_DONE)
            failed += int(st == "FAILED")
    state = "COMPLETE" if done == expected else ("NOT_STARTED" if done == 0 else "ACQUIRING")
    return {"state": state, "reason": None if state == "COMPLETE" else "DATASET_ACQUIRING",
            "1m": {"expected_periods": expected, "done_periods": done, "remaining_periods": expected - done,
                   "failed_retryable_periods": failed, "unit": "member x UTC month"},
            "5m_derived": {"by_status": dict(sorted(five.items()))},
            "supplemental_features": dict(sorted(features.items())), "last_recorded_at_ms": last}


def crypto_broad_coverage() -> Dict[str, Any]:
    path = docs_dir() / BROAD_COVERAGE
    if not path.exists():
        return {"state": "UNAVAILABLE", "reason": "COVERAGE_ARTIFACT_NOT_FOUND"}
    d = json.loads(path.read_text(encoding="utf-8"))
    agg = d.get("aggregate") or {}
    complete = agg.get("complete_symbols") == agg.get("symbols") and not (agg.get("gaps") or agg.get("invalid_rows")
                                                                          or agg.get("duplicate_rows"))
    return {"state": "COMPLETE" if complete and agg.get("reconciles_to_db") else "PARTIAL",
            "symbols": agg.get("symbols"), "complete_symbols": agg.get("complete_symbols"), "rows": agg.get("rows"),
            "young_symbols": agg.get("young_symbols"), "young_symbols_note": agg.get("young_symbols_note"),
            "timeframe": d.get("timeframe"), "window": [d.get("window_start"), d.get("window_end")],
            "coverage_hash": d.get("content_hash"), "universe_hash": d.get("universe_hash")}


# -- holdouts / governance (read-only) --------------------------------------------------------------------------
def holdout_state(db: Any = None) -> Dict[str, Any]:
    """CLOSED unless the registry records an OPENED / BURNED holdout. No holdout content is ever read."""
    rows: List[Tuple[str, str]] = []
    for path in (data_dir() / "certification.db",):
        conn = _ro(path)
        if conn is None:
            continue
        try:
            rows += conn.execute("SELECT holdout_id, event FROM cati_holdout_registry").fetchall()
        except sqlite3.Error:
            pass
        finally:
            conn.close()
    if db is not None:
        try:
            with db.connect() as conn:
                rows += [tuple(r) for r in conn.execute("SELECT holdout_id, event FROM cati_holdout_registry")]
        except Exception:
            pass
    opened = sorted({h for h, e in rows if e in ("OPENED", "BURNED")})
    return {"state": "OPENED" if opened else "CLOSED", "opened_holdouts": opened,
            "reserved_holdouts": len({h for h, e in rows if e == "RESERVED"})}


def governance_phase(db: Any) -> Dict[str, Any]:
    try:
        from app.trading_intelligence.governance.promotion import PromotionGovernance

        return {"phase": PromotionGovernance(db).current_phase(), "reason": None}
    except Exception:
        return {"phase": None, "reason": "GOVERNANCE_STATE_UNAVAILABLE"}


# -- the operator views -----------------------------------------------------------------------------------------
def _manifest_view(name: str) -> Dict[str, Any]:
    res = load_manifest(name)
    m = res["manifest"] or {}
    return {"dataset": name, "status": res["status"], "reason": res["reason"],
            "asset_class": m.get("asset_class"), "role": m.get("role"),
            "provider": m.get("provider") or m.get("source_provider"), "venue": m.get("source_venue"),
            "price_kind": m.get("price_kind"), "instrument_count": len(_members(name, m)) if m else None,
            "timeframes": m.get("timeframes") or ([m["base_resolution"]] if m.get("base_resolution") else None),
            "window_start_ms": m.get("window_start_ms"), "window_end_ms": m.get("window_end_ms"),
            "universe_hash": m.get("universe_hash"), "metadata_hash": m.get("metadata_hash"),
            "parent_universe_hash": m.get("parent_universe_hash"), "code_commit": m.get("code_commit"),
            "execution_authorized": False}


def dataset_manifests(db: Any = None) -> Dict[str, Any]:
    """Safe manifest metadata (no raw data, no holdout observations, no filesystem internals)."""
    fx = _cached("fx_acq", lambda: fx_acquisition(load_manifest("FX_REFERENCE")["manifest"]))
    deep = _cached("deep_acq", lambda: crypto_deep_acquisition(load_manifest("CRYPTO_DEEP")["manifest"]))
    broad = _cached("broad_cov", crypto_broad_coverage)
    hold = holdout_state(db)
    frozen_datasets = []
    if db is not None:
        try:
            with db.connect() as conn:
                frozen_datasets = [{"manifest_id": r[0], "manifest_hash": r[1], "role": r[2], "asset_class": r[3],
                                    "venue": r[4]} for r in conn.execute(
                    "SELECT manifest_id, manifest_hash, role, asset_class, venue FROM dataset_manifests "
                    "ORDER BY created_at")]
        except Exception:
            frozen_datasets = []
    views = []
    for name, acq in (("CRYPTO_BROAD", broad), ("CRYPTO_DEEP", deep), ("FX_REFERENCE", fx)):
        v = _manifest_view(name)
        blockers = []
        if v["status"] != "FROZEN":
            blockers.append(v["reason"] or "MANIFEST_NOT_FROZEN")
        if acq.get("state") != "COMPLETE":
            blockers.append(acq.get("reason") or "DATASET_ACQUIRING")
        views.append({**v, "acquisition": acq, "holdout": hold["state"],
                      "certification": "PRE_HOLDOUT_BLOCKED" if blockers else "PRE_HOLDOUT_EVIDENCE_REQUIRED",
                      "blockers": blockers})
    return {"version": RESEARCH_STATUS_VERSION, "datasets": views, "frozen_dataset_manifests": frozen_datasets,
            "holdout": hold}


def multi_asset_status(db: Any) -> Dict[str, Any]:
    """Per market family, separately: market availability, data, manifest/freeze, pre-holdout certification,
    holdout, governance, execution authority, external venue validation -- never one "ready" boolean."""
    from app.trading_intelligence.venue.registry import ADAPTER_STATUS_REGISTRY

    manifests = dataset_manifests(db)
    by = {d["dataset"]: d for d in manifests["datasets"]}
    gov = governance_phase(db)
    catalog: Dict[str, Dict[str, int]] = {}
    try:
        with db.connect() as conn:
            for venue, ac, n in conn.execute("SELECT venue, asset_class, COUNT(*) FROM venue_instruments WHERE "
                                             "delisted_at_ms IS NULL GROUP BY venue, asset_class"):
                catalog.setdefault(ac, {})[venue] = n
    except Exception:
        catalog = {}
    validated = sorted(f"{v}:{e}" for (v, e) in ADAPTER_STATUS_REGISTRY)

    def external(venues):
        return {v: ("VALIDATED_FOR:" + ",".join(x.split(":")[1] for x in validated if x.startswith(v + ":"))
                    if any(x.startswith(v + ":") for x in validated) else "EXTERNAL_VALIDATION_REQUIRED")
                for v in venues}

    families = {}
    for fam, datasets in (("CRYPTO", ("CRYPTO_BROAD", "CRYPTO_DEEP")), ("FX", ("FX_REFERENCE",))):
        ds = [by[d] for d in datasets]
        data_states = {d["dataset"]: d["acquisition"].get("state") for d in ds}
        primary = ds[0]
        blockers = sorted({b for d in ds[:1] for b in d["blockers"]})
        families[fam] = {
            "market_available": {"state": bool(catalog.get(fam)), "venues": catalog.get(fam, {})},
            "data_readiness": data_states,
            "manifest": {d["dataset"]: {"status": d["status"], "universe_hash": d["universe_hash"]} for d in ds},
            "certification": {"state": "PRE_HOLDOUT_BLOCKED" if blockers else "PRE_HOLDOUT_EVIDENCE_REQUIRED",
                              "reason_codes": blockers or ["PRE_HOLDOUT_RUN_NOT_COMPLETED"],
                              "scope_datasets": [primary["dataset"]]},
            "holdout": {"state": manifests["holdout"]["state"], "reason": "HOLDOUT_CLOSED"
                        if manifests["holdout"]["state"] == "CLOSED" else "HOLDOUT_OPENED"},
            "governance": gov,
            "execution_authority": {"demo": False, "production": False,
                                    "reason": f"GOVERNANCE_PHASE_{gov['phase']}_NO_CATI_AUTHORITY"
                                    if gov["phase"] else gov["reason"]},
            "external_venue_validation": external(("binance_usdm", "bybit_linear", "bingx_swap")),
        }
    return {"version": RESEARCH_STATUS_VERSION, "families": families,
            "note": "states are independent: market availability is not data readiness, not certification, not "
                    "authority. Account eligibility is account-scoped: GET /api/v1/brokers/{account_id}/market-status"}


# -- operator backfill (plan only; acquisition runs as the supervised CLI job) ----------------------------------
#: dataset -> (provider allowlist, timeframes the supervised CLI can scope by instrument AND range)
BACKFILL_PROVIDERS = {"FX_REFERENCE": ("dukascopy", ("1m",)), "CRYPTO_DEEP": ("binance", ("1m",))}
MAX_BACKFILL_DAYS = 3660
MAX_BACKFILL_INSTRUMENTS = 200


class BackfillRequestError(ValueError):
    def __init__(self, reason_code: str, detail: str = ""):
        super().__init__(reason_code)
        self.reason_code, self.detail = reason_code, detail


def backfill_plan(*, dataset: str, provider: str, instruments: List[str], timeframe: str, start: str, end: str,
                  actor_ref: str) -> Dict[str, Any]:
    """Validate a bounded research backfill against the FROZEN universe and return its deterministic job identity,
    current progress and the exact supervised command. Starts nothing, downloads nothing, reads no holdout, changes
    no certification or governance state."""
    from app.trading_intelligence.hashing import short_id

    ds = str(dataset or "").upper()
    if ds not in BACKFILL_PROVIDERS:
        raise BackfillRequestError("DATASET_NOT_ALLOWED", ds)
    allowed_provider, allowed_tf = BACKFILL_PROVIDERS[ds]
    if str(provider or "").lower() != allowed_provider:
        raise BackfillRequestError("PROVIDER_NOT_ALLOWED", str(provider))
    if timeframe not in allowed_tf:
        raise BackfillRequestError("TIMEFRAME_NOT_ALLOWED", str(timeframe))
    try:
        a, b = date.fromisoformat(start), date.fromisoformat(end)
    except (TypeError, ValueError):
        raise BackfillRequestError("INVALID_RANGE", f"{start}..{end}")
    if b < a or (b - a).days > MAX_BACKFILL_DAYS:
        raise BackfillRequestError("RANGE_OUT_OF_BOUNDS", f"{start}..{end}")
    wanted = sorted({str(i).strip().upper() for i in instruments or () if str(i).strip()})
    if not wanted or len(wanted) > MAX_BACKFILL_INSTRUMENTS:
        raise BackfillRequestError("INSTRUMENT_SCOPE_INVALID", str(len(wanted)))
    members = research_members(ds)
    if members is None:
        raise BackfillRequestError("FROZEN_UNIVERSE_UNAVAILABLE", ds)
    window = load_manifest(ds)["manifest"] or {}
    lo_ms = int(window.get("window_start_ms") or 0)
    hi_ms = int(window.get("window_end_ms") or 0)
    a_ms = int(datetime(a.year, a.month, a.day, tzinfo=timezone.utc).timestamp() * 1000)
    b_ms = int(datetime(b.year, b.month, b.day, tzinfo=timezone.utc).timestamp() * 1000)
    if (lo_ms and a_ms < lo_ms) or (hi_ms and b_ms >= hi_ms):
        raise BackfillRequestError("RANGE_OUTSIDE_FROZEN_WINDOW", f"{start}..{end}")
    outside = [i for i in wanted if i not in members]
    if outside:
        # a frozen certification universe never grows through a backfill: a new membership is a new version
        raise BackfillRequestError("INSTRUMENT_OUTSIDE_FROZEN_UNIVERSE", ",".join(outside[:10]))
    if not actor_ref:
        raise BackfillRequestError("ACTOR_REQUIRED")
    spec = {"dataset": ds, "provider": allowed_provider, "instruments": wanted, "timeframe": timeframe,
            "start": a.isoformat(), "end": b.isoformat()}
    job_id = short_id("bfj", spec)
    acq = (fx_acquisition(load_manifest(ds)["manifest"]) if ds == "FX_REFERENCE"
           else crypto_deep_acquisition(load_manifest(ds)["manifest"]))
    if ds == "FX_REFERENCE":
        cmd = ["scripts/acquire_fx_reference_dataset.py", "--db", f"data/research/{DATABASES[ds]}", "--pace", "0.8",
               "minute", "--manifest", f"docs/research/{MANIFESTS[ds]}", "--pairs", ",".join(wanted),
               "--start", spec["start"], "--end", spec["end"]]
    else:  # the deep job scopes by symbol; each member's frozen window bounds the range (done periods are skipped)
        cmd = ["scripts/acquire_crypto_deep_dataset.py", "--db", f"data/research/{DATABASES[ds]}", "--pace", "0.25",
               "acquire", "--manifest", f"docs/research/{MANIFESTS[ds]}", "--symbols", ",".join(wanted)]
    return {"job_id": job_id, "spec": spec, "requested_by": actor_ref, "execution": "SUPERVISED_OPERATOR_JOB",
            "status": acq.get("state"), "progress": acq, "command": cmd, "resumable": True, "idempotent": True,
            "rate_limited": True, "holdout_access": "NONE", "certification_authority_change": "NONE",
            "note": "plan only: the acquisition job is resumable through its ingest log (a repeated run re-fetches "
                    "nothing already done) and is paced + circuit-broken; this API never downloads"}


__all__ = ["BackfillRequestError", "MANIFESTS", "RESEARCH_STATUS_VERSION", "backfill_plan", "crypto_broad_coverage",
           "crypto_deep_acquisition", "dataset_manifests", "fx_acquisition", "governance_phase", "holdout_state",
           "load_manifest", "multi_asset_status", "research_members", "reset_cache"]
