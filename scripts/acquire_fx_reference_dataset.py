#!/usr/bin/env python3
"""Acquire the CATI FX REFERENCE dataset from Dukascopy (no broker account needed).

Reference data is NOT execution data: every row is REFERENCE_MARKET_PRICE in
``fx_reference_quotes``; execution economics come from the venue product
(e.g. Bybit EURUSDUSDT) and are mapped separately
(``app.market_data.reference_mapping``).

Every provider file's price scale is VERIFIED against an independent level
(the triangular level of the pair's USD legs, else its own adjacent periods)
before a row is written: the Dukascopy point is period-dependent (EURCNH /
EURZAR files up to 2024-08 carry six decimals, later files five). A file whose
scale cannot be verified is quarantined (FAILED / QUARANTINED:...), never
guessed. See ``app.market_data.fx_scale``.

Commands
--------
* ``probe``     one monthly hourly BID file per candidate pair -> availability.
* ``broad``     hourly BID+ASK per month for every available pair (USD legs first).
* ``minute``    BROAD 1m BID+ASK for every member of the frozen FX universe over its
                window (``--plan`` = plan only; ``--max-requests`` / ``--max-minutes``
                bound one run; a circuit breaker stops on repeated provider refusals).
* ``deep``      the microstructure subset (venue-mapped FX pairs + USD majors, last N
                days) -- same table / timeframe / source version as ``minute``.
* ``derive``    deterministic 5m / 15m / 4h bid/ask bars from the real 1m quotes.
* ``qa``        FULL-HISTORY scale + cross-rate QA (``--quarantine`` marks failing
                provider periods for controlled re-ingest).
* ``repair-scale`` controlled re-ingest of one proven-corrupt period with lineage in
                ``fx_reference_repairs`` (evidence only unless ``--apply``).
* ``freeze``    the independent frozen FX universe manifest.
* ``validate``  coverage, ingest-log summary, full-history triangular consistency.
* ``tracking``  venue FX perpetual (EXECUTION_VENUE_PRICE) vs reference mid.

Resumable through ``fx_reference_ingest_log``: FETCHED / NO_FILE / EMPTY periods are
never re-downloaded; FAILED periods are retried; rows are written before the log, so
an interruption can only cause a re-fetch (INSERT OR IGNORE), never a lost period.
Saturdays are logged EMPTY (market closed) without a request. Nothing is interpolated.

    python scripts/acquire_fx_reference_dataset.py --db <research.db> qa --out qa.json
    python scripts/acquire_fx_reference_dataset.py --db <research.db> repair-scale --pair EURCNH --period 2024-08 --apply
    python scripts/acquire_fx_reference_dataset.py --db <research.db> freeze --out docs/research/cati_fx_universe_dukascopy_v1.json
    python scripts/acquire_fx_reference_dataset.py --db <research.db> minute --manifest docs/research/cati_fx_universe_dukascopy_v1.json
    python scripts/acquire_fx_reference_dataset.py --db <research.db> derive --manifest docs/research/cati_fx_universe_dukascopy_v1.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [os.path.join(REPO, "backends", "bot-backend"), os.path.join(REPO, "backends", "shared")]

PROVIDER = "dukascopy"
G10 = ("EUR", "GBP", "AUD", "NZD", "USD", "CAD", "CHF", "JPY")  # market-convention priority order
#: liquid non-G10 currencies quoted with the standard 1e5 point (JPY-style large quotes are excluded
#: until their price scale is validated -- see UNVALIDATED_PRICE_SCALE)
NON_G10_STANDARD_POINT = ("SEK", "NOK", "DKK", "PLN", "SGD", "HKD", "CNH", "ZAR", "MXN", "TRY", "ILS")
UNVALIDATED_SCALE = ("HUF", "CZK", "RUB", "THB", "INR", "KRW")
PACE_S = 0.9
USD_MAJORS = ("EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD")
DONE_STATES = ("FETCHED", "NO_FILE", "EMPTY")
QUARANTINE = "QUARANTINED"
REPAIR_VERSION = "fx-period-reingest-verified-scale-v1"


def candidate_pairs():
    out = []
    for i, a in enumerate(G10):
        for b in G10[i + 1:]:
            out.append(a + b)
    for c in NON_G10_STANDARD_POINT:
        out += ["USD" + c, "EUR" + c]
    return sorted(set(out))


class ProviderUnavailable(RuntimeError):
    """The provider kept refusing/resetting requests: stop the run (resume later), never spin."""


class Fetcher:
    def __init__(self, pace_s: float = PACE_S, max_requests: Optional[int] = None,
                 deadline: Optional[float] = None, breaker: int = 6):
        import requests

        self.s = requests.Session()
        self.s.headers["User-Agent"] = "Mozilla/5.0 (cosmicforge-research)"
        self.pace_s = pace_s
        self.last = 0.0
        self.requests = 0
        self.max_requests = max_requests
        self.deadline = deadline
        self.breaker = breaker
        self.consecutive_failures = 0

    def budget_left(self) -> bool:
        if self.max_requests is not None and self.requests >= self.max_requests:
            return False
        return self.deadline is None or time.time() < self.deadline

    def get(self, url):
        """bytes | None (404 = provider has no file); raises after persistent failure (bounded)."""
        import requests

        for attempt in range(5):
            wait = self.pace_s - (time.time() - self.last)
            if wait > 0:
                time.sleep(wait)
            self.last = time.time()
            self.requests += 1
            try:
                r = self.s.get(url, timeout=25)
            except requests.RequestException:
                self._failed()
                time.sleep(min(20 * (attempt + 1), 90))
                continue
            if r.status_code == 404:
                self.consecutive_failures = 0
                return None
            if r.status_code == 429 or r.status_code >= 500:
                self._failed()
                time.sleep(min(30 * (attempt + 1), 120))
                continue
            r.raise_for_status()
            self.consecutive_failures = 0
            return r.content
        raise RuntimeError("persistent failure")

    def _failed(self):
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.breaker:
            raise ProviderUnavailable(f"{self.consecutive_failures} consecutive provider failures")


def _db(path):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.market_data_schema import ensure_market_data_schema

    db = DB(path=path)
    ensure_market_data_schema(db)
    return db


def _logged(conn, pair, tf, period, side):
    r = conn.execute("SELECT status, reason FROM fx_reference_ingest_log WHERE provider=? AND pair=? AND timeframe=? "
                     "AND period=? AND side=?", (PROVIDER, pair, tf, period, side)).fetchone()
    return (r[0], r[1]) if r else (None, None)


def _log(conn, pair, tf, period, side, status, rows=0, reason=None):
    conn.execute("INSERT OR REPLACE INTO fx_reference_ingest_log (provider, pair, timeframe, period, side, status, "
                 "rows, reason, recorded_at) VALUES (?,?,?,?,?,?,?,?,?)",
                 (PROVIDER, pair, tf, period, side, status, rows, reason, int(time.time() * 1000)))


def _done(states) -> bool:
    return all(s in DONE_STATES for s, _ in states.values())


def _quarantined(states) -> bool:
    return any(s == "FAILED" and str(r or "").startswith(QUARANTINE) for s, r in states.values())


def cmd_probe(args, db, f):
    from app.market_data import fx_reference as fx

    y, m = args.probe_year, args.probe_month
    res = {}
    for pair in candidate_pairs():
        url = fx.DUKASCOPY_HOUR_URL.format(pair=pair, y=y, m=m - 1, side="BID")
        body = f.get(url)
        if body is None:
            res[pair] = {"available": False, "reason": "PROVIDER_HAS_NO_FILE"}
        else:
            rows = fx.drop_flat_closed_bars(fx.decode_hour_file(body, year=y, month=m, point=fx.point_for(pair)))
            res[pair] = {"available": bool(rows), "reason": None if rows else "PROVIDER_FILE_EMPTY"}
        print(json.dumps({pair: res[pair]}), flush=True)
    for c in UNVALIDATED_SCALE:
        for p in ("USD" + c, "EUR" + c):
            res[p] = {"available": False, "reason": "UNVALIDATED_PRICE_SCALE"}
    with db.connect() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS fx_reference_availability (provider TEXT, pair TEXT, available INTEGER,"
                     " reason TEXT, probed_period TEXT, recorded_at INTEGER, PRIMARY KEY (provider, pair))")
        for p, v in res.items():
            conn.execute("INSERT OR REPLACE INTO fx_reference_availability VALUES (?,?,?,?,?,?)",
                         (PROVIDER, p, int(v["available"]), v["reason"], f"{y}-{m:02d}", int(time.time() * 1000)))
    print(json.dumps({"available": sorted(p for p, v in res.items() if v["available"]),
                      "unavailable": {p: v["reason"] for p, v in res.items() if not v["available"]}}))


def _available(db):
    with db.connect() as conn:
        return [r[0] for r in conn.execute("SELECT pair FROM fx_reference_availability WHERE provider=? AND "
                                           "available=1 ORDER BY pair", (PROVIDER,))]


def _legs_first(pairs):
    """USD legs before crosses: a cross's scale is verified against its legs."""
    return sorted(pairs, key=lambda p: (0 if "USD" in p else 1, p))


# ── period-verified scale (Section 12.2) ─────────────────────────────────────

def expected_level(conn, pair, start_ms, end_ms, available):
    """An INDEPENDENT level for [start, end): the triangular level from the stored USD legs, else the pair's own
    nearest stored bars outside the period. (level, source) or (None, reason)."""
    from app.market_data import fx_scale

    for tf in ("1h", "1m"):
        lvl = fx_scale.triangular_level(conn, provider=PROVIDER, pair=pair, timeframe=tf, start_ms=start_ms,
                                        end_ms=end_ms, available=available)
        if lvl:
            return lvl, f"TRIANGULAR_USD_LEGS_{tf}"
    for tf in ("1h", "1m"):
        rows = [r[0] for r in conn.execute(
            "SELECT mid_close FROM fx_reference_quotes WHERE provider=? AND pair=? AND timeframe=? AND "
            "mid_close IS NOT NULL AND (open_time<? OR open_time>=?) ORDER BY ABS(open_time-?) LIMIT 200",
            (PROVIDER, pair, tf, start_ms, end_ms, start_ms))]
        if rows:
            return fx_scale.median(rows), f"ADJACENT_PERIOD_CONTINUITY_{tf}"
    return None, "NO_INDEPENDENT_REFERENCE"


def decode_verified(conn, pair, body, *, decode, start_ms, end_ms, available):
    """(rows | None, point, evidence). None = the file's scale could not be verified: quarantine, never guess."""
    from app.market_data import fx_reference as fx
    from app.market_data import fx_scale

    level, source = expected_level(conn, pair, start_ms, end_ms, available)
    raw = [c for c in fx.raw_closes(body)]
    default = fx.point_for(pair)
    if level is None:
        rows = fx.drop_flat_closed_bars(decode(default))
        return rows, default, {"scale": "UNVERIFIED_NO_REFERENCE", "point": default, "reference": source}
    point, ev = fx_scale.infer_point(raw, level)
    ev = {**ev, "reference": source}
    if point is None:
        return None, None, {**ev, "scale": "UNRESOLVED"}
    rows = fx.drop_flat_closed_bars(decode(point))
    return rows, point, {**ev, "scale": "VERIFIED", "point": point,
                         "differs_from_default": point != default}


def _reason(ev):
    return (f"point={ev.get('point'):g};scale={ev.get('scale')};ref={ev.get('reference')}"
            if ev.get("point") else f"{QUARANTINE}:{ev.get('reason') or ev.get('scale')}")


def ingest_period(db, f, store, pair, tf, period, *, start_ms, end_ms, url_for, decoder_for, available,
                  source_version, replace=False, repair_reason=None):
    """Fetch BID+ASK for one provider period, verify each file's scale, then write rows and log -- rows FIRST,
    log second, so an interruption can only cause a re-fetch (INSERT OR IGNORE), never a lost period.
    ``replace`` (repair of a proven-corrupt period): delete + insert + lineage row in ONE transaction."""
    from app.market_data import fx_reference as fx

    base, quote = fx.split_pair(pair)
    sides, statuses, evidence = {}, {}, {}
    for side in ("BID", "ASK"):
        try:
            body = f.get(url_for(side))
        except ProviderUnavailable:
            raise
        except Exception as exc:
            statuses[side] = ("FAILED", 0, f"FETCH_FAILED:{type(exc).__name__}")
            sides[side] = None
            continue
        if body is None:
            sides[side], statuses[side] = [], ("NO_FILE", 0, None)
            continue
        with db.connect() as conn:
            rows, point, ev = decode_verified(conn, pair, body, decode=decoder_for(body), start_ms=start_ms,
                                              end_ms=end_ms, available=available)
        evidence[side] = {**ev, "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}
        if rows is None:
            sides[side], statuses[side] = None, ("FAILED", 0, _reason(ev))
            continue
        sides[side] = rows
        statuses[side] = ("FETCHED" if rows else "EMPTY", len(rows), _reason(ev))
    ok = all(v is not None for v in sides.values())
    written = removed = 0
    if ok:
        merged = fx.merge_sides(sides["BID"], sides["ASK"], pair=pair)
        try:
            with db.connect() as conn:
                if replace:
                    old = {s: _logged(conn, pair, tf, period, s)[0] for s in ("BID", "ASK")}
                    removed = conn.execute("DELETE FROM fx_reference_quotes WHERE provider=? AND pair=? AND "
                                           "timeframe=? AND open_time>=? AND open_time<?",
                                           (PROVIDER, pair, tf, start_ms, end_ms)).rowcount
                written = store.write_fx_quotes(PROVIDER, pair, base, quote, tf, merged,
                                                source_version=source_version, conn=conn)
                if replace:
                    validation = _validate_period(conn, pair, tf, start_ms, end_ms, available)
                    conn.execute(
                        "INSERT INTO fx_reference_repairs (repair_id, provider, pair, timeframe, period, old_status, "
                        "reason, algorithm_version, source_evidence_json, rows_removed, rows_inserted, "
                        "validation_json, recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (f"rep_{PROVIDER}_{pair}_{tf}_{period}_{int(time.time())}", PROVIDER, pair, tf, period,
                         json.dumps(old), repair_reason or "SCALE_BREAK", REPAIR_VERSION,
                         json.dumps(evidence, sort_keys=True), removed, written,
                         json.dumps(validation, sort_keys=True), int(time.time() * 1000)))
                for side in ("BID", "ASK"):
                    st, n, why = statuses[side]
                    _log(conn, pair, tf, period, side, st, n,
                         (f"REPAIRED:{REPAIR_VERSION};" + (why or "")) if replace and st == "FETCHED" else why)
        except ValueError as exc:
            if not str(exc).startswith("FX_"):
                raise
            # the store rejected the period (invalid OHLC / crossed sides / out of order): the transaction rolled
            # back, nothing was written or deleted, and both sides stay retryable with the rejection recorded
            ok, written, removed = False, 0, 0
            statuses = {s: ("FAILED", 0, f"QUALITY_REJECTED:{exc}") for s in statuses}
    if not ok:
        with db.connect() as conn:
            for side in ("BID", "ASK"):
                st, n, why = statuses[side]
                # one side failed/unverified: nothing is written for the period and BOTH sides stay retryable
                _log(conn, pair, tf, period, side, "FAILED" if st != "FAILED" else st, 0,
                     why if st == "FAILED" else f"OTHER_SIDE_FAILED;{st}")
    return {"pair": pair, "period": period, "written": written, "removed": removed,
            "statuses": {s: v[0] for s, v in statuses.items()}, "evidence": evidence, "ok": ok}


def _validate_period(conn, pair, tf, start_ms, end_ms, available):
    """Post-write check of one period: triangular level vs the stored mid (median relative error)."""
    from app.market_data import fx_scale

    level = fx_scale.triangular_level(conn, provider=PROVIDER, pair=pair, timeframe=tf, start_ms=start_ms,
                                      end_ms=end_ms, available=available)
    mids = [r[0] for r in conn.execute("SELECT mid_close FROM fx_reference_quotes WHERE provider=? AND pair=? AND "
                                       "timeframe=? AND open_time>=? AND open_time<? AND mid_close IS NOT NULL",
                                       (PROVIDER, pair, tf, start_ms, end_ms))]
    m = fx_scale.median(mids)
    rel = abs(m - level) / level if (m and level) else None
    return {"rows": len(mids), "median_mid": m, "triangular_level": level, "rel_diff": rel,
            "passed": rel is not None and rel <= 0.02}


def _month_bounds(y, m):
    from app.market_data import fx_reference as fx

    return fx.month_start_ms(y, m), fx.month_start_ms(y + (m == 12), 1 if m == 12 else m + 1)


def _hour_period(db, f, store, pair, y, m, available, *, replace=False, repair_reason=None):
    from app.market_data import fx_reference as fx

    start, end = _month_bounds(y, m)
    return ingest_period(
        db, f, store, pair, "1h", f"{y}-{m:02d}", start_ms=start, end_ms=end,
        url_for=lambda side: fx.DUKASCOPY_HOUR_URL.format(pair=pair, y=y, m=m - 1, side=side),
        decoder_for=lambda body: (lambda point: fx.decode_hour_file(body, year=y, month=m, point=point)),
        available=available, source_version="bi5-candles-hour-1:v1", replace=replace, repair_reason=repair_reason)


def _minute_period(db, f, store, pair, d, available, *, replace=False, repair_reason=None):
    from app.market_data import fx_reference as fx

    start = int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)
    return ingest_period(
        db, f, store, pair, "1m", d.isoformat(), start_ms=start, end_ms=start + 86_400_000,
        url_for=lambda side: fx.DUKASCOPY_URL.format(pair=pair, y=d.year, m=d.month - 1, d=d.day, side=side),
        decoder_for=lambda body: (lambda point: fx.decode_bi5_candles(body, day_start_ms=start, point=point)),
        available=available, source_version="bi5-candles-min-1:v1", replace=replace, repair_reason=repair_reason)


# ── tiers ──────────────────────────────────────────────────────────────────────

def cmd_broad(args, db, f):
    from app.market_data.store import MarketDataStore

    store = MarketDataStore(db)
    today = date.today()
    months = []
    y, m = today.year, today.month
    for _ in range(args.months):
        m -= 1
        if m == 0:
            y, m = y - 1, 12
        months.append((y, m))
    available = _available(db)
    for pair in _legs_first(available):
        n = 0
        for (y, m) in sorted(months):
            with db.connect() as conn:
                states = {s: _logged(conn, pair, "1h", f"{y}-{m:02d}", s) for s in ("BID", "ASK")}
            if _done(states):
                continue
            res = _hour_period(db, f, store, pair, y, m, available, replace=_quarantined(states),
                               repair_reason="QUARANTINED_PERIOD_REINGEST" if _quarantined(states) else None)
            n += res["written"]
        print(json.dumps({"pair": pair, "tf": "1h", "rows_written": n, "requests": f.requests}), flush=True)


def _minute_days(start, end):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def acquire_minutes(db, f, pairs, start, end, *, available):
    """Resumable 1m BID/ASK acquisition over [start, end] (UTC days) for ``pairs``, pair by pair.

    Completed periods (FETCHED / NO_FILE / EMPTY) are skipped; FAILED periods are retried; quarantined periods
    go through the controlled replace path. Saturdays are logged EMPTY (market closed) without a request.
    Bounded by the fetcher's request / time budget and circuit breaker; returns a progress summary."""
    from app.market_data.store import MarketDataStore

    store = MarketDataStore(db)
    summary = {"pairs": {}, "stopped": None, "requests": 0}
    for pair in _legs_first(pairs):
        done = fetched = failed = skipped = 0
        for d in _minute_days(start, end):
            period = d.isoformat()
            with db.connect() as conn:
                states = {s: _logged(conn, pair, "1m", period, s) for s in ("BID", "ASK")}
                if d.weekday() == 5 and not all(st for st, _ in states.values()):
                    for side in ("BID", "ASK"):
                        if states[side][0] is None:
                            _log(conn, pair, "1m", period, side, "EMPTY", 0, "MARKET_CLOSED_SATURDAY")
                    skipped += 1
                    continue
            if _done(states):
                done += 1
                continue
            if not f.budget_left():
                summary["stopped"] = "BUDGET_EXHAUSTED"
                break
            try:
                res = _minute_period(db, f, store, pair, d, available, replace=_quarantined(states),
                                     repair_reason="QUARANTINED_PERIOD_REINGEST" if _quarantined(states) else None)
            except ProviderUnavailable as exc:
                summary["stopped"] = f"PROVIDER_UNAVAILABLE:{exc}"
                break
            fetched += int(res["ok"])
            failed += int(not res["ok"])
        summary["pairs"][pair] = {"already_done": done, "fetched": fetched, "failed": failed,
                                  "saturdays_marked": skipped}
        print(json.dumps({"pair": pair, "tf": "1m", **summary["pairs"][pair], "requests": f.requests}), flush=True)
        if summary["stopped"]:
            break
    summary["requests"] = f.requests
    return summary


def cmd_minute(args, db, f):
    """Broad 1m tier: every member of the frozen FX universe over its requested window (or --start/--end)."""
    from app.market_data.fx_universe import load_fx_universe

    manifest = load_fx_universe(args.manifest)
    pairs = [m["pair"] for m in manifest["members"]]
    if args.pairs:
        wanted = {p.strip().upper() for p in args.pairs.split(",")}
        pairs = [p for p in pairs if p in wanted]
    start = date.fromisoformat(args.start) if args.start else datetime.fromtimestamp(
        manifest["window_start_ms"] / 1000, timezone.utc).date()
    end = date.fromisoformat(args.end) if args.end else (datetime.fromtimestamp(
        manifest["window_end_ms"] / 1000, timezone.utc) - timedelta(days=1)).date()
    if args.plan:
        print(json.dumps(minute_plan(db, pairs, start, end), indent=2))
        return
    print(json.dumps(acquire_minutes(db, f, pairs, start, end, available=_available(db)), indent=2))


def minute_plan(db, pairs, start, end):
    """Plan-only: periods done / remaining and the request estimate (no network)."""
    days = [d for d in _minute_days(start, end) if d.weekday() != 5]
    out = {"pairs": len(pairs), "market_days": len(days), "per_pair": {}, "remaining_periods": 0}
    with db.connect() as conn:
        for pair in pairs:
            rows = dict(conn.execute("SELECT period, MIN(status IN ('FETCHED','NO_FILE','EMPTY')) FROM "
                                     "fx_reference_ingest_log WHERE provider=? AND pair=? AND timeframe='1m' AND "
                                     "period>=? AND period<=? GROUP BY period",
                                     (PROVIDER, pair, start.isoformat(), end.isoformat())).fetchall())
            done = sum(1 for d in days if rows.get(d.isoformat()) == 1)
            out["per_pair"][pair] = {"done": done, "remaining": len(days) - done}
            out["remaining_periods"] += len(days) - done
    out["remaining_requests_estimate"] = out["remaining_periods"] * 2
    return out


def execution_mapped_pairs():
    """FX pairs a connected venue lists as an FX execution instrument, discovered NOW."""
    import requests

    from app.exchange import instruments as I

    pairs = set()
    try:
        def page(cursor):
            d = requests.get("https://api.bybit.com/v5/market/instruments-info",
                             params={"category": "linear", "limit": 1000, **({"cursor": cursor} if cursor else {})},
                             timeout=30).json()
            return d["result"].get("list") or [], d["result"].get("nextPageCursor")

        for r in I.collect_cursor_pages(page, source="bybit instruments-info (fx pairs)"):
            ins = I.parse_bybit_instrument(r)
            if ins and ins.asset_class == I.FX:
                pairs.add(ins.base_currency + ins.quote_currency)
    except Exception as exc:  # discovery failure: fall back to majors only, recorded
        print(json.dumps({"bybit_discovery_error": str(exc)[:200]}))
    try:
        d = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/contracts", timeout=30).json()
        for c in d.get("data") or []:
            ins = I.parse_bingx_contract(c)
            if ins and ins.asset_class == I.FX:
                pairs.add(ins.base_currency + ins.quote_currency)
    except Exception as exc:
        print(json.dumps({"bingx_discovery_error": str(exc)[:200]}))
    return sorted(pairs)


def cmd_deep(args, db, f):
    """Microstructure subset: 1m BID/ASK for the venue-mapped FX pairs + USD majors over the last N days. Uses the
    SAME table/timeframe/source version as the broad 1m tier: one source of truth, INSERT OR IGNORE."""
    avail = set(_available(db))
    pairs = sorted((set(execution_mapped_pairs()) | set(USD_MAJORS)) & avail) if not args.pairs else \
        [p.strip().upper() for p in args.pairs.split(",")]
    print(json.dumps({"deep_pairs": pairs}), flush=True)
    end = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)
    start = end - timedelta(days=args.days - 1)
    print(json.dumps(acquire_minutes(db, f, pairs, start, end, available=sorted(avail)), indent=2))
    for pair in pairs:
        derive(db, pair, ("15m",))


DERIVED = {"5m": 5, "15m": 15, "4h": 240}


def derive(db, pair, timeframes=tuple(DERIVED), *, chunk_days=7):
    """Deterministic bid/ask bars from the real 1m quotes, streamed in whole-day chunks (every target divides a
    UTC day, so chunk edges are window edges). Each side is aggregated from its own 1m OHLC; mid/spread are
    derived afterwards; an incomplete window is dropped, never approximated. 1h stays the provider's own hourly
    file (canonical, source-distinct); the 1m-derived 1h is reproduced on demand by QA (``derive_check``)."""
    from app.market_data import fx_reference as fx
    from app.market_data.store import MarketDataStore

    store = MarketDataStore(db)
    base, quote = fx.split_pair(pair)
    with db.connect() as conn:
        lo, hi = conn.execute("SELECT MIN(open_time), MAX(open_time) FROM fx_reference_quotes WHERE provider=? AND "
                              "pair=? AND timeframe='1m'", (PROVIDER, pair)).fetchone()
    out = {tf: 0 for tf in timeframes}
    if lo is None:
        return out
    day = 86_400_000
    t = (lo // day) * day
    while t <= hi:
        with db.connect() as conn:
            conn.row_factory = sqlite3.Row
            q = [dict(r) for r in conn.execute(
                "SELECT open_time, bid_open, bid_high, bid_low, bid_close, ask_open, ask_high, ask_low, ask_close, "
                "volume FROM fx_reference_quotes WHERE provider=? AND pair=? AND timeframe='1m' AND open_time>=? AND "
                "open_time<? ORDER BY open_time", (PROVIDER, pair, t, t + chunk_days * day))]
        for tf in timeframes:
            rows = fx.resample_quotes(q, DERIVED[tf])
            out[tf] += store.write_fx_quotes(PROVIDER, pair, base, quote, tf, rows,
                                             source_version=f"derived-from-1m:{DERIVED[tf]}:v1")
        t += chunk_days * day
    print(json.dumps({"pair": pair, "derived": out}), flush=True)
    return out


def cmd_derive(args, db, f):
    from app.market_data.fx_universe import load_fx_universe

    pairs = ([p.strip().upper() for p in args.pairs.split(",")] if args.pairs else
             [m["pair"] for m in load_fx_universe(args.manifest)["members"]])
    tfs = tuple(t.strip() for t in args.timeframes.split(","))
    for pair in pairs:
        derive(db, pair, tfs)


# ── QA / repair / freeze ───────────────────────────────────────────────────────

def cmd_qa(args, db, f):
    """Full-history scale + cross-rate QA (replaces the latest-bar check). ``--quarantine`` marks every failing
    provider period FAILED/QUARANTINED in the ingest log so the next run re-ingests it via the replace path."""
    from app.market_data import fx_scale

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    pairs = [r[0] for r in conn.execute("SELECT DISTINCT pair FROM fx_reference_quotes WHERE provider=? AND "
                                        "timeframe=? ORDER BY pair", (PROVIDER, args.timeframe))]
    rep = fx_scale.scale_report(conn, provider=PROVIDER, timeframe=args.timeframe, pairs=pairs)
    q = conn.execute("SELECT COUNT(*), SUM(bid_close IS NULL OR ask_close IS NULL), SUM(spread_close<0), "
                     "SUM(ask_close<bid_close), SUM(bid_high<bid_low OR ask_high<ask_low OR bid_high<bid_close OR "
                     "bid_low>bid_close OR ask_high<ask_close OR ask_low>ask_close OR bid_high<bid_open OR "
                     "bid_low>bid_open OR ask_high<ask_open OR ask_low>ask_open), "
                     "SUM(mid_close IS NOT NULL AND ABS(mid_close-(bid_close+ask_close)/2.0)>1e-9*mid_close) "
                     "FROM fx_reference_quotes WHERE provider=? AND timeframe=?", (PROVIDER, args.timeframe)).fetchone()
    rep["bid_ask_quality"] = dict(zip(("rows", "missing_side", "negative_spread", "ask_below_bid", "invalid_ohlc",
                                       "mid_inconsistent"), [int(v or 0) for v in q]))
    rels = [r for r in rep["relations"].values() if r["aligned_bars"]]
    rep["summary"] = {"relations": len(rels), "aligned_bars": sum(r["aligned_bars"] for r in rels),
                      "worst_median_rel_error": max((r["median_rel_error"] for r in rels), default=None),
                      "worst_bar_rel_error": max((r["max_rel_error"] for r in rels), default=None),
                      "failing_relations": sorted(r["pair"] for r in rels if not r["passed"]),
                      "failing_pairs": rep["failing_pairs"]}
    conn.close()
    if args.quarantine and rep["failing_pairs"]:
        with db.connect() as w:
            for pair in rep["failing_pairs"]:
                bad = set(rep["pairs"][pair]["scale_break_periods"]) | set(
                    rep["pairs"][pair]["cross_rate_failing_periods"])
                for period in sorted(bad):
                    for side in ("BID", "ASK"):
                        _log(w, pair, args.timeframe, period, side, "FAILED", 0, f"{QUARANTINE}:SCALE_QA_FAILED")
                rep["pairs"][pair]["quarantined_periods"] = sorted(bad)
    text = json.dumps(rep, indent=2, sort_keys=True, default=str)
    if args.out:
        open(args.out, "w", encoding="utf-8").write(text)
    print(json.dumps(rep["summary"], indent=2, default=str))
    return rep


def cmd_repair_scale(args, db, f):
    """Controlled re-ingest of ONE proven-corrupt provider period: fetch the raw files again, verify their scale
    against the independent triangular level, then (``--apply``) replace the period's rows atomically with a
    lineage row in fx_reference_repairs. Without ``--apply`` it only reports the evidence."""
    from app.market_data import fx_reference as fx
    from app.market_data.store import MarketDataStore

    pair, tf = args.pair.upper(), args.timeframe
    y, m = (int(x) for x in args.period.split("-")[:2])
    available = _available(db)
    if not args.apply:
        start, end = _month_bounds(y, m)
        with db.connect() as conn:
            level, source = expected_level(conn, pair, start, end, available)
            stored = [r[0] for r in conn.execute("SELECT mid_close FROM fx_reference_quotes WHERE provider=? AND "
                                                 "pair=? AND timeframe=? AND open_time>=? AND open_time<?",
                                                 (PROVIDER, pair, tf, start, end))]
        ev = {}
        for side in ("BID", "ASK"):
            body = f.get(fx.DUKASCOPY_HOUR_URL.format(pair=pair, y=y, m=m - 1, side=side))
            raw = fx.raw_closes(body or b"")
            from app.market_data import fx_scale

            point, e = fx_scale.infer_point(raw, level)
            ev[side] = {**e, "point": point, "sha256": hashlib.sha256(body or b"").hexdigest()}
        print(json.dumps({"pair": pair, "period": args.period, "stored_rows": len(stored),
                          "stored_median_mid": fx_scale.median(stored) if stored else None,
                          "independent_level": level, "level_source": source, "raw_evidence": ev}, indent=2,
                         default=str))
        return
    if tf != "1h":
        raise SystemExit("repair-scale --apply supports the 1h broad tier; 1m periods re-ingest via quarantine")
    res = _hour_period(db, f, MarketDataStore(db), pair, y, m, available, replace=True,
                       repair_reason=args.reason or "SCALE_BREAK_PROVIDER_POINT_CHANGE")
    print(json.dumps(res, indent=2, default=str))


def _git_commit():
    try:
        import subprocess

        return subprocess.check_output(["git", "-C", REPO, "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return None


def cmd_freeze(args, db, f):
    """Freeze the FX reference universe (independent manifest). Refused while any member fails scale QA."""
    from app.market_data import fx_scale
    from app.market_data.fx_reference import split_pair
    from app.market_data.fx_universe import build_fx_universe_manifest
    from app.market_data.gaps import GAP_POLICY_VERSION

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    avail = {r[0]: (r[1], r[2]) for r in conn.execute("SELECT pair, available, reason FROM "
                                                      "fx_reference_availability WHERE provider=?", (PROVIDER,))}
    members_pairs = sorted(p for p, (a, _) in avail.items() if a)
    rep = fx_scale.scale_report(conn, provider=PROVIDER, timeframe="1h", pairs=members_pairs)
    conn.close()
    members = []
    for p in members_pairs:
        b, q = split_pair(p)
        rel = rep["relations"].get(p)
        members.append({"pair": p, "base": b, "quote": q, "scale_status": rep["pairs"][p]["status"],
                        "cross_rate_status": ("PASS" if rel["passed"] else "FAIL") if rel else "NO_RELATION_USD_LEG"})
    excluded = {p: r for p, (a, r) in avail.items() if not a}
    ws = int(datetime.fromisoformat(args.window_start).replace(tzinfo=timezone.utc).timestamp() * 1000)
    we = int(datetime.fromisoformat(args.window_end).replace(tzinfo=timezone.utc).timestamp() * 1000)
    manifest = build_fx_universe_manifest(
        provider=PROVIDER, members=members, excluded=excluded, window_start_ms=ws, window_end_ms=we,
        scale_qa_version=fx_scale.SCALE_QA_VERSION, gap_policy_version=GAP_POLICY_VERSION,
        source_versions=["bi5-candles-min-1:v1", "bi5-candles-hour-1:v1"],
        generated_at=datetime.now(timezone.utc).isoformat(), code_commit=_git_commit())
    if os.path.exists(args.out) and not args.force_new_path:
        from app.market_data.fx_universe import load_fx_universe

        prior = load_fx_universe(args.out)
        if prior["universe_hash"] != manifest["universe_hash"]:
            raise SystemExit(f"{args.out} is a frozen universe with a different identity; write a NEW versioned file")
        print(json.dumps({"unchanged": prior["universe_hash"]}))
        return
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)
    print(json.dumps({"out": args.out, "members": len(members), "excluded": len(excluded),
                      "universe_hash": manifest["universe_hash"]}))


def cmd_validate(args, db, f):
    """Coverage + ingest-log summary + FULL-HISTORY triangular QA (the old latest-bar check is gone)."""
    from app.market_data import fx_scale

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    rep = {"provider": PROVIDER, "pairs": {}}
    for pair, tf, n, lo, hi, nulls, neg in conn.execute(
            "SELECT pair, timeframe, COUNT(*), MIN(open_time), MAX(open_time), "
            "SUM(CASE WHEN bid_close IS NULL OR ask_close IS NULL THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN spread_close < 0 THEN 1 ELSE 0 END) FROM fx_reference_quotes WHERE provider=? "
            "GROUP BY pair, timeframe", (PROVIDER,)):
        rep["pairs"].setdefault(pair, {})[tf] = {"rows": n, "first_ms": lo, "last_ms": hi,
                                                 "one_side_missing": nulls, "negative_spread": neg}
    gaps = {}
    for pair, tf, status, cnt in conn.execute("SELECT pair, timeframe, status, COUNT(*) FROM fx_reference_ingest_log "
                                              "WHERE provider=? GROUP BY 1,2,3", (PROVIDER,)):
        gaps.setdefault(pair, {}).setdefault(tf, {})[status] = cnt
    rep["ingest_log"] = gaps
    rels = fx_scale.cross_rate_history(conn, provider=PROVIDER, timeframe="1h", pairs=sorted(rep["pairs"]))
    rep["triangular_consistency"] = {r.pair: r.to_dict() for r in rels}
    rep["triangular_max_rel_diff"] = max((r.max_rel for r in rels if r.max_rel is not None), default=None)
    conn.close()
    text = json.dumps(rep, indent=2, sort_keys=True)
    if args.out:
        open(args.out, "w", encoding="utf-8").write(text)
    print(json.dumps({"pairs": len(rep["pairs"]), "triangular_checked": len(rels),
                      "triangular_max_rel_diff": rep["triangular_max_rel_diff"]}, indent=2))


def cmd_tracking(args, db, f):
    """Venue FX perpetual (EXECUTION_VENUE_PRICE) vs Dukascopy reference mid, on aligned CLOSED 15m bars."""
    import requests

    from app.exchange import instruments as I
    from app.market_data import venue_history as vh
    from app.market_data.reference_mapping import link_for, tracking_statistics

    def page(cursor):
        d = requests.get("https://api.bybit.com/v5/market/instruments-info",
                         params={"category": "linear", "limit": 1000, **({"cursor": cursor} if cursor else {})},
                         timeout=30).json()
        return d["result"].get("list") or [], d["result"].get("nextPageCursor")

    rows = I.collect_cursor_pages(page, source="bybit instruments-info (tracking)")
    fx_ins = [i for i in (I.parse_bybit_instrument(r) for r in rows) if i and i.asset_class == I.FX]
    get = vh.requests_getter()
    out = {"provider": PROVIDER, "venue": "bybit_linear", "timeframe": "15m", "instruments": {}}
    now = int(time.time() * 1000)
    for ins in fx_ins:
        link = link_for(ins)
        pair = ins.base_currency + ins.quote_currency
        start = max(int(ins.listed_at_ms or 0), now - args.days * 86_400_000)
        venue = [{"open_time": int(k[0]), "close": float(k[4])}
                 for k in vh.bybit_klines(get, ins.venue_symbol, "15m", start, now - 900_000)]
        with db.connect() as conn:
            ref = [{"open_time": r[0], "mid_close": r[1]} for r in conn.execute(
                "SELECT open_time, mid_close FROM fx_reference_quotes WHERE provider=? AND pair=? AND timeframe='15m' "
                "AND open_time>=?", (PROVIDER, pair, start))]
        st = tracking_statistics(venue, ref, timeframe="15m")
        out["instruments"][ins.venue_symbol] = {"link": link.to_dict() if link else None, "tracking": st.to_dict()}
        print(json.dumps({ins.venue_symbol: st.to_dict()}), flush=True)
    if args.out:
        open(args.out, "w", encoding="utf-8").write(json.dumps(out, indent=2, sort_keys=True))




def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", required=True)
    ap.add_argument("--pace", type=float, default=PACE_S, help="seconds between provider requests")
    ap.add_argument("--max-requests", type=int, default=None)
    ap.add_argument("--max-minutes", type=float, default=None)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("probe")
    p.add_argument("--probe-year", type=int, default=2026)
    p.add_argument("--probe-month", type=int, default=8)
    b = sub.add_parser("broad")
    b.add_argument("--months", type=int, default=25)
    mi = sub.add_parser("minute")
    mi.add_argument("--manifest", required=True)
    mi.add_argument("--pairs", default=None)
    mi.add_argument("--start", default=None)
    mi.add_argument("--end", default=None)
    mi.add_argument("--plan", action="store_true")
    d = sub.add_parser("deep")
    d.add_argument("--days", type=int, default=60)
    d.add_argument("--end", default=None)
    d.add_argument("--pairs", default=None)
    dv = sub.add_parser("derive")
    dv.add_argument("--manifest", default=None)
    dv.add_argument("--pairs", default=None)
    dv.add_argument("--timeframes", default="5m,15m,4h")
    q = sub.add_parser("qa")
    q.add_argument("--timeframe", default="1h")
    q.add_argument("--out", default=None)
    q.add_argument("--quarantine", action="store_true")
    r = sub.add_parser("repair-scale")
    r.add_argument("--pair", required=True)
    r.add_argument("--period", required=True, help="YYYY-MM")
    r.add_argument("--timeframe", default="1h")
    r.add_argument("--reason", default=None)
    r.add_argument("--apply", action="store_true")
    fz = sub.add_parser("freeze")
    fz.add_argument("--out", required=True)
    fz.add_argument("--window-start", default="2024-08-01T00:00:00")
    fz.add_argument("--window-end", default="2026-09-01T00:00:00")
    fz.add_argument("--force-new-path", action="store_true")
    v = sub.add_parser("validate")
    v.add_argument("--out", default=None)
    t = sub.add_parser("tracking")
    t.add_argument("--days", type=int, default=60)
    t.add_argument("--out", default=None)
    args = ap.parse_args()
    db = _db(args.db)
    f = Fetcher(pace_s=args.pace, max_requests=args.max_requests,
                deadline=(time.time() + args.max_minutes * 60) if args.max_minutes else None)
    try:
        {"probe": cmd_probe, "broad": cmd_broad, "minute": cmd_minute, "deep": cmd_deep, "derive": cmd_derive,
         "qa": cmd_qa, "repair-scale": cmd_repair_scale, "freeze": cmd_freeze, "validate": cmd_validate,
         "tracking": cmd_tracking}[args.cmd](args, db, f)
    except KeyboardInterrupt:  # completed periods are already committed; the next run resumes
        print(json.dumps({"interrupted": True, "requests": f.requests}), flush=True)
        return 130
    except ProviderUnavailable as exc:
        print(json.dumps({"stopped": "PROVIDER_UNAVAILABLE", "detail": str(exc), "requests": f.requests}), flush=True)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
