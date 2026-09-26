#!/usr/bin/env python3
"""Machine-readable dataset coverage evidence (Sections 10.10, 11.13, 12.9) -- read-only, reproducible.

Every artifact is deterministic JSON (sorted keys, repo-relative names only, no credentials) with a
``content_hash`` over everything except ``generated_at``; re-running on an unchanged database reproduces the
same hash.

    python scripts/dataset_coverage.py crypto-broad --canonical-db <cosmicforge.db> --out docs/research/coverage/crypto_broad_binance_v1.coverage.json
    python scripts/dataset_coverage.py crypto-deep  --db data/research/crypto_deep_binance.db --out docs/research/coverage/crypto_deep_binance_v1.coverage.json
    python scripts/dataset_coverage.py features     --db data/research/crypto_deep_binance.db --out docs/research/coverage/crypto_features_binance_v1.coverage.json
    python scripts/dataset_coverage.py fx           --db data/research/fx_reference_dukascopy.db --out docs/research/coverage/fx_dukascopy_v1.coverage.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [os.path.join(REPO, "backends", "bot-backend"), os.path.join(REPO, "backends", "shared")]

BROAD = os.path.join(REPO, "docs", "research", "cati_crypto_universe_binance_v1.json")
DEEP = os.path.join(REPO, "docs", "research", "cati_crypto_deep_universe_binance_v1.json")
FX = os.path.join(REPO, "docs", "research", "cati_fx_universe_dukascopy_v1.json")
COVERAGE_VERSION = "dataset-coverage-v1"
MIN = 60_000


def _ro(path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def _iso(ms):
    return None if ms is None else datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()


def _write(out: str, body: Dict[str, Any]) -> Dict[str, Any]:
    body = {**body, "coverage_version": COVERAGE_VERSION}
    content = json.dumps(body, sort_keys=True, default=str)
    art = {**body, "content_hash": hashlib.sha256(content.encode()).hexdigest(),
           "generated_at": datetime.now(timezone.utc).isoformat()}
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(art, fh, indent=2, sort_keys=True, default=str)
    return art


def _gaps(open_times, step, start, end, classify):
    from app.market_data.gaps import Gap, find_gaps, summarize

    gaps = [Gap(a, b, n, classify(a, b)) for a, b, n in find_gaps(open_times, step, start_ms=start, end_ms=end)]
    return summarize(gaps), gaps


# ── crypto broad (canonical historical_candles, frozen v1) ────────────────────

def crypto_broad(args) -> Dict[str, Any]:
    from app.market_data.gaps import classify_crypto_gap
    from app.market_data.universe import load_frozen_universe

    m = load_frozen_universe(BROAD)
    s, e, step = int(m["window_start_ms"]), int(m["window_end_ms"]), 15 * MIN
    expected = (e - s) // step
    conn = _ro(args.canonical_db)
    members, agg = [], {"symbols": 0, "rows": 0, "complete_symbols": 0, "gaps": 0, "invalid_rows": 0,
                        "duplicate_rows": 0, "misaligned_rows": 0}
    where = "symbol=? AND interval='15m' AND data_source='binance' AND market_type='crypto' AND open_time>=? AND " \
            "open_time<?"
    for mem in m["members"]:
        sym = mem["venue_symbol"]
        n, lo, hi, distinct, inval, misal, dv, fa, fb = conn.execute(
            f"SELECT COUNT(*), MIN(open_time), MAX(open_time), COUNT(DISTINCT open_time), "
            f"SUM(high<open OR high<close OR high<low OR low>open OR low>close OR volume<0 OR open<=0), "
            f"SUM(open_time % {step} != 0), MAX(data_version), MIN(fetched_at), MAX(fetched_at) FROM historical_candles "
            f"WHERE {where}", (sym, s, e)).fetchone()
        times = (r[0] for r in conn.execute(f"SELECT open_time FROM historical_candles WHERE {where} ORDER BY open_time",
                                            (sym, s, e)))
        summ, _ = _gaps(times, step, s, e, lambda a, b: classify_crypto_gap(a, b, listed_at_ms=mem.get("listed_at_ms")))
        gaps = sum(v["gaps"] for v in summ["by_classification"].values())
        members.append({
            "symbol": sym, "canonical_instrument_id": mem["canonical_instrument_id"], "venue": m["source_venue"],
            "product_type": mem["product_type"], "listed_at_ms": mem.get("listed_at_ms"),
            "listed_at": _iso(mem.get("listed_at_ms")), "requested_start_ms": s, "requested_end_ms": e,
            "actual_start_ms": lo, "actual_last_open_ms": hi, "rows": n, "expected_rows": expected,
            "coverage_pct": round(100.0 * n / expected, 6), "gaps": gaps, "gap_classification": summ,
            "duplicate_rows": n - distinct, "invalid_rows": int(inval or 0), "misaligned_rows": int(misal or 0),
            "provider_failures": "NOT_RECORDED_LEGACY_IMPORTER",
            "history_status": "COMPLETE" if (n == expected and not gaps) else "INCOMPLETE",
            "data_source": "binance", "data_version": dv or "LEGACY_UNVERSIONED",
            "fetched_at_range": [fa, fb]})
        agg["symbols"] += 1
        agg["rows"] += n
        agg["complete_symbols"] += int(n == expected and not gaps)
        agg["gaps"] += gaps
        agg["invalid_rows"] += int(inval or 0)
        agg["duplicate_rows"] += n - distinct
        agg["misaligned_rows"] += int(misal or 0)
    total = conn.execute("SELECT COUNT(*) FROM historical_candles WHERE interval='15m' AND data_source='binance' AND "
                         "market_type='crypto' AND open_time>=? AND open_time<? AND symbol IN (%s)" %
                         ",".join("?" * len(m["members"])), (s, e, *[x["venue_symbol"] for x in m["members"]])
                         ).fetchone()[0]
    conn.close()
    agg["reconciles_to_db"] = total == agg["rows"]
    agg["young_symbols"] = 0
    agg["young_symbols_note"] = "v1 excluded symbols younger than 731 days (manifest rejections: LISTING_TOO_RECENT)"
    return _write(args.out, {
        "scope": "CRYPTO_BROAD", "universe_id": m["universe_id"], "universe_hash": m["universe_hash"],
        "timeframe": "15m", "window_start": _iso(s), "window_end": _iso(e), "expected_rows_per_symbol": expected,
        "members": members, "aggregate": agg,
        "provenance": {
            "store": "canonical historical_candles (Section 22 input; identity/hash semantics unchanged)",
            "venue": "binance_usdm", "environment": "LIVE (public production host fapi.binance.com)",
            "environment_evidence": "backends/bot-backend/scripts/ml/backfill_historical_candles.py BINANCE_API_BASE",
            "product_type": "PERPETUAL", "close_time_rule": "open_time + 15m - 1ms (Binance kline close)",
            "canonical_identity": "per member canonical_instrument_id in the frozen v1 manifest",
            "legacy_limitations": [
                "historical_candles rows carry no per-row venue / environment / canonical id / close_time column; "
                "they are established per series by the manifest and the importer, not guessed per row",
                "data_version is NULL on every v1 row (LEGACY_UNVERSIONED); fetched_at is the ingest timestamp",
                "acquisition failures were not logged per period by the legacy importer",
                "survivorship bias: members were TRADING at generation time; delisted symbols are absent (declared)"]}})


# ── crypto deep ──────────────────────────────────────────────────────────────

def crypto_deep(args) -> Dict[str, Any]:
    from app.market_data.gaps import classify_crypto_gap
    from app.market_data.universe import load_deep_universe

    m = load_deep_universe(DEEP)
    conn = _ro(args.db)
    end = int(m["window_end_ms"])
    members, agg = [], {"symbols": 0, "rows_1m": 0, "rows_5m": 0, "periods_done": 0, "periods_total": 0,
                        "failed_periods": 0, "unknown_gaps": 0, "insufficient_history": 0}
    sys.path.insert(0, os.path.join(REPO, "scripts"))
    from acquire_crypto_deep_dataset import SOURCE, VENUE, months

    for mem in m["members"]:
        sym, start = mem["venue_symbol"], int(mem["requested_start_ms"])
        periods = [p for p, _, _ in months(start, end)]
        log = {r[0]: (r[1], r[2]) for r in conn.execute(
            "SELECT period, status, reason FROM market_ingest_log WHERE venue=? AND venue_symbol=? AND "
            "dataset='klines' AND timeframe='1m'", (VENUE, sym))}
        failed = [p for p, (st, _) in log.items() if st == "FAILED"]
        done = [p for p in periods if log.get(p, (None,))[0] in ("FETCHED", "EMPTY", "NOT_LISTED")]
        stats = {}
        for tf in ("1m", "5m"):
            stats[tf] = conn.execute("SELECT COUNT(*), MIN(open_time), MAX(open_time), SUM(quote_volume IS NULL) FROM "
                                     "market_candles WHERE venue=? AND venue_symbol=? AND timeframe=? AND source=?",
                                     (VENUE, sym, tf, SOURCE)).fetchone()
        complete = len(done) == len(periods)
        fail_windows = []
        for p in failed:
            y, mo = (int(x) for x in p.split("-"))
            a = int(datetime(y, mo, 1, tzinfo=timezone.utc).timestamp() * 1000)
            b = int(datetime(y + (mo == 12), 1 if mo == 12 else mo + 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
            fail_windows.append((a, b))
        summ = None
        if complete:
            times = (r[0] for r in conn.execute(
                "SELECT open_time FROM market_candles WHERE venue=? AND venue_symbol=? AND timeframe='1m' AND source=? "
                "ORDER BY open_time", (VENUE, sym, SOURCE)))
            summ, _ = _gaps(times, MIN, start, end, lambda a, b: classify_crypto_gap(
                a, b, listed_at_ms=mem.get("listed_at_ms"), failed_periods=fail_windows))
        expected = (end - start) // MIN
        members.append({
            "rank": mem["rank"], "symbol": sym, "canonical_instrument_id": mem["canonical_instrument_id"],
            "venue": VENUE, "product_type": mem["product_type"], "listed_at": _iso(mem.get("listed_at_ms")),
            "history_status": mem["history_status"], "requested_start": _iso(start), "requested_end": _iso(end),
            "actual_start": _iso(stats["1m"][1]), "actual_last_open": _iso(stats["1m"][2]),
            "rows_1m": stats["1m"][0], "expected_rows_1m": expected,
            "coverage_pct_1m": round(100.0 * stats["1m"][0] / expected, 4) if expected else None,
            "rows_5m": stats["5m"][0], "periods_total": len(periods), "periods_done": len(done),
            "failed_periods": sorted(failed), "acquisition_status": "COMPLETE" if complete else "IN_PROGRESS",
            "gap_classification": summ if summ is not None else "PENDING_ACQUISITION",
            "source": SOURCE, "environment": "REAL (public production host)"})
        agg["symbols"] += 1
        agg["rows_1m"] += stats["1m"][0]
        agg["rows_5m"] += stats["5m"][0]
        agg["periods_done"] += len(done)
        agg["periods_total"] += len(periods)
        agg["failed_periods"] += len(failed)
        agg["insufficient_history"] += int(mem["history_status"] == "INSUFFICIENT_HISTORY")
        if summ:
            agg["unknown_gaps"] += summ["by_classification"].get("UNKNOWN_GAP", {}).get("gaps", 0)
    conn.close()
    agg["complete_symbols"] = sum(1 for x in members if x["acquisition_status"] == "COMPLETE")
    return _write(args.out, {"scope": "CRYPTO_DEEP", "universe_id": m["universe_id"],
                             "universe_hash": m["universe_hash"], "parent_universe_hash": m["parent_universe_hash"],
                             "timeframes": m["timeframes"], "members": members, "aggregate": agg})


# ── supplemental features ────────────────────────────────────────────────────

def features(args) -> Dict[str, Any]:
    from app.market_data.universe import load_deep_universe

    m = load_deep_universe(DEEP)
    conn = _ro(args.db)
    feats = ("funding_rate", "mark_price", "index_price", "basis_bps", "open_interest", "spread_bps",
             "book_bid_depth", "book_ask_depth", "liquidations_long", "liquidations_short")
    per, agg = {}, {}
    for mem in m["members"]:
        sym = mem["venue_symbol"]
        per[sym] = {}
        for f in feats:
            av, lo, hi = conn.execute("SELECT COUNT(*), MIN(observed_at), MAX(observed_at) FROM "
                                      "market_feature_observations WHERE venue='binance_usdm' AND venue_symbol=? AND "
                                      "feature=? AND status='AVAILABLE'", (sym, f)).fetchone()
            reasons = sorted({r[0] for r in conn.execute(
                "SELECT unavailable_reason FROM market_feature_observations WHERE venue='binance_usdm' AND "
                "venue_symbol=? AND feature=? AND status='UNAVAILABLE'", (sym, f))})
            if av:
                state = "AVAILABLE" if not reasons else "PARTIAL_AVAILABLE"
            elif reasons:
                state = reasons[0].split(":", 1)[0]
            else:
                state = "NOT_ACQUIRED"
            per[sym][f] = {"state": state, "available_rows": av, "first": _iso(lo), "last": _iso(hi),
                           "unavailable_reasons": reasons}
            agg.setdefault(f, {}).setdefault(state, 0)
            agg[f][state] += 1
    conn.close()
    return _write(args.out, {"scope": "CRYPTO_SUPPLEMENTAL_FEATURES", "venue": "binance_usdm",
                             "universe_hash": m["universe_hash"], "members": per,
                             "aggregate_states_by_feature": agg,
                             "semantics": "missing is never 0: an unavailable observation has value NULL and a "
                                          "reason (NO_HISTORICAL_ENDPOINT / NOT_SUPPORTED_BY_PROVIDER / "
                                          "PROVIDER_FAILURE); NOT_ACQUIRED = no observation recorded yet"})


# ── FX ───────────────────────────────────────────────────────────────────────

def fx(args) -> Dict[str, Any]:
    from app.market_data import fx_scale
    from app.market_data.fx_universe import load_fx_universe
    from app.market_data.gaps import GAP_POLICY_VERSION, classify_fx_gap, fx_expected_bars

    m = load_fx_universe(FX)
    s, e = int(m["window_start_ms"]), int(m["window_end_ms"])
    conn = _ro(args.db)
    pairs = [x["pair"] for x in m["members"]]
    qa = fx_scale.scale_report(conn, provider=m["provider"], timeframe="1h", pairs=pairs)
    out: Dict[str, Any] = {}
    agg: Dict[str, Any] = {"pairs": len(pairs)}
    for tf, step, period_kind in (("1h", 60 * MIN, "month"), ("1m", MIN, "day")):
        tagg = {"rows": 0, "missing_side": 0, "negative_spread": 0, "gaps_by_class": {}, "complete_pairs": 0}
        for pair in pairs:
            n, bid, ask, both, neg, lo, hi = conn.execute(
                "SELECT COUNT(*), SUM(bid_close IS NOT NULL), SUM(ask_close IS NOT NULL), "
                "SUM(bid_close IS NOT NULL AND ask_close IS NOT NULL), SUM(spread_close<0), MIN(open_time), "
                "MAX(open_time) FROM fx_reference_quotes WHERE provider=? AND pair=? AND timeframe=? AND "
                "open_time>=? AND open_time<?", (m["provider"], pair, tf, s, e)).fetchone()
            log = {}
            for period, status in conn.execute("SELECT period, status FROM fx_reference_ingest_log WHERE provider=? "
                                               "AND pair=? AND timeframe=?", (m["provider"], pair, tf)):
                prev = log.get(period)
                log[period] = status if prev in (None, status) else ("FAILED" if "FAILED" in (prev, status) else
                                                                       "NO_FILE" if "NO_FILE" in (prev, status) else
                                                                       status)
            periods_expected = ((e - s) // (86_400_000)) if tf == "1m" else None
            done = sum(1 for v in log.values() if v in ("FETCHED", "NO_FILE", "EMPTY"))
            times = (r[0] for r in conn.execute("SELECT open_time FROM fx_reference_quotes WHERE provider=? AND pair=? "
                                                "AND timeframe=? AND open_time>=? AND open_time<? ORDER BY open_time",
                                                (m["provider"], pair, tf, s, e)))
            acquired = n > 0 and (tf == "1h" or done >= (periods_expected or 0))
            summ = _gaps(times, step, s, e, lambda a, b: classify_fx_gap(a, b, step, ingest_status=log,
                                                                         period_kind=period_kind))[0] if acquired \
                else "PENDING_ACQUISITION"
            rec = {"pair": pair, "provider": m["provider"], "timeframe": tf, "requested_start": _iso(s),
                   "requested_end": _iso(e), "actual_start": _iso(lo), "actual_last_open": _iso(hi), "rows": n,
                   "bid_rows": int(bid or 0), "ask_rows": int(ask or 0), "complete_rows": int(both or 0),
                   "missing_side": n - int(both or 0), "negative_spread": int(neg or 0),
                   "provider_no_file_periods": sorted(p for p, v in log.items() if v == "NO_FILE"),
                   "failed_periods": sorted(p for p, v in log.items() if v == "FAILED"),
                   "periods_logged": len(log), "periods_done": done,
                   "gap_classification": summ,
                   "scale_status": qa["pairs"].get(pair, {}).get("status"),
                   "cross_rate_status": ("PASS" if qa["relations"][pair]["passed"] else "FAIL")
                   if pair in qa["relations"] else "NO_RELATION_USD_LEG"}
            if tf == "1h" or acquired:
                rec["expected_market_open_bars"] = fx_expected_bars(s, e, step) if tf == "1h" else None
            unknown = summ["by_classification"].get("UNKNOWN_GAP", {}).get("gaps", 0) if isinstance(summ, dict) else None
            rec["coverage_status"] = ("PENDING_ACQUISITION" if not acquired else
                                      "COMPLETE_TO_PROVIDER_LIMITS" if not rec["failed_periods"] else "FAILED_PERIODS")
            rec["unknown_gaps"] = unknown
            out.setdefault(pair, {})[tf] = rec
            tagg["rows"] += n
            tagg["missing_side"] += rec["missing_side"]
            tagg["negative_spread"] += rec["negative_spread"]
            tagg["complete_pairs"] += int(rec["coverage_status"] == "COMPLETE_TO_PROVIDER_LIMITS")
            if isinstance(summ, dict):
                for k, v in summ["by_classification"].items():
                    g = tagg["gaps_by_class"].setdefault(k, {"gaps": 0, "missing_bars": 0})
                    g["gaps"] += v["gaps"]
                    g["missing_bars"] += v["missing_bars"]
        agg[tf] = tagg
    derived = {tf: conn.execute("SELECT COUNT(*), COUNT(DISTINCT pair) FROM fx_reference_quotes WHERE provider=? AND "
                                "timeframe=?", (m["provider"], tf)).fetchone() for tf in ("5m", "15m", "4h")}
    agg["derived"] = {tf: {"rows": r[0], "pairs": r[1]} for tf, r in derived.items()}
    rel = [r for r in qa["relations"].values() if r["aligned_bars"]]
    agg["cross_rate"] = {"relations": len(rel), "aligned_bars": sum(r["aligned_bars"] for r in rel),
                         "worst_median_rel_error": max((r["median_rel_error"] for r in rel), default=None),
                         "worst_p95_rel_error": max((r["p95_rel_error"] for r in rel), default=None),
                         "worst_bar_rel_error": max((r["max_rel_error"] for r in rel), default=None),
                         "failing": sorted(r["pair"] for r in rel if not r["passed"])}
    agg["repairs"] = [dict(zip(("pair", "timeframe", "period", "reason", "algorithm_version", "rows_removed",
                                "rows_inserted", "validation"), r[:7] + (json.loads(r[7]),)))
                      for r in conn.execute("SELECT pair, timeframe, period, reason, algorithm_version, rows_removed, "
                                            "rows_inserted, validation_json FROM fx_reference_repairs ORDER BY pair")]
    conn.close()
    return _write(args.out, {"scope": "FX_REFERENCE", "universe_id": m["universe_id"],
                             "universe_hash": m["universe_hash"], "gap_policy_version": GAP_POLICY_VERSION,
                             "scale_qa_version": qa["version"], "pairs": out, "aggregate": agg,
                             "excluded": m["excluded"]})


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("crypto-broad")
    a.add_argument("--canonical-db", required=True)
    a.add_argument("--out", required=True)
    for name in ("crypto-deep", "features", "fx"):
        p = sub.add_parser(name)
        p.add_argument("--db", required=True)
        p.add_argument("--out", required=True)
    args = ap.parse_args()
    art = {"crypto-broad": crypto_broad, "crypto-deep": crypto_deep, "features": features, "fx": fx}[args.cmd](args)
    print(json.dumps({"out": args.out, "content_hash": art["content_hash"], "aggregate": art.get("aggregate")},
                     default=str)[:2000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
