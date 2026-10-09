"""Daily Binance USD-M research dataset: discover -> mirror -> normalize -> measure -> freeze (Step 2.3).

    python -m app.market_data.daily_dataset build  --store ../../data/research/binance_usdm_daily_v1
    python -m app.market_data.daily_dataset verify --store ../../data/research/binance_usdm_daily_v1

Raw archive files and the normalized tables stay OUT of Git (reproducible, large). What is committed is the
manifest, the per-symbol coverage report, the per-file raw inventory and the exchange metadata snapshot, so a
result can always name -- and anyone can rebuild and re-verify -- the exact data it used.

Rules that never bend:

* nothing is filled forward, interpolated or repaired; a missing day is a missing day and is counted;
* a duplicate keeps its first occurrence; an invalid bar is quarantined (removed and listed), never corrected;
* listing and delisting are taken from where the archive's own files start and stop -- a reconstruction that
  is labelled as one -- and today's exchange metadata is stored as a snapshot, never presented as history;
* building twice from the same raw files gives the same content hashes.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np

from app.trading_intelligence.hashing import stable_hash

from . import binance_archive as BA

DATASET_ID = "binance_usdm_daily"
DATASET_VERSION = "v1"
TRANSFORMATION_VERSION = "daily-dataset-transform-1"
MANIFEST_SCHEMA = "daily-dataset-manifest-v1"
FIRST_MONTH, LAST_MONTH = "2020-01", "2026-09"
USDT_PERPETUAL = r"^[A-Z0-9]+USDT(SETTLED)*$"
EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
EPOCH = date(1970, 1, 1)
MIDNIGHT_WINDOW_MS = 3_600_000          # a funding event in the first hour of the day is the 00:00 event
KLINE_COLUMNS = ("day", "open", "high", "low", "close", "volume", "quote_volume", "trades")
MISSING_DATA_POLICY = {
    "price_bars": "NEVER_FILLED: a missing daily bar stays missing and is counted",
    "duplicates": "FIRST_OCCURRENCE_KEPT_AND_COUNTED",
    "invalid_bars": "QUARANTINED: removed from the normalized table, listed in the coverage report",
    "out_of_order_rows": "SORTED_BY_TIME_AND_COUNTED",
    "funding": "NEVER_ASSUMED_ZERO: missing events are counted here and charged by the evaluator's registered rule",
    "days_missing_from_monthly_files": "TAKEN_FROM_THE_DAILY_FILES_OF_THE_SAME_ARCHIVE where they exist (same source, "
                                       "same checksum rule); counted per symbol; otherwise left missing",
    "non_trading_bars": "A bar with zero trades (the archive keeps printing them after a contract is settled) is "
                        "kept in the table and NEVER used as a price: the loader treats it as no bar",
}
DAILY_KLINES_ROOT = "data/futures/um/daily/klines/"


def day_index(text: str) -> int:
    return (date.fromisoformat(text) - EPOCH).days


def day_text(index: int) -> str:
    return (EPOCH + timedelta(days=int(index))).isoformat()


# ---------------------------------------------------------------------- discovery and mirror
def discover(*, pattern: str = USDT_PERPETUAL, first_month: str = FIRST_MONTH, last_month: str = LAST_MONTH,
             get: BA.Getter = BA.http_get, workers: int = 16) -> Dict[str, Any]:
    """Every USDT-margined perpetual the archive has ever published, with its monthly files. The symbol list
    is the archive's, so a contract delisted years ago is found exactly like one trading today."""
    everything = BA.archive_symbols(get=get)
    rx = re.compile(pattern)
    symbols = [s for s in everything if rx.fullmatch(s)]

    def files(symbol: str) -> Tuple[str, Dict[str, List[BA.ArchiveObject]]]:
        return symbol, {kind: BA.symbol_files(symbol, kind, first_month=first_month, last_month=last_month, get=get)
                        for kind in ("klines", "funding")}

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        found = dict(pool.map(files, symbols))
    return {"archive_symbols": len(everything), "pattern": pattern, "first_month": first_month,
            "last_month": last_month, "not_in_scope": sorted(set(everything) - set(symbols)),
            "files": {s: {k: [{"key": o.key, "size": o.size, "last_modified": o.last_modified} for o in v]
                          for k, v in found[s].items()} for s in sorted(found)}}


def mirror(store: Path, discovery: Mapping[str, Any], *, get: BA.Getter = BA.http_get, workers: int = 16,
           progress=None) -> List[Dict[str, str]]:
    keys = sorted(f["key"] for s in discovery["files"].values() for kind in ("klines", "funding", "klines_daily")
                  for f in s.get(kind, ()))
    return BA.ArchiveMirror(Path(store) / "raw", get=get).sync(keys, workers=workers, progress=progress)


def complete_from_daily_files(store: Path, discovery: Dict[str, Any], missing_days: Mapping[str, Sequence[int]], *,
                              get: BA.Getter = BA.http_get, workers: int = 16) -> Dict[str, Any]:
    """Days that a symbol's MONTHLY files skip are looked up in the archive's DAILY files for the same symbol.
    Same source, same checksum rule; nothing is interpolated. A day the archive does not have stays missing.
    The result is written into ``discovery`` so a rebuild takes exactly the same files."""
    wanted = {f"{DAILY_KLINES_ROOT}{s}/1d/{s}-1d-{day_text(d)}.zip": s for s, days in missing_days.items() for d in days}
    raw = BA.ArchiveMirror(Path(store) / "raw", get=get)
    results = raw.sync(sorted(wanted), workers=workers)
    found = [r for r in results if r["status"] != "FAILED"]
    absent = sorted(r["key"] for r in results if r["status"] == "FAILED" and "not in the archive" in r.get("error", ""))
    failed = sorted(r["key"] for r in results if r["status"] == "FAILED" and r["key"] not in absent)
    for s in discovery["files"].values():
        s["klines_daily"] = []
    for r in sorted(found, key=lambda r: r["key"]):
        discovery["files"][wanted[r["key"]]]["klines_daily"].append(
            {"key": r["key"], "size": raw.local(r["key"]).stat().st_size, "last_modified": ""})
    discovery["daily_completion"] = {"days_requested": len(wanted), "days_found": len(found),
                                     "days_not_in_archive": len(absent), "failed": failed}
    return discovery["daily_completion"]


def metadata_snapshot(*, get: BA.Getter = BA.http_get, fetched_at: Optional[str] = None) -> Dict[str, Any]:
    """Today's exchange metadata, reduced to what research uses. It is a SNAPSHOT: it says nothing reliable
    about what a contract's filters or status were in the past."""
    raw = get(EXCHANGE_INFO_URL)
    out = {}
    for s in json.loads(raw)["symbols"]:
        f = {x["filterType"]: x for x in s.get("filters", [])}
        out[s["symbol"]] = {
            "contract_type": s.get("contractType"), "underlying_type": s.get("underlyingType"),
            "underlying_sub_type": sorted(s.get("underlyingSubType") or []), "status": s.get("status"),
            "base_asset": s.get("baseAsset"), "quote_asset": s.get("quoteAsset"), "margin_asset": s.get("marginAsset"),
            "onboard_date_ms": s.get("onboardDate"), "delivery_date_ms": s.get("deliveryDate"),
            "market_step_size": (f.get("MARKET_LOT_SIZE") or f.get("LOT_SIZE") or {}).get("stepSize"),
            "market_min_qty": (f.get("MARKET_LOT_SIZE") or f.get("LOT_SIZE") or {}).get("minQty"),
            "min_notional": (f.get("MIN_NOTIONAL") or {}).get("notional")}
    body = {"schema": "exchange-metadata-snapshot-v1", "source": EXCHANGE_INFO_URL,
            "point_in_time": False, "symbols": dict(sorted(out.items()))}
    return {**body, "content_hash": stable_hash(body), "raw_sha256": hashlib.sha256(raw).hexdigest(),
            "fetched_at": fetched_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}


# ---------------------------------------------------------------------- normalization
def clean_klines(rows: Sequence[Tuple[int, float, float, float, float, float, float, int]]) -> Dict[str, Any]:
    """Validate one symbol's raw rows (all files, file order). Returns the normalized arrays and what was found."""
    out_of_order = sum(1 for i in range(1, len(rows)) if rows[i][0] < rows[i - 1][0])
    seen, kept, duplicates, invalid = set(), [], 0, []
    for ts, o, h, l, c, v, qv, n in sorted(rows, key=lambda r: r[0]):   # stable: the first occurrence stays first
        if ts in seen:
            duplicates += 1
            continue
        seen.add(ts)
        vals = (o, h, l, c, v, qv)
        ok = (ts % BA.DAY_MS == 0 and all(np.isfinite(x) for x in vals) and min(o, h, l, c) > 0
              and h >= max(o, c) and l <= min(o, c) and v >= 0 and qv >= 0)
        if not ok:
            invalid.append(int(ts))
            continue
        kept.append((ts // BA.DAY_MS, o, h, l, c, v, qv, n))
    arr = np.array(kept, dtype=np.float64).reshape(-1, 8)
    days = arr[:, 0].astype(np.int64)
    missing = []
    if len(days):
        gaps = np.flatnonzero(np.diff(days) > 1)
        missing = [(int(days[i]) + 1, int(days[i + 1]) - 1) for i in gaps]
    return {"days": days, "values": arr[:, 1:7], "trades": arr[:, 7].astype(np.int64), "duplicates": duplicates,
            "invalid": invalid, "out_of_order": out_of_order, "missing_ranges": missing,
            "zero_volume_days": int(np.sum(arr[:, 6] == 0)) if len(arr) else 0}


def clean_funding(rows: Sequence[Tuple[int, int, float]]) -> Dict[str, Any]:
    out_of_order = sum(1 for i in range(1, len(rows)) if rows[i][0] < rows[i - 1][0])
    seen, kept, duplicates, invalid = set(), [], 0, 0
    for ts, hours, rate in sorted(rows, key=lambda r: r[0]):
        if ts in seen:
            duplicates += 1
        elif not (np.isfinite(rate) and abs(rate) < 0.2 and 1 <= hours <= 24):
            invalid += 1
        else:
            kept.append((ts, hours, rate))
        seen.add(ts)
    return {"time_ms": np.array([k[0] for k in kept], dtype=np.int64),
            "interval_hours": np.array([k[1] for k in kept], dtype=np.int64),
            "rate": np.array([k[2] for k in kept], dtype=np.float64),
            "duplicates": duplicates, "invalid": invalid, "out_of_order": out_of_order}


def _content_hash(*arrays: np.ndarray) -> str:
    h = hashlib.sha256()
    for a in arrays:
        kind = "<i8" if a.dtype.kind in "iu" else "<f8"
        h.update(np.ascontiguousarray(a, dtype=kind).tobytes())     # little-endian: the same bytes on every platform
    return h.hexdigest()


def funding_by_day(funding: Mapping[str, np.ndarray]) -> Dict[int, Tuple[float, float, float, float, int]]:
    """Per day: ``(midnight rate, later positive, later negative, hours covered, events)``. Positive and
    negative later rates are kept apart because the evaluator treats them differently on a stop-out day."""
    out: Dict[int, List[float]] = {}
    for ts, hours, rate in zip(funding["time_ms"], funding["interval_hours"], funding["rate"]):
        d = out.setdefault(int(ts // BA.DAY_MS), [0.0, 0.0, 0.0, 0.0, 0])
        if int(ts % BA.DAY_MS) < MIDNIGHT_WINDOW_MS:
            d[0] += float(rate)
        elif rate > 0:
            d[1] += float(rate)
        else:
            d[2] += float(rate)
        d[3] += float(hours)
        d[4] += 1
    return {k: (v[0], v[1], v[2], v[3], int(v[4])) for k, v in out.items()}


def normalize(store: Path, discovery: Mapping[str, Any], sync: Sequence[Mapping[str, str]],
              metadata: Mapping[str, Any]) -> Dict[str, Any]:
    """Parse every verified raw file into two tables and a per-symbol quality record. Deterministic."""
    import pandas as pd

    store = Path(store)
    raw = BA.ArchiveMirror(store / "raw")
    hashes = {r["key"]: r["sha256"] for r in sync if r["status"] != "FAILED"}
    failed = sorted(r["key"] for r in sync if r["status"] == "FAILED")
    k_frames, f_frames, symbols, inventory, missing_days = [], [], {}, [], {}
    for symbol in sorted(discovery["files"]):
        files = discovery["files"][symbol]
        k_rows, f_rows, unreadable = [], [], []
        for kind, reader, sink in (("klines", BA.read_kline_zip, k_rows), ("klines_daily", BA.read_kline_zip, k_rows),
                                   ("funding", BA.read_funding_zip, f_rows)):
            for f in files.get(kind, ()):
                if f["key"] not in hashes:
                    continue
                inventory.append((f["key"], f["size"], hashes[f["key"]]))
                try:
                    sink.extend(reader(raw.local(f["key"])))
                except Exception as exc:
                    unreadable.append({"key": f["key"], "error": str(exc)[:120]})
        k, fu = clean_klines(k_rows), clean_funding(f_rows)
        days = k["days"]
        if len(days):
            k_frames.append(pd.DataFrame({"symbol": symbol, "day": days.astype(np.int32), **{
                c: k["values"][:, i] for i, c in enumerate(KLINE_COLUMNS[1:7])}, "trades": k["trades"]}))
        if len(fu["time_ms"]):
            f_frames.append(pd.DataFrame({"symbol": symbol, "time_ms": fu["time_ms"],
                                          "interval_hours": fu["interval_hours"].astype(np.int16), "rate": fu["rate"]}))
        per_day = funding_by_day(fu)
        bar_days = set(int(d) for d in days)
        no_funding = sorted(d for d in bar_days if d not in per_day)
        partial = sorted(d for d in bar_days if d in per_day and per_day[d][3] < 24.0)
        first, last = (int(days[0]), int(days[-1])) if len(days) else (None, None)
        if k["missing_ranges"]:
            missing_days[symbol] = [d for a, b in k["missing_ranges"] for d in range(a, b + 1)]
        traded = np.flatnonzero(k["trades"] > 0)
        from_daily = sum(len(BA.read_kline_zip(raw.local(f["key"]))) for f in files.get("klines_daily", ())
                         if f["key"] in hashes)
        meta = metadata["symbols"].get(symbol)
        months = sorted(f["key"][-11:-4] for f in files["klines"])
        symbols[symbol] = {
            "symbol": symbol, "market_type": "USDM_PERPETUAL_USDT_MARGINED",
            "listing_status_source": "EXCHANGE_METADATA_SNAPSHOT" if meta else "ARCHIVE_COVERAGE_ONLY",
            "status_in_snapshot": (meta or {}).get("status"),
            "first_available_day": day_text(first) if first is not None else None,
            "last_available_day": day_text(last) if last is not None else None,
            "expected_candle_count": (last - first + 1) if first is not None else 0,
            "actual_candle_count": int(len(days)),
            "missing_candles": int(sum(b - a + 1 for a, b in k["missing_ranges"])),
            "missing_ranges": [[day_text(a), day_text(b)] for a, b in k["missing_ranges"][:50]],
            "duplicate_candles": k["duplicates"], "invalid_candles": len(k["invalid"]),
            "invalid_candle_times_ms": k["invalid"][:50], "out_of_order_rows": k["out_of_order"],
            "zero_volume_days": k["zero_volume_days"],
            "candles_from_daily_files": int(from_daily),
            "non_trading_bars": int(len(days) - len(traded)),
            "first_traded_day": day_text(days[traded[0]]) if len(traded) else None,
            "last_traded_day": day_text(days[traded[-1]]) if len(traded) else None,
            "non_trading_bars_after_last_trade": int(len(days) - 1 - traded[-1]) if len(traded) else int(len(days)),
            "kline_months": len(months), "first_kline_month": months[0] if months else None,
            "last_kline_month": months[-1] if months else None,
            "funding_records": int(len(fu["time_ms"])), "funding_duplicates": fu["duplicates"],
            "funding_invalid": fu["invalid"], "funding_out_of_order_rows": fu["out_of_order"],
            "funding_gaps": len(no_funding) + len(partial),
            "bar_days_without_any_funding_record": len(no_funding),
            "bar_days_with_incomplete_funding": len(partial),
            "first_bar_day_without_funding": day_text(no_funding[0]) if no_funding else None,
            "raw_file_count": sum(len(files.get(kind, ())) for kind in ("klines", "klines_daily", "funding")),
            "raw_file_checksums": stable_hash(sorted((f["key"], hashes.get(f["key"], "MISSING"))
                                                     for kind in ("klines", "klines_daily", "funding")
                                                     for f in files.get(kind, ()))),
            "normalized_file_checksums": {
                "klines": _content_hash(days, k["values"], k["trades"]),
                "funding": _content_hash(fu["time_ms"], fu["interval_hours"], fu["rate"])},
            "unreadable_files": unreadable, "transformation_version": TRANSFORMATION_VERSION,
            "source_urls": [f"{BA.ARCHIVE_URL}/{BA.KLINES_ROOT}{symbol}/1d/", f"{BA.ARCHIVE_URL}/{BA.FUNDING_ROOT}{symbol}/"],
        }
    out = store / "normalized"
    out.mkdir(parents=True, exist_ok=True)
    klines = pd.concat(k_frames, ignore_index=True) if k_frames else pd.DataFrame()
    funding = pd.concat(f_frames, ignore_index=True) if f_frames else pd.DataFrame()
    klines.to_parquet(out / "klines_1d.parquet", index=False, compression="zstd")
    funding.to_parquet(out / "funding.parquet", index=False, compression="zstd")
    return {"symbols": symbols, "inventory": sorted(inventory), "failed_downloads": failed,
            "missing_days": missing_days, "rows": {"klines": int(len(klines)), "funding": int(len(funding))}}


# ---------------------------------------------------------------------- manifest
def _hash_symbols(symbols: Mapping[str, Mapping[str, Any]], kind: str) -> str:
    return stable_hash([(s, symbols[s]["normalized_file_checksums"][kind]) for s in sorted(symbols)])


def inventory_bytes(inventory: Sequence[Tuple[str, int, str]]) -> bytes:
    """``key,size,sha256`` per raw file, gzip with a zero timestamp so the bytes are reproducible."""
    text = "key,size,sha256\n" + "".join(f"{k},{s},{h}\n" for k, s, h in sorted(inventory))
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0, filename="") as gz:
        gz.write(text.encode("utf-8"))
    return buf.getvalue()


def build_manifest(normalized: Mapping[str, Any], discovery: Mapping[str, Any], metadata: Mapping[str, Any], *,
                   created_at: Optional[str] = None, code_commit: Optional[str] = None) -> Dict[str, Any]:
    symbols = normalized["symbols"]
    with_bars = {s: q for s, q in symbols.items() if q["actual_candle_count"]}
    inv = inventory_bytes(normalized["inventory"])
    first = min((q["first_available_day"] for q in with_bars.values()), default=None)
    last = max((q["last_available_day"] for q in with_bars.values()), default=None)
    ended = sorted(s for s, q in with_bars.items() if (q["last_traded_day"] or "") < (last or ""))
    critical = {
        "failed_downloads": len(normalized["failed_downloads"]),
        "unreadable_files": sum(len(q["unreadable_files"]) for q in symbols.values()),
        "symbols_with_missing_candles": sum(1 for q in with_bars.values() if q["missing_candles"]),
        "missing_candles": sum(q["missing_candles"] for q in with_bars.values()),
        "invalid_candles": sum(q["invalid_candles"] for q in with_bars.values()),
        "duplicate_candles": sum(q["duplicate_candles"] for q in with_bars.values()),
        "symbols_with_funding_gaps": sum(1 for q in with_bars.values() if q["funding_gaps"]),
        "bar_days_without_any_funding_record": sum(q["bar_days_without_any_funding_record"] for q in with_bars.values()),
        "bar_days_with_incomplete_funding": sum(q["bar_days_with_incomplete_funding"] for q in with_bars.values()),
        "bar_days": sum(q["actual_candle_count"] for q in with_bars.values()),
        "candles_from_daily_files": sum(q["candles_from_daily_files"] for q in with_bars.values()),
        "non_trading_bars": sum(q["non_trading_bars"] for q in with_bars.values()),
        "non_trading_bars_after_last_trade": sum(q["non_trading_bars_after_last_trade"] for q in with_bars.values())}
    body = {
        "schema": MANIFEST_SCHEMA, "dataset_id": DATASET_ID, "dataset_version": DATASET_VERSION, "status": "FROZEN",
        "data_sources": [
            {"name": "Binance public archive", "url": BA.ARCHIVE_URL, "roots": [BA.KLINES_ROOT + "<symbol>/1d/",
                                                                                BA.FUNDING_ROOT + "<symbol>/"],
             "kind": "AUTHORITATIVE_HISTORICAL_PUBLIC", "verified_by": "archive .CHECKSUM (SHA-256) per file"},
            {"name": "Binance USD-M exchangeInfo", "url": EXCHANGE_INFO_URL,
             "kind": "CURRENT_SNAPSHOT_NOT_POINT_IN_TIME", "fetched_at": metadata["fetched_at"],
             "content_hash": metadata["content_hash"]}],
        "coverage_start": first, "coverage_end": last, "months": [discovery["first_month"], discovery["last_month"]],
        "symbols": sorted(symbols), "symbols_with_bars": len(with_bars),
        "symbols_without_bars": sorted(set(symbols) - set(with_bars)),
        "symbol_pattern": discovery["pattern"], "archive_symbols_seen": discovery["archive_symbols"],
        "contracts_that_ended_before_coverage_end": {
            "count": len(ended), "symbols": ended,
            "definition": "the last bar with any trade is before the last day of coverage"},
        "daily_completion": dict(discovery.get("daily_completion") or {}),
        "point_in_time_universe_method": {
            "listing": "RECONSTRUCTED: a contract trades from its first to its last archive bar with any trade",
            "membership": "decided per day from bars on or before that day only (rule in the registered mandate)",
            "delisted_contracts": "INCLUDED wherever the archive holds them",
            "authoritative_listing_history": "NOT_AVAILABLE: no public point-in-time listing or filter history exists",
            "exchange_filters": "CURRENT_SNAPSHOT_ONLY"},
        "raw_artifact_hashes": {"files": len(normalized["inventory"]), "inventory_sha256": hashlib.sha256(inv).hexdigest(),
                                "inventory_file": f"{DATASET_ID}_{DATASET_VERSION}.raw_inventory.csv.gz"},
        "normalized_artifact_hashes": {"klines": _hash_symbols(symbols, "klines"),
                                       "funding": _hash_symbols(symbols, "funding"), "rows": normalized["rows"]},
        "funding_coverage": {k: critical[k] for k in ("symbols_with_funding_gaps", "bar_days_without_any_funding_record",
                                                      "bar_days_with_incomplete_funding", "bar_days")},
        "quality_totals": critical, "missing_data_policy": MISSING_DATA_POLICY,
        "known_limitations": [
            "The archive starts on 2020-01-01: contracts listed in 2019 have no earlier bars here.",
            "Listing and delisting dates are reconstructed from archive coverage, not from an exchange record.",
            "After a contract is settled the archive keeps publishing bars with zero trades; they are not prices.",
            "A ticker that was settled and listed again keeps one archive symbol; its history restarts after the gap.",
            "Exchange filters (step size, minimum notional) and contract categories are today's snapshot.",
            "A contract with no archive files at all cannot be seen; survivorship bias is reduced, not proven absent.",
            "Daily bars carry no intraday order of high and low.",
            "No historical order-book, spread or depth data is part of this dataset."],
        "transformation_code_version": TRANSFORMATION_VERSION, "archive_client_version": BA.ARCHIVE_CLIENT_VERSION}
    dataset_hash = stable_hash({"dataset_id": DATASET_ID, "dataset_version": DATASET_VERSION,
                                "normalized": body["normalized_artifact_hashes"],
                                "metadata": metadata["content_hash"], "transformation": TRANSFORMATION_VERSION})
    body["dataset_hash"] = dataset_hash
    return {**body, "manifest_hash": stable_hash(body), "created_at": created_at or datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"), "transformation_code_commit": code_commit}


def verify_manifest(manifest: Mapping[str, Any]) -> str:
    """The manifest is internally consistent (its hash covers everything except operational metadata)."""
    body = {k: v for k, v in manifest.items() if k not in ("manifest_hash", "created_at", "transformation_code_commit")}
    if stable_hash(body) != manifest.get("manifest_hash") or manifest.get("status") != "FROZEN":
        raise ValueError("dataset manifest was edited or is not frozen")
    return manifest["manifest_hash"]


# ---------------------------------------------------------------------- loading
class DailyPanel:
    """Day x symbol matrices (NaN = no bar). Funding is per day: the 00:00 rate, later positive and negative
    rates, and the hours of the day the archive covers."""

    def __init__(self, days: np.ndarray, symbols: List[str], fields: Dict[str, np.ndarray]):
        self.days, self.symbols, self.fields = days, symbols, fields

    def __getattr__(self, name: str) -> np.ndarray:
        try:
            return self.__dict__["fields"][name]
        except KeyError:
            raise AttributeError(name) from None

    @property
    def shape(self) -> Tuple[int, int]:
        return len(self.days), len(self.symbols)

    def truncated(self, last_day: int) -> "DailyPanel":
        keep = self.days <= last_day
        return DailyPanel(self.days[keep], self.symbols, {k: v[keep] for k, v in self.fields.items()})


def load_tables(store: Path, manifest: Mapping[str, Any], coverage: Mapping[str, Any]):
    """The normalized tables, checked against the frozen manifest BEFORE use: a table whose content hash differs
    from the manifest is refused."""
    import pandas as pd

    verify_manifest(manifest)
    klines = pd.read_parquet(Path(store) / "normalized" / "klines_1d.parquet")
    funding = pd.read_parquet(Path(store) / "normalized" / "funding.parquet")
    actual = {"klines": {}, "funding": {}}
    for sym, g in klines.groupby("symbol", sort=True):
        actual["klines"][sym] = _content_hash(g["day"].to_numpy(), g[list(KLINE_COLUMNS[1:7])].to_numpy(), g["trades"].to_numpy())
    for sym, g in funding.groupby("symbol", sort=True):
        actual["funding"][sym] = _content_hash(g["time_ms"].to_numpy(), g["interval_hours"].to_numpy(), g["rate"].to_numpy())
    empty = {"klines": _content_hash(np.array([], dtype=np.int64), np.zeros((0, 6)), np.array([], dtype=np.int64)),
             "funding": _content_hash(np.array([], dtype=np.int64), np.array([], dtype=np.int64), np.array([]))}
    for kind in ("klines", "funding"):
        got = stable_hash([(s, actual[kind].get(s, empty[kind])) for s in sorted(coverage["symbols"])])
        if got != manifest["normalized_artifact_hashes"][kind]:
            raise ValueError(f"normalized {kind} table does not match the frozen manifest")
    return klines, funding


def load_panel(store: Path, manifest: Mapping[str, Any], coverage: Mapping[str, Any], *, first_day: str,
               last_day: str, traded_only: bool = True) -> DailyPanel:
    """``traded_only`` (the default, and the only mode research uses): a bar with zero trades is not a price and
    appears as NaN, exactly like a missing day."""
    klines, funding = load_tables(store, manifest, coverage)
    if traded_only:
        klines = klines[klines["trades"] > 0]
    lo, hi = day_index(first_day), day_index(last_day)
    days = np.arange(lo, hi + 1, dtype=np.int64)
    symbols = sorted(s for s, q in coverage["symbols"].items() if q["actual_candle_count"])
    col = {s: i for i, s in enumerate(symbols)}
    k = klines[(klines["day"] >= lo) & (klines["day"] <= hi)]
    rows, cols = (k["day"].to_numpy() - lo).astype(np.int64), k["symbol"].map(col).to_numpy()
    fields: Dict[str, np.ndarray] = {}
    for name in ("open", "high", "low", "close", "quote_volume"):
        m = np.full((len(days), len(symbols)), np.nan)
        m[rows, cols] = k[name].to_numpy()
        fields[name] = m
    for name in ("funding_midnight", "funding_later_positive", "funding_later_negative", "funding_hours"):
        fields[name] = np.zeros((len(days), len(symbols)))
    f = funding[funding["symbol"].isin(col)]
    fd = (f["time_ms"].to_numpy() // BA.DAY_MS)
    inside = (fd >= lo) & (fd <= hi)
    f, fd = f[inside], fd[inside]
    fr, fc = (fd - lo).astype(np.int64), f["symbol"].map(col).to_numpy()
    rate, tod = f["rate"].to_numpy(), f["time_ms"].to_numpy() % BA.DAY_MS
    mid = tod < MIDNIGHT_WINDOW_MS
    np.add.at(fields["funding_midnight"], (fr[mid], fc[mid]), rate[mid])
    pos, neg = ~mid & (rate > 0), ~mid & (rate <= 0)
    np.add.at(fields["funding_later_positive"], (fr[pos], fc[pos]), rate[pos])
    np.add.at(fields["funding_later_negative"], (fr[neg], fc[neg]), rate[neg])
    np.add.at(fields["funding_hours"], (fr, fc), f["interval_hours"].to_numpy().astype(np.float64))
    return DailyPanel(days, symbols, fields)


# ---------------------------------------------------------------------- CLI
def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes((json.dumps(obj, indent=1, sort_keys=True) + "\n").encode("utf-8"))


def cmd_build(args) -> int:
    from app.replay.identity import code_revision

    store, docs = Path(args.store), Path(args.docs)
    name = f"{DATASET_ID}_{DATASET_VERSION}"
    state = store / "build_state"
    state.mkdir(parents=True, exist_ok=True)
    disc_path, meta_path = state / "discovery.json", docs / "datasets" / f"{name}.metadata.json"
    if disc_path.exists() and not args.rediscover:          # resume: discovery and the snapshot are kept
        discovery = json.loads(disc_path.read_text(encoding="utf-8"))
    else:
        discovery = discover(workers=args.workers)
        _write_json(disc_path, discovery)
    if meta_path.exists() and not args.rediscover:
        metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    else:
        metadata = metadata_snapshot()
        _write_json(meta_path, metadata)
    n_files = sum(len(v["klines"]) + len(v["funding"]) for v in discovery["files"].values())
    print(f"[dataset] {len(discovery['files'])} contracts, {n_files} raw files", flush=True)
    sync = mirror(store, discovery, workers=args.workers,
                  progress=lambda done, total: print(f"[dataset] mirrored {done}/{total}", flush=True))
    counts = {s: sum(1 for r in sync if r["status"] == s) for s in ("DOWNLOADED", "CACHED", "FAILED")}
    print(f"[dataset] mirror {counts}", flush=True)
    if counts["FAILED"] and not args.allow_failed:
        _write_json(state / "failed.json", [r for r in sync if r["status"] == "FAILED"])
        print("[dataset] downloads failed; rerun to resume (nothing was frozen)", flush=True)
        return 2
    normalized = normalize(store, discovery, sync, metadata)
    if "daily_completion" not in discovery:                 # days the monthly files skip: the archive's daily files
        done = complete_from_daily_files(store, discovery, normalized["missing_days"], workers=args.workers)
        _write_json(disc_path, discovery)
        print(f"[dataset] daily completion {done}", flush=True)
        if done["failed"] and not args.allow_failed:
            print("[dataset] daily-file downloads failed; rerun to resume (nothing was frozen)", flush=True)
            return 2
    if any(v.get("klines_daily") for v in discovery["files"].values()):
        sync = mirror(store, discovery, workers=args.workers)
        normalized = normalize(store, discovery, sync, metadata)
    commit, _branch, _dirty = code_revision()
    manifest = build_manifest(normalized, discovery, metadata, code_commit=commit)
    (docs / "datasets").mkdir(parents=True, exist_ok=True)
    (docs / "datasets" / manifest["raw_artifact_hashes"]["inventory_file"]).write_bytes(inventory_bytes(normalized["inventory"]))
    coverage = {"schema": "daily-dataset-coverage-v1", "dataset_id": DATASET_ID, "dataset_version": DATASET_VERSION,
                "dataset_hash": manifest["dataset_hash"], "manifest_hash": manifest["manifest_hash"],
                "not_in_scope_archive_symbols": discovery["not_in_scope"],
                "failed_downloads": normalized["failed_downloads"], "symbols": normalized["symbols"]}
    _write_json(docs / "coverage" / f"{name}.coverage.json", coverage)
    _write_json(docs / "datasets" / f"{name}.dataset.json", manifest)
    print(json.dumps({"dataset_hash": manifest["dataset_hash"], "manifest_hash": manifest["manifest_hash"],
                      "symbols_with_bars": manifest["symbols_with_bars"], "rows": normalized["rows"],
                      "quality_totals": manifest["quality_totals"]}, indent=1), flush=True)
    return 0


def cmd_verify(args) -> int:
    store, docs, name = Path(args.store), Path(args.docs), f"{DATASET_ID}_{DATASET_VERSION}"
    manifest = json.loads((docs / "datasets" / f"{name}.dataset.json").read_text(encoding="utf-8"))
    coverage = json.loads((docs / "coverage" / f"{name}.coverage.json").read_text(encoding="utf-8"))
    load_tables(store, manifest, coverage)
    inv = (docs / "datasets" / manifest["raw_artifact_hashes"]["inventory_file"]).read_bytes()
    ok_inv = hashlib.sha256(inv).hexdigest() == manifest["raw_artifact_hashes"]["inventory_sha256"]
    bad = []
    if args.raw:
        raw = BA.ArchiveMirror(store / "raw")
        for line in gzip.decompress(inv).decode("utf-8").splitlines()[1:]:
            key, _size, digest = line.split(",")
            if raw.verified_sha256(key) != digest:
                bad.append(key)
    print(json.dumps({"manifest_hash": manifest["manifest_hash"], "dataset_hash": manifest["dataset_hash"],
                      "normalized_tables": "MATCH", "raw_inventory": "MATCH" if ok_inv else "MISMATCH",
                      "raw_files_checked": bool(args.raw), "raw_files_not_matching": bad[:20]}, indent=1))
    return 0 if ok_inv and not bad else 1


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="daily_dataset", description="Binance USD-M daily research dataset")
    sub = p.add_subparsers(dest="command", required=True)
    root = Path(__file__).resolve().parents[4]
    for name, fn in (("build", cmd_build), ("verify", cmd_verify)):
        sp = sub.add_parser(name)
        sp.add_argument("--store", default=str(root / "data" / "research" / f"{DATASET_ID}_{DATASET_VERSION}"))
        sp.add_argument("--docs", default=str(root / "docs" / "research"))
        sp.add_argument("--workers", type=int, default=16)
        sp.add_argument("--rediscover", action="store_true", help="list the archive and fetch metadata again")
        sp.add_argument("--allow-failed", action="store_true", help="freeze even when some downloads failed")
        sp.add_argument("--raw", action="store_true", help="verify: also re-hash every raw file")
        sp.set_defaults(func=fn)
    args = p.parse_args(argv)
    return int(args.func(args))


__all__ = ["DATASET_ID", "DATASET_VERSION", "TRANSFORMATION_VERSION", "MISSING_DATA_POLICY", "USDT_PERPETUAL",
           "DailyPanel", "build_manifest", "complete_from_daily_files", "clean_funding", "clean_klines", "day_index", "day_text", "discover",
           "funding_by_day", "inventory_bytes", "load_panel", "load_tables", "metadata_snapshot", "mirror",
           "normalize", "verify_manifest"]

if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
