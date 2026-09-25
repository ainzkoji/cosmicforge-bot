"""FX reference data providers (Phase 4E/4F).

FX research data is decoupled from any execution broker: a provider supplies
REFERENCE_MARKET_PRICE bars (bid/ask per minute where available). CATI FX
intelligence reads the reference store, never a specific broker.

Providers
---------
* ``DukascopyProvider`` -- free historical minute candles, one LZMA-compressed
  ``.bi5`` file per (pair, side, UTC day)::

      https://datafeed.dukascopy.com/datafeed/{PAIR}/{YYYY}/{MM-1:02d}/{DD:02d}/{BID|ASK}_candles_min_1.bi5

  Each record is 24 bytes, big-endian ``>IIIIIf``: seconds-from-midnight,
  open, close, low, high (integer price x point), volume. The point is
  1e3 for JPY-quoted pairs, 1e5 otherwise. The decoder is exercised against
  constructed payloads in tests; the live endpoint is UNVALIDATED here.
* ``CsvFxReferenceProvider`` -- deterministic import of a provider export
  (``timestamp_ms,bid_open,...,ask_close,volume``) with provider name and
  source version recorded.

Nothing is interpolated: a day with no file yields no bars (a gap the
quality report shows), and a bar missing one side keeps that side ``None``.
"""
from __future__ import annotations

import csv
import lzma
import struct
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Dict, Iterable, List, Optional, Protocol, Sequence

MINUTE_MS = 60_000
DUKASCOPY_URL = "https://datafeed.dukascopy.com/datafeed/{pair}/{y:04d}/{m:02d}/{d:02d}/{side}_candles_min_1.bi5"
_RECORD = struct.Struct(">IIIIIf")

MAJORS = ("EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD")


def point_for(pair: str) -> float:
    return 1e3 if pair.upper().endswith("JPY") else 1e5


def split_pair(pair: str) -> tuple[str, str]:
    letters = "".join(ch for ch in pair.upper() if ch.isalpha())
    if len(letters) != 6:
        raise ValueError(f"not an FX pair: {pair!r}")
    return letters[:3], letters[3:]


def fx_session(ts_ms: int) -> str:
    """Coarse UTC session label for a bar (ASIA / LONDON / NEW_YORK / OVERLAP / CLOSED)."""
    dt = datetime.fromtimestamp(ts_ms / 1000, timezone.utc)
    wd, h = dt.weekday(), dt.hour
    if wd == 5 or (wd == 6 and h < 21) or (wd == 4 and h >= 21):
        return "CLOSED"
    london, ny = 7 <= h < 16, 12 <= h < 21
    if london and ny:
        return "LONDON_NY_OVERLAP"
    if london:
        return "LONDON"
    if ny:
        return "NEW_YORK"
    return "ASIA"


class FXReferenceProvider(Protocol):
    name: str
    version: str

    def minute_quotes(self, pair: str, day: date) -> List[Dict]: ...


def decode_bi5_candles(payload: bytes, *, day_start_ms: int, point: float) -> List[Dict]:
    """Decode one Dukascopy ``*_candles_min_1.bi5`` day file."""
    if not payload:
        return []
    raw = lzma.decompress(payload)
    out = []
    for i in range(0, len(raw) - len(raw) % _RECORD.size, _RECORD.size):
        sec, o, c, lo, hi, vol = _RECORD.unpack_from(raw, i)
        out.append({"open_time": day_start_ms + sec * 1000, "open": o / point, "high": hi / point,
                    "low": lo / point, "close": c / point, "volume": float(vol)})
    return out


def merge_sides(bid: Sequence[Dict], ask: Sequence[Dict], *, pair: str) -> List[Dict]:
    """Join BID and ASK minute bars on open_time; a missing side stays None."""
    b = {r["open_time"]: r for r in bid}
    a = {r["open_time"]: r for r in ask}
    rows = []
    for t in sorted(set(b) | set(a)):
        br, ar = b.get(t), a.get(t)
        row = {"open_time": t, "session": fx_session(t), "volume": (br or ar or {}).get("volume")}
        for side, src in (("bid", br), ("ask", ar)):
            for f in ("open", "high", "low", "close"):
                row[f"{side}_{f}"] = src[f] if src else None
        if br and ar:
            row["mid_close"] = (br["close"] + ar["close"]) / 2.0
        rows.append(row)
    return rows


@dataclass
class DukascopyProvider:
    fetch: Callable[[str], Optional[bytes]]  # url -> bytes (None/404 = no data that day)
    name: str = "dukascopy"
    version: str = "bi5-candles-min-1:v1"

    def minute_quotes(self, pair: str, day: date) -> List[Dict]:
        pair = pair.upper()
        start = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000)
        sides = {}
        for side in ("BID", "ASK"):
            url = DUKASCOPY_URL.format(pair=pair, y=day.year, m=day.month - 1, d=day.day, side=side)
            sides[side] = decode_bi5_candles(self.fetch(url) or b"", day_start_ms=start, point=point_for(pair))
        return merge_sides(sides["BID"], sides["ASK"], pair=pair)


@dataclass
class CsvFxReferenceProvider:
    path_for: Callable[[str, date], str]
    name: str = "csv_import"
    version: str = "csv:v1"

    def minute_quotes(self, pair: str, day: date) -> List[Dict]:
        rows = []
        try:
            with open(self.path_for(pair, day), newline="") as fh:
                for r in csv.DictReader(fh):
                    q = {"open_time": int(r["timestamp_ms"])}
                    for k in ("bid_open", "bid_high", "bid_low", "bid_close", "ask_open", "ask_high", "ask_low",
                              "ask_close", "volume"):
                        q[k] = float(r[k]) if r.get(k) not in (None, "") else None
                    if q.get("bid_close") is not None and q.get("ask_close") is not None:
                        q["mid_close"] = (q["bid_close"] + q["ask_close"]) / 2.0
                    q["session"] = fx_session(q["open_time"])
                    rows.append(q)
        except FileNotFoundError:
            return []
        return sorted(rows, key=lambda q: q["open_time"])


def ingest(provider: FXReferenceProvider, store, pairs: Iterable[str], start: date, end: date) -> Dict[str, int]:
    """Pull [start, end] daily and write 1m reference quotes. Returns rows per pair."""
    counts: Dict[str, int] = {}
    for pair in pairs:
        base, quote = split_pair(pair)
        d, n = start, 0
        while d <= end:
            quotes = provider.minute_quotes(pair, d)
            if quotes:
                n += store.write_fx_quotes(provider.name, f"{base}{quote}", base, quote, "1m", quotes,
                                           source_version=provider.version)
            d += timedelta(days=1)
        counts[pair] = n
    return counts


def resample_quotes(quotes: Sequence[Dict], factor: int) -> List[Dict]:
    """Deterministic whole-window aggregation of 1m reference quotes (both sides)."""
    step = MINUTE_MS * factor
    buckets: Dict[int, List[Dict]] = {}
    for q in quotes:
        buckets.setdefault((q["open_time"] // step) * step, []).append(q)
    out = []
    for t in sorted(buckets):
        g = sorted(buckets[t], key=lambda q: q["open_time"])
        if len(g) != factor or g[0]["open_time"] != t:
            continue  # incomplete window: dropped, never approximated
        row = {"open_time": t, "session": fx_session(t), "volume": sum((q.get("volume") or 0) for q in g)}
        for side in ("bid", "ask"):
            if any(q.get(f"{side}_close") is None for q in g):
                row.update({f"{side}_{f}": None for f in ("open", "high", "low", "close")})
                continue
            row[f"{side}_open"] = g[0][f"{side}_open"]
            row[f"{side}_high"] = max(q[f"{side}_high"] for q in g)
            row[f"{side}_low"] = min(q[f"{side}_low"] for q in g)
            row[f"{side}_close"] = g[-1][f"{side}_close"]
        if row.get("bid_close") is not None and row.get("ask_close") is not None:
            row["mid_close"] = (row["bid_close"] + row["ask_close"]) / 2.0
        out.append(row)
    return out


__all__ = ["CsvFxReferenceProvider", "DUKASCOPY_URL", "DukascopyProvider", "FXReferenceProvider", "MAJORS",
           "decode_bi5_candles", "fx_session", "ingest", "merge_sides", "point_for", "resample_quotes", "split_pair"]
