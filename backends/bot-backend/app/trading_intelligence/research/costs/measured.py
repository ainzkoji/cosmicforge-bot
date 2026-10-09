"""Measured market costs (Section H, Step 2.7): records, provenance, statistics, versioned calibration.

A cost figure is only useful when it says where it came from. Every record carries one of five provenances
and they are never merged silently:

* ``HISTORICAL_PUBLIC``        -- published history (the archive's funding rates);
* ``LIVE_PUBLIC_OBSERVATION``  -- a public order-book or funding snapshot taken now (no account involved);
* ``DEMO_EXECUTION``           -- a fill on the exchange's DEMO environment: proves the integration, and is NOT
                                  representative of real-money fill quality or slippage;
* ``SIMULATED``                -- produced by a simulator;
* ``ASSUMED``                  -- a research assumption (for example the frozen mandate's 0.05% + 0.05%).

Rules: a value that was not captured is ``None`` (never zero, never reconstructed); only the first three
provenances count as measurements; extreme observations are kept; a calibration never changes the cost model a
frozen evaluation used -- it produces a NEW cost-model version, and only when there are enough measurements.
Otherwise the answer is ``INSUFFICIENT_DATA`` and the conservative assumptions stay.

No credential is read here. The live collector uses public market-data endpoints only; the demo extractor
reads fill evidence the engine already stored and drops every account identifier.
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from app.trading_intelligence.hashing import stable_hash

MEASURED_COST_SCHEMA = "measured-cost-record-v1"
COST_TABLE_SCHEMA = "measured-cost-table-v1"
HISTORICAL_PUBLIC, LIVE_PUBLIC_OBSERVATION, DEMO_EXECUTION, SIMULATED, ASSUMED = (
    "HISTORICAL_PUBLIC", "LIVE_PUBLIC_OBSERVATION", "DEMO_EXECUTION", "SIMULATED", "ASSUMED")
PROVENANCES = (HISTORICAL_PUBLIC, LIVE_PUBLIC_OBSERVATION, DEMO_EXECUTION, SIMULATED, ASSUMED)
MEASUREMENTS = (HISTORICAL_PUBLIC, LIVE_PUBLIC_OBSERVATION, DEMO_EXECUTION)
#: measurements that describe real-market conditions (a demo fill does not)
REPRESENTATIVE = (HISTORICAL_PUBLIC, LIVE_PUBLIC_OBSERVATION)
CALIBRATED, INSUFFICIENT_DATA = "CALIBRATED", "INSUFFICIENT_DATA"
FIELDS = ("exchange", "symbol", "market_type", "timestamp_utc", "bid_price", "ask_price", "mid_price",
          "spread_absolute", "spread_bps", "order_side", "order_type", "intended_quantity", "submitted_quantity",
          "expected_price", "actual_fill_price", "fill_quantity", "fill_timestamp", "execution_latency",
          "observed_slippage_bps", "fee_amount", "fee_asset", "funding_rate", "funding_timestamp", "data_source",
          "environment", "measurement_quality")
REQUIRED = ("exchange", "symbol", "market_type", "timestamp_utc", "data_source", "environment", "measurement_quality")
BOOK_TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/bookTicker"
PREMIUM_INDEX_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"
TICKER_24H_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"


class CostRecordError(ValueError):
    pass


def make_record(**values: Any) -> Dict[str, Any]:
    """A validated record with every field present (``None`` = not captured) and a content id."""
    unknown = set(values) - set(FIELDS) - {"notes"}
    if unknown:
        raise CostRecordError(f"unknown cost fields {sorted(unknown)}")
    rec = {f: values.get(f) for f in FIELDS}
    missing = [f for f in REQUIRED if rec[f] in (None, "")]
    if missing:
        raise CostRecordError(f"cost record is missing {missing}")
    if rec["data_source"] not in PROVENANCES:
        raise CostRecordError(f"unknown provenance {rec['data_source']!r}")
    for name in ("bid_price", "ask_price", "mid_price", "spread_absolute", "spread_bps", "intended_quantity",
                 "submitted_quantity", "expected_price", "actual_fill_price", "fill_quantity", "execution_latency",
                 "observed_slippage_bps", "fee_amount", "funding_rate"):
        if rec[name] is not None:
            rec[name] = float(rec[name])
            if not math.isfinite(rec[name]):
                raise CostRecordError(f"{name} is not a finite number")
    bid, ask = rec["bid_price"], rec["ask_price"]
    if bid is not None and ask is not None:
        if bid <= 0 or ask < bid:
            raise CostRecordError("a book observation needs 0 < bid <= ask")
        mid = (bid + ask) / 2.0
        derived = {"mid_price": mid, "spread_absolute": ask - bid, "spread_bps": (ask - bid) / mid * 1e4}
        for name, value in derived.items():
            if rec[name] is not None and abs(rec[name] - value) > 1e-9 * max(1.0, abs(value)):
                raise CostRecordError(f"{name} disagrees with the bid and ask")
            rec[name] = value
    elif rec["spread_bps"] is not None and rec["data_source"] in MEASUREMENTS:
        raise CostRecordError("a measured spread needs the bid and ask it was measured from")
    if rec["observed_slippage_bps"] is not None:
        exp, act, side = rec["expected_price"], rec["actual_fill_price"], str(rec["order_side"] or "").upper()
        if exp is None or act is None or side not in ("BUY", "SELL"):
            raise CostRecordError("slippage needs the expected price, the fill price and the side it was measured from")
        derived = (act - exp) / exp * 1e4 * (1.0 if side == "BUY" else -1.0)        # positive = adverse
        if abs(rec["observed_slippage_bps"] - derived) > 1e-6 * max(1.0, abs(derived)):
            raise CostRecordError("observed_slippage_bps disagrees with the expected and fill prices")
    rec["notes"] = str(values.get("notes") or "")
    rec["schema"] = MEASURED_COST_SCHEMA
    rec["unavailable"] = sorted(f for f in FIELDS if rec[f] is None)
    rec["record_id"] = stable_hash({k: rec[k] for k in (*FIELDS, "notes")})[:32]
    return rec


class MeasuredCostStore:
    """Append-only JSON-lines file of validated records; the same observation is stored once."""

    def __init__(self, path: Path):
        self.path = Path(path)

    def records(self) -> List[Dict[str, Any]]:
        if not self.path.exists():
            return []
        return [json.loads(line) for line in self.path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def append(self, records: Iterable[Mapping[str, Any]]) -> int:
        seen = {r["record_id"] for r in self.records()}
        new = []
        for r in records:
            r = make_record(**{k: r.get(k) for k in (*FIELDS, "notes") if k in r})
            if r["record_id"] not in seen:
                seen.add(r["record_id"])
                new.append(r)
        if new:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "ab") as fh:
                for r in new:
                    fh.write((json.dumps(r, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8"))
                fh.flush()
                os.fsync(fh.fileno())
        return len(new)


# ---------------------------------------------------------------------- collectors
def observe_public_book(get: Callable[[str], bytes], symbols: Sequence[str], *, timestamp_utc: str,
                        quality: str = "SINGLE_SNAPSHOT") -> List[Dict[str, Any]]:
    """Best bid and ask (and the current funding rate) from PUBLIC endpoints. No account, no order."""
    want = set(symbols)
    book = {r["symbol"]: r for r in json.loads(get(BOOK_TICKER_URL)) if r["symbol"] in want}
    premium = {r["symbol"]: r for r in json.loads(get(PREMIUM_INDEX_URL)) if r["symbol"] in want}
    out = []
    for s in sorted(book):
        b, p = book[s], premium.get(s) or {}
        bid, ask = float(b["bidPrice"]), float(b["askPrice"])
        if bid <= 0 or ask < bid:
            continue
        out.append(make_record(exchange="BINANCE", symbol=s, market_type="USDM_PERPETUAL", timestamp_utc=timestamp_utc,
                               bid_price=bid, ask_price=ask, funding_rate=p.get("lastFundingRate"),
                               funding_timestamp=p.get("nextFundingTime"), data_source=LIVE_PUBLIC_OBSERVATION,
                               environment="PUBLIC_MARKET_DATA", measurement_quality=quality,
                               notes="top-of-book only; says nothing about depth or the cost of a large order"))
    return out


def most_traded_symbols(get: Callable[[str], bytes], in_scope: Callable[[str], bool], count: int = 20) -> List[str]:
    rows = [r for r in json.loads(get(TICKER_24H_URL)) if r["symbol"].endswith("USDT") and in_scope(r["symbol"])]
    return [r["symbol"] for r in sorted(rows, key=lambda r: -float(r["quoteVolume"]))[:count]]


def demo_execution_records(db_path: Path) -> List[Dict[str, Any]]:
    """Fills the engine already recorded on the DEMO environment (``cati_execution_attempts``), read-only.
    Every account, user and bot identifier is left behind. What the engine did not capture stays ``None``:
    no order-book snapshot or decision-time reference price is reconstructed afterwards."""
    conn = sqlite3.connect(f"file:{Path(db_path).as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    out: List[Dict[str, Any]] = []
    try:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='cati_execution_attempts'").fetchone():
            return out
        for row in conn.execute("SELECT status, payload, recorded_at FROM cati_execution_attempts ORDER BY recorded_at"):
            p = json.loads(row["payload"])
            realized = p.get("realized_costs") or {}
            env = str(p.get("environment") or "").upper()
            if row["status"] != "FILLED" or env not in ("DEMO", "TESTNET", "PAPER") or not realized:
                continue                                  # one record per fill; later states repeat the same evidence
            key = p.get("instrument_key") if isinstance(p.get("instrument_key"), dict) else {}
            requested, filled = p.get("requested_price"), p.get("filled_price")
            side = str(p.get("side") or "").upper()
            order_side = {"LONG": "BUY", "SHORT": "SELL"}.get(side, side if side in ("BUY", "SELL") else None)
            slippage = realized.get("slippage_bps") if (requested and filled and order_side) else None
            sent, acked = p.get("submitted_at"), p.get("acknowledged_at")
            latency = (float(acked) - float(sent)) / 1000.0 if sent and acked and acked != sent else None
            try:
                out.append(make_record(
                    exchange=str(key.get("venue") or p.get("venue") or "BINANCE").upper(),
                    symbol=key.get("venue_symbol") or p.get("canonical_symbol") or p.get("symbol"),
                    market_type="USDM_PERPETUAL", timestamp_utc=_iso(row["recorded_at"]), order_side=order_side,
                    order_type=p.get("actual_order_type") or p.get("requested_order_type"),
                    intended_quantity=p.get("requested_quantity"), submitted_quantity=p.get("submitted_quantity"),
                    expected_price=requested, actual_fill_price=filled, fill_quantity=p.get("filled_quantity"),
                    fill_timestamp=_iso(p.get("resolved_at")), execution_latency=latency,
                    observed_slippage_bps=slippage, fee_amount=realized.get("fees"), fee_asset=realized.get("fee_asset"),
                    data_source=DEMO_EXECUTION, environment=env, measurement_quality="DEMO_FILL_NOT_REPRESENTATIVE",
                    notes="exchange demo environment; validates the integration, not real-money fill quality; the "
                          "expected price is the plan's reference at decision time, not an order-book quote"))
            except CostRecordError:
                continue
    finally:
        conn.close()
    return out


def _iso(ms: Any) -> Optional[str]:
    if ms in (None, ""):
        return None
    from datetime import datetime, timezone

    return datetime.fromtimestamp(float(ms) / 1000.0, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------- statistics and calibration
def _distribution(values: Sequence[float]) -> Dict[str, Any]:
    v = sorted(float(x) for x in values)
    n = len(v)
    if not n:
        return {"n": 0}

    def q(p: float) -> float:
        pos = p * (n - 1)
        lo, hi = math.floor(pos), math.ceil(pos)
        return v[lo] + (v[hi] - v[lo]) * (pos - lo)

    return {"n": n, "mean": sum(v) / n, "median": q(0.5), "p75": q(0.75), "p90": q(0.9), "p95": q(0.95),
            "min": v[0], "max": v[-1]}                               # extremes are kept, never trimmed


def cost_table(records: Sequence[Mapping[str, Any]], *, min_samples: int = 30, min_distinct_days: int = 5) -> Dict[str, Any]:
    """Per exchange and symbol: spread, slippage, fee and funding distributions with their sample counts.
    A component is CALIBRATED only with enough REPRESENTATIVE measurements spread over enough days; demo fills,
    simulations and assumptions are counted and shown but never calibrate anything."""
    groups: Dict[tuple, List[Mapping[str, Any]]] = {}
    for r in records:
        groups.setdefault((r["exchange"], r["symbol"]), []).append(r)
    rows = {}
    for (exchange, symbol), rs in sorted(groups.items()):
        def series(name: str, sources: Sequence[str]):
            return [r for r in rs if r.get(name) is not None and r["data_source"] in sources]

        comp = {}
        for name, key in (("spread_bps", "spread_bps"), ("slippage_bps", "observed_slippage_bps"),
                          ("funding_rate", "funding_rate"), ("fee_amount", "fee_amount")):
            rep = series(key, REPRESENTATIVE)
            days = {str(r["timestamp_utc"])[:10] for r in rep}
            ok = len(rep) >= min_samples and len(days) >= min_distinct_days
            comp[name] = {**_distribution([r[key] for r in rep]), "distinct_days": len(days),
                          "status": CALIBRATED if ok else INSUFFICIENT_DATA,
                          "adverse_stress": _distribution([r[key] for r in rep]).get("p95") if ok else None,
                          "demo_samples": len(series(key, (DEMO_EXECUTION,))),
                          "not_measured_samples": len(series(key, (SIMULATED, ASSUMED)))}
        rows[f"{exchange}:{symbol}"] = {"exchange": exchange, "symbol": symbol, "records": len(rs),
                                        "by_provenance": {p: sum(1 for r in rs if r["data_source"] == p) for p in PROVENANCES
                                                          if any(r["data_source"] == p for r in rs)}, **comp}
    body = {"schema": COST_TABLE_SCHEMA, "min_samples": min_samples, "min_distinct_days": min_distinct_days,
            "records": len(records), "symbols": rows}
    return {**body, "table_hash": stable_hash(body)}


@dataclass(frozen=True)
class Calibration:
    status: str
    cost_model: Mapping[str, Any]
    basis: str
    reasons: Sequence[str] = field(default_factory=tuple)


def calibrate(table: Mapping[str, Any], frozen_cost_model: Mapping[str, Any], *, version: str,
              symbols: Sequence[str]) -> Calibration:
    """A candidate cost-model VERSION from measurements. The frozen model is never modified. Calibration needs a
    CALIBRATED spread and a CALIBRATED slippage for every symbol it is asked to cover; otherwise the conservative
    assumptions of the frozen model stand and the result says why."""
    reasons = []
    for s in symbols:
        row = table["symbols"].get(f"BINANCE:{s}")
        for comp in ("spread_bps", "slippage_bps"):
            if not row or row[comp]["status"] != CALIBRATED:
                reasons.append(f"{s}:{comp}:{INSUFFICIENT_DATA}")
    if reasons or not symbols:
        return Calibration(INSUFFICIENT_DATA, dict(frozen_cost_model),
                           "RETAINED: the frozen mandate's assumed costs (no calibration possible)", tuple(reasons[:40]))
    half_spread = max(table["symbols"][f"BINANCE:{s}"]["spread_bps"]["p95"] for s in symbols) / 2e4
    slippage = max(table["symbols"][f"BINANCE:{s}"]["slippage_bps"]["p95"] for s in symbols) / 1e4
    body = {"cost_model_id": "MEASURED_COSTS_BINANCE_USDM", "cost_model_version": version,
            "model": {**dict(frozen_cost_model["model"]), "spread": half_spread, "slippage": max(slippage, 0.0)},
            "basis": "MEASURED: 95th percentile of representative observations, worst covered symbol",
            "table_hash": table["table_hash"], "supersedes_nothing": "a frozen evaluation keeps the model it was run with"}
    return Calibration(CALIBRATED, {**body, "cost_model_hash": stable_hash(body)}, body["basis"])


__all__ = ["MEASURED_COST_SCHEMA", "COST_TABLE_SCHEMA", "PROVENANCES", "MEASUREMENTS", "REPRESENTATIVE", "FIELDS",
           "HISTORICAL_PUBLIC", "LIVE_PUBLIC_OBSERVATION", "DEMO_EXECUTION", "SIMULATED", "ASSUMED", "CALIBRATED",
           "INSUFFICIENT_DATA", "CostRecordError", "make_record", "MeasuredCostStore", "observe_public_book",
           "most_traded_symbols", "demo_execution_records", "cost_table", "calibrate", "Calibration"]
