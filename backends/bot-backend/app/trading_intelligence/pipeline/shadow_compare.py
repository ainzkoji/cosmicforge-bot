"""The daily reconciliation of the pipeline's shadow portfolio with the Step 2 evaluator (Section H, Step 3.15).

Two things are held against each other for an account and a day:

* what the **pipeline** recorded: the immutable family decision of that day (equity basis, universe, selected
  contracts, strength, stop distance, target notional and fraction, the scaling caps, the rejections) and
  the shadow book's equity;
* what the **evaluator** says for the same day on the same bars, mandate, rule version, risk level, cost
  model and portfolio state: either recomputed (``reference_from_simulation``: the forward comparison, on the
  bars the pipeline itself used) or read from the committed development run
  (``reference_from_csv``: ``development_primary_daily_targets.csv`` and the files beside it).

The two computations share the frozen rule functions and nothing else. The pipeline keeps a book in the
database and moves it one day at a time; the evaluator recomputes the whole period in memory. Agreement is
therefore evidence that the pipeline's state handling, ordering and persistence are right.

**Tolerance.** Values are compared relative to their size. A difference up to ``TOLERANCE`` (1e-9) is
agreement: both sides do the same arithmetic in double precision, and the only source of difference is that
a number stored as JSON text and read back is the same double, so in practice the difference is exactly 0.
Between ``TOLERANCE`` and ``PRECISION_BAND`` (1e-6) it is a ``PRECISION_MISMATCH``: reported, alerted, not
ignored. Above it the divergence is classified by cause. Nothing unexplained is tolerated: what cannot be
classified is an ``UNKNOWN_DEFECT``.

Every comparison is a durable row; every divergence is also a structured alert on the
``cati.shadow_compare`` logger and in the returned list.
"""
from __future__ import annotations

import csv
import json
import logging
import math
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from app.trading_intelligence.hashing import short_id
from shared_lib.persistence.cati_platform_schema import DECISION_TABLE, SHADOW_COMPARISON_TABLE, SHADOW_DAY_TABLE

from .daily_trend import day_of, day_text

log = logging.getLogger("cati.shadow_compare")

TOLERANCE = 1e-9
PRECISION_BAND = 1e-6
MATCH, DIVERGED, NO_REFERENCE, NO_SHADOW = "MATCH", "DIVERGED", "NO_REFERENCE", "NO_SHADOW"
DATA_MISMATCH, RULE_MISMATCH, TIMING_MISMATCH, RISK_POLICY_MISMATCH, PRECISION_MISMATCH = (
    "DATA_MISMATCH", "RULE_MISMATCH", "TIMING_MISMATCH", "RISK_POLICY_MISMATCH", "PRECISION_MISMATCH")
EXPECTED_OPERATIONAL_DIFFERENCE, UNKNOWN_DEFECT = "EXPECTED_OPERATIONAL_DIFFERENCE", "UNKNOWN_DEFECT"
CLASSIFICATIONS = (DATA_MISMATCH, RULE_MISMATCH, TIMING_MISMATCH, RISK_POLICY_MISMATCH, PRECISION_MISMATCH,
                   EXPECTED_OPERATIONAL_DIFFERENCE, UNKNOWN_DEFECT)


def relative_difference(a: float, b: float) -> float:
    a, b = float(a), float(b)
    if a == b:
        return 0.0
    if not (math.isfinite(a) and math.isfinite(b)):
        return math.inf
    return abs(a - b) / max(abs(a), abs(b), 1e-300)


# ----------------------------------------------------------------------------------------------- references
def reference_from_simulation(sim: Any) -> Dict[int, Dict[str, Any]]:
    """Day -> the evaluator's decision at that close, from a ``simulate`` result over the account's period."""
    daily = {int(d): row for row, d in enumerate(sim.days)}
    rejections: Dict[int, Dict[str, str]] = {}
    for r in sim.rejections:
        if r.get("stage") == "DECISION":
            rejections.setdefault(int(r["day"]), {})[r["symbol"]] = r["reason"]
    out: Dict[int, Dict[str, Any]] = {}
    for t in sim.targets:
        day = int(t["day"])
        row = daily.get(day)
        out[day] = {
            "equity": float(t["equity"]), "universe": int(t["universe"]), "qualified": int(t["qualified"]),
            "scale": {k: float(v) for k, v in t["scale"].items()}, "open_risk": float(t["open_risk"]),
            "targets": {s: {"notional": float(v["notional"]), "fraction_of_equity": float(v["fraction_of_equity"]),
                            "strength": float(v["strength"]), "stop_distance": float(v["stop_distance"])}
                        for s, v in t["targets"].items()},
            "rejections": rejections.get(day, {}),
            "flags": None if row is None else {"paused": bool(sim.daily["paused"][row]),
                                               "halved": bool(sim.daily["halved"][row]),
                                               "halted": bool(sim.daily["halted"][row])},
            "gross_notional": sum(float(v["notional"]) for v in t["targets"].values()),
        }
    return out


def reference_from_csv(targets_csv: Path, *, daily_csv: Optional[Path] = None,
                       rejections_csv: Optional[Path] = None) -> Dict[int, Dict[str, Any]]:
    """Day -> the committed development reference. The targets file has a row per targeted contract, so a
    day on which the evaluator targeted nothing is known only from the daily file; without it such a day is
    absent and is reported NO_REFERENCE, never assumed empty."""
    out: Dict[int, Dict[str, Any]] = {}
    if daily_csv is not None:
        with open(daily_csv, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                # the equity at the close of a day is the equity basis of that day's decision
                out[day_of(r["day"])] = {"equity": float(r["equity"]), "targets": {}, "rejections": None,
                                         "universe": None, "scale": None, "flags": _flags(r)}
    with open(targets_csv, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            day = out.setdefault(day_of(r["day"]), {"targets": {}, "rejections": None, "universe": None, "scale": None,
                                                    "flags": None})
            day["equity"] = float(r["equity"])
            day["targets"][r["symbol"]] = {"notional": float(r["target_notional"]),
                                           "fraction_of_equity": float(r["target_fraction_of_equity"]),
                                           "strength": float(r["strength"]), "stop_distance": float(r["stop_distance"])}
    if rejections_csv is not None:
        for v in out.values():
            v["rejections"] = {}
        with open(rejections_csv, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r.get("stage") == "DECISION" and day_of(r["day"]) in out:
                    out[day_of(r["day"])]["rejections"][r["symbol"]] = r["reason"]
    for v in out.values():
        v["gross_notional"] = sum(t["notional"] for t in v["targets"].values())
    return out


def _flags(row: Mapping[str, str]) -> Optional[Dict[str, bool]]:
    if not all(k in row for k in ("paused", "halved", "halted")):
        return None
    return {k: float(row[k]) != 0.0 for k in ("paused", "halved", "halted")}


def shadow_from_db(db: Any, account_id: str, family_id: str) -> Dict[int, Dict[str, Any]]:
    """Day -> what the pipeline recorded for the account."""
    out: Dict[int, Dict[str, Any]] = {}
    with db.connect() as conn:
        rows = [dict(r) for r in conn.execute(
            f"SELECT decision_id, data_cutoff, rule_hash, document FROM {DECISION_TABLE} WHERE account_id=? AND family_id=? "
            "ORDER BY decision_time", (account_id, family_id))]
        equity = {int(r[0]): float(r[1]) for r in conn.execute(
            f"SELECT day, equity FROM {SHADOW_DAY_TABLE} WHERE account_id=? AND family_id=?", (account_id, family_id))}
    for r in rows:
        doc = json.loads(r["document"])
        day = int(doc["day"])
        out[day] = {
            "decision_id": r["decision_id"], "rule_hash": r["rule_hash"], "equity": float(doc["equity_basis"]),
            "book_equity": equity.get(day), "universe": int(doc["universe"]), "qualified": int(doc["qualified"]),
            "scale": {k: float(v) for k, v in doc["scale"].items()}, "open_risk": float(doc["open_risk"]),
            "targets": {s: {k: float(v[k]) for k in ("notional", "fraction_of_equity", "strength", "stop_distance")}
                        for s, v in doc["targets"].items()},
            "rejections": {x["symbol"]: x["reason"] for x in doc["rejections"] if x.get("stage") == "DECISION"},
            "flags": {k: bool(doc["flags"][k]) for k in ("paused", "halved", "halted")},
            "entries_blocked": bool(doc["flags"].get("entries_blocked")),
            "directions": sorted({i.get("direction") for i in doc["intents"] if i.get("direction")}),
            "gross_notional": float(doc["gross_notional"]), "data_snapshot_id": doc.get("data_snapshot_id"),
        }
    return out


# ----------------------------------------------------------------------------------------------- comparison
def compare_day(day: int, shadow: Optional[Mapping[str, Any]], reference: Optional[Mapping[str, Any]], *,
                tolerance: float = TOLERANCE) -> Dict[str, Any]:
    """One day. ``divergences`` lists every field that differs, each with a classification."""
    if reference is None:
        return {"day": day, "status": NO_REFERENCE, "largest_difference": None, "divergences": []}
    if shadow is None:
        return {"day": day, "status": NO_SHADOW, "largest_difference": None,
                "divergences": [{"field": "decision", "classification": TIMING_MISMATCH,
                                 "detail": "the evaluator has a decision for this day and the pipeline recorded none"}]}
    found: List[Dict[str, Any]] = []
    flags_differ = reference.get("flags") is not None and shadow["flags"] != reference["flags"]

    def number(field: str, a: float, b: float, cause: str, symbol: Optional[str] = None) -> None:
        diff = relative_difference(a, b)
        if diff > tolerance:
            found.append({"field": field, "symbol": symbol, "shadow": a, "reference": b, "difference": diff,
                          "classification": PRECISION_MISMATCH if diff <= PRECISION_BAND else cause})

    if shadow.get("directions") not in ([], ["LONG"]):
        found.append({"field": "direction", "shadow": shadow["directions"], "reference": ["LONG"],
                      "classification": RULE_MISMATCH})
    if reference.get("universe") is not None and shadow["universe"] != reference["universe"]:
        found.append({"field": "universe", "shadow": shadow["universe"], "reference": reference["universe"],
                      "classification": DATA_MISMATCH})
    if flags_differ:
        found.append({"field": "brake_flags", "shadow": shadow["flags"], "reference": reference["flags"],
                      "classification": RISK_POLICY_MISMATCH})
    number("equity_basis", shadow["equity"], reference["equity"], UNKNOWN_DEFECT)
    selection_cause = (EXPECTED_OPERATIONAL_DIFFERENCE if shadow.get("entries_blocked") else
                       RISK_POLICY_MISMATCH if flags_differ else
                       DATA_MISMATCH if any(d["field"] == "universe" for d in found) else RULE_MISMATCH)
    s_targets, r_targets = shadow["targets"], reference["targets"]
    for symbol in sorted(set(s_targets) ^ set(r_targets)):
        found.append({"field": "selected_contracts", "symbol": symbol, "shadow": symbol in s_targets,
                      "reference": symbol in r_targets, "classification": selection_cause})
    for symbol in sorted(set(s_targets) & set(r_targets)):
        a, b = s_targets[symbol], r_targets[symbol]
        number("strength", a["strength"], b["strength"], RULE_MISMATCH, symbol)
        number("stop_distance", a["stop_distance"], b["stop_distance"], RULE_MISMATCH, symbol)
        same_inputs = not any(d.get("symbol") == symbol and d["field"] in ("strength", "stop_distance") for d in found) \
            and not any(d["field"] == "equity_basis" for d in found)
        cause = RISK_POLICY_MISMATCH if same_inputs else UNKNOWN_DEFECT
        number("target_notional", a["notional"], b["notional"], cause, symbol)
        number("position_fraction", a["fraction_of_equity"], b["fraction_of_equity"], cause, symbol)
    if reference.get("scale") is not None:
        for key in sorted(set(shadow["scale"]) | set(reference["scale"])):
            number(f"portfolio_cap:{key}", shadow["scale"].get(key, math.nan), reference["scale"].get(key, math.nan),
                   RISK_POLICY_MISMATCH)
    number("cash_allocation", shadow["equity"] - shadow["gross_notional"],
           reference["equity"] - reference["gross_notional"], UNKNOWN_DEFECT)
    if reference.get("rejections") is not None:
        s_rej, r_rej = shadow["rejections"], reference["rejections"]
        for symbol in sorted(set(s_rej) | set(r_rej)):
            if s_rej.get(symbol) != r_rej.get(symbol):
                found.append({"field": "rejections", "symbol": symbol, "shadow": s_rej.get(symbol),
                              "reference": r_rej.get(symbol), "classification": RISK_POLICY_MISMATCH})
    numeric = [d["difference"] for d in found if isinstance(d.get("difference"), float)]
    return {"day": day, "status": DIVERGED if found else MATCH,
            "largest_difference": max(numeric) if numeric else (None if found else 0.0), "divergences": found}


def compare(shadow: Mapping[int, Mapping[str, Any]], reference: Mapping[int, Mapping[str, Any]], *,
            days: Optional[Iterable[int]] = None, tolerance: float = TOLERANCE) -> List[Dict[str, Any]]:
    wanted = sorted(set(days) if days is not None else set(shadow) | set(reference))
    return [compare_day(d, shadow.get(d), reference.get(d), tolerance=tolerance) for d in wanted]


def record(db: Any, *, family_id: str, account_id: str, reference_label: str, comparisons: Iterable[Mapping[str, Any]],
           now_ms: Optional[int] = None) -> List[Dict[str, Any]]:
    """Store every comparison (append-only) and return the alerts: one per day that is not a MATCH. A day
    already recorded against the same reference with the same outcome is not stored twice."""
    ts = int(now_ms if now_ms is not None else time.time() * 1000)
    alerts: List[Dict[str, Any]] = []
    with db.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        for c in comparisons:
            document = json.dumps({"divergences": c["divergences"]}, sort_keys=True, default=str)
            comparison_id = short_id("scmp", {"f": family_id, "a": account_id, "d": c["day"], "r": reference_label,
                                              "s": c["status"], "doc": document})
            if conn.execute(f"SELECT 1 FROM {SHADOW_COMPARISON_TABLE} WHERE comparison_id=?", (comparison_id,)).fetchone():
                continue
            conn.execute(
                f"INSERT INTO {SHADOW_COMPARISON_TABLE} (comparison_id, family_id, account_id, day, reference, status, "
                "largest_difference, document, recorded_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (comparison_id, family_id, account_id, int(c["day"]), reference_label, c["status"],
                 c["largest_difference"], document, ts))
            if c["status"] != MATCH:
                classes = sorted({d["classification"] for d in c["divergences"]}) or [TIMING_MISMATCH]
                alerts.append({"alert": "CATI_SHADOW_DIVERGENCE", "family_id": family_id, "account_id": account_id,
                               "day": day_text(c["day"]), "status": c["status"], "reference": reference_label,
                               "classifications": classes, "largest_difference": c["largest_difference"],
                               "fields": sorted({d["field"] for d in c["divergences"]}),
                               "comparison_id": comparison_id})
    for alert in alerts:
        log.warning(json.dumps(alert, sort_keys=True))
    return alerts


def summary(comparisons: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    comparisons = list(comparisons)
    by_status: Dict[str, int] = {}
    by_class: Dict[str, int] = {}
    worst = 0.0
    for c in comparisons:
        by_status[c["status"]] = by_status.get(c["status"], 0) + 1
        for d in c["divergences"]:
            by_class[d["classification"]] = by_class.get(d["classification"], 0) + 1
        if isinstance(c["largest_difference"], float) and math.isfinite(c["largest_difference"]):
            worst = max(worst, c["largest_difference"])
    return {"days": len(comparisons), "by_status": by_status, "divergences_by_classification": by_class,
            "largest_relative_difference": worst, "tolerance": TOLERANCE, "precision_band": PRECISION_BAND}


def evaluator_simulation(features: Any, *, risk_level: str, policy: str, first_day: int, last_day: int,
                         initial_equity: float) -> Any:
    """The evaluator's own run for an account's period, on the given (already causal) features."""
    from app.trading_intelligence.families.daily_trend import targets as T
    from app.trading_intelligence.research.evaluator import simulator as S

    return S.simulate(features, S.RunConfig(level=T.risk_level(risk_level, policy), start_day=int(first_day),
                                            end_day=int(last_day), initial_equity=float(initial_equity)))


def compare_account_forward(db: Any, *, family_id: str, account_id: str, features: Any, risk_level: str, policy: str,
                            first_day: int, last_day: int, initial_equity: float,
                            now_ms: Optional[int] = None) -> Dict[str, Any]:
    """The forward comparison: the evaluator recomputed on the bars the pipeline used, against what the
    pipeline recorded for the account, for every day of the account's book up to ``last_day``."""
    sim = evaluator_simulation(features, risk_level=risk_level, policy=policy, first_day=first_day, last_day=last_day,
                               initial_equity=initial_equity)
    reference = reference_from_simulation(sim)
    shadow = {d: v for d, v in shadow_from_db(db, account_id, family_id).items() if d <= last_day}
    comparisons = compare(shadow, reference)
    alerts = record(db, family_id=family_id, account_id=account_id,
                    reference_label=f"EVALUATOR_RECOMPUTED:{day_text(first_day)}:{day_text(last_day)}",
                    comparisons=comparisons, now_ms=now_ms)
    return {"summary": summary(comparisons), "alerts": alerts, "comparisons": comparisons}


__all__ = ["TOLERANCE", "PRECISION_BAND", "MATCH", "DIVERGED", "NO_REFERENCE", "NO_SHADOW", "CLASSIFICATIONS",
           "DATA_MISMATCH", "RULE_MISMATCH", "TIMING_MISMATCH", "RISK_POLICY_MISMATCH", "PRECISION_MISMATCH",
           "EXPECTED_OPERATIONAL_DIFFERENCE", "UNKNOWN_DEFECT", "relative_difference", "reference_from_simulation",
           "reference_from_csv", "shadow_from_db", "compare_day", "compare", "record", "summary",
           "evaluator_simulation", "compare_account_forward"]
