"""Training datasets with strict MARKET / EXECUTION / ACCOUNT separation (23.3).

* MARKET labels (OUTCOME / RANKING / REGIME / OOD) come from the market path
  only. A hard-risk REJECTED candidate with an observed future path is still
  a valid market example; an execution failure never becomes a negative
  market label; account outcomes are never labels.
* EXECUTION labels (SLIPPAGE) come only from recorded fills.
* POSITION labels (EXIT) come only from position-path evidence.

Splits are chronological with a label-horizon purge (Section 22 splits) and
a reserved chronological holdout; nothing is shuffled.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Sequence, Tuple

from app.trading_intelligence.hashing import stable_hash

from .contracts import LabelSchema, ModelRole, OutcomeFamily
from .features import FEATURE_SCHEMAS, validate_schema

LABEL_SCHEMAS: Mapping[str, LabelSchema] = {
    ModelRole.OUTCOME.value: LabelSchema("cati_net_profitable", ModelRole.OUTCOME.value, OutcomeFamily.MARKET.value,
                                         "net_R > 0 after canonical venue cost", "BINARY"),
    ModelRole.RANKING.value: LabelSchema("cati_net_R", ModelRole.RANKING.value, OutcomeFamily.MARKET.value,
                                         "net_R (relative, within decision time)", "REGRESSION"),
    ModelRole.REGIME.value: LabelSchema("cati_realized_regime", ModelRole.REGIME.value, OutcomeFamily.MARKET.value,
                                        "regime at the next decision of the same symbol", "MULTICLASS"),
    ModelRole.OOD.value: LabelSchema("cati_support_density", ModelRole.OOD.value, OutcomeFamily.MARKET.value,
                                     "training feature density (unsupervised)", "DENSITY"),
    ModelRole.SLIPPAGE.value: LabelSchema("cati_realized_slippage_bps", ModelRole.SLIPPAGE.value,
                                          OutcomeFamily.EXECUTION.value, "filled vs requested price, bps", "REGRESSION"),
    ModelRole.EXIT.value: LabelSchema("cati_remaining_R", ModelRole.EXIT.value, OutcomeFamily.POSITION.value,
                                      "realized R from this path point to exit", "REGRESSION"),
}


class DatasetContractViolation(ValueError):
    pass


@dataclass(frozen=True)
class MLDataset:
    role: str
    rows: Tuple[Mapping[str, Any], ...]
    labels: Tuple[Any, ...]
    times: Tuple[int, ...]
    source_kind: str
    feature_schema_hash: str
    label_schema_hash: str

    @property
    def n(self) -> int:
        return len(self.rows)

    @property
    def dataset_hash(self) -> str:
        return stable_hash({"role": self.role, "rows": [dict(r) for r in self.rows], "labels": list(self.labels),
                            "times": list(self.times), "source_kind": self.source_kind,
                            "features": self.feature_schema_hash, "labels_schema": self.label_schema_hash})

    def chronological_split(self, *, horizon_ms: int, holdout_fraction: float = 0.2):
        """(train, holdout) by time; train rows whose label window reaches the
        holdout are purged. The holdout is the chronological tail."""
        order = sorted(range(self.n), key=lambda i: (self.times[i], i))
        if not order:
            return self, self
        cut_t = self.times[order[int(len(order) * (1 - holdout_fraction))]] if len(order) > 1 else self.times[order[0]]
        train = [i for i in order if self.times[i] + horizon_ms < cut_t]
        hold = [i for i in order if self.times[i] >= cut_t]
        return self._subset(train), self._subset(hold)

    def _subset(self, idx: Sequence[int]) -> "MLDataset":
        return MLDataset(self.role, tuple(self.rows[i] for i in idx), tuple(self.labels[i] for i in idx),
                         tuple(self.times[i] for i in idx), self.source_kind, self.feature_schema_hash,
                         self.label_schema_hash)


def _features(role: str, record: Mapping[str, Any]) -> Dict[str, Any]:
    return {c: record.get(c) for c in FEATURE_SCHEMAS[role].columns}


def market_dataset(role: str, records: Sequence[Mapping[str, Any]], *, source_kind: str) -> MLDataset:
    """From canonical replay records (Section 22). Every labeled hypothesis is a
    market example regardless of admission / veto / risk verdict."""
    if role not in (ModelRole.OUTCOME.value, ModelRole.RANKING.value, ModelRole.REGIME.value, ModelRole.OOD.value):
        raise DatasetContractViolation(f"{role} is not a MARKET-label role")
    schema = FEATURE_SCHEMAS[role]
    validate_schema(schema)
    rows, labels, times = [], [], []
    ordered = sorted(records, key=lambda r: (r["decision_time"], r["symbol"], r["setup_candidate_id"]))
    next_regime: Dict[str, Any] = {}
    if role == ModelRole.REGIME.value:
        by_symbol: Dict[str, List[Mapping[str, Any]]] = {}
        for r in ordered:
            by_symbol.setdefault(r["symbol"], []).append(r)
        for rs in by_symbol.values():
            for a, b in zip(rs, rs[1:]):
                if b["decision_time"] > a["decision_time"]:
                    next_regime[a["setup_candidate_id"]] = b["regime"]
    for r in ordered:
        if role == ModelRole.OUTCOME.value:
            label = int(float(r["net_R"]) > 0)
        elif role == ModelRole.RANKING.value:
            label = float(r["net_R"])
        elif role == ModelRole.REGIME.value:
            if r["setup_candidate_id"] not in next_regime:
                continue  # no observed future state: no label (never imputed)
            label = next_regime[r["setup_candidate_id"]]
        else:
            label = None
        rows.append(_features(role, r))
        labels.append(label)
        times.append(int(r["decision_time"]))
    return MLDataset(role, tuple(rows), tuple(labels), tuple(times), source_kind, schema.schema_hash,
                     LABEL_SCHEMAS[role].schema_hash)


def market_labels_from_export(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Section 21 export rows -> market labels. Risk-rejected rows KEEP their
    market label; rows without an observed market outcome are skipped; the
    execution/account outcome is never consulted."""
    out = []
    for row in rows:
        mo = row.get("market_outcome") or {}
        term = mo.get("terminal_outcome")
        if term in (None, "UNLABELED") or not mo.get("market_observation_valid", True):
            continue
        out.append({"trade_plan_id": (row.get("lineage") or {}).get("trade_plan_id"), "terminal_outcome": term,
                    "net_R": mo.get("net_R"), "risk_rejected": bool((row.get("execution_outcome") or {})
                                                                     .get("hard_risk_rejected"))})
    return out


def execution_dataset(attempts: Sequence[Mapping[str, Any]], *, source_kind: str) -> MLDataset:
    """SLIPPAGE labels from EXECUTION evidence only: filled attempts with both
    requested and filled prices. Rejected / unknown submits are not labels."""
    schema = FEATURE_SCHEMAS[ModelRole.SLIPPAGE.value]
    validate_schema(schema)
    rows, labels, times = [], [], []
    for a in sorted(attempts, key=lambda x: (x["recorded_at"], x.get("execution_attempt_id", ""))):
        if a.get("status") not in ("FILLED", "PARTIALLY_FILLED"):
            continue
        req, fill = a.get("requested_price"), a.get("filled_price")
        if not req or not fill:
            continue
        sign = 1.0 if str(a.get("side", "LONG")).upper() in ("LONG", "BUY") else -1.0
        labels.append(sign * (float(fill) - float(req)) / float(req) * 10_000.0)
        rows.append({c: a.get(c) for c in schema.columns})
        times.append(int(a["recorded_at"]))
    return MLDataset(ModelRole.SLIPPAGE.value, tuple(rows), tuple(labels), tuple(times), source_kind,
                     schema.schema_hash, LABEL_SCHEMAS[ModelRole.SLIPPAGE.value].schema_hash)


def reject_account_labels(label_family: str) -> None:
    if label_family == OutcomeFamily.ACCOUNT.value:
        raise DatasetContractViolation("ACCOUNT outcomes (capital/slot/margin permission) are never ML labels")


__all__ = ["MLDataset", "LABEL_SCHEMAS", "market_dataset", "execution_dataset", "market_labels_from_export",
           "reject_account_labels", "DatasetContractViolation"]
