"""The one official CATI evaluation of the daily trend mandate (Section H, Step 2.4 / 2.5).

A run is refused unless everything it depends on is pinned in the research register and still matches:

    registered mandate (specification + rule artifact + amendments)  ->  ``verify_mandate``
    frozen dataset (manifest hash, table content hashes)             ->  ``DATASET_FROZEN`` record
    registered cost model                                            ->  ``COST_MODEL_REGISTERED`` record
    committed evaluation source                                      ->  ``source_fingerprint``

It then writes RUN_STARTED, evaluates, writes its artifacts and RUN_COMPLETED (or RUN_FAILED: a failed run
stays in the log). The same inputs give the same run id, so asking again returns the stored result instead of
creating a second run. A development run never loads a held-back row: the dataset handle hands out rows after
the development period only against the ``HoldoutAccess`` token that ``open_holdout_once`` returns after it
has durably recorded the opening.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import platform
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

import numpy as np

from app.trading_intelligence.families.daily_trend import spec, targets as T
from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.research.governance import holdout as H, statistics as STAT
from app.trading_intelligence.research.governance.history import HISTORICAL_ANCHOR
from app.trading_intelligence.research.governance.mandates import (
    EVALUATION_SOURCE_ROOTS, source_fingerprint, verify_mandate,
)
from app.trading_intelligence.research.governance.register import (
    COST_MODEL_REGISTERED, DATASET_FROZEN, HYPOTHESIS_STATUS, RUN_COMPLETED, RUN_FAILED, RUN_STARTED, RegisterError,
    ResearchRegister, repository_root, utc_now_iso,
)

from . import metrics as M, simulator as S

RESULT_SCHEMA = "cati-mandate-evaluation-v1"
EVALUATOR_VERSION = "daily-trend-evaluator-1"
COST_MODEL_ID, COST_MODEL_VERSION = "MANDATE_004_FROZEN_COSTS", "1"
STATISTICAL_POLICY_DECISION = "STATISTICAL_GATE_POLICY_V1"
DEVELOPMENT, HOLDOUT = "DEVELOPMENT", "HOLDOUT"
PRIMARY = (T.MANDATE, "balanced", 1.0, True, False)
VERDICTS = ("CERTIFIED_FOR_RESEARCH_PROMOTION", "FAILED_CERTIFICATION", "INSUFFICIENT_EVIDENCE",
            "BLOCKED_PENDING_AUTHORIZATION", "INVALID_EVALUATION", "PENDING_HOLDOUT")
NUMERIC_TOLERANCE = 1e-9


class EvaluationRefused(RegisterError):
    """A precondition of an official run does not hold; nothing was evaluated and nothing was logged."""


class HoldoutEmbargo(RuntimeError):
    """Held-back rows were requested without the access token of a recorded holdout opening."""


@dataclass(frozen=True)
class Periods:
    development_start: str = spec.DEVELOPMENT_START
    development_end: str = spec.DEVELOPMENT_END
    holdout_start: str = spec.HOLDOUT_START
    holdout_end: str = spec.HOLDOUT_END


# ---------------------------------------------------------------------- dataset handle (the embargo lives here)
class DatasetHandle:
    """The frozen dataset as the evaluator sees it. ``panel`` is the ONLY way rows reach an evaluation."""

    def __init__(self, store: Path, docs: Path):
        from app.market_data import daily_dataset as DD

        name = f"{DD.DATASET_ID}_{DD.DATASET_VERSION}"
        self.store, self.docs = Path(store), Path(docs)
        self.manifest_path = self.docs / "datasets" / f"{name}.dataset.json"
        self.manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.coverage = json.loads((self.docs / "coverage" / f"{name}.coverage.json").read_text(encoding="utf-8"))
        self.metadata = json.loads((self.docs / "datasets" / f"{name}.metadata.json").read_text(encoding="utf-8"))
        DD.verify_manifest(self.manifest)
        if self.coverage["manifest_hash"] != self.manifest["manifest_hash"]:
            raise EvaluationRefused("the coverage report belongs to another dataset manifest")
        if stable_hash({k: v for k, v in self.metadata.items() if k not in ("content_hash", "raw_sha256", "fetched_at")}
                       ) != self.metadata["content_hash"]:
            raise EvaluationRefused("the exchange metadata snapshot was edited")

    @property
    def dataset_hash(self) -> str:
        return self.manifest["dataset_hash"]

    @property
    def manifest_hash(self) -> str:
        return self.manifest["manifest_hash"]

    def _load(self, first_day: str, last_day: str) -> Any:
        from app.market_data import daily_dataset as DD

        return DD.load_panel(self.store, self.manifest, self.coverage, first_day=first_day, last_day=last_day)

    def panel(self, *, periods: Periods, last_day: str, access: Optional[H.HoldoutAccess] = None) -> Any:
        if last_day > periods.development_end:
            ok = (isinstance(access, H.HoldoutAccess) and access.start == periods.holdout_start
                  and access.end == periods.holdout_end and last_day <= access.end)
            if not ok:
                raise HoldoutEmbargo(f"rows after {periods.development_end} are held back: a recorded holdout opening "
                                     "is required")
        return self._load(periods.development_start, last_day)


# ---------------------------------------------------------------------- registered inputs
def frozen_cost_model() -> Dict[str, Any]:
    """The mandate's cost assumptions as CATI's research ``CostModel`` (shared definition, mandate values).
    Slippage here covers the spread: no separate spread is charged, so nothing is counted twice."""
    from app.replay.cost_model import CostModel

    s, i = spec.SPECIFICATION, spec.INTERPRETATION
    model = CostModel(maker_fee=s["taker_fee"], taker_fee=s["taker_fee"], spread=0.0, slippage=s["slippage"],
                      funding_rate=0.0, notes="Mandate 004 frozen costs; funding from actual archive rates")
    body = {"cost_model_id": COST_MODEL_ID, "cost_model_version": COST_MODEL_VERSION, "model": model.to_dict(),
            "replay_cost_model_hash": model.model_hash, "stress_multiple": s["stress_cost_multiple"],
            "funding": s["funding"], "missing_funding_rate_per_8h": i["missing_funding_rate_per_8h"],
            "missing_funding_max_share": i["missing_funding_max_share"],
            "basis": "ASSUMED_BY_THE_FROZEN_MANDATE (0.05% taker fee + 0.05% slippage per side); not measured"}
    return {**body, "cost_model_hash": stable_hash(body)}


def register_cost_model(register: ResearchRegister, *, now: Optional[str] = None) -> Dict[str, Any]:
    cm = frozen_cost_model()
    for r in register.of_type(COST_MODEL_REGISTERED):
        if (r["body"]["cost_model_id"], r["body"]["cost_model_version"]) == (COST_MODEL_ID, COST_MODEL_VERSION):
            if r["body"]["cost_model_hash"] != cm["cost_model_hash"]:
                raise EvaluationRefused("the registered cost model differs from the one in code")
            return r
    return register.append(COST_MODEL_REGISTERED, cm, now=now)


def freeze_dataset_in_register(register: ResearchRegister, dataset: DatasetHandle, *, now: Optional[str] = None) -> Dict[str, Any]:
    m = dataset.manifest
    for r in register.of_type(DATASET_FROZEN):
        if (r["body"]["dataset_id"], r["body"]["dataset_version"]) == (m["dataset_id"], m["dataset_version"]):
            if r["body"]["manifest_hash"] != m["manifest_hash"]:
                raise EvaluationRefused("this dataset version is frozen in the register with a different manifest")
            return r
    rel = dataset.manifest_path.resolve()
    try:
        rel_text = rel.relative_to(repository_root()).as_posix()
    except ValueError:
        rel_text = rel.name
    return register.append(DATASET_FROZEN, {
        "dataset_id": m["dataset_id"], "dataset_version": m["dataset_version"], "manifest_path": rel_text,
        "manifest_hash": m["manifest_hash"], "dataset_hash": m["dataset_hash"],
        "coverage": [m["coverage_start"], m["coverage_end"]], "symbols_with_bars": m["symbols_with_bars"],
        "quality_totals": m["quality_totals"]}, now=now)


def statistical_policy(register: ResearchRegister) -> STAT.StatisticalGatePolicy:
    """The approved thresholds when an owner decision records them; otherwise the fail-closed default."""
    d = register.decision(STATISTICAL_POLICY_DECISION)
    if not d or d["state"] != "APPROVED":
        return STAT.unapproved_policy()
    return STAT.StatisticalGatePolicy(
        name=STATISTICAL_POLICY_DECISION, familywise_alpha=float(d["familywise_alpha"]),
        required_power=float(d["required_power"]), target_annual_sharpe=float(d["target_annual_sharpe"]),
        approved_by=d["decided_by"], authorization_reference=d["authorization_reference"])


def source_state(repo_root: Optional[Path] = None) -> Dict[str, Any]:
    base = Path(repo_root) if repo_root else repository_root()

    def git(*args: str) -> Optional[str]:
        try:
            out = subprocess.run(["git", *args], cwd=str(base), capture_output=True, text=True, timeout=30)
        except Exception:
            return None
        return out.stdout.strip() if out.returncode == 0 else None

    dirty = git("status", "--porcelain", "--", *EVALUATION_SOURCE_ROOTS, spec.SPECIFICATION_PATH)
    return {"fingerprint": source_fingerprint(repo_root=base), "commit": git("rev-parse", "HEAD"),
            "evaluation_source_dirty": None if dirty is None else bool(dirty),
            "dirty_paths": (dirty or "").splitlines()[:20]}


# ---------------------------------------------------------------------- evaluation
def scenario_matrix(feat: S.Features, start_day: int, end_day: int) -> Dict[Tuple, S.SimResult]:
    """Every registered scenario: 3 risk levels x {mandate, executable} x {base, stress} x {brakes on, off},
    plus the funding-overlay secondary test (mandate, base cost, brakes on) per level."""
    out: Dict[Tuple, S.SimResult] = {}
    for policy in (T.MANDATE, T.EXECUTABLE):
        for level in spec.RISK_LEVELS:
            lv = T.risk_level(level, policy)
            for cost in (1.0, float(spec.SPECIFICATION["stress_cost_multiple"])):
                for brakes in (True, False):
                    cfg = S.RunConfig(level=lv, start_day=start_day, end_day=end_day, cost_multiple=cost, drawdown_brakes=brakes)
                    out[(policy, level, cost, brakes, False)] = S.simulate(feat, cfg)
    for level in spec.RISK_LEVELS:
        cfg = S.RunConfig(level=T.risk_level(level, T.MANDATE), start_day=start_day, end_day=end_day, funding_overlay=True)
        out[(T.MANDATE, level, 1.0, True, True)] = S.simulate(feat, cfg)
    return out


def causality_audit(panel: Any, metadata: Mapping[str, Any], *, start_day: int, end_day: int, seed: int = 4004) -> Dict[str, Any]:
    """Adversarial check ON THE DATA OF THE RUN: (a) truncate the panel at a cut, (b) rewrite everything after
    the cut. In both cases every decision, fill and equity value up to the cut must be IDENTICAL to the full run."""
    lv = T.risk_level("balanced", T.MANDATE)
    cfg = S.RunConfig(level=lv, start_day=start_day, end_day=end_day)
    base = S.simulate(S.prepare(panel, metadata), cfg)
    rng = np.random.default_rng(seed)
    span = end_day - start_day
    checks = []
    for share in (0.35, 0.6, 0.85):
        cut = start_day + int(span * share)
        k = int(np.searchsorted(panel.days, cut, side="right"))
        if k < 2 or k >= len(panel.days):
            continue
        variants = {"TRUNCATED": panel.truncated(cut)}
        rewritten = type(panel)(panel.days, panel.symbols, {n: np.array(v, copy=True) for n, v in panel.fields.items()})
        for name, values in rewritten.fields.items():
            values[k:] = values[k:] * rng.uniform(0.25, 4.0, values[k:].shape)
        variants["FUTURE_REWRITTEN"] = rewritten
        for kind, variant in variants.items():
            other = S.simulate(S.prepare(variant, metadata), S.RunConfig(level=lv, start_day=start_day, end_day=cut))
            rows = cut - start_day + 1
            same = bool(np.array_equal(base.daily["equity"][:rows], other.daily["equity"][:rows]))
            for field in ("fills", "targets", "rejections", "events", "entry_decisions"):
                a = [x for x in getattr(base, field) if x["day"] <= cut]
                b = [x for x in getattr(other, field) if x["day"] <= cut]
                same = same and (a if field != "targets" else a[:-1]) == (b if field != "targets" else b[:-1])
            checks.append({"cut_day": M._date(cut).isoformat(), "kind": kind, "identical_up_to_cut": same})
    return {"ok": bool(checks) and all(c["identical_up_to_cut"] for c in checks), "checks": checks,
            "decisions_compared": len(base.targets), "fills_compared": len(base.fills)}


def _label(key: Tuple) -> str:
    policy, level, cost, brakes, overlay = key
    return "|".join([policy, level, "base" if cost == 1.0 else f"cost_x{cost:g}", "brakes_on" if brakes else "brakes_off",
                     "overlay" if overlay else "no_overlay"])


def _capacity(primary: S.SimResult, feat: S.Features) -> Dict[str, Any]:
    row = {int(d): i for i, d in enumerate(feat.days)}
    col = {s: j for j, s in enumerate(feat.symbols)}
    shares = []
    for f in primary.fills:
        if f["kind"] in (T.ENTRY, T.ADJUST):
            vol = feat.quote_volume[row[f["day"]], col[f["symbol"]]]
            if vol and vol > 0:
                shares.append(abs(f["quantity"]) * f["price"] / vol)
    if not shares:
        return {"orders": 0, "note": "no orders to measure"}
    a = np.array(shares)
    base_equity = float(primary.config.initial_equity)
    table = {}
    for capital in (1e4, 1e5, 1e6, 1e7, 1e8):
        k = capital / base_equity
        table[f"{capital:.0f}"] = {"median_order_share_of_daily_volume": float(np.median(a) * k),
                                   "p95_order_share_of_daily_volume": float(np.quantile(a, 0.95) * k),
                                   "max_order_share_of_daily_volume": float(a.max() * k),
                                   "orders_above_1pct_of_daily_volume": float(np.mean(a * k > 0.01))}
    return {"orders": len(shares), "account_equity_usdt": table, "basis": "order notional / that day's traded quote volume",
            "limits": "daily volume is not depth: no order-book, spread or impact data exists in the dataset; the "
                      "frozen 0.05% slippage is not scaled with size. These figures bound participation, not capacity."}


def _statistics(returns: np.ndarray, register: ResearchRegister, *, valid: bool, reasons: Sequence[str], trades: int,
                bootstrap_repetitions: Optional[int]) -> Dict[str, Any]:
    policy = statistical_policy(register)
    if bootstrap_repetitions:
        from dataclasses import replace

        policy = replace(policy, bootstrap_repetitions=int(bootstrap_repetitions))
    return STAT.evaluate_portfolio_gate([float(x) for x in returns], hypotheses_in_register=register.hypothesis_count(),
                                        policy=policy, evidence_valid=valid, invalid_reasons=list(reasons), trade_count=trades)


def _verdict(stage: str, *, pass_rule: str, gate: str, evidence_valid: bool, data_ok: bool) -> str:
    if not evidence_valid:
        return "INVALID_EVALUATION"
    if pass_rule == M.FAIL:
        return "FAILED_CERTIFICATION"            # a fail of the frozen rule is final whatever else is pending
    if not data_ok:
        return "INSUFFICIENT_EVIDENCE"
    if stage == DEVELOPMENT or pass_rule == M.PENDING:
        return "PENDING_HOLDOUT"
    return {STAT.PASS: "CERTIFIED_FOR_RESEARCH_PROMOTION", STAT.FAIL: "FAILED_CERTIFICATION",
            STAT.BLOCKED_PENDING_APPROVAL: "BLOCKED_PENDING_AUTHORIZATION", STAT.INVALID_EVIDENCE: "INVALID_EVALUATION",
            }.get(gate, "INSUFFICIENT_EVIDENCE")


def _period_block(feat: S.Features, start: str, end: str) -> Dict[str, Any]:
    from app.market_data.daily_dataset import day_index

    a, b = day_index(start), day_index(end)
    sims = scenario_matrix(feat, a, b)
    summaries = {_label(k): M.summarize(v) for k, v in sims.items()}
    primary = sims[PRIMARY]
    vol = summaries[_label(PRIMARY)].get("annual_volatility")
    bench = {name: M.scaled_to(r, vol) for name, r in M.benchmark_returns(
        feat, a, b, spec.SPECIFICATION["taker_fee"] + spec.SPECIFICATION["slippage"]).items()}
    members = feat.member[np.searchsorted(feat.days, a):np.searchsorted(feat.days, b, side="right")]
    ever = np.flatnonzero(members.any(axis=0))
    universe = {"days": int(members.shape[0]), "days_with_a_full_universe": int((members.sum(axis=1) == spec.SPECIFICATION["universe_size"]).sum()),
                "days_with_no_universe": int((members.sum(axis=1) == 0).sum()), "contracts_ever_in_universe": int(len(ever)),
                "average_members": float(members.sum(axis=1).mean())}
    return {"first_day": start, "last_day": end, "scenarios": summaries, "benchmarks": bench, "universe": universe,
            "entry_stop_distances": M.entry_stop_distances(primary),
            "capacity": _capacity(primary, feat), "_sims": sims,
            "_ever": [feat.symbols[j] for j in ever]}


def _data_quality(blocks: Sequence[Mapping[str, Any]], dataset: DatasetHandle) -> Dict[str, Any]:
    limit = spec.INTERPRETATION["missing_funding_max_share"]
    reasons, worst, gap_exits = [], 0.0, 0
    ended = set(dataset.manifest["contracts_that_ended_before_coverage_end"]["symbols"])
    for block in blocks:
        for label, s in block["scenarios"].items():
            worst = max(worst, s["funding_imputed_share"])
        gap_exits += block["scenarios"][_label(PRIMARY)]["trades"]["exit_reasons"].get(S.EXIT_DATA_GAP, 0)
    if worst > limit:
        reasons.append("FUNDING_COVERAGE_INSUFFICIENT")
    totals = dataset.manifest["quality_totals"]
    for key in ("failed_downloads", "unreadable_files", "invalid_candles"):
        if totals.get(key):
            reasons.append(f"DATASET_{key.upper()}")
    traded_ended = sorted(set(s for b in blocks for s in b["_ever"]) & ended)
    return {"ok": not reasons, "reason_codes": reasons, "largest_funding_imputed_share": worst,
            "funding_imputed_share_limit": limit, "data_gap_exits_in_primary_run": gap_exits,
            "ended_contracts_that_were_in_the_universe": traded_ended,
            "dataset_missing_candles": totals.get("missing_candles"), "dataset_non_trading_bars": totals.get("non_trading_bars")}


def _csv(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(columns)
    for r in rows:
        w.writerow(["" if r.get(c) is None else (repr(float(r[c])) if isinstance(r.get(c), float) else r[c]) for c in columns])
    return buf.getvalue().encode("utf-8")


def _artifacts(result: Mapping[str, Any], blocks: Mapping[str, Mapping[str, Any]]) -> Dict[str, bytes]:
    files: Dict[str, bytes] = {}
    for name, block in blocks.items():
        sims = block["_sims"]
        primary = sims[PRIMARY]
        days = [M._date(d).isoformat() for d in primary.days]
        labels = [_label(k) for k in sims]
        eq = [dict({"day": days[i]}, **{_label(k): float(v.daily["equity"][i]) for k, v in sims.items()}) for i in range(len(days))]
        files[f"{name}_equity.csv"] = _csv(eq, ["day", *labels])
        d = primary.daily
        daily = [{"day": days[i], **{k: float(d[k][i]) for k in d}} for i in range(len(days))]
        files[f"{name}_primary_daily.csv"] = _csv(daily, ["day", *d])
        dt = lambda rows: [dict(r, day=M._date(r["day"]).isoformat(),                       # noqa: E731
                                **{k: M._date(r[k]).isoformat() for k in ("entry_day", "exit_day") if r.get(k) is not None})
                           for r in rows]
        files[f"{name}_primary_trades.csv"] = _csv(dt([dict(t, day=t["entry_day"]) for t in primary.trades]),
                                                  ["symbol", "entry_day", "exit_day", "entry_price", "exit_price", "exit_reason",
                                                   "quantity", "max_quantity", "entry_stop_distance", "days_held", "pnl_price",
                                                   "fees", "slippage", "funding", "net_pnl", "adjustments"])
        files[f"{name}_primary_fills.csv"] = _csv(dt(primary.fills), ["day", "symbol", "kind", "quantity", "price"])
        files[f"{name}_primary_rejections.csv"] = _csv(dt(primary.rejections), ["day", "stage", "symbol", "reason"])
        files[f"{name}_primary_entry_decisions.csv"] = _csv(dt(primary.entry_decisions),
                                                           ["day", "symbol", "strength", "stop_distance", "accepted", "reason"])
        files[f"{name}_primary_events.csv"] = _csv(dt(primary.events), ["day", "symbol", "event", "stop", "open", "low", "price", "drawdown"])
        targets = [{"day": M._date(t["day"]).isoformat(), "symbol": s, "target_fraction_of_equity": v["fraction_of_equity"],
                    "target_notional": v["notional"], "strength": v["strength"], "stop_distance": v["stop_distance"],
                    "equity": t["equity"]} for t in primary.targets for s, v in t["targets"].items()]
        files[f"{name}_primary_daily_targets.csv"] = _csv(targets, ["day", "symbol", "target_fraction_of_equity", "target_notional",
                                                                    "strength", "stop_distance", "equity"])
    files["result.json"] = (json.dumps(result, indent=1, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")
    return files


def _clean(obj: Any) -> Any:
    """JSON-safe: private keys dropped, numpy scalars to Python, non-finite floats to None."""
    if isinstance(obj, Mapping):
        return {str(k): _clean(v) for k, v in obj.items() if not str(k).startswith("_")}
    if isinstance(obj, (list, tuple)):
        return [_clean(v) for v in obj]
    if isinstance(obj, (np.floating, float)):
        return float(obj) if np.isfinite(obj) else None
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    return obj


def run_evaluation(run_type: str, *, register: Optional[ResearchRegister] = None, dataset: Optional[DatasetHandle] = None,
                   artifacts_root: Optional[Path] = None, periods: Optional[Periods] = None, researcher: str = "UNSPECIFIED",
                   parent_run_id: Optional[str] = None, reason_for_rerun: Optional[str] = None,
                   holdout_id: Optional[str] = None, require_committed_source: bool = True,
                   bootstrap_repetitions: Optional[int] = None, anchor: Optional[Tuple[int, str]] = None,
                   source: Optional[Mapping[str, Any]] = None, now: Optional[Callable[[], str]] = None) -> Dict[str, Any]:
    if run_type not in (DEVELOPMENT, HOLDOUT):
        raise EvaluationRefused(f"unknown run type {run_type!r}")
    now = now or utc_now_iso
    register = register or ResearchRegister()
    if register.path == ResearchRegister().path and anchor is None and HISTORICAL_ANCHOR:
        anchor = tuple(HISTORICAL_ANCHOR)
    register.records(anchor=anchor)                                   # chain + anchored history, or refuse
    periods = periods or Periods()
    root = repository_root()
    dataset = dataset or DatasetHandle(root / "data" / "research" / "binance_usdm_daily_v1", root / "docs" / "research")
    artifacts_root = Path(artifacts_root) if artifacts_root else root / "docs" / "research" / "mandate_004" / "runs"

    pin = verify_mandate(register, spec.MANDATE_ID, rule_artifact=spec.RULE_ARTIFACT)       # unregistered / drift -> refused
    frozen = [r["body"] for r in register.of_type(DATASET_FROZEN) if r["body"]["dataset_hash"] == dataset.dataset_hash
              and r["body"]["manifest_hash"] == dataset.manifest_hash]
    if not frozen:
        raise EvaluationRefused("the dataset is not frozen in the research register (or its manifest changed)")
    cost = frozen_cost_model()
    if not any(r["body"]["cost_model_hash"] == cost["cost_model_hash"] for r in register.of_type(COST_MODEL_REGISTERED)):
        raise EvaluationRefused("the cost model is not registered (or differs from the registered one)")
    src = dict(source) if source is not None else source_state()
    if require_committed_source and src.get("evaluation_source_dirty") is not False:
        raise EvaluationRefused(f"evaluation source is not committed: {src.get('dirty_paths')}")
    policy = statistical_policy(register)
    period = [periods.development_start, periods.development_end] if run_type == DEVELOPMENT else \
        [periods.development_start, periods.holdout_end]
    parameters = {"periods": asdict(periods), "primary": _label(PRIMARY), "evaluator": EVALUATOR_VERSION,
                  "simulator": S.SIMULATOR_VERSION, "statistical_policy_hash": policy.policy_hash,
                  "bootstrap_repetitions": bootstrap_repetitions or policy.bootstrap_repetitions}
    identity = {"run_type": run_type, "strategy_hash": pin["strategy_hash"], "dataset_hash": dataset.dataset_hash,
                "source_fingerprint": src["fingerprint"], "parameter_hash": stable_hash(parameters),
                "cost_model_hash": cost["cost_model_hash"], "period": period, "parent_run_id": parent_run_id}
    run_id = short_id("run", identity)
    out_dir = artifacts_root / run_id

    previous = next((r for r in register.runs(mandate_id=spec.MANDATE_ID) if r["run_id"] == run_id), None)
    if previous is not None:
        if previous["state"] == RUN_COMPLETED:                        # idempotent: the stored result, never a second run
            stored = json.loads((out_dir / "result.json").read_text(encoding="utf-8"))
            if stored["result_hash"] != previous["end"]["result_hash"]:
                raise EvaluationRefused(f"stored result of {run_id} does not match the register")
            return stored
        raise EvaluationRefused(f"run {run_id} is {previous['state']}: start a rerun that names it as parent and says why")

    hid = H.reserve_holdout(register, mandate_id=spec.MANDATE_ID, dataset_hash=dataset.dataset_hash,
                            start=periods.holdout_start, end=periods.holdout_end, now=now())
    if holdout_id is not None and holdout_id != hid:
        raise EvaluationRefused("the holdout id does not belong to this mandate, dataset and window")
    access = None
    authorization = None
    if run_type == HOLDOUT:
        # RUN_STARTED is written first so that even a refused opening leaves the attempt in the log
        authorization = (register.holdout(hid).get("authorization") or {}).get("authorization_reference")
    register.append(RUN_STARTED, {
        "run_id": run_id, "run_type": run_type, "mandate_id": spec.MANDATE_ID, "hypothesis_id": pin["hypothesis_id"],
        "strategy_hash": pin["strategy_hash"], "dataset_hash": dataset.dataset_hash, "code_commit": src.get("commit") or "UNKNOWN",
        "source_fingerprint": src["fingerprint"], "parameter_hash": identity["parameter_hash"],
        "cost_model_hash": cost["cost_model_hash"], "researcher": researcher, "started_at": now(), "period": period,
        "authorization": authorization, "parent_run_id": parent_run_id, "reason_for_rerun": reason_for_rerun,
        "holdout_id": hid, "register_head_before": register.head_hash()}, now=now())
    try:
        if run_type == HOLDOUT:
            access = H.open_holdout_once(register, holdout_id=hid, run_id=run_id,
                                         specification_sha256=pin["specification_sha256"], dataset_hash=dataset.dataset_hash,
                                         source_fingerprint=src["fingerprint"], code_commit=src.get("commit") or "UNKNOWN", now=now())
        from app.market_data.daily_dataset import day_index

        last = periods.development_end if run_type == DEVELOPMENT else periods.holdout_end
        panel = dataset.panel(periods=periods, last_day=last, access=access)
        feat = S.prepare(panel, dataset.metadata)
        audit = causality_audit(panel, dataset.metadata, start_day=day_index(periods.development_start),
                                end_day=day_index(periods.development_end))
        blocks: Dict[str, Dict[str, Any]] = {}
        if run_type == DEVELOPMENT:
            blocks["development"] = _period_block(feat, periods.development_start, periods.development_end)
            p = blocks["development"]["scenarios"]
            rule = M.evaluate_pass_rule(
                full_as_specified=None, full_without_brake=None, holdout_base=None, holdout_stress=None,
                development_as_specified=p[_label(PRIMARY)],
                development_without_brake=p[_label((T.MANDATE, "balanced", 1.0, False, False))])
            stat_block, stat_sample = blocks["development"], "DEVELOPMENT_PERIOD"
        else:
            blocks["full"] = _period_block(feat, periods.development_start, periods.holdout_end)
            blocks["holdout"] = _period_block(feat, periods.holdout_start, periods.holdout_end)
            f, h = blocks["full"]["scenarios"], blocks["holdout"]["scenarios"]
            rule = M.evaluate_pass_rule(
                full_as_specified=f[_label(PRIMARY)], full_without_brake=f[_label((T.MANDATE, "balanced", 1.0, False, False))],
                holdout_base=h[_label(PRIMARY)], holdout_stress=h[_label((T.MANDATE, "balanced", 2.0, True, False))])
            stat_block, stat_sample = blocks["holdout"], "HELD_BACK_PERIOD"
        quality = _data_quality(list(blocks.values()), dataset)
        residual = max(abs(s["ledger_residual"]) for b in blocks.values() for s in b["scenarios"].values())
        integrity = {"mandate_verified": True, "dataset_frozen": True, "data_quality_ok": quality["ok"],
                     "causality_verified": audit["ok"], "costs_applied": True, "risk_limits_applied": True,
                     "ledgers_close": residual < 1e-6, "largest_ledger_residual": residual}
        invalid = [k.upper() for k in ("causality_verified", "ledgers_close") if not integrity[k]]
        primary_sim = stat_block["_sims"][PRIMARY]
        gate = _statistics(primary_sim.daily["return"], register, valid=not invalid, reasons=invalid,
                           trades=len(primary_sim.trades), bootstrap_repetitions=bootstrap_repetitions)
        gate["sample"] = stat_sample
        extra_gate = None
        if run_type == HOLDOUT:
            full_sim = blocks["full"]["_sims"][PRIMARY]
            extra_gate = _statistics(full_sim.daily["return"], register, valid=not invalid, reasons=invalid,
                                     trades=len(full_sim.trades), bootstrap_repetitions=bootstrap_repetitions)
            extra_gate["sample"] = "FULL_PERIOD_INFORMATION_ONLY"
        verdict = _verdict(run_type, pass_rule=rule["status"], gate=gate["status"], evidence_valid=not invalid,
                           data_ok=quality["ok"])
        body = _clean({
            "schema": RESULT_SCHEMA, "run_id": run_id, "run_type": run_type, "mandate_id": spec.MANDATE_ID,
            "hypothesis_id": pin["hypothesis_id"], "hypothesis_number": spec.HYPOTHESIS_NUMBER, "family_id": spec.FAMILY_ID,
            "specification_sha256": pin["specification_sha256"], "rule_artifact_hash": pin["rule_artifact_hash"],
            "strategy_hash": pin["strategy_hash"], "amendments": pin["amendments"],
            "dataset": {"dataset_id": dataset.manifest["dataset_id"], "dataset_version": dataset.manifest["dataset_version"],
                        "dataset_hash": dataset.dataset_hash, "manifest_hash": dataset.manifest_hash},
            "cost_model": cost, "statistical_policy_hash": policy.policy_hash, "source_fingerprint": src["fingerprint"],
            "evaluator_version": EVALUATOR_VERSION, "simulator_version": S.SIMULATOR_VERSION,
            "hypotheses_in_register": register.hypothesis_count(), "periods": asdict(periods), "evaluated_period": period,
            "parent_run_id": parent_run_id, "reason_for_rerun": reason_for_rerun, "holdout_id": hid,
            "holdout_opened_by_this_run": run_type == HOLDOUT, "primary_scenario": _label(PRIMARY),
            "integrity": integrity, "causality_audit": audit, "data_quality": quality, "preparation": feat.notes,
            "blocks": blocks, "pass_rule": rule, "mandate_pass_rule": rule["status"], "statistical_gate": gate["status"],
            "statistical_gate_detail": gate, "statistical_gate_full_period": extra_gate, "verdict": verdict,
            "promotion": {"research_pass": verdict == "CERTIFIED_FOR_RESEARCH_PROMOTION", "trading_authorized": False,
                          "governance_phase_changed": False, "live_trading": "DISABLED"},
            "numeric_tolerance": NUMERIC_TOLERANCE})
        result_hash = stable_hash(body)
        result = {**body, "result_hash": result_hash,
                  "provenance": {"code_commit": src.get("commit"), "completed_at": now(), "researcher": researcher,
                                 "environment": {"python": sys.version.split()[0], "numpy": np.__version__,
                                                 "platform": platform.system()}}}
        files = _artifacts(result, blocks)
        out_dir.mkdir(parents=True, exist_ok=True)
        hashes = {}
        for name, payload in sorted(files.items()):
            (out_dir / name).write_bytes(payload)
            hashes[name] = hashlib.sha256(payload).hexdigest()
        if access is not None:
            H.burn_holdout(register, access, result_hash=result_hash, verdict=verdict, artifact_hashes=hashes, now=now())
        register.append(RUN_COMPLETED, {
            "run_id": run_id, "status": "VALID" if not invalid else "INVALID", "completed_at": now(), "verdict": verdict,
            "mandate_pass_rule": rule["status"], "statistical_gate": gate["status"], "result_hash": result_hash,
            "artifact_dir": out_dir.relative_to(root).as_posix() if out_dir.is_relative_to(root) else out_dir.as_posix(),
            "artifact_hashes": hashes}, now=now())
        return result
    except Exception as exc:
        try:
            register.append(RUN_FAILED, {"run_id": run_id, "status": "ERROR", "completed_at": now(),
                                         "error": f"{type(exc).__name__}: {str(exc)[:300]}"}, now=now())
        except RegisterError:
            pass
        raise


def record_verdict(register: ResearchRegister, run_id: str, *, recorded_by: str, now: Optional[str] = None) -> Dict[str, Any]:
    """Write a FINAL certification outcome of a completed, valid run to the hypothesis. Separate from the run on
    purpose: a development run can be repeated after a defect is fixed, a recorded status cannot be withdrawn."""
    run = next((r for r in register.runs(mandate_id=spec.MANDATE_ID) if r["run_id"] == run_id), None)
    if run is None or run["state"] != RUN_COMPLETED or run["end"]["status"] != "VALID":
        raise EvaluationRefused("a verdict is recorded only from a completed, valid run")
    verdict = run["end"]["verdict"]
    status = {"CERTIFIED_FOR_RESEARCH_PROMOTION": "RESEARCH_PASS", "FAILED_CERTIFICATION": "FAILED"}.get(verdict)
    if status is None:
        raise EvaluationRefused(f"{verdict} is not a final outcome; nothing is recorded on the hypothesis")
    return register.append(HYPOTHESIS_STATUS, {
        "hypothesis_id": spec.HYPOTHESIS_ID, "status": status, "reason": verdict, "run_id": run_id,
        "failure_reasons": [] if status == "RESEARCH_PASS" else [f"mandate pass rule: {run['end']['mandate_pass_rule']}",
                                                               f"statistical gate: {run['end']['statistical_gate']}"],
        "result_hash": run["end"]["result_hash"], "recorded_by": recorded_by}, now=now)


__all__ = ["RESULT_SCHEMA", "EVALUATOR_VERSION", "COST_MODEL_ID", "DEVELOPMENT", "HOLDOUT", "PRIMARY", "VERDICTS",
           "Periods", "DatasetHandle", "EvaluationRefused", "HoldoutEmbargo", "frozen_cost_model", "register_cost_model",
           "freeze_dataset_in_register", "statistical_policy", "source_state", "scenario_matrix", "causality_audit",
           "run_evaluation", "record_verdict", "STATISTICAL_POLICY_DECISION", "NUMERIC_TOLERANCE"]
