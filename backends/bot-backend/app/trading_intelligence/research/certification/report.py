"""Deterministic CertificationReport artifact (Sections 22.29-22.30).

Identity = hash of the analytical content (dataset, policies, versions, stage
inputs/results, cost assumptions). ``generated_at`` is metadata and never
moves ``report_hash``. Language is descriptive and scoped: no "profitable",
"safe" or "guaranteed" -- only measured values with their uncertainty.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple

from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.observability.sanitize import sanitize_payload
from app.trading_intelligence.versions import CERTIFICATION_REPORT_SCHEMA_VERSION

from .contracts import STAGE_ORDER, CertificationStageResult, GateResult


@dataclass(frozen=True)
class CertificationReport:
    overall_status: str
    blocking_gates: Tuple[str, ...]
    ready_for_forward_demo: bool
    scope: Mapping[str, Any]
    source_commit: Optional[str]
    source_tree_dirty: Optional[bool]
    dataset_manifest: Mapping[str, Any]
    policy_freeze: Mapping[str, Any]
    certification_policy: Mapping[str, Any]
    stages: Mapping[str, CertificationStageResult]
    gates: Tuple[GateResult, ...]
    library_recertification: Mapping[str, Any]
    data_still_needed: Tuple[str, ...]
    out_of_scope: Mapping[str, str]
    replay: Mapping[str, Any]
    schema_version: str = CERTIFICATION_REPORT_SCHEMA_VERSION
    generated_at: Optional[str] = field(default=None, compare=False)
    #: operational facts (cache reuse etc.) -- metadata, never analytical identity
    operational: Mapping[str, Any] = field(default_factory=dict, compare=False)

    def analytical(self) -> Dict[str, Any]:
        return sanitize_payload(json.loads(json.dumps({
            "overall_status": self.overall_status, "blocking_gates": list(self.blocking_gates),
            "ready_for_forward_demo": self.ready_for_forward_demo, "scope": dict(self.scope),
            "source_commit": self.source_commit, "source_tree_dirty": self.source_tree_dirty,
            "dataset_manifest": dict(self.dataset_manifest), "policy_freeze": dict(self.policy_freeze),
            "certification_policy": dict(self.certification_policy),
            "stages": {s: self.stages[s].to_dict() for s in STAGE_ORDER if s in self.stages},
            "gates": [g.to_dict() for g in self.gates], "library_recertification": dict(self.library_recertification),
            "data_still_needed": list(self.data_still_needed), "out_of_scope": dict(self.out_of_scope),
            "replay": dict(self.replay), "schema_version": self.schema_version,
            "promotion": {"cati_active_execution_enabled": False, "section_25_owns_promotion": True},
        }, default=str)))

    @property
    def report_hash(self) -> str:
        return stable_hash(self.analytical())

    def to_dict(self) -> Dict[str, Any]:
        return {"report_hash": self.report_hash, **self.analytical(),
                "metadata": {"generated_at": self.generated_at, **dict(self.operational)}}

    def write(self, directory: Path) -> Tuple[Path, Path]:
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        stem = f"certification_{self.report_hash[:16]}"
        jpath, mpath = directory / f"{stem}.json", directory / f"{stem}.md"
        jpath.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True, default=str), encoding="utf-8")
        mpath.write_text(render_markdown(self), encoding="utf-8")
        return jpath, mpath


def _fmt(x: Any, nd: int = 4) -> str:
    if x is None:
        return "n/a"
    if isinstance(x, float):
        return f"{x:.{nd}f}"
    return str(x)


def _pop_line(pop: Mapping[str, Any]) -> str:
    net = pop.get("net") or {}
    dd = pop.get("drawdown_tail") or {}
    return (f"n={pop.get('n')} mean_net_R={_fmt(net.get('mean_R'))} median={_fmt(net.get('median_R'))} "
            f"CI=[{_fmt(net.get('ci_low'))}, {_fmt(net.get('ci_high'))}] P(E>0)={_fmt(net.get('p_expectancy_positive'))} "
            f"gross_mean_R={_fmt(pop.get('gross_mean_R'))} cost_mean_R={_fmt(pop.get('cost_mean_R'))} "
            f"win_rate={_fmt(net.get('win_rate'))} outcomes={net.get('outcome_counts')} "
            f"MFE={_fmt(pop.get('mfe_mean_R'))} MAE={_fmt(pop.get('mae_mean_R'))} "
            f"maxDD_R={_fmt(dd.get('max_drawdown_R'))} worst_R={_fmt(dd.get('worst_trade_R'))} "
            f"ES={_fmt(dd.get('expected_shortfall_R'))} loss_streak={dd.get('max_loss_streak')}")


def render_markdown(r: CertificationReport) -> str:
    a = r.analytical()
    lines = [
        "# CATI Certification Report (Section 22)", "",
        f"- report_hash: `{r.report_hash}`",
        f"- OVERALL: **{r.overall_status}** (blocking: {', '.join(r.blocking_gates) or 'none'})",
        f"- ready_for_forward_demo: {r.ready_for_forward_demo}  | CATI active execution: OFF (Section 25 owns promotion)",
        f"- source commit: `{r.source_commit}` (dirty={r.source_tree_dirty})",
        f"- dataset: manifest `{a['dataset_manifest'].get('manifest_hash', 'n/a')}` "
        f"source_kind={a['dataset_manifest'].get('source_kind')} coverage_days={a['dataset_manifest'].get('coverage_days')}",
        f"- policy freeze: `{a['policy_freeze'].get('freeze_hash')}`  certification policy: "
        f"`{a['certification_policy'].get('policy_hash')}`", "",
        "## Scope", "", "```", json.dumps(a["scope"], indent=1), "```", "",
        "Out of scope (not certified by this report): " + "; ".join(f"{k}: {v}" for k, v in a["out_of_scope"].items()), "",
        "## Stages", "", "| stage | status | reasons |", "|---|---|---|",
    ]
    for s in STAGE_ORDER:
        st = a["stages"].get(s)
        lines.append(f"| {s} | {st['status'] if st else 'NOT_RUN'} | {', '.join(st['reason_codes']) if st else ''} |")
    lines += ["", "## Stage measurements", ""]
    for s in STAGE_ORDER:
        st = a["stages"].get(s)
        m = (st or {}).get("metrics") or {}
        if not m.get("populations"):
            continue
        lines += [f"### {s}", "", f"counts: {m['counts']}", ""]
        for p in ("APPROVED", "ADMISSIBLE", "ALL"):
            lines.append(f"- {p}: {_pop_line(m['populations'][p])}")
        lines.append("")
        lines.append("cost stress (re-selected APPROVED / ADMISSIBLE):")
        for mult, e in m.get("cost_stress", {}).items():
            lines.append(f"- {mult}x: APPROVED {_pop_line(e['reselected']['APPROVED'])}")
            lines.append(f"  - ADMISSIBLE {_pop_line(e['reselected']['ADMISSIBLE'])}; change={e['admission_change']}")
        cal = m.get("calibration") or {}
        lines += ["", f"calibration (all usable forecasts): n={cal.get('n')} brier={_fmt(cal.get('brier'))} "
                      f"log_score={_fmt(cal.get('log_score'))} ece={_fmt(cal.get('ece'))} skill={_fmt(cal.get('brier_skill'))}"]
        conc = m.get("concentration", {})
        lines.append(f"concentration ({conc.get('population')}): " + "; ".join(
            f"{k}: {v.get('largest_segment')} {_fmt(v.get('largest_positive_share'))}" for k, v in conc.get("by", {}).items()))
        nb = m.get("parameter_neighbors", {})
        lines.append(f"parameter neighbors: approved_positive_share={_fmt(nb.get('approved_positive_share'))} "
                     f"admissible_positive_share={_fmt(nb.get('admissible_positive_share'))}")
        of = m.get("overfitting", {})
        lines.append(f"PBO/CSCV: {of.get('pbo_cscv', {}).get('status')} pbo={_fmt(of.get('pbo_cscv', {}).get('pbo'))} "
                     f"| DSR: {of.get('deflated_sharpe', {}).get('status')} "
                     f"dsr={_fmt(of.get('deflated_sharpe', {}).get('deflated_sharpe_ratio'))}")
        lines.append("")
    lines += ["## Gates", "", "| gate | status | reasons |", "|---|---|---|"]
    for g in a["gates"]:
        lines.append(f"| {g['gate']} | {g['status']} | {', '.join(g['reason_codes'])} |")
    lines += ["", "## Historical library", "", json.dumps(a["library_recertification"], default=str), "",
              "## Policy thresholds NOT_CONFIGURED (fail closed)", "",
              ", ".join(a["certification_policy"].get("not_configured", [])) or "none", "",
              "## Data still needed", ""] + [f"- {d}" for d in a["data_still_needed"]] + [""]
    return "\n".join(lines)


__all__ = ["CertificationReport", "render_markdown"]
