"""Section H, Step 2.6: the certification report is generated from a run's own result and cannot disagree
with it. Driven by synthetic official runs; the reports produced here describe a synthetic market."""
from __future__ import annotations

import json
import re

import pytest
from _step2 import SRC, SyntheticDataset, authorize, go, make_setup, new_register

from app.trading_intelligence.families.daily_trend import spec
from app.trading_intelligence.research.evaluator import official as O, report as R
from app.trading_intelligence.research.governance import holdout as H


def render(setup, result):
    reg, ds, _ = setup
    return R.build_report(result, register=reg, manifest=ds.manifest)


def section(text, title):
    start = text.index(f"## {title}")
    nxt = [text.index(f"## {t}", start + 1) for t in R.SECTIONS if f"## {t}" in text[start + 1:] and t != title]
    return text[start:min([n for n in nxt if n > start] or [len(text)])]


@pytest.fixture()
def dev_setup(tmp_path):
    setup = make_setup(tmp_path)
    return setup, go(setup)


def test_every_required_section_is_present_in_order_and_the_verdict_comes_first(dev_setup):
    setup, dev = dev_setup
    text = render(setup, dev)
    positions = [text.index(f"## {t}") for t in R.SECTIONS]
    assert positions == sorted(positions) and len(R.SECTIONS) == 13
    head = text[:positions[1]]
    for item in ("Mandate", "Strategy family", "Hypothesis number", "Evaluation date", "Research status",
                 "Certification verdict", "Major result", "Principal risks", "Promotion eligibility"):
        assert item in head, item
    assert head.index("Certification verdict") < head.index("Major result")
    assert "A research pass, a governance promotion and a trading authorization are three different things" in head


def test_the_report_carries_the_mandate_hash_the_hypothesis_number_and_the_run_identity(dev_setup):
    setup, dev = dev_setup
    text = render(setup, dev)
    assert spec.SPECIFICATION_SHA256 in text and dev["result_hash"] in text and dev["run_id"] in text
    assert "| Hypothesis number | 18 of 18 in the research register |" in text
    assert setup[1].manifest["dataset_hash"] in text and "MANDATE_004" in text
    register_part = section(text, "Part L — Complete experiment register")
    assert all(f"| {n} |" in register_part for n in range(1, 19))             # all eighteen hypotheses are listed
    assert "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1" in register_part and "Amendment 2" in register_part


def test_headline_numbers_are_the_runs_numbers(dev_setup):
    setup, dev = dev_setup
    text = render(setup, dev)
    s = dev["blocks"]["development"]["scenarios"][dev["primary_scenario"]]
    row = next(line for line in text.splitlines() if line.startswith("| MANDATE / balanced / base / brakes_on / no_overlay |"))
    cells = [c.strip() for c in row.strip("|").split("|")]
    assert cells[1] == R.pct(s["net_return"]) and cells[4] == R.num(s["sharpe"]) and cells[6] == R.pct(s["max_drawdown"])
    assert int(cells[8]) == s["trades"]["trades_closed"]
    assert f"returned {R.pct(s['net_return'])} net" in text
    mach = R.machine_result(dev, setup[0])
    assert mach["headline"]["development"]["net_return"] == s["net_return"] and mach["result_hash"] == dev["result_hash"]
    assert mach["dataset_hash"] == dev["dataset"]["dataset_hash"] and mach["run_id"] == dev["run_id"]
    assert mach["schema"] == R.REPORT_SCHEMA and mach["content_hash"] == R.machine_result(dev, setup[0])["content_hash"]
    by = mach["headline"]["development"]["by_risk_level"]
    assert by["EXECUTABLE"]["balanced"]["risk_per_trade_effective"] == 0.004 and by["MANDATE"]["balanced"]["risk_per_trade_effective"] == 0.005


def test_a_development_report_shows_no_held_back_figure_and_blocks_instead_of_passing(dev_setup):
    setup, dev = dev_setup
    text = render(setup, dev)
    assert dev["verdict"] == "PENDING_HOLDOUT"
    assert "READY FOR HOLDOUT / BLOCKED ON AUTHORIZATION" in text and "**BLOCKED PENDING AUTHORIZATION**" in text
    perf = section(text, "Part D — Performance")
    assert "### Development period: 2020-01-01 to 2020-12-31" in perf and "**Held-back period.** NOT EVALUATED" in perf
    assert "Held-back period (fresh account)" not in text and "Full period (one continuous account)" not in text
    gate = section(text, "Part I — Statistical gate")
    assert "| Result | **BLOCKED_PENDING_APPROVAL**" in gate and "NOT APPROVED" in gate
    assert not re.search(r"\| Result \| \*\*PASS", gate)                       # a blocked gate is never printed as passed
    assert "| Held-back period | RESERVED" in text and "| Authorization | NONE RECORDED |" in text and "| Opened | NOT OPENED |" in text
    assert "no trading is authorized by this report" in text and "CATI stays in shadow" in text
    assert "CERTIFIED FOR RESEARCH PROMOTION" not in text
    mach = R.machine_result(dev, setup[0])
    assert mach["certification_status"] == {"family": "DAILY_TREND", "certified": False, "eligible_for_demo": False,
                                            "eligible_for_live": False, "cati_mode": "SHADOW_ONLY"}
    assert mach["promotion"]["rule_based_admission"]["status"] == "NOT_ELIGIBLE" and mach["holdout"]["status"] == "HOLDOUT_RESERVED"


def test_both_risk_policies_and_all_levels_are_shown_and_never_merged(dev_setup):
    setup, dev = dev_setup
    part = section(render(setup, dev), "Part E — Per-risk-level analysis")
    for policy in ("Mandate", "Executable"):
        for level in ("Conservative", "Balanced", "Aggressive"):
            assert f"| {policy} | {level} |" in part
    assert "| Executable | Balanced | 0.50% | 0.40% |" in part and "| Executable | Aggressive | 0.75% | 0.40% |" in part
    assert "| Mandate | Balanced | 0.50% | 0.50% |" in part and "not deployable until the owner decides RISK-01" in part
    k = section(render(setup, dev), "Part K — Rejection and veto accounting")
    assert "55-of-74" in k and "hypothesis 15" in k and "wider than the engine's 15% limit" in k


def test_a_failed_holdout_report_says_failed_keeps_periods_apart_and_lists_every_run(tmp_path):
    setup = make_setup(tmp_path)
    reg = setup[0]
    dev = go(setup)
    with pytest.raises(H.HoldoutNotAuthorized):
        go(setup, O.HOLDOUT)                                                    # a refused attempt: it must stay visible
    authorize(setup, dev)
    failed_id = reg.runs()[-1]["run_id"]
    res = go(setup, O.HOLDOUT, parent_run_id=failed_id, reason_for_rerun="authorization recorded")
    assert res["verdict"] == "FAILED_CERTIFICATION"
    text = render(setup, res)
    assert "**FAILED CERTIFICATION**" in text and "| Certification verdict | **FAILED CERTIFICATION** |" in text
    perf = section(text, "Part D — Performance")
    assert "### Held-back period (fresh account): 2021-01-01 to 2021-06-30" in perf
    assert "### Full period (one continuous account): 2020-01-01 to 2021-06-30" in perf
    assert "### Development period:" not in perf                               # never mixed into the held-back report
    runs = section(text, "Part L — Complete experiment register")
    assert failed_id in runs and "| FAILED |" in runs and "HoldoutNotAuthorized" in runs and dev["run_id"] in runs
    assert "authorization recorded" in runs and "| Authorization | Owner Name — test message |" in runs
    why = section(text, "Part I — Statistical gate")
    assert "Why it did not qualify" in why and "2 positive calendar years" in why
    final = section(text, "Part M — Final recommendation")
    assert "failed its own frozen pass rule" in final and "hypothesis 19" in final
    assert R.machine_result(res, reg)["recommendation"] == "FAILED CERTIFICATION"
    assert R.machine_result(res, reg)["holdout"]["status"] == "HOLDOUT_BURNED"


def test_a_passing_rule_without_approved_thresholds_is_reported_as_blocked_not_certified(tmp_path):
    reg = new_register(tmp_path / "register.jsonl")
    ds = SyntheticDataset(seed=3, drift=0.004, vol=0.012, end="2026-09-30")
    O.freeze_dataset_in_register(reg, ds)
    O.register_cost_model(reg)
    setup = (reg, ds, tmp_path / "runs")
    kw = dict(register=reg, dataset=ds, artifacts_root=tmp_path / "runs", researcher="tester", source=SRC, bootstrap_repetitions=200)
    dev = O.run_evaluation(O.DEVELOPMENT, **kw)
    authorize(setup, dev, periods=O.Periods())
    res = O.run_evaluation(O.HOLDOUT, **kw)
    assert res["mandate_pass_rule"] == "PASS" and res["verdict"] == "BLOCKED_PENDING_AUTHORIZATION"
    text = render(setup, res)
    assert "| Certification verdict | **BLOCKED PENDING AUTHORIZATION** |" in text and "| Frozen pass rule | PASS |" in text
    assert "| Statistical gate | BLOCKED_PENDING_APPROVAL |" in text and "cannot be recorded as certified" in text
    assert "CERTIFIED FOR RESEARCH PROMOTION" not in text
    mach = R.machine_result(res, reg)
    assert mach["certification_status"]["certified"] is False and mach["promotion"]["trading_authorized"] is False
    assert mach["promotion"]["rule_based_admission"]["status"] == "NOT_ELIGIBLE"   # the statistical gate did not pass
    assert all(v == "PASS" for v in mach["pass_rule_criteria"].values())


def test_write_report_reads_the_registered_run_and_refuses_a_result_that_was_edited(tmp_path):
    setup = make_setup(tmp_path)
    reg, ds, runs = setup
    dev = go(setup)
    root = tmp_path / "repo"
    (root / "docs" / "research" / "datasets").mkdir(parents=True)
    (root / "docs" / "research" / "datasets" / f"synthetic_{ds.manifest['dataset_version']}.dataset.json").write_text(
        json.dumps(ds.manifest), encoding="utf-8")
    path = R.write_report(register=reg, repo_root=root)
    text = path.read_text(encoding="utf-8")
    assert path.name == "CERTIFICATION_REPORT.md" and dev["result_hash"] in text and "\r" not in path.read_bytes().decode("utf-8")
    mach = json.loads((path.parent / "certification_result.json").read_text(encoding="utf-8"))
    assert mach["run_id"] == dev["run_id"] and mach["verdict"] == dev["verdict"]
    stored = runs / dev["run_id"] / "result.json"
    data = json.loads(stored.read_text(encoding="utf-8"))
    data["result_hash"] = "0" * 64
    stored.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match="does not match the research register"):
        R.write_report(register=reg, repo_root=root)


def test_undefined_figures_print_as_not_available():
    assert R.pct(None) == "n/a" and R.num(None) == "n/a" and R.pct(0.12345) == "12.35%" and R.num(1234.5) == "1,234.50"
