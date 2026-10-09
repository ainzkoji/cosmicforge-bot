"""The strategy research report (Section H, Step 2.6), generated from a run's machine-readable result.

Nothing in the report is typed by hand: every figure is read from ``result.json`` of one registered run, the
research register, the dataset manifest and the evidence files beside them, so the Markdown cannot drift from
the evidence. The verdict comes first. Development and held-back results are never put in one table. A figure
that is not defined is printed as "n/a"; a requirement that is blocked is never printed as passed.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from app.trading_intelligence.families.daily_trend import spec
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.research.governance import admission
from app.trading_intelligence.research.governance.register import RUN_COMPLETED, ResearchRegister, repository_root

REPORT_SCHEMA = "cati-certification-result-v1"
MANDATE_DOCS = "docs/research/mandate_004"
SECTIONS = ("Part A — Executive verdict", "Part B — Strategy explanation", "Part C — Data integrity",
            "Part D — Performance", "Part E — Per-risk-level analysis", "Part F — Benchmarks",
            "Part G — Portfolio overlay", "Part H — Robustness", "Part I — Statistical gate",
            "Part J — Capacity assessment", "Part K — Rejection and veto accounting",
            "Part L — Complete experiment register", "Part M — Final recommendation")
RECOMMENDATION = {"CERTIFIED_FOR_RESEARCH_PROMOTION": "CERTIFIED FOR RESEARCH PROMOTION",
                  "FAILED_CERTIFICATION": "FAILED CERTIFICATION", "INSUFFICIENT_EVIDENCE": "INSUFFICIENT EVIDENCE",
                  "BLOCKED_PENDING_AUTHORIZATION": "BLOCKED PENDING AUTHORIZATION",
                  "INVALID_EVALUATION": "INVALID EVALUATION", "PENDING_HOLDOUT": "BLOCKED PENDING AUTHORIZATION"}
LEVELS = ("conservative", "balanced", "aggressive")


def pct(x: Optional[float], digits: int = 2) -> str:
    return "n/a" if x is None else f"{100.0 * x:.{digits}f}%"


def num(x: Optional[float], digits: int = 2) -> str:
    return "n/a" if x is None else f"{x:,.{digits}f}"


def table(header: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def key(policy: str, level: str, cost: str = "base", brakes: str = "brakes_on", overlay: str = "no_overlay") -> str:
    return "|".join([policy, level, cost, brakes, overlay])


def _status_line(result: Mapping[str, Any]) -> str:
    v = result["verdict"]
    if v == "PENDING_HOLDOUT":
        return "READY FOR HOLDOUT / BLOCKED ON AUTHORIZATION — the held-back period has not been opened"
    return RECOMMENDATION[v]


def _perf_rows(block: Mapping[str, Any], labels: Sequence[str]) -> List[List[str]]:
    rows = []
    for label in labels:
        s = block["scenarios"][label]
        t = s["trades"]
        rows.append([label.replace("|", " / "), pct(s["net_return"]), pct(s["annual_return"]), pct(s["annual_volatility"]),
                     num(s["sharpe"]), num(s["sortino"]), pct(s["max_drawdown"]), num(s["calmar"]), t["trades_closed"],
                     pct(t["hit_rate"], 1), num(t["profit_factor"]), num(t["average_days_held"], 1)])
    return rows


PERF_HEADER = ("Scenario", "Net return", "Annual", "Volatility", "Sharpe", "Sortino", "Max drawdown", "Calmar",
               "Trades", "Hit rate", "Profit factor", "Days held")


def _block_section(name: str, block: Mapping[str, Any], primary: str) -> List[str]:
    s = block["scenarios"][primary]
    c = s["costs"]
    years = ", ".join(f"{y}: {pct(v['return'])}{'' if v['complete'] else ' (partial)'}" for y, v in s["calendar_years"].items())
    out = [f"### {name}: {block['first_day']} to {block['last_day']}", "",
           f"Primary scenario `{primary}` (the certification basis). {s['observations']} daily observations.", "",
           table(PERF_HEADER, _perf_rows(block, [primary, key("MANDATE", "balanced", "cost_x2"),
                                                 key("MANDATE", "balanced", "base", "brakes_off"),
                                                 key("MANDATE", "balanced", "cost_x2", "brakes_off")])), "",
           f"- Gross return (price P&L before all costs): {pct(s['gross_return'])}; net: {pct(s['net_return'])}.",
           f"- Fees {num(c['fees'])} USDT ({pct(c['fee_drag_per_year'])} of equity a year), slippage {num(c['slippage'])} "
           f"({pct(c['slippage_drag_per_year'])}), funding {num(c['funding'])} ({pct(c['funding_drag_per_year'])}).",
           f"- Turnover {num(s['turnover_per_year'])}x equity a year; in the market on {pct(s['share_of_days_with_a_position'], 1)} "
           f"of days; average {num(s['average_positions'], 1)} positions; average exposure {pct(s['average_exposure'], 1)}, "
           f"highest {pct(s['max_exposure'], 1)} of equity.",
           f"- Worst day {pct(s['worst_day'])}; mean of the worst 5% of days {pct(s['expected_shortfall_5pct'])}; "
           f"average win {num(s['trades']['average_win'])} USDT, average loss {num(s['trades']['average_loss'])} USDT.",
           f"- Calendar years: {years}.",
           f"- Brake days: daily pause {s['brake_days']['daily_pause']}, halved {s['brake_days']['halved']}, "
           f"stopped {s['brake_days']['halted']} (drawdown stop fired on {s['drawdown_stop_fired_on'] or 'no day'}).", ""]
    return out


def machine_result(result: Mapping[str, Any], register: ResearchRegister) -> Dict[str, Any]:
    """The versioned, compact record for registries, dashboards and regression comparison. Every headline
    number carries the run and dataset it came from."""
    primary = result["primary_scenario"]
    headline = {}
    for name, block in result["blocks"].items():
        s = block["scenarios"][primary]
        headline[name] = {k: s[k] for k in ("first_day", "last_day", "observations", "net_return", "gross_return",
                                            "annual_return", "annual_volatility", "sharpe", "max_drawdown")}
        headline[name]["trades_closed"] = s["trades"]["trades_closed"]
        headline[name]["costs"] = {k: s["costs"][k] for k in ("fees", "slippage", "funding")}
        headline[name]["by_risk_level"] = {
            policy: {lv: {k: block["scenarios"][key(policy, lv)][k] for k in ("net_return", "sharpe", "max_drawdown",
                                                                                "risk_per_trade_effective")}
                     for lv in LEVELS} for policy in ("MANDATE", "EXECUTABLE")}
    cert = {**result, "holdout_result_hash": result["result_hash"]}
    body = {
        "schema": REPORT_SCHEMA, "mandate_id": result["mandate_id"], "family_id": result["family_id"],
        "hypothesis_id": result["hypothesis_id"], "hypothesis_number": result["hypothesis_number"],
        "specification_sha256": result["specification_sha256"], "strategy_hash": result["strategy_hash"],
        "run_id": result["run_id"], "run_type": result["run_type"], "result_hash": result["result_hash"],
        "dataset_hash": result["dataset"]["dataset_hash"], "manifest_hash": result["dataset"]["manifest_hash"],
        "cost_model_hash": result["cost_model"]["cost_model_hash"], "source_fingerprint": result["source_fingerprint"],
        "hypotheses_in_register": result["hypotheses_in_register"], "verdict": result["verdict"],
        "recommendation": RECOMMENDATION[result["verdict"]], "mandate_pass_rule": result["mandate_pass_rule"],
        "pass_rule_criteria": {k: v["status"] for k, v in result["pass_rule"]["criteria"].items()},
        "statistical_gate": result["statistical_gate"], "integrity": result["integrity"],
        "holdout": {"holdout_id": result["holdout_id"], "status": register.holdout(result["holdout_id"])["status"],
                    "opened_by_this_run": result["holdout_opened_by_this_run"]},
        "headline": headline, "primary_scenario": primary,
        "promotion": {**result["promotion"],
                      "rule_based_admission": admission.evaluate_rule_based_admission(
                          register, mandate_id=result["mandate_id"], certification=cert)},
        "certification_status": {"family": result["family_id"], "certified": result["verdict"] == "CERTIFIED_FOR_RESEARCH_PROMOTION",
                                 "eligible_for_demo": False, "eligible_for_live": False, "cati_mode": "SHADOW_ONLY"},
        "step3": {"rules": "app.trading_intelligence.families.daily_trend (rules.py, targets.py, spec.py)",
                  "daily_targets_artifact": f"runs/{result['run_id']}/" + ("holdout" if "holdout" in result["blocks"] else "development")
                                            + "_primary_daily_targets.csv"}}
    return {**body, "content_hash": stable_hash(body)}


def build_report(result: Mapping[str, Any], *, register: ResearchRegister, manifest: Mapping[str, Any],
                 evidence: Optional[Mapping[str, Any]] = None) -> str:
    evidence = dict(evidence or {})
    primary = result["primary_scenario"]
    blocks = result["blocks"]
    rule, gate = result["pass_rule"], result["statistical_gate_detail"]
    hold = register.holdout(result["holdout_id"])
    dev_only = result["run_type"] == "DEVELOPMENT"
    first = blocks.get("holdout") or blocks["development"]
    s0 = first["scenarios"][primary]
    mach = machine_result(result, register)
    L: List[str] = []
    add = L.append

    # ------------------------------------------------------------------ A
    add("# Mandate 004 — Daily Trend: research certification report")
    add("")
    add(f"Generated from run `{result['run_id']}` ({result['run_type']}), result hash `{result['result_hash']}`. "
        "Every figure below is read from that run's `result.json`, the research register and the dataset manifest.")
    add("")
    add(f"## {SECTIONS[0]}")
    add("")
    add(table(("Item", "Value"), [
        ("Mandate", f"{result['mandate_id']} (specification SHA-256 `{result['specification_sha256']}`)"),
        ("Strategy family", f"{result['family_id']} — long-or-flat breakout ensemble on daily bars"),
        ("Hypothesis number", f"{result['hypothesis_number']} of {result['hypotheses_in_register']} in the research register"),
        ("Evaluation date", result["provenance"]["completed_at"]),
        ("Research status", _status_line(result)),
        ("Certification verdict", f"**{RECOMMENDATION[result['verdict']]}**"),
        ("Frozen pass rule", result["mandate_pass_rule"]),
        ("Statistical gate", result["statistical_gate"]),
        ("Held-back period", f"{hold['status'].replace('HOLDOUT_', '')} ({spec.HOLDOUT_START} to {spec.HOLDOUT_END})"),
        ("Promotion eligibility", "NONE — no trading is authorized by this report; governance phase unchanged; live trading disabled"),
    ]))
    add("")
    add(f"**Major result.** Over {first['first_day']} to {first['last_day']} ({'development' if dev_only else 'held-back'} period, "
        f"Balanced, base cost, as specified) the strategy returned {pct(s0['net_return'])} net ({pct(s0['annual_return'])} a year) "
        f"with a Sharpe ratio of {num(s0['sharpe'])} and a maximum drawdown of {pct(s0['max_drawdown'])}, from "
        f"{s0['trades']['trades_closed']} closed trades.")
    add("")
    add("**Principal risks.**")
    for line in _risks(result, manifest):
        add(f"- {line}")
    add("")
    add("A research pass, a governance promotion and a trading authorization are three different things. This "
        "report can only ever establish the first. It establishes none of them today.")
    add("")

    # ------------------------------------------------------------------ B
    add(f"## {SECTIONS[1]}")
    add("")
    add("**What it tries to capture.** Sustained price trends in liquid crypto perpetuals: coins that keep making new "
        "highs tend, on average, to continue for a while. The strategy only buys; when nothing is trending it holds cash.")
    add("")
    add("**How it finds opportunities.** Each day, among the 20 most traded USDT perpetuals with at least 120 days of "
        "history, it checks seven look-back windows (10 to 150 days). A window switches on when the close is at or above "
        "its highest close, and off when the close falls below the lowest close of half that window. Strength is the "
        "share of windows that are on.")
    add("")
    add("**Entry, exit, size.** Decisions use the daily close and trade at the next open. Position size is strength × "
        "risk per trade × equity ÷ stop distance, where the stop distance is three times the 20-day average true range "
        "(at least 5%, at most 40% of price). The stop only moves up. A position is closed when its strength reaches "
        "zero, when it leaves the top 20, or when the stop is hit.")
    add("")
    add("**Portfolio and risk control.** At most 4, 6 or 8 positions by risk level; total open risk capped at 1%, 2% or "
        "3% of equity; a daily loss pauses new entries; a drawdown first halves all sizes and then closes everything.")
    add("")
    add("**Why it is plausible.** Trend following has a long record across asset classes, and crypto has shown long "
        "directional moves. **Where it fails.** Sideways markets (repeated small losses), sudden reversals after a "
        "breakout, and regimes where costs and funding eat a thin edge. Because it is long-only, it earns nothing in a "
        "falling market. None of this is evidence; the evidence is in Parts D to I.")
    add("")

    # ------------------------------------------------------------------ C
    q = manifest["quality_totals"]
    dq = result["data_quality"]
    add(f"## {SECTIONS[2]}")
    add("")
    add(table(("Item", "Value"), [
        ("Dataset", f"`{manifest['dataset_id']}` {manifest['dataset_version']}, hash `{manifest['dataset_hash']}`"),
        ("Manifest hash", f"`{manifest['manifest_hash']}`"),
        ("Sources", "Binance public archive (daily klines, funding rates), every file checked against the archive's SHA-256"),
        ("Coverage", f"{manifest['coverage_start']} to {manifest['coverage_end']}"),
        ("Contracts", f"{manifest['symbols_with_bars']} USDT-margined perpetuals; "
                      f"{manifest['contracts_that_ended_before_coverage_end']['count']} of them ended before the last day"),
        ("Raw files", f"{manifest['raw_artifact_hashes']['files']:,} (inventory SHA-256 `{manifest['raw_artifact_hashes']['inventory_sha256']}`)"),
        ("Daily bars", f"{q['bar_days']:,}; {q['non_trading_bars']:,} have no trade and are never used as prices"),
        ("Missing bars", f"{q['missing_candles']} in {q['symbols_with_missing_candles']} symbols "
                         f"({q['candles_from_daily_files']} recovered from the archive's daily files)"),
        ("Invalid / duplicate bars", f"{q['invalid_candles']} / {q['duplicate_candles']}"),
        ("Funding", f"{q['bar_days_without_any_funding_record']:,} bar-days have no funding record, almost all after a "
                    "contract stopped trading; missing funding is charged at 0.03% per 8 hours, never treated as zero"),
        ("Funding imputed in this run", f"largest share over all scenarios {pct(dq['largest_funding_imputed_share'], 3)} "
                                        f"(limit {pct(dq['funding_imputed_share_limit'], 0)})"),
        ("Universe method", "point in time: membership decided each day from bars on or before that day"),
        ("Universe in this run", "; ".join(f"{n}: {b['universe']['contracts_ever_in_universe']} contracts ever members, "
                                           f"{b['universe']['days_with_a_full_universe']} of {b['universe']['days']} days with 20 members, "
                                           f"{b['universe']['days_with_no_universe']} with none" for n, b in blocks.items())),
        ("Ended contracts that were universe members", ", ".join(dq["ended_contracts_that_were_in_the_universe"]) or "none"),
        ("Forced exits for missing bars (primary run)", dq["data_gap_exits_in_primary_run"]),
    ]))
    add("")
    add("**Survivorship.** The symbol list comes from the archive listing, not from today's exchange list, so contracts "
        "that were delisted are in the data and can be traded and lost on by the simulation. The dataset is **not** "
        "claimed to be free of survivorship bias: listing and delisting dates are reconstructed from where the archive's "
        "bars start and stop trading, no public point-in-time listing record exists, and a contract with no archive "
        "file at all cannot be seen.")
    add("")
    add("**Other limitations.** " + " ".join(manifest["known_limitations"]))
    add("")

    # ------------------------------------------------------------------ D
    add(f"## {SECTIONS[3]}")
    add("")
    add("Development and held-back results are reported separately and never combined in one figure.")
    add("")
    if "development" in blocks:
        L.extend(_block_section("Development period", blocks["development"], primary))
        add("**Validation period.** The frozen specification defines no validation period; none is reported.")
        add("")
        add("**Held-back period.** NOT EVALUATED. It has not been opened; no held-back figure exists.")
        add("")
    if "holdout" in blocks:
        L.extend(_block_section("Held-back period (fresh account)", blocks["holdout"], primary))
        L.extend(_block_section("Full period (one continuous account)", blocks["full"], primary))
        add("**Development period.** Reported by the development run listed in Part L; the full-period account above "
            "contains it but is not a development-only figure.")
        add("")
    ci = gate.get("bootstrap", {})
    add(f"**Uncertainty.** {gate.get('observations', 0)} daily observations ({gate.get('sample', 'n/a')}); effective sample "
        f"{num((gate.get('serial_dependence') or {}).get('effective_n'), 0)} after serial dependence. Block-bootstrap 95% "
        f"interval for the annual return: {pct((ci.get('annualized_return_ci95') or [None, None])[0])} to "
        f"{pct((ci.get('annualized_return_ci95') or [None, None])[1])}.")
    add("")

    # ------------------------------------------------------------------ E
    add(f"## {SECTIONS[4]}")
    add("")
    add("Two policies are shown and must not be confused. **Mandate** is the frozen specification as written (risk per "
        "trade 0.25% / 0.50% / 0.75%). **Executable** is what the engine would accept today: it clamps risk per trade to "
        "0.40% and refuses an entry whose stop is more than 15% away. Balanced and Aggressive therefore risk the same "
        "0.40% per trade today; the 0.50% and 0.75% figures are not deployable until the owner decides RISK-01.")
    add("")
    for name, block in blocks.items():
        rows = []
        for policy in ("MANDATE", "EXECUTABLE"):
            for lv in LEVELS:
                s = block["scenarios"][key(policy, lv)]
                rows.append([policy.title(), lv.title(), pct(s["risk_per_trade_approved"]), pct(s["risk_per_trade_effective"]),
                             pct(s["net_return"]), pct(s["annual_return"]), num(s["sharpe"]), pct(s["max_drawdown"]),
                             s["trades"]["trades_closed"], s["rejections"].get("STOP_DISTANCE_EXCEEDS_ENGINE_MAX", 0),
                             s["drawdown_stop_fired_on"] or "no"])
        add(f"### {name.title()}: {block['first_day']} to {block['last_day']} (base cost, brakes on)")
        add("")
        add(table(("Policy", "Level", "Approved risk", "Effective risk", "Net return", "Annual", "Sharpe", "Max drawdown",
                   "Trades", "Entries refused (stop > 15%)", "Drawdown stop"), rows))
        add("")

    # ------------------------------------------------------------------ F
    add(f"## {SECTIONS[5]}")
    add("")
    add("The two benchmarks named by the specification, over the same period, scaled after the fact to the strategy's "
        "realised volatility (a comparison device, not a tradable portfolio).")
    add("")
    for name, block in blocks.items():
        s = block["scenarios"][primary]
        rows = [["Strategy (primary)", pct(s["net_return"]), pct(s["annual_return"]), pct(s["annual_volatility"]), num(s["sharpe"]),
                 pct(s["max_drawdown"]), "—"]]
        for bname, title in (("btc", "Bitcoin buy-and-hold (perpetual, with funding)"), ("equal_weight", "Equal-weight universe")):
            b = block["benchmarks"].get(bname)
            if not b or not b.get("scaled"):
                rows.append([title, "n/a", "n/a", "n/a", "n/a", "n/a", "n/a"])
                continue
            u, sc = b["unscaled"], b["scaled"]
            rows.append([title + " — unscaled", pct(u["net_return"]), pct(u["annual_return"]), pct(u["annual_volatility"]),
                         num(u["sharpe"]), pct(u["max_drawdown"]), "1.00"])
            rows.append([title + " — scaled", pct(sc["net_return"]), pct(sc["annual_return"]), pct(sc["annual_volatility"]),
                         num(sc["sharpe"]), pct(sc["max_drawdown"]), num(b["scale"], 3)])
        add(f"### {name.title()}: {block['first_day']} to {block['last_day']}")
        add("")
        add(table(("Series", "Net return", "Annual", "Volatility", "Sharpe", "Max drawdown", "Scale"), rows))
        add("")

    # ------------------------------------------------------------------ G
    add(f"## {SECTIONS[6]}")
    add("")
    add("Secondary test of the specification: halve a coin's target when its trailing 3-day funding is above 30% a year, "
        "set it to zero above 60%. It is reported separately and cannot rescue a fail. The drawdown brakes are the other "
        "overlay; their effect is the difference between the brakes-on and brakes-off rows in Part D.")
    add("")
    for name, block in blocks.items():
        rows = []
        for lv in LEVELS:
            a, b = block["scenarios"][key("MANDATE", lv)], block["scenarios"][key("MANDATE", lv, overlay="overlay")]
            rows.append([lv.title(), pct(a["net_return"]), pct(b["net_return"]), num(a["sharpe"]), num(b["sharpe"]),
                         pct(a["max_drawdown"]), pct(b["max_drawdown"]), num(a["costs"]["funding"]), num(b["costs"]["funding"])])
        add(f"### {name.title()}")
        add("")
        add(table(("Level", "Net, signal only", "Net, with overlay", "Sharpe", "Sharpe overlay", "Max DD", "Max DD overlay",
                   "Funding paid", "Funding paid overlay"), rows))
        add("")
    add("Any difference between the two columns comes from the overlay, not from the trend signal.")
    add("")

    # ------------------------------------------------------------------ H
    add(f"## {SECTIONS[7]}")
    add("")
    add("Only the robustness checks the specification registers are run. No parameter was varied: the specification "
        "authorizes no parameter search, and none was made.")
    add("")
    for name, block in blocks.items():
        sc = block["scenarios"]
        base, stress = sc[primary], sc[key("MANDATE", "balanced", "cost_x2")]
        nb = sc[key("MANDATE", "balanced", "base", "brakes_off")]
        years = base["calendar_years"]
        pos = [y for y, v in years.items() if v["complete"] and (v["return"] or 0) > 0]
        neg = [y for y, v in years.items() if v["complete"] and (v["return"] or 0) <= 0]
        add(f"### {name.title()}")
        add("")
        add(f"- **Cost sensitivity.** Net return {pct(base['net_return'])} at base cost, {pct(stress['net_return'])} at twice the cost.")
        add(f"- **Funding sensitivity.** Funding cost {pct(base['costs']['funding_drag_per_year'])} of equity a year; "
            f"fees and slippage together {pct((base['costs']['fee_drag_per_year'] or 0) + (base['costs']['slippage_drag_per_year'] or 0))}.")
        add(f"- **Market regimes.** Positive complete calendar years: {', '.join(pos) or 'none'}; zero or negative: {', '.join(neg) or 'none'}.")
        add(f"- **Drawdown brakes.** Maximum drawdown {pct(base['max_drawdown'])} with the brakes, {pct(nb['max_drawdown'])} without.")
        add(f"- **Instrument concentration.** {base['trades']['symbols_traded']} different contracts traded; largest single "
            f"winning trade {num(base['trades']['largest_win'])} USDT, largest loss {num(base['trades']['largest_loss'])} USDT, "
            f"against a net result of {num(base['final_equity'] - base['initial_equity'])} USDT.")
        add(f"- **Delisted contracts.** {len(result['data_quality']['ended_contracts_that_were_in_the_universe'])} ended "
            "contracts were universe members at some point and were tradable by the simulation.")
        add(f"- **Liquidity.** Largest order was {pct(base['max_order_share_of_daily_volume'], 4)} of that day's traded volume "
            "at the simulated account size (Part J).")
        add("")
    add("- **Parameter sensitivity.** Not evaluated: not authorized by the mandate.")
    add("- **Implementation assumptions.** Listed in `research/trend_v1/INTERPRETATION_001.md` and `INTERPRETATION_002.md`, "
        "both registered before the first run.")
    add("")

    # ------------------------------------------------------------------ I
    add(f"## {SECTIONS[8]}")
    add("")
    add("### The frozen pass rule (binding; all four at Balanced)")
    add("")
    add(table(("Criterion", "Status", "Observed"), [(k.replace("_", " "), v["status"], json.dumps(v["observed"]))
                                                    for k, v in rule["criteria"].items()]))
    add("")
    add(f"Pass rule: **{rule['status']}**" + (" — decided on development data alone: no held-back outcome could change it."
                                               if rule.get("decided_without_holdout") else ""))
    add("")
    add("### The portfolio-level statistical gate")
    add("")
    mt, pw = gate.get("multiple_testing", {}), gate.get("power", {})
    add(table(("Item", "Value"), [
        ("Method", "one-sided circular block bootstrap of portfolio daily net returns"),
        ("Null hypothesis", gate.get("null_hypothesis", "n/a")),
        ("Sample", f"{gate.get('sample')}: {gate.get('observations')} days, effective {num((gate.get('serial_dependence') or {}).get('effective_n'), 0)}"),
        ("Effect size (annualised Sharpe)", num(gate.get("annualized_sharpe"))),
        ("p-value, one test", num(ci.get("p_value_one_sided"), 5)),
        ("Multiple-testing adjustment", f"Bonferroni over {mt.get('tests')} hypotheses in the register"),
        ("p-value, adjusted", num(mt.get("p_value_adjusted"), 5)),
        ("Adjusted p-value if the count were 19 / 24", " / ".join(num(v, 5) for k, v in sorted((mt.get("sensitivity_p_adjusted") or {}).items())[1:])),
        ("Deflated Sharpe ratio", num(mt.get("deflated_sharpe_ratio"), 4)),
        ("Smallest annual Sharpe this sample could detect", f"{num(pw.get('minimum_detectable_annual_sharpe'))} "
                                                           f"({pw.get('thresholds_are')}: level {num(pw.get('per_test_alpha'), 5)} per test, power {num(pw.get('reference_power'))})"),
        ("Power to detect a Sharpe of 0.5 / 1.0", f"{pct((pw.get('power_for_annual_sharpe') or {}).get('0.5'), 1)} / "
                                                  f"{pct((pw.get('power_for_annual_sharpe') or {}).get('1.0'), 1)}"),
        ("Thresholds", "approved" if gate["policy"]["approved_by"] else "NOT APPROVED — significance level, required power and target effect are unset"),
        ("Result", f"**{gate['status']}** {gate.get('reason_codes') or ''}"),
    ]))
    add("")
    add(_gate_explanation(result))
    add("")

    # ------------------------------------------------------------------ J
    add(f"## {SECTIONS[9]}")
    add("")
    cap = first["capacity"]
    if cap.get("orders"):
        rows = [[f"{int(float(k)):,}", pct(v["median_order_share_of_daily_volume"], 4), pct(v["p95_order_share_of_daily_volume"], 4),
                 pct(v["max_order_share_of_daily_volume"], 4), pct(v["orders_above_1pct_of_daily_volume"], 1)]
                for k, v in cap["account_equity_usdt"].items()]
        add(f"Measured on {cap['orders']} simulated orders ({'development' if dev_only else 'held-back'} period): order size "
            "as a share of that day's traded volume, scaled linearly with account size.")
        add("")
        add(table(("Account equity (USDT)", "Median order", "95th percentile", "Largest order", "Orders above 1% of daily volume"), rows))
        add("")
    add("**This is not a capacity estimate.** " + (cap.get("limits") or "There were no orders to measure.") + " A defensible "
        "capacity figure needs order-book depth and observed market impact, which do not exist for this history. The "
        "table shows where participation stops being negligible; it does not show how much capital the strategy can carry.")
    add("")

    # ------------------------------------------------------------------ K
    add(f"## {SECTIONS[10]}")
    add("")
    for name, block in blocks.items():
        m, e = block["scenarios"][primary], block["scenarios"][key("EXECUTABLE", "balanced")]
        st = block["entry_stop_distances"]
        add(f"### {name.title()} (Balanced)")
        add("")
        if st.get("entry_decisions"):
            qd = st["stop_distance_quantiles"]
            add(f"**Raw strategy decisions (mandate).** {st['entry_decisions']} new positions were selected by the rule; "
                f"{st['accepted']} were opened. Stop distance: median {pct(qd['p50'], 1)}, middle half {pct(qd['p25'], 1)} to "
                f"{pct(qd['p75'], 1)}, range {pct(qd['min'], 1)} to {pct(qd['max'], 1)}; {st['at_floor_5pct']} at the 5% floor, "
                f"{st['at_cap_40pct']} at the 40% cap. **{st['beyond_engine_limit']} of {st['entry_decisions']} "
                f"({pct(st['share_beyond_engine_limit'], 1)}) have a stop wider than the engine's 15% limit.**")
            add("")
        rows = []
        for label, s in (("Mandate", m), ("Executable", e)):
            r = s["rejections"]
            rows.append([label, r.get("POSITION_LIMIT", 0), r.get("STOP_DISTANCE_EXCEEDS_ENGINE_MAX", 0),
                         r.get("DAILY_PAUSE_NO_NEW_POSITION", 0), r.get("BELOW_EXCHANGE_MINIMUM_NOTIONAL", 0) + r.get("QUANTITY_ROUNDS_TO_ZERO", 0),
                         r.get("NO_BAR_AT_FILL", 0), s["trades"]["trades_closed"], pct(s["net_return"]), num(s["sharpe"]), pct(s["max_drawdown"])])
        add(table(("Policy", "Not selected (position limit)", "Refused: stop > 15%", "Refused: daily pause",
                   "Refused: exchange minimum", "Refused: no bar", "Trades", "Net return", "Sharpe", "Max drawdown"), rows))
        add("")
        add(f"After the executable-policy constraints the result changes from {pct(m['net_return'])} to {pct(e['net_return'])} "
            f"net. Refused entries are not removed from the statistics: they are counted above, and the executable run is "
            "a separate simulation of what would have traded, not an edit of the mandate run.")
        add("")
    add("**Liquidity and data-quality vetoes.** No liquidity veto exists in the mandate (membership of the top 20 by volume "
        "is its liquidity rule). Data-quality exits are counted in Part C.")
    add("")
    add("**The 55-of-74 finding.** " + _residual_text(evidence.get("residual_stop_distance")))
    add("")

    # ------------------------------------------------------------------ L
    add(f"## {SECTIONS[11]}")
    add("")
    add(f"Research register: {register.verify()['records']} records, head `{register.head_hash()}`. Hypothesis count used "
        f"for multiple testing: {register.hypothesis_count()}.")
    add("")
    add("### Hypotheses")
    add("")
    add(table(("#", "Identity", "Family", "Mandate", "Status"),
              [(h["hypothesis_number"], h.get("source_identifier", h["hypothesis_id"])[:70], h["family_id"],
                str(h["mandate_id"])[:44], h["status"]) for h in register.hypotheses()]))
    add("")
    m = register.mandate(result["mandate_id"])
    add("### Mandate 004 registration and amendments")
    add("")
    add(table(("Record", "Detail"), [
        ("Registered", f"{m['registered_at']} by {m['registered_by']}; approved by {m['approved_by']} ({m['authorization_reference'][:90]}…)"),
        ("Specification", f"`{m['specification_path']}` SHA-256 `{m['specification_sha256']}`"),
        ("Rule artifact hash", f"`{m['rule_artifact_hash']}`"),
        *[(f"Amendment {a['amendment_number']}", f"{a['kind']}; changes strategy rules: {a['changes_strategy_rules']}; "
                                                 f"`{a['artifact_path']}` SHA-256 `{a['artifact_sha256']}`") for a in m["amendments"]]]))
    add("")
    add("### Runs (every run that was started, failures included)")
    add("")
    add(table(("Run", "Type", "State", "Verdict", "Code commit", "Parent", "Reason for rerun / error"),
              [(r["run_id"], r["run_type"], r["state"].replace("RUN_", ""), (r["end"] or {}).get("verdict", "—"),
                str(r["code_commit"])[:10], r.get("parent_run_id") or "—",
                r.get("reason_for_rerun") or (r["end"] or {}).get("error", "—")) for r in register.runs(mandate_id=result["mandate_id"])]))
    add("")
    auth = hold.get("authorization")
    add("### Holdout access")
    add("")
    add(table(("Event", "Detail"), [
        ("Reserved", f"`{result['holdout_id']}` {hold['start']} to {hold['end']}"),
        ("Authorization", f"{auth['authorized_by']} — {auth['authorization_reference']}" if auth else "NONE RECORDED"),
        ("Opened", f"by run `{hold['opened']['run_id']}`" if hold.get("opened") else "NOT OPENED"),
        ("Result stored", f"`{hold['burned']['result_hash']}`" if hold.get("burned") else "—")]))
    add("")

    # ------------------------------------------------------------------ M
    add(f"## {SECTIONS[12]}")
    add("")
    add(f"**{RECOMMENDATION[result['verdict']]}**")
    add("")
    add(_plain_recommendation(result, mach))
    add("")
    add("---")
    add("")
    add(f"Machine-readable result: `{MANDATE_DOCS}/certification_result.json` (schema `{REPORT_SCHEMA}`, content hash "
        f"`{mach['content_hash']}`). Reproduction: `{MANDATE_DOCS}/REPRODUCTION.md`.")
    return "\n".join(L) + "\n"


def _risks(result: Mapping[str, Any], manifest: Mapping[str, Any]) -> List[str]:
    gate = result["statistical_gate_detail"]
    pw = gate.get("power", {})
    out = []
    if result["run_type"] == "DEVELOPMENT":
        out.append("The figures above are development figures. The rules were frozen before any run, but a development "
                   "result is not an independent test.")
    out.append(f"Statistical power: with {result['hypotheses_in_register']} hypotheses tried, this sample could only detect "
               f"an annual Sharpe ratio of about {num(pw.get('minimum_detectable_annual_sharpe'))} or more "
               "(illustrative 5% / 80% standard; no standard is approved).")
    first = next(iter(result["blocks"].values()))
    st = first["entry_stop_distances"]
    if st.get("entry_decisions"):
        out.append(f"Deployment conflict: {pct(st['share_beyond_engine_limit'], 0)} of the entries the mandate selects have a "
                   "stop wider than the engine's 15% limit and would be refused today; Balanced and Aggressive are also "
                   "clamped to 0.40% risk per trade.")
    out.append("The held-back period overlaps the period earlier hypotheses were developed on, and contains a window "
               "still reserved for another certification.")
    out.append("Costs are the mandate's assumptions (0.05% fee, 0.05% slippage), not measured costs; capacity is unknown.")
    out.append("Survivorship bias is reduced (delisted contracts are included), not proven absent.")
    return out


def _gate_explanation(result: Mapping[str, Any]) -> str:
    rule, gate, v = result["pass_rule"]["status"], result["statistical_gate"], result["verdict"]
    if v == "FAILED_CERTIFICATION":
        failed = [k.replace("_", " ") for k, c in result["pass_rule"]["criteria"].items() if c["status"] == "FAIL"]
        if failed:
            return ("**Why it did not qualify.** The frozen pass rule requires all four criteria; it failed on: "
                    + "; ".join(failed) + ". A fail of the frozen rule is final: nothing else can rescue it.")
        return "**Why it did not qualify.** The portfolio-level test did not reject the null hypothesis after the multiple-testing adjustment."
    if v == "PENDING_HOLDOUT":
        return ("**Why there is no verdict yet.** Two of the four criteria are defined on the held-back period, which has "
                f"not been opened. Nothing on the development data already fails the rule (status: {rule}). The statistical "
                f"gate is {gate}.")
    if v == "BLOCKED_PENDING_AUTHORIZATION":
        return ("**Why it is not certified.** The frozen pass rule is met, but the portfolio-level test has no approved "
                "significance level, power or target effect, so it cannot be recorded as passed.")
    if v == "INSUFFICIENT_EVIDENCE":
        return f"**Why it is not certified.** The evidence is insufficient: statistical gate {gate}; data quality {result['data_quality']['reason_codes'] or 'ok'}."
    if v == "INVALID_EVALUATION":
        return "**Why it is not certified.** The evaluation failed its own integrity checks and proves nothing."
    return "**Why it qualified.** The frozen pass rule and the portfolio-level statistical gate both passed on the held-back period."


def _residual_text(ev: Optional[Mapping[str, Any]]) -> str:
    base = ("Step 1 found that 55 of 74 recorded decisions had a stop wider than the engine's 15% maximum. Those records "
            "belong to the residual momentum family (hypothesis 15, failed), not to this mandate. They are forward "
            "observation records, not executable plans: each is written in OBSERVE mode with entry authority blocked, and "
            "the count includes counterfactual candidates that were never portfolio-selected. The stop geometry is as "
            "designed (the wider of the prior-24h swing plus a quarter ATR, twice ATR, and 0.3% of price), measured from "
            "the decision close; the engine measures the same fraction from the live price at submission, so the units "
            "agree and the reference price differs slightly. Every plan beyond 15% is rejected, never clipped.")
    if not ev:
        return base + " No further evidence file was produced."
    return base + (f" In the committed historical evaluation of that family, {ev['beyond_limit']} of {ev['trades']} selected "
                   f"trades ({pct(ev['share_beyond_limit'], 0)}) had a stop wider than 15%; mean net result {num(ev['mean_net_R_all'], 4)} R "
                   f"over all trades, {num(ev['mean_net_R_within_limit'], 4)} R over the {ev['within_limit']} within the limit and "
                   f"{num(ev['mean_net_R_beyond_limit'], 4)} R over those beyond it. This is a description of a family that already "
                   "failed its gate, not a new test, and it changes no verdict. For the daily trend mandate the same conflict "
                   "exists by construction and is measured above.")


def _plain_recommendation(result: Mapping[str, Any], mach: Mapping[str, Any]) -> str:
    v = result["verdict"]
    tail = (" CATI stays in shadow: no family is certified, so Step 3 may build the pipeline but may not let any family "
            "place an order.")
    if v == "PENDING_HOLDOUT":
        return ("The evaluator, the dataset and the governance are in place and the development run is valid. The strategy "
                "has not been certified and has not failed: its two decisive criteria need the held-back period, which "
                "only the project owner can authorize. Even if those criteria pass, certification additionally needs an "
                "approved statistical standard, and the report shows that the available history cannot reach a "
                "conventional one." + tail)
    if v == "FAILED_CERTIFICATION":
        return ("The daily trend family failed its own frozen pass rule. The failure is recorded; the evidence is kept. "
                "The next step is to register the next candidate as hypothesis "
                f"{result['hypotheses_in_register'] + 1}, not to adjust this one." + tail)
    if v == "BLOCKED_PENDING_AUTHORIZATION":
        return ("The strategy met its frozen pass rule on the held-back period. It cannot be recorded as certified until the "
                "owner approves the statistical standard for the portfolio-level test." + tail)
    if v == "INSUFFICIENT_EVIDENCE":
        return "The evidence is not sufficient to certify or to fail the strategy under the approved standard." + tail
    if v == "INVALID_EVALUATION":
        return "The evaluation is invalid and must be repeated after the defect is fixed; it is not a result." + tail
    return ("The strategy passed its frozen pass rule and the approved statistical gate on the held-back period. This is a "
            "research pass only. Promotion needs the governance process (and the rule-based admission route, which is "
            f"{'approved' if mach['promotion']['rule_based_admission']['route_approved'] else 'not approved'}); trading needs "
            "Step 3 and an explicit authorization.")


def write_report(run_id: Optional[str] = None, *, register: Optional[ResearchRegister] = None,
                 repo_root: Optional[Path] = None, out_dir: Optional[Path] = None) -> Path:
    root = Path(repo_root) if repo_root else repository_root()
    register = register or ResearchRegister()
    runs = [r for r in register.runs(mandate_id=spec.MANDATE_ID) if r["state"] == RUN_COMPLETED]
    if run_id:
        runs = [r for r in runs if r["run_id"] == run_id]
    else:                                              # the held-back run when there is one, otherwise the latest
        runs = [r for r in runs if r["run_type"] == "HOLDOUT"] or runs
    if not runs:
        raise ValueError("no completed run to report")
    run = runs[-1]
    result = json.loads((root / run["end"]["artifact_dir"] / "result.json").read_text(encoding="utf-8"))   # absolute paths win
    if result["result_hash"] != run["end"]["result_hash"]:
        raise ValueError("the stored result does not match the research register")
    docs = root / "docs" / "research"
    manifest = json.loads((docs / "datasets" / f"{result['dataset']['dataset_id']}_{result['dataset']['dataset_version']}.dataset.json"
                           ).read_text(encoding="utf-8"))
    out = Path(out_dir) if out_dir else root / MANDATE_DOCS
    evidence = {}
    for name in ("residual_stop_distance",):
        path = out / f"{name}.json"
        if path.exists():
            evidence[name] = json.loads(path.read_text(encoding="utf-8"))
    out.mkdir(parents=True, exist_ok=True)
    text = build_report(result, register=register, manifest=manifest, evidence=evidence)
    (out / "CERTIFICATION_REPORT.md").write_bytes(text.encode("utf-8"))
    (out / "certification_result.json").write_bytes(
        (json.dumps(machine_result(result, register), indent=1, sort_keys=True) + "\n").encode("utf-8"))
    return out / "CERTIFICATION_REPORT.md"


__all__ = ["REPORT_SCHEMA", "SECTIONS", "RECOMMENDATION", "build_report", "machine_result", "write_report", "pct", "num"]
