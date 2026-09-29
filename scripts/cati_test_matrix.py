"""CATI Section 24 test-matrix evidence: requirement family -> test files / tests -> ACTUAL results.

Results are never typed by hand: they are read from JUnit XML produced by the real runs, e.g.

    cd backends/bot-backend && ../venv/Scripts/python.exe -m pytest tests -q --junitxml=<out>/bot.xml
    cd frontends/user-frontend && node --experimental-strip-types --test --test-reporter=junit \
        --test-reporter-destination=<out>/ui.xml src/tests/*.test.ts
    python scripts/cati_test_matrix.py <out>/bot.xml <out>/ui.xml --out docs/cati_test_matrix.json

A family whose selectors match no executed test is reported NOT_RUN (never PASS).
"""
from __future__ import annotations

import argparse
import json
import re
import xml.etree.ElementTree as ET
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Tuple

T = "tests/trading_intelligence/"
#: family -> (requirement, [(test file, test-name regex)])
FAMILIES: "OrderedDict[str, Tuple[str, List[Tuple[str, str]]]]" = OrderedDict([
    ("24.1 REGRESSION", ("existing CATI and full backend suites stay green", [("*", ".*")])),
    ("24.2 DISCOVERY", ("pagination, new / delisted symbols, refresh triggers, metadata refresh, canonical mapping, "
                        "same instrument on different venues", [
        ("tests/test_multi_asset_closure.py", "test_bybit_discovery_paginates_live_shapes"),
        ("tests/test_sections7_9_closure.py", "pagination|mass_delisting"),
        ("tests/test_section7_refresh_triggers.py", ".*"),
        ("tests/test_phase3_execution_parity.py", "test_catalog_keeps_delisted_rows"),
        (T + "test_instrument_identity.py", ".*"),
        ("tests/test_cati_section27_acceptance.py", "test_every_discovered_instrument"),
        ("tests/test_cati_section20_api.py", "test_instrument_sync"),
        (T + "test_cati_section19_closure.py", "test_instrument_identity_and_delisting_retention")])),
    ("24.3 CLASSIFICATION", ("deterministic CRYPTO / FX / TradFi / unsupported product classification", [
        ("tests/test_multi_asset_closure.py", "classified"),
        ("tests/test_phase3_execution_parity.py", "classifies"),
        ("tests/test_cati_section27_acceptance.py", "test_every_discovered_instrument"),
        (T + "test_cati_section18_closure.py", "forced_through_crypto")])),
    ("24.4 TENANCY", ("account A never affects account B: balances, capabilities, transfers, reservations, "
                      "economics, fee tier, execution, API responses", [
        ("tests/test_phase2b_permissions_transfers.py", "isolation"),
        ("tests/test_cati_section20_api.py", "owner_scoped"),
        (T + "test_portfolio.py", "isolat"),
        (T + "test_section17_venue_economics.py", "account_isolation"),
        (T + "test_cati_section16_closure.py", "scope_isolation|never_shares_account_fees"),
        (T + "test_cati_section17_closure.py", "seen_by_every_bot_on_the_account"),
        (T + "test_section21_evidence_observability.py", "never_appears_for_account_b"),
        (T + "test_cati_section18_closure.py", "another_tenant"),
        (T + "test_cati_section19_closure.py", "not_account_capability|account_scoped")])),
    ("24.5 PERMISSIONS", ("missing trade / internal-transfer permission fails closed; withdrawal never required", [
        ("tests/test_phase2b_permissions_transfers.py", "withdraw|uninspectable"),
        ("tests/test_cati_section25_security.py", "withdraw|permissions_are_distinct|least_privilege")])),
    ("24.6 UNIFIED ACCOUNT", ("no unnecessary physical transfer; logical reservation only", [
        (T + "test_cati_section16_transfer_runtime.py", "unified"),
        (T + "test_cati_section17_closure.py", "unified|full_cycle_reserves_capital"),
        ("tests/test_cati_section27_acceptance.py", "unified")])),
    ("24.7 SEGMENTED ACCOUNT", ("official route, idempotent intent, reservation first, confirmation before order", [
        (T + "test_cati_section16_transfer_runtime.py", "segmented"),
        (T + "test_capital_readiness_boundary.py", ".*"),
        ("tests/test_phase2b_permissions_transfers.py", "idempotent"),
        ("tests/test_cati_section20_api.py", "side_effect_free")])),
    ("24.8 AMBIGUOUS TRANSFER", ("UNKNOWN / RESOLUTION_PENDING, no duplicate submit, reconcile before retry", [
        ("tests/test_phase2b_permissions_transfers.py", "unknown|reconcil"),
        ("tests/test_cati_section20_api.py", "ambiguous"),
        ("tests/test_cati_section25_security.py", "storm")])),
    ("24.9 DATASET", (">=100 broad crypto, provenance, coverage, gaps, manifest determinism, young symbols, "
                      "real vs synthetic", [
        (T + "test_cati_section13_closure.py", ".*"),
        ("tests/test_sections10_12_datasets.py", ".*"),
        ("tests/test_cati_section27_acceptance.py", "at_least_100")])),
    ("24.10 FX DATA", ("reference != execution price identity, bid/ask preserved, scale QA, no reference spread as "
                       "executable spread", [
        ("tests/test_sections10_12_datasets.py", "scale|bid|fx"),
        (T + "test_cati_section14_closure.py", "reference_spread"),
        (T + "test_cati_section16_closure.py", "reference_spread"),
        (T + "test_cati_section19_closure.py", "fx_reference_and_execution"),
        ("tests/test_phase4_market_data.py", "fx")])),
    ("24.11 RISK", ("2.5% daily hard loss, slots, leverage, margin, currency / account exposure, emergency halt "
                    "remain superior", [
        (T + "test_capital_readiness_boundary.py", "hard_daily_cap|daily_risk_budget"),
        (T + "test_section20_execution_boundary.py", "daily|slot|margin|leverage|sizing"),
        (T + "test_global_state_cross_asset.py", "currency_factor_cap|shared_broker_account"),
        (T + "test_cati_section17_closure.py", "kill_switch")])),
    ("24.12 EXECUTION", ("unsupported product reason, no adapter coercion, stale metadata refresh/block, capital "
                         "readiness, unknown order reconciles, no duplicate submission", [
        (T + "test_cati_section18_closure.py", ".*"),
        (T + "test_section20_execution_boundary.py", "unknown|duplicate|reconcil|recover")])),
    ("24.13 CERTIFICATION", ("crypto / FX separation, holdouts closed, frozen manifests, threshold immutability, no "
                             "auto promotion", [
        (T + "test_cati_section22_guards.py", ".*"),
        (T + "test_section22_framework.py", ".*"),
        ("tests/test_phase6_certification_security.py", ".*")])),
    ("24.14 GLOBALMARKETSTATE", ("shadow state cannot alter TradePlan, admission, risk or execution", [
        (T + "test_cati_section14_closure.py", ".*"),
        (T + "test_global_state_cross_asset.py", "global_state|cycle_stage")])),
    ("SECTION 20 API", ("backend API surface, tenancy, reason codes, redaction", [("tests/test_cati_section20_api.py", ".*")])),
    ("SECTION 21 UI", ("UI safety view-model", [("src/tests/multiAssetView.test.ts", ".*")])),
    ("SECTION 25 SECURITY", ("credentials, redaction, withdrawal, signatures, rate limits, lineage",
                             [("tests/test_cati_section25_security.py", ".*")])),
    ("SECTION 26 OBSERVABILITY", ("events, metrics, alerts, determinism boundary",
                                  [("tests/test_cati_section26_observability.py", ".*")])),
    ("SECTION 27 ACCEPTANCE", ("definition-of-done invariants", [("tests/test_cati_section27_acceptance.py", ".*")])),
])


def _module_to_file(classname: str) -> str:
    """pytest junit classname 'tests.trading_intelligence.test_x' -> 'tests/trading_intelligence/test_x.py'."""
    return classname.replace(".", "/") + ".py"


def _in_file(case: Dict[str, str], file_sel: str) -> bool:
    """Match by file path (node / xunit1) or by module classname (pytest xunit2, incl. ``module.TestClass``)."""
    if file_sel == "*" or case["file"].endswith(file_sel):
        return True
    module = file_sel[:-3].replace("/", ".") if file_sel.endswith(".py") else None
    return bool(module) and (case.get("module", "") == module or case.get("module", "").startswith(module + "."))


def load_cases(paths: List[str]) -> List[Dict[str, str]]:
    cases = []
    for p in paths:
        root = ET.parse(p).getroot()
        for tc in root.iter("testcase"):
            cls, name, file_attr = tc.get("classname", ""), tc.get("name", ""), tc.get("file") or ""
            if tc.find("failure") is not None or tc.find("error") is not None:
                result = "FAILED"
            elif tc.find("skipped") is not None:
                result = "SKIPPED"
            else:
                result = "PASSED"
            path = file_attr.replace("\\", "/") if file_attr else _module_to_file(cls)
            cases.append({"file": path, "module": cls, "name": name, "result": result})
    return cases


def build(cases: List[Dict[str, str]]) -> Dict[str, object]:
    out: "OrderedDict[str, object]" = OrderedDict()
    for family, (requirement, selectors) in FAMILIES.items():
        rows = []
        for file_sel, name_re in selectors:
            rx = re.compile(name_re)
            hits = [c for c in cases if _in_file(c, file_sel) and rx.search(c["name"])]
            rows.append({"file": file_sel, "tests": name_re, "executed": len(hits),
                         "passed": sum(c["result"] == "PASSED" for c in hits),
                         "failed": sum(c["result"] == "FAILED" for c in hits),
                         "skipped": sum(c["result"] == "SKIPPED" for c in hits)})
        executed = sum(r["executed"] for r in rows)
        failed = sum(r["failed"] for r in rows)
        status = "NOT_RUN" if executed == 0 or any(r["executed"] == 0 for r in rows) else ("FAIL" if failed else "PASS")
        out[family] = {"requirement": requirement, "status": status, "executed": executed, "failed": failed,
                       "selectors": rows}
    return {"version": "cati-test-matrix-v1", "families": out,
            "totals": {"cases": len(cases), "passed": sum(c["result"] == "PASSED" for c in cases),
                       "failed": sum(c["result"] == "FAILED" for c in cases),
                       "skipped": sum(c["result"] == "SKIPPED" for c in cases)}}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("junit", nargs="+")
    ap.add_argument("--out", default="docs/cati_test_matrix.json")
    args = ap.parse_args()
    matrix = build(load_cases(args.junit))
    Path(args.out).write_text(json.dumps(matrix, indent=2) + "\n", encoding="utf-8")
    bad = [f for f, v in matrix["families"].items() if v["status"] != "PASS"]
    print(json.dumps({"out": args.out, "totals": matrix["totals"], "not_pass": bad}))
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
