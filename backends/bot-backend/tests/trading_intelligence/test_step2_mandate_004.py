"""Section H, Step 2.2: Mandate 004 / hypothesis 18 is registered, versioned and hash-pinned in the committed
research register, before any evaluation, from the frozen daily trend specification."""
from __future__ import annotations

from decimal import Decimal

import pytest

from app.trading_intelligence.families.daily_trend import spec
from app.trading_intelligence.families.daily_trend.registration import hypothesis_record, register_mandate_004
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.research.governance.history import HISTORICAL_ANCHOR
from app.trading_intelligence.research.governance.mandates import MandateHashMismatch, verify_mandate
from app.trading_intelligence.research.governance.register import (
    HYPOTHESIS, MANDATE_AMENDMENT, MANDATE_REGISTERED, RegisterError, ResearchRegister, canonical_text_sha256,
    repository_root,
)

REPO = repository_root()


def test_hypothesis_eighteen_follows_the_seventeen():
    reg = ResearchRegister()
    hyps = reg.hypotheses()
    reg.records(anchor=tuple(HISTORICAL_ANCHOR))
    assert [h["hypothesis_number"] for h in hyps[:18]] == list(range(1, 19))
    h18 = hyps[17]
    assert (h18["hypothesis_id"], h18["mandate_id"], h18["family_id"]) == ("H-018", "MANDATE_004", "DAILY_TREND")
    assert h18["status_history"][0]["status"] == "REGISTERED" and h18["research_engine"] == "CATI"
    assert {k: h18[k] for k in hypothesis_record()} == hypothesis_record()      # the record is re-derivable
    assert reg.hypothesis_count() >= 18


def test_the_mandate_is_pinned_to_the_frozen_specification_and_rule_artifact():
    reg = ResearchRegister()
    m = reg.mandate(spec.MANDATE_ID)
    assert m["specification_path"] == "research/trend_v1/SPEC.md" and m["specification_version"] == "trend-ensemble-v1"
    assert m["specification_sha256"] == spec.SPECIFICATION_SHA256 == canonical_text_sha256(REPO / spec.SPECIFICATION_PATH)
    assert m["rule_artifact_hash"] == stable_hash(dict(spec.RULE_ARTIFACT))
    assert m["rule_artifact"] == dict(spec.RULE_ARTIFACT)                        # the artifact itself is on record
    for field in ("research_code_commit", "registry_version", "registered_at", "registered_by", "approved_at",
                  "approved_by", "authorization_reference"):
        assert m[field] and m[field] != "UNKNOWN", field
    assert m["live_trading"] == "DISABLED" and m["demo_promotion"] == "NOT_AUTHORIZED"
    pin = verify_mandate(reg, spec.MANDATE_ID, rule_artifact=spec.RULE_ARTIFACT)
    assert pin["specification_sha256"] == spec.SPECIFICATION_SHA256 and len(pin["strategy_hash"]) == 64


def test_the_interpretation_is_amendment_one_and_changes_no_rule():
    m = ResearchRegister().mandate(spec.MANDATE_ID)
    a1 = m["amendments"][0]
    assert a1["amendment_number"] == 1 and a1["kind"] == "IMPLEMENTATION_INTERPRETATION"
    assert a1["changes_strategy_rules"] is False and a1["artifact_path"] == spec.INTERPRETATION_PATH
    assert a1["artifact_sha256"] == canonical_text_sha256(REPO / spec.INTERPRETATION_PATH)
    text = (REPO / spec.INTERPRETATION_PATH).read_text(encoding="utf-8")
    assert "before any price series was loaded" in text
    assert all(f"## I{i}." in text for i in range(1, 21))                         # every numbered point is written down


def test_registration_and_every_amendment_precede_any_run():
    reg = ResearchRegister()
    recs = reg.records()
    seq = {t: [r["seq"] for r in recs if r["type"] == t] for t in (HYPOTHESIS, MANDATE_REGISTERED, MANDATE_AMENDMENT)}
    first_run = min((r["started_seq"] for r in reg.runs(mandate_id=spec.MANDATE_ID)), default=None)
    assert seq[HYPOTHESIS][17] < seq[MANDATE_REGISTERED][0] < seq[MANDATE_AMENDMENT][0]
    if first_run is not None:
        assert seq[MANDATE_AMENDMENT][0] < first_run
        assert all(r["strategy_hash"] and r["dataset_hash"] for r in reg.runs(mandate_id=spec.MANDATE_ID))


def test_a_second_registration_or_an_edited_rule_is_refused(tmp_path):
    reg = ResearchRegister(tmp_path / "copy.jsonl")
    reg.path.write_bytes(ResearchRegister().path.read_bytes())
    with pytest.raises(RegisterError):
        register_mandate_004(reg, research_code_commit="c", registered_by="t", approved_by="o", approved_at="d",
                             authorization_reference="r")
    edited = {**dict(spec.RULE_ARTIFACT), "specification": {**spec.RULE_ARTIFACT["specification"], "atr_days": 14}}
    with pytest.raises(MandateHashMismatch, match="rule set"):
        verify_mandate(reg, spec.MANDATE_ID, rule_artifact=edited)
    with pytest.raises(RegisterError, match="not registered"):
        verify_mandate(reg, "MANDATE_005", rule_artifact=spec.RULE_ARTIFACT)


def test_the_frozen_numbers_are_the_specification_and_the_shared_risk_library():
    """The rule artifact is data copied from SPEC.md; this ties it to the text and to the platform's own
    risk-level table, so neither can move without the other being noticed."""
    from shared_lib.risk_levels import PROFILES, SYSTEM_PER_TRADE_RISK_CEILING_PCT

    text = (REPO / spec.SPECIFICATION_PATH).read_text(encoding="utf-8")
    s = spec.SPECIFICATION
    assert "{10, 20, 30, 45, 65, 100, 150}" in text and s["lookbacks"] == [10, 20, 30, 45, 65, 100, 150]
    assert "3 x ATR(20) / close, floored at 5% and capped at 40%" in text
    assert (s["atr_days"], s["stop_atr_multiple"], s["stop_floor"], s["stop_cap"]) == (20, 3.0, 0.05, 0.40)
    assert "the 20 contracts with the highest median daily quote volume over" in text and "at least 120 days" in text
    assert (s["universe_size"], s["universe_volume_days"], s["universe_min_history_days"]) == (20, 30, 120)
    assert "0.05% taker fee plus 0.05% slippage per side" in text and (s["taker_fee"], s["slippage"]) == (0.0005, 0.0005)
    assert "more than 25% of the target" in text and s["rebalance_band"] == 0.25
    assert "Development period: 2020-01-01 to 2024-12-31" in text and "Held-back period: 2025-01-01 to 2026-09-30" in text
    assert s["development"] == ["2020-01-01", "2024-12-31"] and s["holdout"] == ["2025-01-01", "2026-09-30"]
    assert "at least 4 of the 6 calendar years 2020 to 2025" in text and "stays inside 15%" in text
    assert "at least 0.3" in text
    rule = s["pass_rule"]
    assert (rule["positive_calendar_years_min"], rule["max_drawdown_without_brake"], rule["holdout_net_sharpe_min"]) == (4, 0.15, 0.3)
    for level, row in s["risk_levels"].items():
        p = PROFILES[level]
        assert Decimal(str(row["risk_per_trade"])) * 100 == p.per_trade_risk_pct
        assert Decimal(str(row["max_open_risk"])) * 100 == p.max_open_risk_pct
        assert (row["max_positions"], row["leverage"]) == (p.max_positions, p.leverage_ceiling)
        assert Decimal(str(row["daily_pause"])) * 100 == p.daily_loss_pause_pct
        assert Decimal(str(row["halve_drawdown"])) * 100 == p.drawdown_reduce_pct
        assert Decimal(str(row["stop_drawdown"])) * 100 == p.drawdown_stop_pct
    ex = spec.INTERPRETATION["executable_policy_scenario"]
    assert Decimal(str(ex["risk_per_trade_ceiling"])) * 100 == SYSTEM_PER_TRADE_RISK_CEILING_PCT


def test_registering_the_family_created_no_specialist_and_no_authority():
    from app.risk.system_limits import SystemLimits
    from app.trading_intelligence.execution.risk_sizing import MAX_STOP_DISTANCE
    from app.trading_intelligence.setups.registry import SPECIALIST_REGISTRY

    assert len(SPECIALIST_REGISTRY) == 4 and not any("TREND_ENSEMBLE" in k or "DAILY_TREND" in k for k in SPECIALIST_REGISTRY)
    limits = SystemLimits()
    assert limits.max_risk_per_trade_ceiling == 0.004 and limits.max_stop_loss_pct == 0.15   # ceilings untouched
    assert float(MAX_STOP_DISTANCE) == spec.INTERPRETATION["executable_policy_scenario"]["max_entry_stop_distance"]
