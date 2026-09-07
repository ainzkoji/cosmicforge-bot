from __future__ import annotations

from scripts.ml.validate_model import evaluate_acceptance
from shared_lib.ml.contract import (
    ML_CONTRACT_VERSION,
    ML_FEATURE_COLUMNS,
    ML_FEATURE_SCHEMA_HASH,
)


def _metrics(**overrides):
    values = {
        "holdout_auc": 0.63,
        "auc_std": 0.08,
        "quartile_win_rate_gap": 0.07,
        "row_count": 500,
        "logistic_baseline_auc": 0.60,
    }
    values.update(overrides)
    return values


def _contract(**overrides):
    values = {
        "contract_version": ML_CONTRACT_VERSION,
        "schema_hash": ML_FEATURE_SCHEMA_HASH,
        "feature_columns": list(ML_FEATURE_COLUMNS),
        "runtime_compatible": True,
    }
    values.update(overrides)
    return values


def test_validate_model_accepts_desired_auc_when_all_gates_pass():
    assert evaluate_acceptance(_metrics(), _contract())["accepted"] is True


def test_validate_model_rejects_auc_below_hard_floor():
    result = evaluate_acceptance(_metrics(holdout_auc=0.51), _contract())
    assert result["accepted"] is False
    assert any("below minimum 0.55" in reason for reason in result["rejection_reasons"])


def test_validate_model_rejects_auc_above_hard_ceiling():
    result = evaluate_acceptance(_metrics(holdout_auc=0.91), _contract())
    assert result["accepted"] is False
    assert any("hard ceiling 0.90" in reason for reason in result["rejection_reasons"])


def test_validate_model_rejects_schema_hash_mismatch():
    result = evaluate_acceptance(_metrics(), _contract(schema_hash="wrong"))
    assert result["accepted"] is False


def test_validate_model_rejects_wrong_feature_contract():
    result = evaluate_acceptance(_metrics(), _contract(contract_version="legacy_entry_quality_v1"))
    assert result["accepted"] is False


def test_validate_model_rejects_quartile_gap_below_hard_floor():
    result = evaluate_acceptance(_metrics(quartile_win_rate_gap=0.02), _contract())
    assert result["accepted"] is False
    assert any("below minimum 0.05" in reason for reason in result["rejection_reasons"])


def test_validate_model_rejects_unstable_auc():
    result = evaluate_acceptance(_metrics(auc_std=0.16), _contract())
    assert result["accepted"] is False
    assert any("exceeds 0.15" in reason for reason in result["rejection_reasons"])


def test_validate_model_rejects_lightgbm_below_required_baseline_improvement():
    result = evaluate_acceptance(
        _metrics(holdout_auc=0.605, logistic_baseline_auc=0.60),
        _contract(),
    )
    assert result["accepted"] is False
    assert any("by required 0.0100" in reason for reason in result["rejection_reasons"])
