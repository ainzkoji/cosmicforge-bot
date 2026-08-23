from __future__ import annotations

from pathlib import Path

from app.core.config import settings
from app.strategy.iofs_components.models import IOFSGateResult
from app.strategy.iofs_gate import gate_result_details, is_session_allowed, is_symbol_allowed


BACKEND_ROOT = Path(__file__).resolve().parents[1]


def test_active_validation_config_is_paper_shadow_only():
    assert settings.EXECUTION_MODE == "paper"
    assert settings.ML_ENABLED is False
    assert settings.IOFS_GATE_ENABLED is True
    assert settings.IOFS_GATE_MODE == "shadow"
    assert settings.IOFS_RISK_PROFILE == "balanced"
    assert settings.IOFS_ALLOWED_SYMBOLS == "BTCUSDT,ETHUSDT"
    assert settings.IOFS_SESSION_WINDOWS_UTC == "07:00-10:00,13:00-16:00"


def test_active_allowed_symbols_are_limited_to_btc_and_eth():
    allowed = settings.IOFS_ALLOWED_SYMBOLS
    assert is_symbol_allowed("BTCUSDT", allowed)
    assert is_symbol_allowed("ETHUSDT", allowed)
    assert not is_symbol_allowed("SOLUSDT", allowed)


def test_active_session_window_boundaries():
    from datetime import datetime, timezone

    windows = settings.IOFS_SESSION_WINDOWS_UTC
    assert is_session_allowed(windows, datetime(2026, 6, 13, 7, 0, tzinfo=timezone.utc))
    assert is_session_allowed(windows, datetime(2026, 6, 13, 13, 0, tzinfo=timezone.utc))
    assert not is_session_allowed(windows, datetime(2026, 6, 13, 10, 0, tzinfo=timezone.utc))
    assert not is_session_allowed(windows, datetime(2026, 6, 13, 16, 0, tzinfo=timezone.utc))


def test_iofs_log_payload_contains_score_reason_and_trace_fields():
    result = IOFSGateResult(
        False, "NONE", 0, "TREND_NOT_ALIGNED", None, None, None, "balanced", 72
    )
    details = gate_result_details("BTCUSDT", "shadow", result, blocked_trade=False)
    assert details["score"] == 0
    assert details["reason"] == "TREND_NOT_ALIGNED"
    assert details["blocked_trade"] is False
    assert details["timestamp_utc"]


def test_validation_artifacts_exist_and_remain_pending():
    reports = BACKEND_ROOT / "models" / "reports"
    review = (reports / "iofs_paper_trade_review_template.md").read_text(encoding="utf-8")
    status = (reports / "iofs_paper_validation_status.md").read_text(encoding="utf-8")
    checklist = (reports / "section4_paper_validation_checklist.md").read_text(encoding="utf-8")
    assert "reviewed_within_24h" in review
    assert "status: In Progress" in status
    assert "Section 4 is not passed" in status
    assert checklist.lower().count("- [x]") == 5
    assert "- [ ] Minimum 20 complete closed paper trades collected" in checklist
    assert "- [ ] Minimum 4 calendar weeks completed" in checklist
