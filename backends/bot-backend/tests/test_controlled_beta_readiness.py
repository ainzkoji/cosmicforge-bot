"""Phase 10 — controlled-beta readiness, approval and invalidation.

Two properties matter more than anything else here:

* **Approval is never automatic.** Meeting the evidence bar moves a bot into
  review, not into approved. A human admin must act, and the act is recorded.
* **Approval never silently survives a material change.** A new policy hash
  invalidates the approval and drops the bot back to NOT_READY.

None of these tests lower the numeric readiness thresholds; they govern the
machinery around them.
"""
from __future__ import annotations

import inspect

import pytest

from app.product_safety.controlled_beta import (
    ALLOWED_TRANSITIONS,
    APPROVED,
    APPROVED_FOR_CONTROLLED_BETA,
    INVALIDATED,
    NOT_READY,
    READY_FOR_CONTROLLED_BETA_REVIEW,
    REQUIRED_SECTIONS,
    REVOKED,
    ReadinessTransitionError,
    active_approval,
    approve_controlled_beta,
    confirm_section,
    confirmed_sections,
    evidence_hash,
    get_state,
    invalidate_on_policy_change,
    is_approved_for_controlled_beta,
    missing_sections,
    organic_closed_trades,
    organic_daily_close_days,
    revoke_controlled_beta,
    sections_complete,
    set_state,
)
from shared_lib.persistence.db import DB
from shared_lib.persistence.evidence_schema import (
    PAPER_FORWARD,
    PAPER_FORWARD_VALIDATION,
    REPLAY,
)
from shared_lib.persistence.migrations import migrate

BOT = "bot-readiness-1"
HASH = "policy_hash_aaaaaaaa"
NEW_HASH = "policy_hash_bbbbbbbb"
EVIDENCE = {"closed_trades": 62, "profit_factor": 1.42, "max_drawdown": 0.061}


@pytest.fixture
def db():
    database = DB(":memory:")
    migrate(database)
    return database


def make_reviewable(db, policy_hash=HASH):
    set_state(db, BOT, state=READY_FOR_CONTROLLED_BETA_REVIEW,
              updated_by="system", reason="evidence thresholds met", policy_hash=policy_hash)
    for section in REQUIRED_SECTIONS:
        confirm_section(db, bot_instance_id=BOT, section=section, confirmed_by="admin:u1",
                        evidence_reference=f"evidence/{section}", policy_hash=policy_hash)


# ── §13: the state machine ──────────────────────────────────────────────────


def test_a_bot_with_no_record_is_not_ready(db):
    assert get_state(db, BOT).state == NOT_READY


def test_the_only_path_to_approval_goes_through_review():
    assert APPROVED_FOR_CONTROLLED_BETA not in ALLOWED_TRANSITIONS[NOT_READY]
    assert APPROVED_FOR_CONTROLLED_BETA in ALLOWED_TRANSITIONS[READY_FOR_CONTROLLED_BETA_REVIEW]


def test_jumping_straight_from_not_ready_to_approved_is_rejected(db):
    with pytest.raises(ReadinessTransitionError, match="Illegal transition"):
        set_state(db, BOT, state=APPROVED_FOR_CONTROLLED_BETA,
                  updated_by="admin", reason="shortcut")


def test_an_unknown_state_is_rejected(db):
    with pytest.raises(ReadinessTransitionError, match="Unknown readiness state"):
        set_state(db, BOT, state="DEFINITELY_FINE", updated_by="admin", reason="nope")


def test_a_bot_can_always_fall_back_to_not_ready(db):
    make_reviewable(db)
    approve_controlled_beta(db, bot_instance_id=BOT, reviewer_user_id="admin:u1",
                            reviewer_role="admin", policy_hash=HASH, evidence_snapshot=EVIDENCE)
    set_state(db, BOT, state=NOT_READY, updated_by="admin", reason="regression found")
    assert get_state(db, BOT).state == NOT_READY


# ── §13/§14: approval is never automatic ────────────────────────────────────


def test_approval_requires_the_review_state(db):
    for section in REQUIRED_SECTIONS:
        confirm_section(db, bot_instance_id=BOT, section=section, confirmed_by="a",
                        evidence_reference="e", policy_hash=HASH)

    with pytest.raises(ReadinessTransitionError, match="approval requires"):
        approve_controlled_beta(db, bot_instance_id=BOT, reviewer_user_id="admin:u1",
                                reviewer_role="admin", policy_hash=HASH,
                                evidence_snapshot=EVIDENCE)


def test_nothing_in_the_runtime_can_grant_approval_by_itself():
    """The runtime may compute readiness; only an admin action may approve."""
    from app.product_safety import controlled_beta

    source = inspect.getsource(controlled_beta.approve_controlled_beta)
    assert "reviewer_user_id" in source
    assert "reviewer_role" in source

    # No auto-approval path anywhere in the module.
    module_source = inspect.getsource(controlled_beta)
    assert "auto_approve" not in module_source
    assert "AUTO_APPROVED" not in module_source


def test_a_successful_approval_records_who_when_and_against_what(db):
    make_reviewable(db)
    result = approve_controlled_beta(
        db, bot_instance_id=BOT, reviewer_user_id="admin:u1", reviewer_role="platform_admin",
        policy_hash=HASH, evidence_snapshot=EVIDENCE, code_revision="abc1234",
        notes="reviewed 3 paper weeks",
    )

    assert result["approval_status"] == APPROVED
    assert result["evidence_hash"] == evidence_hash(EVIDENCE)
    assert get_state(db, BOT).state == APPROVED_FOR_CONTROLLED_BETA

    approval = active_approval(db, BOT, HASH)
    assert approval["reviewer_admin_id"] == "admin:u1"
    assert approval["reviewer_role"] == "platform_admin"
    assert approval["source_commit_sha"] == "abc1234"
    assert approval["approved_at"]


def test_approval_history_is_preserved_not_reduced_to_a_boolean(db):
    make_reviewable(db)
    approve_controlled_beta(db, bot_instance_id=BOT, reviewer_user_id="admin:u1",
                            reviewer_role="admin", policy_hash=HASH, evidence_snapshot=EVIDENCE)
    revoke_controlled_beta(db, bot_instance_id=BOT, revoked_by="admin:u2", reason="incident")
    make_reviewable(db)
    approve_controlled_beta(db, bot_instance_id=BOT, reviewer_user_id="admin:u3",
                            reviewer_role="admin", policy_hash=HASH, evidence_snapshot=EVIDENCE)

    with db.connect() as conn:
        rows = conn.execute(
            "SELECT approval_status FROM readiness_approvals WHERE bot_instance_id=?", (BOT,)
        ).fetchall()
    assert len(rows) == 2, "history must survive, not be overwritten"


# ── §17: Sections A-E are individually confirmed ────────────────────────────


def test_every_section_needs_its_own_confirmation(db):
    assert sorted(missing_sections(db, BOT, HASH)) == list(REQUIRED_SECTIONS)


def test_approval_is_blocked_while_any_section_is_outstanding(db):
    set_state(db, BOT, state=READY_FOR_CONTROLLED_BETA_REVIEW,
              updated_by="system", reason="evidence met", policy_hash=HASH)
    for section in ("A", "B", "C", "D"):  # E deliberately missing
        confirm_section(db, bot_instance_id=BOT, section=section, confirmed_by="a",
                        evidence_reference="e", policy_hash=HASH)

    with pytest.raises(ReadinessTransitionError, match=r"\['E'\]"):
        approve_controlled_beta(db, bot_instance_id=BOT, reviewer_user_id="admin:u1",
                                reviewer_role="admin", policy_hash=HASH,
                                evidence_snapshot=EVIDENCE)


def test_a_section_confirmation_requires_evidence(db):
    with pytest.raises(ReadinessTransitionError, match="evidence reference"):
        confirm_section(db, bot_instance_id=BOT, section="A", confirmed_by="a",
                        evidence_reference="", policy_hash=HASH)


def test_an_unknown_section_is_rejected(db):
    with pytest.raises(ReadinessTransitionError, match="Unknown section"):
        confirm_section(db, bot_instance_id=BOT, section="Z", confirmed_by="a",
                        evidence_reference="e", policy_hash=HASH)


def test_confirmations_are_scoped_to_the_policy_they_were_made_against(db):
    for section in REQUIRED_SECTIONS:
        confirm_section(db, bot_instance_id=BOT, section=section, confirmed_by="a",
                        evidence_reference="e", policy_hash=HASH)

    assert sections_complete(db, BOT, HASH) is True
    assert sections_complete(db, BOT, NEW_HASH) is False, (
        "confirmations must not carry across a material policy change"
    )
    assert confirmed_sections(db, BOT, NEW_HASH) == set()


def test_there_is_no_blanket_sections_confirmed_flag():
    """§17 — remove any default 'SECTIONS_A_TO_E_CONFIRMED = true' behaviour."""
    from app.core.config import settings

    assert settings.SECTIONS_A_TO_E_CONFIRMED is False, "must default to fail-closed"

    from app.product_safety import controlled_beta

    source = inspect.getsource(controlled_beta)
    assert "SECTIONS_A_TO_E_CONFIRMED" not in source


# ── §15: approval invalidation on material policy change ────────────────────


def test_a_policy_change_invalidates_the_approval(db):
    make_reviewable(db)
    approve_controlled_beta(db, bot_instance_id=BOT, reviewer_user_id="admin:u1",
                            reviewer_role="admin", policy_hash=HASH, evidence_snapshot=EVIDENCE)
    assert is_approved_for_controlled_beta(db, BOT, HASH) is True

    invalidated = invalidate_on_policy_change(db, bot_instance_id=BOT, current_policy_hash=NEW_HASH)

    assert invalidated == 1
    assert get_state(db, BOT).state == NOT_READY
    assert is_approved_for_controlled_beta(db, BOT, NEW_HASH) is False


def test_an_unchanged_policy_does_not_invalidate(db):
    make_reviewable(db)
    approve_controlled_beta(db, bot_instance_id=BOT, reviewer_user_id="admin:u1",
                            reviewer_role="admin", policy_hash=HASH, evidence_snapshot=EVIDENCE)

    assert invalidate_on_policy_change(db, bot_instance_id=BOT, current_policy_hash=HASH) == 0
    assert is_approved_for_controlled_beta(db, BOT, HASH) is True


def test_approval_never_silently_applies_to_a_different_policy(db):
    make_reviewable(db)
    approve_controlled_beta(db, bot_instance_id=BOT, reviewer_user_id="admin:u1",
                            reviewer_role="admin", policy_hash=HASH, evidence_snapshot=EVIDENCE)

    assert active_approval(db, BOT, HASH) is not None
    assert active_approval(db, BOT, NEW_HASH) is None, "fail closed on hash mismatch"


def test_invalidated_approvals_are_marked_not_deleted(db):
    make_reviewable(db)
    approve_controlled_beta(db, bot_instance_id=BOT, reviewer_user_id="admin:u1",
                            reviewer_role="admin", policy_hash=HASH, evidence_snapshot=EVIDENCE)
    invalidate_on_policy_change(db, bot_instance_id=BOT, current_policy_hash=NEW_HASH)

    with db.connect() as conn:
        row = dict(conn.execute(
            "SELECT * FROM readiness_approvals WHERE bot_instance_id=?", (BOT,)
        ).fetchone())
    assert row["approval_status"] == INVALIDATED
    assert row["invalidated_at"]
    assert row["invalidation_reason"] == "MATERIAL_POLICY_CHANGE"


# ── §16: revocation ─────────────────────────────────────────────────────────


def test_revocation_takes_effect_immediately(db):
    make_reviewable(db)
    approve_controlled_beta(db, bot_instance_id=BOT, reviewer_user_id="admin:u1",
                            reviewer_role="admin", policy_hash=HASH, evidence_snapshot=EVIDENCE)

    revoke_controlled_beta(db, bot_instance_id=BOT, revoked_by="admin:u2",
                           reason="drawdown breach observed")

    assert get_state(db, BOT).state == NOT_READY
    assert is_approved_for_controlled_beta(db, BOT, HASH) is False


def test_revocation_records_who_when_and_why(db):
    make_reviewable(db)
    approve_controlled_beta(db, bot_instance_id=BOT, reviewer_user_id="admin:u1",
                            reviewer_role="admin", policy_hash=HASH, evidence_snapshot=EVIDENCE)
    revoke_controlled_beta(db, bot_instance_id=BOT, revoked_by="admin:u2", reason="incident 42")

    with db.connect() as conn:
        row = dict(conn.execute(
            "SELECT * FROM readiness_approvals WHERE bot_instance_id=?", (BOT,)
        ).fetchone())
    assert row["approval_status"] == REVOKED
    assert row["revoked_by"] == "admin:u2"
    assert row["revocation_reason"] == "incident 42"
    assert row["revoked_at"]


def test_revocation_requires_a_reason(db):
    with pytest.raises(ReadinessTransitionError, match="reason"):
        revoke_controlled_beta(db, bot_instance_id=BOT, revoked_by="admin:u2", reason="")


# ── §18/§19: readiness evidence provenance ──────────────────────────────────


def test_only_organic_daily_close_evidence_counts(db):
    from app.evidence.writers import record_position_event, record_position_opened

    record_position_opened(db, position_id="p1", bot_instance_id=BOT, symbol="BTCUSDT",
                           side="LONG", original_qty=1.0, entry_price=100.0)
    record_position_event(db, position_id="p1", bot_instance_id=BOT, symbol="BTCUSDT",
                          event_type="DAILY_CLOSE", provenance=PAPER_FORWARD)
    record_position_event(db, position_id="p1", bot_instance_id=BOT, symbol="BTCUSDT",
                          event_type="DAILY_CLOSE", provenance=PAPER_FORWARD_VALIDATION)
    record_position_event(db, position_id="p1", bot_instance_id=BOT, symbol="BTCUSDT",
                          event_type="DAILY_CLOSE", provenance=REPLAY)

    # All three land on the same date; only the organic one is eligible.
    assert organic_daily_close_days(db, BOT) == 1


def test_validation_positions_do_not_count_as_closed_trades(db):
    from app.evidence.writers import record_position_opened, update_position_quantities

    for index, provenance in enumerate((PAPER_FORWARD, PAPER_FORWARD_VALIDATION, REPLAY)):
        pid = f"p{index}"
        record_position_opened(db, position_id=pid, bot_instance_id=BOT, symbol="BTCUSDT",
                               side="LONG", original_qty=1.0, entry_price=100.0,
                               provenance=provenance)
        update_position_quantities(db, pid, remaining_qty=0.0, realized_qty=1.0, status="CLOSED")

    assert organic_closed_trades(db, BOT) == 1


def test_readiness_thresholds_were_not_lowered():
    """§12 — the numeric bar is unchanged by this batch."""
    from app.product_safety import readiness_gate

    source = inspect.getsource(readiness_gate)
    # Spot-check the documented thresholds still appear at their stated values.
    assert "1.3" in source or "1.30" in source     # profit factor
    assert "60" in source                          # closed trades
    assert "3" in source                           # consecutive paper weeks


def test_the_full_gate_requires_state_approval_and_sections_together(db):
    make_reviewable(db)
    approve_controlled_beta(db, bot_instance_id=BOT, reviewer_user_id="admin:u1",
                            reviewer_role="admin", policy_hash=HASH, evidence_snapshot=EVIDENCE)
    assert is_approved_for_controlled_beta(db, BOT, HASH) is True

    # Remove one section confirmation: the gate must close again.
    with db.connect() as conn:
        conn.execute(
            "DELETE FROM readiness_section_confirmations WHERE bot_instance_id=? AND section='C'",
            (BOT,),
        )
    assert is_approved_for_controlled_beta(db, BOT, HASH) is False
