"""Phase 13 §13.9 / §13.10 / §13.12 — replay identity and reproducibility.

A replay result is worth something only if someone can reproduce it, and only
if it can never be mistaken for evidence the live bot produced. Both of those
are properties of the manifest, so they are asserted here rather than trusted.
"""
from __future__ import annotations

import pytest

from app.replay.cost_model import CostModel
from app.replay.identity import ReplayIdentity, dataset_hash
from shared_lib.persistence.evidence_schema import (
    NON_ORGANIC_PROVENANCE,
    ORGANIC_PROVENANCE,
    REPLAY,
)


def identity(**overrides) -> ReplayIdentity:
    base = dict(
        dataset_id="ds_btc_2024",
        dataset_hash="abc123",
        symbols=("BTCUSDT",),
        timeframes=("15m", "1h"),
        start_ms=1_700_000_000_000,
        end_ms=1_700_086_400_000,
        policy_hash="policy_abc",
        strategy_id="master_ensemble",
        strategy_version="1.0.0",
        cost_model_hash=CostModel().model_hash,
        fill_model="next_bar_open",
        intrabar_policy="conservative_stop_first",
        seed=7,
        code_revision="deadbeef",
        branch="main",
        working_tree_dirty=False,
    )
    base.update(overrides)
    return ReplayIdentity(**base)


# ── §13.10 provenance ───────────────────────────────────────────────────────


def test_replay_provenance_is_fixed_and_not_organic():
    assert identity().provenance == REPLAY
    assert REPLAY in NON_ORGANIC_PROVENANCE
    assert REPLAY not in ORGANIC_PROVENANCE


def test_provenance_cannot_be_passed_in():
    """A settable provenance is how the guarantee gets lost later."""
    with pytest.raises(TypeError):
        ReplayIdentity(
            dataset_id="d", dataset_hash="h", symbols=("BTCUSDT",),
            timeframes=("15m",), start_ms=0, end_ms=1, policy_hash="p",
            strategy_id="s", strategy_version="1", cost_model_hash="c",
            fill_model="next_bar_open", intrabar_policy="conservative_stop_first",
            provenance="PAPER_FORWARD",  # type: ignore[call-arg]
        )


def test_the_manifest_carries_provenance_and_the_hash():
    manifest = identity().manifest()
    assert manifest["provenance"] == REPLAY
    assert manifest["replay_hash"]
    assert manifest["replay_id"].startswith("rpl_")


# ── §13.12 determinism ──────────────────────────────────────────────────────


def test_the_same_inputs_hash_the_same_whenever_they_are_run():
    a, b = identity(), identity()
    assert a.replay_id != b.replay_id          # distinct runs
    assert a.created_at is not None
    assert a.replay_hash == b.replay_hash      # same experiment
    assert a.reproduces(b)


@pytest.mark.parametrize(
    "field,value",
    [
        ("dataset_hash", "different"),
        ("policy_hash", "different"),
        ("cost_model_hash", "different"),
        ("fill_model", "next_bar_market"),
        ("intrabar_policy", "optimistic_target_first"),
        ("seed", 8),
        ("code_revision", "cafebabe"),
        ("working_tree_dirty", True),
        ("start_ms", 1),
        ("strategy_version", "1.0.1"),
    ],
)
def test_any_input_that_changes_the_result_changes_the_hash(field, value):
    assert not identity().reproduces(identity(**{field: value}))


def test_differences_names_exactly_what_changed():
    diff = identity().differences(identity(seed=99, fill_model="next_bar_market"))
    assert set(diff) == {"seed", "fill_model"}
    assert diff["seed"] == (7, 99)


def test_two_runs_of_the_same_experiment_have_no_differences():
    assert identity().differences(identity()) == {}


# ── Dataset fingerprint ─────────────────────────────────────────────────────


def series(closes):
    return {"BTCUSDT": {"15m": [[i, "1", "2", "0", str(c), "1", i + 1, "0", 0, "0", "0", "0"]
                                for i, c in enumerate(closes)]}}


def test_the_dataset_hash_is_of_the_data_not_of_a_filename():
    assert dataset_hash(series([1, 2, 3])) == dataset_hash(series([1, 2, 3]))
    assert dataset_hash(series([1, 2, 3])) != dataset_hash(series([1, 2, 4]))


def test_a_shorter_dataset_hashes_differently():
    assert dataset_hash(series([1, 2, 3])) != dataset_hash(series([1, 2]))


def test_the_dataset_hash_is_stable_across_symbol_ordering():
    a = {"BTCUSDT": {"15m": [[0, "1", "2", "0", "1", "1", 1, "0", 0, "0", "0", "0"]]},
         "ETHUSDT": {"15m": [[0, "1", "2", "0", "2", "1", 1, "0", 0, "0", "0", "0"]]}}
    b = {"ETHUSDT": a["ETHUSDT"], "BTCUSDT": a["BTCUSDT"]}
    assert dataset_hash(a) == dataset_hash(b)
