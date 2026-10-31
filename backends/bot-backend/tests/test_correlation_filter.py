"""The production correlation filter blocks a second same-direction position in a correlated symbol.

Moved from ``test_replay_production_parity.py`` when the legacy replay engine (``app/replay/engine.py``) was
retired in Section I Batch 7: this check never used the replay engine. Its neighbour, the cross-symbol slot
capacity check, is covered by ``test_fill_resolution_and_slots.py::test_max_two_slots_block_a_third_position``.
"""
from __future__ import annotations

from app.risk.correlation_filter import CorrelationFilter


def test_production_correlation_filter_blocks_second_same_direction_correlated_symbol():
    blocked, reason = CorrelationFilter().should_block(
        "ETHUSDT", "LONG", {"BTCUSDT": "LONG"},
    )

    assert blocked
    assert "correlation_block" in reason
    assert "BTCUSDT" in reason
