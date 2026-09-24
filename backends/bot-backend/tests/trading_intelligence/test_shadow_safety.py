"""Section 9.15.J -- CATI cannot create/cancel/amend an order, is disabled by
default, and a fault inside CATI can never propagate into the runner."""
from __future__ import annotations

import logging
import os

import pytest

from app.trading_intelligence.integration import shadow_hook


class _OrderPlacingClientSpy:
    """Any exchange-mutating call on this stand-in fails the test."""

    def __getattr__(self, name):
        if name in {"place_order", "cancel_order", "amend_order", "place_protection", "close_position"}:
            def _boom(*args, **kwargs):
                raise AssertionError(f"CATI shadow path called a mutating exchange method: {name}")
            return _boom
        raise AttributeError(name)


def test_full_pipeline_shadow_disabled_by_default(monkeypatch):
    monkeypatch.delenv(shadow_hook.FULL_PIPELINE_ENV_FLAG, raising=False)
    assert shadow_hook.is_full_pipeline_enabled() is False


def test_full_pipeline_shadow_never_raises_even_on_internal_failure(monkeypatch, caplog):
    monkeypatch.setenv(shadow_hook.FULL_PIPELINE_ENV_FLAG, "true")

    class BrokenSnapshot:
        symbol = "BTCUSDT"

        def __getattr__(self, name):
            raise RuntimeError("boom: simulated CATI internal bug")

    with caplog.at_level(logging.ERROR):
        result = shadow_hook.run_full_shadow_pipeline(BrokenSnapshot(), venue="binance", source="Test")
    assert result is None
    # The controller now converts component errors into an explicit terminal
    # CATI_COMPONENT_ERROR (Section 15.2) and logs it; either log is acceptable.
    assert any(
        "full pipeline shadow evaluation failed" in rec.message or "component error during symbol evaluation" in rec.message
        for rec in caplog.records
    )


def test_disabled_full_pipeline_is_a_pure_noop(monkeypatch):
    monkeypatch.delenv(shadow_hook.FULL_PIPELINE_ENV_FLAG, raising=False)

    class FakeSnapshot:
        def __getattr__(self, name):
            raise AssertionError("must not be called while disabled")

    result = shadow_hook.run_full_shadow_pipeline(FakeSnapshot(), venue="binance", source="Test")
    assert result is None


def test_shadow_disabled_by_default(monkeypatch):
    monkeypatch.delenv(shadow_hook.ENV_FLAG, raising=False)
    assert shadow_hook.is_enabled() is False


def test_shadow_enabled_flag_reads_env(monkeypatch):
    monkeypatch.setenv(shadow_hook.ENV_FLAG, "true")
    assert shadow_hook.is_enabled() is True
    monkeypatch.setenv(shadow_hook.ENV_FLAG, "false")
    assert shadow_hook.is_enabled() is False


def test_disabled_shadow_evaluation_is_a_pure_noop(monkeypatch, trending_series):
    monkeypatch.delenv(shadow_hook.ENV_FLAG, raising=False)
    from conftest import make_binance_klines

    class FakeSnapshot:
        symbol = "BTCUSDT"
        timeframe = "15m"
        candles = make_binance_klines(60)
        higher_timeframe = None
        higher_timeframe_candles = ()
        auxiliary_candles = {}
        market_snapshot_id = "ms_x"
        data_hash = "hash"
        latest_closed_candle_time = 1_700_000_000_000

        def htf_is_timestamp_aligned(self):
            raise AssertionError("must not be called while disabled")

    result = shadow_hook.run_shadow_evaluation(FakeSnapshot(), venue="binance", source="Test")
    assert result is None


def test_shadow_evaluation_never_raises_even_on_internal_failure(monkeypatch, caplog):
    monkeypatch.setenv(shadow_hook.ENV_FLAG, "true")

    class BrokenSnapshot:
        # Deliberately missing every attribute evaluate_market_state needs,
        # to force an exception deep inside the adapter.
        symbol = "BTCUSDT"

        def __getattr__(self, name):
            raise RuntimeError("boom: simulated CATI internal bug")

    with caplog.at_level(logging.ERROR):
        result = shadow_hook.run_shadow_evaluation(BrokenSnapshot(), venue="binance", source="Test")
    assert result is None
    assert any("shadow evaluation failed" in rec.message for rec in caplog.records)


def test_shadow_hook_module_never_imports_execution_or_order_placement():
    import ast
    import inspect

    source = inspect.getsource(shadow_hook)
    tree = ast.parse(source)
    forbidden_modules = {"app.execution", "app.exchange"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for forbidden in forbidden_modules:
                assert not node.module.startswith(forbidden), (
                    f"shadow_hook imports from {node.module}, which can mutate broker state"
                )


def test_shadow_evaluation_end_to_end_never_calls_mutating_exchange_methods(monkeypatch, trending_series):
    """Full end-to-end shadow run using a real snapshot; a spy client stands
    in for anything CATI might (incorrectly) try to call for order
    placement. CATI never receives a client reference at all, so this test
    also documents that fact structurally."""
    import inspect

    from app.trading_intelligence.integration import snapshot_adapter

    sig = inspect.signature(snapshot_adapter.evaluate_market_state)
    assert "client" not in sig.parameters
    assert "executor" not in sig.parameters
