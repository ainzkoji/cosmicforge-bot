"""The strategy loader may drop params only when the constructor's signature rejects them.

The old loader caught *any* TypeError from ``cls(client, interval, **params)``
and rebuilt the strategy with no params. A TypeError raised inside ``__init__``
-- a bug -- was silently converted into a strategy running on defaults: the same
failure class as the get_signal retry that relabelled NO_OPPORTUNITY as
SESSION_BLOCKED.
"""
from __future__ import annotations

import json

import pytest

import app.strategy.loader as loader


def _build(monkeypatch, cls, params):
    monkeypatch.setattr(loader, "get_strategy_class", lambda name: cls)
    return loader.build_strategy(
        name="loader_probe", client=None, interval="15m",
        params_json=json.dumps(params) if params is not None else None,
    )


def test_an_internal_constructor_typeerror_propagates(monkeypatch):
    class InternalBug:
        def __init__(self, client, interval, fast=10):
            raise TypeError("a bug inside __init__")

    with pytest.raises(TypeError, match="a bug inside __init__"):
        _build(monkeypatch, InternalBug, {"fast": 5})


def test_an_internal_typeerror_in_a_kwargs_constructor_propagates(monkeypatch):
    class AcceptsAnything:
        def __init__(self, client, interval, **kwargs):
            raise TypeError("still a bug")

    with pytest.raises(TypeError, match="still a bug"):
        _build(monkeypatch, AcceptsAnything, {"anything": 1})


def test_params_the_signature_rejects_fall_back_to_defaults(monkeypatch):
    class NoParams:
        def __init__(self, client, interval):
            self.interval = interval

    strategy = _build(monkeypatch, NoParams, {"unknown_param": 3})
    assert isinstance(strategy, NoParams)
    assert strategy.interval == "15m"


def test_a_typeerror_without_params_is_never_retried(monkeypatch):
    class Broken:
        def __init__(self, client, interval):
            raise TypeError("broken with no params")

    with pytest.raises(TypeError, match="broken with no params"):
        _build(monkeypatch, Broken, None)


def test_accepted_params_are_applied(monkeypatch):
    class Fast:
        def __init__(self, client, interval, fast=10):
            self.fast = fast

    assert _build(monkeypatch, Fast, {"fast": 5}).fast == 5


def test_rejection_is_decided_by_signature_binding_only():
    class Sig:
        def __init__(self, client, interval, fast=10):
            pass

    assert loader._signature_rejects(Sig, client=None, interval="15m", slow=3) is True
    assert loader._signature_rejects(Sig, client=None, interval="15m", fast=3) is False
