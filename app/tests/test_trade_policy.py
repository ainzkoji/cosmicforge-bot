from app.policy.trade_policy import PolicyInputs, decide, Action


def _inp(**overrides):
    base = dict(
        position="flat",
        adds=0,
        pending_open=None,
        reentry_confirm_signal=None,
        reentry_confirm_count=0,
        last_trade_ms=0,
        last_stop_ms=0,
        signal="HOLD",
        cooldown_seconds=60,
        sl_cooldown_seconds=3600,
        max_adds=2,
        trade_mode="flip",
        reentry_confirmations=1,
        now_ms=1_000_000,
        kill_switch=False,
    )
    base.update(overrides)
    return PolicyInputs(**base)


def test_hold_signal():
    r = decide(_inp(signal="HOLD"))
    assert r.action == Action.HOLD
    assert r.reason == "signal=HOLD"


def test_kill_switch_blocks_open_add():
    r = decide(_inp(signal="BUY", kill_switch=True))
    assert r.action == Action.HOLD
    assert "blocked" in r.reason


def test_cooldown_blocks_open():
    r = decide(
        _inp(
            signal="BUY", last_trade_ms=999_900, now_ms=1_000_000, cooldown_seconds=200
        )
    )
    assert r.action == Action.HOLD
    assert "blocked" in r.reason


def test_sl_cooldown_blocks_open():
    r = decide(
        _inp(
            signal="BUY",
            last_stop_ms=999_900,
            now_ms=1_000_000,
            sl_cooldown_seconds=200,
        )
    )
    assert r.action == Action.HOLD
    assert "blocked" in r.reason


def test_open_long_from_flat():
    r = decide(_inp(signal="BUY", position="flat"))
    assert r.action == Action.OPEN_LONG


def test_open_short_from_flat():
    r = decide(_inp(signal="SELL", position="flat"))
    assert r.action == Action.OPEN_SHORT


def test_add_long_respects_max_adds():
    r1 = decide(_inp(signal="BUY", position="long", adds=1, max_adds=2))
    assert r1.action == Action.ADD_LONG

    r2 = decide(_inp(signal="BUY", position="long", adds=2, max_adds=2))
    assert r2.action == Action.HOLD
    assert "max_adds" in r2.reason


def test_add_short_respects_max_adds():
    r1 = decide(_inp(signal="SELL", position="short", adds=1, max_adds=2))
    assert r1.action == Action.ADD_SHORT

    r2 = decide(_inp(signal="SELL", position="short", adds=2, max_adds=2))
    assert r2.action == Action.HOLD
    assert "max_adds" in r2.reason


def test_flip_long_to_short_sets_pending_open():
    r = decide(_inp(signal="SELL", position="long", trade_mode="flip"))
    assert r.action == Action.FLIP_TO_SHORT
    assert r.pending_open == "SELL"


def test_flip_short_to_long_sets_pending_open():
    r = decide(_inp(signal="BUY", position="short", trade_mode="flip"))
    assert r.action == Action.FLIP_TO_LONG
    assert r.pending_open == "BUY"


def test_oneway_close_long():
    r = decide(_inp(signal="SELL", position="long", trade_mode="oneway"))
    assert r.action == Action.CLOSE


def test_reentry_confirmations_open_after_two():
    a = decide(_inp(signal="BUY", position="flat", reentry_confirmations=2))
    assert a.action == Action.HOLD
    assert a.reentry_confirm_signal == "BUY"
    assert a.reentry_confirm_count == 1

    b = decide(
        _inp(
            signal="BUY",
            position="flat",
            reentry_confirmations=2,
            reentry_confirm_signal=a.reentry_confirm_signal,
            reentry_confirm_count=a.reentry_confirm_count,
        )
    )
    assert b.action == Action.OPEN_LONG
    assert b.reentry_confirm_signal is None
    assert b.reentry_confirm_count == 0
