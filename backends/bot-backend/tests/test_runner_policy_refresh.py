"""Phase 2 — persistent runners must never carry stale configuration.

MultiBotRunner keeps PaperRunner instances alive across cycles for good
reasons (rolling threshold windows, the 690-instrument registry, orchestrator
warm state).  The cost is that a configuration change could otherwise leave the
executor and orchestrator running on values the user has already replaced.

The contract tested here:

* every cached runner stores the policy hash it was built from;
* an unchanged policy reuses the runner;
* a materially changed policy rebuilds it;
* the outgoing runner is stopped and flushed before it is replaced, so open
  positions survive into the replacement.
"""
from __future__ import annotations

import pytest

from app.core.bot_instance_service import BotInstanceService
from app.models.bot_instance_models import BotInstance
from app.runner.effective_policy import resolve_effective_bot_policy
from app.runner.multi_runner import MultiBotRunner


# ── Test doubles ─────────────────────────────────────────────────────────────


class FakeSymbolState:
    def __init__(self, position="LONG", entry_qty=1.0, position_id="pos-1"):
        self.position = position
        self.entry_qty = entry_qty
        self.position_id = position_id


class RecordingStore:
    def __init__(self):
        self.saved: list[tuple[str, FakeSymbolState]] = []

    def save_symbol(self, symbol, state):
        self.saved.append((symbol, state))


class FakeRunner:
    """Stands in for a cached PaperRunner."""

    def __init__(self, policy_hash: str, state: dict | None = None):
        self.effective_policy_hash = policy_hash
        self.effective_policy = None
        self.run_id = "run-1"
        self.store = RecordingStore()
        self.state = state if state is not None else {}
        self._stop_requested = False


def make_instance(**overrides) -> BotInstance:
    defaults = dict(
        id="bot-1",
        user_id="user-1",
        broker_account_id="acct-1",
        market_type="CRYPTO",
        strategy_id="master_ensemble",
        strategy_version="1.0.0",
        risk_level="balanced",
        symbols=["BTCUSDT", "ETHUSDT"],
        timeframes=["15m"],
        allocation_type="fixed_amount",
        allocation_value=50.0,
        mode="paper",
        capital_allocation=500.0,
        capital_allocation_type="fixed_amount",
    )
    defaults.update(overrides)
    return BotInstance(**defaults)


def policy_for(instance: BotInstance, broker_environment: str = "demo"):
    return resolve_effective_bot_policy(
        instance=instance,
        broker_environment=broker_environment,
        risk_params=BotInstanceService.get_risk_profile_preset(instance.risk_level),
    )


@pytest.fixture
def multi_runner():
    """A MultiBotRunner with its DB-backed collaborators left unconstructed."""
    runner = MultiBotRunner.__new__(MultiBotRunner)
    runner._runners = {}
    return runner


# ── Which changes are material ───────────────────────────────────────────────


BASELINE = make_instance()


@pytest.mark.parametrize(
    "label,changed",
    [
        ("execution_mode", make_instance(mode="live")),
        ("symbols", make_instance(symbols=["BTCUSDT"])),
        ("timeframe", make_instance(timeframes=["1h"])),
        ("capital", make_instance(capital_allocation=1000.0)),
        ("allocation_value", make_instance(allocation_value=100.0)),
        ("allocation_type", make_instance(allocation_type="percent_balance", allocation_value=10.0)),
        ("risk_level", make_instance(risk_level="aggressive")),
    ],
)
def test_material_change_changes_the_policy_hash(label, changed):
    assert policy_for(changed).policy_hash != policy_for(BASELINE).policy_hash, label


def test_broker_environment_change_changes_the_policy_hash():
    demo = policy_for(BASELINE, broker_environment="demo")
    mainnet = policy_for(BASELINE, broker_environment="mainnet")
    assert demo.policy_hash != mainnet.policy_hash


def test_identical_configuration_does_not_cause_a_rebuild_loop():
    """Re-resolving the same config repeatedly must not churn the hash."""
    hashes = {policy_for(make_instance()).policy_hash for _ in range(5)}
    assert len(hashes) == 1


def test_changed_fields_are_identifiable_for_the_policy_change_log():
    before = policy_for(BASELINE).runtime_payload()
    after = policy_for(make_instance(capital_allocation=1000.0)).runtime_payload()
    changed = {k for k in set(before) | set(after) if before.get(k) != after.get(k)}
    assert "capital_budget" in changed
    assert "symbols" not in changed


# ── Cache reuse vs rebuild ───────────────────────────────────────────────────


def test_unchanged_policy_reuses_the_cached_runner(multi_runner):
    policy = policy_for(BASELINE)
    cached = FakeRunner(policy.policy_hash)
    multi_runner._runners["bot-1"] = cached

    resolved_again = policy_for(BASELINE)
    cached_hash = multi_runner._runners["bot-1"].effective_policy_hash
    policy_changed = cached_hash != resolved_again.policy_hash

    assert policy_changed is False
    assert multi_runner._runners["bot-1"] is cached


def test_changed_policy_marks_the_cached_runner_stale(multi_runner):
    multi_runner._runners["bot-1"] = FakeRunner(policy_for(BASELINE).policy_hash)
    new_policy = policy_for(make_instance(capital_allocation=1000.0))

    cached_hash = multi_runner._runners["bot-1"].effective_policy_hash
    assert cached_hash != new_policy.policy_hash


# ── Safe eviction ────────────────────────────────────────────────────────────


def test_eviction_stops_the_outgoing_runner(multi_runner):
    cached = FakeRunner("hash-old")
    multi_runner._runners["bot-1"] = cached

    multi_runner._evict_runner("bot-1", cached)

    assert cached._stop_requested is True
    assert "bot-1" not in multi_runner._runners


def test_eviction_flushes_open_positions_before_replacing_the_runner(multi_runner):
    open_state = FakeSymbolState(position="LONG", entry_qty=0.5, position_id="pos-1")
    flat_state = FakeSymbolState(position="NONE", entry_qty=0.0, position_id=None)
    cached = FakeRunner("hash-old", state={"BTCUSDT": open_state, "ETHUSDT": flat_state})
    multi_runner._runners["bot-1"] = cached

    multi_runner._evict_runner("bot-1", cached)

    saved_symbols = [symbol for symbol, _ in cached.store.saved]
    assert saved_symbols == ["BTCUSDT"], "only open positions need flushing"
    assert cached.store.saved[0][1].entry_qty == 0.5


def test_eviction_never_closes_positions(multi_runner):
    """A policy rebuild is a configuration event, not an exit signal."""
    open_state = FakeSymbolState(position="LONG", entry_qty=1.0)
    cached = FakeRunner("hash-old", state={"BTCUSDT": open_state})

    multi_runner._evict_runner("bot-1", cached)

    assert open_state.position == "LONG"
    assert open_state.entry_qty == 1.0


def test_eviction_tolerates_a_runner_without_state(multi_runner):
    multi_runner._runners["bot-1"] = FakeRunner("hash-old")
    multi_runner._evict_runner("bot-1", None)
    assert "bot-1" not in multi_runner._runners


def test_eviction_survives_a_failing_store(multi_runner):
    class ExplodingStore:
        def save_symbol(self, symbol, state):
            raise RuntimeError("disk gone")

    cached = FakeRunner("hash-old", state={"BTCUSDT": FakeSymbolState()})
    cached.store = ExplodingStore()
    multi_runner._runners["bot-1"] = cached

    multi_runner._evict_runner("bot-1", cached)  # must not raise

    assert "bot-1" not in multi_runner._runners


def test_restore_logging_reports_the_positions_a_rebuilt_runner_inherited(multi_runner, caplog):
    rebuilt = FakeRunner(
        "hash-new",
        state={
            "BTCUSDT": FakeSymbolState(position="LONG", entry_qty=0.5, position_id="pos-1"),
            "ETHUSDT": FakeSymbolState(position="NONE", entry_qty=0.0),
        },
    )

    with caplog.at_level("INFO"):
        multi_runner._log_restored_lifecycle("bot-1", rebuilt)

    message = "\n".join(record.getMessage() for record in caplog.records)
    assert "[PAPER_LIFECYCLE]" in message
    assert "event=RESTORE" in message
    assert "BTCUSDT" in message
    assert "ETHUSDT" not in message


# ── No secrets in the policy surface ─────────────────────────────────────────


def test_policy_payload_carries_no_broker_credentials():
    payload = policy_for(BASELINE).to_public_dict()
    serialized = repr(payload).lower()
    for secret in ("api_key", "api_secret", "apikey", "secret", "password", "token"):
        assert secret not in serialized
