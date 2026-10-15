from types import SimpleNamespace
from unittest.mock import Mock

from app.trading_intelligence.integration.venue_runtime import VenueRuntime


def test_sessions_are_isolated_rotated_removed_and_closed_on_ownership_loss():
    accounts = [{"id": "a", "user_id": "u1"}, {"id": "b", "user_id": "u2"}]
    version, owns = [1], [True]
    made = []
    def resolve(account, user, db):
        return SimpleNamespace(account_id=account, user_id=user, broker_type="binance", environment="demo",
                               credential_version=version[0], key_fingerprint=account)
    def session(*args, **kwargs):
        s = Mock()
        made.append((kwargs["account_ref"], s))
        return s
    runtime = VenueRuntime(None, owns_runtime=lambda db: owns[0], server_address="shared-egress",
                           accounts=lambda db: accounts, resolve=resolve, build=lambda auth: auth,
                           session_factory=session)
    runtime.tick()
    assert [key for key, _ in made] == ["a", "b"]
    assert all(s.step.call_count == 1 for _, s in made)
    runtime.tick()
    assert len(made) == 2
    version[0] = 2
    runtime.last_discovery = 0
    runtime.tick()
    assert len(made) == 4 and all(s.close.call_count == 1 for _, s in made[:2])
    accounts.pop()
    runtime.last_discovery = 0
    runtime.tick()
    assert set(runtime.sessions) == {"a"} and made[-1][1].close.call_count == 1
    owns[0] = False
    runtime.tick()
    assert runtime.sessions == {} and made[-2][1].close.call_count == 1


def test_one_accounts_failure_does_not_stop_others_or_expose_raw_exception(caplog):
    runtime = VenueRuntime(None, owns_runtime=lambda db: True, server_address="shared-egress",
                           accounts=lambda db: [], resolve=Mock(), build=Mock())
    bad, good = Mock(), Mock()
    bad.step.side_effect = ValueError("credential-secret")
    import time
    runtime.last_discovery = time.monotonic()
    runtime.sessions = {"a": (None, bad), "b": (None, good)}
    runtime.tick()
    assert good.step.call_count == 1
    assert "credential-secret" not in caplog.text
    runtime.request_stop()
    runtime.tick()
    assert good.close.call_count == 1 and bad.close.call_count == 1
