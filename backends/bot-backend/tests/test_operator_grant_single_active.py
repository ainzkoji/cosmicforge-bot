"""K2 one-active-grant rule, enforced in the grant transaction (closeout section 13).

The unique index ``billing_operator_grants(user_id) WHERE revoked_at IS NULL`` cannot be applied: an expired grant is
never revoked, so a re-granted user legitimately holds two unrevoked rows. The rule that IS true is "at most one grant
in force (unrevoked and unexpired) per user at any instant"; it depends on the current time, so no partial index can
express it, and ``operator_grants.grant`` enforces it inside its serialised transaction instead.

The real legacy schema (``migrate``) and the real ``grant`` / ``revoke`` / ``active_grant`` run here on SQLite; under
``-p tests.legacy_pg_plugin`` with ``LEGACY_TEST_POSTGRES_URL`` the same module runs on a throwaway PostgreSQL
database through the legacy adapter. Synthetic users only.
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone

import pytest

from shared_lib.billing import operator_grants as G

NOW = datetime(2026, 10, 31, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setenv("COSMICFORGE_TEST_MODE", "1")
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate
    database = DB(path=str(tmp_path / "grants.db"))
    migrate(database)
    with database.connect() as c:
        G.ensure_schema(c)
        for uid in ("alice", "bob"):
            c.execute("INSERT INTO users (id, email, hashed_password, status, created_at, updated_at) VALUES (?,?,?,?,?,?)",
                      (uid, f"{uid}@example.test", "x", "active", NOW.isoformat(), NOW.isoformat()))
    return database


def _grant(db, user="alice", *, now=NOW, days=30, plan="plan_pro"):
    return G.grant(db, user_id=user, plan_id=plan, issued_by="admin-ops", reason="pilot cohort", duration_days=days,
                   now=now)


def _rows(db, user="alice"):
    with db.connect() as c:
        return [dict(r) for r in c.execute("SELECT * FROM billing_operator_grants WHERE user_id=? ORDER BY granted_at, "
                                           "grant_id", (user,)).fetchall()]


def _conflicts(db, now):
    with db.connect() as c:
        return G.grants_in_force_conflicts(c, now)


def _active(db, now, user="alice"):
    with db.connect() as c:
        return G.active_grant(c, user, now)


def test_a_regrant_after_expiry_keeps_the_expired_row_as_legitimate_history(db):
    first = _grant(db, days=10)
    later = NOW + timedelta(days=20)                       # the first grant expired ten days ago, never revoked
    second = _grant(db, now=later, days=10)
    rows = _rows(db)
    assert [r["grant_id"] for r in rows] == [first["grant_id"], second["grant_id"]]
    assert [r["revoked_at"] for r in rows] == [None, None]                 # two unrevoked rows: allowed
    assert second["superseded_grant_id"] is None and second["superseded_grant_ids"] == []
    assert _active(db, later)["grant_id"] == second["grant_id"]
    assert _conflicts(db, later) == []                                      # expired history is not a conflict


def test_a_regrant_while_active_supersedes_and_leaves_exactly_one_in_force(db):
    first = _grant(db)
    second = _grant(db, now=NOW + timedelta(days=1), plan="plan_whale")
    assert second["superseded_grant_ids"] == [first["grant_id"]]
    rows = {r["grant_id"]: r for r in _rows(db)}
    assert rows[first["grant_id"]]["revoke_reason"] == "SUPERSEDED" and rows[second["grant_id"]]["revoked_at"] is None
    assert _active(db, NOW + timedelta(days=2))["grant_id"] == second["grant_id"]
    assert _conflicts(db, NOW + timedelta(days=2)) == []


def test_two_grants_in_force_written_outside_grant_are_reported_and_the_next_grant_repairs_them(db):
    original = {"user_id": "alice", "plan_id": "plan_basic", "status": "active"}
    with db.connect() as c:                                 # a state ``grant`` never produces (a direct write)
        for gid, at, prev in (("g_old", NOW, json.dumps(original)), ("g_new", NOW + timedelta(hours=1), None)):
            c.execute("INSERT INTO billing_operator_grants (grant_id,user_id,plan_id,issued_by,reason,granted_at,"
                      "expires_at,previous_row_json) VALUES (?,?,?,?,?,?,?,?)",
                      (gid, "alice", "plan_pro", "x", "direct", at.isoformat(), (at + timedelta(days=30)).isoformat(),
                       prev))
    probe = NOW + timedelta(days=2)
    assert _conflicts(db, probe) == [{"user_id": "alice", "grant_ids": ["g_new", "g_old"]}]
    assert _active(db, probe)["grant_id"] == "g_new"        # the newest grant in force, as before
    third = _grant(db, now=probe)
    assert sorted(third["superseded_grant_ids"]) == ["g_new", "g_old"]
    assert _conflicts(db, probe) == []
    rows = {r["grant_id"]: r for r in _rows(db)}
    assert rows[third["grant_id"]]["previous_row_json"] == json.dumps(original)   # the chain's original row


def test_the_post_insert_check_rolls_back_every_write_when_a_supersede_did_not_apply(db):
    first = _grant(db)
    with db.connect() as c:
        sub_before = dict(c.execute("SELECT * FROM subscriptions WHERE user_id='alice'").fetchone())
        # a schema-level interference the grant code cannot see: the supersede UPDATE is silently skipped
        c.execute("CREATE TRIGGER IF NOT EXISTS trg_test_skip_supersede BEFORE UPDATE ON billing_operator_grants "
                  "WHEN NEW.revoke_reason = 'SUPERSEDED' BEGIN SELECT RAISE(IGNORE); END")
    with pytest.raises(G.GrantError) as err:
        _grant(db, now=NOW + timedelta(days=1), plan="plan_whale")
    assert err.value.code == "ACTIVE_GRANT_CONFLICT"
    assert [r["grant_id"] for r in _rows(db)] == [first["grant_id"]]          # no second row
    with db.connect() as c:
        assert dict(c.execute("SELECT * FROM subscriptions WHERE user_id='alice'").fetchone()) == sub_before
    assert _conflicts(db, NOW + timedelta(days=1)) == []


def test_concurrent_grants_are_serialised_to_one_grant_in_force(db):
    errors, results = [], []

    def worker(i):
        try:
            results.append(_grant(db, now=NOW + timedelta(seconds=i)))
        except Exception as exc:  # noqa: BLE001 -- reported below
            errors.append(repr(exc))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == [] and len(results) == 6
    probe = NOW + timedelta(minutes=1)
    assert _conflicts(db, probe) == []
    in_force = [r for r in _rows(db) if r["revoked_at"] is None]
    assert len(in_force) == 1 and _active(db, probe)["grant_id"] == in_force[0]["grant_id"]


def test_users_are_independent_and_revocation_is_unchanged(db):
    a = _grant(db, "alice")
    b = _grant(db, "bob")
    assert _conflicts(db, NOW) == []
    out = G.revoke(db, grant_id=a["grant_id"], revoked_by="admin-ops", reason="cohort closed", now=NOW)
    assert out["restored"] == "FREE_PLAN"
    assert _active(db, NOW) is None and _active(db, NOW, "bob")["grant_id"] == b["grant_id"]
