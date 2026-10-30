"""The runtime-owner lease across hosts and containers (Step 5.2, multi-process trading).

A PID is only meaningful in the process table it came from. Two trading containers on one database have separate
PID namespaces (and their own hostnames), so a holder on ANOTHER host cannot be inspected from here: it is alive
until its heartbeat goes stale (``LEASE_STALE_SECONDS``). On this host a dead holder is still taken over at once.
"""
from __future__ import annotations

import os
import socket
from datetime import datetime, timedelta, timezone

import pytest

from app.ops.runtime_ownership import (LEASE_STALE_SECONDS, TRADING_SCHEDULER, RuntimeOwnership, holder_is_alive,
                                       is_local_host)
from app.ops.runtime_preflight import PreflightStatus, preflight
from shared_lib.persistence.db import DB

OTHER_HOST = "bot-trading-2f9c"          # another container's hostname
DEAD_PID = 999_999                        # no such process on this host


@pytest.fixture
def db(tmp_path):
    database = DB(str(tmp_path / "runtime.db"))
    RuntimeOwnership(database, database_path=database.path)  # schema via ensure on first acquire
    from app.ops.runtime_ownership import ensure_ownership_schema
    ensure_ownership_schema(database)
    return database


def write_lease(db, *, pid, hostname, heartbeat_at=None, started_at=None):
    now = datetime.now(timezone.utc)
    heartbeat_at, started_at = heartbeat_at or now, started_at or now
    with db.connect() as conn:
        conn.execute("INSERT OR REPLACE INTO runtime_ownership VALUES (?,?,?,?,?,?,?,?,?,NULL,NULL)",
                     (TRADING_SCHEDULER, os.path.abspath(db.path), "own_other", "rts_other", pid, hostname,
                      started_at.isoformat(), heartbeat_at.isoformat(), "production"))


def contender(db):
    return RuntimeOwnership(db, database_path=db.path, database_role="production")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_a_live_lease_of_another_container_is_never_taken_by_pid():
    assert is_local_host(socket.gethostname()) and is_local_host(None) and not is_local_host(OTHER_HOST)
    assert holder_is_alive(DEAD_PID, datetime.now(timezone.utc).isoformat(), hostname=OTHER_HOST)
    assert not holder_is_alive(DEAD_PID, datetime.now(timezone.utc).isoformat(), hostname=socket.gethostname())


def test_a_second_trading_container_does_not_take_a_fresh_lease_from_another_host(db):
    write_lease(db, pid=DEAD_PID, hostname=OTHER_HOST)            # PID unknown HERE, heartbeat fresh
    result = contender(db).acquire()
    assert not result.acquired and result.reason == "RUNTIME_OWNERSHIP_HELD_BY_ANOTHER_PROCESS"
    assert result.holder_hostname == OTHER_HOST
    report = preflight(db=db, port=free_port())
    assert report.lease_owner_alive is True and report.status != PreflightStatus.STALE_LEASE


def test_another_hosts_lease_is_taken_over_once_its_heartbeat_is_stale(db):
    stale = datetime.now(timezone.utc) - timedelta(seconds=LEASE_STALE_SECONDS + 5)
    write_lease(db, pid=DEAD_PID, hostname=OTHER_HOST, heartbeat_at=stale, started_at=stale)
    assert contender(db).acquire().acquired


def test_a_dead_holder_on_this_host_is_still_taken_over_at_once(db):
    write_lease(db, pid=DEAD_PID, hostname=socket.gethostname())  # fresh heartbeat, but the process is gone
    assert contender(db).acquire().acquired


def test_the_same_pid_number_in_another_container_is_not_this_process(db):
    """Containers commonly run the service under the same low PID. A lease written by PID N in another container
    must not make PID N here believe it is the runtime owner."""
    from app.trading_intelligence.integration.residual_prospective import owner_current
    write_lease(db, pid=os.getpid(), hostname=OTHER_HOST)
    assert not owner_current(db)
    assert not contender(db).acquire().acquired
    write_lease(db, pid=os.getpid(), hostname=socket.gethostname())
    assert owner_current(db)
