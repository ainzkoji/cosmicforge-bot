"""Decide whether this process may become the canonical trading runtime.

The defect this exists for is in uvicorn's own sequencing. ``Server.startup()``
awaits ``lifespan.startup()`` and only afterwards creates the socket:

    await self.startup(sockets=sockets)
      -> await self.lifespan.startup()      # the whole app initialises here
      -> ...                                # and only then:
      -> "Standard case. Create a socket from a host/port pair."

So a second ``python -m uvicorn app.main:app --port 9000`` ran the entire
trading startup -- canonical runtime session, signal scheduler, MultiBotRunner,
bot restore -- discovered the lease was held, and *then* died on

    [Errno 10048] only one usage of each socket address is normally permitted

leaving evidence behind. The runtime database has 48 sessions marked RUNNING
against one live process, several of them one minute apart, which is what that
looks like accumulated over time.

The answer is to decide before any of it. This module is called at **import**
of ``app.main``, which uvicorn does during ``config.load()`` -- before
``_serve`` and therefore before lifespan startup. A refusal here costs nothing
and writes nothing.

Two independent authorities, both required:

* the **lease**, scoped to the canonical database, which is what actually
  guarantees one scheduler;
* the **port**, which is only a network bind check but catches the case where
  the lease has not been taken yet.

Neither subsumes the other. A second backend on a different port against the
same database is still a duplicate scheduler; a foreign process on 9000 is not
a scheduler at all but still blocks the bind.
"""
from __future__ import annotations

import logging
import os
import socket
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_PORT = 9000

#: Set by conftest and by any tool that imports the app without serving.
TEST_MODE_ENV = "COSMICFORGE_TEST_MODE"
#: Explicit escape hatch for tooling that must import app.main deliberately.
SKIP_ENV = "COSMICFORGE_SKIP_RUNTIME_PREFLIGHT"


class PreflightStatus(str, Enum):
    """Why this process may or may not become the runtime."""

    READY = "READY"
    ALREADY_RUNNING = "ALREADY_RUNNING"
    STALE_LEASE = "STALE_LEASE"
    PORT_OCCUPIED_BY_CANONICAL_RUNTIME = "PORT_OCCUPIED_BY_CANONICAL_RUNTIME"
    PORT_OCCUPIED_BY_OTHER_PROCESS = "PORT_OCCUPIED_BY_OTHER_PROCESS"
    OWNERSHIP_CONFLICT = "OWNERSHIP_CONFLICT"
    ERROR = "ERROR"


#: Statuses that permit this process to continue into trading startup.
MAY_START = frozenset({PreflightStatus.READY, PreflightStatus.STALE_LEASE})


@dataclass(frozen=True)
class PreflightResult:
    status: PreflightStatus
    message: str

    database_path: str | None = None
    database_role: str | None = None

    lease_pid: int | None = None
    lease_hostname: str | None = None
    lease_heartbeat_at: str | None = None
    lease_heartbeat_age_seconds: float | None = None
    lease_session_id: str | None = None
    lease_stale: bool | None = None
    lease_owner_alive: bool | None = None

    port: int = DEFAULT_PORT
    port_pid: int | None = None
    port_command: str | None = None

    code_revision: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)

    @property
    def may_start(self) -> bool:
        return self.status in MAY_START

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "may_start": self.may_start,
            "message": self.message,
            "database_path": self.database_path,
            "database_role": self.database_role,
            "lease_pid": self.lease_pid,
            "lease_hostname": self.lease_hostname,
            "lease_heartbeat_at": self.lease_heartbeat_at,
            "lease_heartbeat_age_seconds": self.lease_heartbeat_age_seconds,
            "lease_session_id": self.lease_session_id,
            "lease_stale": self.lease_stale,
            "lease_owner_alive": self.lease_owner_alive,
            "port": self.port,
            "port_pid": self.port_pid,
            "port_command": self.port_command,
            "code_revision": self.code_revision,
            **({"detail": self.detail} if self.detail else {}),
        }


# ── Port inspection ─────────────────────────────────────────────────────────


def port_listener_pid(port: int) -> int | None:
    """PID listening on ``port``, or None. Never raises."""
    try:
        import psutil

        for conn in psutil.net_connections(kind="tcp"):
            if (
                conn.status == psutil.CONN_LISTEN
                and conn.laddr
                and conn.laddr.port == port
            ):
                return conn.pid
    except Exception as exc:  # psutil missing, or access denied
        logger.debug("[PREFLIGHT] could not enumerate listeners: %s", exc)
    return None


def port_is_free(port: int, host: str = "0.0.0.0") -> bool:
    """Bind test. Authoritative for "can I listen", unlike a PID lookup.

    Deliberately does not set SO_REUSEADDR: the question is whether this
    process could actually bind, and on Windows a reuse flag would answer a
    different question.
    """
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind((host, port))
        return True
    except OSError:
        return False
    finally:
        probe.close()


def process_command(pid: int | None) -> str | None:
    if not pid:
        return None
    try:
        import psutil

        return " ".join(psutil.Process(pid).cmdline())
    except Exception:
        return None


# ── The decision ────────────────────────────────────────────────────────────


def preflight(
    *,
    port: int = DEFAULT_PORT,
    db: Any = None,
    host: str = "0.0.0.0",
) -> PreflightResult:
    """Classify the runtime situation. Read-only; acquires nothing."""
    try:
        from app.core.config import settings
        from app.ops.runtime_ownership import (
            TRADING_SCHEDULER,
            current_owner,
            lease_is_stale,
            pid_is_alive,
        )
        from shared_lib.persistence.db import DB

        database = db if db is not None else DB()
        database_path = database.path
        database_role = str(getattr(settings, "DATABASE_ROLE", "development"))

        # current_owner() returns None on *any* error, which is the right
        # failure mode for acquiring ("assume you are not the owner") and the
        # wrong one here: an unreadable database would read as "no lease held"
        # and permit a second scheduler. Probe first so a broken database
        # becomes ERROR below instead of a silent green light.
        with database.connect() as conn:
            conn.execute("SELECT 1").fetchone()

        owner = current_owner(database, database_path, TRADING_SCHEDULER)
        free = port_is_free(port, host)
        listener_pid = None if free else port_listener_pid(port)
        listener_cmd = process_command(listener_pid)

        base = dict(
            database_path=database_path,
            database_role=database_role,
            port=port,
            port_pid=listener_pid,
            port_command=listener_cmd,
        )

        # ── No lease held ───────────────────────────────────────────────────
        if owner is None:
            if free:
                return PreflightResult(
                    status=PreflightStatus.READY,
                    message="No runtime lease is held and the port is free.",
                    **base,
                )
            # CASE D: something is on the port but owns no trading lease.
            # Fail closed. It may be an unrelated service, or a runtime that
            # has not taken the lease yet; either way this process must not
            # assume it can take over.
            return PreflightResult(
                status=PreflightStatus.PORT_OCCUPIED_BY_OTHER_PROCESS,
                message=(
                    f"Port {port} is in use by pid {listener_pid} but no process "
                    f"holds the trading lease for this database. Refusing to "
                    f"start rather than guess what that process is."
                ),
                **base,
            )

        lease_pid = int(owner.get("pid") or 0) or None
        heartbeat = owner.get("heartbeat_at")
        stale = lease_is_stale(heartbeat)
        alive = pid_is_alive(lease_pid) if lease_pid else False
        age = _age_seconds(heartbeat)
        revision = _session_revision(database, owner.get("runtime_session_id"))

        base.update(
            lease_pid=lease_pid,
            lease_hostname=owner.get("hostname"),
            lease_heartbeat_at=heartbeat,
            lease_heartbeat_age_seconds=age,
            lease_session_id=owner.get("runtime_session_id"),
            lease_stale=stale,
            lease_owner_alive=alive,
            code_revision=revision,
        )

        # ── Lease held by a dead process, or gone stale ─────────────────────
        if not alive or stale:
            # CASE C. Takeover is legitimate, and the existing lease logic
            # performs it with its own checks; preflight only says "proceed".
            if free:
                return PreflightResult(
                    status=PreflightStatus.STALE_LEASE,
                    message=(
                        f"The lease is held by pid {lease_pid}, which is "
                        f"{'not running' if not alive else 'stale'}"
                        f"{f' (heartbeat {age:.0f}s old)' if age is not None else ''}. "
                        f"It will be taken over."
                    ),
                    **base,
                )
            # A dead lease holder but an occupied port means something else is
            # listening. Do not take over blind.
            return PreflightResult(
                status=PreflightStatus.OWNERSHIP_CONFLICT,
                message=(
                    f"The lease is stale (pid {lease_pid}) but port {port} is "
                    f"held by pid {listener_pid}. These disagree; operator "
                    f"intervention is needed."
                ),
                **base,
            )

        # ── Lease held by a live process ────────────────────────────────────
        if not free and listener_pid is not None and listener_pid != lease_pid:
            # CASE E: port owned by A, lease owned by B, both alive.
            return PreflightResult(
                status=PreflightStatus.OWNERSHIP_CONFLICT,
                message=(
                    f"Port {port} is held by pid {listener_pid} but the trading "
                    f"lease is held by pid {lease_pid}. Two different processes "
                    f"claim the runtime; refusing to start."
                ),
                **base,
            )

        if not free:
            # CASE B: the healthy canonical runtime, listening and holding the
            # lease. The common case, and the one the operator sees most.
            return PreflightResult(
                status=PreflightStatus.ALREADY_RUNNING,
                message="The canonical trading runtime is already active.",
                **base,
            )

        # CASE F: port free, but a live process holds the lease. It may be
        # serving its API on another port. The lease is scoped to the database,
        # and the database is what must have one scheduler.
        return PreflightResult(
            status=PreflightStatus.ALREADY_RUNNING,
            message=(
                f"Port {port} is free, but pid {lease_pid} holds the trading "
                f"lease for this database and is alive. A second scheduler "
                f"against one database is what the lease exists to prevent."
            ),
            **base,
        )

    except Exception as exc:
        logger.error("[PREFLIGHT] failed: %s", exc)
        return PreflightResult(
            status=PreflightStatus.ERROR,
            message=f"Runtime preflight failed: {type(exc).__name__}: {exc}",
            port=port,
        )


def _age_seconds(iso: str | None) -> float | None:
    if not iso:
        return None
    try:
        when = datetime.fromisoformat(iso)
    except Exception:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return round((datetime.now(timezone.utc) - when).total_seconds(), 1)


def _session_revision(db: Any, session_id: str | None) -> str | None:
    if not session_id:
        return None
    try:
        with db.connect() as conn:
            row = conn.execute(
                "SELECT code_revision FROM runtime_sessions WHERE runtime_session_id=?",
                (session_id,),
            ).fetchone()
        return (row[0] if row else None) or None
    except Exception:
        return None


# ── Enforcement ─────────────────────────────────────────────────────────────


def should_skip() -> bool:
    """True when this import is not an attempt to serve.

    Tests import ``app.main`` constantly and must never be refused, and neither
    must a diagnostic script. Both are marked explicitly rather than guessed at
    from the process name.
    """
    return bool(os.environ.get(TEST_MODE_ENV) or os.environ.get(SKIP_ENV))


def render_refusal(result: PreflightResult) -> str:
    """The message an operator sees instead of a stack trace."""
    lines = [
        "",
        "[COSMICFORGE_RUNTIME]",
        result.message,
        "",
    ]
    for label, value in (
        ("pid", result.lease_pid),
        ("port", result.port),
        ("port_pid", result.port_pid),
        ("runtime_session_id", result.lease_session_id),
        ("lease_status", _lease_word(result)),
        ("lease_heartbeat_age_s", result.lease_heartbeat_age_seconds),
        ("code_revision", result.code_revision),
        ("database", result.database_path),
        ("database_role", result.database_role),
    ):
        if value is not None:
            lines.append(f"  {label}={value}")
    lines += [
        "",
        "  No second scheduler was started.",
        "  No MultiBotRunner was created.",
        "  No runtime evidence was written.",
        "",
        "  Use the running runtime, or stop it first:",
        "    .\\scripts\\trading_runtime.ps1 status",
        "    .\\scripts\\trading_runtime.ps1 stop",
        "",
    ]
    return "\n".join(lines)


def _lease_word(result: PreflightResult) -> str | None:
    if result.lease_pid is None:
        return None
    if result.lease_stale:
        return "STALE"
    if result.lease_owner_alive is False:
        return "OWNER_GONE"
    return "HEALTHY"


def enforce(
    result: PreflightResult | None = None,
    *,
    port: int = DEFAULT_PORT,
    stream: Any = None,
) -> PreflightResult:
    """Refuse to continue when another runtime already owns this database.

    Exits the process rather than raising into uvicorn's importer, so the
    operator gets the message above instead of a traceback followed -- much
    later -- by Errno 10048.
    """
    if should_skip():
        return PreflightResult(
            status=PreflightStatus.READY,
            message="Preflight skipped: not a serving process.",
            port=port,
        )

    result = result or preflight(port=port)
    if result.may_start:
        logger.info(
            "[PREFLIGHT] %s -- %s", result.status.value, result.message,
        )
        return result

    # Anything already buffered on stdout would otherwise interleave with the
    # refusal and make it look like startup continued.
    try:
        sys.stdout.flush()
    except Exception:
        pass
    print(render_refusal(result), file=stream or sys.stderr, flush=True)
    raise SystemExit(1)
