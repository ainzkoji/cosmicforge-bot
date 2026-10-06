"""Process-level liveness for the canonical trading runtime.

uvicorn answering requests proves the web server is alive. It proves nothing
about the trading loop, which lives in background tasks of the same process and
can stop while every endpoint keeps returning 200:

* the ownership loop returns (lease refused, or an exception escaping it) and
  nothing renews the heartbeat, so every trading gate reads "not the owner";
* the lease was never acquired, and the process settles into serving the API
  with no scheduler, indefinitely;
* the CATI production task ends, or a cycle blocks and never completes;
* the market-data collector stops enrolling hourly decisions.

Under a service manager each of these is the worst kind of outage: the unit is
"active (running)", nothing restarts it, and no order is ever sent.

The supervisor runs on its OWN THREAD -- an asyncio task could not notice a
blocked event loop -- and asks, every few seconds, whether the runtime is
actually advancing. A fault that persists is fatal: new entries are stopped,
the lease is released when it still can be, and the process exits non-zero so
the service manager starts a fresh one. Recovery after that restart is the
ordinary, already-proven path: stale-lease takeover, durable intents read back
from the broker, positions and protection rediscovered.

Only INTERNAL stalls are fatal. A broker or network outage is not: the loop
keeps cycling, reads fail closed, and restarting would fix nothing. Those are
reported as warnings and the runtime resumes by itself when the venue returns.

With systemd (``Type=notify`` + ``WatchdogSec``) the same thread reports
READY once the lease is held and the production task runs, and sends the
keep-alive only while healthy -- so a process frozen too hard to exit by itself
is killed and restarted from outside as well.
"""
from __future__ import annotations

import logging
import os
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

logger = logging.getLogger(__name__)

#: sysexits EX_SOFTWARE. Any non-zero status makes ``Restart=always`` (and the
#: Windows launcher) start a replacement; this one says the runtime chose to go.
EXIT_CODE = 70

STARTING = "STARTING"
HEALTHY = "HEALTHY"
FAILING = "FAILING"
STOPPING = "STOPPING"

RUNNER_LOOP_EXITED = "RUNNER_LOOP_EXITED"
OWNERSHIP_NOT_ACQUIRED = "RUNTIME_OWNERSHIP_NOT_ACQUIRED"
OWNERSHIP_LOST = "RUNTIME_OWNERSHIP_LOST"
LEASE_HEARTBEAT_STALE = "LEASE_HEARTBEAT_STALE"
PRODUCTION_TASK_NOT_RUNNING = "CATI_PRODUCTION_TASK_NOT_RUNNING"
PRODUCTION_CYCLE_STALLED = "CATI_PRODUCTION_CYCLE_STALLED"
COLLECTION_STALLED = "MARKET_DATA_COLLECTION_STALLED"
DECISION_STALLED = "CATI_DECISION_STALLED"

#: Faults read from the collector's own records. After the host was suspended,
#: or the venue unreachable for hours, they stay true until the collector has
#: caught up and enrolled the skipped hours -- a minute or more of real work.
#: They are given longer before the runtime is judged stuck.
SLOW_FAULTS = frozenset({COLLECTION_STALLED, DECISION_STALLED})

#: Below this much free space on the database volume the runtime says so. The
#: evidence tables are append-only and a full disk stops every write, including
#: the order intent that must precede a broker CREATE.
DISK_LOW_BYTES = 2 * 1024 ** 3


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, "") or default)
    except ValueError:
        return default


@dataclass(frozen=True)
class Limits:
    #: How often the runtime is examined.
    interval_seconds: float = 15.0
    #: Time allowed to import, take the lease and start the production task.
    startup_grace_seconds: float = 180.0
    #: Consecutive failing examinations before a fault is acted on. A resume
    #: from suspend or one slow cycle recovers well inside this.
    confirmations: int = 4
    #: The same, for faults that clear only once the collector has caught up.
    slow_confirmations: int = 20
    #: The lease is renewed every ~10 s and is taken over by others at 90 s.
    lease_heartbeat_stale_seconds: float = 150.0
    #: A production cycle is ~30 s; broker timeouts stretch one to a few minutes.
    cycle_stall_seconds: float = 600.0
    #: The collector heartbeats every minute, except while it walks the whole
    #: universe at the top of the hour or waits out unreachable hosts.
    collector_stall_seconds: float = 3600.0
    #: One decision is enrolled per hour, including explicit skips.
    decision_stall_seconds: float = 9000.0
    #: Collector evidence is not judged until it has had time to catch up.
    collector_grace_seconds: float = 900.0
    #: Longest a failing runtime may spend stopping cleanly before it is ended.
    shutdown_grace_seconds: float = 45.0
    #: An API-only process that never takes the lease is a fault by default.
    require_ownership: bool = True

    @classmethod
    def from_env(cls) -> "Limits":
        base = cls()
        return cls(
            interval_seconds=_env_float("RUNTIME_SUPERVISOR_INTERVAL_SECONDS", base.interval_seconds),
            startup_grace_seconds=_env_float("RUNTIME_SUPERVISOR_STARTUP_GRACE_SECONDS", base.startup_grace_seconds),
            confirmations=max(1, int(_env_float("RUNTIME_SUPERVISOR_CONFIRMATIONS", base.confirmations))),
            slow_confirmations=max(1, int(_env_float(
                "RUNTIME_SUPERVISOR_SLOW_CONFIRMATIONS", base.slow_confirmations))),
            lease_heartbeat_stale_seconds=_env_float(
                "RUNTIME_SUPERVISOR_LEASE_STALE_SECONDS", base.lease_heartbeat_stale_seconds),
            cycle_stall_seconds=_env_float("RUNTIME_SUPERVISOR_CYCLE_STALL_SECONDS", base.cycle_stall_seconds),
            collector_stall_seconds=_env_float(
                "RUNTIME_SUPERVISOR_COLLECTOR_STALL_SECONDS", base.collector_stall_seconds),
            decision_stall_seconds=_env_float(
                "RUNTIME_SUPERVISOR_DECISION_STALL_SECONDS", base.decision_stall_seconds),
            collector_grace_seconds=_env_float(
                "RUNTIME_SUPERVISOR_COLLECTOR_GRACE_SECONDS", base.collector_grace_seconds),
            shutdown_grace_seconds=_env_float(
                "RUNTIME_SUPERVISOR_SHUTDOWN_GRACE_SECONDS", base.shutdown_grace_seconds),
            require_ownership=os.environ.get("RUNTIME_SUPERVISOR_REQUIRE_OWNERSHIP", "true").strip().lower()
            not in ("0", "false", "no", "off"),
        )


@dataclass(frozen=True)
class Assessment:
    state: str
    faults: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    @property
    def healthy(self) -> bool:
        return self.state == HEALTHY


def assess(obs: Mapping[str, Any], limits: Limits) -> Assessment:
    """Classify one observation. Pure: what is unknown is never a fault."""
    if obs.get("stopping"):
        return Assessment(STOPPING)

    uptime = float(obs.get("uptime") or 0.0)
    started = uptime > limits.startup_grace_seconds
    faults: list[str] = []
    warnings: list[str] = [f"OBSERVATION_FAILED:{e}" for e in obs.get("errors") or ()]

    if obs.get("runner_task_alive") is False:
        faults.append(RUNNER_LOOP_EXITED)

    owns = bool(obs.get("owns_runtime"))
    if not owns:
        if started and limits.require_ownership and RUNNER_LOOP_EXITED not in faults:
            faults.append(OWNERSHIP_NOT_ACQUIRED)
    else:
        lease = obs.get("lease")
        if lease is not None:
            age = lease.get("heartbeat_age")
            if not lease.get("held") or not lease.get("ours"):
                faults.append(OWNERSHIP_LOST)
            elif age is not None and age > limits.lease_heartbeat_stale_seconds:
                faults.append(LEASE_HEARTBEAT_STALE)

        task_alive = obs.get("production_task_alive")
        if started and task_alive is not True:
            faults.append(PRODUCTION_TASK_NOT_RUNNING)
        cycle = obs.get("cycle") or {}
        running = cycle.get("loop_running_seconds")
        completed = cycle.get("last_cycle_completed_age_seconds")
        if task_alive and running is not None and running > limits.cycle_stall_seconds and (
                completed is None or completed > limits.cycle_stall_seconds):
            faults.append(PRODUCTION_CYCLE_STALLED)
        if cycle.get("last_error"):
            warnings.append(f"LAST_CYCLE_ERROR:{cycle['last_error']}")

        tracker = obs.get("tracker")
        if tracker is not None and uptime > max(limits.startup_grace_seconds, limits.collector_grace_seconds):
            heartbeat = tracker.get("heartbeat_age")
            decision = tracker.get("last_decision_age")
            if heartbeat is not None and heartbeat > limits.collector_stall_seconds:
                faults.append(COLLECTION_STALLED)
            if decision is not None and decision > limits.decision_stall_seconds:
                faults.append(DECISION_STALLED)
        if tracker is not None and (tracker.get("heartbeat_age") or 0) > 300:
            warnings.append("MARKET_DATA_COLLECTION_DELAYED")
        if obs.get("lease_renew_failures"):
            warnings.append(f"LEASE_RENEW_FAILING:{obs['lease_renew_failures']}")

    free = (obs.get("database") or {}).get("disk_free_bytes")
    if free is not None and free < DISK_LOW_BYTES:
        warnings.append("DATABASE_DISK_LOW")

    if faults:
        return Assessment(FAILING, tuple(faults), tuple(warnings))
    if not owns or obs.get("production_task_alive") is not True:
        return Assessment(STARTING, (), tuple(warnings))
    return Assessment(HEALTHY, (), tuple(warnings))


# ── systemd notification ────────────────────────────────────────────────────


def sd_notify(message: str) -> bool:
    """Send one state line to the service manager. A no-op without systemd."""
    address = os.environ.get("NOTIFY_SOCKET")
    family = getattr(socket, "AF_UNIX", None)
    if not address or family is None:
        return False
    if address.startswith("@"):
        address = "\0" + address[1:]
    try:
        with socket.socket(family, socket.SOCK_DGRAM) as sock:
            sock.connect(address)
            sock.sendall(message.encode("utf-8"))
        return True
    except OSError as exc:
        logger.debug("[RUNTIME_SUPERVISOR] sd_notify failed: %s", exc)
        return False


# ── Observation ─────────────────────────────────────────────────────────────


def _iso_age(stamp: str | None) -> float | None:
    if not stamp:
        return None
    from datetime import datetime, timezone

    try:
        when = datetime.fromisoformat(stamp)
    except ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return round((datetime.now(timezone.utc) - when).total_seconds(), 1)


def tracker_state(db: Any) -> dict[str, Any] | None:
    """The collector's own heartbeat and the last hourly decision it enrolled."""
    from app.trading_intelligence.integration.residual_prospective import REGISTRY_HASH

    now_ms = time.time() * 1000
    with db.connect() as conn:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='cati_residual_tracker'").fetchone():
            return None
        row = conn.execute(
            "SELECT heartbeat_at,status,last_decision_time FROM cati_residual_tracker WHERE registry_hash=?",
            (REGISTRY_HASH,)).fetchone()
    if row is None:
        return None
    return {"status": row[1],
            "heartbeat_age": None if row[0] is None else round((now_ms - row[0]) / 1000, 1),
            "last_decision_time_ms": row[2],
            "last_decision_age": None if row[2] is None else round((now_ms - row[2]) / 1000, 1)}


def observe_runtime(started_monotonic: float) -> dict[str, Any]:
    """Read the live runtime. Each source that cannot be read is named, not guessed."""
    import app.main as main_module
    from app.ops.runtime_shutdown import stop_requested

    obs: dict[str, Any] = {"uptime": round(time.monotonic() - started_monotonic, 1),
                           "stopping": stop_requested(), "errors": []}
    service = getattr(main_module, "runner_service", None)
    task = getattr(service, "task", None)
    obs["runner_task_alive"] = None if task is None else not task.done()
    multi = getattr(service, "multi_runner", None)
    obs["owns_runtime"] = bool(getattr(multi, "owns_runtime", False))
    obs["ownership_reason"] = getattr(multi, "ownership_reason", None)
    obs["lease_renew_failures"] = getattr(getattr(multi, "_ownership", None), "renew_failures", 0)
    production = getattr(main_module.app.state, "cati_production_task", None)
    obs["production_task_alive"] = None if production is None else not production.done()
    try:
        from app.trading_intelligence.integration import production_runtime

        obs["cycle"] = production_runtime.progress()
    except Exception as exc:
        obs["errors"].append(f"cycle:{type(exc).__name__}")
    try:
        from shared_lib.persistence.db import DB

        from app.ops.runtime_ownership import current_owner, holder_is_alive

        db = DB()
        owner = current_owner(db, db.path)
        obs["lease"] = {
            "held": owner is not None,
            "ours": bool(owner and owner["pid"] == os.getpid()
                         and holder_is_alive(owner["pid"], owner["started_at"])),
            "heartbeat_age": _iso_age(owner["heartbeat_at"]) if owner else None,
            "runtime_session_id": owner["runtime_session_id"] if owner else None,
        }
        obs["tracker"] = tracker_state(db)
        import shutil

        usage = shutil.disk_usage(os.path.dirname(os.path.abspath(db.path)))
        obs["database"] = {"size_bytes": os.path.getsize(db.path), "disk_free_bytes": usage.free,
                           "disk_total_bytes": usage.total}
    except Exception as exc:
        obs["errors"].append(f"database:{type(exc).__name__}")
    return obs


# ── The supervisor ──────────────────────────────────────────────────────────


@dataclass
class RuntimeSupervisor:
    observe: Callable[[], Mapping[str, Any]]
    limits: Limits = field(default_factory=Limits)
    notify: Callable[[str], Any] = sd_notify
    exit_process: Callable[[int], Any] = os._exit
    quiesce: Callable[[str], Any] | None = None

    def __post_init__(self) -> None:
        self._failing = 0
        self._streaks: dict[str, int] = {}
        self._ready = False
        self._stopping_notified = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.last_observation: Mapping[str, Any] | None = None
        self.last_assessment: Assessment | None = None
        self.last_checked_at: float | None = None
        self.fatal: tuple[str, ...] | None = None

    def tick(self) -> Assessment:
        """Examine the runtime once and act on what is found."""
        try:
            obs = self.observe()
        except Exception as exc:  # an examiner that cannot look is not a verdict
            obs = {"uptime": 0.0, "errors": [f"observe:{type(exc).__name__}"]}
        verdict = assess(obs, self.limits)
        self.last_observation, self.last_assessment, self.last_checked_at = obs, verdict, time.time()

        if verdict.state == STOPPING:
            if not self._stopping_notified:
                self._stopping_notified = True
                self.notify("STOPPING=1")
            return verdict
        if verdict.faults:
            # Each fault is counted by itself: one that has only just appeared
            # does not inherit the patience already spent on another.
            self._streaks = {fault: self._streaks.get(fault, 0) + 1 for fault in verdict.faults}
            self._failing = max(self._streaks.values())
            logger.error("[RUNTIME_SUPERVISOR] faults=%s consecutive=%s", ",".join(verdict.faults), self._streaks)
            due = tuple(fault for fault, count in self._streaks.items() if count >= (
                self.limits.slow_confirmations if fault in SLOW_FAULTS else self.limits.confirmations))
            if due:
                self._fail(Assessment(FAILING, due, verdict.warnings))
            return verdict

        self._failing, self._streaks = 0, {}
        if verdict.state == HEALTHY and not self._ready:
            self._ready = True
            lease = obs.get("lease") or {}
            print(f"[RUNTIME_SUPERVISOR] READY lease_owner_pid={os.getpid()} "
                  f"runtime_session_id={lease.get('runtime_session_id')} "
                  f"uptime_s={obs.get('uptime')}", flush=True)
            self.notify("READY=1")
        self.notify(f"STATUS={verdict.state}" + (f" warnings={','.join(verdict.warnings)}" if verdict.warnings else ""))
        self.notify("WATCHDOG=1")
        return verdict

    def _fail(self, verdict: Assessment) -> None:
        self.fatal = verdict.faults
        reason = verdict.faults[0]
        print(f"[RUNTIME_SUPERVISOR] FATAL faults={','.join(verdict.faults)} -- stopping entries and exiting "
              f"{EXIT_CODE} so the service manager starts a fresh runtime", flush=True)
        logger.critical("[RUNTIME_SUPERVISOR] FATAL %s observation=%s", verdict.faults, self.last_observation)
        self.notify(f"STATUS=FAILING {','.join(verdict.faults)}")
        if self.quiesce is not None:
            # Bounded: a runtime too broken to stop cleanly is ended anyway, and
            # its lease is recovered by staleness on the next start.
            worker = threading.Thread(target=self._quiesce, args=(f"SUPERVISOR_{reason}",), daemon=True)
            worker.start()
            worker.join(self.limits.shutdown_grace_seconds)
        self.exit_process(EXIT_CODE)

    def _quiesce(self, reason: str) -> None:
        try:
            self.quiesce(reason)
        except Exception as exc:
            logger.error("[RUNTIME_SUPERVISOR] quiesce failed: %s", exc)

    def snapshot(self) -> dict[str, Any]:
        """Public liveness: no credentials, no account identifiers."""
        verdict, obs = self.last_assessment, dict(self.last_observation or {})
        lease, cycle, tracker = obs.get("lease") or {}, obs.get("cycle") or {}, obs.get("tracker") or {}
        return {
            "state": verdict.state if verdict else STARTING,
            "faults": list(verdict.faults) if verdict else [],
            "warnings": list(verdict.warnings) if verdict else [],
            "consecutive_failing_checks": self._failing,
            "ready": self._ready,
            "checked_age_seconds": None if self.last_checked_at is None else round(time.time() - self.last_checked_at, 1),
            "uptime_seconds": obs.get("uptime"),
            "lease": {"owned": bool(obs.get("owns_runtime")), "held_by_this_process": lease.get("ours"),
                      "heartbeat_age_seconds": lease.get("heartbeat_age"),
                      "renew_failures": obs.get("lease_renew_failures", 0)},
            "scheduler": {"runner_loop_alive": obs.get("runner_task_alive"),
                          "cati_production_task_alive": obs.get("production_task_alive"),
                          "last_cycle_completed_age_seconds": cycle.get("last_cycle_completed_age_seconds"),
                          "cycles_completed": cycle.get("cycles_completed"),
                          "cycle_in_flight": cycle.get("cycle_in_flight")},
            "market_data": {"collector_status": tracker.get("status"),
                            "collector_heartbeat_age_seconds": tracker.get("heartbeat_age")},
            "cati": {"last_decision_age_seconds": tracker.get("last_decision_age"),
                     "last_decision_time_ms": tracker.get("last_decision_time_ms")},
            "database": dict(obs.get("database") or {}),
        }

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="runtime-supervisor", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.wait(self.limits.interval_seconds):
            try:
                self.tick()
            except Exception as exc:
                logger.error("[RUNTIME_SUPERVISOR] check failed: %s", exc)


_supervisor: RuntimeSupervisor | None = None


def get_supervisor() -> RuntimeSupervisor | None:
    return _supervisor


def enabled() -> bool:
    """Only a serving production runtime is supervised: never a test or a tool."""
    from app.core.config import settings
    from app.ops.runtime_preflight import should_skip

    if should_skip() or not settings.production:
        return False
    return os.environ.get("RUNTIME_SUPERVISOR_ENABLED", "true").strip().lower() not in ("0", "false", "no", "off")


def start() -> RuntimeSupervisor | None:
    """Start supervising this process. Idempotent."""
    global _supervisor
    if _supervisor is not None or not enabled():
        return _supervisor
    from app.ops.runtime_shutdown import quiesce

    started = time.monotonic()
    limits = Limits.from_env()
    _supervisor = RuntimeSupervisor(observe=lambda: observe_runtime(started), limits=limits,
                                    quiesce=lambda reason: quiesce(reason=reason))
    _supervisor.start()
    print(f"[RUNTIME_SUPERVISOR] started interval_s={limits.interval_seconds:g} "
          f"startup_grace_s={limits.startup_grace_seconds:g} confirmations={limits.confirmations} "
          f"systemd_notify={'yes' if os.environ.get('NOTIFY_SOCKET') else 'no'}", flush=True)
    return _supervisor


def reset_for_tests() -> None:
    global _supervisor
    if _supervisor is not None:
        _supervisor.stop()
    _supervisor = None
