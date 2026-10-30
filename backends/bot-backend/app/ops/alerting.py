"""Production alerting for the trading platform (Section H Step 8.5).

The engine itself (categories, severities, routing, dedup, escalation, acknowledge/resolve, the append-only event
log, delivery with retries) is ``shared_lib.operations.alerting`` -- shared so the user-backend admin API acts on
the same alerts. This module adds what only the trading platform has:

* **evaluators** that turn available signals into alerts (and resolve them when the signal clears):
  ``JobQueue.backlog()`` (overdue jobs, dead letters), ``platform_workers`` heartbeat staleness, account
  assignments that expired without a takeover, ``Database.health()``, disk usage and growth of a path, and the
  age of the newest verified backup manifest;
* **sinks**: ``NotifyChannelsSink`` delivers through the existing ``scripts/notify_failure.py`` channels
  (webhook / Telegram / e-mail from ``/etc/cosmicforge/alerts.env``); ``JobQueueSink`` enqueues a NOTIFICATION job
  on the platform ``JobQueue`` (idempotent per delivery) so a worker delivers it with the queue's own retries and
  dead-lettering; ``notification_job_handler`` is that worker-side handler;
* ``run_cycle`` -- evaluate, escalate, deliver -- for a periodic operations job;
* ``run_tick`` / ``main`` -- that job, owned by exactly one process at a time through ``TickLease`` (platform
  database lease with fencing), either one tick per call (systemd timer: ``cosmicforge-ops-tick.timer``) or as a
  long-running loop (``--loop --interval 60``, the container topology).

The producers over the legacy runtime database (unknown order outcomes, unprotected positions, reconciliation,
kill switches, emergency flattens, risk brakes, connectivity, market data, instrument rules, certifications,
billing, authentication, latency) are in ``app.ops.alert_producers``.

Every evaluator is pure with respect to time when ``now_ms`` is given, so tests inject the clock; otherwise
time is the database server's.
"""
from __future__ import annotations

import importlib.util
import json
import logging
import os
import shutil
import signal
import socket
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

from shared_lib.operations.alerting import (  # noqa: F401  (re-exported API)
    ACKNOWLEDGED, CATEGORIES, CRITICAL, DEFAULT_DEDUP_WINDOW_MS, ESCALATION_MINUTES, HIGH, LOW, MEDIUM, OPEN, RESOLVED,
    SEVERITIES, AlertEngine, AlertError, AlertMessage, AlertSink, CallableSink, CategoryPolicy, RaiseResult,
    fingerprint, pseudonym)
from shared_lib.operations.oncall import OPERATIONS_OWNER_REQUIRED, ROTATIONS, OncallDirectory  # noqa: F401
from shared_lib.operations.tick_lease import TICK_ERROR, TICK_OK, TickLease
from shared_lib.persistence.sql import Database

from app.platform_workers.jobs import NOTIFICATION, Job, JobQueue, PermanentJobError

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[4]
NOTIFY_SCRIPT = REPO_ROOT / "scripts" / "notify_failure.py"
DEFAULT_ALERTS_ENV = "/etc/cosmicforge/alerts.env"
BACKUP_NAME_SUFFIXES = (".db.json", ".db.gz.json")


# ── sinks ───────────────────────────────────────────────────────────────────────────────────────────────
def _load_notify_module():
    spec = importlib.util.spec_from_file_location("cosmicforge_notify_failure", NOTIFY_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class NotifyChannelsSink:
    """The notify_failure.py channels. A send with no channel configured, or with any channel failing, raises --
    the engine retries it and, after every retry, raises ALERT_DELIVERY_FAILURE."""

    name = "notify_channels"

    def __init__(self, *, env_file: Optional[str] = DEFAULT_ALERTS_ENV, environ: Optional[Mapping[str, str]] = None,
                 module=None) -> None:
        self._module = module or _load_notify_module()
        self._config = self._module.load_config(env_file, os.environ if environ is None else environ)

    def configured(self) -> bool:
        return bool(self._module.configured_channels(self._config))

    def send_text(self, subject: str, text: str) -> None:
        if not self.configured():
            raise RuntimeError("NO_ALERT_CHANNEL_CONFIGURED")
        delivered, failed = self._module.deliver(self._config, subject, text)
        if failed or not delivered:
            raise RuntimeError(f"ALERT_CHANNELS_FAILED:{','.join(failed) or 'none_delivered'}")

    def send(self, message: AlertMessage) -> None:
        self.send_text(message.subject(), message.text())


class JobQueueSink:
    """Hands the delivery to the platform JobQueue (category NOTIFICATION). Enqueue is idempotent per delivery
    id, so a retried engine delivery never produces two jobs."""

    name = "notification_job"

    def __init__(self, queue: JobQueue, *, max_attempts: int = 8) -> None:
        self.queue = queue
        self.max_attempts = max_attempts

    def send(self, message: AlertMessage) -> None:
        self.queue.enqueue(NOTIFICATION, f"ops-alert-delivery:{message.delivery_id}",
                           {"kind": "ops_alert", "alert_id": message.alert_id, "delivery_id": message.delivery_id,
                            "severity": message.severity, "subject": message.subject(), "text": message.text()},
                           priority=10 if message.severity == CRITICAL else 0, max_attempts=self.max_attempts)


def notification_job_handler(channels: Any) -> Callable[[Job], Dict[str, Any]]:
    """JobProcessor handler for NOTIFICATION jobs carrying an ops alert. ``channels`` has ``send_text``.
    A failure raises, so the queue retries with backoff and dead-letters after ``max_attempts``."""
    def handle(job: Job) -> Dict[str, Any]:
        if job.payload.get("kind") != "ops_alert":
            raise PermanentJobError(f"UNSUPPORTED_NOTIFICATION_KIND:{job.payload.get('kind')}")
        channels.send_text(str(job.payload["subject"]), str(job.payload["text"]))
        return {"delivered": True, "alert_id": job.payload.get("alert_id")}
    return handle


def default_sinks(db: Database, *, environ: Optional[Mapping[str, str]] = None) -> List[AlertSink]:
    """Production wiring: the queue sink when the platform queue exists, plus the direct channels when configured
    (the direct path still reaches a human if the workers that drain the queue are down)."""
    sinks: List[AlertSink] = [JobQueueSink(JobQueue(db))]
    try:
        direct = NotifyChannelsSink(environ=environ)
        if direct.configured():
            sinks.append(direct)
    except Exception as exc:  # noqa: BLE001
        logger.warning("notify channels unavailable: %s", exc)
    return sinks


# ── evaluators ──────────────────────────────────────────────────────────────────────────────────────────
@dataclass
class Evaluation:
    category: str
    key: Any
    firing: bool
    title: str = ""
    detail: Dict[str, Any] = field(default_factory=dict)
    severity: Optional[str] = None
    result: Optional[RaiseResult] = None
    resolved: bool = False


def _apply(engine: AlertEngine, ev: Evaluation, source: str, now_ms: Optional[int]) -> Evaluation:
    if ev.firing:
        ev.result = engine.raise_alert(ev.category, key=ev.key, title=ev.title, source=source, detail=ev.detail,
                                       severity=ev.severity, now_ms=now_ms)
    else:
        ev.resolved = engine.resolve_condition(ev.category, key=ev.key, actor=f"evaluator:{source}",
                                               note="condition cleared", now_ms=now_ms)
    return ev


def _db_now(db: Database, now_ms: Optional[int]) -> int:
    if now_ms is not None:
        return int(now_ms)
    with db.transaction(write=False) as conn:
        return conn.now_ms()


def evaluate_queue_backlog(engine: AlertEngine, queue: JobQueue, *, max_due_age_ms: int = 5 * 60_000,
                           now_ms: Optional[int] = None) -> List[Evaluation]:
    backlog = queue.backlog()
    age, dead = int(backlog["oldest_due_age_ms"]), int(backlog["dead"])
    overdue = Evaluation("QUEUE_BACKLOG", "queue:overdue", age > max_due_age_ms,
                         f"Oldest due job has waited {age // 1000}s (bound {max_due_age_ms // 1000}s)",
                         {"oldest_due_age_ms": age, "bound_ms": max_due_age_ms, "by_category": backlog["by_category"]})
    dead_ev = Evaluation("QUEUE_BACKLOG", "queue:dead_letters", dead > 0, f"{dead} job(s) in the dead-letter state",
                         {"dead": dead, "by_category": backlog["by_category"]})
    return [_apply(engine, overdue, "queue_backlog", now_ms), _apply(engine, dead_ev, "queue_backlog", now_ms)]


def evaluate_worker_heartbeats(engine: AlertEngine, db: Database, *, stale_ms: int = 90_000,
                               now_ms: Optional[int] = None) -> List[Evaluation]:
    now = _db_now(db, now_ms)
    with db.transaction(write=False) as conn:
        rows = conn.all("SELECT worker_id, role, heartbeat_at, state FROM platform_workers")
    out = []
    for worker_id, role, heartbeat_at, state in rows:
        age = now - int(heartbeat_at)
        firing = state == "ACTIVE" and age > stale_ms
        out.append(_apply(engine, Evaluation("WORKER_OUTAGE", {"worker_id": worker_id}, firing,
                                             f"Worker ({role}) has not heartbeated for {age // 1000}s",
                                             {"worker_id": worker_id, "role": role, "heartbeat_age_ms": age,
                                              "bound_ms": stale_ms}), "worker_heartbeats", now_ms))
    return out


def evaluate_lease_takeovers(engine: AlertEngine, db: Database, *, grace_ms: int = 60_000,
                             now_ms: Optional[int] = None) -> Evaluation:
    """ACTIVE accounts nobody owns: never claimed, released, or expired -- for longer than ``grace_ms``."""
    now = _db_now(db, now_ms)
    cutoff = now - grace_ms
    with db.transaction(write=False) as conn:
        rows = conn.all(
            "SELECT a.account_id, s.worker_id, s.expires_at, s.released_at FROM platform_accounts a "
            "LEFT JOIN platform_account_assignments s ON s.account_id = a.account_id WHERE a.state='ACTIVE' AND ("
            "(s.account_id IS NULL AND a.created_at <= ?) OR (s.released_at IS NOT NULL AND s.released_at <= ?) "
            "OR (s.released_at IS NULL AND s.expires_at <= ?)) ORDER BY a.account_id", (cutoff, cutoff, cutoff))
    orphans = [r[0] for r in rows]
    ev = Evaluation("LEASE_TAKEOVER_FAILURE", "assignments:orphaned", bool(orphans),
                    f"{len(orphans)} active account(s) have no owning worker beyond the {grace_ms // 1000}s grace",
                    {"orphaned_accounts": len(orphans), "accounts": [pseudonym(a) for a in orphans[:50]],
                     "grace_ms": grace_ms})
    return _apply(engine, ev, "lease_takeovers", now_ms)


def evaluate_database(engine: AlertEngine, target: Database, *, name: str = "platform",
                      now_ms: Optional[int] = None) -> Evaluation:
    """``Database.health()`` of ``target``. When the target IS the alert store and it is down, the alert cannot
    be stored: it is sent straight to the engine's sinks instead (best effort) so a human still hears of it."""
    health = target.health()
    ev = Evaluation("DATABASE_UNAVAILABLE", {"database": name}, not health.get("ready", False),
                    f"Database '{name}' is not ready", {"database": name, "dialect": health.get("dialect"),
                                                        "error": health.get("error")})
    try:
        return _apply(engine, ev, "database_health", now_ms)
    except Exception as exc:  # noqa: BLE001 -- the store itself is unreachable
        if not ev.firing:
            raise
        message = AlertMessage(alert_id="unstored", category=ev.category, severity=CRITICAL, title=ev.title,
                               detail={**ev.detail, "store_error": f"{type(exc).__name__}"}, reason="RAISED_UNSTORED",
                               escalation_level=0, rotation_key=CATEGORIES[ev.category].rotation_key,
                               owner_required=True, targets=[], delivery_id=f"unstored-{int(time.time())}",
                               occurrences=1)
        for sink in engine.sinks:
            try:
                sink.send(message)
            except Exception as send_exc:  # noqa: BLE001
                logger.error("direct alert delivery via %s failed: %s", sink.name, send_exc)
        ev.detail["delivered_unstored"] = True
        return ev


@dataclass
class DiskSample:
    at_ms: int
    bytes_used: int


def path_size(path: Path) -> int:
    path = Path(path)
    if path.is_file():
        return path.stat().st_size
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += (Path(root) / f).stat().st_size
            except OSError:
                pass
    return total


def evaluate_disk_growth(engine: AlertEngine, path: Path, *, state: Dict[str, DiskSample],
                         max_used_fraction: float = 0.85, max_growth_bytes_per_hour: int = 2 * 1024 ** 3,
                         now_ms: Optional[int] = None, usage: Optional[Callable[[Path], Any]] = None,
                         size_of: Callable[[Path], int] = path_size) -> List[Evaluation]:
    """Volume fill level and growth rate of ``path`` (a data directory or database file). ``state`` keeps the
    previous sample between calls (the caller owns it: a dict kept by the periodic job)."""
    path = Path(path)
    now = int(now_ms if now_ms is not None else time.time() * 1000)
    du = (usage or shutil.disk_usage)(path if path.is_dir() else path.parent)
    fraction = du.used / du.total if du.total else 0.0
    size = size_of(path)
    key = str(path)
    previous = state.get(key)
    state[key] = DiskSample(now, size)
    growth_per_hour = None
    if previous is not None and now > previous.at_ms:
        growth_per_hour = int((size - previous.bytes_used) * 3_600_000 / (now - previous.at_ms))
    full = Evaluation("DISK_GROWTH", {"path": key, "kind": "fill"}, fraction > max_used_fraction,
                      f"Volume holding {path.name} is {fraction:.0%} full (bound {max_used_fraction:.0%})",
                      {"path": key, "used_fraction": round(fraction, 4), "free_bytes": du.free}, severity=HIGH)
    growth = Evaluation("DISK_GROWTH", {"path": key, "kind": "growth"},
                        growth_per_hour is not None and growth_per_hour > max_growth_bytes_per_hour,
                        f"{path.name} grows {0 if growth_per_hour is None else growth_per_hour // 1024 ** 2} MiB/h",
                        {"path": key, "bytes": size, "growth_bytes_per_hour": growth_per_hour,
                         "bound_bytes_per_hour": max_growth_bytes_per_hour})
    return [_apply(engine, full, "disk_growth", now_ms), _apply(engine, growth, "disk_growth", now_ms)]


def newest_backup_manifest(backup_dir: Path) -> Optional[Dict[str, Any]]:
    best = None
    try:
        names = sorted((p for p in Path(backup_dir).iterdir() if p.name.startswith("cosmicforge-")
                        and p.name.endswith(BACKUP_NAME_SUFFIXES)), key=lambda p: p.name, reverse=True)
    except OSError:
        return None
    for p in names:
        try:
            manifest = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if manifest.get("quick_check") == "ok":
            best = manifest
            break
    return best


def evaluate_backup_age(engine: AlertEngine, backup_dir: Path, *, max_age_hours: float = 36.0,
                        now_ms: Optional[int] = None) -> Evaluation:
    from datetime import datetime
    now = int(now_ms if now_ms is not None else time.time() * 1000)
    manifest = newest_backup_manifest(Path(backup_dir))
    if manifest is None:
        ev = Evaluation("BACKUP_FAILURE", "backup:newest", True, "No verified database backup was found",
                        {"backup_dir": str(backup_dir)})
    else:
        created = datetime.fromisoformat(str(manifest["created_at"]).replace("Z", "+00:00"))
        age_h = (now - created.timestamp() * 1000) / 3_600_000
        ev = Evaluation("BACKUP_FAILURE", "backup:newest", age_h > max_age_hours,
                        f"Newest verified backup is {age_h:.1f}h old (bound {max_age_hours:g}h)",
                        {"file": manifest.get("file"), "age_hours": round(age_h, 2), "bound_hours": max_age_hours})
    return _apply(engine, ev, "backup_age", now_ms)


#: Written by ``scripts/backup_trading_db.py`` / ``scripts/offsite_backup.py`` after every run (outcome, exit code,
#: time, the last OK run carried forward). A failed run therefore leaves a durable marker even when it wrote no
#: backup and no manifest.
BACKUP_STATUS_FILE, OFFSITE_STATUS_FILE = "backup-status.json", "offsite-status.json"


def _status_marker(path: Path) -> Optional[Dict[str, Any]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        return {"outcome": "UNREADABLE", "message": f"{type(exc).__name__}"}
    return data if isinstance(data, dict) else {"outcome": "UNREADABLE"}


def _iso_to_ms(value: Any) -> Optional[int]:
    from datetime import datetime, timezone
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return int((dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp() * 1000)


def evaluate_backup_status(engine: AlertEngine, backup_dir: Path, *, offsite_max_age_hours: float = 36.0,
                           now_ms: Optional[int] = None) -> List[Evaluation]:
    """Backup FAILURES (not only age): the last local backup run failed; the last off-site run failed; the off-site
    copy has not succeeded for ``offsite_max_age_hours`` (only once an off-site run was ever recorded)."""
    now = int(now_ms if now_ms is not None else time.time() * 1000)
    local = _status_marker(Path(backup_dir) / BACKUP_STATUS_FILE)
    offsite = _status_marker(Path(backup_dir) / OFFSITE_STATUS_FILE)
    out: List[Evaluation] = []

    def failed(marker: Optional[Dict[str, Any]]) -> bool:
        return marker is not None and marker.get("outcome") != "OK"

    def detail(marker: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        m = marker or {}
        return {k: m.get(k) for k in ("outcome", "exit_code", "finished_at", "message", "last_ok_at")}

    out.append(_apply(engine, Evaluation("BACKUP_FAILURE", "backup:local_run", failed(local),
                                         f"The last database backup run failed (exit {(local or {}).get('exit_code')})",
                                         detail(local)), "backup_status", now_ms))
    out.append(_apply(engine, Evaluation("BACKUP_FAILURE", "backup:offsite_run", failed(offsite),
                                         f"The last off-site backup run failed (exit {(offsite or {}).get('exit_code')})",
                                         detail(offsite)), "backup_status", now_ms))
    last_ok = _iso_to_ms((offsite or {}).get("last_ok_at"))
    stale = offsite is not None and (last_ok is None or (now - last_ok) / 3_600_000 > offsite_max_age_hours)
    age_h = None if last_ok is None else round((now - last_ok) / 3_600_000, 2)
    out.append(_apply(engine, Evaluation("BACKUP_FAILURE", "backup:offsite_age", stale,
                                         "No verified off-site copy " + ("was ever made" if last_ok is None else
                                                                         f"for {age_h} h"),
                                         {"last_ok_at": (offsite or {}).get("last_ok_at"), "age_hours": age_h,
                                          "bound_hours": offsite_max_age_hours}), "backup_status", now_ms))
    return out


def run_cycle(engine: AlertEngine, evaluators: Sequence[Callable[[], Any]] = (), *,
              now_ms: Optional[int] = None) -> Dict[str, Any]:
    """One operations tick: run evaluators (each isolated), escalate overdue alerts, deliver what is due. A failing
    evaluator, an unreachable channel or a hanging sink never stops the rest (sinks run with a timeout)."""
    errors, evaluated = [], 0
    for ev in evaluators:
        try:
            ev()
            evaluated += 1
        except Exception as exc:  # noqa: BLE001 -- one broken signal must not stop the others
            from shared_lib.core.security.redaction import redact_text
            errors.append(redact_text(f"{getattr(ev, '__name__', 'evaluator')}: {type(exc).__name__}: {exc}")[:300])
    escalated = engine.escalate_due(now_ms=now_ms)
    delivered = engine.deliver_pending(now_ms=now_ms)
    return {"evaluators": evaluated, "evaluator_errors": errors, "escalated": escalated, "delivery": delivered,
            "health": engine.health(now_ms=now_ms)}


def default_evaluators(engine: AlertEngine, db: Database, *, backup_dir: Optional[Path] = None,
                       disk_paths: Sequence[Path] = (), disk_state: Optional[Dict[str, DiskSample]] = None,
                       runtime_db: Any = None, runtime_db_path: Optional[str] = None,
                       producer_state: Optional[Dict[str, Any]] = None):
    """The evaluators a periodic operations job runs: the platform database ones, the backup and disk ones, and
    -- when the legacy runtime database is given -- every producer of ``app.ops.alert_producers``."""
    from app.ops import alert_producers as P
    queue = JobQueue(db)
    state = disk_state if disk_state is not None else {}

    def named(fn: Callable[[], Any], name: str) -> Callable[[], Any]:
        fn.__name__ = name
        return fn
    evaluators: List[Callable[[], Any]] = [
        named(lambda: evaluate_database(engine, db), "evaluate_database"),
        named(lambda: evaluate_queue_backlog(engine, queue), "evaluate_queue_backlog"),
        named(lambda: evaluate_worker_heartbeats(engine, db), "evaluate_worker_heartbeats"),
        named(lambda: evaluate_lease_takeovers(engine, db), "evaluate_lease_takeovers"),
    ]
    if runtime_db_path is not None:
        evaluators.append(named(lambda: evaluate_database(engine, P.RuntimeDatabaseProbe(runtime_db_path),
                                                          name="runtime"), "evaluate_runtime_database"))
    if backup_dir is not None:
        evaluators.append(named(lambda: evaluate_backup_age(engine, Path(backup_dir)), "evaluate_backup_age"))
        evaluators.append(named(lambda: evaluate_backup_status(engine, Path(backup_dir)), "evaluate_backup_status"))
    for p in disk_paths:
        evaluators.append(named(lambda p=p: evaluate_disk_growth(engine, Path(p), state=state), "evaluate_disk_growth"))
    if runtime_db is not None:
        ctx = P.ProducerContext(engine, runtime_db=runtime_db, platform_db=db,
                                state=producer_state if producer_state is not None else {})
        evaluators.extend(P.runtime_producers(ctx))
    return evaluators


# ── the operations tick: one owner, persisted between ticks ─────────────────────────────────────────────────
def _summary(out: Mapping[str, Any]) -> Dict[str, Any]:
    health = out.get("health") or {}
    return {"evaluators": out.get("evaluators"), "evaluator_errors": out.get("evaluator_errors"),
            "escalated": len(out.get("escalated") or ()), "delivery": out.get("delivery"),
            "health_status": health.get("status"), "owner_required": health.get("owner_required"),
            "error": out.get("error")}


def run_tick(engine: AlertEngine, lease: TickLease,
             build_evaluators: Callable[[Dict[str, Any], Dict[str, DiskSample]], Sequence[Callable[[], Any]]], *,
             min_interval_ms: int = 0, release: bool = False, now_ms: Optional[int] = None) -> Dict[str, Any]:
    """Take the tick lease, run one cycle, record its time/result/state under the fencing token. Never raises for
    a failing evaluator, sink or recording: the result says what happened (``ran``, ``lease``, ``recorded``)."""
    grant = lease.acquire(min_interval_ms=min_interval_ms)
    if not grant.acquired:
        return {"ran": False, "reason": grant.reason, "holder": grant.holder, "lease_expires_at": grant.expires_at}
    state = dict(grant.state or {})
    disk = {k: DiskSample(int(v[0]), int(v[1])) for k, v in (state.get("disk") or {}).items()
            if isinstance(v, (list, tuple)) and len(v) == 2}
    state.setdefault("pending", {})
    try:
        out: Dict[str, Any] = run_cycle(engine, build_evaluators(state, disk), now_ms=now_ms)
        outcome = TICK_OK if not out["evaluator_errors"] else "DEGRADED"
    except Exception as exc:  # noqa: BLE001 -- the loop must survive anything a tick meets
        from shared_lib.core.security.redaction import redact_text
        out = {"error": redact_text(f"{type(exc).__name__}: {exc}")[:300], "evaluator_errors": []}
        outcome = TICK_ERROR
    state["disk"] = {k: [s.at_ms, s.bytes_used] for k, s in disk.items()}
    recorded = False
    try:
        recorded = lease.record(grant.token, started_at_ms=int(grant.now_ms or 0), outcome=outcome,
                                result=_summary(out), state=state)
        if release:
            lease.release(grant.token)
    except Exception as exc:  # noqa: BLE001
        logger.error("operations tick result not recorded: %s", type(exc).__name__)
    out.update({"ran": True, "outcome": outcome,
                "lease": {"owner": lease.owner, "token": grant.token, "grant": grant.reason,
                          "takeover": grant.takeover, "recorded": recorded}})
    if not recorded:
        out["lease"]["fenced"] = True   # another owner took over while this tick ran; its result was not kept
    return out


def escalation_policy_from_env(environ: Optional[Mapping[str, str]] = None) -> Dict[str, Any]:
    """Engine policy overrides for the operations owner: ``OPS_ESCALATION_MINUTES`` (JSON, e.g.
    ``{"CRITICAL": [5, 15], "HIGH": [15, 60]}``, merged over the defaults) and ``OPS_DEDUP_WINDOW_MINUTES``.
    The defaults are engineering defaults pending the operations owner's decision, not a commitment."""
    env = os.environ if environ is None else environ
    out: Dict[str, Any] = {}
    raw = (env.get("OPS_ESCALATION_MINUTES") or "").strip()
    if raw:
        given = json.loads(raw)
        merged = dict(ESCALATION_MINUTES)
        merged.update({str(k).upper(): tuple(int(x) for x in v) for k, v in given.items()})
        out["escalation_minutes"] = merged
    window = (env.get("OPS_DEDUP_WINDOW_MINUTES") or "").strip()
    if window:
        out["dedup_window_ms"] = int(float(window) * 60_000)
    return out


def runtime_database_path(environ: Optional[Mapping[str, str]] = None) -> Optional[str]:
    """The legacy runtime database file named by ``DATABASE_URL`` (``sqlite:///...``; a relative path is resolved
    against ``backends/bot-backend``, as ``shared_lib.persistence.db.DB`` does). None when it names no SQLite file."""
    env = os.environ if environ is None else environ
    url = env.get("DATABASE_URL") or ""
    if not url.startswith("sqlite:///"):
        return None
    path = url[len("sqlite:///"):]
    if not os.path.isabs(path):
        path = os.path.abspath(os.path.join(Path(__file__).resolve().parents[2], path))
    return path


def open_runtime_db(path: Optional[str]) -> Any:
    """The legacy ``DB`` for an EXISTING file (never creates one); None otherwise."""
    if not path or not Path(path).is_file():
        return None
    from shared_lib.persistence.db import DB
    return DB(path)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """The operations tick: ``python -m app.ops.alerting [--loop --interval 60] [--backup-dir DIR] [--disk-path P]``.

    Reads ``PLATFORM_DATABASE_URL`` (then ``DATABASE_URL``) for the platform database -- the migrations must already
    be applied -- and the legacy runtime database from ``--runtime-db`` (default: the SQLite file of
    ``DATABASE_URL``). One process at a time owns the tick (``TickLease``); others wait (``--loop``) or exit 0
    (one-shot: ``LEASE_HELD`` / ``TICK_NOT_DUE``). Prints JSON (no secrets: details are redacted at storage).
    One-shot exit status: 0 the tick ran (or was not this process's turn) and the alerting system is healthy;
    2 it ran and the alerting system reports a CRITICAL finding (e.g. OPERATIONS_OWNER_REQUIRED); 1 it cannot run.
    ``--loop`` returns 0 after SIGTERM/SIGINT (the lease is released for an immediate successor)."""
    import argparse
    ap = argparse.ArgumentParser(prog="python -m app.ops.alerting")
    ap.add_argument("--backup-dir")
    ap.add_argument("--disk-path", action="append", default=[])
    ap.add_argument("--alerts-env", default=DEFAULT_ALERTS_ENV)
    ap.add_argument("--runtime-db", help="legacy runtime SQLite file (default: DATABASE_URL)")
    ap.add_argument("--no-runtime-db", action="store_true", help="platform-database evaluators only")
    ap.add_argument("--dotenv", help="load this .env first (variables already set win)")
    ap.add_argument("--loop", action="store_true", help="run ticks until SIGTERM (container topology)")
    ap.add_argument("--interval", type=float, default=60.0, help="seconds between ticks (default 60)")
    ap.add_argument("--owner", help="lease owner name (default host:pid)")
    ap.add_argument("--lease-ttl", type=float, help="seconds a silent owner keeps the lease (default 3 x interval, "
                                                     "at least 180)")
    ap.add_argument("--max-ticks", type=int, help=argparse.SUPPRESS)       # tests: stop the loop after N ticks
    a = ap.parse_args(argv)
    if a.interval < 5:
        print(json.dumps({"ok": False, "error": "--interval must be at least 5 seconds"}))
        return 1
    if a.dotenv:
        try:
            import dotenv
            dotenv.load_dotenv(a.dotenv, override=False)
        except Exception as exc:  # noqa: BLE001
            print(json.dumps({"ok": False, "error": f"dotenv: {type(exc).__name__}"}))
            return 1
    try:
        db = Database.from_env(pool_size=6)       # sinks that time out may still hold a connection for a while
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"[:300]}))
        return 1
    try:
        sinks: List[AlertSink] = [JobQueueSink(JobQueue(db))]
        direct = NotifyChannelsSink(env_file=a.alerts_env)
        if direct.configured():
            sinks.append(direct)
        engine = AlertEngine(db, sinks=sinks, **escalation_policy_from_env())
        runtime_path = None if a.no_runtime_db else (a.runtime_db or runtime_database_path())
        runtime_db = open_runtime_db(runtime_path)
        owner = a.owner or f"{socket.gethostname()}:{os.getpid()}"
        ttl_ms = int(1000 * (a.lease_ttl if a.lease_ttl else max(180.0, 3 * a.interval)))
        lease = TickLease(db, owner=owner, ttl_ms=ttl_ms)

        def build(state: Dict[str, Any], disk: Dict[str, DiskSample]):
            return default_evaluators(engine, db, backup_dir=Path(a.backup_dir) if a.backup_dir else None,
                                      disk_paths=[Path(p) for p in a.disk_path], disk_state=disk,
                                      runtime_db=runtime_db, runtime_db_path=runtime_path, producer_state=state)
        if a.loop:
            return run_loop(engine, lease, build, interval_s=a.interval, max_ticks=a.max_ticks)
        try:
            out = run_tick(engine, lease, build, min_interval_ms=int(a.interval * 500), release=True)
        except Exception as exc:  # noqa: BLE001 -- e.g. the platform database is unreachable: exit 1, alert via OnFailure
            print(json.dumps({"ok": False, "ran": False, "error": f"{type(exc).__name__}"}))
            return 1
        print(json.dumps(out, default=str))
        if not out.get("ran"):
            return 0
        return 2 if (out.get("health") or {}).get("status") == "CRITICAL" else (1 if out.get("error") else 0)
    finally:
        db.close()


def run_loop(engine: AlertEngine, lease: TickLease, build, *, interval_s: float = 60.0,
             stop: Optional[threading.Event] = None, max_ticks: Optional[int] = None) -> int:
    """Long-running ticker (container ``ops-tick`` service). The lease is kept between ticks and renewed by each;
    a second ticker waits (``LEASE_HELD``) and takes over once the first is gone and its lease expired. Nothing a
    tick meets ends the loop; SIGTERM/SIGINT end it after the current tick and release the lease."""
    stop = stop or threading.Event()
    previous: Dict[int, Any] = {}
    if threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                previous[sig] = signal.signal(sig, lambda *_: stop.set())
            except (ValueError, OSError):  # pragma: no cover - platform without the signal
                pass
    try:
        return _loop(engine, lease, build, interval_s=interval_s, stop=stop, max_ticks=max_ticks)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def _loop(engine: AlertEngine, lease: TickLease, build, *, interval_s: float, stop: threading.Event,
          max_ticks: Optional[int]) -> int:
    ticks = 0
    while not stop.is_set():
        started = time.monotonic()
        try:
            out = run_tick(engine, lease, build)
            line = _summary(out) if out.get("ran") else {"ran": False, "reason": out.get("reason"),
                                                          "holder": out.get("holder")}
            line["lease"] = out.get("lease")
            print(json.dumps(line, default=str), flush=True)
        except Exception as exc:  # noqa: BLE001 -- e.g. the platform database is down: try again next interval
            logger.error("operations tick failed: %s", type(exc).__name__)
            print(json.dumps({"ran": False, "error": type(exc).__name__}), flush=True)
        ticks += 1
        if max_ticks is not None and ticks >= max_ticks:
            break
        stop.wait(max(1.0, interval_s - (time.monotonic() - started)))
    try:
        status = lease.status()
        if status.get("held") and status.get("owner") == lease.owner:
            lease.release(int(status["fencing_token"]))
    except Exception as exc:  # noqa: BLE001
        logger.warning("tick lease not released: %s", type(exc).__name__)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
