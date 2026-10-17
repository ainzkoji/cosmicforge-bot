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
* ``run_cycle`` -- evaluate, escalate, deliver -- for a periodic operations job.

Every evaluator is pure with respect to time when ``now_ms`` is given, so tests inject the clock; otherwise
time is the database server's.
"""
from __future__ import annotations

import importlib.util
import json
import logging
import os
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

from shared_lib.operations.alerting import (  # noqa: F401  (re-exported API)
    ACKNOWLEDGED, CATEGORIES, CRITICAL, ESCALATION_MINUTES, HIGH, LOW, MEDIUM, OPEN, RESOLVED, SEVERITIES,
    AlertEngine, AlertError, AlertMessage, AlertSink, CallableSink, CategoryPolicy, RaiseResult, fingerprint)
from shared_lib.operations.oncall import OPERATIONS_OWNER_REQUIRED, ROTATIONS, OncallDirectory  # noqa: F401
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
                    {"orphaned_accounts": len(orphans), "account_ids": orphans[:50], "grace_ms": grace_ms})
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


def run_cycle(engine: AlertEngine, evaluators: Sequence[Callable[[], Any]] = (), *,
              now_ms: Optional[int] = None) -> Dict[str, Any]:
    """One operations tick: run evaluators (each isolated), escalate overdue alerts, deliver what is due."""
    errors = []
    for ev in evaluators:
        try:
            ev()
        except Exception as exc:  # noqa: BLE001 -- one broken signal must not stop the others
            errors.append(f"{getattr(ev, '__name__', 'evaluator')}: {type(exc).__name__}: {exc}"[:300])
    escalated = engine.escalate_due(now_ms=now_ms)
    delivered = engine.deliver_pending(now_ms=now_ms)
    return {"evaluator_errors": errors, "escalated": escalated, "delivery": delivered, "health": engine.health(now_ms=now_ms)}


def default_evaluators(engine: AlertEngine, db: Database, *, backup_dir: Optional[Path] = None,
                       disk_paths: Sequence[Path] = (), disk_state: Optional[Dict[str, DiskSample]] = None):
    """The evaluators a periodic operations job runs against the platform database."""
    queue = JobQueue(db)
    state = disk_state if disk_state is not None else {}
    evaluators: List[Callable[[], Any]] = [
        lambda: evaluate_database(engine, db),
        lambda: evaluate_queue_backlog(engine, queue),
        lambda: evaluate_worker_heartbeats(engine, db),
        lambda: evaluate_lease_takeovers(engine, db),
    ]
    if backup_dir is not None:
        evaluators.append(lambda: evaluate_backup_age(engine, Path(backup_dir)))
    for p in disk_paths:
        evaluators.append(lambda p=p: evaluate_disk_growth(engine, Path(p), state=state))
    return evaluators


def main(argv: Optional[Sequence[str]] = None) -> int:
    """One operations tick: ``python -m app.ops.alerting [--backup-dir DIR] [--disk-path PATH ...]``.

    Reads ``PLATFORM_DATABASE_URL`` (then ``DATABASE_URL``); the platform migrations must already be applied.
    Prints one JSON object (no secrets: details are redacted at storage). Exit 0 when the alerting system itself
    is healthy, 2 when it reports a CRITICAL finding (e.g. OPERATIONS_OWNER_REQUIRED), 1 when it cannot run."""
    import argparse
    ap = argparse.ArgumentParser(prog="python -m app.ops.alerting")
    ap.add_argument("--backup-dir")
    ap.add_argument("--disk-path", action="append", default=[])
    ap.add_argument("--alerts-env", default=DEFAULT_ALERTS_ENV)
    a = ap.parse_args(argv)
    try:
        db = Database.from_env(pool_size=4)
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"[:300]}))
        return 1
    try:
        sinks: List[AlertSink] = [JobQueueSink(JobQueue(db))]
        direct = NotifyChannelsSink(env_file=a.alerts_env)
        if direct.configured():
            sinks.append(direct)
        engine = AlertEngine(db, sinks=sinks)
        out = run_cycle(engine, default_evaluators(engine, db, backup_dir=Path(a.backup_dir) if a.backup_dir else None,
                                                   disk_paths=[Path(p) for p in a.disk_path]))
        print(json.dumps(out, default=str))
        return 2 if out["health"]["status"] == "CRITICAL" else 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
