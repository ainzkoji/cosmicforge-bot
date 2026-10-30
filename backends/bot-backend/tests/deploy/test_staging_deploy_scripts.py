"""Step 5.6 failure injection: the REAL deploy/staging scripts run under bash with a fake ``docker`` first on PATH.

No Docker, no container, no network, no database: ``fake_docker.py`` simulates the stack (images per tag, the
migration job, container start/stop/drain, the trading process's /health?ready=1 answer) and records every call.
What is under test is the scripts' control flow -- ordering, gates, automatic rollback, state records -- which is
what a hosted staging run would otherwise be the only evidence of. These tests are NOT evidence that a real stack
deploys or operates; that needs an authorized staging host (EXT-STG-01).
"""
from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[4]
STAGING = REPO / "deploy" / "staging"
FAKE = Path(__file__).with_name("fake_docker.py")
SCRIPTS = ("deploy.sh", "rollback.sh", "restore.sh", "lib.sh")
REPOS = ("bot-backend", "user-backend", "admin-backend", "user-frontend", "admin-frontend")


def _find_bash():
    candidates = []
    if os.name == "nt":   # Git Bash; System32\bash.exe is the WSL launcher and sees another file system
        for base in (os.environ.get("ProgramFiles", r"C:\Program Files"), os.environ.get("ProgramW6432", "")):
            if base:
                candidates.append(Path(base, "Git", "bin", "bash.exe"))
    found = shutil.which("bash")
    if found:
        candidates.append(Path(found))
    for c in candidates:
        if not c.exists() or (os.name == "nt" and "system32" in str(c).lower()):
            continue
        try:
            r = subprocess.run([str(c), "-c", "command -v sha256sum >/dev/null && command -v tee >/dev/null && echo ok"],
                               capture_output=True, text=True, timeout=60)
        except (OSError, subprocess.SubprocessError):
            continue
        if r.stdout.strip() == "ok":
            return str(c)
    return None


BASH = _find_bash()
needs_bash = pytest.mark.skipif(BASH is None, reason="bash with coreutils is not available")
#: Every bash fork costs ~0.3 s on this Windows workstation (one deploy run is ~60 of them), so on Windows only the
#: core scenarios run by default; Linux CI runs the whole matrix. CF_DEPLOY_SCRIPT_TESTS=all runs it here too.
FULL_MATRIX = os.name != "nt" or os.environ.get("CF_DEPLOY_SCRIPT_TESTS") == "all"
matrix = pytest.mark.skipif(not FULL_MATRIX, reason="full deploy-script matrix: Linux CI, or CF_DEPLOY_SCRIPT_TESTS=all")


def _posix(p) -> str:
    return Path(p).as_posix()


class FakeStack:
    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        self.bin = tmp / "bin"
        self.bin.mkdir()
        self.state_file, self.log_file = tmp / "docker-state.json", tmp / "docker-calls.jsonl"
        self.deploy_state, self.backups = tmp / "deploy-state", tmp / "backups"
        self.deploy_state.mkdir()
        shim = self.bin / "docker"
        with open(shim, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(f'#!/usr/bin/env bash\nexec "{_posix(sys.executable)}" "{_posix(FAKE)}" "$@"\n')
        shim.chmod(0o755)
        self.log_file.write_text("", encoding="utf-8")
        self.state = {"images": [], "releases": {}, "containers": {},
                      "db": {"version": 0, "irreversible": [], "up": False, "pg_ready": True}}
        self.save()

    # ── setting the scene ──────────────────────────────────────────────────────────────────────────────────
    def release(self, tag: str, schema: int = 44, **behaviour) -> None:
        self.state["images"] += [f"cosmicforge/{r}:{tag}" for r in REPOS]
        self.state["releases"][tag] = {"schema": schema, **behaviour}
        self.save()

    def serving(self, tag: str, previous: str | None = None) -> None:
        """The stack as a completed deploy of ``tag`` leaves it (without running one)."""
        self.state["db"].update(version=self.state["releases"][tag]["schema"], up=True)
        for svc in ("bot-trading", "bot-api", "user-backend", "admin-backend", "user-frontend", "admin-frontend",
                    "healthcheck"):
            self.state["containers"][svc] = {"tag": tag, "running": True, "id": f"c-{svc}-{tag}", "polls": 0,
                                             "exit_code": None}
        (self.deploy_state / "current_tag").write_text(tag + "\n", encoding="utf-8")
        if previous:
            (self.deploy_state / "previous_tag").write_text(previous + "\n", encoding="utf-8")
        self.save()

    def save(self) -> None:
        self.state_file.write_text(json.dumps(self.state), encoding="utf-8")

    def load(self) -> dict:
        self.state = json.loads(self.state_file.read_text(encoding="utf-8"))
        return self.state

    # ── running ────────────────────────────────────────────────────────────────────────────────────────────
    def run(self, script: str, *args: str, env: dict | None = None, timeout: int = 300):
        e = dict(os.environ)
        e.update({
            "PATH": f"{self.bin}{os.pathsep}{e.get('PATH', '')}",
            "DOCKER_BIN": _posix(self.bin / "docker"),
            "PYTHON_BIN": _posix(sys.executable),
            "FAKE_DOCKER_STATE": str(self.state_file),
            "FAKE_DOCKER_LOG": str(self.log_file),
            "DEPLOY_STATE_DIR": _posix(self.deploy_state),
            "DEPLOY_BACKUP_DIR": _posix(self.backups),
            "DEPLOY_READY_TIMEOUT_S": "2",
            "DEPLOY_POLL_INTERVAL_S": "0",
            "DEPLOY_DB_TIMEOUT_S": "2",
        })
        for k in ("COMPOSE_PROFILES", "DEPLOY_LOCK_HELD", "DEPLOY_REQUIRE_SUPERVISED", "DEPLOY_STRICT_HEALTH_CHECK",
                  "DEPLOY_COMPOSE_OVERRIDE", "DEPLOY_API_SERVICES", "DEPLOY_STOP_GRACE_S", "IMAGE_TAG"):
            e.pop(k, None)
        e.update(env or {})
        r = subprocess.run([BASH, _posix(STAGING / script), *args], env=e, capture_output=True, text=True,
                           encoding="utf-8", errors="replace", timeout=timeout)
        self.load()
        return r

    # ── reading the outcome ────────────────────────────────────────────────────────────────────────────────
    def calls(self) -> list:
        return [json.loads(line) for line in self.log_file.read_text(encoding="utf-8").splitlines() if line.strip()]

    def compose_calls(self, sub: str, service: str | None = None) -> list:
        out = []
        for i, c in enumerate(self.calls()):
            a = c["argv"]
            if a[:1] == ["compose"]:
                j = 1
                while j < len(a) and a[j] in ("-f", "--file"):
                    j += 2
                if j < len(a) and a[j] == sub and (service is None or service in a[j + 1:]):
                    out.append((i, c))
        return out

    def result(self) -> dict:
        return json.loads((self.deploy_state / "last_result.json").read_text(encoding="utf-8"))

    def history(self) -> list:
        return [json.loads(x) for x in (self.deploy_state / "history.jsonl").read_text(encoding="utf-8").splitlines()]

    def tag(self, name: str = "current_tag") -> str:
        p = self.deploy_state / name
        return p.read_text(encoding="utf-8").strip() if p.exists() else ""

    def running(self, svc: str) -> tuple:
        c = self.state["containers"].get(svc) or {}
        return c.get("tag"), c.get("running")

    def drains(self) -> list:
        return [json.loads(x) for p in sorted(self.deploy_state.glob("drain-*.jsonl"))
                for x in p.read_text(encoding="utf-8").splitlines()]


@pytest.fixture
def stack(tmp_path):
    return FakeStack(tmp_path)


def _explain(r) -> str:
    return f"exit {r.returncode}\n--- stdout ---\n{r.stdout[-6000:]}\n--- stderr ---\n{r.stderr[-3000:]}"


def _no_mutation(stack: FakeStack) -> None:
    """The scripts only ever READ the trading process (GET /health, GET emergency status)."""
    assert not stack.state.get("mutations"), stack.state.get("mutations")
    for c in stack.calls():
        if "curl" in c["argv"]:
            assert not {"-X", "--request", "-d", "--data", "--json"} & set(c["argv"]), c


# ── the scripts themselves ─────────────────────────────────────────────────────────────────────────────────
def test_scripts_keep_lf_line_endings():
    """deploy/** is read by bash on Linux: a CRLF would break it (.gitattributes pins eol=lf)."""
    for name in SCRIPTS + ("release_gate.py", "docker-compose.staging.yml"):
        assert b"\r\n" not in (STAGING / name).read_bytes(), name
    assert "deploy/** text eol=lf" in (REPO / ".gitattributes").read_text(encoding="utf-8")


@needs_bash
def test_scripts_parse_under_bash():
    for name in SCRIPTS:
        r = subprocess.run([BASH, "-n", _posix(STAGING / name)], capture_output=True, text=True, timeout=60)
        assert r.returncode == 0, (name, r.stderr)


# ── deploy: the happy path and its ordering ────────────────────────────────────────────────────────────────
@needs_bash
def test_first_deploy_then_upgrade_in_order_with_restore_point_and_clean_drain(stack):
    stack.release("r1")
    stack.release("r2", schema=45)
    r = stack.run("deploy.sh", "r1")
    assert r.returncode == 0, _explain(r)
    assert stack.tag() == "r1" and stack.result()["status"] == "DEPLOYED"
    stack.log_file.write_text("", encoding="utf-8")

    r = stack.run("deploy.sh", "r2")
    assert r.returncode == 0, _explain(r)
    assert (stack.tag(), stack.tag("previous_tag")) == ("r2", "r1")
    assert stack.state["db"]["version"] == 45
    assert stack.running("bot-trading") == ("r2", True) and stack.running("bot-api") == ("r2", True)
    res = stack.result()
    assert res["status"] == "DEPLOYED" and "READY_UNSUPERVISED" in res["detail"]
    # restore point first: a dump with its recorded checksum
    dumps = list(stack.backups.glob("platform-*.dump"))
    assert dumps and all((stack.deploy_state / f"backup-{d.stem[len('platform-'):]}.sha256").exists() for d in dumps)
    # ordering: database ready -> dump -> runtime backup (previous image) -> migrate (new image) -> compat (previous
    # image) -> API tier -> drain trading -> start trading -> release gate
    first = lambda sub, svc=None: stack.compose_calls(sub, svc)[0][0]          # noqa: E731
    migrate = [i for i, c in stack.compose_calls("run", "migrate") if c["argv"][-1] == "migrate"]
    backup = [i for i, c in stack.compose_calls("run", "bot-trading") if c["image_tag"] == "r1"]
    assert first("exec", "postgres") < backup[0] < migrate[0] < first("up", "bot-api") \
        < first("stop", "bot-trading") < first("up", "bot-trading")
    assert [c["image_tag"] for i, c in stack.compose_calls("run", "migrate") if i in migrate] == ["r2"]
    assert any(c["run_by"] == "r1" and c["ok"] for c in stack.state["compat_checks"])
    assert "REDEPLOY of r1 is schema-compatible" in (stack.deploy_state / "rollback-mode").read_text()
    assert stack.drains()[-1] == {"service": "bot-trading", "outgoing_tag": "r1", "outcome": "DRAINED",
                                  "quiesce": "clean", "grace_s": 120}
    assert list((stack.deploy_state / "logs").glob("*-bot-trading-r1.log"))     # outgoing logs kept
    assert not (stack.deploy_state / "lock").exists()
    _no_mutation(stack)


# ── failure injection ──────────────────────────────────────────────────────────────────────────────────────
@needs_bash
def test_failed_migration_stops_the_rollout_and_the_old_release_keeps_serving(stack):
    stack.release("r1")
    stack.release("r2", schema=45, migrate="fail")
    stack.serving("r1", previous="r0")
    r = stack.run("deploy.sh", "r2")
    assert r.returncode == 10, _explain(r)
    res = stack.result()
    assert (res["status"], res["phase"], res["serving_tag"]) == ("FAILED_BEFORE_SWAP", "migrate", "r1")
    assert stack.tag() == "r1" and stack.tag("previous_tag") == "r0"
    for svc in ("bot-trading", "bot-api", "user-backend"):
        assert stack.running(svc) == ("r1", True), svc
    assert not stack.compose_calls("up", "bot-api") and not stack.compose_calls("stop")   # nothing swapped
    assert "r1 (still serving) is compatible" in (stack.deploy_state / "rollback-mode").read_text()
    assert not (stack.deploy_state / "lock").exists()


@needs_bash
@matrix
def test_backup_failure_stops_before_any_migration(stack):
    stack.release("r1")
    stack.release("r2", schema=45)
    stack.serving("r1")
    stack.state["db"]["pg_dump"] = "fail"
    stack.save()
    r = stack.run("deploy.sh", "r2")
    assert r.returncode == 10, _explain(r)
    assert (stack.result()["phase"], stack.tag()) == ("backup", "r1")
    assert not stack.compose_calls("run", "migrate") and stack.state["db"]["version"] == 44


@needs_bash
@matrix
def test_missing_previous_image_refuses_before_any_change(stack):
    stack.release("r1")
    stack.release("r2")
    stack.serving("r1")
    stack.state["images"] = [i for i in stack.state["images"] if not i.endswith(":r1")]   # pruned: no way back
    stack.save()
    r = stack.run("deploy.sh", "r2")
    assert r.returncode == 10 and stack.result()["phase"] == "preflight", _explain(r)
    assert not stack.compose_calls("run") and not stack.compose_calls("up")


@needs_bash
@matrix
@pytest.mark.parametrize("failing", ["bot-trading", "bot-api"])
def test_container_that_fails_to_start_is_rolled_back_automatically(stack, failing):
    stack.release("r1")
    stack.release("r2", start_fail=[failing])
    stack.serving("r1", previous="r0")
    r = stack.run("deploy.sh", "r2")
    assert r.returncode == 11, _explain(r)
    res = stack.result()
    assert (res["operation"], res["status"], res["serving_tag"]) == ("deploy", "ROLLED_BACK", "r1")
    assert res["phase"] == ("trading" if failing == "bot-trading" else "api")
    assert stack.tag() == "r1" and stack.tag("previous_tag") == "r0"
    assert stack.running("bot-trading") == ("r1", True) and stack.running("bot-api") == ("r1", True)
    assert [h["status"] for h in stack.history()][-2:] == ["ROLLED_BACK", "ROLLED_BACK"]   # rollback.sh, then deploy
    _no_mutation(stack)


@needs_bash
@matrix
def test_worker_that_crashes_after_start_is_rolled_back(stack):
    stack.release("r1")
    stack.release("r2", ready="crash")
    stack.serving("r1")
    r = stack.run("deploy.sh", "r2")
    assert r.returncode == 11, _explain(r)
    assert '"reason": "NO_ANSWER"' in stack.result()["detail"]
    assert stack.running("bot-trading") == ("r1", True) and stack.tag() == "r1"


@needs_bash
def test_release_never_ready_rolls_back_to_the_release_it_replaced_not_an_older_one(stack):
    """deploy.sh used to exec rollback.sh without a target, which read previous_tag -- the release BEFORE the one
    being replaced (r0 here) -- or found none on a second deploy."""
    stack.release("r0")
    stack.release("r1")
    stack.release("r2", ready="never")
    stack.serving("r1", previous="r0")
    r = stack.run("deploy.sh", "r2")
    assert r.returncode == 11, _explain(r)
    assert stack.tag() == "r1" and stack.running("bot-trading") == ("r1", True)
    assert not [c for c in stack.calls() if c["image_tag"] == "r0" and "up" in c["argv"]]
    assert '"reason": "HTTP_503"' in stack.result()["detail"]
    verdicts = [json.loads(p.read_text()) for p in stack.deploy_state.glob("verdict-*-bot-trading.json")]
    assert {v["verdict"] for v in verdicts} == {"NOT_READY", "READY_UNSUPERVISED"}   # r2 refused, r1 accepted


@needs_bash
@matrix
def test_lost_exchange_connection_is_degraded_never_accepted_and_nothing_is_mutated(stack):
    stack.release("r1", ready="ready")
    stack.release("r2", ready="degraded")
    stack.serving("r1")
    r = stack.run("deploy.sh", "r2", env={"DEPLOY_REQUIRE_SUPERVISED": "1"})
    assert r.returncode == 11, _explain(r)
    assert '"verdict": "DEGRADED"' in stack.result()["detail"]
    assert "BROKER_RECONCILIATION_NOT_SYNCED" in stack.result()["detail"]
    assert stack.running("bot-trading") == ("r1", True)
    _no_mutation(stack)


@needs_bash
@matrix
def test_exchange_down_for_every_release_pages_the_owner_and_still_mutates_nothing(stack):
    stack.release("r1", ready="degraded")
    stack.release("r2", ready="degraded")
    stack.serving("r1")
    r = stack.run("deploy.sh", "r2")
    assert r.returncode == 12, _explain(r)
    statuses = [h["status"] for h in stack.history()]
    assert statuses[-2:] == ["ROLLED_BACK_NOT_READY", "ROLLBACK_FAILED"]
    assert stack.tag() == "r1" and stack.running("bot-trading") == ("r1", True)   # the state file says what runs
    _no_mutation(stack)


@needs_bash
@matrix
def test_live_order_gate_open_on_staging_is_rejected_at_once(stack):
    stack.release("r1")
    stack.release("r2", ready="live")
    stack.serving("r1")
    r = stack.run("deploy.sh", "r2", env={"DEPLOY_READY_TIMEOUT_S": "120"})
    assert r.returncode == 11, _explain(r)
    assert '"verdict": "UNSAFE"' in stack.result()["detail"]
    assert stack.state["containers"]["bot-trading"]["tag"] == "r1"


@needs_bash
@matrix
def test_api_process_answering_as_the_trading_service_is_not_ready(stack):
    stack.release("r1")
    stack.release("r2", ready="role-api")
    stack.serving("r1")
    r = stack.run("deploy.sh", "r2")
    assert r.returncode == 11 and "PROCESS_ROLE_API" in stack.result()["detail"], _explain(r)


@needs_bash
@matrix
def test_unsupervised_release_is_refused_when_supervision_is_required(stack):
    stack.release("r1", ready="ready")
    stack.release("r2")                      # non-production profile: no supervisor
    stack.serving("r1")
    r = stack.run("deploy.sh", "r2", env={"DEPLOY_REQUIRE_SUPERVISED": "1"})
    assert r.returncode == 11 and "RUNTIME_UNSUPERVISED" in stack.result()["detail"], _explain(r)


@needs_bash
@matrix
def test_strict_health_check_gates_only_when_requested(stack):
    stack.release("r1")
    stack.release("r2", health_check_exit=2)
    stack.serving("r1")
    r = stack.run("deploy.sh", "r2", env={"DEPLOY_STRICT_HEALTH_CHECK": "1"})
    assert r.returncode == 11 and stack.result()["phase"] == "verify", _explain(r)
    r = stack.run("deploy.sh", "r2")          # default: the report is evidence, not a gate
    assert r.returncode == 0 and stack.tag() == "r2", _explain(r)
    assert list(stack.deploy_state.glob("health-check-*.json"))


@needs_bash
@matrix
def test_first_deploy_without_previous_release_is_rejected_not_rolled_back(stack):
    stack.release("r1", ready="never")
    r = stack.run("deploy.sh", "r1")
    assert r.returncode == 13, _explain(r)
    assert stack.result()["status"] == "REJECTED_NO_PREVIOUS" and stack.tag() == ""


@needs_bash
@matrix
def test_drain_that_exceeds_the_grace_is_recorded(stack):
    stack.release("r1", drain_exit=137)
    stack.release("r2")
    stack.serving("r1")
    r = stack.run("deploy.sh", "r2")
    assert r.returncode == 0, _explain(r)
    assert stack.drains()[-1]["outcome"] == "KILLED_AFTER_GRACE" and stack.drains()[-1]["quiesce"] == "not_logged"
    assert "did not drain cleanly" in r.stdout


@needs_bash
@matrix
def test_stop_grace_below_the_drain_budget_is_refused(stack):
    stack.release("r1")
    r = stack.run("deploy.sh", "r1", env={"DEPLOY_STOP_GRACE_S": "10"})
    assert r.returncode == 2 and not stack.calls(), _explain(r)


@needs_bash
@matrix
def test_a_second_operation_is_refused_while_one_holds_the_lock(stack):
    stack.release("r1")
    (stack.deploy_state / "lock").mkdir()
    (stack.deploy_state / "lock" / "owner").write_text("deploy pid=1", encoding="utf-8")
    for script, args in (("deploy.sh", ("r1",)), ("rollback.sh", ("r1",))):
        r = stack.run(script, *args)
        assert r.returncode == 9 and "REFUSED" in r.stdout, _explain(r)
    assert not stack.compose_calls("up")


@needs_bash
@matrix
def test_invalid_tag_is_refused(stack):
    r = stack.run("deploy.sh", "r1;rm -rf /")
    assert r.returncode == 2 and not stack.calls(), _explain(r)


# ── rollback and restore ───────────────────────────────────────────────────────────────────────────────────
@needs_bash
@matrix
def test_manual_rollback_waits_for_post_start_reconciliation(stack):
    stack.release("r1", ready="after:2")     # the rolled-back process reconciles for two polls before it is ready
    stack.release("r2", ready="ready")
    stack.serving("r2", previous="r1")
    r = stack.run("rollback.sh", env={"DEPLOY_READY_TIMEOUT_S": "120", "DEPLOY_REQUIRE_SUPERVISED": "1"})
    assert r.returncode == 0, _explain(r)
    res = stack.result()
    assert (res["operation"], res["status"], res["from_tag"], res["serving_tag"]) == ("rollback", "ROLLED_BACK", "r2", "r1")
    assert "SUPERVISED_RUNTIME_HEALTHY_AND_RECONCILED" in res["detail"]
    assert stack.state["containers"]["bot-trading"]["polls"] >= 3      # it waited through the not-ready answers
    assert stack.tag() == "r1" and stack.running("bot-api") == ("r1", True)
    assert stack.drains()[-1] == {"service": "bot-trading", "outgoing_tag": "r2", "outcome": "DRAINED",
                                  "quiesce": "clean", "grace_s": 120}
    _no_mutation(stack)


@needs_bash
@matrix
def test_rollback_is_refused_before_any_change_when_the_schema_is_irreversible(stack):
    stack.release("r1", schema=44)
    stack.release("r2", schema=45, irreversible=[45])
    stack.serving("r1", previous="r0")
    r = stack.run("deploy.sh", "r2")
    assert r.returncode == 0, _explain(r)
    assert "RESTORE REQUIRED" in (stack.deploy_state / "rollback-mode").read_text()
    stack.log_file.write_text("", encoding="utf-8")
    r = stack.run("rollback.sh")
    assert r.returncode == 3, _explain(r)
    assert stack.result()["status"] == "REFUSED_SCHEMA" and stack.tag() == "r2"
    assert not stack.compose_calls("up") and not stack.compose_calls("stop")
    assert stack.running("bot-trading") == ("r2", True)


@needs_bash
@matrix
def test_automatic_rollback_refused_by_schema_reports_rollback_failed(stack):
    stack.release("r1", schema=44)
    stack.release("r2", schema=45, irreversible=[45], ready="never")
    stack.serving("r1")
    r = stack.run("deploy.sh", "r2")
    assert r.returncode == 12, _explain(r)
    assert [h["status"] for h in stack.history()][-2:] == ["REFUSED_SCHEMA", "ROLLBACK_FAILED"]
    assert stack.result()["serving_tag"] == "r2" and stack.running("bot-trading") == ("r2", True)   # nothing hidden


@needs_bash
@matrix
def test_restore_checks_the_dump_drains_trading_first_and_waits_for_the_release_gate(stack):
    stack.release("r1")
    stack.release("r2", schema=45)
    stack.serving("r1")
    assert stack.run("deploy.sh", "r2").returncode == 0
    dump = next(stack.backups.glob("platform-*.dump"))
    stack.log_file.write_text("", encoding="utf-8")

    r = stack.run("restore.sh", _posix(dump), "r1")                       # no --confirm
    assert r.returncode == 2 and not stack.calls()
    tampered = stack.tmp / dump.name
    tampered.write_text("PGDMP-fake version=99\n", encoding="utf-8")
    r = stack.run("restore.sh", _posix(tampered), "r1", "--confirm")
    assert r.returncode == 3 and "checksum mismatch" in r.stdout, _explain(r)
    assert not stack.compose_calls("stop")

    r = stack.run("restore.sh", _posix(dump), "r1", "--confirm")
    assert r.returncode == 0, _explain(r)
    assert stack.state["restored_from"] == "PGDMP-fake version=44" and stack.state["db"]["version"] == 44
    drop = [i for i, c in stack.compose_calls("exec", "postgres") if any("dropdb" in a for a in c["argv"])][0]
    stops = stack.compose_calls("stop")
    assert stops[0][1]["argv"][-1] == "bot-trading" and all(i < drop for i, _ in stops)      # trading first
    assert {"bot-api", "user-backend", "admin-backend", "healthcheck"} <= set(stops[1][1]["argv"])
    assert stack.result()["status"] == "RESTORED" and stack.tag() == "r1"
    assert stack.running("bot-trading") == ("r1", True) and stack.running("bot-api") == ("r1", True)
    _no_mutation(stack)


# ── topology ───────────────────────────────────────────────────────────────────────────────────────────────
@needs_bash
@matrix
def test_multi_worker_profile_rolls_both_trading_processes_one_at_a_time(stack):
    stack.release("r1")
    stack.release("r2")
    stack.serving("r1")
    stack.state["containers"]["bot-trading-assigned"] = {"tag": "r1", "running": True, "id": "c-assigned-r1",
                                                         "polls": 0, "exit_code": None}
    stack.save()
    r = stack.run("deploy.sh", "r2", env={"COMPOSE_PROFILES": "multi-worker"})
    assert r.returncode == 0, _explain(r)
    events = sorted((i, sub, c["argv"][-1]) for sub in ("stop", "up") for i, c in stack.compose_calls(sub)
                    if c["argv"][-1] in ("bot-trading", "bot-trading-assigned"))
    assert [(sub, svc) for _, sub, svc in events] == [
        ("stop", "bot-trading"), ("up", "bot-trading"), ("stop", "bot-trading-assigned"), ("up", "bot-trading-assigned")]
    assert stack.running("bot-trading") == ("r2", True) and stack.running("bot-trading-assigned") == ("r2", True)
    assert {d["service"] for d in stack.drains()} == {"bot-trading", "bot-trading-assigned"}
    assert len(list(stack.deploy_state.glob("verdict-*-bot-trading-assigned.json"))) == 1


@needs_bash
@matrix
def test_default_topology_never_touches_the_optional_worker(stack):
    stack.release("r1")
    stack.release("r2")
    stack.serving("r1")
    r = stack.run("deploy.sh", "r2")
    assert r.returncode == 0, _explain(r)
    assert not [c for c in stack.calls() if "bot-trading-assigned" in c["argv"]]


def test_compose_topology_contract():
    """What the scripts and the runbook rely on in the compose file (Docker itself validates it in the CI job)."""
    yaml = pytest.importorskip("yaml")
    doc = yaml.safe_load((STAGING / "docker-compose.staging.yml").read_text(encoding="utf-8"))
    services = doc["services"]
    trading, api, extra = services["bot-trading"], services["bot-api"], services["bot-trading-assigned"]
    assert trading["environment"]["PROCESS_ROLE"] == "trading" and api["environment"]["PROCESS_ROLE"] == "api"
    assert trading["deploy"]["replicas"] == 1 and trading["stop_signal"] == "SIGTERM"
    assert int(trading["stop_grace_period"].rstrip("s")) >= 75
    assert "ready=1" in " ".join(trading["healthcheck"]["test"])
    for svc in ("bot-trading", "bot-api", "user-backend", "admin-backend", "bot-trading-assigned"):
        assert services[svc]["depends_on"]["migrate"]["condition"] == "service_completed_successfully", svc
        assert services[svc]["read_only"] is True and services[svc]["logging"]["options"]["max-file"], svc
    assert services["migrate"]["depends_on"]["postgres"]["condition"] == "service_healthy"
    anchor = "/var/lib/cosmicforge-anchor"
    for svc in (trading, api, extra):
        assert svc["environment"]["GOLIVE_REVOCATION_ANCHOR"].startswith(anchor + "/")
        assert any(v.endswith(":" + anchor) and "golive-anchor" in v for v in svc["volumes"])
        assert not any(v.startswith("runtime-state:" + anchor) for v in svc["volumes"])   # never on a DB volume
    assert set(doc["volumes"]) >= {"pg-data", "runtime-state", "golive-anchor"}
    # the optional worker: profile-gated, assigned mode, everything else like the trading process
    assert extra["profiles"] == ["multi-worker"] and extra["environment"]["BROKER_CYCLE_ASSIGNMENT_MODE"] == "assigned"
    assert extra["environment"]["PROCESS_ROLE"] == "trading" and extra["stop_grace_period"] == trading["stop_grace_period"]
    assert all("profiles" not in s for name, s in services.items() if name != "bot-trading-assigned")
    dockerfile = (REPO / "deploy" / "containers" / "backend.Dockerfile").read_text(encoding="utf-8")
    assert '"--timeout-graceful-shutdown", "20"' in dockerfile and "/var/lib/cosmicforge-anchor" in dockerfile


# ── the release gate on its own ────────────────────────────────────────────────────────────────────────────
def _gate():
    spec = importlib.util.spec_from_file_location("staging_release_gate", STAGING / "release_gate.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_release_gate_verdicts():
    g = _gate()
    supervised = {"process_role": "trading", "runtime": {"state": "HEALTHY"}, "components": {"runtime_owns_lease": True},
                  "discovered_accounts": 3, "synced_accounts": 3, "reconciliation_health": "SYNCED",
                  "live_system_gate": False}
    v = lambda *a, **k: g.evaluate(*a, **k)["verdict"]          # noqa: E731
    assert v(200, supervised, emergency_status=401) == "READY"
    assert v(200, dict(supervised, discovered_accounts=0, synced_accounts=0, reconciliation_health="NOT_SYNCED")) == "READY"
    assert v(200, dict(supervised, synced_accounts=2)) == "DEGRADED"
    assert v(200, dict(supervised, reconciliation_health="NOT_SYNCED")) == "DEGRADED"
    assert v(200, dict(supervised, live_system_gate=True)) == "UNSAFE"
    assert v(503, dict(supervised, live_system_gate=True)) == "UNSAFE"
    assert v(503, dict(supervised, runtime={"state": "STARTING"})) == "NOT_READY"
    assert v(200, dict(supervised, runtime={"state": "FAILING"})) == "NOT_READY"
    assert v(200, dict(supervised, components={"runtime_owns_lease": False})) == "NOT_READY"
    assert v(200, dict(supervised, process_role="api")) == "NOT_READY"
    assert v(200, supervised, emergency_status=409) == "NOT_READY"      # an API replica answered
    assert v(200, supervised, emergency_status=404) == "NOT_READY"      # emergency router not mounted
    assert v(0, None) == "NOT_READY" and v(200, None) == "NOT_READY" and v(502, "x") == "NOT_READY"
    unsupervised = {"process_role": "trading", "status": "ok"}
    assert v(200, unsupervised) == "READY_UNSUPERVISED"
    assert v(200, unsupervised, require_supervised=True) == "NOT_READY"
    assert g.EXIT == {"READY": 0, "READY_UNSUPERVISED": 0, "NOT_READY": 10, "DEGRADED": 11, "UNSAFE": 12}


def test_release_gate_cli_reads_stdin_and_exits_with_the_verdict():
    body = json.dumps({"process_role": "trading"})
    r = subprocess.run([sys.executable, str(STAGING / "release_gate.py"), "--http-status", "200"], input=body,
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0 and json.loads(r.stdout)["verdict"] == "READY_UNSUPERVISED"
    r = subprocess.run([sys.executable, str(STAGING / "release_gate.py"), "--http-status", "0"], input="",
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 10 and json.loads(r.stdout)["reason"] == "NO_ANSWER"
