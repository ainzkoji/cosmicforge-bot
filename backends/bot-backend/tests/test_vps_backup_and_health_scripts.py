"""The two operator scripts a server deployment depends on.

``backup_trading_db.py`` must produce a consistent, verified copy of a WAL
database that is being written to, and must fail loudly rather than leave a
bad file behind. ``vps_health_check.py`` must turn the runtime's own health
report into an exit status, and must never read missing evidence as a pass.
"""
from __future__ import annotations

import gzip
import importlib.util
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[3] / "scripts"


def load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


backup = load("backup_trading_db")
health_check = load("vps_health_check")


@pytest.fixture
def live_database(tmp_path):
    """A WAL database with a writer connection still open, as in production."""
    path = tmp_path / "source" / "cosmicforge.db"
    path.parent.mkdir()
    writer = sqlite3.connect(str(path))
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute("PRAGMA wal_autocheckpoint=0")       # keep every commit in the WAL
    writer.execute("CREATE TABLE intents(id INTEGER PRIMARY KEY, body TEXT)")
    writer.executemany("INSERT INTO intents(body) VALUES (?)", [("x" * 500,) for _ in range(400)])
    writer.commit()
    assert Path(str(path) + "-wal").stat().st_size > 0   # the main file alone is incomplete
    yield path, writer
    writer.close()


def run(source, out, *extra):
    return backup.main(["--database", str(source), "--output-dir", str(out), *extra])


def backups_in(out):
    return sorted(p for p in out.iterdir() if backup.NAME.match(p.name))


def test_backup_captures_committed_work_still_in_the_wal(live_database, tmp_path):
    source, writer = live_database
    out = tmp_path / "backups"
    assert run(source, out) == backup.EXIT_OK
    [copy] = backups_in(out)
    # An uncommitted transaction in flight is not part of the snapshot.
    writer.execute("INSERT INTO intents(body) VALUES ('uncommitted')")
    restored = sqlite3.connect(str(copy))
    assert restored.execute("SELECT COUNT(*) FROM intents").fetchone()[0] == 400
    assert restored.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    assert restored.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    restored.close()
    # One self-contained file, described by a manifest that matches it.
    assert not Path(str(copy) + "-wal").exists()
    manifest = json.loads(Path(str(copy) + ".json").read_text())
    assert manifest["quick_check"] == "ok" and manifest["sha256"] == backup.sha256_of(copy)
    assert manifest["size_bytes"] == copy.stat().st_size
    # The source was not disturbed.
    writer.rollback()
    assert writer.execute("SELECT COUNT(*) FROM intents").fetchone()[0] == 400


def test_a_plain_file_copy_of_the_same_database_would_have_lost_the_data(live_database, tmp_path):
    source, _writer = live_database
    naive = tmp_path / "naive.db"
    naive.write_bytes(source.read_bytes())               # what `cp cosmicforge.db` does
    conn = sqlite3.connect(str(naive))
    try:
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute("SELECT COUNT(*) FROM intents").fetchone()
    finally:
        conn.close()


def test_compressed_backup_restores_to_a_sound_database(live_database, tmp_path):
    source, _writer = live_database
    out = tmp_path / "backups"
    assert run(source, out, "--compress") == backup.EXIT_OK
    [packed] = backups_in(out)
    assert packed.name.endswith(".db.gz")
    restored = tmp_path / "restored.db"
    restored.write_bytes(gzip.decompress(packed.read_bytes()))
    assert backup.quick_check(restored) == "ok"
    assert not list(out.glob("*.partial"))


def test_retention_keeps_only_the_newest(live_database, tmp_path):
    source, _writer = live_database
    out = tmp_path / "backups"
    out.mkdir()
    for day in ("01", "02", "03", "04"):
        (out / f"cosmicforge-202610{day}T000000Z.db").write_bytes(b"old")
        (out / f"cosmicforge-202610{day}T000000Z.db.json").write_text("{}")
    (out / "unrelated.db").write_bytes(b"not ours")
    assert run(source, out, "--keep", "2") == backup.EXIT_OK
    kept = [p.name for p in backups_in(out)]
    assert len(kept) == 2 and "cosmicforge-20261004T000000Z.db" in kept
    assert not (out / "cosmicforge-20261001T000000Z.db.json").exists()
    assert (out / "unrelated.db").exists()                # never touches other files


def test_a_copy_that_fails_its_integrity_check_is_discarded(live_database, tmp_path, monkeypatch):
    source, _writer = live_database
    out = tmp_path / "backups"
    monkeypatch.setattr(backup, "quick_check", lambda path: "row 7 missing from index intents_idx")
    assert run(source, out) == backup.EXIT_INTEGRITY_FAILED
    assert list(out.iterdir()) == []


def test_missing_or_unreadable_sources_fail_with_a_distinct_status(tmp_path):
    out = tmp_path / "backups"
    assert run(tmp_path / "absent.db", out) == backup.EXIT_USAGE
    garbage = tmp_path / "garbage.db"
    garbage.write_bytes(b"this is not a database" * 100)
    assert run(garbage, out) in (backup.EXIT_BACKUP_FAILED, backup.EXIT_INTEGRITY_FAILED)
    assert not backups_in(out) and not list(out.glob("*.partial"))
    assert run(garbage, out, "--keep", "0") == backup.EXIT_USAGE


def test_the_source_is_resolved_exactly_as_the_backend_resolves_it(monkeypatch, tmp_path):
    absolute = tmp_path / "data" / "cosmicforge.db"
    assert backup.database_from_url("sqlite:///" + absolute.as_posix()) == absolute
    relative = backup.database_from_url("sqlite:///../shared/shared_lib/persistence/cosmicforge.db")
    assert relative == (backup.BACKEND / ".." / "shared" / "shared_lib" / "persistence" / "cosmicforge.db").resolve()
    assert backup.database_from_url("postgresql://host/db") is None
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + absolute.as_posix())
    assert backup.resolve_source(None) == absolute


# ── vps_health_check ────────────────────────────────────────────────────────


def healthy():
    return {
        "status": "ok", "runtime_revision": "b8e60749127c", "cati_production_running": True,
        "discovered_accounts": 1, "synced_accounts": 1, "reconciliation_health": "SYNCED",
        "risk_engine_health": "HEALTHY", "market_data_status": "COLLECTING",
        "demo_system_gate": True, "live_system_gate": False,
        "components": {"runtime_owns_lease": True},
        "production_configuration": {"DATABASE ROLE": "PRODUCTION"},
        "runtime": {"state": "HEALTHY", "faults": [], "warnings": [],
                    "database": {"size_bytes": 8 * 1024 ** 3, "disk_free_bytes": 40 * 1024 ** 3},
                    "lease": {"held_by_this_process": True, "heartbeat_age_seconds": 3.0},
                    "scheduler": {"runner_loop_alive": True, "last_cycle_completed_age_seconds": 11.0}},
        "trading": {"auto_trading_enabled_accounts": 1, "kill_switch_engaged_accounts": 0,
                    "broker_sync_max_age_seconds": 14.0, "execution_portfolio_states": ["AVAILABLE"],
                    "latest_decision": {"age_seconds": 900.0, "reason": "SELECTED_TOP1", "symbol": "ADAUSDT"}},
    }


def verdict(health, **kw):
    options = dict(max_broker_sync_age=180, max_decision_age=7800, allow_live_gate=False, expect_revision=None)
    checks = health_check.evaluate(health, **{**options, **kw})
    worst = max((level for level, _, _ in checks), key=lambda level: health_check.EXIT[level])
    return health_check.EXIT[worst], {name: level for level, name, _ in checks}


def test_a_trading_capable_runtime_is_healthy():
    code, checks = verdict(healthy())
    assert code == 0 and set(checks.values()) == {"OK"}


@pytest.mark.parametrize("path,value,check", [
    (("runtime", "state"), "FAILING", "runtime_supervisor"),
    (("runtime", "lease", "heartbeat_age_seconds"), 400.0, "runtime_lease"),
    (("components", "runtime_owns_lease"), False, "runtime_lease"),
    (("cati_production_running",), False, "trading_scheduler"),
    (("production_configuration", "DATABASE ROLE"), "PAPER", "database_role"),
    (("synced_accounts",), 0, "broker_sync"),
    (("trading", "broker_sync_max_age_seconds"), 900.0, "broker_sync"),
    (("market_data_status",), "STALE", "market_data"),
    (("trading", "latest_decision", "age_seconds"), 20_000.0, "cati_decision"),
    (("live_system_gate",), True, "live_order_gate"),
    (("runtime", "database", "disk_free_bytes"), 200 * 1024 ** 2, "database_disk"),
])
def test_each_condition_that_stops_trading_is_unhealthy(path, value, check):
    health = healthy()
    target = health
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    code, checks = verdict(health)
    assert code == 2 and checks[check] == "FAIL"


def test_missing_evidence_is_never_a_pass():
    code, checks = verdict({"status": "ok"})
    assert code == 2
    assert checks["runtime_lease"] == checks["broker_sync"] == checks["cati_decision"] == "FAIL"


def test_attention_states_are_degraded_not_unhealthy_and_revision_can_be_pinned():
    health = healthy()
    health["trading"]["kill_switch_engaged_accounts"] = 1
    assert verdict(health)[0] == 1
    filling = healthy()
    filling["runtime"]["database"]["disk_free_bytes"] = 3 * 1024 ** 3
    assert verdict(filling) == (1, {**verdict(healthy())[1], "database_disk": "WARN"})
    assert verdict(healthy(), expect_revision="b8e60749")[0] == 0
    assert verdict(healthy(), expect_revision="deadbeef")[0] == 2
    live = healthy()
    live["live_system_gate"] = True
    assert verdict(live, allow_live_gate=True)[0] == 0


def test_an_unreachable_backend_is_unhealthy(capsys):
    assert health_check.main(["--url", "http://127.0.0.1:9", "--timeout", "1", "--json"]) == 2
    report = json.loads(capsys.readouterr().out)
    assert report["verdict"] == "UNHEALTHY" and report["checks"][0]["check"] == "api"


# ── notify_failure ──────────────────────────────────────────────────────────
#
# The alert a human receives when a unit fails. No test here opens a socket:
# the HTTP and SMTP entry points are replaced.

notify = load("notify_failure")
offsite = load("offsite_backup")

WEBHOOK = "https://hooks.example.test/services/T000/B000/s3cretWebhookPath"
BOT_TOKEN = "123456789:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsaw"
ALERT_CONFIG = {"ALERT_WEBHOOK_URL": WEBHOOK, "ALERT_TELEGRAM_BOT_TOKEN": BOT_TOKEN,
                "ALERT_TELEGRAM_CHAT_ID": "-100200300", "ALERT_EMAIL_TO": "ops@example.test, oncall@example.test",
                "SMTP_HOST": "smtp.example.test", "SMTP_USER": "mailer", "SMTP_PASSWORD": "smtp-pa55word-value"}
WHEN = datetime(2026, 10, 7, 3, 0, 5, tzinfo=timezone.utc)


class _Response:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return b"ok"


@pytest.fixture
def outbox(monkeypatch):
    """Every outgoing alert, captured instead of sent; the journal is canned."""
    sent = {"http": [], "mail": [], "fail_http": None}

    def urlopen(request, timeout=None):
        if sent["fail_http"] and sent["fail_http"] in request.full_url:
            raise ValueError(f"unknown url type: {request.full_url!r}")      # an error text that quotes the URL
        sent["http"].append({"url": request.full_url, "timeout": timeout,
                             "body": json.loads(request.data.decode("utf-8"))})
        return _Response()

    class SMTP:
        def __init__(self, host, port, timeout=None):
            self.record = {"host": host, "port": port, "timeout": timeout, "tls": False, "login": None}

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def starttls(self, context=None):
            self.record["tls"] = True

        def login(self, user, password):
            self.record["login"] = user

        def send_message(self, mail, to_addrs=None):
            sent["mail"].append({**self.record, "to": to_addrs, "subject": mail["Subject"],
                                 "body": mail.get_content()})

    monkeypatch.setattr(notify.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(notify.smtplib, "SMTP", SMTP)
    monkeypatch.setattr(notify, "journal_tail", lambda unit, lines=20: [
        "Oct 07 03:00:01 vps uvicorn[811]: [RUNTIME_SUPERVISOR] FAILING faults=['LEASE_STALE']",
        "Oct 07 03:00:04 vps systemd[1]: cosmicforge-trading.service: Main process exited, status=70"])
    monkeypatch.setattr(notify, "unit_state", lambda unit: "ActiveState=activating SubState=auto-restart Result=exit-code")
    return sent


def alert(tmp_path, config, *argv):
    env_file = tmp_path / "alerts.env"
    env_file.write_text("# alert channels\n" + "".join(f"{key}={value}\n" for key, value in config.items()))
    return notify.main([*argv, "--env-file", str(env_file)], environ={})


def test_the_alert_names_the_unit_the_host_the_time_and_the_last_log_lines():
    lines = [f"Oct 07 03:00:0{n} vps uvicorn[811]: line {n}" for n in range(5)]
    message = notify.build_message("cosmicforge-trading.service", "vps-1", WHEN + timedelta(0), lines,
                                   "ActiveState=failed Result=watchdog")
    head = message.splitlines()
    assert head[0] == "[CosmicForge ALERT] cosmicforge-trading.service failed"
    assert head[1:4] == ["host: vps-1", "time: 2026-10-07 03:00:05 UTC", "state: ActiveState=failed Result=watchdog"]
    assert head[-5:] == lines
    # The time is always reported in UTC, whatever zone it was taken in.
    local = WHEN.astimezone(timezone(timedelta(hours=2)))
    assert "time: 2026-10-07 03:00:05 UTC" in notify.build_message("x.service", "h", local)
    # An unreadable journal is said, not hidden.
    assert "no journal lines could be read" in notify.build_message("x.service", "h", WHEN, [])


@pytest.mark.parametrize("line,secret,kept", [
    ("GET /fapi/v1/order?symbol=ADAUSDT&signature=9f86d081884c7d65&timestamp=1", "9f86d081884c7d65", "symbol=ADAUSDT"),
    ("retry api_key=AKIAIOSFODNN7EXAMPLE secret=wJalrXUtnFEMI/K7MDENG attempt=3", "AKIAIOSFODNN7EXAMPLE", "attempt=3"),
    ("retry api_key=AKIAIOSFODNN7EXAMPLE secret=wJalrXUtnFEMI/K7MDENG attempt=3", "wJalrXUtnFEMI", "retry"),
    ("env BROKER_SECRET_KEY=Zm9vYmFyYmF6cXV4 loaded", "Zm9vYmFyYmF6cXV4", "loaded"),
    ("login password=hunter2-hunter2 user=bob", "hunter2-hunter2", "user=bob"),
    ("refresh token=eyJhbGciOiJIUzI1NiJ9.e30.abc123 ok", "eyJhbGciOiJIUzI1NiJ9", "ok"),
    ("headers Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.e30.sig path=/x", "eyJhbGciOiJIUzI1NiJ9", "path=/x"),
    ("headers {'Authorization': 'Basic dXNlcjpwYXNzd29yZA=='}", "dXNlcjpwYXNzd29yZA", "headers"),
    ('body {"password": "correct horse battery", "user": "bob"}', "correct horse battery", '"user": "bob"'),
    ("X-MBX-APIKEY: vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A", "vmPUZE6mv9SD5VNHk4", "X-MBX-APIKEY"),
    ("connect postgresql://admin:pa55w0rd@db.internal/cosmic failed", "pa55w0rd", "db.internal/cosmic"),
    (f"POST https://api.telegram.org/bot{BOT_TOKEN}/sendMessage 502", "AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsaw", "sendMessage 502"),
])
def test_anything_that_looks_like_a_key_or_token_is_masked_in_journal_lines(line, secret, kept):
    message = notify.build_message("cosmicforge-trading.service", "vps-1", WHEN, [line])
    assert secret not in message
    assert kept in message and notify.MASK in message
    # Ordinary runtime lines pass through untouched.
    plain = "[RUNTIME_SUPERVISOR] READY lease_owner_pid=811 runtime_session_id=rts_42 reason=SELECTED_TOP1"
    assert plain in notify.build_message("cosmicforge-trading.service", "vps-1", WHEN, [plain])


def test_configured_secrets_and_the_environment_never_reach_the_message(monkeypatch):
    monkeypatch.setenv("BROKER_SECRET_KEY", "environment-only-value-1234567890")
    leaked = [f"notifier failed posting to {WEBHOOK}", "smtp said: smtp-pa55word-value rejected"]
    message = notify.build_message("cosmicforge-trading.service", "vps-1", WHEN, leaked,
                                   secrets=notify.secret_values(ALERT_CONFIG))
    for value in (WEBHOOK, "s3cretWebhookPath", "smtp-pa55word-value", "environment-only-value-1234567890"):
        assert value not in message
    assert "notifier failed posting to" in message
    test_message = notify.build_message("delivery-test", "vps-1", WHEN, test=True)
    assert "TEST" in test_message and "environment-only-value" not in test_message


def test_a_long_journal_is_shortened_keeping_the_header_and_the_newest_lines():
    lines = [f"line-{n:03d} " + "x" * 500 for n in range(40)]
    message = notify.build_message("cosmicforge-trading.service", "vps-1", WHEN, lines, "ActiveState=failed")
    assert max(len(row) for row in message.splitlines()) <= notify.MAX_LINE_CHARS
    short = notify.fit(message, notify.WEBHOOK_MAX_CHARS)
    assert len(short) <= notify.WEBHOOK_MAX_CHARS
    assert short.startswith("[CosmicForge ALERT] cosmicforge-trading.service failed\nhost: vps-1")
    assert "line-039" in short and "line-000" not in short and "older lines omitted" in short
    assert notify.fit("short", 100) == "short"


def test_every_configured_channel_receives_the_alert_and_only_those(tmp_path, outbox):
    assert alert(tmp_path, ALERT_CONFIG, "cosmicforge-trading.service") == 0
    webhook, telegram = outbox["http"]
    # Slack-compatible receivers read `text`, Discord-compatible ones `content`.
    assert webhook["url"] == WEBHOOK and webhook["body"]["text"] == webhook["body"]["content"]
    assert "cosmicforge-trading.service failed" in webhook["body"]["text"]
    assert "LEASE_STALE" in webhook["body"]["text"] and "Result=exit-code" in webhook["body"]["text"]
    assert telegram["url"] == f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    assert telegram["body"]["chat_id"] == "-100200300" and "LEASE_STALE" in telegram["body"]["text"]
    assert webhook["timeout"] == telegram["timeout"] == 10
    [mail] = outbox["mail"]
    assert mail["host"] == "smtp.example.test" and mail["port"] == 587 and mail["timeout"] == 10
    assert mail["tls"] and mail["login"] == "mailer"
    assert mail["to"] == ["ops@example.test", "oncall@example.test"]
    assert "cosmicforge-trading.service failed" in mail["subject"] and "LEASE_STALE" in mail["body"]

    outbox["http"].clear(), outbox["mail"].clear()
    assert alert(tmp_path, {"ALERT_TELEGRAM_BOT_TOKEN": BOT_TOKEN}, "cosmicforge-trading.service") == 0
    assert outbox["http"] == [] and outbox["mail"] == []        # a token without a chat id is not a channel
    assert [name for name, _ in notify.configured_channels({"ALERT_WEBHOOK_URL": WEBHOOK})] == ["webhook"]


def test_a_failing_channel_neither_fails_the_alert_nor_stops_the_others_nor_leaks(tmp_path, outbox, capsys):
    outbox["fail_http"] = "hooks.example.test"
    assert alert(tmp_path, ALERT_CONFIG, "cosmicforge-trading.service") == 0     # exit 0: the alert unit must not fail
    assert [call["url"] for call in outbox["http"]] == [f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"]
    assert len(outbox["mail"]) == 1
    captured = capsys.readouterr()
    assert "channel=webhook FAILED" in captured.err and "failed=webhook" in captured.err
    for secret in (WEBHOOK, "s3cretWebhookPath", BOT_TOKEN, "smtp-pa55word-value"):
        assert secret not in captured.err and secret not in captured.out
    # Nothing configured at all: said loudly, still exit 0 for a real alert.
    assert alert(tmp_path, {}, "cosmicforge-trading.service") == 0
    assert "NO ALERT CHANNEL IS CONFIGURED" in capsys.readouterr().err


def test_the_test_flag_proves_delivery_and_fails_when_nothing_is_delivered(tmp_path, outbox, monkeypatch):
    def no_journal(*args, **kwargs):
        raise AssertionError("a delivery test reads no journal")

    monkeypatch.setattr(notify, "journal_tail", no_journal)
    assert alert(tmp_path, {"ALERT_WEBHOOK_URL": WEBHOOK}, "--test") == 0
    [call] = outbox["http"]
    assert "TEST" in call["body"]["text"] and "nothing is wrong" in call["body"]["text"]
    assert alert(tmp_path, {}, "--test") == 1                                    # no channel configured
    outbox["fail_http"] = "hooks.example.test"
    assert alert(tmp_path, {"ALERT_WEBHOOK_URL": WEBHOOK}, "--test") == 1        # configured, but it failed


def test_a_repeating_failure_is_not_sent_again_within_the_cooldown(tmp_path, outbox):
    state = tmp_path / "state"
    args = ("cosmicforge-healthcheck.service", "--cooldown", "900", "--state-dir", str(state))
    config = {"ALERT_WEBHOOK_URL": WEBHOOK}
    assert alert(tmp_path, config, *args) == 0 and len(outbox["http"]) == 1
    assert alert(tmp_path, config, *args) == 0 and len(outbox["http"]) == 1      # suppressed
    # Another unit is a different alert; and an alert that reached nobody is not remembered.
    assert alert(tmp_path, config, "cosmicforge-trading.service", *args[1:]) == 0 and len(outbox["http"]) == 2
    outbox["fail_http"] = "hooks.example.test"
    other = ("cosmicforge-db-backup.service", *args[1:])
    assert alert(tmp_path, config, *other) == 0
    outbox["fail_http"] = None
    assert alert(tmp_path, config, *other) == 0 and len(outbox["http"]) == 3


def test_only_a_unit_name_is_accepted_as_the_unit(tmp_path, outbox):
    assert alert(tmp_path, {"ALERT_WEBHOOK_URL": WEBHOOK}, "x.service; rm -rf /") == 2
    assert alert(tmp_path, {"ALERT_WEBHOOK_URL": WEBHOOK}) == 2                  # neither a unit nor --test
    assert outbox["http"] == []
    assert alert(tmp_path, {"ALERT_WEBHOOK_URL": WEBHOOK}, "cosmicforge-alert@test.service") == 0


# ── offsite_backup ──────────────────────────────────────────────────────────
#
# Which local backup leaves the server, the refusal to send it unencrypted,
# and the exact commands handed to age / gpg / rclone / rsync. The external
# tools are never run: `offsite.run` is replaced.


def stamp(hours_ago=1.0):
    return (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).strftime("%Y%m%dT%H%M%SZ")


def make_backup(directory, when, body=b"sqlite-backup-bytes", *, manifest=True, quick_check="ok", suffix=".db.gz"):
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"cosmicforge-{when}{suffix}"
    path.write_bytes(body)
    if manifest:
        Path(str(path) + ".json").write_text(json.dumps({
            "file": path.name, "size_bytes": len(body), "quick_check": quick_check,
            "sha256": backup.sha256_of(path)}))
    return path


class Tools:
    """Stands in for `offsite.run`: records each command and what was staged."""

    def __init__(self, fail=None, rsync_report=""):
        self.calls, self.staged, self.fail, self.rsync_report = [], [], fail, rsync_report

    def __call__(self, command, timeout):
        self.calls.append(command)
        failed = self.fail is not None and command[:len(self.fail)] == self.fail
        if command[0] in ("age", "gpg") and not failed:
            target = command[command.index("-o" if command[0] == "age" else "--output") + 1]
            Path(target).write_bytes(b"ciphertext")
        if command[:2] == ["rclone", "copy"] or (command[0] == "rsync" and "--dry-run" not in command):
            self.staged.append(sorted(p.name for p in Path(command[-2].rstrip("/")).iterdir()))
        report = self.rsync_report if "--dry-run" in command else ""
        return offsite.subprocess.CompletedProcess(command, 1 if failed else 0, stdout=report, stderr="boom" if failed else "")


def run_offsite(monkeypatch, backups, settings, *argv, tools=None, installed=True):
    tools = tools or Tools()
    monkeypatch.setattr(offsite, "run", tools)
    monkeypatch.setattr(offsite.shutil, "which", lambda name: f"/usr/bin/{name}" if installed else None)
    code = offsite.main(["--env-file", str(backups / "absent.env"), "--backup-dir", str(backups), *argv],
                        environ=settings)
    return code, tools


def test_offsite_selects_backups_by_the_name_the_backup_script_writes():
    assert offsite.NAME.pattern == backup.NAME.pattern
    assert offsite.backup_time("cosmicforge-20261004T033012Z.db.gz") == datetime(2026, 10, 4, 3, 30, 12, tzinfo=timezone.utc)


def test_the_newest_verified_backup_is_the_one_selected(tmp_path):
    now = datetime(2026, 10, 7, 4, 30, tzinfo=timezone.utc)
    make_backup(tmp_path, "20261005T033000Z")
    expected = make_backup(tmp_path, "20261006T033000Z", b"the good one")
    make_backup(tmp_path, "20261007T033000Z", quick_check="row 7 missing from index")      # failed verification
    make_backup(tmp_path, "20261007T040000Z", manifest=False)                              # still being written
    (tmp_path / "cosmicforge-20261007T041500Z.db.gz.partial").write_bytes(b"partial")
    (tmp_path / "unrelated.db").write_bytes(b"not ours")
    chosen, manifest = offsite.newest_verified_backup(tmp_path, 36, now)
    assert chosen == expected and manifest["sha256"] == backup.sha256_of(expected)


def test_a_stale_missing_or_altered_backup_is_refused(tmp_path):
    now = datetime(2026, 10, 7, 4, 30, tzinfo=timezone.utc)
    with pytest.raises(offsite.OffsiteError) as empty:
        offsite.newest_verified_backup(tmp_path, 36, now)
    assert empty.value.code == offsite.EXIT_NO_BACKUP
    old = make_backup(tmp_path, "20261001T033000Z")
    with pytest.raises(offsite.OffsiteError) as stale:                  # local backups have stopped
        offsite.newest_verified_backup(tmp_path, 36, now)
    assert stale.value.code == offsite.EXIT_NO_BACKUP and "old" in str(stale.value)
    assert offsite.newest_verified_backup(tmp_path, 24 * 30, now)[0] == old
    old.write_bytes(b"sqlite-backup-bytez")                             # same size, different content
    with pytest.raises(offsite.OffsiteError) as altered:
        offsite.newest_verified_backup(tmp_path, 24 * 30, now)
    assert altered.value.code == offsite.EXIT_INTEGRITY


def test_an_unencrypted_upload_is_refused_unless_explicitly_allowed(tmp_path, monkeypatch, capsys):
    source = make_backup(tmp_path, stamp())
    code, tools = run_offsite(monkeypatch, tmp_path, {"BACKUP_RCLONE_REMOTE": "b2:bucket/cosmicforge"})
    assert code == offsite.EXIT_CONFIG and tools.calls == []
    assert "REFUSED" in capsys.readouterr().out
    for not_true in ("false", "0", "no", ""):
        code, tools = run_offsite(monkeypatch, tmp_path, {"BACKUP_RCLONE_REMOTE": "b2:bucket/cosmicforge",
                                                         "BACKUP_ALLOW_PLAINTEXT_OFFSITE": not_true})
        assert code == offsite.EXIT_CONFIG and tools.calls == []

    code, tools = run_offsite(monkeypatch, tmp_path, {"BACKUP_RCLONE_REMOTE": "b2:bucket/cosmicforge",
                                                     "BACKUP_ALLOW_PLAINTEXT_OFFSITE": "true"})
    assert code == offsite.EXIT_OK
    assert [call[:2] for call in tools.calls] == [["rclone", "copy"], ["rclone", "check"]]
    assert tools.staged == [[source.name, source.name + ".json"]]
    assert "UNENCRYPTED" in capsys.readouterr().out


def test_the_backup_is_encrypted_uploaded_and_verified_with_these_commands(tmp_path, monkeypatch):
    source = make_backup(tmp_path, stamp())
    code, tools = run_offsite(monkeypatch, tmp_path, {
        "BACKUP_ENCRYPTION_RECIPIENT": "age1qqqsample", "BACKUP_RCLONE_REMOTE": "b2:bucket/cosmicforge",
        "BACKUP_RSYNC_TARGET": "backup@host:/srv/cosmicforge-backups"})
    assert code == offsite.EXIT_OK
    encrypt, copy, check, sync, compare = tools.calls
    staging = Path(copy[2])
    assert staging.parent == tmp_path and staging.name.startswith(offsite.STAGING_PREFIX)
    assert encrypt == ["age", "--encrypt", "-r", "age1qqqsample", "-o", str(staging / (source.name + ".age")), str(source)]
    assert copy == ["rclone", "copy", str(staging), "b2:bucket/cosmicforge"]
    assert check == ["rclone", "check", str(staging), "b2:bucket/cosmicforge", "--one-way"]
    assert sync == ["rsync", "-a", f"{staging}/", "backup@host:/srv/cosmicforge-backups/"]
    assert compare == ["rsync", "-a", "--checksum", "--dry-run", "--itemize-changes", f"{staging}/",
                       "backup@host:/srv/cosmicforge-backups/"]
    # Only the ciphertext and the manifest were staged, and nothing is left behind.
    assert tools.staged == [[source.name + ".age", source.name + ".json"]] * 2
    assert not staging.exists() and source.exists()

    gpg = offsite.encrypt_command("gpg", "ops@example.test", source, tmp_path / "out.gpg")
    assert gpg == ["gpg", "--batch", "--yes", "--trust-model", "always", "--recipient", "ops@example.test",
                   "--output", str(tmp_path / "out.gpg"), "--encrypt", str(source)]
    recipients = tmp_path / "recipients.txt"
    recipients.write_text("age1aaa\nage1bbb\n")
    assert offsite.encrypt_command("age", str(recipients), source, tmp_path / "o")[2:4] == ["-R", str(recipients)]
    assert offsite.encrypt_command("age", "age1aaa, age1bbb", source, tmp_path / "o")[2:6] == ["-r", "age1aaa", "-r", "age1bbb"]
    code, tools = run_offsite(monkeypatch, tmp_path, {"BACKUP_GPG_RECIPIENT": "ops@example.test",
                                                     "BACKUP_RSYNC_TARGET": "/mnt/other-disk/cosmicforge/"})
    assert code == offsite.EXIT_OK and tools.calls[0][0] == "gpg"
    assert tools.calls[1][-1] == "/mnt/other-disk/cosmicforge/" and tools.staged == [[source.name + ".gpg", source.name + ".json"]]


def test_a_dry_run_selects_and_reports_but_runs_nothing(tmp_path, monkeypatch, capsys):
    source = make_backup(tmp_path, stamp())
    code, tools = run_offsite(monkeypatch, tmp_path, {"BACKUP_ENCRYPTION_RECIPIENT": "age1qqqsample",
                                                     "BACKUP_RCLONE_REMOTE": "b2:bucket/cosmicforge"}, "--dry-run")
    out = capsys.readouterr().out
    assert code == offsite.EXIT_OK and tools.calls == []
    assert source.name in out and "DRY-RUN would run: age --encrypt -r age1qqqsample" in out
    assert "DRY-RUN would run: rclone copy" in out and "DRY-RUN would run: rclone check" in out
    assert sorted(p.name for p in tmp_path.iterdir()) == [source.name, source.name + ".json"]
    # A dry run still refuses what the real run would refuse.
    assert run_offsite(monkeypatch, tmp_path, {"BACKUP_RCLONE_REMOTE": "b2:bucket/x"}, "--dry-run")[0] == offsite.EXIT_CONFIG


@pytest.mark.parametrize("settings,tools,installed,expected", [
    ({"BACKUP_ENCRYPTION_RECIPIENT": "age1q"}, {}, True, "EXIT_CONFIG"),                       # no destination
    ({"BACKUP_ENCRYPTION_RECIPIENT": "age1q", "BACKUP_RCLONE_REMOTE": "b2:b/c"}, {}, False, "EXIT_CONFIG"),
    ({"BACKUP_ENCRYPTION_RECIPIENT": "age1q", "BACKUP_RCLONE_REMOTE": "b2:b/c"}, {"fail": ["age"]}, True, "EXIT_ENCRYPT"),
    ({"BACKUP_ENCRYPTION_RECIPIENT": "age1q", "BACKUP_RCLONE_REMOTE": "b2:b/c"},
     {"fail": ["rclone", "copy"]}, True, "EXIT_UPLOAD"),
    ({"BACKUP_ENCRYPTION_RECIPIENT": "age1q", "BACKUP_RCLONE_REMOTE": "b2:b/c"},
     {"fail": ["rclone", "check"]}, True, "EXIT_REMOTE_VERIFY"),
    ({"BACKUP_ENCRYPTION_RECIPIENT": "age1q", "BACKUP_RSYNC_TARGET": "h:/b"},
     {"rsync_report": ".d..t...... ./\n<f.st...... cosmicforge-x.db.gz.age\n"}, True, "EXIT_REMOTE_VERIFY"),
    ({"BACKUP_ENCRYPTION_RECIPIENT": "age1q", "BACKUP_RSYNC_TARGET": "h:/b"},
     {"rsync_report": ".d..t...... ./\n"}, True, "EXIT_OK"),                                    # only the directory time
])
def test_every_failure_is_a_non_zero_exit_so_systemd_alerts(tmp_path, monkeypatch, settings, tools, installed, expected):
    make_backup(tmp_path, stamp())
    code, used = run_offsite(monkeypatch, tmp_path, settings, tools=Tools(**tools), installed=installed)
    assert code == getattr(offsite, expected) and (code != 0 or expected == "EXIT_OK")
    assert not list(tmp_path.glob(offsite.STAGING_PREFIX + "*"))        # the staging copy never outlives the run
    # With no usable backup nothing is encrypted or uploaded at all.
    code, used = run_offsite(monkeypatch, tmp_path / "empty", {"BACKUP_ENCRYPTION_RECIPIENT": "age1q",
                                                              "BACKUP_RCLONE_REMOTE": "b2:b/c"})
    assert code == offsite.EXIT_NO_BACKUP and used.calls == []
