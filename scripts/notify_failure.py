#!/usr/bin/env python3
"""Tell a human that a CosmicForge unit failed.

systemd restarts a crashed service, but a restart is silent: nobody learns that
the trading runtime died at 03:00, or that it has been dying every ten seconds
since. This script is what ``cosmicforge-alert@.service`` runs with the failed
unit as its argument. It sends one short message to every channel configured
in the alerts env file:

    python3 scripts/notify_failure.py cosmicforge-trading.service
    python3 scripts/notify_failure.py --test          # verify delivery

The message holds the unit name, the host, the time (UTC), the unit's state as
systemd reports it and the last journal lines of the unit when they can be
read. It never holds a configuration or environment value, and anything in the
journal lines that looks like a key or token is masked before it is sent.

Channels (each is used only when its variables are set; see
``deploy/alerts.env.example``):

* ``ALERT_WEBHOOK_URL`` -- JSON POST with both ``text`` and ``content`` keys,
  which Slack-compatible and Discord-compatible webhooks accept;
* ``ALERT_TELEGRAM_BOT_TOKEN`` + ``ALERT_TELEGRAM_CHAT_ID``;
* ``ALERT_EMAIL_TO`` + ``SMTP_HOST`` (and ``SMTP_PORT``, ``SMTP_USER``,
  ``SMTP_PASSWORD``, ``SMTP_FROM_EMAIL`` as the user-backend names them).

Values are read from ``--env-file`` (default ``/etc/cosmicforge/alerts.env``)
and from the process environment, which wins.

Exit status: always 0 for a real alert, even when a channel fails or none is
configured -- the failure is logged to stderr (the journal) and must not turn
the alert unit itself into a failed unit. ``--test`` exits 1 when no channel
is configured or any configured channel fails, so the operator sees it. 2 is a
usage error. Every network call has a 10 second timeout. Standard library only.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import smtplib
import socket
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path

DEFAULT_ENV_FILE = "/etc/cosmicforge/alerts.env"
TIMEOUT_SECONDS = 10
JOURNAL_LINES = 20
MAX_LINE_CHARS = 300
MASK = "***REDACTED***"
# Discord rejects a `content` longer than 2000 characters; Telegram a text
# longer than 4096. The same shortened message is sent to both keys/channels.
WEBHOOK_MAX_CHARS = 1900
TELEGRAM_MAX_CHARS = 3900
CONFIG_KEYS = (
    "ALERT_WEBHOOK_URL", "ALERT_TELEGRAM_BOT_TOKEN", "ALERT_TELEGRAM_CHAT_ID", "ALERT_EMAIL_TO",
    "SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASSWORD", "SMTP_PASS", "SMTP_FROM_EMAIL", "SMTP_FROM",
)
# Values that must never appear in a message or in this script's own output.
SECRET_KEYS = ("ALERT_WEBHOOK_URL", "ALERT_TELEGRAM_BOT_TOKEN", "SMTP_PASSWORD", "SMTP_PASS")
UNIT_NAME = re.compile(r"^[A-Za-z0-9_.@:\\][A-Za-z0-9_.@:\\-]{0,255}$")

# The same idea as shared_lib.core.security.redaction (which masks the values
# the application logs): mask whatever follows a secret-looking name. Kept
# local so this script still runs when the application's environment does not.
_NAME = r"[A-Za-z0-9_.\-]*(?:key|token|secret|password|passwd|passphrase|signature|sign|credentials?|authorization)"
_REDACTIONS = (
    # "api_key": "value"  /  'token': 'value'
    (re.compile(rf"(?i)([\"']{_NAME}[\"']\s*:\s*[\"'])[^\"']+([\"'])"), rf"\g<1>{MASK}\g<2>"),
    # Authorization: Bearer <token>  /  Authorization=Basic <...>
    (re.compile(r"(?i)\b(authorization)(\s*[:=]\s*)(?:(?:bearer|basic|token)\s+)?[^\s,;'\"]+"), rf"\g<1>\g<2>{MASK}"),
    (re.compile(r"(?i)\b(bearer\s+)[A-Za-z0-9_\-.=+/]{8,}"), rf"\g<1>{MASK}"),
    # key=value, token=value, secret=value, password=value (query strings, log text)
    (re.compile(rf"(?i)\b({_NAME})(\s*=\s*)[^\s&'\",;}}]+"), rf"\g<1>\g<2>{MASK}"),
    # Header-Name: value (X-MBX-APIKEY: ..., api-key: ...)
    (re.compile(rf"(?i)\b({_NAME})(\s*:\s*)[A-Za-z0-9_\-.=+/]{{8,}}"), rf"\g<1>\g<2>{MASK}"),
    # scheme://user:password@host
    (re.compile(r"(?i)\b([a-z][a-z0-9+.\-]*://)[^\s/@:]+:[^\s/@]+@"), rf"\g<1>{MASK}@"),
    # A Telegram bot token wherever it appears (api.telegram.org/bot<id>:<secret>/...).
    (re.compile(r"\b(bot)?\d{6,}:[A-Za-z0-9_\-]{30,}"), MASK),
)


def log(message: str) -> None:
    print(f"[ALERT] {message}", file=sys.stderr, flush=True)


def redact(text: str, secrets: tuple[str, ...] | list[str] = ()) -> str:
    """Mask secret-looking values in free text, and every literal in ``secrets``."""
    out = str(text)
    for secret in sorted((s for s in secrets if s and len(s) >= 6), key=len, reverse=True):
        out = out.replace(secret, MASK)
    for pattern, replacement in _REDACTIONS:
        out = pattern.sub(replacement, out)
    return out


def parse_env_file(path: Path) -> dict[str, str]:
    """``KEY=value`` lines as systemd's EnvironmentFile= reads them (no expansion)."""
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line[0] in "#;" or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def load_config(env_file: str | None, environ) -> dict[str, str]:
    """The alert settings: the env file first, the process environment on top."""
    config: dict[str, str] = {}
    if env_file:
        try:
            config.update(parse_env_file(Path(env_file)))
        except FileNotFoundError:
            pass
        except OSError as exc:
            log(f"cannot read {env_file}: {type(exc).__name__}")
    for key in CONFIG_KEYS:
        if environ.get(key):
            config[key] = environ[key]
    return {key: value.strip() for key, value in config.items() if key in CONFIG_KEYS and value.strip()}


def secret_values(config: dict[str, str]) -> list[str]:
    return [config[key] for key in SECRET_KEYS if config.get(key)]


def _command_output(command: list[str]) -> str | None:
    try:
        done = subprocess.run(command, capture_output=True, text=True, errors="replace",
                              timeout=TIMEOUT_SECONDS, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    return done.stdout if done.returncode == 0 else None


def journal_tail(unit: str, lines: int = JOURNAL_LINES) -> list[str]:
    """The unit's last journal lines, or [] when the journal cannot be read."""
    output = _command_output(["journalctl", "-u", unit, "-n", str(lines), "--no-pager"])
    # "-- No entries --" and similar markers are not log lines.
    return [line for line in (output or "").splitlines() if line.strip() and not line.startswith("-- ")][-lines:]


def unit_state(unit: str) -> str:
    """``ActiveState=... Result=...`` as systemd reports it now; '' if unknown."""
    output = _command_output(["systemctl", "show", unit, "--no-pager",
                              "--property=ActiveState,SubState,Result,ExecMainStatus,NRestarts"])
    return " ".join(line.strip() for line in (output or "").splitlines() if "=" in line)


def build_message(unit: str, host: str, when: datetime, journal_lines: list[str] | tuple[str, ...] = (),
                  state: str = "", *, test: bool = False, secrets: tuple[str, ...] | list[str] = ()) -> str:
    """The alert text. Built only from its arguments -- never from the environment."""
    stamp = when.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    if test:
        head = ["[CosmicForge TEST] alert delivery check -- nothing is wrong", f"host: {host}", f"time: {stamp}",
                "If you can read this, this channel will receive real failure alerts."]
        return redact("\n".join(head), secrets)
    head = [f"[CosmicForge ALERT] {unit} failed", f"host: {host}", f"time: {stamp}"]
    if state:
        head.append(f"state: {state}")
    if journal_lines:
        head.append(f"last {len(journal_lines)} log lines:")
        head.extend(line.rstrip()[:MAX_LINE_CHARS] for line in journal_lines)
    else:
        head.append(f"(no journal lines could be read; on the server: journalctl -u {unit} -n 50 --no-pager)")
    return redact("\n".join(head), secrets)


def fit(message: str, limit: int) -> str:
    """Shorten to ``limit`` characters, keeping the header and the NEWEST log lines."""
    if len(message) <= limit:
        return message
    lines = message.split("\n")
    head, tail, marker = lines[:4], lines[4:], "[... older lines omitted ...]"
    while tail and len("\n".join([*head, marker, *tail])) > limit:
        tail.pop(0)
    return "\n".join([*head, marker, *tail])[:limit]


def _post_json(url: str, payload: dict) -> None:
    request = urllib.request.Request(
        url, data=json.dumps(payload).encode("utf-8"), method="POST",
        # Some webhook hosts refuse the default Python-urllib agent.
        headers={"Content-Type": "application/json", "User-Agent": "cosmicforge-alert/1"})
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        response.read()


def send_webhook(config: dict[str, str], subject: str, message: str) -> None:
    text = fit(message, WEBHOOK_MAX_CHARS)
    _post_json(config["ALERT_WEBHOOK_URL"], {"text": text, "content": text})


def send_telegram(config: dict[str, str], subject: str, message: str) -> None:
    token = urllib.parse.quote(config["ALERT_TELEGRAM_BOT_TOKEN"], safe=":")
    _post_json(f"https://api.telegram.org/bot{token}/sendMessage",
               {"chat_id": config["ALERT_TELEGRAM_CHAT_ID"], "text": fit(message, TELEGRAM_MAX_CHARS),
                "disable_web_page_preview": True})


def send_email(config: dict[str, str], subject: str, message: str) -> None:
    recipients = [address.strip() for address in config["ALERT_EMAIL_TO"].split(",") if address.strip()]
    user = config.get("SMTP_USER", "")
    password = config.get("SMTP_PASSWORD") or config.get("SMTP_PASS") or ""
    mail = EmailMessage()
    mail["Subject"] = subject
    mail["From"] = config.get("SMTP_FROM_EMAIL") or config.get("SMTP_FROM") or user or f"cosmicforge@{socket.getfqdn()}"
    mail["To"] = ", ".join(recipients)
    mail.set_content(message)
    port = int(config.get("SMTP_PORT") or 587)
    context = ssl.create_default_context()
    if port == 465:
        server = smtplib.SMTP_SSL(config["SMTP_HOST"], port, timeout=TIMEOUT_SECONDS, context=context)
    else:
        server = smtplib.SMTP(config["SMTP_HOST"], port, timeout=TIMEOUT_SECONDS)
    with server:
        if port != 465:
            server.starttls(context=context)       # as the application's mailer does
        if user and password:
            server.login(user, password)
        server.send_message(mail, to_addrs=recipients)


def configured_channels(config: dict[str, str]) -> list[tuple[str, object]]:
    channels: list[tuple[str, object]] = []
    if config.get("ALERT_WEBHOOK_URL"):
        channels.append(("webhook", send_webhook))
    if config.get("ALERT_TELEGRAM_BOT_TOKEN") and config.get("ALERT_TELEGRAM_CHAT_ID"):
        channels.append(("telegram", send_telegram))
    if config.get("ALERT_EMAIL_TO") and config.get("SMTP_HOST"):
        channels.append(("email", send_email))
    return channels


def deliver(config: dict[str, str], subject: str, message: str) -> tuple[list[str], list[str]]:
    """Send to every configured channel. One failing never stops the others."""
    delivered, failed = [], []
    secrets = secret_values(config)
    for name, send in configured_channels(config):
        try:
            send(config, subject, message)
        except Exception as exc:  # noqa: BLE001 -- a broken channel must not stop the rest
            status = f" status={exc.code}" if isinstance(exc, urllib.error.HTTPError) else ""
            reason = redact(str(getattr(exc, "reason", "") or exc), secrets)[:200]
            log(f"channel={name} FAILED {type(exc).__name__}{status}: {reason}")
            failed.append(name)
        else:
            log(f"channel={name} sent")
            delivered.append(name)
    return delivered, failed


def _stamp_path(state_dir: str | None, unit: str) -> Path | None:
    return Path(state_dir) / (re.sub(r"[^A-Za-z0-9_.@-]", "_", unit) + ".last-alert") if state_dir else None


def in_cooldown(stamp: Path | None, cooldown: float, now: float) -> bool:
    if stamp is None or cooldown <= 0:
        return False
    try:
        return 0 <= now - stamp.stat().st_mtime < cooldown
    except OSError:
        return False


def main(argv: list[str] | None = None, environ=None) -> int:
    environ = os.environ if environ is None else environ
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("unit", nargs="?", help="the failed systemd unit, e.g. cosmicforge-trading.service")
    parser.add_argument("--test", action="store_true", help="send a test message to every configured channel")
    parser.add_argument("--env-file", default=DEFAULT_ENV_FILE, help=f"alert settings (default {DEFAULT_ENV_FILE})")
    parser.add_argument("--lines", type=int, default=JOURNAL_LINES, help="journal lines to include (default 20)")
    parser.add_argument("--cooldown", type=float, default=0.0,
                        help="seconds during which a repeated alert for the same unit is not sent again "
                             "(a crash loop or a failing 2-minute check would otherwise page continuously)")
    parser.add_argument("--state-dir", default=environ.get("STATE_DIRECTORY", "").split(":")[0] or None,
                        help="where the cooldown is remembered (default: systemd's $STATE_DIRECTORY)")
    args = parser.parse_args(argv)

    unit = args.unit or ("delivery-test" if args.test else None)
    if not unit or not UNIT_NAME.match(unit):
        log("usage: notify_failure.py UNIT | --test (the unit name is missing or not a valid unit name)")
        return 2

    config = load_config(args.env_file, environ)
    channels = [name for name, _ in configured_channels(config)]
    host = socket.gethostname()
    now = datetime.now(timezone.utc)

    if args.test:
        message = build_message(unit, host, now, test=True, secrets=secret_values(config))
        subject = f"[CosmicForge TEST] alert delivery check from {host}"
    else:
        stamp = _stamp_path(args.state_dir, unit)
        if in_cooldown(stamp, args.cooldown, time.time()):
            log(f"unit={unit} suppressed: already alerted within the last {args.cooldown:.0f}s")
            return 0
        message = build_message(unit, host, now, journal_tail(unit, max(1, args.lines)), unit_state(unit),
                                secrets=secret_values(config))
        subject = f"[CosmicForge ALERT] {unit} failed on {host}"

    if not channels:
        log(f"unit={unit} NO ALERT CHANNEL IS CONFIGURED in {args.env_file}: nobody was told. "
            "See deploy/alerts.env.example.")
        return 1 if args.test else 0
    delivered, failed = deliver(config, subject, message)
    log(f"unit={unit} delivered={','.join(delivered) or 'none'} failed={','.join(failed) or 'none'}")
    if args.test:
        return 0 if delivered and not failed else 1
    if delivered and stamp is not None and args.cooldown > 0:
        try:
            stamp.parent.mkdir(parents=True, exist_ok=True)
            stamp.touch()
        except OSError as exc:
            log(f"cooldown not recorded: {type(exc).__name__}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
