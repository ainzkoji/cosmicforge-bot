"""A fake ``docker`` client for exercising the real deploy/staging scripts without Docker.

The scripts call ``docker`` (a bash shim on PATH execs this file). Every invocation is appended to
``$FAKE_DOCKER_LOG`` (one JSON object per line: argv, IMAGE_TAG, profiles). The simulated stack lives in
``$FAKE_DOCKER_STATE`` (JSON) and is driven per release tag::

    {"images": ["cosmicforge/bot-backend:r1", ...],
     "releases": {"r1": {"schema": 44,            # build-range of the image
                         "migrate": "ok"|"fail",  # the one-shot migration job
                         "irreversible": [45],    # versions its migration marks irreversible
                         "start_fail": ["bot-trading"],   # `compose up` of these services fails
                         "ready": "unsupervised"|"ready"|"never"|"crash"|"degraded"|"live"|"role-api"|"after:N",
                         "emergency": 401,        # unauthenticated GET of the emergency status route
                         "drain_exit": 0,         # exit code when stopped (137: killed after the grace)
                         "backup": "ok"|"fail",   # scripts/backup_trading_db.py in a one-off container
                         "health_check_exit": 2}},
     "db": {"version": 44, "irreversible": [], "pg_ready": true, "pg_dump": "ok"},
     "containers": {"bot-trading": {"tag": "r1", "running": true, "id": "...", "polls": 0, "exit_code": null}}}

Arguments may arrive path-converted on Windows (Git Bash rewrites POSIX-looking arguments for native programs), so
matching is by substring. The fake never touches a real container, database or network.
"""
from __future__ import annotations

import json
import os
import sys
import uuid

TRADING = ("bot-trading", "bot-trading-assigned")
DEFAULT_SERVICES = ["postgres", "migrate", "bot-trading", "bot-api", "user-backend", "admin-backend",
                    "user-frontend", "admin-frontend", "healthcheck"]


def _load():
    with open(os.environ["FAKE_DOCKER_STATE"], encoding="utf-8") as fh:
        return json.load(fh)


def _save(state):
    path = os.environ["FAKE_DOCKER_STATE"]
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=1, sort_keys=True)
    os.replace(tmp, path)


def _log(argv):
    with open(os.environ["FAKE_DOCKER_LOG"], "a", encoding="utf-8") as fh:
        fh.write(json.dumps({"argv": argv, "image_tag": os.environ.get("IMAGE_TAG"),
                             "profiles": os.environ.get("COMPOSE_PROFILES", "")}) + "\n")


def _release(state, tag):
    return dict(state.get("releases", {}).get(tag or "", {}))


def _out(text):
    sys.stdout.write(text)
    sys.stdout.flush()


def _err(text, code):
    sys.stderr.write(text + "\n")
    return code


def _health(state, svc, container):
    """(http_code, body) of /health?ready=1 for a running trading container."""
    rel = _release(state, container["tag"])
    mode = rel.get("ready", "unsupervised")
    container["polls"] = container.get("polls", 0) + 1
    if mode.startswith("after:"):
        mode = "ready" if container["polls"] > int(mode.split(":", 1)[1]) else "never"
    role = "api" if mode == "role-api" else "trading"
    supervised = {"status": "ok", "process_role": role, "runtime": {"state": "HEALTHY", "faults": []},
                  "components": {"runtime_owns_lease": True}, "discovered_accounts": 2, "synced_accounts": 2,
                  "reconciliation_health": "SYNCED", "live_system_gate": False, "demo_system_gate": False}
    if mode in ("unsupervised", "role-api"):
        return 200, {"status": "ok", "process_role": role, "components": {"runtime_owns_lease": True}}
    if mode == "ready":
        return 200, supervised
    if mode == "degraded":   # the venue is unreachable: supervisor healthy, broker truth not reconciled
        return 200, dict(supervised, status="degraded", synced_accounts=0, reconciliation_health="NOT_SYNCED")
    if mode == "live":
        return 200, dict(supervised, live_system_gate=True)
    return 503, {"status": "ok", "process_role": role, "runtime": {"state": "STARTING", "faults": []}}


def _curl(state, svc, container, args):
    if any(a in ("-X", "--request", "-d", "--data", "--data-raw", "--json", "-F") for a in args):
        state.setdefault("mutations", []).append({"service": svc, "args": args})
        _out("\n409")
        return 0
    url = next((a for a in args if a.startswith("http")), "")
    fmt_index = args.index("-w") + 1 if "-w" in args else None
    discard = "-o" in args
    if "/health" in url:
        code, body = _health(state, svc, container)
    elif "/emergency/status" in url:
        code, body = _release(state, container["tag"]).get("emergency", 401), {"detail": "Not authenticated"}
    else:
        code, body = 404, {"detail": "Not Found"}
    text = "" if discard else json.dumps(body)
    if fmt_index is not None:
        text += args[fmt_index].replace("\\n", "\n").replace("%{http_code}", str(code))
    _out(text)
    return 0 if "-f" not in args or code < 400 else 22


def _compat(state, a_min, a_max):
    db = state["db"]
    v = db["version"]
    if v < a_min:
        return False, f"DATABASE_TOO_OLD: {v} < {a_min}"
    if v <= a_max:
        return True, "WITHIN_RANGE"
    blocking = sorted(x for x in db.get("irreversible", []) if x > a_max)
    if blocking:
        return False, f"IRREVERSIBLE_NEWER_SCHEMA:{blocking}"
    return True, "ADDITIVE_NEWER_SCHEMA"


def _compose(state, args):
    i = 0
    while i < len(args) and args[i] in ("-f", "--file", "-p", "--project-name"):
        i += 2
    sub, rest = args[i], args[i + 1:]
    tag = os.environ.get("IMAGE_TAG")
    containers = state.setdefault("containers", {})
    if sub == "config":
        if state.get("compose_config") == "fail":
            return _err("invalid compose file", 15)
        if "--services" in rest:
            services = list(state.get("services", DEFAULT_SERVICES))
            if "multi-worker" in os.environ.get("COMPOSE_PROFILES", "").split(","):
                services.append("bot-trading-assigned")
            _out("\n".join(services) + "\n")
        return 0
    if sub == "up":
        services = [a for a in rest if not a.startswith("-")]
        for svc in services:
            if svc == "postgres":
                state["db"]["up"] = True
                continue
            rel = _release(state, tag)
            if svc in rel.get("start_fail", []):
                return _err(f"Error response from daemon: container {svc} ({tag}) failed to start", 1)
            crash = svc in TRADING and rel.get("ready") == "crash"
            containers[svc] = {"tag": tag, "running": not crash, "id": f"c-{svc}-{uuid.uuid4().hex[:8]}",
                               "polls": 0, "exit_code": 1 if crash else None}
        return 0
    if sub == "stop":
        services = [a for a in rest if not a.startswith("-") and not a.isdigit()]
        for svc in services:
            c = containers.get(svc)
            if c and c["running"]:
                c["running"] = False
                c["exit_code"] = int(_release(state, c["tag"]).get("drain_exit", 0))
        return 0
    if sub == "ps":
        services = [a for a in rest if not a.startswith("-")]
        _out("".join(containers[s]["id"] + "\n" for s in services if s in containers and containers[s]["running"]))
        return 0
    if sub == "logs":
        services = [a for a in rest if not a.startswith("-")]
        for s in services:
            c = containers.get(s, {})
            _out(f"{s} | fake log of {c.get('tag')}\n")
            if s in TRADING and c and not c.get("running") and c.get("exit_code") == 0:   # it drained on SIGTERM
                _out(f"{s} | [RUNTIME_SHUTDOWN] clean=True reason=APPLICATION_SHUTDOWN lease_released=True\n")
        return 0
    if sub == "exec":
        svc = next(a for a in rest if not a.startswith("-"))
        cmd = rest[rest.index(svc) + 1:]
        if svc == "postgres":
            db = state["db"]
            if not db.get("up"):
                return _err('service "postgres" is not running', 1)
            if cmd and cmd[0] == "pg_isready":
                return 0 if db.get("pg_ready", True) else 2
            if cmd and cmd[0] == "pg_dump":
                if db.get("pg_dump", "ok") == "fail":
                    return _err("pg_dump: error: connection failed", 1)
                if db.get("pg_dump") != "empty":
                    _out(f"PGDMP-fake version={db['version']}\n")
                return 0
            if cmd and cmd[0] == "pg_restore":
                data = sys.stdin.read()
                state["restored_from"] = data.strip()
                if "version=" in data:
                    db["version"] = int(data.split("version=")[1].split()[0])
                return 0
            state.setdefault("postgres_commands", []).append(cmd)
            return 0
        c = containers.get(svc)
        if not c or not c["running"]:
            return _err(f'service "{svc}" is not running', 1)
        if cmd and cmd[0] == "curl":
            return _curl(state, svc, c, cmd[1:])
        if any("vps_health_check" in a for a in cmd):
            _out(json.dumps({"verdict": "UNHEALTHY", "checks": []}) + "\n")
            return int(_release(state, c["tag"]).get("health_check_exit", 2))
        return 0
    if sub == "run":
        svc = next(a for a in rest if not a.startswith("-"))
        cmd = rest[rest.index(svc) + 1:]
        rel = _release(state, tag)
        if svc == "migrate":
            if not state["db"].get("up"):
                state["db"]["up"] = True        # compose starts the dependency
            if not cmd or cmd[-1] == "migrate":
                if rel.get("migrate", "ok") == "fail":
                    _out(json.dumps({"ok": False, "error": "MIGRATION_FAILED", "detail": "fake"}) + "\n")
                    return 5
                before = state["db"]["version"]
                state["db"]["version"] = max(before, int(rel.get("schema", before)))
                state["db"].setdefault("irreversible", []).extend(rel.get("irreversible", []))
                _out(json.dumps({"ok": True, "applied": list(range(before + 1, state["db"]["version"] + 1)),
                                 "version": state["db"]["version"]}) + "\n")
                return 0
            if "check-compat" in cmd:
                a_min = int(cmd[cmd.index("--app-min") + 1])
                a_max = int(cmd[cmd.index("--app-max") + 1])
                ok, reason = _compat(state, a_min, a_max)
                state.setdefault("compat_checks", []).append({"run_by": tag, "min": a_min, "max": a_max, "ok": ok})
                _out(json.dumps({"ok": ok, "database_version": state["db"]["version"], "reason": reason}) + "\n")
                return 0 if ok else 3
            if "status" in cmd:
                _out(json.dumps({"ok": True, "version": state["db"]["version"]}) + "\n")
                return 0
            return 0
        if svc in TRADING and any("backup_trading_db" in a for a in cmd):
            return 2 if rel.get("backup", "ok") == "fail" else 0
        return 0
    return _err(f"fake docker: unsupported compose command {sub}", 64)


def main(argv):
    sys.stdout.reconfigure(newline="\n")      # a Linux docker prints LF; so must the fake, on Windows too
    _log(argv)
    state = _load()
    try:
        if argv[:2] == ["image", "inspect"]:
            missing = [a for a in argv[2:] if a not in state.get("images", [])]
            return _err(f"Error: No such image: {missing[0]}", 1) if missing else 0
        if argv and argv[0] == "inspect":
            cid = argv[-1]
            c = next((c for c in state.get("containers", {}).values() if c["id"] == cid), None)
            if c is None:
                return _err(f"Error: No such object: {cid}", 1)
            _out(f"{c.get('exit_code') if c.get('exit_code') is not None else 0} false\n")
            return 0
        if argv and argv[0] == "run":
            image = next(a for a in argv[1:] if a.startswith("cosmicforge/"))
            if image not in state.get("images", []):
                return _err(f"Unable to find image '{image}' locally", 125)
            if any("build-range" in a for a in argv):
                schema = int(_release(state, image.split(":", 1)[1]).get("schema", 44))
                _out(json.dumps({"app_schema_min": schema, "app_schema_max": schema}) + "\n")
            return 0
        if argv and argv[0] == "compose":
            return _compose(state, argv[1:])
        return _err(f"fake docker: unsupported command {argv[:2]}", 64)
    finally:
        _save(state)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
