"""Static deployment-readiness checks (31 October 2026): the staging compose topology, the systemd units and the
nginx sites, parsed from the files in ``deploy/``. Nothing here starts a container or a unit -- Docker runs only in
GitHub CI and no authorized staging host exists (EXT-STG-01); these checks keep the FILES honest in between.

Compose (``deploy/staging/docker-compose.staging.yml``): the platform PostgreSQL runtime and its migration step,
exactly one canonical trading process, API-only replicas that never trade, the operator emergency path reaching the
process that can act, SIGTERM drains, restart policy, persistent volumes, secrets only from the env file / secret
files, and the operations tick with its dead-man healthcheck. ``test_staging_deploy_scripts.py`` covers the deploy,
rollback and restore scripts themselves.

systemd (``deploy/systemd``): every timer has its service; every oneshot unit pages on failure; a unit hardened with
``ProtectSystem=strict`` lists every path it writes in ``ReadWritePaths`` and holds no capability.

nginx (``deploy/nginx``): the security headers at server level with ``always`` (and no ``add_header`` in a location,
which would silently drop all of them there), modern TLS only, no version banner, internal routes denied.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[4]
DEPLOY = REPO / "deploy"
yaml = pytest.importorskip("yaml")


# ── compose ────────────────────────────────────────────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def compose():
    return yaml.safe_load((DEPLOY / "staging" / "docker-compose.staging.yml").read_text(encoding="utf-8"))


def _env(service):
    env = service.get("environment") or {}
    return env if isinstance(env, dict) else dict(item.split("=", 1) for item in env)


def test_the_platform_runtime_is_postgresql_migrated_once_before_anything_starts(compose):
    services = compose["services"]
    pg = services["postgres"]
    assert pg["image"].startswith("postgres:") and "pg-data:/var/lib/postgresql/data" in pg["volumes"]
    assert "pg_isready" in " ".join(pg["healthcheck"]["test"])
    migrate = services["migrate"]
    assert migrate["command"] == ["python", "-m", "shared_lib.persistence.sql", "migrate"]
    assert migrate["restart"] == "no" and migrate["depends_on"]["postgres"]["condition"] == "service_healthy"
    example = (DEPLOY / "staging" / "staging.env.example").read_text(encoding="utf-8")
    assert re.search(r"^PLATFORM_DATABASE_URL=postgresql://cosmicforge:<secret>@postgres:5432/cosmicforge$", example, re.M)
    for name, svc in services.items():
        if name not in ("postgres", "migrate", "user-frontend", "admin-frontend", "healthcheck"):
            assert svc["depends_on"]["migrate"]["condition"] == "service_completed_successfully", name


def test_exactly_one_canonical_trading_process_and_api_only_replicas(compose):
    services = compose["services"]
    trading = {n: s for n, s in services.items() if _env(s).get("PROCESS_ROLE") == "trading" and "profiles" not in s}
    assert list(trading) == ["bot-trading"] and trading["bot-trading"]["deploy"]["replicas"] == 1
    api = services["bot-api"]
    assert _env(api)["PROCESS_ROLE"] == "api" and api["deploy"]["replicas"] >= 2
    assert "BROKER_CYCLE_ASSIGNMENT_MODE" not in _env(api)
    assert _env(services["ops-tick"])["PROCESS_ROLE"] == "api"              # the tick never trades
    # The default assignment mode is owner (one trading process); the optional assigned worker stays profile-gated.
    example = (DEPLOY / "staging" / "staging.env.example").read_text(encoding="utf-8")
    assert re.search(r"^BROKER_CYCLE_ASSIGNMENT_MODE=owner$", example, re.M)
    assert services["bot-trading-assigned"]["profiles"] == ["multi-worker"]


def test_the_operator_emergency_path_reaches_the_process_that_can_act(compose):
    # user-backend proxies kill switch / flatten to the bot backend: it must be the trading process, because an
    # api-role process refuses those routes (app/ops/process_role.py, TRADING_PROCESS_ROUTES).
    assert _env(compose["services"]["user-backend"])["BOT_BACKEND_URL"] == "http://bot-trading:8000"


def test_safe_shutdown_restart_and_persistent_volumes(compose):
    services = compose["services"]
    for name in ("bot-trading", "bot-trading-assigned", "ops-tick"):
        svc = services[name]
        assert svc["stop_signal"] == "SIGTERM", name
        assert int(str(svc["stop_grace_period"]).rstrip("s")) >= 75, name
    for name, svc in services.items():
        expected = "no" if name == "migrate" else "unless-stopped"
        assert svc.get("restart") == expected, name
    assert set(compose["volumes"]) >= {"pg-data", "runtime-state", "golive-anchor"}
    for name in ("bot-trading", "bot-api", "ops-tick", "user-backend", "admin-backend"):
        vols = services[name].get("volumes") or []
        assert any(str(v).startswith("runtime-state:/var/lib/cosmicforge") for v in vols), name
        assert services[name]["read_only"] is True and services[name]["cap_drop"] == ["ALL"], name


SECRETISH = re.compile(r"(SECRET|PASSWORD|PASSWD|TOKEN|API_KEY|PRIVATE|CREDENTIAL)", re.I)


def test_secrets_are_injected_never_written_in_the_deployment_files(compose):
    for name, svc in compose["services"].items():
        for key, value in _env(svc).items():
            if SECRETISH.search(key):
                assert key.endswith("_FILE") and str(value).startswith("/run/secrets/"), (name, key)
            assert not re.search(r"://[^/@\s:]+:[^@\s<]+@", str(value)), (name, key)   # no password in a URL
        assert svc.get("env_file", ["${STAGING_ENV_FILE:-./staging.env}"]) == ["${STAGING_ENV_FILE:-./staging.env}"], name
    assert compose["secrets"]["postgres_password"]["file"].startswith("${POSTGRES_PASSWORD_FILE")
    for line in (DEPLOY / "staging" / "staging.env.example").read_text(encoding="utf-8").splitlines():
        if line.strip() and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            if SECRETISH.search(key):
                assert value.startswith("<secret"), key
    ignored = (DEPLOY / "staging" / ".gitignore").read_text(encoding="utf-8").split()
    assert {"staging.env", "secrets/", "backups/", ".deploy-state/"} <= set(ignored)


def test_trading_authority_stays_closed_in_staging():
    example = (DEPLOY / "staging" / "staging.env.example").read_text(encoding="utf-8")
    for flag in ("LIVE_ORDER_SUBMISSION_ENABLED", "DEMO_ORDER_SUBMISSION_ENABLED", "BILLING_ENFORCED"):
        assert re.search(rf"^{flag}=false$", example, re.M), flag


def test_the_operations_tick_runs_with_a_dead_man_healthcheck(compose):
    tick = compose["services"]["ops-tick"]
    assert tick["command"][:5] == ["python", "-u", "-m", "app.ops.alerting", "--loop"]
    probes = [tick["command"][i + 1] for i, a in enumerate(tick["command"]) if a == "--health-url"]
    assert {p.split("=", 1)[0] for p in probes} == {"user-backend", "admin-backend", "bot-api", "bot-trading"}
    check = tick["healthcheck"]["test"]
    assert check[:5] == ["CMD", "python", "-m", "app.ops.alerting", "--check-tick"]
    assert int(check[check.index("--max-tick-age") + 1]) <= 600


# ── systemd ────────────────────────────────────────────────────────────────────────────────────────────────
SYSTEMD = DEPLOY / "systemd"


def _unit(path):
    """Directives of a unit file (continuation lines joined); repeated keys keep every value."""
    out: dict = {}
    text = re.sub(r"\\\n\s*", " ", path.read_text(encoding="utf-8"))
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith(("#", ";", "[")) or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out.setdefault(key.strip(), []).append(value.strip())
    return out


def test_every_timer_has_its_service_and_every_oneshot_unit_pages_on_failure():
    for timer in SYSTEMD.glob("*.timer"):
        assert (SYSTEMD / (timer.stem + ".service")).is_file(), timer.name
    for service in SYSTEMD.glob("*.service"):
        u = _unit(service)
        if service.name == "cosmicforge-alert@.service":
            assert "OnFailure" not in u                                     # an alert about an alert would loop
            continue
        assert u.get("OnFailure") == ["cosmicforge-alert@%n.service"], service.name
        assert u.get("User") == ["cosmicforge"] and u.get("NoNewPrivileges") == ["true"], service.name


def test_strictly_sandboxed_units_can_write_exactly_what_they_need():
    strict = {s.name: _unit(s) for s in SYSTEMD.glob("*.service") if _unit(s).get("ProtectSystem") == ["strict"]}
    assert {"cosmicforge-healthcheck.service", "cosmicforge-ops-deadman.service",
            "cosmicforge-platform-db-backup.service"} <= set(strict)
    for name, u in strict.items():
        assert u.get("CapabilityBoundingSet") == [""], name                 # no capability at all
        exec_start = " ".join(u["ExecStart"])
        writable = " ".join(u.get("ReadWritePaths", []))
        for flag in ("--output-dir", "--backup-dir", "--state-dir"):
            m = re.search(rf"{flag}\s+(\S+)", exec_start)
            if m:
                assert m.group(1) in writable.split(), (name, flag)
    backup = strict["cosmicforge-platform-db-backup.service"]
    assert backup["ReadWritePaths"] == ["/var/backups/cosmicforge"]
    assert "scripts/backup_platform_db.py" in " ".join(backup["ExecStart"])
    assert "--check-tick" in " ".join(strict["cosmicforge-ops-deadman.service"]["ExecStart"])


def test_units_reference_scripts_and_modules_that_exist_and_carry_no_secret():
    for service in SYSTEMD.glob("*.service"):
        u = _unit(service)
        for exec_start in u.get("ExecStart", []):
            for script in re.findall(r"(scripts/[\w./-]+\.py)", exec_start):
                assert (REPO / script).is_file(), (service.name, script)
            for module in re.findall(r"-m\s+(app\.[\w.]+)", exec_start):
                assert (REPO / "backends" / "bot-backend" / (module.replace(".", "/") + ".py")).is_file(), module
        for env in u.get("Environment", []):
            assert not SECRETISH.search(env.split("=", 1)[0]), (service.name, env)


def test_the_dead_man_timer_is_independent_of_the_tick_it_watches():
    timer = _unit(SYSTEMD / "cosmicforge-ops-deadman.timer")
    assert timer.get("OnUnitActiveSec") == ["5min"]
    deadman = _unit(SYSTEMD / "cosmicforge-ops-deadman.service")
    assert "cosmicforge-ops-tick" not in " ".join(sum(deadman.values(), []))   # neither Requires= nor After= it


# ── nginx ──────────────────────────────────────────────────────────────────────────────────────────────────
def _blocks(text):
    """A minimal nginx parser: ``[(name, args, children_or_None)]`` (comments removed, quotes kept)."""
    text = re.sub(r"#[^\n]*", "", text)
    tokens = re.findall(r'"[^"]*"|\'[^\']*\'|[{};]|[^\s{};]+', text)
    pos = 0

    def parse():
        nonlocal pos
        out, current = [], []
        while pos < len(tokens):
            tok = tokens[pos]
            pos += 1
            if tok == ";":
                out.append((current[0], current[1:], None))
                current = []
            elif tok == "{":
                children = parse()
                out.append((current[0], current[1:], children))
                current = []
            elif tok == "}":
                return out
            else:
                current.append(tok)
        return out
    return parse()


def _servers(path):
    servers = []

    def walk(nodes):
        for name, args, children in nodes:
            if name == "server" and children is not None:
                servers.append(children)
            elif children is not None:
                walk(children)
    walk(_blocks(path.read_text(encoding="utf-8")))
    return servers


def _directives(children, name):
    return [args for n, args, c in children if n == name and c is None]


def _locations(children):
    for n, args, c in children:
        if n == "location" and c is not None:
            yield args, c
            yield from _locations(c)


NGINX = sorted((DEPLOY / "nginx").glob("*.conf"))


@pytest.mark.parametrize("conf", NGINX, ids=lambda p: p.name)
def test_every_tls_server_sends_the_security_headers_everywhere(conf):
    tls = [s for s in _servers(conf) if any("443" in " ".join(a) for a in _directives(s, "listen"))]
    assert tls, conf.name
    for server in tls:
        headers = {a[0].lower(): a[1:] for a in _directives(server, "add_header")}
        for name, value in (("strict-transport-security", "max-age=31536000"), ("x-content-type-options", "nosniff"),
                            ("x-frame-options", "DENY"), ("referrer-policy", "no-referrer")):
            assert name in headers and value in headers[name][0] and headers[name][-1] == "always", (conf.name, name)
        assert _directives(server, "server_tokens") == [["off"]]
        protocols = set(sum(_directives(server, "ssl_protocols"), []))
        assert protocols and protocols <= {"TLSv1.2", "TLSv1.3"}, protocols
        for args, children in _locations(server):
            # an add_header inside a location replaces EVERY server-level header for its responses
            assert not _directives(children, "add_header"), (conf.name, args)


def test_html_hosts_send_a_content_security_policy_that_forbids_framing():
    for server in _servers(DEPLOY / "nginx" / "cosmicforge-app.conf"):
        if not any("443" in " ".join(a) for a in _directives(server, "listen")):
            continue
        csp = [a for a in _directives(server, "add_header") if a[0].lower() == "content-security-policy"]
        assert csp and "frame-ancestors 'none'" in csp[0][1] and "default-src 'self'" in csp[0][1], server[:1]


def test_the_api_host_denies_internal_routes_and_keeps_health_local():
    [_, server] = _servers(DEPLOY / "nginx" / "cosmicforge.conf")
    denied = {" ".join(args) for args, children in _locations(server) if ["all"] in _directives(children, "deny")}
    assert {"/debug/", "/runner/", "/binance/", "/docs", "/redoc", "= /openapi.json"} <= denied
    health = [c for args, c in _locations(server) if args == ["/health"]][0]
    assert _directives(health, "allow") == [["127.0.0.1"], ["::1"]] and _directives(health, "deny") == [["all"]]
