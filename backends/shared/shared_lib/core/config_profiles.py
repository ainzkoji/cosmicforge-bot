"""Environment configuration profiles and their validator (master prompt section 22).

Six profiles, each a secret-free template under ``deploy/profiles/<name>.env.example``:

  development          a developer machine; SQLite or PostgreSQL; demo/testnet endpoints; no orders
  test                 the automated test suite; demo/testnet endpoints; no orders
  shadow               the production topology on PostgreSQL; live market data; no orders at all (SHADOW_ONLY)
  demo_certification   the production topology; orders ONLY to DEMO broker accounts (venue demo certification)
  live_beta            controlled live beta (Stage A/B); refused in this build: not live-capable
  scaled_live          future scaled live (multi-worker); refused in this build: not live-capable

``validate_profile(text)`` returns a ``ProfileReport``: ``structure_ok`` (every rule below holds) and ``admissible``
(structure_ok AND the profile may run on THIS build). The live profiles validate structurally but are never
admissible here: ``PROFILE_REQUIRES_LIVE_CAPABLE_BUILD:<live_build reason>``. Rules (each its own code):

* ``CONFIG_PROFILE`` names a known profile (and the expected one, if given); KEY=VALUE lines only, no duplicates.
* ``APP_ENV``: DEVELOPMENT / TEST / PRODUCTION as the profile requires.
* ``PROCESS_ROLE``: all/api/trading in development and test; trading or api elsewhere (never ``all``).
* ``DATABASE_URL`` and, outside development/test, ``PLATFORM_DATABASE_URL``: PostgreSQL outside development/test,
  never with a password in the URL (the password comes from ``PGPASSFILE``).
* Exchange environment: development/test use the DEMO endpoints of every venue; the production-topology profiles use
  the canonical LIVE endpoints with ``BROKER_ENVIRONMENT=ACCOUNT_SCOPED`` (each account's own environment decides).
* Rate budget: ``CATI_EGRESS_SCOPE`` and ``UNIVERSE_REQUEST_WEIGHT_BUDGET_FRACTION`` in (0, 0.8].
* Worker lease: ``BROKER_ASSIGNMENT_TTL_MS`` > 0, ``BROKER_ASSIGNMENT_FENCE_MARGIN_MS`` below it, a known
  ``BROKER_CYCLE_ASSIGNMENT_MODE`` (``assigned`` only in scaled_live, development and test), pool size 1-64.
* Risk: ``MAX_RISK_PER_TRADE_PCT`` <= 0.40 (percent of equity) and ``STOP_LOSS_PCT`` <= 0.15 -- the platform ceilings
  (``app/risk/system_limits.py``); a profile can only be tighter.
* Order flags: ``LIVE_ORDER_SUBMISSION_ENABLED=false`` in EVERY template; in the live profiles the line above it says
  ``OPERATOR ACT`` (turning it on is the operator's act on the host, never a template value).
  ``DEMO_ORDER_SUBMISSION_ENABLED`` is true in demo_certification (it needs it) and false everywhere else.
  ``BILLING_ENFORCED=false`` (the business owner's step, EXT-BILL-01).
* Alerting and backups (production topology): ``PROFILE_ALERTS_ENV_FILE``, ``PROFILE_OFFSITE_BACKUP_ENV_FILE``
  (absolute paths; the systemd units' EnvironmentFile), ``PROFILE_BACKUP_TIMERS`` including the platform pg_dump
  timer, and ``GOLIVE_REVOCATION_ANCHOR`` (absolute path on separate storage).
* Secrets: no secret-shaped key appears in a profile at all. Secrets are delivered by file: ``PROFILE_SECRETS_ENV_FILE``
  (absolute path, outside the repository; required in the production topology) and ``*_FILE`` references such as
  ``PGPASSFILE`` must be absolute paths.

``PROFILE_*`` keys are read by the process manager / deployer (systemd ``EnvironmentFile=``, compose ``env_file``),
never by the application, which ignores unknown keys.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Tuple

PROFILES = ("development", "test", "shadow", "demo_certification", "live_beta", "scaled_live")
LIVE_PROFILES = ("live_beta", "scaled_live")
LOCAL_PROFILES = ("development", "test")
APP_ENV = {"development": "DEVELOPMENT", "test": "TEST", "shadow": "PRODUCTION", "demo_certification": "PRODUCTION",
           "live_beta": "PRODUCTION", "scaled_live": "PRODUCTION"}
#: Platform ceilings (``app/risk/system_limits.py``: max_risk_per_trade_ceiling 0.004, max_stop_loss_pct 0.15).
MAX_RISK_PER_TRADE_PCT_CEILING = Decimal("0.40")
MAX_STOP_LOSS_PCT_CEILING = Decimal("0.15")
MAX_BUDGET_FRACTION = Decimal("0.8")       # shared_lib.broker.rate_budget.DEFAULT_UTILISATION
VENUES = (("binance", "BINANCE_ENV", "BINANCE_FAPI_BASE_URL"), ("bybit", "BYBIT_ENV", "BYBIT_BASE_URL"),
          ("bingx", "BINGX_ENV", "BINGX_BASE_URL"))
PLATFORM_DUMP_TIMER = "cosmicforge-platform-db-backup.timer"
_SECRET = re.compile(r"(SECRET|PASSWORD|PASSWD|PASSPHRASE|TOKEN|PRIVATE|CREDENTIAL|_KEY$|^KEY$|API_KEY|WEBHOOK_URL)")
_KEY = re.compile(r"[A-Z][A-Z0-9_]*")
_URL_PASSWORD = re.compile(r"^[a-z0-9+]+://[^/@:]*:[^/@]*@", re.I)

PARSE_ERROR = "PROFILE_PARSE_ERROR"
NAME_MISSING = "PROFILE_NAME_MISSING"
UNKNOWN = "PROFILE_UNKNOWN"
NAME_MISMATCH = "PROFILE_NAME_MISMATCH"
APP_ENV_MISMATCH = "APP_ENV_MISMATCH"
ROLE_INVALID = "PROCESS_ROLE_NOT_ALLOWED"
DB_MISSING = "DATABASE_URL_MISSING"
PLATFORM_DB_MISSING = "PLATFORM_DATABASE_URL_MISSING"
DB_KIND = "DATABASE_KIND_NOT_ALLOWED"
DB_PASSWORD_INLINE = "DATABASE_PASSWORD_INLINE"
EXCHANGE_ENV = "EXCHANGE_ENVIRONMENT_MISMATCH"
RATE_BUDGET = "RATE_BUDGET_INVALID"
LEASE = "WORKER_LEASE_INVALID"
ASSIGNMENT_MODE = "ASSIGNMENT_MODE_NOT_ALLOWED"
RISK_MISSING = "RISK_LIMIT_MISSING"
RISK_ABOVE = "RISK_LIMIT_ABOVE_CEILING"
FLAG_MISSING = "ORDER_FLAG_MISSING"
FLAG_NOT_ALLOWED = "ORDER_FLAG_NOT_ALLOWED"
OPERATOR_ACT = "LIVE_FLAG_OPERATOR_ACT_UNDOCUMENTED"
BILLING = "BILLING_ENFORCED_NOT_ALLOWED"
ALERTING = "ALERTING_SETTINGS_MISSING"
BACKUP = "BACKUP_SETTINGS_MISSING"
ANCHOR = "GOLIVE_REVOCATION_ANCHOR_MISSING"
SECRET_INLINE = "SECRET_INLINE"
SECRETS_FILE = "SECRETS_FILE_REFERENCE_INVALID"
LIVE_BUILD_REQUIRED = "PROFILE_REQUIRES_LIVE_CAPABLE_BUILD"


@dataclass
class ProfileReport:
    profile: Optional[str]
    errors: List[Tuple[str, str]] = field(default_factory=list)
    refusal: Optional[str] = None

    @property
    def structure_ok(self) -> bool:
        return not self.errors

    @property
    def admissible(self) -> bool:
        return self.structure_ok and self.refusal is None

    @property
    def codes(self) -> List[str]:
        return [code for code, _ in self.errors]

    def to_plain(self) -> Dict[str, Any]:
        return {"profile": self.profile, "structure_ok": self.structure_ok, "admissible": self.admissible,
                "errors": [{"code": c, "detail": d} for c, d in self.errors], "refusal": self.refusal}


def parse_env(text: str) -> Tuple[Dict[str, str], Dict[str, str], List[str]]:
    """(values, comment block directly above each key, parse errors). Plain KEY=VALUE lines (systemd and compose
    read the same format): no ``export``, no quotes needed, a comment only on its own line."""
    values: Dict[str, str] = {}
    above: Dict[str, str] = {}
    errors: List[str] = []
    comments: List[str] = []
    for n, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line:
            comments = []
            continue
        if line.startswith("#"):
            comments.append(line.lstrip("#").strip())
            continue
        key, sep, value = line.partition("=")
        if not sep or not _KEY.fullmatch(key):
            errors.append(f"line {n}: not KEY=VALUE")
            continue
        if key in values:
            errors.append(f"line {n}: duplicate {key}")
        values[key] = value
        above[key] = " ".join(comments)
        comments = []
    return values, above, errors


def _bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    v = value.strip().lower()
    return True if v in ("true", "1", "yes") else False if v in ("false", "0", "no") else None


def _decimal(value: Optional[str]) -> Optional[Decimal]:
    try:
        d = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None
    return d if d.is_finite() else None


def _int(value: Optional[str]) -> Optional[int]:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _absolute(value: Optional[str]) -> bool:
    return bool(value) and value.startswith("/") and ".." not in value.split("/")


def validate_profile(text: str, *, expected: Optional[str] = None,
                     live_status: Optional[Callable[[], Any]] = None) -> ProfileReport:
    values, above, parse_errors = parse_env(text)
    name = values.get("CONFIG_PROFILE")
    report = ProfileReport(name)
    err = report.errors.append
    for problem in parse_errors:
        err((PARSE_ERROR, problem))
    if not name:
        err((NAME_MISSING, "CONFIG_PROFILE"))
        return report
    if name not in PROFILES:
        err((UNKNOWN, name))
        return report
    if expected is not None and name != expected:
        err((NAME_MISMATCH, f"{name} != {expected}"))
    local, live = name in LOCAL_PROFILES, name in LIVE_PROFILES
    get = values.get

    if get("APP_ENV") != APP_ENV[name]:
        err((APP_ENV_MISMATCH, f"APP_ENV must be {APP_ENV[name]}"))
    roles = ("all", "api", "trading") if local else ("trading", "api")
    if get("PROCESS_ROLE") not in roles:
        err((ROLE_INVALID, f"PROCESS_ROLE must be one of {'/'.join(roles)}"))

    # databases
    db_keys = ("DATABASE_URL",) if local else ("DATABASE_URL", "PLATFORM_DATABASE_URL")
    for key in ("DATABASE_URL", "PLATFORM_DATABASE_URL"):
        url = get(key)
        if not url:
            if key in db_keys:
                err((DB_MISSING if key == "DATABASE_URL" else PLATFORM_DB_MISSING, key))
            continue
        kind = url.split(":", 1)[0].lower()
        allowed = ("sqlite", "postgresql") if local else ("postgresql",)
        if kind not in allowed:
            err((DB_KIND, f"{key} must be {' or '.join(allowed)} in {name}"))
        if _URL_PASSWORD.match(url):
            err((DB_PASSWORD_INLINE, f"{key}: the password comes from PGPASSFILE, never the URL"))
    if not local and not _absolute(get("PGPASSFILE")):
        err((SECRETS_FILE, "PGPASSFILE must be an absolute path"))

    # exchange environment
    try:
        from shared_lib.broker.environment import BrokerEnvironment, normalize_environment, resolve_base_url
        want = BrokerEnvironment.DEMO if local else BrokerEnvironment.LIVE
        for venue, env_key, url_key in VENUES:
            try:
                env = normalize_environment(get(env_key, ""))
            except ValueError:
                env = None
            if env != want:
                err((EXCHANGE_ENV, f"{env_key} must name the {want.value.upper()} environment in {name}"))
            elif (get(url_key) or "").rstrip("/") != resolve_base_url(venue, want).rstrip("/"):
                err((EXCHANGE_ENV, f"{url_key} must be the canonical {want.value.upper()} endpoint"))
    except ImportError as exc:  # pragma: no cover
        err((EXCHANGE_ENV, f"environment registry unavailable: {exc}"))
    if not local and get("BROKER_ENVIRONMENT") != "ACCOUNT_SCOPED":
        err((EXCHANGE_ENV, "BROKER_ENVIRONMENT must be ACCOUNT_SCOPED"))

    # rate budget
    if not (get("CATI_EGRESS_SCOPE") or "").strip():
        err((RATE_BUDGET, "CATI_EGRESS_SCOPE (the shared request-budget scope) is required"))
    fraction = _decimal(get("UNIVERSE_REQUEST_WEIGHT_BUDGET_FRACTION"))
    if fraction is None or not Decimal(0) < fraction <= MAX_BUDGET_FRACTION:
        err((RATE_BUDGET, f"UNIVERSE_REQUEST_WEIGHT_BUDGET_FRACTION must be in (0, {MAX_BUDGET_FRACTION}]"))

    # worker lease
    ttl, margin = _int(get("BROKER_ASSIGNMENT_TTL_MS")), _int(get("BROKER_ASSIGNMENT_FENCE_MARGIN_MS"))
    if ttl is None or ttl <= 0:
        err((LEASE, "BROKER_ASSIGNMENT_TTL_MS must be a positive integer"))
    elif margin is None or not 0 <= margin < ttl:
        err((LEASE, "BROKER_ASSIGNMENT_FENCE_MARGIN_MS must be below the TTL"))
    pool = get("PLATFORM_DB_POOL_SIZE")
    if pool is not None and not (1 <= (_int(pool) or 0) <= 64):
        err((LEASE, "PLATFORM_DB_POOL_SIZE must be 1-64"))
    mode = get("BROKER_CYCLE_ASSIGNMENT_MODE")
    modes = ("owner", "assigned") if (local or name == "scaled_live") else ("owner",)
    if mode not in modes:
        err((ASSIGNMENT_MODE, f"BROKER_CYCLE_ASSIGNMENT_MODE must be {'/'.join(modes)} in {name}"))

    # risk ceilings
    for key, ceiling in (("MAX_RISK_PER_TRADE_PCT", MAX_RISK_PER_TRADE_PCT_CEILING),
                         ("STOP_LOSS_PCT", MAX_STOP_LOSS_PCT_CEILING)):
        value = _decimal(get(key))
        if value is None:
            err((RISK_MISSING, key))
        elif not Decimal(0) < value <= ceiling:
            err((RISK_ABOVE, f"{key} must be in (0, {ceiling}]"))

    # order flags
    for key in ("LIVE_ORDER_SUBMISSION_ENABLED", "DEMO_ORDER_SUBMISSION_ENABLED", "BILLING_ENFORCED"):
        if _bool(get(key)) is None:
            err((FLAG_MISSING, f"{key} must be present as true or false"))
    if _bool(get("LIVE_ORDER_SUBMISSION_ENABLED")) is True:
        err((FLAG_NOT_ALLOWED, "LIVE_ORDER_SUBMISSION_ENABLED is never true in a template (an operator act)"))
    if live and "OPERATOR ACT" not in above.get("LIVE_ORDER_SUBMISSION_ENABLED", "").upper():
        err((OPERATOR_ACT, "the comment above LIVE_ORDER_SUBMISSION_ENABLED must say OPERATOR ACT"))
    demo = _bool(get("DEMO_ORDER_SUBMISSION_ENABLED"))
    if demo is not None and demo != (name == "demo_certification"):
        err((FLAG_NOT_ALLOWED, f"DEMO_ORDER_SUBMISSION_ENABLED must be {str(name == 'demo_certification').lower()} "
                               f"in {name}"))
    if _bool(get("BILLING_ENFORCED")) is True:
        err((BILLING, "BILLING_ENFORCED is the business owner's step, never a template value"))

    # alerting, backups, revocation anchor
    if not local:
        if not _absolute(get("PROFILE_ALERTS_ENV_FILE")):
            err((ALERTING, "PROFILE_ALERTS_ENV_FILE must be an absolute path"))
        if not _absolute(get("PROFILE_OFFSITE_BACKUP_ENV_FILE")):
            err((BACKUP, "PROFILE_OFFSITE_BACKUP_ENV_FILE must be an absolute path"))
        timers = [t.strip() for t in (get("PROFILE_BACKUP_TIMERS") or "").split(",") if t.strip()]
        if PLATFORM_DUMP_TIMER not in timers:
            err((BACKUP, f"PROFILE_BACKUP_TIMERS must include {PLATFORM_DUMP_TIMER}"))
        if not _absolute(get("GOLIVE_REVOCATION_ANCHOR")):
            err((ANCHOR, "GOLIVE_REVOCATION_ANCHOR must be an absolute path on separate storage"))

    # secrets: by file only
    for key, value in values.items():
        if key.endswith("_FILE") or key == "PGPASSFILE":
            if key not in ("PROFILE_ALERTS_ENV_FILE", "PROFILE_OFFSITE_BACKUP_ENV_FILE") and value \
                    and not _absolute(value):
                err((SECRETS_FILE, f"{key} must be an absolute path"))
            continue
        if _SECRET.search(key):
            err((SECRET_INLINE, f"{key} must not appear in a profile: deliver it by file"))
    if not local and not _absolute(get("PROFILE_SECRETS_ENV_FILE")):
        err((SECRETS_FILE, "PROFILE_SECRETS_ENV_FILE must be an absolute path outside the repository"))

    # this build
    if live and report.structure_ok:
        if live_status is None:
            from shared_lib.core.live_build import live_build_status as live_status
        status = live_status()
        if not status.capable:
            report.refusal = f"{LIVE_BUILD_REQUIRED}:{status.reason}"
    return report
