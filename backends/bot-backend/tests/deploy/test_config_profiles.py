"""Master prompt section 22: separate, validated, secret-free configuration profiles (``deploy/profiles``).

The live-beta and scaled-live templates validate structurally but are refused on this build (not live-capable).
Nothing here reads an operator's real ``.env``.
"""
from __future__ import annotations

import importlib.util
import re
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from shared_lib.core import config_profiles as CP

REPO = Path(__file__).resolve().parents[4]
PROFILE_DIR = REPO / "deploy" / "profiles"
FILES = {name: PROFILE_DIR / f"{name.replace('_', '-')}.env.example" for name in CP.PROFILES}
CAPABLE = lambda: SimpleNamespace(capable=True, reason="LIVE_BUILD_CAPABLE")   # noqa: E731  (test-only stand-in)


def text(name):
    return FILES[name].read_text(encoding="utf-8")


def with_line(source, key, value=None):
    """Replace KEY=... (value None: delete the line); append it when absent."""
    pattern = re.compile(rf"^{re.escape(key)}=.*$", re.M)
    if value is None:
        return pattern.sub("", source)
    if pattern.search(source):
        return pattern.sub(f"{key}={value}", source)
    return source + f"\n{key}={value}\n"


def test_there_is_one_template_per_profile_and_nothing_else():
    assert sorted(p.name for p in PROFILE_DIR.iterdir()) == sorted(p.name for p in FILES.values())


@pytest.mark.parametrize("name", CP.PROFILES)
def test_every_template_validates_as_the_profile_it_names(name):
    report = CP.validate_profile(text(name), expected=name)
    assert report.structure_ok, report.errors
    assert report.profile == name


@pytest.mark.parametrize("name", ["development", "test", "shadow", "demo_certification"])
def test_the_non_live_profiles_are_admissible_on_this_build(name):
    assert CP.validate_profile(text(name)).admissible


@pytest.mark.parametrize("name", CP.LIVE_PROFILES)
def test_the_live_profiles_are_refused_on_this_build_with_a_clear_reason(name):
    report = CP.validate_profile(text(name))
    assert report.structure_ok and not report.admissible
    assert report.refusal == "PROFILE_REQUIRES_LIVE_CAPABLE_BUILD:LIVE_NOT_PERMITTED_IN_THIS_BUILD"
    # the refusal comes from the build alone: with a (test-only) capable build the same template is admissible
    assert CP.validate_profile(text(name), live_status=CAPABLE).admissible


@pytest.mark.parametrize("name", CP.PROFILES)
def test_no_template_holds_a_secret_and_the_live_flag_is_never_true(name):
    values, above, _ = CP.parse_env(text(name))
    assert values["LIVE_ORDER_SUBMISSION_ENABLED"] == "false" and values["BILLING_ENFORCED"] == "false"
    assert not [k for k in values if CP._SECRET.search(k) and not k.endswith("_FILE")]
    assert not re.search(r"(sk|rk)_(live|test)_|whsec_|-----BEGIN", text(name))
    if name in CP.LIVE_PROFILES:
        assert "OPERATOR ACT" in above["LIVE_ORDER_SUBMISSION_ENABLED"]


MUTATIONS = [
    ("shadow", "DATABASE_URL", "sqlite:////var/lib/cosmicforge/cosmicforge.db", CP.DB_KIND),
    ("demo_certification", "PLATFORM_DATABASE_URL", None, CP.PLATFORM_DB_MISSING),
    ("shadow", "PLATFORM_DATABASE_URL", "postgresql://cosmicforge:" + "pw" * 4 + "@postgres:5432/cf", CP.DB_PASSWORD_INLINE),
    ("live_beta", "PGPASSFILE", "secrets/pgpass", CP.SECRETS_FILE),
    ("development", "DATABASE_URL", None, CP.DB_MISSING),
    ("shadow", "APP_ENV", "DEVELOPMENT", CP.APP_ENV_MISMATCH),
    ("test", "APP_ENV", "PRODUCTION", CP.APP_ENV_MISMATCH),
    ("shadow", "PROCESS_ROLE", "all", CP.ROLE_INVALID),
    ("scaled_live", "PROCESS_ROLE", None, CP.ROLE_INVALID),
    ("shadow", "BINANCE_ENV", "demo", CP.EXCHANGE_ENV),
    ("live_beta", "BYBIT_BASE_URL", "https://api-demo.bybit.com", CP.EXCHANGE_ENV),
    ("demo_certification", "BROKER_ENVIRONMENT", "DEMO", CP.EXCHANGE_ENV),
    ("development", "BINGX_ENV", "mainnet", CP.EXCHANGE_ENV),
    ("test", "BINANCE_FAPI_BASE_URL", "https://fapi.binance.com", CP.EXCHANGE_ENV),
    ("shadow", "CATI_EGRESS_SCOPE", None, CP.RATE_BUDGET),
    ("live_beta", "UNIVERSE_REQUEST_WEIGHT_BUDGET_FRACTION", "0.95", CP.RATE_BUDGET),
    ("shadow", "BROKER_ASSIGNMENT_FENCE_MARGIN_MS", "60000", CP.LEASE),
    ("demo_certification", "BROKER_ASSIGNMENT_TTL_MS", "0", CP.LEASE),
    ("scaled_live", "PLATFORM_DB_POOL_SIZE", "65", CP.LEASE),
    ("live_beta", "BROKER_CYCLE_ASSIGNMENT_MODE", "assigned", CP.ASSIGNMENT_MODE),
    ("shadow", "BROKER_CYCLE_ASSIGNMENT_MODE", "anything", CP.ASSIGNMENT_MODE),
    ("shadow", "MAX_RISK_PER_TRADE_PCT", "0.41", CP.RISK_ABOVE),
    ("live_beta", "MAX_RISK_PER_TRADE_PCT", "0.75", CP.RISK_ABOVE),
    ("scaled_live", "STOP_LOSS_PCT", "0.16", CP.RISK_ABOVE),
    ("development", "STOP_LOSS_PCT", None, CP.RISK_MISSING),
    ("shadow", "LIVE_ORDER_SUBMISSION_ENABLED", "true", CP.FLAG_NOT_ALLOWED),
    ("live_beta", "LIVE_ORDER_SUBMISSION_ENABLED", "true", CP.FLAG_NOT_ALLOWED),
    ("test", "LIVE_ORDER_SUBMISSION_ENABLED", None, CP.FLAG_MISSING),
    ("shadow", "DEMO_ORDER_SUBMISSION_ENABLED", "true", CP.FLAG_NOT_ALLOWED),
    ("demo_certification", "DEMO_ORDER_SUBMISSION_ENABLED", "false", CP.FLAG_NOT_ALLOWED),
    ("live_beta", "BILLING_ENFORCED", "true", CP.BILLING),
    ("shadow", "PROFILE_ALERTS_ENV_FILE", None, CP.ALERTING),
    ("demo_certification", "PROFILE_OFFSITE_BACKUP_ENV_FILE", None, CP.BACKUP),
    ("live_beta", "PROFILE_BACKUP_TIMERS", "cosmicforge-db-backup.timer", CP.BACKUP),
    ("scaled_live", "GOLIVE_REVOCATION_ANCHOR", None, CP.ANCHOR),
    ("shadow", "SECRET_KEY", "x" * 48, CP.SECRET_INLINE),
    ("development", "BINANCE_API_SECRET", "", CP.SECRET_INLINE),
    ("live_beta", "STRIPE_WEBHOOK_SECRET", "placeholder", CP.SECRET_INLINE),
    ("test", "ALERT_TELEGRAM_BOT_TOKEN", "t", CP.SECRET_INLINE),
    ("shadow", "PROFILE_SECRETS_ENV_FILE", "backends/bot-backend/.env", CP.SECRETS_FILE),
    ("live_beta", "PROFILE_SECRETS_ENV_FILE", None, CP.SECRETS_FILE),
    ("development", "SECRET_KEY_FILE", "relative/secret", CP.SECRETS_FILE),
]


@pytest.mark.parametrize("name,key,value,code", MUTATIONS, ids=[f"{m[0]}-{m[1]}-{m[3]}" for m in MUTATIONS])
def test_each_rule_refuses_with_its_own_code(name, key, value, code):
    report = CP.validate_profile(with_line(text(name), key, value), live_status=CAPABLE)
    assert not report.structure_ok and not report.admissible
    assert code in report.codes, report.errors


def test_the_live_flag_without_its_operator_act_note_is_refused():
    source = text("live_beta").replace("# OPERATOR ACT:", "# note:")
    assert CP.OPERATOR_ACT in CP.validate_profile(source, live_status=CAPABLE).codes


@pytest.mark.parametrize("source,code", [
    ("APP_ENV=TEST\n", CP.NAME_MISSING), ("CONFIG_PROFILE=staging\n", CP.UNKNOWN),
    ("CONFIG_PROFILE=test\nCONFIG_PROFILE=test\n", CP.PARSE_ERROR), ("CONFIG_PROFILE=test\nexport X=1\n", CP.PARSE_ERROR),
])
def test_unknown_unnamed_or_unparseable_profiles_are_refused(source, code):
    assert code in CP.validate_profile(source).codes


def test_a_file_declaring_another_profile_is_refused():
    assert CP.NAME_MISMATCH in CP.validate_profile(text("shadow"), expected="live_beta").codes


def test_the_validator_ceilings_are_the_platform_ceilings():
    from app.risk.system_limits import SystemLimits
    from shared_lib.broker.rate_budget import DEFAULT_UTILISATION
    limits = SystemLimits()
    assert CP.MAX_RISK_PER_TRADE_PCT_CEILING == Decimal(str(limits.max_risk_per_trade_ceiling)) * 100
    assert CP.MAX_STOP_LOSS_PCT_CEILING == Decimal(str(limits.max_stop_loss_pct))
    assert CP.MAX_BUDGET_FRACTION == Decimal(str(DEFAULT_UTILISATION))


@pytest.mark.parametrize("name", ["shadow", "demo_certification", "live_beta", "scaled_live"])
def test_the_production_topology_templates_satisfy_the_real_production_profile(name):
    from pydantic import ValidationError
    from app.core.config import Settings
    values, _, _ = CP.parse_env(text(name))
    settings = Settings(_env_file=None, **values)
    assert settings.production and settings.LIVE_ORDER_SUBMISSION_ENABLED is False
    assert settings.DEMO_ORDER_SUBMISSION_ENABLED is (name == "demo_certification")
    assert settings.BROKER_ASSIGNMENT_FENCE_MARGIN_MS < settings.BROKER_ASSIGNMENT_TTL_MS
    with pytest.raises(ValidationError, match="LIVE_ORDER_SUBMISSION_ENABLED must be false"):   # K4, this build
        Settings(_env_file=None, **{**values, "LIVE_ORDER_SUBMISSION_ENABLED": "true"})


def test_the_ops_paths_are_the_systemd_units_paths():
    values, _, _ = CP.parse_env(text("shadow"))
    systemd = REPO / "deploy" / "systemd"
    assert f"EnvironmentFile=-{values['PROFILE_ALERTS_ENV_FILE']}" in (systemd / "cosmicforge-alert@.service").read_text(
        encoding="utf-8")
    assert f"EnvironmentFile=-{values['PROFILE_OFFSITE_BACKUP_ENV_FILE']}" in (
        systemd / "cosmicforge-offsite-backup.service").read_text(encoding="utf-8")
    for timer in values["PROFILE_BACKUP_TIMERS"].split(","):
        assert (systemd / timer).is_file(), timer
    assert values["GOLIVE_REVOCATION_ANCHOR"].startswith("/var/lib/cosmicforge-anchor/")   # the image's anchor mount
    assert "/var/lib/cosmicforge-anchor" in (REPO / "deploy" / "containers" / "backend.Dockerfile").read_text(
        encoding="utf-8")


def test_the_cli_refuses_the_live_profiles_and_accepts_their_structure(capsys):
    spec = importlib.util.spec_from_file_location("validate_config_profile_cli",
                                                  REPO / "scripts" / "validate_config_profile.py")
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    files = [str(p) for p in FILES.values()]
    assert cli.main(files) == 1
    out = capsys.readouterr().out
    assert out.count("PROFILE_REQUIRES_LIVE_CAPABLE_BUILD") == 2
    assert cli.main(["--structure-only", *files]) == 0
    assert cli.main([str(FILES["shadow"])]) == 0
    assert cli.main(["--expect", "live_beta", str(FILES["shadow"])]) == 1
