"""``scripts/run_venue_certification.py``: what the operator entry point refuses before anything is built.
No network and no credential: every path that would resolve an account or build a client is replaced by one
that fails the test if it is reached."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[4] / "scripts" / "run_venue_certification.py"


@pytest.fixture
def cli(monkeypatch, tmp_path):
    spec = importlib.util.spec_from_file_location("run_venue_certification", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    import dotenv
    from shared_lib.broker import resolver
    monkeypatch.setattr(dotenv, "load_dotenv", lambda *a, **k: False)          # never the operator's .env
    monkeypatch.setattr(resolver, "resolve_broker_auth",
                        lambda *a, **k: pytest.fail("an account was resolved before the refusal"))
    monkeypatch.chdir(tmp_path)
    return module


def test_live_cannot_be_named(cli):
    with pytest.raises(SystemExit):
        cli.parser().parse_args(["--venue", "bybit", "--execute", "--environment", "LIVE"])
    assert cli.parser().parse_args(["--venue", "bybit"]).environment == "DEMO"


def test_a_real_run_without_its_references_is_refused_before_any_account_is_resolved(cli):
    with pytest.raises(SystemExit, match="REFUSED: a real run names account_id, user_id, operator, "
                                         "authorization_ref, scope, out_dir, max_notional"):
        cli.main(["--venue", "bingx", "--execute"])


def test_a_real_run_with_its_references_but_without_the_settings_is_refused_by_preflight(cli, monkeypatch, capsys, tmp_path):
    for name in ("BROKER_SECRET_KEY", "DATABASE_URL", "DEMO_ORDER_SUBMISSION_ENABLED"):
        monkeypatch.delenv(name, raising=False)
    code = cli.main(["--venue", "binance", "--execute", "--account-id", "brk_x", "--user-id", "u", "--operator", "op",
                     "--authorization-ref", "owner-ref", "--scope", "one symbol", "--max-notional", "200",
                     "--allow-orders", "--out-dir", str(tmp_path / "evidence")])
    assert code == 2
    refused = json.loads(capsys.readouterr().out)
    assert refused["status"] == "REFUSED" and set(refused["problems"]) == {
        "SETTING_NOT_PRESENT:BROKER_SECRET_KEY", "SETTING_NOT_PRESENT:DATABASE_URL",
        "DEMO_ORDER_SUBMISSION_ENABLED_IS_NOT_SET"}


def test_evidence_is_never_written_under_docs_research(cli):
    with pytest.raises(SystemExit, match="docs/research"):
        cli._write(str(Path(cli.REPO) / "docs" / "research" / "x"), {}, "bybit", "demo")
