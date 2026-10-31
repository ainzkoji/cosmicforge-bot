"""Master prompt section 30: a live-capable build artifact only through an explicit, reviewed release, and a capable
build still authorizes nothing by itself.

Both release keys are injected into the test process only (``_live_release.install_capable_release``: monkeypatch
plus a manifest under ``tmp_path``). The repository's constant stays False and its allowlist stays empty.
"""
from __future__ import annotations

import ast
import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from app.golive import gate as G
from app.golive.authorization import GoLiveAuthority
from app.golive.ledger import GoLiveLedger
from shared_lib.core import live_build as LB
from shared_lib.core import live_release_allowlist as ALLOW
from shared_lib.core import production as P

from _live_release import commit_id, install_capable_release, manifest_doc
from test_golive_gate import ALICE, BOB, REQ, fully_authorized, green_sources

REPO = Path(__file__).resolve().parents[4]
BINANCE_LIVE = "https://fapi.binance.com"


@pytest.fixture(autouse=True)
def no_constructor_network(monkeypatch):
    response = Mock(status_code=200, content=b"{}", headers={}, text="{}")
    response.json.return_value = {"symbols": []}
    monkeypatch.setattr("requests.sessions.Session.get", Mock(return_value=response))


@pytest.fixture
def ledger(platform_db, tmp_path):
    return GoLiveLedger(platform_db, anchor_path=str(tmp_path / "anchor" / "revocations.jsonl"))


def production(**changes):
    from app.core.config import Settings
    values = dict(APP_ENV="PRODUCTION", DATABASE_ROLE="production", ENVIRONMENT_NAME="production", EXECUTION_MODE="live")
    values.update(changes)
    return Settings(_env_file=None, **values)


def blocking(decision):
    return {c.id: c.reason for c in decision.blocking}


# ── the repository build ─────────────────────────────────────────────────────────────────────────────────────
def test_the_repository_build_is_not_live_capable():
    assert P.BUILD_PERMITS_LIVE is False
    assert ALLOW.APPROVED_LIVE_RELEASES == frozenset() and isinstance(ALLOW.APPROVED_LIVE_RELEASES, frozenset)
    status = LB.live_build_status()
    assert status.capable is False and status.reason == LB.NOT_PERMITTED == "LIVE_NOT_PERMITTED_IN_THIS_BUILD"
    assert not any(status.keys.values())


def test_the_source_text_holds_no_approved_release_and_the_constant_false():
    """Read from the files, not the (monkeypatchable) modules."""
    shared = REPO / "backends" / "shared" / "shared_lib" / "core"
    allow = ast.parse((shared / "live_release_allowlist.py").read_text(encoding="utf-8"))
    [assign] = [n for n in allow.body if isinstance(n, ast.AnnAssign)]
    assert ast.unparse(assign.value) == "frozenset()"
    prod = ast.parse((shared / "production.py").read_text(encoding="utf-8"))
    [const] = [n for n in prod.body if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == "BUILD_PERMITS_LIVE"]
    assert isinstance(const.value, ast.Constant) and const.value.value is False
    assert not Path(LB.LIVE_RELEASE_MANIFEST_PATH).exists()


def test_default_build_refuses_a_live_entry_at_the_go_live_gate_with_the_build_reason(ledger):
    fully_authorized(ledger)
    d = G.GoLiveGate(ledger, green_sources()).evaluate(REQ)
    assert not d.authorized and blocking(d) == {"BUILD_PERMITS_LIVE": "LIVE_NOT_PERMITTED_IN_THIS_BUILD"}


# ── each key on its own, and every malformed or mismatched artifact ──────────────────────────────────────────
def test_both_keys_present_and_matching_is_the_only_capable_build(monkeypatch, tmp_path):
    rel = install_capable_release(monkeypatch, tmp_path)
    s = LB.live_build_status()
    assert s.capable and s.reason == LB.CAPABLE and all(s.keys.values())
    assert s.manifest_sha256 == rel["sha256"] and s.release_id == "test-live-0001" and s.build_commit == commit_id()


@pytest.mark.parametrize("case,expected", [
    ("constant_false", LB.NOT_PERMITTED),                 # a baked, pinned manifest without the source constant
    ("allowlist_empty", LB.NOT_APPROVED_IN_SOURCE),       # constant only
    ("allowlist_not_frozenset", LB.ALLOWLIST_INVALID),
    ("allowlist_uppercase", LB.ALLOWLIST_INVALID),
    ("manifest_absent", LB.MANIFEST_ABSENT),
    ("manifest_unpinned", LB.MANIFEST_NOT_APPROVED),
    ("not_json", LB.MANIFEST_MALFORMED),
    ("unknown_field", LB.MANIFEST_MALFORMED),
    ("missing_field", LB.MANIFEST_MALFORMED),
    ("bad_schema", LB.MANIFEST_MALFORMED),
    ("bad_commit", LB.MANIFEST_MALFORMED),
    ("bad_created_at", LB.MANIFEST_MALFORMED),
    ("one_reviewer", LB.REVIEWERS_INSUFFICIENT),
    ("same_reviewer_twice", LB.REVIEWERS_INSUFFICIENT),
    ("no_approval_ref", LB.APPROVAL_REFS_MISSING),
    ("build_commit_unset", LB.BUILD_COMMIT_UNKNOWN),
    ("build_commit_unknown", LB.BUILD_COMMIT_UNKNOWN),
    ("other_commit", LB.COMMIT_MISMATCH),
    ("other_sources", LB.SOURCE_MISMATCH),
])
def test_anything_missing_malformed_unpinned_or_mismatched_is_not_capable(monkeypatch, tmp_path, case, expected):
    kw = {}
    if case == "constant_false":
        kw["constant"] = False
    elif case == "allowlist_empty":
        kw["pin"] = False
    elif case == "manifest_unpinned":
        kw["pin"] = False
    elif case == "not_json":
        kw["manifest"] = b"{not json"
    elif case == "unknown_field":
        kw["extra_field"] = "x"
    elif case == "missing_field":
        kw["manifest"] = json.dumps({k: v for k, v in manifest_doc().items() if k != "approval_refs"}).encode()
    elif case == "bad_schema":
        kw["schema"] = "cosmicforge.live-release/0"
    elif case == "bad_commit":
        kw["git_commit"] = "HEAD"
    elif case == "bad_created_at":
        kw["created_at"] = "2026-13-40T00:00:00Z"
    elif case == "one_reviewer":
        kw["reviewers"] = ["reviewer-one"]
    elif case == "same_reviewer_twice":
        kw["reviewers"] = ["Reviewer-One", " reviewer-one "]
    elif case == "no_approval_ref":
        kw["approval_refs"] = [" "]
    elif case == "build_commit_unset":
        kw["build_version"] = None
    elif case == "build_commit_unknown":
        kw["build_version"] = "unknown"
    elif case == "other_commit":
        kw["build_version"] = commit_id("another build")
    elif case == "other_sources":
        kw["source_tree_sha256"] = LB.manifest_sha256(b"different code")
    rel = install_capable_release(monkeypatch, tmp_path, **kw)
    if case == "allowlist_empty":
        rel["path"].unlink()
    if case == "manifest_unpinned":
        monkeypatch.setattr(ALLOW, "APPROVED_LIVE_RELEASES", frozenset({LB.manifest_sha256(b"another manifest")}))
    if case == "allowlist_not_frozenset":
        monkeypatch.setattr(ALLOW, "APPROVED_LIVE_RELEASES", {rel["sha256"]})
    if case == "allowlist_uppercase":
        monkeypatch.setattr(ALLOW, "APPROVED_LIVE_RELEASES", frozenset({rel["sha256"].upper()}))
    if case == "manifest_absent":
        rel["path"].unlink()
    s = LB.live_build_status()
    assert s.capable is False and s.reason == expected, s
    assert expected in LB.REASON_CODES


def test_a_manifest_edited_after_pinning_is_no_longer_approved(monkeypatch, tmp_path):
    rel = install_capable_release(monkeypatch, tmp_path)
    doc = json.loads(rel["raw"])
    doc["reviewers"].append("reviewer-three")
    rel["path"].write_bytes(json.dumps(doc, sort_keys=True).encode())
    assert LB.live_build_status().reason == LB.MANIFEST_NOT_APPROVED


def test_source_identity_excludes_only_the_allowlist_and_ignores_line_endings(tmp_path):
    shared, app = tmp_path / "shared_lib", tmp_path / "app"
    (shared / "core").mkdir(parents=True)
    app.mkdir()
    (shared / "core" / "production.py").write_bytes(b"BUILD_PERMITS_LIVE = True\n")
    (shared / "core" / "live_release_allowlist.py").write_bytes(b"APPROVED_LIVE_RELEASES = frozenset()\n")
    (app / "main.py").write_bytes(b"x = 1\ny = 2\n")
    roots = {"shared_lib": shared, "app": app}
    base = LB.source_tree_sha256(roots)
    (shared / "core" / "live_release_allowlist.py").write_bytes(b"APPROVED_LIVE_RELEASES = frozenset({'ab'})\n")
    assert LB.source_tree_sha256(roots) == base                 # the pin commit keeps the reviewed identity
    (app / "main.py").write_bytes(b"x = 1\r\ny = 2\r\n")
    assert LB.source_tree_sha256(roots) == base                 # a Windows checkout of the same commit
    (app / "main.py").write_bytes(b"x = 1\ny = 3\n")
    assert LB.source_tree_sha256(roots) != base                 # any code change breaks it
    (app / "main.py").write_bytes(b"x = 1\ny = 2\n")
    (app / "extra.py").write_bytes(b"")
    assert LB.source_tree_sha256(roots) != base                 # so does an added module


# ── K4: the startup refusal of the LIVE flag holds for every non-capable build ───────────────────────────────
@pytest.mark.parametrize("setup,reason", [
    ("default", LB.NOT_PERMITTED), ("constant_only", LB.NOT_APPROVED_IN_SOURCE),
    ("no_manifest", LB.MANIFEST_ABSENT), ("other_commit", LB.COMMIT_MISMATCH),
])
def test_k4_refuses_the_live_flag_at_startup_in_a_non_capable_build(monkeypatch, tmp_path, setup, reason):
    if setup != "default":
        rel = install_capable_release(monkeypatch, tmp_path, pin=setup != "constant_only",
                                      build_version=commit_id("x") if setup == "other_commit" else "default")
        if setup == "no_manifest":
            rel["path"].unlink()
    with pytest.raises(ValidationError, match=f"LIVE_ORDER_SUBMISSION_ENABLED must be false.*{reason}"):
        production(LIVE_ORDER_SUBMISSION_ENABLED=True)
    assert production().LIVE_ORDER_SUBMISSION_ENABLED is False       # the flag off always starts


def test_a_capable_build_may_start_with_the_live_flag_but_the_flag_is_still_the_operators(monkeypatch, tmp_path):
    install_capable_release(monkeypatch, tmp_path)
    assert production(LIVE_ORDER_SUBMISSION_ENABLED=True).LIVE_ORDER_SUBMISSION_ENABLED is True
    assert production().LIVE_ORDER_SUBMISSION_ENABLED is False       # capability never turns the flag on
    assert production().configuration_matrix()["LIVE BUILD"] == LB.CAPABLE


# ── a capable build authorizes nothing by itself: every other prerequisite still refuses with its own code ───
def test_a_capable_build_with_everything_else_in_place_is_the_only_authorized_entry(ledger, monkeypatch, tmp_path):
    install_capable_release(monkeypatch, tmp_path)
    fully_authorized(ledger)
    d = G.GoLiveGate(ledger, green_sources()).evaluate(REQ)
    assert d.authorized
    build = {c.id: c for c in d.conditions}["BUILD_PERMITS_LIVE"]
    assert build.status == G.PASS and build.evidence["release_id"] == "test-live-0001"


def test_a_capable_build_alone_with_nothing_else_authorizes_nothing(ledger, monkeypatch, tmp_path):
    install_capable_release(monkeypatch, tmp_path)
    d = G.GoLiveGate(ledger, green_sources(live_submission_enabled=lambda: False)).evaluate(REQ)
    assert not d.authorized and "BUILD_PERMITS_LIVE" not in blocking(d)
    for cid in ("LIVE_ORDER_SUBMISSION_ENABLED", "OPERATOR_LIVE_SWITCH_ON", "OPERATOR_APPROVAL_AUDITED",
                "CUSTOMER_LIVE_RISK_CONSENT", "STRATEGY_DEMO_RUN_COMPLETED", "LIVE_BETA_ENROLLMENT_CURRENT"):
        assert cid in blocking(d), cid


MISSING = {
    "operator_submission_flag": (dict(live_submission_enabled=lambda: False),
                                 {"LIVE_ORDER_SUBMISSION_ENABLED": "LIVE_ORDER_SUBMISSION_DISABLED"}),
    "exchange_environment_demo": (dict(account=lambda a: {"id": a, "user_id": "alice", "broker_id": "binance",
                                                          "status": "connected", "environment": "DEMO"}),
                                  {"ACCOUNT_CONNECTION_VALID": "ACCOUNT_ENVIRONMENT_NOT_LIVE"}),
    "exchange_credentials_disconnected": (dict(account=lambda a: {"id": a, "user_id": "alice", "broker_id": "binance",
                                                                  "status": "disconnected", "environment": "LIVE"}),
                                          {"ACCOUNT_CONNECTION_VALID": "ACCOUNT_STATUS_DISCONNECTED"}),
    "exchange_venue_mismatch": (dict(account=lambda a: {"id": a, "user_id": "alice", "broker_id": "bybit",
                                                        "status": "connected", "environment": "LIVE"}),
                                {"ACCOUNT_CONNECTION_VALID": "ACCOUNT_VENUE_MISMATCH"}),
    "family_certification": (dict(family_entry=lambda f: {"family_version": "1.0.0", "certification_status": "NOT_CERTIFIED",
                                                          "allowed_modes": ["LIVE"], "kill_switch_state": "OFF"}),
                             {"STRATEGY_RESEARCH_PASSED": "RESEARCH_NOT_CERTIFIED"}),
    "family_promotion": (dict(family_entry=lambda f: {"family_version": "1.0.0", "certification_status": "CERTIFIED",
                                                      "allowed_modes": ["SHADOW", "DEMO"], "governance_stage": "M6",
                                                      "kill_switch_state": "OFF"}),
                         {"FAMILY_STAGE_PERMITS_LIVE": "STAGE_M6_DOES_NOT_PERMIT_LIVE"}),
    "venue_certification": (dict(adapter_certified=lambda v, e, n: (False, "VENUE_ADAPTER_NOT_CERTIFIED")),
                            {"VENUE_ADAPTER_CERTIFIED": "VENUE_ADAPTER_NOT_CERTIFIED"}),
}


@pytest.mark.parametrize("missing", sorted(MISSING))
def test_a_capable_build_still_refuses_each_missing_prerequisite_with_its_own_code(ledger, monkeypatch, tmp_path,
                                                                                    missing):
    install_capable_release(monkeypatch, tmp_path)
    fully_authorized(ledger)
    override, expected = MISSING[missing]
    d = G.GoLiveGate(ledger, green_sources(**override)).evaluate(REQ)
    assert not d.authorized and blocking(d) == expected


def test_a_capable_build_refuses_without_customer_consent(ledger, monkeypatch, tmp_path):
    install_capable_release(monkeypatch, tmp_path)
    fully_authorized(ledger)
    GoLiveAuthority(ledger).revoke_consent(account_id="acct-1", user_id="alice")
    assert blocking(G.GoLiveGate(ledger, green_sources()).evaluate(REQ)) == {"CUSTOMER_LIVE_RISK_CONSENT": "CONSENT_REVOKED"}


def test_a_capable_build_refuses_while_the_operator_live_switch_is_off(ledger, monkeypatch, tmp_path):
    install_capable_release(monkeypatch, tmp_path)
    fully_authorized(ledger)
    GoLiveAuthority(ledger).switch_off("binance", BOB, reason="operator closes the venue")
    assert blocking(G.GoLiveGate(ledger, green_sources()).evaluate(REQ)) == {"OPERATOR_LIVE_SWITCH_ON": "LIVE_SWITCH_OFF"}


# ── revocation: new entries refused at once, protective/reducing recovery still allowed ──────────────────────
def _transport(payload, method="POST"):
    from shared_lib.core.production import require_broker_mutation_permission
    require_broker_mutation_permission(method, "/fapi/v1/order", environment="LIVE", broker="binance",
                                       base_url=BINANCE_LIVE, client=None, payload=payload)


@pytest.mark.parametrize("revoked", ["operator_live_switch", "venue_certification"])
def test_revocation_refuses_new_entries_immediately_while_reducing_recovery_stays_allowed(ledger, monkeypatch, tmp_path,
                                                                                          revoked):
    from app.core import config
    install_capable_release(monkeypatch, tmp_path)
    monkeypatch.setattr(config, "settings", production(LIVE_ORDER_SUBMISSION_ENABLED=True))
    fully_authorized(ledger)
    certified = {"ok": True}
    gate = G.GoLiveGate(ledger, green_sources(
        adapter_certified=lambda v, e, n: (True, "") if certified["ok"] else (False, "CERTIFICATION_REVOKED")))
    assert gate.evaluate(REQ).authorized                           # before: the entry would pass the gate
    if revoked == "operator_live_switch":
        GoLiveAuthority(ledger).switch_off("binance", ALICE, reason="incident: one operator closes at once")
        expected = {"OPERATOR_LIVE_SWITCH_ON": "LIVE_SWITCH_OFF"}
    else:
        certified["ok"] = False
        expected = {"VENUE_ADAPTER_CERTIFIED": "CERTIFICATION_REVOKED"}
    assert blocking(gate.evaluate(REQ)) == expected                # the very next entry is refused
    # Protective / reducing orders never pass through the go-live gate: the transport still sends them.
    _transport({"symbol": "BTCUSDT", "side": "SELL", "type": "STOP_MARKET", "reduceOnly": "true"})
    _transport({"symbol": "BTCUSDT", "side": "SELL", "type": "STOP_MARKET", "closePosition": "true"})
    _transport({"symbol": "BTCUSDT", "orderId": "1"}, method="DELETE")
    # ...while an opening order without the boundary's entry permit is refused before the network.
    with pytest.raises(ValueError, match="CATI_ENTRY_AUTHORITY_REQUIRED"):
        _transport({"symbol": "BTCUSDT", "side": "BUY", "type": "MARKET", "quantity": "0.001"})


def test_a_build_that_is_not_capable_refuses_the_live_entry_even_when_every_other_condition_passes(ledger, monkeypatch,
                                                                                                    tmp_path):
    rel = install_capable_release(monkeypatch, tmp_path)
    fully_authorized(ledger)
    gate = G.GoLiveGate(ledger, green_sources())
    assert gate.evaluate(REQ).authorized
    rel["path"].unlink()                                            # an image without the baked manifest
    assert blocking(gate.evaluate(REQ)) == {"BUILD_PERMITS_LIVE": LB.MANIFEST_ABSENT}


# ── the other readers of the build status ────────────────────────────────────────────────────────────────────
def test_l08_live_policy_follows_the_build_status_not_the_constant(monkeypatch, tmp_path):
    from app.trading_intelligence.scale import market_entry_policy as M
    assert M._live_permitted() is False
    monkeypatch.setattr(P, "BUILD_PERMITS_LIVE", True)
    assert M._live_permitted() is False                            # the constant alone is not enough
    install_capable_release(monkeypatch, tmp_path)
    assert M._live_permitted() is True


def test_admin_build_status_is_read_only_and_names_the_reason(monkeypatch, tmp_path):
    from app.api import golive
    assert golive.build(admin_id="op")["live_build"]["reason"] == LB.NOT_PERMITTED
    assert production().configuration_matrix()["LIVE BUILD"] == LB.NOT_PERMITTED
    install_capable_release(monkeypatch, tmp_path, pin=False)
    monkeypatch.setattr(ALLOW, "APPROVED_LIVE_RELEASES", frozenset({LB.manifest_sha256(b"another manifest")}))
    body = golive.build(admin_id="op")["live_build"]
    assert body["capable"] is False and body["reason"] == LB.MANIFEST_NOT_APPROVED
    assert {r.path for r in golive.router.routes if "GET" in r.methods} >= {"/api/v1/admin/golive/build"}


# ── the release tool: validates and prints, refuses CI, never writes ─────────────────────────────────────────
@pytest.fixture
def release_tool():
    spec = importlib.util.spec_from_file_location("live_release_manifest_tool",
                                                  REPO / "scripts" / "release" / "live_release_manifest.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def tree(tmp_path):
    shared, app = tmp_path / "src" / "shared_lib", tmp_path / "src" / "app"
    (shared / "core").mkdir(parents=True)
    app.mkdir(parents=True)
    (shared / "core" / "production.py").write_bytes(b"BUILD_PERMITS_LIVE = True\n")
    (app / "main.py").write_bytes(b"x = 1\n")
    return {"shared_lib": shared, "app": app}


def _write_manifest(tmp_path, tree, **overrides):
    doc = {**manifest_doc(source_tree_sha256=LB.source_tree_sha256(tree)), **overrides}
    path = tmp_path / "review" / "manifest.json"
    path.parent.mkdir(exist_ok=True)
    path.write_bytes(json.dumps(doc).encode())
    return path


def _fake_git(head, changed=()):
    def git(*args):
        if args[0] == "rev-parse":
            return head + "\n"
        if args[0] == "diff":
            return "\n".join(changed)
        return ""
    return git


def _snapshot(*dirs):
    return {p: p.read_bytes() for d in dirs for p in Path(d).rglob("*") if p.is_file()}


@pytest.mark.parametrize("var", ["CI", "GITHUB_ACTIONS"])
def test_the_release_tool_refuses_to_run_in_ci(release_tool, tmp_path, tree, monkeypatch, capsys, var):
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.setenv(var, "true")
    assert release_tool.main([str(_write_manifest(tmp_path, tree))]) == release_tool.EXIT_CI
    assert json.loads(capsys.readouterr().out)["reason"] == "LIVE_RELEASE_TOOL_REFUSED_IN_CI"


def test_the_release_tool_validates_and_prints_the_hash_and_writes_nothing(release_tool, tmp_path, tree):
    path = _write_manifest(tmp_path, tree)
    allowlist = REPO / release_tool.ALLOWLIST_PATH
    before, allow_before = _snapshot(tmp_path), allowlist.read_bytes()
    code, report = release_tool.check(path, roots=tree, git=_fake_git(commit_id()))
    assert code == release_tool.EXIT_OK and report["valid"] is True
    assert report["manifest_sha256"] == LB.manifest_sha256(path.read_bytes())
    assert any(report["manifest_sha256"] in step for step in report["next_steps"])
    assert _snapshot(tmp_path) == before and allowlist.read_bytes() == allow_before
    # the pin commit may differ from the reviewed code commit in the allowlist module only
    code, _ = release_tool.check(path, roots=tree, git=_fake_git(commit_id("pin"), [release_tool.ALLOWLIST_PATH]))
    assert code == release_tool.EXIT_OK


def test_the_release_tool_never_writes_files(release_tool):
    tree = ast.parse((REPO / "scripts" / "release" / "live_release_manifest.py").read_text(encoding="utf-8"))
    names = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | {
        n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    assert not names & {"write_text", "write_bytes", "open", "unlink", "rename", "replace_file", "mkdir"}


@pytest.mark.parametrize("problem,expected_code,reason", [
    ("one_reviewer", 4, LB.REVIEWERS_INSUFFICIENT),
    ("bad_commit", 4, LB.MANIFEST_MALFORMED),
    ("other_sources", 5, LB.SOURCE_MISMATCH),
    ("head_beyond_allowlist", 5, "HEAD_DIFFERS_FROM_RELEASE_COMMIT_BEYOND_THE_ALLOWLIST"),
])
def test_the_release_tool_refuses_what_the_runtime_would_refuse(release_tool, tmp_path, tree, problem, expected_code,
                                                                reason):
    overrides = {"one_reviewer": {"reviewers": ["solo"]}, "bad_commit": {"git_commit": "main"},
                 "other_sources": {"source_tree_sha256": LB.manifest_sha256(b"x")}}.get(problem, {})
    path = _write_manifest(tmp_path, tree, **overrides)
    changed = ["backends/bot-backend/app/main.py"] if problem == "head_beyond_allowlist" else []
    code, report = release_tool.check(path, roots=tree, git=_fake_git(commit_id("later"), changed) if changed
                                      else _fake_git(commit_id()))
    assert code == expected_code and report["valid"] is False and report["reason"] == reason


def test_the_go_live_gate_records_the_build_keys_as_evidence(ledger, monkeypatch, tmp_path):
    install_capable_release(monkeypatch, tmp_path, build_version=commit_id("other"))
    fully_authorized(ledger)
    c = {c.id: c for c in G.GoLiveGate(ledger, green_sources()).evaluate(REQ).conditions}["BUILD_PERMITS_LIVE"]
    assert c.status == G.FAIL and c.reason == LB.COMMIT_MISMATCH
    assert c.evidence["keys"] == {"source_constant": True, "source_allowlist": True, "artifact_manifest": True,
                                  "manifest_pinned": True, "commit_match": False, "source_match": False}
