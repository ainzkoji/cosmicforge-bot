"""Mandate registration and hash pinning (Section H, Step 2.2).

A mandate is registered ONCE, before any official run, as an immutable register record that pins:

* ``specification_sha256`` -- the frozen specification text (CRLF-insensitive, i.e. the hash Git stores);
* ``rule_artifact_hash``   -- the machine-readable rule set the code actually executes;
* ``research_code_commit`` -- the commit the registration was made on.

``verify_mandate`` recomputes all of it from the working tree on every evaluation and refuses on any
difference. A permitted change is a numbered amendment with its own pinned artifact; the original record is
never replaced. An amendment that changes a strategy rule does not belong to the same hypothesis: the register
refuses to treat it as one (``changes_strategy_rules`` must be False for a run to proceed).
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional

from app.trading_intelligence.hashing import stable_hash

from .register import (
    HYPOTHESIS, MANDATE_AMENDMENT, MANDATE_REGISTERED, REGISTER_SCHEMA_VERSION, RegisterError, ResearchRegister,
    canonical_text_sha256, repository_root, utc_now_iso,
)

#: source trees whose content IS the evaluation: a holdout authorization is bound to their fingerprint
EVALUATION_SOURCE_ROOTS = (
    "backends/bot-backend/app/trading_intelligence/families",
    "backends/bot-backend/app/trading_intelligence/research/governance",
    "backends/bot-backend/app/trading_intelligence/research/evaluator",
    "backends/bot-backend/app/trading_intelligence/research/certification/stats.py",
    "backends/bot-backend/app/market_data/binance_archive.py",
    "backends/bot-backend/app/market_data/daily_dataset.py",
)


class MandateHashMismatch(RegisterError):
    """The specification, rule artifact or an amendment no longer matches what was registered."""


def source_fingerprint(roots: Iterable[str] = EVALUATION_SOURCE_ROOTS, *, repo_root: Optional[Path] = None) -> str:
    """One hash over every evaluation source file (path + CRLF-insensitive content hash). Unlike a commit id it
    does not move when only documents or the register are committed."""
    base = Path(repo_root) if repo_root else repository_root()
    files = []
    for root in roots:
        p = base / root
        paths = [p] if p.is_file() else sorted(q for q in p.rglob("*.py")) if p.is_dir() else []
        files += [(q.relative_to(base).as_posix(), canonical_text_sha256(q)) for q in paths]
    return stable_hash(sorted(files))


def register_hypothesis_and_mandate(register: ResearchRegister, *, hypothesis: Mapping[str, Any],
                                    mandate_id: str, specification_path: str, specification_version: str,
                                    rule_artifact: Mapping[str, Any], research_code_commit: str, registered_by: str,
                                    approved_by: str, approved_at: str, authorization_reference: str,
                                    repo_root: Optional[Path] = None, now: Optional[str] = None) -> Dict[str, Any]:
    """Registers the hypothesis (next free number) and its mandate in that order. Refused by the register when
    the number is not the next one, the identity already exists or the mandate id is taken."""
    base = Path(repo_root) if repo_root else repository_root()
    spec = base / specification_path
    if not spec.is_file():
        raise RegisterError(f"frozen specification {specification_path} is not in the working tree")
    spec_hash = canonical_text_sha256(spec)
    if hypothesis.get("specification_hash") != spec_hash:
        raise MandateHashMismatch("the hypothesis record must carry the hash of the specification it registers")
    when = now or utc_now_iso()
    hyp = register.append(HYPOTHESIS, hypothesis, now=when)
    man = register.append(MANDATE_REGISTERED, {
        "mandate_id": mandate_id, "hypothesis_id": hypothesis["hypothesis_id"], "family_id": hypothesis["family_id"],
        "specification_version": specification_version, "specification_path": specification_path,
        "specification_sha256": spec_hash, "rule_artifact_hash": stable_hash(dict(rule_artifact)),
        "rule_artifact": dict(rule_artifact), "research_code_commit": research_code_commit,
        "registry_version": REGISTER_SCHEMA_VERSION, "registered_at": when, "registered_by": registered_by,
        "approved_at": approved_at, "approved_by": approved_by, "authorization_reference": authorization_reference,
        "live_trading": "DISABLED", "demo_promotion": "NOT_AUTHORIZED", "research_engine": "CATI",
    }, now=when)
    return {"hypothesis_record": hyp, "mandate_record": man}


def amend_mandate(register: ResearchRegister, *, mandate_id: str, kind: str, summary: str, artifact_path: str,
                  changes_strategy_rules: bool, recorded_by: str, authorization_reference: Optional[str] = None,
                  repo_root: Optional[Path] = None, now: Optional[str] = None) -> Dict[str, Any]:
    base = Path(repo_root) if repo_root else repository_root()
    number = len(register.mandate(mandate_id)["amendments"]) + 1
    return register.append(MANDATE_AMENDMENT, {
        "mandate_id": mandate_id, "amendment_number": number, "kind": kind, "summary": summary,
        "artifact_path": artifact_path, "artifact_sha256": canonical_text_sha256(base / artifact_path),
        "changes_strategy_rules": bool(changes_strategy_rules), "recorded_by": recorded_by,
        "authorization_reference": authorization_reference or "NOT_RECORDED"}, now=now)


def verify_mandate(register: ResearchRegister, mandate_id: str, *, rule_artifact: Mapping[str, Any],
                   repo_root: Optional[Path] = None) -> Dict[str, Any]:
    """Called at the start of EVERY evaluation. Returns the pinned identity; raises on any drift."""
    base = Path(repo_root) if repo_root else repository_root()
    m = register.mandate(mandate_id)                      # RegisterError when unregistered
    spec = base / m["specification_path"]
    if not spec.is_file():
        raise MandateHashMismatch(f"frozen specification {m['specification_path']} is missing")
    actual = canonical_text_sha256(spec)
    if actual != m["specification_sha256"]:
        raise MandateHashMismatch(f"specification hash {actual[:16]} differs from the registered "
                                  f"{m['specification_sha256'][:16]}: the frozen mandate was edited")
    if stable_hash(dict(rule_artifact)) != m["rule_artifact_hash"]:
        raise MandateHashMismatch("the rule set in code differs from the registered rule artifact")
    for a in m["amendments"]:
        path = base / a["artifact_path"]
        if not path.is_file() or canonical_text_sha256(path) != a["artifact_sha256"]:
            raise MandateHashMismatch(f"amendment {a['amendment_number']} artifact was edited or removed")
        if a["changes_strategy_rules"]:
            raise MandateHashMismatch(f"amendment {a['amendment_number']} changes a strategy rule: that is a new "
                                      "hypothesis, not a revision of this one")
    strategy_hash = hashlib.sha256("|".join([m["specification_sha256"], m["rule_artifact_hash"],
                                             *[a["artifact_sha256"] for a in m["amendments"]]]).encode()).hexdigest()
    return {"mandate_id": mandate_id, "hypothesis_id": m["hypothesis_id"], "family_id": m["family_id"],
            "specification_sha256": m["specification_sha256"], "rule_artifact_hash": m["rule_artifact_hash"],
            "amendments": [{"number": a["amendment_number"], "artifact_sha256": a["artifact_sha256"]}
                           for a in m["amendments"]],
            "strategy_hash": strategy_hash, "registration_record_hash": m["registration_record_hash"]}


__all__ = ["register_hypothesis_and_mandate", "amend_mandate", "verify_mandate", "source_fingerprint",
           "MandateHashMismatch", "EVALUATION_SOURCE_ROOTS"]
