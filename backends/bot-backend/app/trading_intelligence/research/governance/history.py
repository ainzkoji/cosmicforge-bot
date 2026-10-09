"""One-time import of the reconstructed historical hypotheses into the register (Section H, Step 2.1).

The source is a reviewed, committed document (``docs/research/registry/historical_hypotheses_v1.json``) built
from the registries, reports and Git history that already exist. Nothing in those files is moved, rewritten or
deleted: the register records point at them. Unsupported fields carry the literal ``UNKNOWN``.

The import is recoverable and repeatable: it is identified by the source hash, an interrupted import resumes
where it stopped, a completed one is a no-op, and a DIFFERENT source is refused -- history is imported once.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from .register import (
    HISTORICAL_IMPORT, HYPOTHESIS, HYPOTHESIS_FIELDS, RegisterError, ResearchRegister, canonical_text_sha256,
    repository_root,
)

HISTORICAL_SOURCE_RELATIVE_PATH = "docs/research/registry/historical_hypotheses_v1.json"
#: ``(seq, record_hash)`` of the last imported historical hypothesis in the committed register. Every reader of
#: the authoritative register pins it, so the seventeen earlier attempts can be neither edited nor dropped.
HISTORICAL_ANCHOR = (18, "eb3a0b71e499e8fa50c1bf70475ac092e3d2819f24ee9205e83fae907c03908f")


def _record_body(h: Dict[str, Any]) -> Dict[str, Any]:
    return {**{f: h[f] for f in HYPOTHESIS_FIELDS}, "source_identifier": h["source_identifier"],
            "notes": h.get("notes", ""), "origin": "HISTORICAL_IMPORT"}


def import_historical_hypotheses(register: ResearchRegister, source: Optional[Path] = None, *,
                                 now: Optional[str] = None) -> Dict[str, Any]:
    path = Path(source) if source else repository_root() / HISTORICAL_SOURCE_RELATIVE_PATH
    doc = json.loads(path.read_text(encoding="utf-8"))
    digest = canonical_text_sha256(path)
    hyps = sorted(doc["hypotheses"], key=lambda h: h["hypothesis_number"])
    if [h["hypothesis_number"] for h in hyps] != list(range(1, len(hyps) + 1)):
        raise RegisterError("historical source: hypothesis numbers must run 1..N without a gap")
    prior = register.of_type(HISTORICAL_IMPORT)
    if prior and prior[0]["body"]["source_sha256"] != digest:
        raise RegisterError("the historical record was already imported from a different source; it is never "
                            "re-imported or replaced")
    if not prior:
        if register.hypothesis_count():
            raise RegisterError("historical hypotheses are imported into an EMPTY register, before any new one")
        register.append(HISTORICAL_IMPORT, {
            "source_path": HISTORICAL_SOURCE_RELATIVE_PATH if source is None else path.name,
            "source_sha256": digest, "counting_rule": doc["counting_rule"], "hypotheses_imported": len(hyps),
            "related_records_not_numbered": doc.get("related_records_not_numbered", []),
            "known_gaps": doc.get("known_gaps", []), "prepared_from": doc.get("prepared_from")}, now=now)
    existing = {h["hypothesis_number"]: h for h in register.hypotheses() if h.get("origin") == "HISTORICAL_IMPORT"}
    added = 0
    for h in hyps:
        body = _record_body(h)
        have = existing.get(h["hypothesis_number"])
        if have is not None:
            if {k: have[k] for k in body} != json.loads(json.dumps(body)):
                raise RegisterError(f"hypothesis {h['hypothesis_number']} in the register differs from the source")
            continue
        register.append(HYPOTHESIS, body, now=now)
        added += 1
    recs = register.records()
    last = [r for r in recs if r["type"] == HYPOTHESIS and r["body"].get("origin") == "HISTORICAL_IMPORT"][-1]
    return {"imported_now": added, "historical_hypotheses": len(hyps), "source_sha256": digest,
            "anchor": [last["seq"], last["record_hash"]]}


__all__ = ["import_historical_hypotheses", "HISTORICAL_SOURCE_RELATIVE_PATH", "HISTORICAL_ANCHOR"]
