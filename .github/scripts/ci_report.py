#!/usr/bin/env python3
"""Post a compact failure summary for a CI job as a pull-request comment.

Reads junit XML files and/or plain log files and writes one markdown comment.
Only used on pull requests; never fails the job.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

LIMIT = 60000


def junit_section(path: Path) -> str:
    try:
        root = ET.parse(path).getroot()
    except Exception as exc:  # unreadable or missing report
        return f"### {path.name}\ncould not read report: {exc}\n"
    suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
    tot = {k: sum(int(s.get(k, 0)) for s in suites) for k in ("tests", "failures", "errors", "skipped")}
    lines = [f"### {path.stem}: {tot['tests']} tests, {tot['failures']} failed, {tot['errors']} errors, {tot['skipped']} skipped"]
    for case in root.iter("testcase"):
        for kind in ("failure", "error"):
            node = case.find(kind)
            if node is None:
                continue
            msg = (node.get("message") or "").strip().replace("\n", " ")[:220]
            tail = (node.text or "").strip().splitlines()[-1:] or [""]
            lines.append(f"- `{case.get('classname')}::{case.get('name')}` [{kind}] {msg} | {tail[0][:200]}")
    return "\n".join(lines) + "\n"


def log_section(path: Path) -> str:
    try:
        text = path.read_text(errors="replace")
    except Exception as exc:
        return f"### {path.name}\ncould not read log: {exc}\n"
    return f"### {path.name} (last lines)\n```\n{text[-12000:]}\n```\n"


def main() -> int:
    title = sys.argv[1]
    parts = [f"## CI report: {title}"]
    for arg in sys.argv[2:]:
        p = Path(arg)
        if not p.exists():
            parts.append(f"### {p.name}\n(not produced)\n")
        elif p.suffix == ".xml":
            parts.append(junit_section(p))
        else:
            parts.append(log_section(p))
    body = "\n".join(parts)
    if len(body) > LIMIT:
        body = body[:LIMIT] + "\n… truncated"
    repo, pr, token = os.environ.get("GITHUB_REPOSITORY"), os.environ.get("PR_NUMBER"), os.environ.get("GITHUB_TOKEN")
    if not (repo and pr and token):
        print(body)
        return 0
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/issues/{pr}/comments",
        data=json.dumps({"body": body}).encode(),
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=30)
    except Exception as exc:
        print(f"could not post comment: {exc}\n{body}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
