#!/usr/bin/env python3
"""Validate environment configuration profiles (master prompt section 22).

    python scripts/validate_config_profile.py deploy/profiles/*.env.example
    python scripts/validate_config_profile.py --structure-only deploy/profiles/live-beta.env.example
    python scripts/validate_config_profile.py --expect shadow /etc/cosmicforge/bot-backend.env

Rules: ``backends/shared/shared_lib/core/config_profiles.py``. Prints one JSON report per file. It never prints a value,
only key names and reason codes, and it never writes anything.

Exit status: 0 every file is admissible on this build (with --structure-only: every file's structure is valid);
1 at least one file is refused (the live profiles always are in this build: PROFILE_REQUIRES_LIVE_CAPABLE_BUILD);
2 usage or unreadable file.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "backends" / "shared"))

from shared_lib.core.config_profiles import PROFILES, validate_profile  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--expect", choices=PROFILES, help="the profile the file must declare")
    parser.add_argument("--structure-only", action="store_true",
                        help="judge the structure only (a live profile is still reported as refused)")
    args = parser.parse_args(argv)
    worst = 0
    for path in args.files:
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            print(json.dumps({"file": str(path), "error": type(exc).__name__}))
            worst = 2
            continue
        report = validate_profile(text, expected=args.expect)
        print(json.dumps({"file": str(path), **report.to_plain()}, sort_keys=True))
        ok = report.structure_ok if args.structure_only else report.admissible
        if not ok:
            worst = max(worst, 1)
    return worst


if __name__ == "__main__":
    sys.exit(main())
