#!/usr/bin/env python3
"""Re-encrypt stored broker credentials under the dedicated BROKER_SECRET_KEY.

Earlier revisions encrypted broker credentials with a key derived from the
generic SECRET_KEY (or a constant development key) when BROKER_SECRET_KEY was
absent. This script re-writes every blob that only opens under such a legacy
key so that it opens under the primary key, after which production can run
without ``BROKER_LEGACY_KEY_DECRYPT``.

    BROKER_SECRET_KEY=... BROKER_LEGACY_KEY_DECRYPT=1 \\
        python backends/scripts/rotate_broker_credential_encryption.py --db data/bot.db          # dry run
    ... --apply                                                                              # write

Prints counts only: never a key, a secret or a decrypted field.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "shared"))

from shared_lib.core.security.broker_security import (  # noqa: E402
    ENV_KEY, decrypt_credentials_strict, encrypt_credentials, needs_reencryption,
)
from shared_lib.persistence.db import DB  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if not os.getenv(ENV_KEY):
        print(f"refusing: {ENV_KEY} must be set to the NEW dedicated key")
        return 2
    db = DB(path=args.db)
    counts = {"checked": 0, "already_primary": 0, "reencrypted": 0, "undecryptable": 0}
    with db.connect() as conn:
        for table, key_cols in (("broker_credentials_v2", ("account_id", "version")), ("broker_credentials", ("account_id",))):
            try:
                rows = conn.execute(f"SELECT {', '.join(key_cols)}, encrypted_blob FROM {table}").fetchall()
            except Exception:
                continue
            for row in rows:
                counts["checked"] += 1
                blob = row["encrypted_blob"]
                if not needs_reencryption(blob):
                    counts["already_primary"] += 1
                    continue
                try:
                    data = decrypt_credentials_strict(blob)
                except Exception:
                    counts["undecryptable"] += 1
                    continue
                if args.apply:
                    where = " AND ".join(f"{c}=?" for c in key_cols)
                    conn.execute(f"UPDATE {table} SET encrypted_blob=? WHERE {where}",
                                 (encrypt_credentials(data), *[row[c] for c in key_cols]))
                counts["reencrypted"] += 1
    print({**counts, "applied": bool(args.apply)})
    return 0 if counts["undecryptable"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
