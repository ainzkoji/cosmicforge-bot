#!/usr/bin/env python3
"""Explicit credential rotation. Dry-run default. No key material in output.
Load BROKER_SECRET_KEY and historical SECRET_KEY from the service environment.
Runtime legacy decryption need not be enabled. No implicit DB or schema init.
"""
import argparse
import os
import sqlite3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'shared'))
from shared_lib.broker.credential_migration import rotate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if not os.getenv('BROKER_SECRET_KEY') or not Path(args.db).is_file():
        print('CONFIGURATION_REQUIRED')
        return 2
    conn = sqlite3.connect(args.db, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        results = rotate(conn, apply=args.apply)
        for account, status in results:
            print(account, 'MIGRATED' if args.apply and status == 'LEGACY' else status)
        return int(any(status == 'UNREADABLE' for _, status in results))
    finally:
        conn.close()


if __name__ == '__main__':
    raise SystemExit(main())
