"""Activate or inspect the canonical local simulator; never a broker account."""
import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "backends/bot-backend"), str(ROOT / "backends/shared")]
from dotenv import load_dotenv
load_dotenv(ROOT / "backends/bot-backend/.env", override=True)
from shared_lib.persistence.db import DB
from app.trading_intelligence.integration.residual_simulation import Book

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["activate", "status"])
    parser.add_argument("--owner")
    parser.add_argument("--virtual-capital", type=float, default=10000.)
    args = parser.parse_args()
    db = DB(str(ROOT / "backends/shared/shared_lib/persistence/cosmicforge.db"))
    book = Book(db)
    if args.action == "activate":
        with db.connect() as c:
            owner = args.owner
            if not owner:
                owners = c.execute("SELECT DISTINCT b.user_id FROM bot_instances b JOIN users u ON u.id=b.user_id WHERE b.status='active' AND u.status='active'").fetchall()
                if len(owners) != 1:
                    raise SystemExit("Exactly one active owner required; specify --owner")
                owner = owners[0][0]
            if not c.execute("SELECT 1 FROM users WHERE id=? AND status='active'", (owner,)).fetchone():
                raise SystemExit("Active owner required")
        book.activate(owner, int(time.time() * 1000), args.virtual_capital)
    print(json.dumps(book.status(), indent=2))
