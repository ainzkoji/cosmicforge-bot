from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402
from shared_lib.persistence.tradingview import (  # noqa: E402
    MODE_ADVISORY_ONLY,
    MODE_EXTERNAL_SIGNAL_CANDIDATE,
    create_webhook,
)


def _db_path() -> str | None:
    url = os.environ.get("DATABASE_URL", "")
    if url.startswith("sqlite:///"):
        return url.replace("sqlite:///", "")
    return None


def main() -> None:
    db_path = _db_path()
    migrate(db_path)
    db = DB(path=db_path)
    bot_id = os.environ.get("TRADINGVIEW_SEED_BOT_ID", "dev-bot")
    mode = os.environ.get("TRADINGVIEW_SEED_MODE", MODE_ADVISORY_ONLY).strip().upper()
    if mode not in {MODE_ADVISORY_ONLY, MODE_EXTERNAL_SIGNAL_CANDIDATE}:
        raise SystemExit(
            "TRADINGVIEW_SEED_MODE must be ADVISORY_ONLY or EXTERNAL_SIGNAL_CANDIDATE"
        )
    result = create_webhook(
        db,
        bot_id=bot_id,
        name=f"Dev TradingView {mode} Webhook",
        allowed_symbols=None,
        allowed_actions=["BUY", "SELL"],
        mode=mode,
    )
    webhook_url = f"http://127.0.0.1:9000/api/v1/tradingview/webhook/{result['token']}"
    sample_payload = {
        "token": result["token"],
        "bot_id": bot_id,
        "alert_id": "tv-dev-{{time}}",
        "strategy_name": f"TradingView Dev {mode}",
        "symbol": "BINANCE:BTCUSDT.P",
        "exchange": "BINANCE",
        "timeframe": "{{interval}}",
        "action": "BUY",
        "side": "LONG",
        "price": "{{close}}",
        "timestamp": "{{timenow}}",
        "confidence": 0.75,
        "comment": "{{strategy.order.comment}}",
    }
    print(f"TradingView {mode} webhook created.")
    print(f"webhook_id={result['id']}")
    print(f"webhook_url={webhook_url}")
    print("raw token is shown once; it is stored hashed only.")
    print(json.dumps(sample_payload, indent=2))


if __name__ == "__main__":
    main()
