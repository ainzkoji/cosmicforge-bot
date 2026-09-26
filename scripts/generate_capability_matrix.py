#!/usr/bin/env python3
"""Generate docs/architecture/cati_market_capability_matrix.json FROM CODE (no hand-written states).

Sources:
* declared broker profiles (``shared_lib.broker.capabilities``) -- per capability state + reason;
* the account capability engine (``app.activation.market``) evaluated per venue x environment over
  the venue's CURRENT public discovery (``--discover``) with a trade-only key, no withdraw permission;
* CATI auto-activation states (``app.activation.cati``) at governance phase M0 (no approved transition);
* the canonical registry summary over the discovered instruments.

    python scripts/generate_capability_matrix.py --discover --out docs/architecture/cati_market_capability_matrix.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [os.path.join(REPO, "backends", "bot-backend"), os.path.join(REPO, "backends", "shared")]

TRADE_ONLY_KEY = {"READ_ACCOUNT": True, "READ_POSITIONS": True, "READ_ORDERS": True, "TRADE": True,
                  "INTERNAL_TRANSFER": True, "WITHDRAW": False}


def discover():
    import requests

    from app.exchange import instruments as I

    out = {}
    info = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=30).json()
    out["binance"] = [i for i in (I.parse_binance_symbol(s) for s in info.get("symbols", [])) if i]
    def bybit_page(cursor):
        d = requests.get("https://api.bybit.com/v5/market/instruments-info",
                         params={"category": "linear", "limit": 1000, **({"cursor": cursor} if cursor else {})},
                         timeout=30).json()
        if d.get("retCode") != 0:
            raise RuntimeError(f"bybit instruments-info retCode={d.get('retCode')}")
        return d["result"].get("list") or [], d["result"].get("nextPageCursor")

    # the same truncation / repeated-cursor protection as BybitClient.discover_instruments
    rows = I.collect_cursor_pages(bybit_page, source="bybit instruments-info (matrix)")
    out["bybit"] = [i for i in (I.parse_bybit_instrument(r) for r in rows) if i]
    d = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/contracts", timeout=30).json()
    out["bingx"] = [i for i in (I.parse_bingx_contract(c) for c in d.get("data") or []) if i]
    return out


def _cati_at_m0(act):
    """CATI states against a fresh governance DB: no approved transition = M0, no promoted model."""
    import tempfile

    from shared_lib.persistence.cati_schema import ensure_cati_schema
    from shared_lib.persistence.db import DB

    from app.trading_intelligence.ml.registry import ModelRegistry

    with tempfile.TemporaryDirectory() as d:
        db = DB(path=os.path.join(d, "m0.db"))
        ensure_cati_schema(db)
        return act.cati_status(db, registry=ModelRegistry(db))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--discover", action="store_true", help="use the venues' current public discovery")
    args = ap.parse_args()

    from shared_lib.broker.capabilities import DECLARED_PROFILES, WITHDRAWALS_SUPPORTED_BY_PLATFORM

    from app.activation import cati as act
    from app.activation.market import account_market_status
    from app.exchange.canonical_registry import build_registry

    for flag in act.OVERRIDE_FLAGS.values():  # the matrix shows AUTO (no operator override)
        os.environ.pop(flag, None)
    discovered = discover() if args.discover else {}
    matrix = {
        "schema": "cati-market-capability-matrix-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_by": "scripts/generate_capability_matrix.py",
        "withdrawals_supported_by_platform": WITHDRAWALS_SUPPORTED_BY_PLATFORM,
        "withdrawal_permission_required": False,
        "declared_broker_profiles": {b: p.to_dict()["capabilities"] for b, p in DECLARED_PROFILES.items()},
        "account_capabilities": {},
        "discovery": {},
        "cati_activation_at_M0": _cati_at_m0(act),
    }
    for broker, ins in discovered.items():
        by_class = {}
        for i in ins:
            k = f"{i.asset_class}:{'API_TRADABLE' if i.api_tradable else 'NOT_TRADABLE'}"
            by_class[k] = by_class.get(k, 0) + 1
        matrix["discovery"][broker] = {"instruments": len(ins), "by_asset_class_and_state": dict(sorted(by_class.items()))}
        for env in ("DEMO", "LIVE"):
            st = account_market_status(broker=broker, environment=env, permissions=TRADE_ONLY_KEY, instruments=ins,
                                       transfers_in_flight=0)
            matrix["account_capabilities"][f"{broker}:{env}"] = {
                # the account mode is not read here (no key): mode-dependent brokers show UNKNOWN topology
                "topology_class": st["topology_class"],
                "capabilities": {k: {"state": v["state"], "status": v["status"], "reason": v["reason"]}
                                 for k, v in st["capabilities"].items()},
                "markets": {f: {k: m[k] for k in ("markets_available", "markets_api_tradable",
                                                  "markets_cati_eligible", "blocked_reasons")}
                            | {"execution_status": m["execution"]["status"],
                               "execution_reason": m["execution"]["reason"],
                               "execution_reason_class": m["execution"]["reason_class"],
                               "cati_status": m["cati"]["status"], "cati_reason": m["cati"]["reason"],
                               "cati_reason_class": m["cati"]["reason_class"]}
                            for f, m in st["markets"].items()}}
    if discovered:
        matrix["canonical_registry"] = build_registry([i for v in discovered.values() for i in v]).summary()
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(matrix, fh, indent=2, sort_keys=True, default=str)
    print(json.dumps({"out": args.out, "venues": list(discovered)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
