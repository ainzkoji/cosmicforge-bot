"""Shared helpers for the Section 16 / multi-asset closure tests."""
from __future__ import annotations

import math
import random
from typing import Optional

from _helpers import instrument

from app.trading_intelligence.contracts.exposure import AccountExposureSnapshot, ExposureRecord, ExposureStatus
from app.trading_intelligence.contracts.instrument import FUTURES, InstrumentKey, from_fx_pair
from app.trading_intelligence.contracts.portfolio_intel import PortfolioPolicy
from app.trading_intelligence.contracts.ranking import RankedOpportunity
from app.trading_intelligence.portfolio.context import build_portfolio_market_context
from app.trading_intelligence.portfolio.selector import select_portfolio

T0 = 1_700_000_000_000
BAR = 900_000
N = 300
NOW = T0 + N * BAR


def rows_from_returns(rets, start=T0):
    closes, price = [100.0], 100.0
    for r in rets:
        price *= math.exp(r)
        closes.append(price)
    return [[start + i * BAR, c, c, c, c, 1000.0, start + (i + 1) * BAR - 1] for i, c in enumerate(closes)]


def gen(seed, n=N, scale=0.01):
    rng = random.Random(seed)
    return [rng.gauss(0, scale) for _ in range(n)]


def fx(symbol: str) -> InstrumentKey:
    return from_fx_pair(venue="oanda", venue_symbol=symbol, base=symbol[:3], quote=symbol[3:6])


def fut(root: str) -> InstrumentKey:
    return InstrumentKey(asset_class=FUTURES, base_asset=root, quote_asset="USD", settlement_asset="USD",
                         contract_type="FUTURE", canonical_symbol=f"{root}/USD:FUTURE", venue="ibkr", venue_symbol=root)


def key_for(symbol: str) -> InstrumentKey:
    if len(symbol) == 6 and symbol.isalpha() and not symbol.endswith("USDT"):
        return fx(symbol)
    return instrument(symbol)


def ranked(symbol, score, *, side="LONG", liq=0.8, pos=1, cid=None, key: Optional[InstrumentKey] = None,
           bot="botA", account="acct", cycle="c1"):
    inst = key or key_for(symbol)
    return RankedOpportunity(
        ranked_opportunity_id=f"rk_{symbol}_{side}", economic_opportunity_id=f"eco_{symbol}", veto_decision_id=f"v_{symbol}",
        setup_candidate_id=cid or f"cand_{symbol}_{side}", bot_instance_id=bot, broker_account_id=account, cycle_id=cycle,
        instrument_key=inst, side=side, setup_family="TREND_PULLBACK_V2", rank_score=score,
        normalized_conservative_edge=0.5, normalized_lower_tail=0.5, support_quality=0.5, liquidity_quality=liq,
        uncertainty_penalty_component=0.0, ood_penalty_component=0.0, execution_uncertainty_component=0.0,
        rank_position=pos, ranking_policy_version="1.0.0", ranking_policy_hash="h",
    )


def held(symbol, side="LONG", bot="botB", status=ExposureStatus.OPEN.value, key: Optional[InstrumentKey] = None):
    return ExposureRecord(bot, key or key_for(symbol), side, 1.0, 100.0, 100.0, status)


def snapshot(records=(), account="acct"):
    return AccountExposureSnapshot.build(broker_account_id=account, as_of_time=NOW, open_exposures=tuple(records))


def context(policy=None, factor_rows=None, asset_classes=None, **series):
    policy = policy or PortfolioPolicy()
    rows = {k: rows_from_returns(v) for k, v in series.items()}
    return build_portfolio_market_context(rows, NOW, policy, factor_rows=factor_rows or {}, asset_classes=asset_classes)


def select(cands, ctx, *, slots=2, existing=(), policy=None):
    return select_portfolio(ranked=cands, exposure=snapshot(existing), context=ctx, policy=policy or PortfolioPolicy(),
                            bot_instance_id="botA", cycle_id="c1", available_slots=slots, decision_time=NOW)


# -- DB helpers (canonical migration) ---------------------------------------------------------
def make_db(path):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate

    db = DB(str(path))
    migrate(db)
    return db


def add_bot(db, bot_id, account, broker="binance", market_type="crypto"):
    with db.connect() as c:
        c.execute("INSERT OR IGNORE INTO broker_accounts (id, user_id, broker_id, market_type, status, created_at, updated_at)"
                  " VALUES (?,?,?,?,?,?,?)", (account, "u1", broker, market_type, "active", "2026-01-01", "2026-01-01"))
        c.execute("INSERT INTO bot_instances (id, user_id, broker_account_id, market_type, strategy_id, mode, status,"
                  " created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                  (bot_id, "u1", account, market_type, "master_ensemble", "paper", "active", "2026-01-01", "2026-01-01"))


def add_position(db, bot, symbol, side="LONG", qty=1.0, px=100.0, pid=None):
    with db.connect() as c:
        c.execute("INSERT INTO positions (position_id, bot_instance_id, symbol, side, original_qty, remaining_qty, entry_price, status, opened_at)"
                  " VALUES (?,?,?,?,?,?,?,?,?)", (pid or f"p_{bot}_{symbol}", bot, symbol, side, qty, qty, px, "OPEN", "2026-01-01"))


def add_pending(db, bot, symbol, side="LONG"):
    with db.connect() as c:
        c.execute("INSERT INTO pending_entries (bot_id, symbol, side, client_order_id, state, intended_notional) VALUES (?,?,?,?,?,?)",
                  (bot, symbol, side, f"cid_{bot}_{symbol}_{side}", "PENDING_OPEN", 100.0))


def sel(symbol, side="LONG", cid=None, venue="binance"):
    """A reservation ``Selected`` tuple for a crypto perp symbol."""
    base = symbol.replace("USDT", "")
    return (cid or f"cand_{symbol}", f"{base}/USDT:PERP", venue, symbol, side)
