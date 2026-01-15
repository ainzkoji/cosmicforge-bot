from __future__ import annotations

from typing import Optional

from app.persistence.db import DB, utc_now_iso


def _get_row(
    conn,
    strategy: str,
    strategy_version: str,
    symbol: str,
    asset_class: str,
    broker_id: str,
    account_id: str,
    timeframe: str,
):
    return conn.execute(
        """
        SELECT * FROM strategy_performance
        WHERE strategy=? AND strategy_version=? AND symbol=? AND asset_class=? AND broker_id=? AND account_id=? AND timeframe=?
        LIMIT 1
        """,
        (
            strategy,
            strategy_version,
            symbol,
            asset_class,
            broker_id,
            account_id,
            timeframe,
        ),
    ).fetchone()


def update_strategy_performance(
    db: DB,
    *,
    strategy: str,
    strategy_version: str,
    symbol: str,
    asset_class: str,
    broker_id: str,
    account_id: str,
    timeframe: str,
    net_pnl_delta: float,
    gross_pnl_delta: float,
    fees_delta: float = 0.0,
    slippage_delta: float = 0.0,
    outcome_win: Optional[bool] = None,
    r_multiple: Optional[float] = None,
) -> None:
    with db.connect() as conn:
        row = _get_row(
            conn,
            strategy,
            strategy_version,
            symbol,
            asset_class,
            broker_id,
            account_id,
            timeframe,
        )
        if row is None:
            trades = 1
            wins = 1 if outcome_win else 0
            losses = 0 if outcome_win else 1
            avg_r = float(r_multiple) if r_multiple is not None else 0.0
            conn.execute(
                """
                INSERT INTO strategy_performance (
                    strategy, strategy_version, symbol, asset_class, broker_id, account_id, timeframe,
                    trades, wins, losses, net_pnl, gross_pnl, max_drawdown, avg_r, avg_slippage, fees, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    strategy,
                    strategy_version,
                    symbol,
                    asset_class,
                    broker_id,
                    account_id,
                    timeframe,
                    trades,
                    wins,
                    losses,
                    float(net_pnl_delta),
                    float(gross_pnl_delta),
                    0.0,
                    avg_r,
                    float(slippage_delta),
                    float(fees_delta),
                    utc_now_iso(),
                ),
            )
            return

        trades = int(row["trades"]) + 1
        wins = int(row["wins"]) + (1 if outcome_win else 0)
        losses = int(row["losses"]) + (0 if outcome_win else 1)

        net_pnl = float(row["net_pnl"]) + float(net_pnl_delta)
        gross_pnl = float(row["gross_pnl"]) + float(gross_pnl_delta)
        fees = float(row["fees"]) + float(fees_delta)
        avg_slippage = (
            float(row["avg_slippage"]) * (trades - 1) + float(slippage_delta)
        ) / trades

        if r_multiple is not None:
            prev_avg_r = float(row["avg_r"])
            avg_r = (prev_avg_r * (trades - 1) + float(r_multiple)) / trades
        else:
            avg_r = float(row["avg_r"])

        conn.execute(
            """
            UPDATE strategy_performance
            SET trades=?, wins=?, losses=?, net_pnl=?, gross_pnl=?, fees=?, avg_slippage=?, avg_r=?, updated_at=?
            WHERE id=?
            """,
            (
                trades,
                wins,
                losses,
                net_pnl,
                gross_pnl,
                fees,
                avg_slippage,
                avg_r,
                utc_now_iso(),
                int(row["id"]),
            ),
        )


def record_signal_outcome(
    db: DB,
    *,
    strategy: str,
    strategy_version: str,
    symbol: str,
    asset_class: str,
    broker_id: str,
    account_id: str,
    timeframe: str,
    confidence: float,
    pnl: float,
    outcome_win: bool,
    r_multiple: Optional[float] = None,
    opened_at: Optional[str] = None,
    closed_at: Optional[str] = None,
) -> None:
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO signal_outcomes (
                strategy, strategy_version, symbol, asset_class, broker_id, account_id, timeframe,
                confidence, outcome, r_multiple, pnl, opened_at, closed_at, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                strategy,
                strategy_version,
                symbol,
                asset_class,
                broker_id,
                account_id,
                timeframe,
                float(confidence),
                1 if outcome_win else 0,
                float(r_multiple) if r_multiple is not None else None,
                float(pnl),
                opened_at,
                closed_at,
                utc_now_iso(),
            ),
        )
