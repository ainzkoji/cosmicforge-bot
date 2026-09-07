from __future__ import annotations


class MLAdminQueries:
    """Centralized raw SQL used by the admin ML module."""

    OVERVIEW_SQL = """
        WITH latest_trace AS (
            SELECT
                ml_model_version,
                ml_threshold,
                ts
            FROM decision_traces
            WHERE ml_model_version IS NOT NULL OR ml_threshold IS NOT NULL
            ORDER BY ts DESC
            LIMIT 1
        ),
        latest_run AS (
            SELECT started_at
            FROM runs
            ORDER BY started_at DESC
            LIMIT 1
        )
        SELECT
            (SELECT ml_model_version FROM latest_trace) AS current_model_version,
            (SELECT ml_threshold FROM latest_trace) AS current_threshold,
            (SELECT ts FROM latest_trace) AS latest_trace_ts,
            (SELECT started_at FROM latest_run) AS last_bot_restart_time
    """

    TOTAL_OPENS_SQL = """
        SELECT COUNT(*) AS total_opens
        FROM trade_fills
        WHERE action = 'OPEN'
          AND COALESCE(account_id, '') != 'backfill'
    """

    LINKED_COMPLETED_ROWS_SQL = """
        WITH open_fills AS (
            SELECT
                tf.id AS open_fill_id,
                tf.run_id,
                tf.cycle_id,
                tf.symbol,
                tf.side,
                tf.qty AS open_qty,
                tf.price AS open_price,
                tf.timestamp_utc AS open_timestamp_utc,
                tf.position_id,
                tf.bot_instance_id,
                tf.stop_loss_price,
                tf.trace_id,
                tf.account_id
            FROM trade_fills tf
            WHERE tf.action = 'OPEN'
              AND COALESCE(tf.account_id, '') != 'backfill'
        ),
        close_agg AS (
            SELECT
                tf.position_id,
                tf.symbol,
                tf.side,
                SUM(tf.qty) AS close_qty,
                SUM(CASE WHEN tf.realized_pnl IS NOT NULL THEN tf.realized_pnl ELSE 0 END) AS realized_pnl_sum,
                SUM(CASE WHEN tf.realized_pnl IS NOT NULL THEN 1 ELSE 0 END) AS realized_pnl_count,
                SUM(tf.qty * tf.price) / NULLIF(SUM(tf.qty), 0) AS avg_close_price,
                MAX(tf.timestamp_utc) AS close_timestamp_utc
            FROM trade_fills tf
            WHERE tf.action = 'CLOSE'
              AND tf.position_id IS NOT NULL
              AND COALESCE(tf.account_id, '') != 'backfill'
            GROUP BY tf.position_id, tf.symbol, tf.side
        ),
        trace_candidates AS (
            SELECT
                of.open_fill_id,
                dt.trace_id,
                dt.ts AS trace_ts,
                dt.timeframe,
                dt.regime_state,
                dt.regime_confidence,
                dt.signal,
                dt.confidence,
                dt.chosen_strategy,
                dt.ml_score,
                dt.ml_action,
                dt.adx,
                dt.atr_pct,
                dt.ma_slope,
                dt.compression_ratio,
                dt.breakout_pressure,
                dt.buy_score,
                dt.sell_score,
                dt.threshold,
                dt.sl_plan,
                dt.tp_plan,
                dt.active_strategy_count,
                dt.htf_opposed,
                dt.drawdown_pct,
                dt.portfolio_risk_used,
                dt.open_positions_count,
                dt.margin_level,
                dt.last_price,
                dt.mark_price,
                dt.ml_model_version,
                dt.ml_threshold,
                ROW_NUMBER() OVER (
                    PARTITION BY of.open_fill_id
                    ORDER BY
                        CASE
                            WHEN of.trace_id IS NOT NULL AND dt.trace_id = of.trace_id THEN 0
                            WHEN of.position_id IS NOT NULL AND dt.position_id = of.position_id THEN 1
                            ELSE 2
                        END,
                        dt.ts DESC,
                        dt.trace_id DESC
                ) AS trace_rank
            FROM open_fills of
            LEFT JOIN decision_traces dt
                ON (
                    of.trace_id IS NOT NULL
                    AND dt.trace_id = of.trace_id
                ) OR (
                    of.position_id IS NOT NULL
                    AND dt.position_id = of.position_id
                ) OR (
                    dt.run_id = of.run_id
                    AND dt.cycle_id = of.cycle_id
                    AND dt.symbol = of.symbol
                )
        )
        SELECT
            of.open_fill_id,
            of.run_id,
            of.cycle_id,
            of.symbol,
            of.side,
            of.open_qty,
            of.open_price,
            of.open_timestamp_utc,
            of.position_id,
            of.bot_instance_id,
            of.stop_loss_price,
            of.account_id,
            ca.close_qty,
            ca.realized_pnl_sum,
            ca.realized_pnl_count,
            ca.avg_close_price,
            ca.close_timestamp_utc,
            tc.trace_id,
            tc.trace_ts,
            tc.timeframe,
            tc.regime_state,
            tc.regime_confidence,
            tc.signal,
            tc.confidence,
            tc.chosen_strategy,
            tc.ml_score,
            tc.ml_action,
            tc.adx,
            tc.atr_pct,
            tc.ma_slope,
            tc.compression_ratio,
            tc.breakout_pressure,
            tc.buy_score,
            tc.sell_score,
            tc.threshold,
            tc.sl_plan,
            tc.tp_plan,
            tc.active_strategy_count,
            tc.htf_opposed,
            tc.drawdown_pct,
            tc.portfolio_risk_used,
            tc.open_positions_count,
            tc.margin_level,
            tc.last_price,
            tc.mark_price,
            tc.ml_model_version,
            tc.ml_threshold
        FROM open_fills of
        JOIN close_agg ca
            ON ca.position_id = of.position_id
           AND ca.symbol = of.symbol
           AND ca.side = of.side
        LEFT JOIN trace_candidates tc
            ON tc.open_fill_id = of.open_fill_id
           AND tc.trace_rank = 1
        WHERE of.position_id IS NOT NULL
          AND ca.close_qty >= of.open_qty * 0.99
    """

    POST_FIX_START_SQL = """
        SELECT MIN(timestamp_utc) AS post_fix_start
        FROM trade_fills
        WHERE COALESCE(account_id, '') != 'backfill'
    """

    ACTIVITY_RECENT_ROWS_SQL = """
        WITH open_links AS (
            SELECT
                run_id,
                cycle_id,
                symbol,
                MIN(position_id) AS position_id
            FROM trade_fills
            WHERE action = 'OPEN'
            GROUP BY run_id, cycle_id, symbol
        ),
        close_links AS (
            SELECT position_id
            FROM trade_fills
            WHERE action = 'CLOSE'
              AND position_id IS NOT NULL
            GROUP BY position_id
        )
        SELECT
            dt.ts AS timestamp,
            dt.symbol,
            CASE
                WHEN UPPER(COALESCE(dt.signal, '')) IN ('BUY', 'LONG') THEN 'LONG'
                WHEN UPPER(COALESCE(dt.signal, '')) IN ('SELL', 'SHORT') THEN 'SHORT'
                ELSE NULL
            END AS side,
            dt.ml_score,
            dt.ml_action,
            dt.ml_model_version,
            COALESCE(dt.ml_threshold, dt.threshold) AS threshold,
            dt.regime_state AS regime,
            CASE
                WHEN CAST(strftime('%H', dt.ts) AS INTEGER) BETWEEN 0 AND 7 THEN 'asia'
                WHEN CAST(strftime('%H', dt.ts) AS INTEGER) BETWEEN 8 AND 15 THEN 'london'
                ELSE 'ny'
            END AS session,
            CASE
                WHEN ol.position_id IS NOT NULL AND cl.position_id IS NOT NULL THEN 'fully_linked_completed'
                WHEN ol.position_id IS NOT NULL THEN 'open_fill_linked'
                ELSE 'unlinked'
            END AS linkage_status
        FROM decision_traces dt
        LEFT JOIN open_links ol
            ON ol.run_id = dt.run_id
           AND ol.cycle_id = dt.cycle_id
           AND ol.symbol = dt.symbol
        LEFT JOIN close_links cl
            ON cl.position_id = ol.position_id
        WHERE dt.ml_action IS NOT NULL
          AND dt.ts >= datetime('now', :window_start)
        ORDER BY dt.ts DESC
        LIMIT :limit OFFSET :offset
    """
