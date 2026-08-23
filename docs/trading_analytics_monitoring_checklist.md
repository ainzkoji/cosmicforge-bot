## Trading Analytics Monitoring Checklist (UX-9)

Purpose: keep user-facing trading analytics (Dashboard / Completed Trades / Raw Fills / Reconciliation) trustworthy and reconcilable.

Scope: read-only monitoring. Do not change execution or broker logic as part of routine checks.

### Daily checks (fast)

1) **Reconciliation sanity**
- Call `GET /api/analytics/reconciliation?days=30`
- Confirm:
  - `trade_fills.total_net_pnl` equals `positions_history.total_net_pnl` (should match)
  - `positions_history.synthetic_closed_count` is stable/non-explosive
  - `metadata.warnings` is empty or explainable
  - `difference.likely_explanation` is plausible (often `open_unrealized_pnl` or `deposits_withdrawals`; otherwise investigate)

2) **Equity snapshot freshness**
- Call `GET /api/v1/analytics/equity-latest`
- Confirm latest `accounts[*].timestamp` is recent for active brokers.

3) **Completed Trades completeness**
- Call `GET /api/analytics/positions/history?timeframe=30d&status=closed&page=1&page_size=200`
- Confirm `summary.closed_count > 0` for active users and values look plausible.

### Weekly checks (deeper)

1) **Synthetic fill_<id> rate**
- Monitor `fills_without_position_id` from positions/history metadata (ALL time and 30d).
- If synthetic increases sharply, investigate the fill persistence path producing missing `position_id`.

2) **User scoping safety**
- Spot-check a small sample of users:
  - `positions/history` and `analytics/trades` should only return that user's trades.

3) **CSV export parity**
- Call `GET /api/analytics/positions/history/export?timeframe=ALL`
- Confirm CSV includes synthetic `fill_<id>` records when `fills_without_position_id > 0`.

### Recommended SQL checks (SQLite)

Run against the durable DB (example path in this repo: `backends/shared/shared_lib/persistence/cosmicforge.db`).

#### A) Total fills
```sql
SELECT COUNT(*) AS total_fills FROM trade_fills;
```

#### B) Fills missing user_id (should trend down)
```sql
SELECT COUNT(*) AS fills_missing_user_id
FROM trade_fills
WHERE user_id IS NULL OR user_id = '';
```

#### C) Fills missing position_id (synthetic candidates)
```sql
SELECT COUNT(*) AS fills_missing_position_id
FROM trade_fills
WHERE position_id IS NULL OR position_id = '';
```

#### D) CLOSE fills missing position_id (unlinked closes)
```sql
SELECT COUNT(*) AS close_missing_position_id
FROM trade_fills
WHERE action = 'CLOSE'
  AND (position_id IS NULL OR position_id = '');
```

#### E) Raw CLOSE PnL vs grouped-by-position PnL (must reconcile)
This uses the same synthetic grouping rule used in user analytics:
```sql
WITH scoped AS (
  SELECT
    CASE
      WHEN position_id IS NULL OR position_id = '' THEN ('fill_' || CAST(id AS TEXT))
      ELSE position_id
    END AS pos_id,
    action,
    COALESCE(realized_pnl, 0) AS realized_pnl,
    COALESCE(fee, 0) AS fee
  FROM trade_fills
  -- Add a WHERE clause here to scope to a user (recommended):
  -- WHERE user_id = '...'
),
raw_close AS (
  SELECT
    SUM(CASE WHEN action='CLOSE' THEN realized_pnl ELSE 0 END) AS gross_realized,
    SUM(CASE WHEN action='CLOSE' THEN fee ELSE 0 END) AS fees
  FROM scoped
),
grouped AS (
  SELECT
    pos_id,
    SUM(CASE WHEN action='CLOSE' THEN realized_pnl ELSE 0 END) AS realized_sum,
    SUM(fee) AS fees_sum,
    SUM(CASE WHEN action='CLOSE' THEN 1 ELSE 0 END) AS close_count
  FROM scoped
  GROUP BY pos_id
),
grouped_closed AS (
  SELECT
    SUM(realized_sum) AS gross_realized,
    SUM(fees_sum) AS fees
  FROM grouped
  WHERE close_count > 0
)
SELECT
  raw_close.gross_realized AS raw_gross_realized,
  raw_close.fees AS raw_fees,
  (raw_close.gross_realized - raw_close.fees) AS raw_net,
  grouped_closed.gross_realized AS grouped_gross_realized,
  grouped_closed.fees AS grouped_fees,
  (grouped_closed.gross_realized - grouped_closed.fees) AS grouped_net
FROM raw_close, grouped_closed;
```

### Triage guidance

- **Equity up, completed net down**: commonly open positions are in profit, deposits occurred, or demo balance changed. Use reconciliation `likely_explanation` and equity snapshots to explain.
- **Synthetic count rising**: investigate why some CLOSE fills are missing `position_id` (persistence path / data lineage).
- **Reconciliation warnings non-empty**: treat as a user-facing integrity issue; do not hide. Fix read-model or persistence.

