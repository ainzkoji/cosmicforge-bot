# Admin Backend Cutover Monitoring Checklist

Phase: 3N-B
Window: 14 days after browser verification

## Daily Checks

- Admin Backend 500 count: record total and affected routes.
- Admin Backend 503 count: record any SQLite busy/locked responses.
- Admin Backend 401 count: watch for spikes after auth/session changes.
- Endpoint latency: record p95 and p99 for migrated Admin Backend routes.
- Frontend runtime errors: record browser console errors on admin pages.
- Frontend timeout errors: record route, page, and backend port.
- CORS errors: record origin, target port, and endpoint.
- Snapshot age: verify Profitability and ML snapshots are under 24 hours old.
- Snapshot refresh: record success/failure and elapsed time for each refresh job.
- ML linkage warnings: record unlinked completed trade count and reason counts.
- User Backend login latency: record response time for admin login/refresh/me.
- Port safety: confirm no write/action request is sent to port 8100.

## Alert Thresholds

- Any migrated route above 500 ms sustained.
- Any migrated route above 2 seconds once.
- Any snapshot older than 24 hours.
- Any SQLite busy/locked 503.
- Any unexpected POST, PUT, PATCH, or DELETE request to port 8100.
- Any admin page stuck in a loading state.
- Any CORS error on migrated read routes.
- Any login, refresh, or me endpoint regression on port 8000.

## Snapshot Refresh Commands

```powershell
python backends/bot-backend/scripts/refresh_admin_analytics_snapshots.py --profitability --sizing-events
python backends/bot-backend/scripts/refresh_admin_analytics_snapshots.py --ml
```

## Cleanup Rule

Do not remove duplicated user-backend GET routes or fallback flags during this monitoring window.
If the window is clean, remove duplicate routes in this order:

1. Revenue
2. TradingView
3. Signals
4. Events and News
5. Bot Monitor
6. Dashboard
7. Profitability
8. ML Monitoring

ML action routes, admin auth, and all write/action routes remain on user-backend.

## Rollback Procedure

If a migrated read group causes sustained errors during the monitoring window, roll it back by setting its flag to `false` in `frontends/admin-frontend/.env` and restarting the Vite dev server (or rebuilding for production). No backend changes are required — the duplicate route on user-backend remains live throughout the window.

Rollback order (reverse of cleanup order — most isolated first):

1. ML Monitoring — set `VITE_USE_ADMIN_BACKEND_ML=false`
2. Profitability — set `VITE_USE_ADMIN_BACKEND_PROFITABILITY=false`
3. Dashboard — set `VITE_USE_ADMIN_BACKEND_DASHBOARD=false`
4. Bot Monitor — set `VITE_USE_ADMIN_BACKEND_BOT_MONITOR=false`
5. Events and News — set `VITE_USE_ADMIN_BACKEND_EVENTS=false` and `VITE_USE_ADMIN_BACKEND_NEWS=false`
6. Signals — set `VITE_USE_ADMIN_BACKEND_SIGNALS=false`
7. TradingView — set `VITE_USE_ADMIN_BACKEND_TRADINGVIEW=false`
8. Revenue — set `VITE_USE_ADMIN_BACKEND_REVENUE=false`

After rollback, verify the affected pages load from port 8000 and record the incident in the daily check log before resuming the monitoring window.

Full rollback (all groups simultaneously): set all `VITE_USE_ADMIN_BACKEND_*` flags to `false` in `.env` and restart Vite. Auth, writes, and ML actions are unaffected — they always route through port 8000.
