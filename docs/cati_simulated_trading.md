# CATI live-market simulated trading

CATI's canonical runtime has a separately enrolled local simulation book for
`RESIDUAL_MOMENTUM_PORTFOLIO_TOP1`. The user authorized active simulated/test
execution with live market data, real money disabled and real broker live orders
disabled. This does not grant a broker promotion, consume another research run,
train a model, access holdout, or change the immutable Mandate 003 registry.

The frozen prospective collector still computes the exact original universe,
score, hourly boundary, selection, geometry and costs. Its existing forward
observations continue independently. Activation enrolls only the next future
hourly boundary; old selected observations are never turned into trades. The
simulation considers each new top1 candidate once, independently of the older
observer portfolio's occupancy, with its own single-position portfolio rule.

Entry orders and model fills are created at actual acknowledgment time, before
the first native 15m outcome bar closes. The price remains the frozen next
native open **model reference**, with distinct creation/reference/receipt times.
It is not a claim that a delayed broker order could fill at that earlier price.
Late entry windows are rejected. No future H/L/C is used to open a position.
Partial bars cannot settle a trade. Frozen stop-first ambiguous native bars,
gap-aware stops, fixed target and 192-bar timeout settle incrementally from a
durable cursor. Entry and OCO stop/target/timeout orders, fills, positions and
cost breakdowns are written atomically and restored without duplicate fills.

The public market client has only allowlisted unauthenticated GET routes and
no order method, credentials or execution adapter. No order, protection, cancel,
leverage request or even a test order is sent to an exchange account. IDs use
`cati_sim_`; broker order/fill IDs are null. The original broker governance
remains blocked. Internal global `EXECUTION_MODE=paper` remains appropriate;
CATI reports `LIVE_MARKET / ACTIVE / SIMULATED/TEST` separately from broker mode.

The default wallet contains **10,000 virtual USDT**, at 1x simulated leverage.
Admission reserves at most 0.25% equity including conservative full frozen
costs, below the system's 0.4% per-trade ceiling. All existing SystemLimits
apply: stop distance and ATR bounds, symbol/total/correlated exposure, margin
buffer, position/count ceilings, consecutive-loss, weekly and emergency
drawdown limits. A conflicting top1 geometry is rejected unchanged; there is
no lower-ranked substitute. The governance global new-entry kill also applies.

The permanent daily hard-loss cap is `min(2.5%, system limit)`. It includes
realized losses, open live-mark losses, prepaid full funding buffer and estimated
exit costs. A breach creates a local risk-reduction order/fill, cancels protection
orders, and latches entries off for that UTC day. Weekly and emergency/consecutive
halts persist. Discrete live price gaps can exceed a cap before the next quote;
the system immediately halts and records the actual loss instead of clipping PnL.
The strategy's stop/target evaluation remains native closed-bar based; live
equity emergency reductions operate independently. Stale/missing prices block
new entries, and incomplete exit paths preserve the cursor and report errors.

Canonical SQLite tables: `cati_sim_account`, `cati_sim_decisions`,
`cati_sim_orders`, `cati_sim_fills`, `cati_sim_positions`. A five-second worker
requires the canonical scheduler lease, handles public rate-limit backoff, and
resumes the persisted book after supervised runtime restart. All times are UTC
milliseconds. The raw observer stream retains its existing OBSERVE provenance.

Owner-scoped authenticated API: `GET /api/v1/cati/simulation/status` with existing
`bot:read` permission. There is no API that enables real-money execution or
rewrites the strategy. The dashboard displays live equity, realized/unrealized
PnL, daily loss/cap, all positions and recent orders/fills. Its production build
is also served by the canonical runtime at `/cati`; it uses the same access-token
storage and owner-scoped API as the normal dashboard. `/health` exposes only
runtime flags/heartbeat, not account or ledger data.

Operator commands, run from the repository's main checkout:

```powershell
$env:VITE_API_BASE='http://localhost:9000'
Set-Location frontends/user-frontend
npm run build
Set-Location ../..
backends/venv/Scripts/python.exe scripts/cati_simulation.py activate
scripts/trading_runtime.ps1 restart -Mode supervised -TimeoutSeconds 60
backends/venv/Scripts/python.exe scripts/cati_simulation.py status
```

Activation is idempotent and never resets equity, owner, enrollment or an open
book. The CLI validates the existing active account owner and uses the explicit
canonical database path. FX acquisition is already complete; a singleton
finalization watcher stays running, reports strict `NOT_FROZEN` gap failures,
and retries the existing finalizer only when acquisition evidence changes. It
never lowers completeness criteria or duplicates an acquisition writer.

Synthetic tests cover long/short model fills, target, ambiguous/gapped stop,
full timeout, cost/cash reconciliation, restarts, portfolio overlap, expired
entries, geometry rejection, live unrealized loss/daily halt, kill switch,
ownership, public-only transport and API authentication/tenant isolation.
