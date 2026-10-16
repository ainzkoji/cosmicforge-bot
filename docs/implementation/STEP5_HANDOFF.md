# Step 5 handoff: scale and operational validation

Prepared 16 October 2026. This is an engineering handoff, not deployment or trading
approval. Start from the final pushed revision after verifying its complete CI run.
The [requirement register](STEP3_STEP4_IMPLEMENTATION_STATUS.md),
[A–N validation](STEP3_STEP4_VALIDATION.md) and
[carry-forward register](STEP3_STEP4_CARRY_FORWARD.md) distinguish component tests
from operational and venue certification.

## Runtime entry points

- `production_runtime.run`: owns the canonical broker cycle and supervised shadow
  and venue services. It joins in-flight work on cancellation.
- `ShadowScheduler`: independent daily SHADOW_ONLY evaluation, model accounts,
  connected shadow books, durable cycle fencing, daily evaluator audit and bounded
  archival retention. It has no executable callback.
- `ConnectedPortfolioRuntime`: the canonical broker cycle's authorized-family
  handoff. It reads broker holdings, checks existing operating risk, uses measured
  close brakes, records its own immutable executable decision and calls the single
  portfolio boundary. A shadow record cannot be relabelled as executable.
- `VenueRuntime` / `AccountSession`: rules refresh, private-stream lifecycle and
  reconciliation. Instrument rules, HTTP budgets and connection budgets are
  persisted; credential rotation and ownership loss close sessions.
- `CertificationHarness`: explicit, scoped DEMO infrastructure testing. Invoke
  it through a supervised worker of the owning runtime, never on the asyncio event
  loop. It takes the broker-cycle lock, retains normal order flags and requires
  a current owner permit. No runtime service starts it automatically.

## Reusable evidence and harnesses

| Artifact | What it establishes |
|---|---|
| `evidence/step3/development_shadow_reconciliation.json` | 1,827 development days; six model books and committed reference agree; zero holdout rows and orders. |
| `evidence/step3/cycle_performance.json` | Synthetic Windows driver measurements at 1/10/100/1,000 accounts, including resources and recovery. |
| `evidence/step3/coordination_comparison.json` | Sequential versus 1/2/4/8 workers on the same synthetic accounts, with/without durable leases. |
| `evidence/step3/database_growth.json` | Per-table growth and retention classification. Allocated SQLite pages need not grow for each insert. |
| `evidence/step4/bingx_public_metadata.json` | Timestamped unauthenticated metadata status, counts and response hashes from LIVE and VST. |
| `pipeline.performance` | Reproducible scale harness. Use temporary databases, the same fixtures and host labels; do not interpret it as venue throughput. |

## Next validation sequence

1. Verify exact-final-commit Linux CI and preserve the run URL and test summary.
2. On an explicitly authorized continuous host, deploy only the intended shadow
   and observation services. Confirm a single runtime owner, database backups,
   broker observation freshness and bounded retention before collecting elapsed
   evidence. This implementation work did not deploy or restart that host.
3. Measure actual one-week and four-week operation. Record missed closes and
   outages, compare model books daily, exercise restart/takeover and emergency
   preemption, and measure Linux CPU, memory, queue depth, database growth and
   feed load using the same account scales. Keep real elapsed-time evidence.
4. Verify venue-specific HTTP, UID and handshake constraints and account-specific
   capabilities before throughput tuning. Default egress scope is deliberately
   shared; configure the actual topology without splitting one public IP into
   separate customer allowances.
5. Only with the explicitly scoped owner permit, complete genuine DEMO venue
   certification. Retain unknown-outcome resolution and final cleanup evidence.
   BingX VST private-stream support remains unverified.
6. Any future strategy execution additionally needs legitimate research
   certification, promotion and account consent. No change to risk ceilings,
   frozen rules, failed-family history or the unopened holdout is authorized here.

Full Step 3/4 operational closure and production-scale certification remain subject
to the external gates in the carry-forward register.
