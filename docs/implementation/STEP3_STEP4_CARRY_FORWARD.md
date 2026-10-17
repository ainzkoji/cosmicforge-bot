# Section H carry-forward register

Updated 16 October 2026. Code: `2bc06fb`, `cda273a`, `49b81ae`,
`6e4d725`, `6372b83`, `20527f3`, `61d3199`. This register grants no trading authority. Steps 5–8 continue it in
[STEP5_STEP8_EXTERNAL_GATES.md](STEP5_STEP8_EXTERNAL_GATES.md) (17 October 2026); every gate below still applies.
Local component evidence is in [the A–N report](STEP3_STEP4_VALIDATION.md).
The final pushed commit must have green Linux CI before engineering closure.

| ID | Status | Severity / trigger | Dependency / owner | Required action and acceptance evidence |
|---|---|---|---|---|
| H-CI | Required release gate | HIGH / before release | Engineering / GitHub Actions | Verify all jobs for the exact final pushed SHA. Fix new failures; do not add quarantines. |
| H-RESEARCH | BLOCKED | CRITICAL / before any strategy order | Research governance | No real family is certified. DAILY_TREND/H-018 remains development-valid with holdout unopened; H-015 remains failed. Follow the governed research process. Never promote from replay evidence. |
| H-PROMOTION | BLOCKED | CRITICAL / before account execution | Authenticated governance owner | Real families remain M0. Research certification, family stage, account consent, venue certification and global authority must independently permit an entry. |
| H-VENUE | BLOCKED | CRITICAL / before venue selection | Account owner / named operator | Provide explicit account, DEMO environment, symbol, notional bound, operator and authorization reference. Run the concrete certification harness on the owning runtime, retain its complete run/action/check chain, verify final flat state. No such run occurred in this work. LIVE certification is refused by this build. |
| H-BINGX | BLOCKED | CRITICAL / before BingX execution | Owner / venue verification | Public LIVE/VST metadata is recorded, with different coverage. VST private-stream routing remains unverified and refused. Capture authenticated protocol evidence and complete genuine VST certification before certified execution is selectable. |
| H-SHADOW-7D | DEFERRED — INFRASTRUCTURE | HIGH / continuous host available | Continuous-host owner | Collect seven actual elapsed days with gaps, ownership, daily comparisons, account closes, exceptions and restarts visible. Historical replay cannot satisfy this. |
| H-SHADOW-28D | DEFERRED — INFRASTRUCTURE | HIGH / after initial shadow acceptance | Continuous-host owner | Complete the four-week operational acceptance after the initial period; retain uninterrupted measurement and incident evidence. |
| H-THROUGHPUT | BLOCKED for production-scale certification | HIGH / before production-scale deployment | Venue / infrastructure owner | HTTP and connection budgets are shared and enforced. Conservative envelopes and Binance's 100-unit request reservation are local policies, not measured endpoint throughput. Verify account/endpoint limits, warm-up times, egress topology and Linux capacity before changing configuration. |
| H-CLOSE-EVIDENCE | Operational prerequisite | CRITICAL / at every executable decision close | Owning runtime | Maintain fresh broker equity samples at consecutive UTC closes and complete transfer history. Missing evidence must produce a refusal; retain existing high-water marks. |

Safety limits remain 0.40% per trade and 15% maximum entry stop. The four-specialist
invariant, frozen research artifacts and failure history remain unchanged. No
deployment, production migration, service restart, strategy order or real venue
certification was performed during this implementation audit.
