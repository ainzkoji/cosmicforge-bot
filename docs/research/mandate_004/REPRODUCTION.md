# Mandate 004: independent reproduction package

Deliverable D12 (Section H, Step 2.5). Everything here runs from the repository and public data. No exchange
account, API key or customer credential is needed or read.

## What you need

- Python 3.12 with the pinned packages: `pip install -e backends/shared` and
  `pip install -r backends/bot-backend/requirements.txt -c deploy/constraints.txt`
  (numpy 2.4.1, pandas 3.0.0, pyarrow 23.0.1).
- About 300 MB of disk and network access to `data.binance.vision`.
- The commands below use the Windows interpreter path of this workstation
  (`backends/venv/Scripts/python.exe`); on Linux use the interpreter of your environment.

## 1. Check the register and the pinned inputs

```bash
cd backends/bot-backend && ../venv/Scripts/python.exe -m app.trading_intelligence.research.governance verify
```

```bash
cd backends/bot-backend && ../venv/Scripts/python.exe -m app.trading_intelligence.research.governance status
```

Expected: the chain verifies, the anchor is checked, 18 hypotheses, Mandate 004 registered with two
amendments, and the holdout `RESERVED` or later.

| Pinned input | Value |
|---|---|
| Specification `research/trend_v1/SPEC.md` SHA-256 | `ea02ace43ecc75ea409dd2bb3592640e0d9b77412dc4774ddf58492ccf7e4edb` |
| Rule artifact hash | `7795ad79de962300351fcf4d419cd511256dce356c9ee46cdbbc21842582cbb9` |
| Amendment 1 `INTERPRETATION_001.md` SHA-256 | `cd252fc540a861c5da0958c6c7394d10cecb26bc6d2eb1255343d062512745ec` |
| Amendment 2 `INTERPRETATION_002.md` SHA-256 | `f0dc1853e4f94424c006e62b60224e5e18ef4537101d7c5fd58b018a59e97d42` |
| Strategy hash (specification + rules + amendments) | `eb07acdb40a4d517bb44e6c89e48693d729ec55e8f95715455d488a18cb51437` |
| Dataset `binance_usdm_daily` v1 hash | `0c3392d8daf78e08244fdd21edac5a93ef0c6cfd5386e3cecbf6cb379fde284e` |
| Dataset manifest hash | `38d940f14f98a604ffe7bd3336e51b51e07fdda571b057db19327b4019a6a450` |
| Cost model hash | `1a261984c62ad598297a5c05d2243987b6a5b2d7b813615f088ca8b9cde4b3f9` |

The file hashes are computed with line endings normalised to LF, so they are the same on Windows and Linux.

## 2. Rebuild the dataset

```bash
cd backends/bot-backend && ../venv/Scripts/python.exe -m app.market_data.daily_dataset build --workers 64
```

This lists the archive, downloads 43,626 files (each checked against the archive's own SHA-256), takes the
days the monthly files skip from the archive's daily files, and writes the tables to
`data/research/binance_usdm_daily_v1/`. It is resumable: run it again after an interruption. It keeps the
committed exchange metadata snapshot (`docs/research/datasets/binance_usdm_daily_v1.metadata.json`); do not
pass `--rediscover`, which would fetch today's metadata and give a different dataset.

Expected output: `dataset_hash` and `manifest_hash` equal to the table above, 911 contracts, 663,145 daily
bars, 2,735,181 funding records, 0 failed downloads, 0 invalid bars.

```bash
cd backends/bot-backend && ../venv/Scripts/python.exe -m app.market_data.daily_dataset verify --raw
```

Expected: `normalized_tables: MATCH`, `raw_inventory: MATCH`, no raw file listed as not matching. If Binance
has republished an archive file since 9 October 2026, this step names it and the dataset hash will differ:
that is a finding about the source, not a tolerance to widen.

## 3. Recompute the run

```bash
backends/venv/Scripts/python.exe scripts/reproduce_mandate_004.py --run-id run_0b88d389f8da50dc48de5e51
```

The official command is idempotent and would only return the stored result, so this script recomputes the run
against a scratch copy of the register as it was before the run started. The committed register and
artifacts are not written to.

Expected for the development run `run_0b88d389f8da50dc48de5e51`:

| Figure (Balanced, mandate, base cost, brakes on, 2020-01-01 to 2024-12-31) | Value |
|---|---|
| Result hash | `a48879dde9f4a9c6cdf38ea86c9e7d1ef986f3a99f56202cc83b9a7ed36f64e9` |
| Daily observations | 1,827 |
| Net return | 0.3367803431170495 |
| Gross return (before all costs) | 0.40853752852717395 |
| Annual volatility | 0.060569702385035055 |
| Sharpe ratio | 0.9877405782570482 |
| Maximum drawdown | 0.11542041550868753 |
| Maximum drawdown without the drawdown brakes | 0.1117462805240651 |
| Closed trades | 1,192 |
| New positions selected by the rule | 1,251, of which 1,072 with a stop beyond 15% |
| Fees / slippage / funding (USDT on 10,000) | 168.7731114432509 / 168.77393463847503 / 380.02480801949184 |
| Calendar years 2020 to 2024 | +13.87%, +11.25%, −7.54%, +6.34%, +7.32% |
| Hypotheses in the register | 18 |
| Frozen pass rule | `PENDING_HOLDOUT` (criterion 2 already met; criteria 1, 3, 4 need the held-back period) |
| Statistical gate | `BLOCKED_PENDING_APPROVAL` |
| Verdict | `PENDING_HOLDOUT` |

**Tolerances.** On the same platform and library versions the run id and the result hash are identical. On
another platform compare numbers: every figure of every scenario must agree within a relative tolerance of
1e-9 (`--tolerance`), and counts (observations, trades, decisions, rejections) must be equal. The script
reports `REPRODUCED` or lists what differs.

## 4. Check the run without the evaluator

```bash
backends/venv/Scripts/python.exe scripts/verify_mandate_004_run.py --run-id run_0b88d389f8da50dc48de5e51 --samples 60
```

This script does not import the evaluator, the rule functions or the simulator. With plain loops over the
tables it recomputes a random sample of entry decisions (signal strength, ATR, stop distance, history,
volume rank and universe membership), a random sample of trades (entry and exit fills, price result, fee,
slippage, and funding from the archive's own records), and the whole daily ledger. It reads no row after
the period the run evaluated. Expected: `status: AGREES` with no failures.

## 5. Run the tests

```bash
cd backends/bot-backend && ../venv/Scripts/python.exe -m pytest tests/trading_intelligence -k step2 -q
```

Expected: every test passes. They cover the register, mandate pinning, the statistical gate, the dataset
pipeline, the rules, the simulator (with hand-computed accounts), no-lookahead, the official run, the
holdout protocol and the report.

## What a reproduction does not do

- It does not open the held-back period. The official `holdout` command needs the project owner's recorded
  authorization; `reproduce_mandate_004.py` recomputes a held-back run only after the register shows that
  holdout as evaluated.
- It does not change the register, the mandate, the dataset manifest or the stored artifacts.
- It does not trade and does not contact any account endpoint.
