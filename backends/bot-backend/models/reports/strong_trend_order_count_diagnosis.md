# STRONG_TREND Order Count Diagnosis

- generated_at_utc: 2026-07-15T21:18:19.851245+00:00
- clean_start_time: 2026-06-15T04:46:39.294507Z
- experiment_start_time: 2026-06-15T17:21:40.112591Z
- active_bot_instance_id: None
- bot_instance_filter_applied: false

## Summary

- total_reported_paper_orders: 68
- valid_post_experiment_strong_trend_orders: 0
- failed_attempts: 68
- historical_paper_only_skipped_attempts: 68
- old_orders_before_experiment: 0
- old_orders_before_clean_start: 347
- wrong_regime_orders: 3
- wrong_symbol_orders: 0
- wrong_bot_instance_orders: 0
- duplicate_orders: 0
- test_fixture_rows: 0
- unknown_rows: 0

## Aggregates

- by_symbol: `{'BTCUSDT': 35, 'ETHUSDT': 33}`
- by_status: `{'PAPER_ONLY': 68}`
- by_error_message: `{'Paper mode (paper) - no execution': 68}`
- by_bot_instance_id: `{'bot_e5fe913972a9': 68}`
- by_regime: `{'STRONG_TREND': 68}`
- duplicate_order_ids: `{}`

## First 20 Reported Rows

| attempt_id | created_at | symbol | side | status | bot_instance_id | real_order | failed_attempt | error |
|---|---|---|---|---|---|---:|---:|---|
| e05c20f2-2816-4c46-97e3-c05f4f8b5721 | 2026-06-16T12:15:53.781547+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| 600b54a4-a46e-4ca3-bb7e-df80df66c182 | 2026-06-16T12:16:11.437760+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| 8605d35b-b60a-424f-87fe-fa100ba1988b | 2026-06-16T12:16:28.928450+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| 3bbffa2a-4b8e-4aed-820c-3146c81432fc | 2026-06-16T12:16:46.173826+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| faf748cd-45f9-44b7-b4f3-2d3d02d1e82b | 2026-06-16T12:17:05.197513+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| 2585a639-110d-40c8-a5b0-7574f00d5445 | 2026-06-16T12:17:23.707836+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| dfdabc3a-ab1c-4ea9-963a-615528334828 | 2026-06-16T12:17:40.492596+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| 46bdac34-9d5b-4358-b97e-8f1c2ab6353a | 2026-06-16T12:30:56.762389+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| 810dde38-b195-4ded-83ad-7f327054dac0 | 2026-06-16T12:32:04.283870+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| da0bf4ac-73b6-4daa-8606-7125008be1aa | 2026-06-16T12:33:09.933225+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| 74bf96ae-53fc-46f9-a0af-264d98055fc1 | 2026-06-16T12:33:27.941143+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| 8de10f79-813f-48ec-aaed-2e8bd3e2d9a3 | 2026-06-16T12:33:45.944438+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| 271e8978-b284-4c29-9ba9-9fe61275c2a9 | 2026-06-16T12:34:03.769859+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| 74a4c894-2a8f-4448-92f2-d49a27d4a9c9 | 2026-06-16T12:34:21.137280+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| d2f4d56a-4182-4a27-b260-56f48d08c1a8 | 2026-06-16T12:34:38.628689+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| 9454dc48-37c0-418a-ad32-e28be68d4926 | 2026-06-16T12:34:55.527255+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| 17851e33-a6c5-45a8-9cf9-b576555bebc7 | 2026-06-16T12:35:12.596739+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| effa5d08-acf3-4eaa-babc-ed311b7cfece | 2026-06-16T12:36:53.854835+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| 004cbebe-f5e5-48f5-8069-9cc40f966411 | 2026-06-16T12:37:13.330302+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |
| 031c0cf1-bf21-4e77-81f1-0d79c1d173f5 | 2026-06-16T12:37:30.758863+00:00 | ETHUSDT | BUY | PAPER_ONLY | bot_e5fe913972a9 | false | true | Paper mode (paper) - no execution |

The monitor now treats submit attempts, created paper orders, and failed attempts as separate metrics.
