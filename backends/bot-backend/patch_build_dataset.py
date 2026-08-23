import re

with open("scripts/ml/build_dataset.py", "r", encoding="utf-8") as f:
    code = f.read()

# 1. Rename _load_completed_trades to _load_traces
code = code.replace(
    "def _load_completed_trades(\n    conn: sqlite3.Connection,\n    exclude_before: Optional[str],\n    exclude_accounts: Optional[list[str]],\n) -> pd.DataFrame:",
    "def _load_traces(\n    conn: sqlite3.Connection,\n    exclude_before: Optional[str],\n    exclude_accounts: Optional[list[str]],\n    executed_only: bool = True,\n) -> pd.DataFrame:"
)

code = code.replace(
    "tf_open.timestamp_utc >= ?",
    "ut.ts >= ?"
)
code = code.replace(
    "tf_open.account_id",
    "ut.dt_account_id"
)

# Replace the inner join and where clause logic
old_sql_1 = """        FROM decision_traces
        WHERE gate_allowed = 1
    )
    SELECT"""

new_sql_1 = """        FROM decision_traces
        { "WHERE gate_allowed = 1" if executed_only else "" }
    )
    SELECT"""
code = code.replace(old_sql_1, new_sql_1)

old_sql_2 = """    FROM completed t
    INNER JOIN unique_traces ut
        ON  ut.run_id   = t.open_run_id
        AND ut.cycle_id = t.open_cycle_id
        AND ut.symbol   = t.symbol
        AND ut.rn = 1
    {where_clause}
    ORDER BY t.open_timestamp ASC"""

new_sql_2 = """    FROM unique_traces ut
    { "INNER JOIN" if executed_only else "LEFT JOIN" } completed t
        ON  ut.run_id   = t.open_run_id
        AND ut.cycle_id = t.open_cycle_id
        AND ut.symbol   = t.symbol
    {where_clause + (" AND " if where_clause else "WHERE ") + "ut.rn = 1"}
    ORDER BY ut.ts ASC"""
code = code.replace(old_sql_2, new_sql_2)

# Fix feature "side"
code = code.replace(
    "out[\"side\"]            = (raw[\"side\"] == \"LONG\").astype(\"int8\")   # 1=LONG, 0=SHORT",
    "out[\"side\"]            = (raw[\"side\"].fillna(raw.get(\"signal\", \"\")) == \"BUY\").astype(\"int8\")   # 1=LONG, 0=SHORT"
)

# In sql, we need to extract signal too!
old_sql_3 = """            chosen_strategy,
            -- Use row_number"""

new_sql_3 = """            chosen_strategy,
            gate_allowed,
            signal,
            -- Use row_number"""
code = code.replace(old_sql_3, new_sql_3)

old_sql_4 = """        ut.mark_price,
        ut.chosen_strategy,"""

new_sql_4 = """        ut.mark_price,
        ut.chosen_strategy,
        ut.gate_allowed,
        ut.signal,"""
code = code.replace(old_sql_4, new_sql_4)

# Update build labels for "would this trade have been profitable"
old_labels = """    realized_pnl = pd.to_numeric(raw["realized_pnl"], errors="coerce")
    out["label_win"]            = (realized_pnl > 0).astype("int8")"""

new_labels = """    realized_pnl = pd.to_numeric(raw["realized_pnl"], errors="coerce")
    gate_allowed = pd.to_numeric(raw.get("gate_allowed", 1), errors="coerce").fillna(1)
    
    # Executed trades (gate=1): win if pnl > 0
    # Blocked trades (gate=0): 0 (filtered bad signal)
    has_pnl_win = realized_pnl > 0
    out["label_win"]            = (has_pnl_win & (gate_allowed == 1)).astype("int8")"""
code = code.replace(old_labels, new_labels)

# Update main
main_old = """        # ── Load completed trades ─────────────────────────────────────────────
        print("\\nLoading completed trades (decision_traces ⋈ trade_fills) ...")
        raw = _load_completed_trades(conn, args.exclude_before, exclude_accounts)

        if raw.empty:
            print("❌ No completed trades found in database.", file=sys.stderr)
            sys.exit(1)

        n_trades = len(raw)
        print(f"  Found {n_trades:,} completed trades")

        if n_trades < args.min_trades:
            print(
                f"\\n❌ Insufficient data: {n_trades:,} trades found, "
                f"minimum required is {args.min_trades:,}.\\n"
                f"   Accumulate more trades before building the ML dataset.",
                file=sys.stderr,
            )
            sys.exit(1)

        # ── Build feature / label / metadata frames ───────────────────────────
        print("\\nBuilding feature columns ...")
        features = _build_features(raw)

        print("Building label columns ...")
        labels   = _build_labels(raw)

        print("Building metadata columns ...")
        metadata = _build_metadata(raw, build_ts)

        # Verify column sets are exactly as specified
        assert list(features.columns) == FEATURE_COLUMNS, \\
            f"Feature column mismatch. Expected {len(FEATURE_COLUMNS)}, got {len(features.columns)}"
        assert list(labels.columns) == LABEL_COLUMNS, \\
            f"Label column mismatch. Expected {len(LABEL_COLUMNS)}, got {len(labels.columns)}"

        # ── Leakage check ─────────────────────────────────────────────────────
        sep = "=" * 62
        print(f"\\n{sep}")
        print("  LEAKAGE CHECK")
        print(sep)
        leakage_passed = _check_leakage(features, labels, raw)
        print(f"\\n  → LEAKAGE CHECK {'PASSED ✅' if leakage_passed else 'FAILED ❌'}")
        if not leakage_passed:
            print(
                "  Review feature columns before proceeding with model training.",
                file=sys.stderr,
            )

        # ── Assemble final dataset ────────────────────────────────────────────
        final_df = pd.concat([metadata, features, labels], axis=1)

        # ── Quality report ────────────────────────────────────────────────────
        qc = _quality_report(final_df, features, labels, raw, args.verbose)

        # ── Write Parquet — main training dataset ─────────────────────────────
        print(f"\\nWriting training dataset → {parquet_path}")
        final_df.to_parquet(str(parquet_path), index=False, engine=_PARQUET_ENGINE)
        size_kb = parquet_path.stat().st_size / 1024
        print(f"  ✅ {len(final_df):,} rows, {len(final_df.columns)} columns ({size_kb:.1f} KB)")"""

main_new = """        # ── Load ENTRY dataset ────────────────────────────────────────────────
        print("\\nLoading completed trades for ENTRY dataset ...")
        raw_entry = _load_traces(conn, args.exclude_before, exclude_accounts, executed_only=True)

        if len(raw_entry) < args.min_trades:
            print(f"❌ Insufficient data: {len(raw_entry)} < {args.min_trades}", file=sys.stderr)
            sys.exit(1)

        feat_entry = _build_features(raw_entry)
        lab_entry  = _build_labels(raw_entry)
        meta_entry = _build_metadata(raw_entry, build_ts)

        leakage_passed = _check_leakage(feat_entry, lab_entry, raw_entry)
        final_entry = pd.concat([meta_entry, feat_entry, lab_entry], axis=1)

        qc = _quality_report(final_entry, feat_entry, lab_entry, raw_entry, args.verbose)

        print(f"\\nWriting ENTRY dataset → {parquet_path}")
        final_entry.to_parquet(str(parquet_path), index=False, engine=_PARQUET_ENGINE)
        
        # ── Load GATE dataset ──────────────────────────────────────────────────
        print("\\nLoading all signals for GATE dataset ...")
        raw_gate = _load_traces(conn, args.exclude_before, exclude_accounts, executed_only=False)
        feat_gate = _build_features(raw_gate)
        lab_gate  = _build_labels(raw_gate)
        meta_gate = _build_metadata(raw_gate, build_ts)
        final_gate = pd.concat([meta_gate, feat_gate, lab_gate], axis=1)
        
        print(f"Writing GATE dataset → {gate_path}")
        final_gate.to_parquet(str(gate_path), index=False, engine=_PARQUET_ENGINE)"""

code = code.replace(main_old, main_new)

# Remove the old _load_gate_trace_summary call inside main since we generated full gate dataset
old_gate_write = """        # ── Write gate traces (separate file — all signals inc. blocked) ───────
        try:
            print(f"\\nLoading all decision traces (gate analysis) ...")
            gate_df = _load_gate_trace_summary(conn, args.exclude_before, exclude_accounts)
            if not gate_df.empty:
                gate_df.to_parquet(str(gate_path), index=False, engine=_PARQUET_ENGINE)
                n_blocked = int((gate_df["gate_allowed"] == 0).sum()) if "gate_allowed" in gate_df.columns else 0
                print(f"  ✅ {len(gate_df):,} traces written ({n_blocked:,} gate-blocked)")
        except Exception as e:
            print(f"  ⚠️  Gate traces skipped: {e}")"""
code = code.replace(old_gate_write, "")

# fix raw vs raw_entry in metadata writing
code = code.replace(
    'str(raw["open_timestamp"].min())  if not raw.empty else None',
    'str(raw_entry["open_timestamp"].min())  if not raw_entry.empty else None'
)
code = code.replace(
    'str(raw["close_timestamp"].max()) if not raw.empty else None',
    'str(raw_entry["close_timestamp"].max()) if not raw_entry.empty else None'
)
code = code.replace(
    'len(final_df)',
    'len(final_entry)'
)

with open("scripts/ml/build_dataset.py", "w", encoding="utf-8") as f:
    f.write(code)

print("Patch applied successfully.")
