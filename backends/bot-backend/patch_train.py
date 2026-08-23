import re
import sys

with open("scripts/ml/train_entry_model.py", "r", encoding="utf-8") as f:
    code = f.read()

# 1. Update config globals
code = code.replace(
    "_MIN_TRAIN   = 300    # minimum training set size before first fold\n_VAL_WINDOW  = 100    # validation window size (trades)\n_STEP        = 50     # folds advance this many trades each step",
    "_MIN_TRAIN   = 300    # minimum required dataset size\n_TRAIN_FRAC  = 0.70\n_VAL_FRAC    = 0.15\n_TEST_FRAC   = 0.15"
)

# 2. Add _make_splits and replace _make_folds
old_make_folds = """def _make_folds(n: int) -> list[tuple[int, int, int, int]]:
    \"\"\"
    Expanding-window walk-forward folds.
    Each fold: (0, train_end, train_end, val_end).
    Training grows by _STEP each fold; validation is a fixed _VAL_WINDOW window.
    \"\"\"
    folds = []
    train_end = _MIN_TRAIN
    while train_end + _VAL_WINDOW <= n:
        val_end = min(train_end + _VAL_WINDOW, n)
        folds.append((0, train_end, train_end, val_end))
        train_end += _STEP
    return folds"""

new_make_splits = """def _make_splits(n: int) -> tuple[tuple[int, int, int, int], tuple[int, int, int, int]]:
    \"\"\"
    Chronological Train/Val/Test split.
    val_fold is used for hyperparameter tuning.
    test_fold is used for final unseen evaluation.
    \"\"\"
    val_size = int(n * _VAL_FRAC)
    test_size = int(n * _TEST_FRAC)
    train_size = n - val_size - test_size
    
    val_fold = (0, train_size, train_size, train_size + val_size)
    # the test fold trains on train + val sets
    test_fold = (0, train_size + val_size, train_size + val_size, n)
    return val_fold, test_fold"""
code = code.replace(old_make_folds, new_make_splits)

# 3. Fix _search_params
old_search_params_start = """def _search_params(
    X: pd.DataFrame,
    y_win: pd.Series,
    y_r: pd.Series,
    df_clean: pd.DataFrame,
    cat_cols: list[str],
    n_search: int = _N_SEARCH,
    max_folds: int = _MAX_FOLDS_SEARCH,
    verbose: bool = False,
) -> tuple[dict, list[dict]]:
    \"\"\"
    Random search: evaluate n_search param sets on the first max_folds folds.
    Returns (best_params, search_log).
    \"\"\"
    all_folds    = _make_folds(len(X))
    search_folds = all_folds[:max_folds]"""

new_search_params_start = """def _search_params(
    X: pd.DataFrame,
    y_win: pd.Series,
    y_r: pd.Series,
    df_clean: pd.DataFrame,
    search_folds: list,
    cat_cols: list[str],
    n_search: int = _N_SEARCH,
    verbose: bool = False,
) -> tuple[dict, list[dict]]:
    \"\"\"
    Random search: evaluate n_search param sets on the provided search folds (validation split).
    Returns (best_params, search_log).
    \"\"\"
    if not search_folds:
"""
# Need to use regex effectively or just a simple exact chunk replacement for search params
pattern_search_params = r"def _search_params\([\s\S]*?search_folds = all_folds\[:max_folds\]"
import re
code = re.sub(pattern_search_params, new_search_params_start, code)


# 4. Modify main() references
old_wf_setup = """    # ── Walk-forward folds ────────────────────────────────────────────────────
    folds = _make_folds(len(X))
    print(f"\\n{SEP}\\n  WALK-FORWARD SETUP\\n{SEP}")
    print(f"  Rows: {len(X):,}  |  Folds: {len(folds)}  |  "
          f"Min-train: {_MIN_TRAIN}  Val-window: {_VAL_WINDOW}  Step: {_STEP}")

    if not folds:
        print("❌ Not enough data for a single walk-forward fold.", file=sys.stderr)
        sys.exit(1)

    # ── Hyperparam search — binary classifier ─────────────────────────────────
    print(f"\\n{SEP}\\n  HYPERPARAMETER SEARCH — LightGBM Binary\\n{SEP}")
    best_params, search_log = _search_params(
        X, y_win, y_r, df_clean, cat_cols,
        n_search=_N_SEARCH, max_folds=min(_MAX_FOLDS_SEARCH, len(folds)),
        verbose=args.verbose,
    )
    _timeout_check()

    # ── Full walk-forward — binary classifier (champion params) ───────────────
    print(f"\\n{SEP}\\n  FULL WALK-FORWARD — LightGBM Binary\\n{SEP}")
    lgbm_wf = _walk_forward_binary(X, y_win, y_r, df_clean, folds,
                                   best_params, cat_cols, verbose=args.verbose)
    _timeout_check()"""

new_wf_setup = """    # ── Train/Val/Test Splits ──────────────────────────────────────────────────
    val_fold, test_fold = _make_splits(len(X))
    print(f"\\n{SEP}\\n  DATA SPLIT SETUP\\n{SEP}")
    train_size = val_fold[1]
    val_size = val_fold[3] - val_fold[2]
    test_size = test_fold[3] - test_fold[2]
    print(f"  Total Rows: {len(X):,} | Train: {train_size} | Val: {val_size} | Test: {test_size}")

    if train_size < _MIN_TRAIN:
        print(f"❌ Not enough data for training (Train size < {_MIN_TRAIN}).", file=sys.stderr)
        sys.exit(1)

    # ── Hyperparam search — binary classifier ─────────────────────────────────
    print(f"\\n{SEP}\\n  HYPERPARAMETER SEARCH — LightGBM Binary (on Validation Set)\\n{SEP}")
    best_params, search_log = _search_params(
        X, y_win, y_r, df_clean, [val_fold], cat_cols,
        n_search=_N_SEARCH,
        verbose=args.verbose,
    )
    _timeout_check()

    # ── Final unseen evaluation — binary classifier (champion params) ────────
    print(f"\\n{SEP}\\n  FINAL EVALUATION — LightGBM Binary (on Test Set)\\n{SEP}")
    lgbm_wf = _walk_forward_binary(X, y_win, y_r, df_clean, [test_fold],
                                   best_params, cat_cols, verbose=args.verbose)
    _timeout_check()"""
code = code.replace(old_wf_setup, new_wf_setup)

# Update Logistic baseline
old_logistic = """    # ── Logistic regression baseline ──────────────────────────────────────────
    print(f"\\n{SEP}\\n  WALK-FORWARD — Logistic Regression Baseline\\n{SEP}")
    logistic_result = _logistic_walk_forward(X, y_win, folds, verbose=args.verbose)"""

new_logistic = """    # ── Logistic regression baseline ──────────────────────────────────────────
    print(f"\\n{SEP}\\n  FINAL EVALUATION — Logistic Regression Baseline\\n{SEP}")
    logistic_result = _logistic_walk_forward(X, y_win, [test_fold], verbose=args.verbose)"""
code = code.replace(old_logistic, new_logistic)

# Update R-multiple regression
old_reg_search = """        reg_params, _ = _search_params(
            X, y_win, y_r, df_clean, cat_cols,
            n_search=min(10, _N_SEARCH), max_folds=min(3, len(folds)), verbose=False,
        )
        lgbm_reg_wf = _walk_forward_regression(
            X, y_r, df_clean, folds, reg_params, cat_cols, verbose=args.verbose)"""

new_reg_search = """        reg_params, _ = _search_params(
            X, y_win, y_r, df_clean, [val_fold], cat_cols,
            n_search=min(10, _N_SEARCH), verbose=False,
        )
        lgbm_reg_wf = _walk_forward_regression(
            X, y_r, df_clean, [test_fold], reg_params, cat_cols, verbose=args.verbose)"""
code = code.replace(old_reg_search, new_reg_search)


# Update val_doc metadata n_folds
code = code.replace(
    '"n_folds":                 len(folds),',
    '"n_folds":                 1,'
)
code = code.replace(
    '"val_window":              _VAL_WINDOW,\n        "step":                    _STEP,',
    '"val_frac":                _VAL_FRAC,\n        "test_frac":               _TEST_FRAC,'
)

# Also handle "Folds" printing string
code = code.replace(
    'print(f"  Folds:           {len(folds)} total / {agg[\'n_folds\']} valid")',
    'print(f"  Folds evaluated: {agg[\'n_total_folds\']}")'
)

with open("scripts/ml/train_entry_model.py", "w", encoding="utf-8") as f:
    f.write(code)

print("Train patch applied successfully.")
