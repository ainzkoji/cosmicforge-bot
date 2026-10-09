"""Shared synthetic market for the Step 2 official-evaluation and report tests.

Synthetic data drives the real entry points with controlled input. It is never evidence about a strategy.
"""
from __future__ import annotations

import numpy as np

from app.market_data.daily_dataset import DailyPanel, day_index
from app.trading_intelligence.families.daily_trend import spec
from app.trading_intelligence.families.daily_trend.registration import register_mandate_004
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.research.evaluator import official as O
from app.trading_intelligence.research.governance import holdout as H
from app.trading_intelligence.research.governance.history import import_historical_hypotheses
from app.trading_intelligence.research.governance.mandates import amend_mandate
from app.trading_intelligence.research.governance.register import ResearchRegister, repository_root

PERIODS = O.Periods("2020-01-01", "2020-12-31", "2021-01-01", "2021-06-30")
SRC = {"fingerprint": "f" * 64, "commit": "c0ffee", "evaluation_source_dirty": False, "dirty_paths": []}
N = 26


class SyntheticDataset(O.DatasetHandle):
    """A stand-in for the frozen dataset: deterministic random-walk coins, one listed late, one that ends."""

    def __init__(self, seed: int = 7, drift: float = 0.002, vol: float = 0.03, end: str = "2021-06-30"):
        self.seed, self.drift, self.vol, self.end, self.loads = seed, drift, vol, end, []
        self.metadata = {"symbols": {}}
        self.manifest = {
            "dataset_id": "synthetic", "dataset_version": f"s{seed}", "dataset_hash": stable_hash(["synthetic", seed, drift, vol, end]),
            "manifest_hash": stable_hash(["manifest", seed, drift, vol, end]), "coverage_start": "2020-01-01",
            "coverage_end": end, "symbols_with_bars": N,
            "quality_totals": {"failed_downloads": 0, "unreadable_files": 0, "invalid_candles": 0, "duplicate_candles": 0,
                               "missing_candles": 0, "symbols_with_missing_candles": 0, "candles_from_daily_files": 0,
                               "non_trading_bars": 0, "bar_days": 12345, "bar_days_without_any_funding_record": 0},
            "contracts_that_ended_before_coverage_end": {"count": 1, "symbols": ["C03USDT"]},
            "raw_artifact_hashes": {"files": 0, "inventory_sha256": "0" * 64}, "known_limitations": ["Synthetic test market."]}
        self.coverage = {"manifest_hash": self.manifest["manifest_hash"], "symbols": {}}
        self.manifest_path = repository_root() / "synthetic.dataset.json"

    def _load(self, first_day: str, last_day: str):
        self.loads.append(last_day)
        lo, hi = day_index("2020-01-01"), day_index(self.end)
        t = hi - lo + 1
        rng = np.random.default_rng(self.seed)
        close = 100.0 * np.exp(np.cumsum(rng.normal(self.drift, self.vol, (t, N)), axis=0))
        open_ = np.vstack([close[:1], close[:-1]]) * np.exp(rng.normal(0, 0.004, (t, N)))
        high = np.maximum(open_, close) * np.exp(np.abs(rng.normal(0, 0.008, (t, N))))
        low = np.minimum(open_, close) * np.exp(-np.abs(rng.normal(0, 0.008, (t, N))))
        qv = np.exp(rng.normal(16, 1.2, (1, N))) * np.exp(rng.normal(0, 0.25, (t, N)))
        fields = {"open": open_, "high": high, "low": low, "close": close, "quote_volume": qv}
        for m in fields.values():
            m[:60, 5] = np.nan                                  # a late listing
            m[400:, 3] = np.nan                                 # a contract that ends
        fields["funding_midnight"] = rng.normal(0.0001, 0.0001, (t, N))
        fields["funding_later_positive"] = np.abs(rng.normal(0.0001, 0.0001, (t, N)))
        fields["funding_later_negative"] = -np.abs(rng.normal(0.00003, 0.00005, (t, N)))
        fields["funding_hours"] = np.full((t, N), 24.0)
        panel = DailyPanel(np.arange(lo, hi + 1), [f"C{j:02d}USDT" for j in range(N)], fields)
        return panel.truncated(day_index(last_day)) if day_index(last_day) < hi else panel


def new_register(path) -> ResearchRegister:
    """History imported, Mandate 004 registered with both interpretations: the state before any run."""
    reg = ResearchRegister(path)
    import_historical_hypotheses(reg)
    register_mandate_004(reg, research_code_commit="c0ffee", registered_by="tester", approved_by="owner",
                         approved_at="2026-10-09", authorization_reference="test")
    amend_mandate(reg, mandate_id=spec.MANDATE_ID, kind="IMPLEMENTATION_INTERPRETATION", summary="non-trading bars",
                  artifact_path="research/trend_v1/INTERPRETATION_002.md", changes_strategy_rules=False, recorded_by="tester")
    return reg


def make_setup(tmp_path, **dataset_kw):
    reg = new_register(tmp_path / "register.jsonl")
    ds = SyntheticDataset(**dataset_kw)
    O.freeze_dataset_in_register(reg, ds)
    O.register_cost_model(reg)
    return reg, ds, tmp_path / "runs"


def go(setup, run_type=O.DEVELOPMENT, **kw):
    reg, ds, runs = setup
    args = dict(register=reg, dataset=ds, artifacts_root=runs, periods=PERIODS, researcher="tester", source=SRC,
                bootstrap_repetitions=300)
    args.update(kw)
    return O.run_evaluation(run_type, **args)


def authorize(setup, dev, periods=PERIODS, **over):
    reg, ds, _ = setup
    hid = H.holdout_id_for(spec.MANDATE_ID, ds.dataset_hash, periods.holdout_start, periods.holdout_end)
    kw = dict(facts={k: True for k, _ in H.REQUIREMENTS}, mandate_id=spec.MANDATE_ID,
              specification_sha256=dev["specification_sha256"], dataset_hash=ds.dataset_hash,
              source_fingerprint=SRC["fingerprint"], development_run_id=dev["run_id"])
    kw.update(over)
    H.record_owner_holdout_authorization(reg, holdout_id=hid, readiness=H.pre_holdout_readiness(**kw),
                                         authorized_by="Owner Name", authorization_reference="test message", reason="final")
    return hid
