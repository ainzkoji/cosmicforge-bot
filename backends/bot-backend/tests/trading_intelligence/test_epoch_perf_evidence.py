"""[CATI_EPOCH_PERF]: per-epoch whole-universe deadline evidence (observation only)."""
import logging
from types import SimpleNamespace

from app.trading_intelligence.integration import cycle_shadow as cs
from app.trading_intelligence.observability.metrics import METRICS


def test_epoch_perf_line_reports_counts_and_stage_deltas_since_the_epoch_opened(caplog):
    METRICS.observe("cati_stage_latency_ms", 5.0, stage="FORECAST")  # before the epoch: excluded
    info = {"epoch": 1790712900000, "perf0": cs._perf_baseline()}
    METRICS.observe("cati_stage_latency_ms", 7.0, stage="FORECAST")
    METRICS.observe("cati_stage_latency_ms", 3.0, stage="RANKING")
    batch = SimpleNamespace(batch_complete=True, expected_due_instruments=("A", "B"), completed_instruments=("A", "B"),
                            failed_instruments=())
    with caplog.at_level(logging.INFO, logger=cs.logger.name):
        cs._log_epoch_perf("bot1", info, "ALL_TERMINAL", batch)
    line = next(r.getMessage() for r in caplog.records if "[CATI_EPOCH_PERF]" in r.getMessage())
    assert "complete=True expected=2 completed=2 failed=0" in line and "wall_s=" in line
    assert '"FORECAST": {"ms": 7.0, "n": 1}' in line and '"RANKING": {"ms": 3.0, "n": 1}' in line
