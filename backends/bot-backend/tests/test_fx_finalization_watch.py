"""Read-only completeness accounting and evidence-triggered FX supervision."""
import importlib.util
import json
from pathlib import Path
import sqlite3


def test_watcher_matches_completed_days_without_querying_price_rows(tmp_path, monkeypatch):
    script = Path(__file__).resolve().parents[3] / "scripts/fx_finalization_watch.py"
    spec = importlib.util.spec_from_file_location("fxwatch_test", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    db = tmp_path / "fx.db"
    manifest = tmp_path / "manifest.json"
    # Fri/Sat/Sun; Saturday is excluded by the frozen acquisition planner.
    manifest.write_text(json.dumps({"provider": "D", "window_start_ms": 1788480000000,
                                   "window_end_ms": 1788739200000, "members": [{"pair": "EURUSD"}]}))
    with sqlite3.connect(db) as c:
        c.execute("CREATE TABLE fx_reference_ingest_log (provider TEXT,pair TEXT,timeframe TEXT,period TEXT,side TEXT,status TEXT)")
        c.executemany("INSERT INTO fx_reference_ingest_log VALUES ('D','EURUSD','1m',?,?,?)",
                      [("2026-09-04", "BID", "FETCHED"), ("2026-09-04", "ASK", "FETCHED"),
                       ("2026-09-06", "BID", "EMPTY"), ("2026-09-06", "ASK", "NO_FILE")])
    monkeypatch.setattr(module, "DB", db)
    monkeypatch.setattr(module, "MANIFEST", manifest)
    assert module.remaining() == 0
    original = module.fingerprint()
    with sqlite3.connect(db) as c:
        c.execute("UPDATE fx_reference_ingest_log SET status='FAILED' WHERE side='ASK' AND period='2026-09-06'")
    assert module.remaining() == 1
    assert module.fingerprint() != original
