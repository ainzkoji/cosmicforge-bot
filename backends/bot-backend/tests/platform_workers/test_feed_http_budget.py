"""Every runtime construction of the daily feed shares the platform request budget.

The feed reads exchange metadata from ``fapi.binance.com``, the same host the order path uses. Without the shared
budget a feed refresh can spend the request weight the order path needs. ``budgeted_archive_get`` routes those reads
through ``http_budget.public_get`` and leaves the archive CDN on its own allowance.
"""
import ast
from pathlib import Path

APP = Path(__file__).resolve().parents[2] / "app"

# No exception is left: the daily_feed operator CLI no longer builds a feed for an update; the update runs from
# ``python -m app.trading_intelligence.pipeline feed-update``, which passes the budgeted getter.
KNOWN_UNBUDGETED = set()


def _calls():
    for path in sorted(APP.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for node in ast.walk(fn):
                if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "default_daily_bars":
                    yield path.relative_to(APP).as_posix(), fn.name, node


def test_runtime_feeds_pass_the_budgeted_getter():
    found, unbudgeted = [], []
    for rel, fn, call in _calls():
        found.append((rel, fn))
        if not any(k.arg == "http_get" for k in call.keywords) and (rel, fn) not in KNOWN_UNBUDGETED:
            unbudgeted.append(f"{rel}:{call.lineno} in {fn}()")
    assert len(found) >= 4, found  # portfolio runtime, shadow comparison job, pipeline run-due and feed-update
    assert not unbudgeted, f"daily feed built without the shared request budget: {unbudgeted}"


def test_shadow_comparison_source_uses_the_budget(monkeypatch, tmp_path):
    from app.market_data import daily_feed
    from app.platform_workers import runtime_jobs
    from app.trading_intelligence.integration import platform_runtime

    seen = {}
    marker = object()
    monkeypatch.setattr(platform_runtime, "budgeted_archive_get", lambda db: marker)
    monkeypatch.setattr(daily_feed, "default_daily_bars", lambda db, **kw: seen.update(kw) or "bars")
    services = runtime_jobs.RuntimeServices.__new__(runtime_jobs.RuntimeServices)
    services.legacy_db, services._shadow = object(), None
    try:
        services.shadow_source()
    except Exception:
        pass  # the registry and accounts are not under test; the feed is built first
    assert seen.get("http_get") is marker


def test_the_daily_feed_cli_refuses_an_unbudgeted_update(monkeypatch, capsys):
    from app.market_data import daily_feed

    monkeypatch.setattr(daily_feed, "default_daily_bars", lambda *a, **kw: (_ for _ in ()).throw(AssertionError("built")))
    assert daily_feed.main(["update", "--max-requests", "3"]) == 2
    assert "pipeline feed-update" in capsys.readouterr().err


def test_the_pipeline_feed_update_uses_the_shared_budget(monkeypatch, capsys):
    from app.market_data import daily_feed
    from app.trading_intelligence.integration import platform_runtime
    from app.trading_intelligence.pipeline import __main__ as cli

    marker, seen = object(), {}

    class Feed:
        def update(self):
            return {"status": daily_feed.COMPLETED}

    monkeypatch.setattr(cli, "_db", lambda: "isolated-db")
    monkeypatch.setattr(platform_runtime, "budgeted_archive_get", lambda db: seen.setdefault("budget_db", db) and marker)
    monkeypatch.setattr(daily_feed, "default_daily_bars", lambda db, **kw: seen.update(kw, db=db) or Feed())
    assert cli.main(["feed-update", "--max-requests", "7", "--workers", "2"]) == 0
    assert seen["http_get"] is marker and seen["budget_db"] == seen["db"] == "isolated-db"
    assert seen["policy"].max_requests_per_update == 7 and seen["workers"] == 2
