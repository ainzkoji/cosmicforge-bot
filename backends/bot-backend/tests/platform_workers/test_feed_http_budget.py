"""Every runtime construction of the daily feed shares the platform request budget.

The feed reads exchange metadata from ``fapi.binance.com``, the same host the order path uses. Without the shared
budget a feed refresh can spend the request weight the order path needs. ``budgeted_archive_get`` routes those reads
through ``http_budget.public_get`` and leaves the archive CDN on its own allowance.
"""
import ast
from pathlib import Path

APP = Path(__file__).resolve().parents[2] / "app"

# The daily_feed operator CLI (``python -m app.market_data.daily_feed update``) is the one remaining unbudgeted
# caller. It is in a file that carries someone else's uncommitted change, so it is recorded, not edited:
# docs/implementation/MASTER_PLAN_I_L_COMPLIANCE.md, "Uncommitted change".
KNOWN_UNBUDGETED = {("market_data/daily_feed.py", "main")}


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
    assert len(found) >= 3, found  # portfolio runtime, shadow comparison job, pipeline CLI
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
