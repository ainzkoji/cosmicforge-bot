from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
DB_PATH = ROOT / "backends" / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"
ENV_PATH = ROOT / "backends" / "bot-backend" / ".env"

VERDICT_READY = "PHASE 6 LIMITED MODE READY"
VERDICT_NEEDS_FIX = "PHASE 6 NEEDS FIX"
VERDICT_RUNTIME_STALE = "PHASE 6 RUNTIME STALE - RESTART REQUIRED"
VERDICT_UNSAFE = "UNSAFE — DO NOT ENABLE PHASE 6"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_csv(name: str, default: str = "") -> list[str]:
    return [p.strip().upper() for p in os.getenv(name, default).split(",") if p.strip()]


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def load_dotenv_defaults(path: Path = ENV_PATH) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ[key] = value


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _latest_source_config_mtime() -> datetime:
    paths = [
        ENV_PATH,
        ROOT / "backends" / "bot-backend" / "app" / "main.py",
        ROOT / "backends" / "bot-backend" / "app" / "queue" / "external_signal_processor.py",
        ROOT / "backends" / "bot-backend" / "app" / "core" / "config.py",
    ]
    latest = max(path.stat().st_mtime for path in paths if path.exists())
    return datetime.fromtimestamp(latest, tz=timezone.utc)


def _port_owner_pid(runtime_url: str) -> int | None:
    parsed = urllib.parse.urlparse(runtime_url)
    if parsed.hostname not in {"127.0.0.1", "localhost"} or not parsed.port:
        return None
    try:
        out = subprocess.check_output(["netstat", "-ano"], text=True, timeout=5)
    except Exception:
        return None
    needle = f":{parsed.port}"
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 5 and needle in parts[1] and parts[3].upper() == "LISTENING":
            try:
                return int(parts[-1])
            except Exception:
                return None
    return None


def _pid_matches_or_child(pid: Any, owner_pid: Any) -> bool:
    if pid is None or owner_pid is None:
        return False
    try:
        pid_i = int(pid)
        owner_i = int(owner_pid)
    except Exception:
        return False
    if pid_i == owner_i:
        return True
    try:
        import psutil  # type: ignore

        proc = psutil.Process(pid_i)
        if int(proc.ppid()) == owner_i:
            return True
    except Exception:
        pass
    return False


def fetch_runtime_fingerprint(runtime_url: str, *, timeout: int = 5) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(runtime_url, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        return {"reachable": False, "error": str(exc)}
    fp = data.get("tradingview_runtime_fingerprint") if isinstance(data, dict) else None
    if not isinstance(fp, dict):
        return {
            "reachable": True,
            "fingerprint_present": False,
            "raw_status_keys": sorted(data.keys()) if isinstance(data, dict) else [],
        }
    fp["reachable"] = True
    fp["fingerprint_present"] = True
    fp["port_owner_pid"] = _port_owner_pid(runtime_url)
    return fp


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone() is not None


@dataclass
class Phase6ValidationReport:
    generated_at: str
    verdict: str
    config: dict[str, Any] = field(default_factory=dict)
    runtime_fingerprint: dict[str, Any] = field(default_factory=dict)
    proof_report: dict[str, Any] = field(default_factory=dict)
    safety_checks: dict[str, Any] = field(default_factory=dict)
    runtime_stale_findings: list[str] = field(default_factory=list)
    config_findings: list[str] = field(default_factory=list)
    lockout_findings: list[str] = field(default_factory=list)
    safety_findings: list[str] = field(default_factory=list)
    blocking_findings: list[str] = field(default_factory=list)
    markdown_report_path: str | None = None
    json_report_path: str | None = None


def latest_phase5c_success_report(output_dir: Path) -> dict[str, Any]:
    candidates = sorted(
        output_dir.glob("phase5c_controlled_clean_candle_execution_proof_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for path in candidates:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if data.get("final_verdict") == "SUCCESSFUL LIVE-MODE TRADINGVIEW EXECUTION PROOF PASSED":
            return {"exists": True, "path": str(path), "final_verdict": data.get("final_verdict")}
    return {"exists": False, "path": None, "final_verdict": None}


def evaluate(
    db_path: Path,
    proof_dir: Path,
    *,
    runtime_url: str = "http://127.0.0.1:9000/health",
    require_runtime: bool = True,
    required_restart_after: str | None = None,
) -> Phase6ValidationReport:
    config = {
        "TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED": env_bool("TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED", False),
        "TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED": env_bool("TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED", False),
        "TRADINGVIEW_ALLOWED_ACTIONS": env_csv("TRADINGVIEW_ALLOWED_ACTIONS", "BUY,SELL"),
        "TRADINGVIEW_ALLOWED_SYMBOLS": env_csv("TRADINGVIEW_ALLOWED_SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT"),
        "TRADINGVIEW_MAX_TRADE_USDT_CAP": env_float("TRADINGVIEW_MAX_TRADE_USDT_CAP", 150.0),
        "TRADINGVIEW_MAX_SIGNALS_PER_HOUR": env_int("TRADINGVIEW_MAX_SIGNALS_PER_HOUR", 5),
        "TRADINGVIEW_MAX_SIGNALS_PER_DAY": env_int("TRADINGVIEW_MAX_SIGNALS_PER_DAY", 20),
        "TRADINGVIEW_MAX_EXECUTIONS_PER_DAY": env_int("TRADINGVIEW_MAX_EXECUTIONS_PER_DAY", 3),
        "TRADINGVIEW_MAX_QUEUE_PER_CYCLE": env_int("TRADINGVIEW_MAX_QUEUE_PER_CYCLE", 1),
        "TRADINGVIEW_REQUIRE_SLTP_PROTECTION": env_bool("TRADINGVIEW_REQUIRE_SLTP_PROTECTION", True),
        "TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL": env_bool("TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL", True),
        "forbidden_capabilities_disabled": not any(
            env_bool(name, False)
            for name in [
                "TRADINGVIEW_ALLOW_CLOSE",
                "TRADINGVIEW_ALLOW_REVERSE",
                "TRADINGVIEW_ALLOW_REDUCE",
                "TRADINGVIEW_ALLOW_CANCEL",
                "TRADINGVIEW_ALLOW_EXTERNAL_SLTP",
                "TRADINGVIEW_ALLOW_EXTERNAL_SIZE",
                "TRADINGVIEW_ALLOW_RISK_OVERRIDE",
            ]
        ),
    }
    proof = latest_phase5c_success_report(proof_dir)
    checks: dict[str, Any] = {}
    config_findings: list[str] = []
    lockout_findings: list[str] = []
    safety_findings: list[str] = []
    runtime_stale_findings: list[str] = []

    if not proof["exists"]:
        safety_findings.append("No successful Phase 5B/5C proof report found")
    if not config["TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED"]:
        config_findings.append("TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED is false")
    if not config["TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED"]:
        config_findings.append("TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED is false")
    if set(config["TRADINGVIEW_ALLOWED_ACTIONS"]) != {"BUY", "SELL"}:
        config_findings.append("Allowed actions must be exactly BUY,SELL")
    if not config["TRADINGVIEW_ALLOWED_SYMBOLS"] or len(config["TRADINGVIEW_ALLOWED_SYMBOLS"]) > 20:
        config_findings.append("Allowed symbols must be restricted to 1-20 symbols for Phase 6")
    if config["TRADINGVIEW_MAX_TRADE_USDT_CAP"] <= 0:
        config_findings.append("Max trade cap must be positive")
    if config["TRADINGVIEW_MAX_EXECUTIONS_PER_DAY"] <= 0 or config["TRADINGVIEW_MAX_EXECUTIONS_PER_DAY"] > 3:
        config_findings.append("Daily execution cap must be 1-3 for limited rollout")
    if config["TRADINGVIEW_MAX_QUEUE_PER_CYCLE"] != 1:
        config_findings.append("Max queue per cycle should be 1 for limited rollout")
    if not config["TRADINGVIEW_REQUIRE_SLTP_PROTECTION"]:
        config_findings.append("SL/TP protection must be required")
    if not config["TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL"]:
        config_findings.append("Auto-disable on invariant failure must be enabled")
    if not config["forbidden_capabilities_disabled"]:
        config_findings.append("One or more forbidden TradingView capabilities is enabled")

    runtime = fetch_runtime_fingerprint(runtime_url)
    checks["runtime_url"] = runtime_url
    checks["latest_source_config_mtime"] = _latest_source_config_mtime().isoformat()
    if require_runtime:
        if not runtime.get("reachable"):
            runtime_stale_findings.append(f"Runtime endpoint unreachable: {runtime.get('error')}")
        elif not runtime.get("fingerprint_present"):
            runtime_stale_findings.append("Runtime endpoint does not expose Phase 6 fingerprint")
        else:
            if not runtime.get("phase6_gate_available"):
                runtime_stale_findings.append("Runtime reports phase6_gate_available=false")
            if runtime.get("phase6_gate_code_version") != "phase6_limited_gate_v1_2026-05-21":
                runtime_stale_findings.append(
                    "Runtime phase6_gate_code_version is missing or not phase6_limited_gate_v1_2026-05-21"
                )
            pid = runtime.get("pid")
            owner_pid = runtime.get("port_owner_pid")
            if owner_pid is not None and pid is not None and not _pid_matches_or_child(pid, owner_pid):
                runtime_stale_findings.append(f"Runtime pid {pid} does not match port owner pid {owner_pid}")
            cwd = str(runtime.get("working_directory") or "").replace("\\", "/")
            pyexe = str(runtime.get("python_executable") or "").replace("\\", "/")
            if not cwd.endswith("backends/bot-backend"):
                runtime_stale_findings.append(f"Runtime working_directory is wrong: {runtime.get('working_directory')}")
            if "backends/venv/" not in pyexe:
                runtime_stale_findings.append(f"Runtime python_executable is wrong: {runtime.get('python_executable')}")
            if required_restart_after:
                started = _parse_dt(runtime.get("process_started_at"))
                required = _parse_dt(required_restart_after)
                if started and required and started < required:
                    runtime_stale_findings.append(
                        f"Runtime process_started_at {started.isoformat()} is older than required restart timestamp {required.isoformat()}"
                    )
            expected_runtime = {
                "TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED": config["TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED"],
                "TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED": config["TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED"],
                "TRADINGVIEW_ALLOWED_SYMBOLS": config["TRADINGVIEW_ALLOWED_SYMBOLS"],
                "TRADINGVIEW_ALLOWED_ACTIONS": config["TRADINGVIEW_ALLOWED_ACTIONS"],
                "TRADINGVIEW_MAX_QUEUE_PER_CYCLE": config["TRADINGVIEW_MAX_QUEUE_PER_CYCLE"],
                "TRADINGVIEW_MAX_EXECUTIONS_PER_DAY": config["TRADINGVIEW_MAX_EXECUTIONS_PER_DAY"],
                "TRADINGVIEW_MAX_SIGNALS_PER_HOUR": config["TRADINGVIEW_MAX_SIGNALS_PER_HOUR"],
                "TRADINGVIEW_MAX_SIGNALS_PER_DAY": config["TRADINGVIEW_MAX_SIGNALS_PER_DAY"],
                "TRADINGVIEW_MAX_TRADE_USDT_CAP": config["TRADINGVIEW_MAX_TRADE_USDT_CAP"],
                "TRADINGVIEW_REQUIRE_SLTP_PROTECTION": config["TRADINGVIEW_REQUIRE_SLTP_PROTECTION"],
                "TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL": config["TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL"],
            }
            for key, expected in expected_runtime.items():
                if runtime.get(key) != expected:
                    config_findings.append(
                        f"Runtime mismatch for {key}: expected {expected!r}, got {runtime.get(key)!r}"
                    )
            for key in [
                "TRADINGVIEW_ALLOW_CLOSE",
                "TRADINGVIEW_ALLOW_REVERSE",
                "TRADINGVIEW_ALLOW_REDUCE",
                "TRADINGVIEW_ALLOW_CANCEL",
                "TRADINGVIEW_ALLOW_EXTERNAL_SLTP",
                "TRADINGVIEW_ALLOW_EXTERNAL_SIZE",
                "TRADINGVIEW_ALLOW_RISK_OVERRIDE",
            ]:
                if runtime.get(key) is not False:
                    config_findings.append(f"Runtime forbidden capability {key} is not false")
            if runtime.get("active_safety_lockout"):
                lockout_findings.append("TradingView safety lockout is active in live runtime")

    conn = sqlite3.connect(db_path, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=20000")
    try:
        if table_exists(conn, "external_signal_queue"):
            stuck = conn.execute(
                "SELECT COUNT(*) AS c FROM external_signal_queue WHERE status='CLAIMED'"
            ).fetchone()["c"]
            checks["stuck_claimed_rows"] = int(stuck)
            if stuck:
                safety_findings.append("Stuck CLAIMED external signal rows exist")
        if table_exists(conn, "position_lifecycle_state"):
            unprotected = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM position_lifecycle_state
                WHERE COALESCE(exchange_position_active, 0) = 1
                  AND (
                    sl_order_id IS NULL OR tp_order_id IS NULL
                    OR sl_order_id LIKE '%DUPLICATE_4130%'
                    OR tp_order_id LIKE '%DUPLICATE_4130%'
                  )
                """
            ).fetchone()["c"]
            checks["unprotected_positions"] = int(unprotected)
            if unprotected:
                safety_findings.append("Unprotected active positions exist")
        if table_exists(conn, "tradingview_processor_heartbeat"):
            hb = conn.execute(
                "SELECT * FROM tradingview_processor_heartbeat ORDER BY updated_at DESC LIMIT 1"
            ).fetchone()
            checks["processor_heartbeat"] = dict(hb) if hb else None
            if not hb:
                safety_findings.append("Processor heartbeat missing")
        if table_exists(conn, "tradingview_safety_lockouts"):
            lockouts = conn.execute(
                "SELECT * FROM tradingview_safety_lockouts WHERE is_locked=1"
            ).fetchall()
            checks["active_safety_lockouts"] = [dict(r) for r in lockouts]
            if lockouts:
                lockout_findings.append("TradingView safety lockout is active")
    finally:
        conn.close()

    findings = (
        runtime_stale_findings
        + config_findings
        + lockout_findings
        + safety_findings
    )
    verdict = VERDICT_READY if not findings else VERDICT_NEEDS_FIX
    if runtime_stale_findings:
        verdict = VERDICT_RUNTIME_STALE
    if checks.get("unprotected_positions"):
        verdict = VERDICT_UNSAFE
    return Phase6ValidationReport(
        generated_at=utc_now(),
        verdict=verdict,
        config=config,
        runtime_fingerprint=runtime,
        proof_report=proof,
        safety_checks=checks,
        runtime_stale_findings=runtime_stale_findings,
        config_findings=config_findings,
        lockout_findings=lockout_findings,
        safety_findings=safety_findings,
        blocking_findings=findings,
    )


def render_markdown(report: Phase6ValidationReport) -> str:
    return "\n\n".join(
        [
            "# Phase 6 — Live-Mode Limited TradingView Candidate Mode Validation",
            f"Final verdict: `{report.verdict}`",
            "## Config\n```json\n" + json.dumps(report.config, indent=2, default=str) + "\n```",
            "## Runtime Fingerprint\n```json\n" + json.dumps(report.runtime_fingerprint, indent=2, default=str) + "\n```",
            "## Phase 5B/5C Proof\n```json\n" + json.dumps(report.proof_report, indent=2, default=str) + "\n```",
            "## Safety Checks\n```json\n" + json.dumps(report.safety_checks, indent=2, default=str) + "\n```",
            "## Runtime Stale Findings\n```json\n" + json.dumps(report.runtime_stale_findings, indent=2, default=str) + "\n```",
            "## Config Findings\n```json\n" + json.dumps(report.config_findings, indent=2, default=str) + "\n```",
            "## Lockout Findings\n```json\n" + json.dumps(report.lockout_findings, indent=2, default=str) + "\n```",
            "## Safety Findings\n```json\n" + json.dumps(report.safety_findings, indent=2, default=str) + "\n```",
            "## Blocking Findings\n```json\n" + json.dumps(report.blocking_findings, indent=2, default=str) + "\n```",
        ]
    )


def write_reports(report: Phase6ValidationReport, output_dir: Path) -> Phase6ValidationReport:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = output_dir / f"phase6_live_mode_limited_validation_{stamp}"
    json_path = base.with_suffix(".json")
    md_path = base.with_suffix(".md")
    report.json_report_path = str(json_path)
    report.markdown_report_path = str(md_path)
    json_path.write_text(json.dumps(asdict(report), indent=2, default=str), encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--proof-dir", type=Path, default=ROOT / "reports" / "tradingview_phase5")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "reports" / "tradingview_phase6")
    parser.add_argument("--runtime-url", default="http://127.0.0.1:9000/health")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--required-restart-after", default=None)
    args = parser.parse_args()
    load_dotenv_defaults()
    report = write_reports(
        evaluate(
            args.db_path,
            args.proof_dir,
            runtime_url=args.runtime_url,
            require_runtime=args.strict,
            required_restart_after=args.required_restart_after,
        ),
        args.output_dir,
    )
    print(f"Phase 6 validation: {report.verdict}")
    print(f"Markdown report: {report.markdown_report_path}")
    print(f"JSON report: {report.json_report_path}")
    if report.verdict == VERDICT_READY:
        return 0
    if report.verdict == VERDICT_UNSAFE:
        return 2
    if report.verdict == VERDICT_RUNTIME_STALE:
        return 3
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
