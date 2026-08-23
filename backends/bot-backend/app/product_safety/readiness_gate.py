from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone, date
from enum import Enum
from typing import Any, Optional

from app.core.config import settings
from shared_lib.persistence.db import DB, utc_now_iso


class ReadinessStatus(str, Enum):
    NOT_READY = "NOT_READY"
    PAPER_VALIDATION_RUNNING = "PAPER_VALIDATION_RUNNING"
    READY_FOR_CONTROLLED_BETA_REVIEW = "READY_FOR_CONTROLLED_BETA_REVIEW"
    APPROVED_FOR_CONTROLLED_BETA = "APPROVED_FOR_CONTROLLED_BETA"
    REJECTED = "REJECTED"


class RejectionReason(str, Enum):
    USER_CAPITAL_READINESS_NOT_MET = "USER_CAPITAL_READINESS_NOT_MET"
    PAPER_TRADING_PERIOD_TOO_SHORT = "PAPER_TRADING_PERIOD_TOO_SHORT"
    INSUFFICIENT_CLOSED_TRADES = "INSUFFICIENT_CLOSED_TRADES"
    MISSING_DECISION_TRACES = "MISSING_DECISION_TRACES"
    NEGATIVE_EXPECTANCY = "NEGATIVE_EXPECTANCY"
    MAX_DRAWDOWN_TOO_HIGH = "MAX_DRAWDOWN_TOO_HIGH"
    PROFIT_FACTOR_TOO_LOW = "PROFIT_FACTOR_TOO_LOW"
    SIZING_FAILURES_DETECTED = "SIZING_FAILURES_DETECTED"
    DAILY_CLOSE_NOT_VALIDATED = "DAILY_CLOSE_NOT_VALIDATED"
    SIGNAL_SCHEDULER_NOT_VALIDATED = "SIGNAL_SCHEDULER_NOT_VALIDATED"
    SECTIONS_A_TO_E_NOT_CONFIRMED = "SECTIONS_A_TO_E_NOT_CONFIRMED"


@dataclass
class UserCapitalReadinessReport:
    sections_a_to_e_deployed: bool

    paper_start_date: Optional[str]
    paper_end_date: Optional[str]
    paper_days_count: int
    consecutive_paper_weeks: int

    closed_trade_count: int

    trades_with_complete_decision_traces: int
    trades_missing_decision_traces: int

    win_rate: float
    average_win: float
    average_loss: float
    expectancy_after_fees: float

    gross_profit: float
    gross_loss: float
    net_pnl: float
    profit_factor: float

    max_drawdown_pct: float
    sizing_failure_count: int

    daily_close_expected_count: int
    daily_close_success_count: int
    daily_close_missing_days: list[str]

    signal_generation_expected_count: int
    signal_generation_success_count: int
    signal_generation_missing_runs: list[str]

    readiness_status: ReadinessStatus
    blocking_reasons: list[str]
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["readiness_status"] = self.readiness_status.value
        return d


class UserCapitalReadinessError(ValueError):
    def __init__(self, payload: dict[str, Any]):
        super().__init__(payload.get("reason") or payload.get("status") or "USER_CAPITAL_READINESS_NOT_MET")
        self.payload = payload


def _parse_iso(ts: str) -> datetime:
    # Handles "2026-05-02T10:40:58.040482+00:00" and "Z"
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def _iso_week_key(dt: date) -> int:
    iso = dt.isocalendar()
    return iso.year * 100 + iso.week


def _consecutive_weeks_count(dates: list[date]) -> int:
    if not dates:
        return 0
    weeks = sorted({_iso_week_key(d) for d in dates})
    # count longest consecutive streak
    best = 1
    cur = 1
    for i in range(1, len(weeks)):
        prev = weeks[i - 1]
        curr = weeks[i]
        # increment by 1 week across year boundaries is not strictly +1 in this encoding,
        # so we approximate by stepping through calendar weeks using date arithmetic instead.
        # Easiest: compare actual Monday dates.
        # Convert week keys back is messy; instead just use adjacent dates check:
        # If there exists a date in the next week, it will be in the set.
        # Here: use numeric key gap; for most cases it's fine; year boundary handled below.
        if curr == prev + 1:
            cur += 1
        else:
            cur = 1
        best = max(best, cur)
    # year boundary correction: if we have (YYYY-W52) then (YYYY+1-W01) keys won't be +1.
    # If we only had a boundary gap, longest streak may be undercounted; fix by simulating in dates.
    # Build week start dates and recompute.
    monday_starts = sorted({(d - timedelta(days=d.weekday())) for d in dates})
    if not monday_starts:
        return best
    best2 = 1
    cur2 = 1
    for i in range(1, len(monday_starts)):
        if (monday_starts[i] - monday_starts[i - 1]).days == 7:
            cur2 += 1
        else:
            cur2 = 1
        best2 = max(best2, cur2)
    return max(best, best2)


def evaluate_user_capital_readiness(
    *,
    db: DB,
    bot_instance_id: str,
    now_utc: Optional[datetime] = None,
) -> UserCapitalReadinessReport:
    now = now_utc or datetime.now(timezone.utc)
    now_iso = now.isoformat().replace("+00:00", "Z")

    sections_confirmed = bool(getattr(settings, "SECTIONS_A_TO_E_CONFIRMED", False))

    # Pull closed fills (paper runs only)
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT f.timestamp_utc, f.realized_pnl, f.fee, f.total_fees, f.net_pnl, f.trace_id
            FROM trade_fills f
            LEFT JOIN runs r ON r.run_id = f.run_id
            WHERE f.action='CLOSE'
              AND (f.bot_instance_id = ? OR (f.bot_instance_id IS NULL AND ? = 'default'))
              AND (r.mode IS NULL OR lower(r.mode) = 'paper')
            ORDER BY f.timestamp_utc ASC
            """,
            (bot_instance_id, bot_instance_id),
        ).fetchall()

    close_dates: list[date] = []
    net_pnls: list[float] = []
    missing_trace = 0
    complete_trace = 0

    paper_start = None
    paper_end = None

    for r in rows:
        ts = r["timestamp_utc"]
        if not ts:
            continue
        dt = _parse_iso(str(ts))
        close_dates.append(dt.date())
        if paper_start is None:
            paper_start = dt
        paper_end = dt

        realized = float(r["realized_pnl"] or 0.0)
        fee = float(r["total_fees"] or r["fee"] or 0.0)
        net = r["net_pnl"]
        if net is None:
            net_val = realized - fee
        else:
            net_val = float(net)
        net_pnls.append(net_val)

        if r["trace_id"]:
            complete_trace += 1
        else:
            missing_trace += 1

    closed_trade_count = len(net_pnls)

    # Decision trace completeness: require trace_id exists AND row exists in decision_traces
    if closed_trade_count > 0:
        with db.connect() as conn:
            traced = conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM trade_fills f
                WHERE f.action='CLOSE'
                  AND (f.bot_instance_id = ? OR (f.bot_instance_id IS NULL AND ? = 'default'))
                  AND f.trace_id IS NOT NULL
                """,
                (bot_instance_id, bot_instance_id),
            ).fetchone()["cnt"]
            joined = conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM trade_fills f
                JOIN decision_traces dt ON dt.trace_id = f.trace_id
                WHERE f.action='CLOSE'
                  AND (f.bot_instance_id = ? OR (f.bot_instance_id IS NULL AND ? = 'default'))
                """,
                (bot_instance_id, bot_instance_id),
            ).fetchone()["cnt"]
        trades_with_complete_traces = int(joined or 0)
        trades_missing_traces = int(closed_trade_count - trades_with_complete_traces)
    else:
        trades_with_complete_traces = 0
        trades_missing_traces = 0

    if paper_start and paper_end:
        paper_days_count = (paper_end.date() - paper_start.date()).days + 1
    else:
        paper_days_count = 0

    consecutive_paper_weeks = _consecutive_weeks_count(close_dates)

    wins = [p for p in net_pnls if p > 0]
    losses = [p for p in net_pnls if p < 0]

    win_rate = round((len(wins) / closed_trade_count), 4) if closed_trade_count else 0.0
    avg_win = round((sum(wins) / len(wins)), 4) if wins else 0.0
    avg_loss = round((abs(sum(losses)) / len(losses)), 4) if losses else 0.0
    loss_rate = 1.0 - win_rate if closed_trade_count else 0.0
    expectancy = round((win_rate * avg_win) - (loss_rate * avg_loss), 6) if closed_trade_count else 0.0

    gross_profit = round(sum(wins), 6) if wins else 0.0
    gross_loss_abs = round(abs(sum(losses)), 6) if losses else 0.0
    net_pnl = round(sum(net_pnls), 6) if net_pnls else 0.0
    profit_factor = round((gross_profit / gross_loss_abs), 6) if gross_loss_abs > 0 else (999.0 if gross_profit > 0 else 0.0)

    # Max drawdown in paper period from equity_snapshots (bot-scoped)
    max_dd = 0.0
    if paper_start and paper_end:
        with db.connect() as conn:
            eq_rows = conn.execute(
                """
                SELECT timestamp_utc, equity
                FROM equity_snapshots
                WHERE (bot_instance_id = ? OR (bot_instance_id IS NULL AND ? = 'default'))
                  AND timestamp_utc >= ? AND timestamp_utc <= ?
                  AND equity IS NOT NULL
                ORDER BY timestamp_utc ASC
                """,
                (bot_instance_id, bot_instance_id, paper_start.isoformat(), paper_end.isoformat()),
            ).fetchall()
        peak = None
        trough = None
        for er in eq_rows:
            eq = float(er["equity"] or 0.0)
            if peak is None or eq > peak:
                peak = eq
                trough = eq
            else:
                trough = min(trough or eq, eq)
                if peak and peak > 0:
                    dd = ((peak - (trough or eq)) / peak) * 100.0
                    max_dd = max(max_dd, dd)
    max_drawdown_pct = round(max_dd, 6)

    # Sizing failures: count SIZING_FAILURE audit events for this bot during paper period
    sizing_failures = 0
    if paper_start and paper_end:
        with db.connect() as conn:
            ev_rows = conn.execute(
                """
                SELECT details_json
                FROM events
                WHERE event_type = 'SIZING_FAILURE'
                  AND timestamp_utc >= ? AND timestamp_utc <= ?
                """,
                (paper_start.isoformat(), paper_end.isoformat()),
            ).fetchall()
        for ev in ev_rows:
            try:
                details = json.loads(ev["details_json"] or "{}")
                if details.get("bot_instance_id") == bot_instance_id:
                    sizing_failures += 1
            except Exception:
                continue

    # Daily close evidence: require per-day trigger events (within paper span)
    daily_close_expected = paper_days_count
    daily_close_success_days: set[str] = set()
    if paper_start and paper_end:
        with db.connect() as conn:
            dc_rows = conn.execute(
                """
                SELECT timestamp_utc, details_json
                FROM events
                WHERE event_type = 'DAILY_PROFIT_CLOSE_TRIGGERED'
                  AND timestamp_utc >= ? AND timestamp_utc <= ?
                """,
                (paper_start.isoformat(), paper_end.isoformat()),
            ).fetchall()
        for r in dc_rows:
            try:
                details = json.loads(r["details_json"] or "{}")
                if details.get("bot_instance_id") != bot_instance_id:
                    continue
            except Exception:
                continue
            try:
                daily_close_success_days.add(_parse_iso(r["timestamp_utc"]).date().isoformat())
            except Exception:
                continue
    daily_close_success = len(daily_close_success_days)
    missing_days: list[str] = []
    if paper_start and paper_end and daily_close_expected > 0:
        d = paper_start.date()
        while d <= paper_end.date():
            key = d.isoformat()
            if key not in daily_close_success_days:
                missing_days.append(key)
            d = d + timedelta(days=1)

    # Signal scheduler evidence: count SIGNAL_GENERATION_COMPLETED events per day+slot
    from app.signals.signal_scheduler_config import SIGNAL_GENERATION_TIMES_UTC

    signal_expected = paper_days_count * len(SIGNAL_GENERATION_TIMES_UTC) if paper_days_count else 0
    signal_success_keys: set[str] = set()
    if paper_start and paper_end:
        with db.connect() as conn:
            sg_rows = conn.execute(
                """
                SELECT timestamp_utc, details_json
                FROM events
                WHERE event_type = 'SIGNAL_GENERATION_COMPLETED'
                  AND timestamp_utc >= ? AND timestamp_utc <= ?
                """,
                (paper_start.isoformat(), paper_end.isoformat()),
            ).fetchall()
        for r in sg_rows:
            try:
                details = json.loads(r["details_json"] or "{}")
                slot = str(details.get("scheduled_time_utc") or "")
                if not slot:
                    continue
                day = _parse_iso(r["timestamp_utc"]).date().isoformat()
                signal_success_keys.add(f"{day}:{slot}")
            except Exception:
                continue
    signal_success = len(signal_success_keys)
    signal_missing: list[str] = []
    if paper_start and paper_end and paper_days_count:
        d = paper_start.date()
        while d <= paper_end.date():
            for slot in SIGNAL_GENERATION_TIMES_UTC:
                key = f"{d.isoformat()}:{slot}"
                if key not in signal_success_keys:
                    signal_missing.append(key)
            d = d + timedelta(days=1)

    blocking: list[str] = []
    if not sections_confirmed:
        blocking.append(RejectionReason.SECTIONS_A_TO_E_NOT_CONFIRMED.value)
    if consecutive_paper_weeks < 3:
        blocking.append(RejectionReason.PAPER_TRADING_PERIOD_TOO_SHORT.value)
    if closed_trade_count < 60:
        blocking.append(RejectionReason.INSUFFICIENT_CLOSED_TRADES.value)
    if trades_missing_traces > 0:
        blocking.append(RejectionReason.MISSING_DECISION_TRACES.value)
    if expectancy <= 0:
        blocking.append(RejectionReason.NEGATIVE_EXPECTANCY.value)
    if max_drawdown_pct > 8.0:
        blocking.append(RejectionReason.MAX_DRAWDOWN_TOO_HIGH.value)
    if profit_factor < 1.3:
        blocking.append(RejectionReason.PROFIT_FACTOR_TOO_LOW.value)
    if sizing_failures > 0:
        blocking.append(RejectionReason.SIZING_FAILURES_DETECTED.value)
    if daily_close_expected > 0 and daily_close_success < daily_close_expected:
        blocking.append(RejectionReason.DAILY_CLOSE_NOT_VALIDATED.value)
    if signal_expected > 0 and signal_success < signal_expected:
        blocking.append(RejectionReason.SIGNAL_SCHEDULER_NOT_VALIDATED.value)

    if closed_trade_count == 0 or consecutive_paper_weeks == 0:
        status = ReadinessStatus.NOT_READY
    elif blocking:
        status = ReadinessStatus.PAPER_VALIDATION_RUNNING
    else:
        # Passing numeric gates is still NOT an auto-approval for user capital.
        status = ReadinessStatus.READY_FOR_CONTROLLED_BETA_REVIEW

    report = UserCapitalReadinessReport(
        sections_a_to_e_deployed=sections_confirmed,
        paper_start_date=paper_start.date().isoformat() if paper_start else None,
        paper_end_date=paper_end.date().isoformat() if paper_end else None,
        paper_days_count=paper_days_count,
        consecutive_paper_weeks=consecutive_paper_weeks,
        closed_trade_count=closed_trade_count,
        trades_with_complete_decision_traces=trades_with_complete_traces,
        trades_missing_decision_traces=trades_missing_traces,
        win_rate=win_rate,
        average_win=avg_win,
        average_loss=avg_loss,
        expectancy_after_fees=expectancy,
        gross_profit=gross_profit,
        gross_loss=gross_loss_abs,
        net_pnl=net_pnl,
        profit_factor=profit_factor,
        max_drawdown_pct=max_drawdown_pct,
        sizing_failure_count=sizing_failures,
        daily_close_expected_count=daily_close_expected,
        daily_close_success_count=daily_close_success,
        daily_close_missing_days=missing_days,
        signal_generation_expected_count=signal_expected,
        signal_generation_success_count=signal_success,
        signal_generation_missing_runs=signal_missing,
        readiness_status=status,
        blocking_reasons=blocking,
        created_at=now_iso,
    )
    return report


def assert_user_capital_activation_allowed(*, db: DB, bot_instance_id: str) -> None:
    report = evaluate_user_capital_readiness(db=db, bot_instance_id=bot_instance_id)
    if report.readiness_status != ReadinessStatus.APPROVED_FOR_CONTROLLED_BETA:
        payload = {
            "status": "REJECTED",
            "reason": RejectionReason.USER_CAPITAL_READINESS_NOT_MET.value,
            "missing_requirements": report.blocking_reasons,
            "readiness_report": report.to_dict(),
        }
        raise UserCapitalReadinessError(payload)

