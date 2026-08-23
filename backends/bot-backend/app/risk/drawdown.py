from __future__ import annotations
from datetime import date
from typing import Optional

from shared_lib.persistence.state_store import StateStore
from app.risk.state import PeriodSnapshot, get_week_start, get_month_start

class DrawdownMonitor:
    def __init__(self, store: StateStore):
        self.store = store

    def update_snapshots(self, day: date, current_equity: float) -> None:
        """
        Updates weekly and monthly snapshots with current equity.
        Creates new snapshots if they don't exist (start of period).
        Updates peak_equity if current_equity is higher.
        """
        if current_equity <= 0:
            return

        # 1. Weekly
        ws_date = get_week_start(day)
        weekly = self.store.load_weekly_snapshot(ws_date)
        if not weekly:
            weekly = PeriodSnapshot(
                start_date=ws_date,
                start_equity=current_equity,
                peak_equity=current_equity,
                low_equity=current_equity
            )
        else:
            if current_equity > weekly.peak_equity:
                weekly.peak_equity = current_equity
            if current_equity < weekly.low_equity:
                weekly.low_equity = current_equity
                
        self.store.save_weekly_snapshot(weekly)

        # 2. Monthly
        ms_date = get_month_start(day)
        monthly = self.store.load_monthly_snapshot(ms_date)
        if not monthly:
            monthly = PeriodSnapshot(
                start_date=ms_date,
                start_equity=current_equity,
                peak_equity=current_equity,
                low_equity=current_equity
            )
        else:
            if current_equity > monthly.peak_equity:
                monthly.peak_equity = current_equity
            if current_equity < monthly.low_equity:
                monthly.low_equity = current_equity
                
        self.store.save_monthly_snapshot(monthly)

    def check_weekly_drawdown(self, max_loss_pct: float, current_equity: float, snapshot: Optional[PeriodSnapshot]) -> bool:
        """
        Returns True if drawdown from WEEKLY PEAK exceeds max_loss_pct.
        """
        if max_loss_pct <= 0 or not snapshot or snapshot.peak_equity <= 0:
            return False
            
        # Drawdown amount
        dd_amount = snapshot.peak_equity - current_equity
        if dd_amount <= 0:
            return False
            
        dd_pct = (dd_amount / snapshot.peak_equity) * 100.0
        return dd_pct >= max_loss_pct

    def check_monthly_drawdown(self, max_loss_pct: float, current_equity: float, snapshot: Optional[PeriodSnapshot]) -> bool:
        """
        Returns True if drawdown from MONTHLY PEAK exceeds max_loss_pct.
        """
        if max_loss_pct <= 0 or not snapshot or snapshot.peak_equity <= 0:
            return False
            
        dd_amount = snapshot.peak_equity - current_equity
        if dd_amount <= 0:
            return False
            
        dd_pct = (dd_amount / snapshot.peak_equity) * 100.0
        return dd_pct >= max_loss_pct
