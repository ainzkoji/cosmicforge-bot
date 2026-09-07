import math
import random
from typing import List, Dict, Any, Tuple
from statistics import mean, stdev

def monte_carlo_drawdown_ci(
    pnl_series: List[float], 
    initial_capital: float, 
    iterations: int = 1000, 
    confidence_level: float = 0.99
) -> Dict[str, float]:
    """
    Perform Monte Carlo resampling on a sequence of PnL values to generate
    a statistical distribution of Maximum Drawdowns and calculate a 
    confidence interval.
    """
    if not pnl_series:
        return {
            "mean_max_drawdown_pct": 0.0,
            "worst_max_drawdown_pct": 0.0,
            f"ci_{int(confidence_level*100)}_max_drawdown_pct": 0.0
        }

    max_dds = []
    n_trades = len(pnl_series)
    
    for _ in range(iterations):
        # Sample with replacement
        sampled_pnls = [random.choice(pnl_series) for _ in range(n_trades)]
        
        equity = initial_capital
        peak = initial_capital
        max_dd = 0.0
        
        for pnl in sampled_pnls:
            equity += pnl
            if equity > peak:
                peak = equity
            
            if peak > 0:
                dd_pct = ((peak - equity) / peak) * 100.0
                if dd_pct > max_dd:
                    max_dd = dd_pct
                    
        max_dds.append(max_dd)

    # Sort to find confidence intervals
    max_dds.sort()
    
    # Example: for 99% CI, we want the 99th percentile worst drawdown.
    # Since we sorted ascending, index = int(iterations * 0.99)
    idx = int(iterations * confidence_level) - 1
    idx = max(0, min(idx, iterations - 1))
    
    ci_val = max_dds[idx]
    
    return {
        "mean_max_drawdown_pct": mean(max_dds),
        "worst_max_drawdown_pct": max_dds[-1],
        f"ci_{int(confidence_level*100)}_max_drawdown_pct": ci_val
    }

def generate_regime_segmented_report(regime_pnl: Dict[str, List[float]]) -> Dict[str, Any]:
    """
    Generate a detailed performance report broken down by market regime.
    """
    report = {}
    total_trades = sum(len(trades) for trades in regime_pnl.values())
    
    for regime, trades in regime_pnl.items():
        count = len(trades)
        if count == 0:
            continue
            
        wins = [p for p in trades if p > 0]
        losses = [p for p in trades if p <= 0]
        
        win_rate = len(wins) / count
        avg_w = sum(wins) / len(wins) if wins else 0.0
        avg_l = sum(losses) / len(losses) if losses else 0.0
        
        expectancy = (win_rate * avg_w) + ((1.0 - win_rate) * avg_l)
        
        report[regime] = {
            "trade_count": count,
            "trade_frequency_pct": (count / total_trades) * 100 if total_trades > 0 else 0.0,
            "win_rate": win_rate,
            "avg_win": avg_w,
            "avg_loss": avg_l,
            "expectancy": expectancy,
            "net_pnl": sum(trades)
        }
        
    return report

class WalkForwardTester:
    """
    Standard Walk-Forward Testing framework.
    """
    def __init__(self, data_length: int, num_windows: int = 4, oos_pct: float = 0.25):
        self.data_length = data_length
        self.num_windows = num_windows
        self.oos_pct = oos_pct
        
    def get_windows(self) -> List[Dict[str, Tuple[int, int]]]:
        """
        Calculates window indices for Walk-Forward Optimization/Testing.
        Returns a list of dicts with 'train' and 'test' indices (start, end).
        """
        windows = []
        if self.num_windows < 1:
            return windows
            
        # Total size of one full train+test window block
        block_size = self.data_length // self.num_windows
        test_size = int(block_size * self.oos_pct)
        train_size = block_size - test_size
        
        for i in range(self.num_windows):
            start = i * block_size
            train_end = start + train_size
            test_end = start + block_size
            
            # Bound check
            test_end = min(test_end, self.data_length)
            
            windows.append({
                "train": (start, train_end),
                "test": (train_end, test_end)
            })
            
        return windows

