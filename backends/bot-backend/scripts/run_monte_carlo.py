import random
import math
import statistics
import time
import concurrent.futures
from dataclasses import dataclass
from typing import List, Tuple, Dict, Any
import json
import os

# =============================================================================
# MONTE CARLO SURVIVABILITY MODEL
# =============================================================================

@dataclass
class SimulationParams:
    # Core R-Distribution Parameters
    win_rate: float            # e.g., 0.45 (45%)
    avg_win_r: float           # e.g., 1.5R
    avg_loss_r: float          # e.g., 1.0R
    win_r_std: float           # Std deviation of wins, e.g. 0.5R
    loss_r_std: float          # Std deviation of losses, e.g. 0.2R
    
    # Capital & Risk Settings
    starting_capital: float    # e.g., 10000.0
    base_risk_pct: float       # e.g., 0.01 (1% risk per trade)
    slippage_penalty_pct: float # e.g., 0.05 (adds 5% onto loss risk theoretically, or straight subtraction)
    
    # Dynamic Position Sizing & Safety Engines
    kill_switch_drawdown_pct: float # e.g., 0.30 (30% max absolute drawdown before system halts)
    loss_streak_compression_start: int # Start reducing size after N consecutive losses
    loss_streak_multiplier: float   # Reduce risk by this factor per subsequent loss (e.g. 0.8)
    
    # Stress Scenarios Overrides
    force_consecutive_losses: int = 0
    volatility_regime_shift: bool = False # halves win-rate midway
    
    # Simulation Settings
    num_trades_per_path: int = 1000
    num_iterations: int = 10000

@dataclass
class PathResult:
    final_equity: float
    max_equity: float
    max_drawdown_pct: float
    is_ruined: bool            # True if hit kill_switch or zeroed
    recovery_time_max: int     # Max trades spent in drawdown
    equity_curve: List[float]

def simulate_trade(params: SimulationParams, is_win: bool) -> float:
    """Generate a random R-multiple outcome preserving the distribution."""
    if is_win:
        # Generate win R-multiple (ensure it's at least slightly positive)
        r = random.gauss(params.avg_win_r, params.win_r_std)
        return max(0.1, r)
    else:
        # Generate loss R-multiple (loss is 1R nominally, but can vary)
        r = random.gauss(params.avg_loss_r, params.loss_r_std)
        # Apply slippage directly to the realized loss. E.g. nominal 1R loss becomes 1.05R
        r = r * (1.0 + params.slippage_penalty_pct)
        return -max(0.1, r)

def run_single_path(params: SimulationParams) -> PathResult:
    """Runs a single equity curve path simulating dynamic risks and sizing."""
    equity = params.starting_capital
    peak_equity = equity
    max_dd_pct = 0.0
    is_ruined = False
    
    current_dd_trades = 0
    max_recovery_trades = 0
    consecutive_losses = 0
    
    # Optional: We only save full curve for the worst-case path analysis, 
    # but storing 1000 floats * 10,000 arrays = ~80MB, totally fine.
    curve = [equity]
    
    for i in range(1, params.num_trades_per_path + 1):
        if is_ruined:
            break
            
        # Check Kill Switch
        current_dd = (peak_equity - equity) / peak_equity
        if current_dd >= params.kill_switch_drawdown_pct or equity <= 0:
            is_ruined = True
            max_dd_pct = max(max_dd_pct, current_dd)
            break
            
        # 1. Determine Win Rate (Regime Shift Stress Scenario)
        current_win_rate = params.win_rate
        if params.volatility_regime_shift and i > params.num_trades_per_path // 2:
            current_win_rate *= 0.5 # Halve the win rate during regime shift
            
        # 2. Determine Outcome
        is_win = random.random() < current_win_rate
        
        # Stress Scenario: Force Initial Consecutive Losses
        if i <= params.force_consecutive_losses:
            is_win = False
            
        # 3. Dynamic Position Sizing
        current_risk_pct = params.base_risk_pct
        if consecutive_losses >= params.loss_streak_compression_start:
            # Apply multiplier exponentially based on streak depth
            depth = consecutive_losses - params.loss_streak_compression_start + 1
            current_risk_pct = current_risk_pct * (params.loss_streak_multiplier ** depth)
            
        amount_at_risk = equity * current_risk_pct
        
        # 4. Execute Trade Outcome
        r_multiple = simulate_trade(params, is_win)
        trade_pnl = amount_at_risk * r_multiple
        
        equity += trade_pnl
        curve.append(equity)
        
        # 5. Update Tracking Metrics
        if not is_win:
            consecutive_losses += 1
        else:
            consecutive_losses = 0
            
        if equity > peak_equity:
            peak_equity = equity
            max_recovery_trades = max(max_recovery_trades, current_dd_trades)
            current_dd_trades = 0
        else:
            current_dd_trades += 1
            dd_pct = (peak_equity - equity) / peak_equity
            max_dd_pct = max(max_dd_pct, dd_pct)

    # Final recovery capture if still in drawdown
    max_recovery_trades = max(max_recovery_trades, current_dd_trades)
    
    return PathResult(
        final_equity=equity,
        max_equity=peak_equity,
        max_drawdown_pct=max_dd_pct,
        is_ruined=is_ruined,
        recovery_time_max=max_recovery_trades,
        equity_curve=curve
    )

def analyze_paths(results: List[PathResult], title: str, starting_cap: float) -> Dict[str, Any]:
    """Calculate aggregate statistics from the simulation results."""
    total_paths = len(results)
    ruin_count = sum(1 for r in results if r.is_ruined)
    ruin_prob = ruin_count / total_paths
    
    drawdowns = sorted([r.max_drawdown_pct for r in results])
    dd_95th = drawdowns[int(total_paths * 0.95)] if total_paths > 0 else 0.0
    
    prob_20pct_dd = sum(1 for r in results if r.max_drawdown_pct >= 0.20) / total_paths
    
    avg_recovery = statistics.mean([r.recovery_time_max for r in results])
    max_recovery = max((r.recovery_time_max for r in results), default=0)
    
    # Find the worst-case curve based on largest absolute drawdown
    worst_path = max(results, key=lambda r: r.max_drawdown_pct)
    
    # Calculate Survivability Rating
    survivability_rating = "A+"
    if ruin_prob > 0: survivability_rating = "F"
    elif dd_95th > 0.40: survivability_rating = "D"
    elif dd_95th > 0.30: survivability_rating = "C"
    elif dd_95th > 0.20: survivability_rating = "B"
    elif dd_95th > 0.15: survivability_rating = "A"
    
    # Recommend max risk based on matching 95th %ile DD to 25% max
    recommended_risk_adj = 0.25 / dd_95th if dd_95th > 0 else 1.0
    
    return {
        "title": title,
        "ruin_probability": ruin_prob,
        "95th_percentile_max_dd": dd_95th,
        "prob_20pct_dd": prob_20pct_dd,
        "avg_recovery_trades": avg_recovery,
        "max_recovery_trades": max_recovery,
        "survivability_rating": survivability_rating,
        "recommended_risk_multiplier": recommended_risk_adj,
        "worst_case_curve": worst_path.equity_curve
    }

def print_report(analysis: Dict[str, Any], params: SimulationParams):
    print(f"\n{'='*60}")
    print(f" SCENARIO: {analysis['title']}")
    print(f" {'='*60}")
    print(f" Total Paths Run        : {params.num_iterations:,}")
    print(f" Trades Per Path        : {params.num_trades_per_path}")
    print(f" Base Risk Per Trade    : {params.base_risk_pct*100:.2f}%")
    print("-" * 60)
    print(f" 1. Ruin Probability    : {analysis['ruin_probability']*100:.2f}%")
    if analysis['ruin_probability'] > 0:
        print("    [!] SYSTEM RISK CRITICAL - Kill switch triggered in Monte Carlo")
    print(f" 2. 95th %ile Max DD    : {analysis['95th_percentile_max_dd']*100:.2f}%")
    print(f" 3. Prob >20% Drawdown  : {analysis['prob_20pct_dd']*100:.2f}%")
    print(f" 4. Avg/Max Recovery    : {analysis['avg_recovery_trades']:.0f} / {analysis['max_recovery_trades']} trades")
    print(f" 5. Survivability Rating: {analysis['survivability_rating']}")
    
    rec_risk = params.base_risk_pct * analysis['recommended_risk_multiplier']
    print(f" 6. Recommended Max Risk: {rec_risk*100:.2f}%")
    
    # Quick text-based render of the worst case equity curve
    if min(analysis['worst_case_curve']) > 0:
        print("\n\tWorst-Case Equity Curve Summary:")
        curve = analysis['worst_case_curve']
        start = curve[0]
        trough = min(curve)
        end = curve[-1]
        print(f"\tStart: ${start:,.2f}  |  Trough: ${trough:,.2f}  |  End: ${end:,.2f}")

def run_simulation() -> None:
    print("Initializing Monte Carlo Engine...")
    t0 = time.time()
    
    base_params = SimulationParams(
        win_rate=0.45,
        avg_win_r=1.5,
        avg_loss_r=1.0,
        win_r_std=0.5,
        loss_r_std=0.2,
        starting_capital=10000.0,
        base_risk_pct=0.01,
        slippage_penalty_pct=0.10, # 10% slippage on losses
        kill_switch_drawdown_pct=0.30,
        loss_streak_compression_start=3, # start reducing after 3 losses
        loss_streak_multiplier=0.8,
        num_trades_per_path=500,
        num_iterations=10000
    )
    
    scenarios = [
        ("Base Case", base_params),
        ("10 Consecutive Losses", SimulationParams(**{**base_params.__dict__, "force_consecutive_losses": 10})),
        ("20 Consecutive Losses", SimulationParams(**{**base_params.__dict__, "force_consecutive_losses": 20})),
        ("2x Normal Slippage", SimulationParams(**{**base_params.__dict__, "slippage_penalty_pct": 0.20})),
        ("Volatility Regime Shift", SimulationParams(**{**base_params.__dict__, "volatility_regime_shift": True}))
    ]
    
    for title, params in scenarios:
        # Run paths in parallel utilizing full CPU
        with concurrent.futures.ProcessPoolExecutor() as executor:
            # Send duplicate parameters to match iteration count
            futures = [executor.submit(run_single_path, params) for _ in range(params.num_iterations)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
            
        analysis = analyze_paths(results, title, params.starting_capital)
        print_report(analysis, params)
        
        # Save worst-case curve to a CSV for that scenario
        safe_title = title.replace(" ", "_").lower()
        if not os.path.exists("logs/monte_carlo"):
            os.makedirs("logs/monte_carlo", exist_ok=True)
        with open(f"logs/monte_carlo/worst_case_{safe_title}.csv", "w") as f:
            f.write("Trade,Equity\n")
            for i, val in enumerate(analysis['worst_case_curve']):
                f.write(f"{i},{val:.2f}\n")

    t1 = time.time()
    print(f"\n============================================================")
    print(f"Simulation completed in {t1-t0:.2f} seconds.")
    print(f"Results saved to logs/monte_carlo/")
    print(f"============================================================")

if __name__ == "__main__":
    run_simulation()
