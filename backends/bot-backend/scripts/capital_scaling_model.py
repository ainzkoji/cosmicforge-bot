"""
CAPITAL SCALING & EXPOSURE MODEL
=================================
Simulates performance across capital tiers with realistic fee/slippage scaling,
liquidity stress, and institutional stress tests.

Tiers: $1,000 | $5,000 | $10,000 | $50,000 | $100,000+

Run:
    python scripts/capital_scaling_model.py
"""
import random
import math
import statistics
import time
import os
import json
import concurrent.futures
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

# =============================================================================
# CONSTANTS — Based on Binance USDT-M Futures fee structure
# =============================================================================
MAKER_FEE = 0.0002       # 0.02% per side
TAKER_FEE = 0.0004       # 0.04% per side (market orders)
ROUND_TRIP_FEE = TAKER_FEE * 2  # Entry + Exit, both market orders

# Simulate ~500 trades over 6 months (avg 2.5 trades/day)
NUM_TRADES_PER_PATH = 500
NUM_ITERATIONS = 10_000

# Kill-switch at 30% peak-to-trough drawdown
KILL_SWITCH_DD = 0.30

# Base Monte Carlo parameters (matched to live system risk profile)
WIN_RATE = 0.48
AVG_WIN_R = 1.45
AVG_LOSS_R = 1.00
WIN_R_STD = 0.50
LOSS_R_STD = 0.25
BASE_RISK_PCT = 0.01          # 1% risk per trade
LOSS_STREAK_COMPRESSION = 3   # Start compressing after 3 consecutive losses
LOSS_STREAK_MULT = 0.80       # Reduce risk by 20% per added loss in streak


# =============================================================================
# TIER DEFINITIONS
# =============================================================================
@dataclass
class CapitalTier:
    name: str
    capital: float
    # Slippage scales with size — small accounts have tighter fills, large get slippage
    slippage_pct: float
    # Leverage recommendation per tier (lower for larger capital = more conservative)
    recommended_leverage: float
    # Hard leverage cap for the tier
    max_leverage: float
    # Min notional per trade (Binance minimum is $5 notional)
    min_notional_usdt: float = 5.0
    # Notes for the output report
    notes: str = ""


CAPITAL_TIERS = [
    CapitalTier(
        name="$1,000",
        capital=1_000.0,
        slippage_pct=0.0005,   # Near-zero slippage at small size
        recommended_leverage=3.0,
        max_leverage=5.0,
        notes="Min notional constraint bites more at this tier. Works best with 1R = ~$10."
    ),
    CapitalTier(
        name="$5,000",
        capital=5_000.0,
        slippage_pct=0.0008,
        recommended_leverage=5.0,
        max_leverage=10.0,
        notes="Sweet spot for retail. Slippage nearly negligible."
    ),
    CapitalTier(
        name="$10,000",
        capital=10_000.0,
        slippage_pct=0.0012,
        recommended_leverage=5.0,
        max_leverage=10.0,
        notes="Optimal tier. Full fee/slippage coverage from expected alpha."
    ),
    CapitalTier(
        name="$50,000",
        capital=50_000.0,
        slippage_pct=0.0025,   # Slippage begins to meaningful at this size
        recommended_leverage=3.0,
        max_leverage=5.0,
        notes="Slippage starts materially impacting P&L. Leverage should be reduced."
    ),
    CapitalTier(
        name="$100,000+",
        capital=100_000.0,
        slippage_pct=0.0050,   # 0.5% slippage on large market orders
        recommended_leverage=2.0,
        max_leverage=3.0,
        notes="Institutional range. Market impact is significant. Low leverage required."
    ),
]


# =============================================================================
# INSTITUTIONAL STRESS SCENARIOS
# =============================================================================
@dataclass
class StressScenario:
    name: str
    extra_slippage_pct: float = 0.0    # Additional slippage on top of tier baseline
    partial_fill_pct: float = 1.0       # 1.0 = full fill, 0.7 = 70% partial fill
    spread_widening_mult: float = 1.0   # Multiply base spread by this factor
    liquidity_gap: bool = False         # If True, one random trade per path hits a gap
    liquidity_gap_extra_loss_r: float = 0.0  # Extra R loss due to gap slippage


STRESS_SCENARIOS = [
    StressScenario(name="Normal"),
    StressScenario(
        name="Large Position Partial Fill",
        partial_fill_pct=0.75,          # 75% of intended size gets filled
    ),
    StressScenario(
        name="Liquidity Gap",
        liquidity_gap=True,
        liquidity_gap_extra_loss_r=0.5, # One trade per path has 0.5R extra adverse slippage
    ),
    StressScenario(
        name="Spread Widening (3x)",
        spread_widening_mult=3.0,
        extra_slippage_pct=0.002,       # 0.2% additional per trade during crypto volatility
    ),
]


# =============================================================================
# CORE SIMULATION ENGINE
# =============================================================================
@dataclass
class TradeResult:
    pnl: float
    is_win: bool

@dataclass
class PathResult:
    final_equity: float
    max_drawdown_pct: float
    is_ruined: bool
    monthly_returns: List[float]
    equity_curve: List[float]
    recovery_time_max: int


def simulate_single_trade(
    equity: float,
    risk_pct: float,
    win_rate: float,
    tier: CapitalTier,
    stress: StressScenario,
    force_loss: bool = False,
    is_gap_trade: bool = False,
) -> TradeResult:
    """Simulate a single trade with realistic fee + slippage deduction."""
    is_win = (random.random() < win_rate) and not force_loss

    # R-multiple draw from Gaussian distribution
    if is_win:
        r = max(0.1, random.gauss(AVG_WIN_R, WIN_R_STD))
    else:
        r = max(0.1, random.gauss(AVG_LOSS_R, LOSS_R_STD))
        if is_gap_trade:
            r += stress.liquidity_gap_extra_loss_r

    amount_at_risk = equity * risk_pct

    # Apply partial fill: less capital deployed = proportionally less risk realized
    fill_pct = stress.partial_fill_pct
    effective_amount = amount_at_risk * fill_pct

    # Fees: round-trip taker fee on notional
    # Assume leverage=1 for fee calc simplicity (fee on position value is separate from PnL)
    fee_cost = effective_amount * ROUND_TRIP_FEE

    # Slippage: tier baseline + spread widening + extra stress
    total_slippage = (
        tier.slippage_pct * stress.spread_widening_mult
        + stress.extra_slippage_pct
    )
    slippage_cost = effective_amount * total_slippage

    # Net PnL
    if is_win:
        gross_pnl = effective_amount * r
        net_pnl = gross_pnl - fee_cost - slippage_cost
    else:
        gross_pnl = -effective_amount * r
        net_pnl = gross_pnl - fee_cost - slippage_cost  # fees/slippage compound losses

    return TradeResult(pnl=net_pnl, is_win=is_win)


def run_path(tier: CapitalTier, stress: StressScenario) -> PathResult:
    """Run a single equity curve path under the given tier + stress scenario."""
    equity = tier.capital
    peak_equity = equity
    max_dd_pct = 0.0
    is_ruined = False
    consecutive_losses = 0
    
    curve = [equity]
    monthly_returns: List[float] = []
    trades_this_month = 0
    month_start_equity = equity
    
    current_dd_trades = 0
    max_recovery_trades = 0
    
    # Randomly pick one trade per path to be a liquidity gap trade (if stress scenario)
    gap_trade_idx = random.randint(0, NUM_TRADES_PER_PATH - 1) if stress.liquidity_gap else -1

    for i in range(NUM_TRADES_PER_PATH):
        if is_ruined:
            break

        # Kill-switch check
        current_dd = (peak_equity - equity) / peak_equity
        if current_dd >= KILL_SWITCH_DD or equity <= 0:
            is_ruined = True
            max_dd_pct = max(max_dd_pct, current_dd)
            break

        # Dynamic risk sizing (loss streak compression)
        risk_pct = BASE_RISK_PCT
        if consecutive_losses >= LOSS_STREAK_COMPRESSION:
            depth = consecutive_losses - LOSS_STREAK_COMPRESSION + 1
            risk_pct = risk_pct * (LOSS_STREAK_MULT ** depth)

        # Execute trade
        result = simulate_single_trade(
            equity=equity,
            risk_pct=risk_pct,
            win_rate=WIN_RATE,
            tier=tier,
            stress=stress,
            is_gap_trade=(i == gap_trade_idx),
        )

        equity += result.pnl
        curve.append(equity)

        # Tracking
        if result.is_win:
            consecutive_losses = 0
        else:
            consecutive_losses += 1

        if equity > peak_equity:
            peak_equity = equity
            max_recovery_trades = max(max_recovery_trades, current_dd_trades)
            current_dd_trades = 0
        else:
            current_dd_trades += 1
            dd_pct = (peak_equity - equity) / peak_equity
            max_dd_pct = max(max_dd_pct, dd_pct)

        # Monthly return tracking (~21 trades = 1 month at 1 trade/day)
        trades_this_month += 1
        if trades_this_month >= 21:
            monthly_ret = (equity - month_start_equity) / month_start_equity
            monthly_returns.append(monthly_ret)
            month_start_equity = equity
            trades_this_month = 0

    max_recovery_trades = max(max_recovery_trades, current_dd_trades)

    return PathResult(
        final_equity=equity,
        max_drawdown_pct=max_dd_pct,
        is_ruined=is_ruined,
        monthly_returns=monthly_returns,
        equity_curve=curve,
        recovery_time_max=max_recovery_trades,
    )


# =============================================================================
# ANALYSIS & REPORTING
# =============================================================================
@dataclass
class TierStressAnalysis:
    tier_name: str
    stress_name: str
    ruin_probability: float
    p95_max_dd: float
    prob_20pct_dd: float
    avg_monthly_return: float
    median_monthly_return: float
    survivability_rating: str
    recommended_risk_pct: float
    worst_start: float
    worst_trough: float
    worst_end: float


def analyze(
    results: List[PathResult],
    tier: CapitalTier,
    stress: StressScenario,
) -> TierStressAnalysis:
    total = len(results)
    ruin_count = sum(1 for r in results if r.is_ruined)
    ruin_prob = ruin_count / total

    drawdowns = sorted(r.max_drawdown_pct for r in results)
    p95_dd = drawdowns[int(total * 0.95)]
    prob_20 = sum(1 for r in results if r.max_drawdown_pct >= 0.20) / total

    all_monthly = [m for r in results for m in r.monthly_returns]
    avg_monthly = statistics.mean(all_monthly) if all_monthly else 0.0
    med_monthly = statistics.median(all_monthly) if all_monthly else 0.0

    # Survivability rating
    if ruin_prob > 0.01:
        rating = "F"
    elif p95_dd > 0.40:
        rating = "D"
    elif p95_dd > 0.30:
        rating = "C"
    elif p95_dd > 0.20:
        rating = "B"
    elif p95_dd > 0.12:
        rating = "A"
    else:
        rating = "A+"

    # Recommended risk to target 95th-pct DD <= 20%
    if p95_dd > 0:
        recommended_risk = BASE_RISK_PCT * (0.20 / p95_dd)
        recommended_risk = min(recommended_risk, 0.025)  # hard cap at 2.5%
    else:
        recommended_risk = BASE_RISK_PCT

    # Worst case equity curve
    worst = max(results, key=lambda r: r.max_drawdown_pct)
    w_curve = worst.equity_curve
    return TierStressAnalysis(
        tier_name=tier.name,
        stress_name=stress.name,
        ruin_probability=ruin_prob,
        p95_max_dd=p95_dd,
        prob_20pct_dd=prob_20,
        avg_monthly_return=avg_monthly,
        median_monthly_return=med_monthly,
        survivability_rating=rating,
        recommended_risk_pct=recommended_risk,
        worst_start=w_curve[0],
        worst_trough=min(w_curve),
        worst_end=w_curve[-1],
    )


def print_tier_header(tier: CapitalTier):
    print()
    print("=" * 65)
    print(f"  CAPITAL TIER: {tier.name}  (Recommended Leverage: {tier.recommended_leverage:.0f}x, Max: {tier.max_leverage:.0f}x)")
    print(f"  {tier.notes}")
    print("=" * 65)


def print_analysis(a: TierStressAnalysis):
    print(f"\n  [{a.stress_name}]")
    print(f"    Ruin Probability      : {a.ruin_probability*100:.2f}%")
    print(f"    95th %ile Max DD      : {a.p95_max_dd*100:.1f}%")
    print(f"    Prob >20% Drawdown    : {a.prob_20pct_dd*100:.1f}%")
    print(f"    Avg Monthly Return    : {a.avg_monthly_return*100:.2f}%")
    print(f"    Median Monthly Return : {a.median_monthly_return*100:.2f}%")
    print(f"    Survivability Rating  : {a.survivability_rating}")
    print(f"    Recommended Max Risk  : {a.recommended_risk_pct*100:.2f}%")
    print(f"    Worst Curve           : ${a.worst_start:,.0f} → ${a.worst_trough:,.0f} → ${a.worst_end:,.0f}")


# =============================================================================
# MAIN RUNNER
# =============================================================================
def run():
    t0 = time.time()
    print("=" * 65)
    print("  COSMICFORGE — CAPITAL SCALING & EXPOSURE MODEL")
    print(f"  Paths per scenario: {NUM_ITERATIONS:,}  |  Trades per path: {NUM_TRADES_PER_PATH}")
    print("=" * 65)

    all_analyses: List[TierStressAnalysis] = []

    for tier in CAPITAL_TIERS:
        print_tier_header(tier)
        for stress in STRESS_SCENARIOS:
            with concurrent.futures.ProcessPoolExecutor() as executor:
                futures = [executor.submit(run_path, tier, stress) for _ in range(NUM_ITERATIONS)]
                results = [f.result() for f in concurrent.futures.as_completed(futures)]

            analysis = analyze(results, tier, stress)
            all_analyses.append(analysis)
            print_analysis(analysis)

    # -------------------------------------------------------------------------
    # CONSOLIDATED OUTPUT TABLE
    # -------------------------------------------------------------------------
    print()
    print("=" * 65)
    print("  SUMMARY: SAFE CAPITAL SCALING LIMITS (Normal Scenario)")
    print("=" * 65)
    print(f"  {'Tier':<14} {'Rating':<8} {'95th DD':<10} {'Avg/Mo Return':<16} {'Rec. Risk':<10} {'Rec. Leverage'}")
    print("  " + "-" * 62)
    for a in all_analyses:
        if a.stress_name == "Normal":
            print(f"  {a.tier_name:<14} {a.survivability_rating:<8} {a.p95_max_dd*100:<9.1f}% {a.avg_monthly_return*100:<15.2f}% {a.recommended_risk_pct*100:<9.2f}%")

    # -------------------------------------------------------------------------
    # INSTITUTIONAL STRESS TABLE
    # -------------------------------------------------------------------------
    print()
    print("=" * 65)
    print("  INSTITUTIONAL STRESS IMPACT (at $100,000+ Tier)")
    print("=" * 65)
    print(f"  {'Scenario':<35} {'Ruin%':<8} {'95th DD':<10} {'Rating'}")
    print("  " + "-" * 60)
    for a in all_analyses:
        if a.tier_name == "$100,000+":
            print(f"  {a.stress_name:<35} {a.ruin_probability*100:<8.2f} {a.p95_max_dd*100:<9.1f}% {a.survivability_rating}")

    # -------------------------------------------------------------------------
    # LEVERAGE ADJUSTMENT RULES
    # -------------------------------------------------------------------------
    print()
    print("=" * 65)
    print("  LEVERAGE ADJUSTMENT RULES PER TIER")
    print("=" * 65)
    for tier in CAPITAL_TIERS:
        print(f"  {tier.name:<14}  Recommended: {tier.recommended_leverage:.0f}x   Hard Cap: {tier.max_leverage:.0f}x")

    # -------------------------------------------------------------------------
    # SAVE JSON
    # -------------------------------------------------------------------------
    os.makedirs("logs/capital_scaling", exist_ok=True)
    output = [
        {
            "tier": a.tier_name,
            "stress": a.stress_name,
            "ruin_probability": round(a.ruin_probability, 4),
            "p95_max_dd": round(a.p95_max_dd, 4),
            "prob_20pct_dd": round(a.prob_20pct_dd, 4),
            "avg_monthly_return": round(a.avg_monthly_return, 4),
            "survivability_rating": a.survivability_rating,
            "recommended_risk_pct": round(a.recommended_risk_pct, 4),
        }
        for a in all_analyses
    ]
    with open("logs/capital_scaling/results.json", "w") as f:
        json.dump(output, f, indent=2)

    elapsed = time.time() - t0
    print()
    print("=" * 65)
    print(f"  Simulation complete in {elapsed:.1f}s.")
    print(f"  Results saved → logs/capital_scaling/results.json")
    print("=" * 65)


if __name__ == "__main__":
    run()
