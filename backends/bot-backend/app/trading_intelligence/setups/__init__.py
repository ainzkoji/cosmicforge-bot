"""Section 11 setup discovery specialists.

Each specialist consumes a pinned MarketSnapshot + MarketState +
RegimeDistribution + its own versioned policy, and returns zero or more
deterministic SetupCandidate objects. Specialists never approve trades,
size positions, submit orders, use account capital/PnL, or consult the old
V2 confidence threshold.
"""
