"""
app/adaptive/smoothing.py

Provides asymmetric exponential moving average (EMA) smoothing for adaptive outputs.
Satisfies Phase 5 Learning Logic Rules: Gradualism and Asymmetry.
"""

from typing import Optional


class AsymmetricEMA:
    """
    Asymmetric Exponential Moving Average.

    Applies different smoothing factors (alpha) depending on whether the target
    is moving up or down relative to the current state.
    
    This fulfills the "Asymmetry" requirement: defensive adaptation (penalization)
    can happen faster than aggressive expansion (recovery).
    """

    def __init__(self, alpha_up: float, alpha_down: float) -> None:
        """
        Initialization.
        
        Args:
            alpha_up: The smoothing factor to apply when target > current_value.
            alpha_down: The smoothing factor to apply when target < current_value.
        """
        if not (0.0 <= alpha_up <= 1.0):
            raise ValueError(f"alpha_up {alpha_up} must be in [0, 1]")
        if not (0.0 <= alpha_down <= 1.0):
            raise ValueError(f"alpha_down {alpha_down} must be in [0, 1]")
            
        self.alpha_up = alpha_up
        self.alpha_down = alpha_down
        self._current: Optional[float] = None

    def update(self, target: float) -> float:
        """
        Updates the EMA with a new target value and returns the smoothed result.
        If this is the first update (cold start), it will instantly snap to the target
        to guarantee deterministic restart safety.
        """
        if self._current is None:
            self._current = float(target)
            return self._current

        alpha = self.alpha_up if target > self._current else self.alpha_down
        
        # Apply EMA formula
        self._current = (target * alpha) + (self._current * (1.0 - alpha))
        return self._current

    @property
    def value(self) -> Optional[float]:
        """Returns the current smoothed value, or None if not initialized."""
        return self._current

    def reset(self) -> None:
        """Resets the state to None."""
        self._current = None
