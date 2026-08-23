from . import sma_cross
from . import trend_pullback
from . import bollinger_reversion
from . import vwap_reversion
from . import donchian_breakout
from . import supertrend
from . import squeeze_breakout
from . import master_ensemble
from . import robust_ensemble
from . import activity_targets

# Activity targets exports for easy access
from .activity_targets import (
    ActivityTargets,
    ActivityTargetsMixin,
    ActivityCheckpoint,
    NudgeResult,
    default_activity_targets,
    conservative_activity_targets,
    moderate_activity_targets,
)

# This ensures all strategies register themselves
