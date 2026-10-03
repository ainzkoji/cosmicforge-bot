"""Historical strategy modules are explicitly imported by research/tests only."""
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

# Legacy auto-registration is intentionally removed.
