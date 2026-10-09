"""CATI strategy families defined by a registered research mandate.

A family here is a set of PURE rule functions plus the frozen parameters they run on. The research evaluator
(``research/evaluator``) and the future Step 3 pipeline import the same functions, so a daily target computed
in research and one computed in the pipeline cannot drift apart.

This package is deliberately separate from ``setups`` (the four forecast-driven specialists of the Section 22
route): registering a family here creates no specialist, no entry authority and no governance change.
"""
