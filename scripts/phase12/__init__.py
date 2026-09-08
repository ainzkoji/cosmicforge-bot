"""Phase 12 — live paper lifecycle validation harness.

This package is deliberately outside ``app``: nothing in the production import
graph may reach the controlled opportunity injector. It exists to drive the
*real* runtime — real ``PaperRunner``, real ``TradingDecisionEngine``, real
risk, feasibility, EntryProtection, ``PaperExecutor`` and ``PositionManager``
— against a deterministic market so a full position lifecycle can be observed
end to end, including across a genuine process restart.

What it does NOT do:

* lower any production threshold (the real engine performs the real comparison
  against the real resolved threshold, and the harness aborts if the controlled
  confidence would not clear it);
* reach a broker (every order-submitting client method raises);
* touch ``bot_a8117dc719fc`` or ``bot_e5fe913972a9``;
* write into the canonical runtime database.
"""
