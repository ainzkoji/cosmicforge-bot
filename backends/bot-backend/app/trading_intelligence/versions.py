"""Immutable version/schema identity for every CATI analytical record.

P10 (VERSION EVERYTHING): a canonical hash is only meaningful if the schema
and engine that produced it are pinned. Never use a mutable string such as
``"latest"`` as analytical identity -- bump the relevant constant instead,
the same way ``app/threshold/policy.py`` bumps ``POLICY_VERSION`` whenever a
field's *meaning* changes, not merely its value.

Only the versions needed for Sections 8-10 are defined here. Later
components (setup policies, forecast library, cost model, admission policy,
portfolio policy) get their own version constants in their own modules when
they are built -- do not pre-declare speculative versions here.
"""
from __future__ import annotations

#: Identity/venue mapping shape (app/trading_intelligence/contracts/instrument.py).
INSTRUMENT_SCHEMA_VERSION = "1.0.0"

#: Capability/data-manifest shape (app/trading_intelligence/contracts/data_quality.py).
DATA_MANIFEST_SCHEMA_VERSION = "1.0.0"

#: MarketState contract shape (app/trading_intelligence/contracts/market_state.py).
#: Bump when a field is added, removed or reinterpreted.
MARKET_STATE_SCHEMA_VERSION = "1.0.0"

#: The engine that computes MarketState from causal inputs
#: (app/trading_intelligence/market_state/engine.py). Bump when a feature
#: calculation changes in a way that would change output for identical input.
MARKET_STATE_ENGINE_VERSION = "1.0.0"

#: RegimeDistribution contract shape (app/trading_intelligence/regime/contracts.py).
REGIME_SCHEMA_VERSION = "1.0.0"

#: The deterministic V1 regime engine (app/trading_intelligence/regime/engine.py).
REGIME_MODEL_VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# Section 6/7 architectural-closure contracts (Part A). Shapes only -- no
# business logic version is needed yet because no engine produces these from
# analytical inputs; they are hand-assembled by future callers.
# ---------------------------------------------------------------------------

#: AccountExposureSnapshot / ExposureRecord shape (contracts/exposure.py).
EXPOSURE_SNAPSHOT_SCHEMA_VERSION = "1.0.0"

#: AccountPortfolioReservation shape (contracts/portfolio.py).
PORTFOLIO_RESERVATION_SCHEMA_VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# Section 11 -- Setup Discovery Specialists
# ---------------------------------------------------------------------------

#: SetupCandidate contract shape (contracts/setup.py). Bump when a field is
#: added/removed/reinterpreted -- independent of any one specialist's own
#: setup_version, which versions that specialist's *discovery logic*.
SETUP_CANDIDATE_SCHEMA_VERSION = "1.0.0"

#: SetupSpecialistPolicy contract shape (setups/policy.py).
SETUP_POLICY_SCHEMA_VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# Section 12 -- Trade Forecast and Outcome Distribution
# ---------------------------------------------------------------------------

#: SetupOutcomeLabel contract shape, including the same-bar stop-before-target
#: conservative rule (contracts/forecast.py / forecast/labels.py). Bumping
#: this invalidates every previously-labeled historical row.
LABEL_POLICY_VERSION = "1.0.0"

#: Research cost-labeling model version (forecast/labels.py cost columns).
#: Distinct from Section 13's live/venue CostEstimate model version below --
#: this one labels *historical* rows, that one estimates a *current* trade's
#: cost.
RESEARCH_COST_MODEL_VERSION = "1.0.0"

#: OutcomeForecast contract shape (contracts/forecast.py).
OUTCOME_FORECAST_SCHEMA_VERSION = "1.0.0"

#: HistoricalOutcomeLibrary identity shape (forecast/library.py).
OUTCOME_LIBRARY_SCHEMA_VERSION = "2.0.0"

#: Cohort dimensions + backoff sequence definition (forecast/cohorts.py).
#: Bump when the bucket definitions or backoff order change meaning.
COHORT_SCHEMA_VERSION = "1.0.0"

#: The deterministic forecast engine (posterior + distribution + OOD
#: composition) (forecast/engine.py).
FORECAST_ENGINE_VERSION = "1.0.0"

#: DistributionShiftAssessment shape (contracts/forecast.py).
OOD_SCHEMA_VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# Section 13 -- Economic Edge & Admission Authority
# ---------------------------------------------------------------------------

#: CostEstimate contract shape (contracts/economics.py). 1.1.0: additive
#: Section 17 venue fields (run/cycle identity, environment, observation
#: lineage, per-component uncertainty/lineage, reference size, cost curve).
COST_ESTIMATE_SCHEMA_VERSION = "1.1.0"

#: Live/venue cost model version (economics/costs.py) -- see
#: RESEARCH_COST_MODEL_VERSION above for the distinct research-labeling one.
VENUE_COST_MODEL_VERSION = "1.0.0"

#: AdmissionPolicy contract + RESEARCH DEFAULT values (economics/policy.py).
#: These are explicitly research defaults, not replay-calibrated production
#: thresholds -- see the module docstring in economics/policy.py.
ADMISSION_POLICY_SCHEMA_VERSION = "1.0.0"

#: EconomicOpportunity contract shape (contracts/economics.py).
ECONOMIC_OPPORTUNITY_SCHEMA_VERSION = "1.0.0"

#: The deterministic economic-admission engine (EV/penalties/gates)
#: (economics/engine.py).
ECONOMICS_ENGINE_VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# Shadow pipeline orchestration (controller/cati_controller.py).
# ---------------------------------------------------------------------------
SHADOW_PIPELINE_VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# Section 12.15 closure -- historical library artifact + calibration
# ---------------------------------------------------------------------------

#: On-disk library artifact layout (forecast/artifact.py). Bump on any change
#: to manifest fields or row serialization.
LIBRARY_ARTIFACT_SCHEMA_VERSION = "2.0.0"

#: The offline builder's own logic version (forecast/build_library.py).
LIBRARY_BUILDER_VERSION = "1.0.0"

#: OOD reference-feature schema (which continuous features rows carry).
OOD_FEATURE_SCHEMA_VERSION = "1.0.0"

#: Calibration status contract + report shape (forecast/calibration_report.py).
CALIBRATION_SCHEMA_VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# Section 14 -- Veto and Abstention
# ---------------------------------------------------------------------------
VETO_DECISION_SCHEMA_VERSION = "1.0.0"
VETO_POLICY_SCHEMA_VERSION = "2.0.0"
VETO_ENGINE_VERSION = "1.0.0"
EVENT_RISK_SCHEMA_VERSION = "2.0.0"

# ---------------------------------------------------------------------------
# Section 15 -- Cross-Universe Ranking
# ---------------------------------------------------------------------------
BATCH_SCHEMA_VERSION = "1.0.0"
RANKING_POLICY_SCHEMA_VERSION = "1.0.0"
RANKING_ENGINE_VERSION = "1.0.0"
RANKED_OPPORTUNITY_SCHEMA_VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# Section 16 -- Portfolio Intelligence
# ---------------------------------------------------------------------------
PORTFOLIO_POLICY_SCHEMA_VERSION = "2.0.0"
PORTFOLIO_ENGINE_VERSION = "1.0.0"
PORTFOLIO_CONTEXT_SCHEMA_VERSION = "1.0.0"
PORTFOLIO_SELECTION_SCHEMA_VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# Pre-Section-17 closure
# ---------------------------------------------------------------------------
#: Library source classification + trusted-runtime rules (forecast/source.py).
LIBRARY_SOURCE_POLICY_VERSION = "1.0.0"
#: Normalized MarketEvent / MaintenanceContext contract (contracts/events.py).
MARKET_EVENT_SCHEMA_VERSION = "1.0.0"
#: Event -> instrument scope rules (currency legs, stablecoin pegs).
EVENT_SCOPE_POLICY_VERSION = "1.0.0"
#: BrokerHealthContext contract (contracts/system_health.py).
BROKER_HEALTH_SCHEMA_VERSION = "1.0.0"
#: Asset-class-neutral factor contracts (contracts/factors.py).
FACTOR_SCHEMA_VERSION = "1.0.0"
#: Default factor-set configuration (portfolio/factors.py).
FACTOR_POLICY_VERSION = "1.0.0"
#: cati_portfolio_reservations canonical-migration table shape.
PORTFOLIO_RESERVATION_TABLE_VERSION = "2.0.0"

# ---------------------------------------------------------------------------
# Section 17 -- Broker / Venue Economic Adaptation
# ---------------------------------------------------------------------------
#: VenueEconomicObservation + component observation shapes (contracts/venue_economics.py).
VENUE_ECONOMIC_OBSERVATION_SCHEMA_VERSION = "1.0.0"
#: ExecutionCapabilities / InstrumentMetadata shapes.
VENUE_CAPABILITY_SCHEMA_VERSION = "1.0.0"
#: VenueCostPolicy shape + research-default values (venue/policy.py).
VENUE_COST_POLICY_SCHEMA_VERSION = "1.0.0"
#: The venue-neutral observation -> CostEstimate model (venue/cost_model.py).
VENUE_ECONOMIC_COST_MODEL_VERSION = "1.0.0"
#: Declared adapter validation statuses (venue/registry.py).
ADAPTER_STATUS_REGISTRY_VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# Section 18 -- Trade Plan and Invalidation
# ---------------------------------------------------------------------------
TRADE_PLAN_SCHEMA_VERSION = "1.0.0"
TRADE_PLAN_POLICY_SCHEMA_VERSION = "1.0.0"
TRADE_PLAN_BUILDER_VERSION = "1.0.0"
#: cati_trade_plans canonical-migration table shape.
TRADE_PLAN_TABLE_VERSION = "1.0.0"
