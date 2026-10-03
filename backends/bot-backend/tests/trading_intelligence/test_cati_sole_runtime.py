"""Runtime cutover: CATI observation is independent of entry authority."""
import inspect
from types import SimpleNamespace

import pytest

from app.trading_intelligence.governance import runtime_authority as router
from app.trading_intelligence.governance.phases import PHASES
from app.trading_intelligence.execution.entry_permit import boundary_entry_permit, entry_permitted
from app.execution.executor import BinanceExecutor
from app.runner.runner import PaperRunner
from app.strategy.loader import build_strategy
from app.strategy.registry import get_strategy_class, list_strategies


@pytest.mark.parametrize('phase', PHASES)
def test_no_phase_scope_health_or_flag_can_grant_legacy_authority(phase, monkeypatch):
    gov = SimpleNamespace(current_phase=lambda: phase.phase, kill_switch_on=lambda **kw: False,
                          scope_granted=lambda **kw: False)
    monkeypatch.setattr(router, 'PromotionGovernance', lambda db: gov)
    assert phase.v2_authority == 'NONE' and not phase.v2_fallback_allowed
    for environment in ('DEMO', 'LIVE', '', 'UNKNOWN'):
        for healthy in (True, False):
            authority = router.resolve_order_authority(None, broker_account_id='a', venue='binance',
                                                        environment=environment, cati_healthy=healthy)
            assert authority.owner in (router.CATI, router.NONE)
            assert not authority.allows(router.V2)
            assert authority.to_dict()['cati_runtime_active']
            if phase.phase == 'M0':
                assert authority.to_dict()['cati_entry_authority'] == 'BLOCKED'


def test_historical_configuration_never_constructs_an_old_strategy():
    for old_name in ('master_ensemble', 'robust_ensemble', 'sma_cross', 'unknown', 'cati'):
        binding = build_strategy(name=old_name, client=None, interval='15m')
        assert binding.strategy_id == 'cati'
        with pytest.raises(RuntimeError, match='REMOVED'):
            binding.get_signal('BTCUSDT')
    assert [s.name for s in list_strategies()] == ['cati']
    assert get_strategy_class('master_ensemble') is None


def test_scalar_executor_and_direct_internal_executor_cannot_open_entries():
    executor = BinanceExecutor.__new__(BinanceExecutor)
    for signal in ('BUY', 'SELL', 'ADD_LONG_1', 'REVERSE', 'OPEN_LONG'):
        # No broker/client is needed: denial occurs before any broker access.
        result = executor._execute_impl('BTCUSDT', signal, 500, intent_identity='forged')
        assert not result.success and result.error == 'CATI_BOUNDARY_PERMIT_REQUIRED'
    assert PaperRunner._v2_order_authority_block(None, 'BTCUSDT', 'BUY').status == 'BLOCKED'


def test_permit_binds_identity_symbol_direction_size_and_resets_on_unknown_result():
    request = SimpleNamespace(risk_decision_id='approved-risk', trade_plan_id='tp', trade_plan_hash='h',
                              venue_symbol='BTCUSDT', side='LONG', notional=500., intent_identity='tp|h')
    assert not entry_permitted('BTCUSDT', 'BUY', 500., 'tp|h')
    with pytest.raises(RuntimeError):
        with boundary_entry_permit(request):
            assert entry_permitted('BTCUSDT', 'BUY', 500., 'tp|h')
            assert not entry_permitted('ETHUSDT', 'BUY', 500., 'tp|h')
            assert not entry_permitted('BTCUSDT', 'SELL', 500., 'tp|h')
            assert not entry_permitted('BTCUSDT', 'BUY', 1000., 'tp|h')
            raise RuntimeError('broker result unknown')
    assert not entry_permitted('BTCUSDT', 'BUY', 500., 'tp|h')


def test_runtime_analysis_path_has_no_legacy_signal_or_intent_constructor():
    for method in (PaperRunner._step_symbol_evaluate, PaperRunner._step_symbol_orchestrated):
        source = inspect.getsource(method)
        assert '.get_signal(' not in source
        assert '.process_signal(' not in source
    assert '_cycle_shadow.record_symbol' in inspect.getsource(PaperRunner._step_symbol_evaluate)
    assert 'CATI_OBSERVE' in inspect.getsource(PaperRunner._step_symbol_orchestrated)


def test_secondary_registry_and_scalar_orchestrator_cannot_generate_intents():
    from app.strategy.strategy_framework import StrategyRegistry, MovingAverageCross
    from app.core.trading_orchestrator import TradingOrchestrator
    StrategyRegistry.register(MovingAverageCross())
    assert StrategyRegistry.get("ma_cross") is None
    assert StrategyRegistry.list_all() == []
    orchestrator = TradingOrchestrator.__new__(TradingOrchestrator)
    result = orchestrator.process_trading_opportunity("BTCUSDT", [], 1, 100, 0, 100, 0, None)
    assert result["decision"] == "blocked"
    assert not result["details"]["execution_attempted"]


def test_hard_daily_cap_cannot_be_relaxed_by_custom_system_limits():
    from app.risk.system_limits import SystemLimits, UserConfigurableLimits, ConfigValidator
    requested = UserConfigurableLimits(max_daily_loss_pct=0.10)
    for ceiling in (0.025, 0.10, 0.01):
        validated, _ = ConfigValidator(SystemLimits(max_daily_loss_pct=ceiling)).validate_and_clamp(requested)
        assert validated.max_daily_loss_pct == min(0.025, ceiling)


def test_observation_reason_is_canonical_and_never_approved_or_filled():
    from app.decision.reasons import is_known_reason
    from app.evidence.runner_bridge import canonical_reason, final_action
    result = {"decision": "CATI_OBSERVE", "reason_code": "CATI_OBSERVE", "executed": False}
    assert is_known_reason(canonical_reason(result))
    assert final_action(result) == "HOLD"
