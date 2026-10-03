from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Type

from app.strategy.base import Strategy


@dataclass(frozen=True)
class StrategySpec:
    name: str
    version: str
    supports_asset_classes: List[str]
    description: str = ""
    params_schema: Optional[Dict[str, Any]] = None


_REGISTRY: Dict[str, tuple[Type[Strategy], StrategySpec]] = {}


def register_strategy(
    *,
    name: str,
    version: str = "1.0.0",
    supports_asset_classes: Optional[List[str]] = None,
    description: str = "",
    params_schema: Optional[Dict[str, Any]] = None,
) -> Callable[[Type[Strategy]], Type[Strategy]]:
    supports_asset_classes = supports_asset_classes or ["CRYPTO", "FOREX"]

    def _decorator(cls: Type[Strategy]) -> Type[Strategy]:
        _REGISTRY[name] = (
            cls,
            StrategySpec(
                name=name,
                version=version,
                supports_asset_classes=supports_asset_classes,
                description=description,
                params_schema=params_schema,
            ),
        )
        return cls

    return _decorator


def list_strategies() -> List[StrategySpec]:
    return [StrategySpec(name="cati", version="cati-sole-runtime-1", supports_asset_classes=["CRYPTO", "FOREX"], description="CATI sole governed intelligence authority")]


def get_strategy_class(name: str) -> Optional[Type[Strategy]]:
    if name != "cati":
        return None
    from app.trading_intelligence.integration.runtime_binding import CatiRuntimeBinding
    return CatiRuntimeBinding


def get_strategy_spec(name: str) -> Optional[StrategySpec]:
    return list_strategies()[0] if name == "cati" else None
