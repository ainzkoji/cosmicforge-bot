"""Trading costs for replay (§13.6), stored whole with every run.

A replay result without its cost model is not a result, it is a number. Two
runs over the same data with different fee or slippage assumptions are not
comparable, and a strategy that is profitable at 0 bps and unprofitable at 8
bps has told you something important only if you can see which was assumed.

So the model is a frozen value object, it is hashed, and the hash goes in the
replay manifest alongside the dataset and the code revision.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class CostModel:
    """Every cost a replay charges, in one place.

    Rates are fractions, not percentages or basis points: 0.0004 is 4 bps. The
    field names say which side of the book they apply to, because "fee" alone
    has silently meant maker in one place and taker in another before now.
    """

    maker_fee: float = 0.0002
    taker_fee: float = 0.0004
    #: Half-spread paid on crossing, as a fraction of price.
    spread: float = 0.0001
    #: Additional adverse movement on a market fill, as a fraction of price.
    slippage: float = 0.0002
    #: Perpetual funding, charged per funding interval on notional.
    funding_rate: float = 0.0001
    funding_interval_ms: int = 8 * 60 * 60 * 1000
    #: Costs the model does not attempt yet, named so they are not forgotten.
    models_latency: bool = False
    models_market_impact: bool = False
    models_borrow: bool = False
    notes: str = ""

    def entry_cost(self, notional: float, *, taker: bool = True) -> float:
        """Fee plus spread plus slippage on entering ``notional``."""
        notional = abs(float(notional))
        fee = notional * (self.taker_fee if taker else self.maker_fee)
        return fee + notional * self.spread + notional * self.slippage

    def exit_cost(self, notional: float, *, taker: bool = True) -> float:
        """Symmetric with :meth:`entry_cost`; separate so it can diverge."""
        return self.entry_cost(notional, taker=taker)

    def funding_cost(self, notional: float, held_ms: int) -> float:
        """Funding charged over a holding period.

        Charged per *completed* interval. A position closed before the first
        funding stamp pays no funding, which is how perpetuals actually work.
        """
        if self.funding_interval_ms <= 0 or held_ms <= 0:
            return 0.0
        intervals = int(held_ms) // int(self.funding_interval_ms)
        return abs(float(notional)) * self.funding_rate * intervals

    def round_trip_cost(
        self, notional: float, held_ms: int = 0, *, taker: bool = True
    ) -> float:
        return (
            self.entry_cost(notional, taker=taker)
            + self.exit_cost(notional, taker=taker)
            + self.funding_cost(notional, held_ms)
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def model_hash(self) -> str:
        """Stable fingerprint, so a run can be tied to its assumptions."""
        payload = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()[:16]

    @classmethod
    def zero(cls) -> "CostModel":
        """No costs at all. Only honest if the result is labelled as gross."""
        return cls(
            maker_fee=0.0, taker_fee=0.0, spread=0.0, slippage=0.0,
            funding_rate=0.0, notes="gross of all costs",
        )


#: Binance USD-M futures, standard tier, as of the time of writing. Named
#: rather than inlined so a run can say which venue assumption it used.
BINANCE_FUTURES_STANDARD = CostModel(
    maker_fee=0.0002,
    taker_fee=0.0004,
    spread=0.0001,
    slippage=0.0002,
    funding_rate=0.0001,
    notes="Binance USD-M futures standard tier; funding is an average, not per-stamp actuals",
)
