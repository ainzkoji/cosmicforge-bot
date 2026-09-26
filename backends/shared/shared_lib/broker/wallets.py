"""Broker wallet topology: native wallet types -> canonical purposes.

Wallet names are NOT shared across brokers ("MAIN" at Binance is the spot
wallet; Bybit has no "MAIN"; BingX calls its funding wallet "fund"). Each
broker declares its native wallets, what canonical PURPOSE each serves, how
an internal move between two wallets is expressed natively, and whether the
account's collateral is shared (unified) so that no physical move is needed.

Canonical purposes: FUNDING, SPOT, DERIVATIVES, UNIFIED, TRADFI, FX, STOCKS,
UNKNOWN.

Topology modes (per pair of purposes, for a given account mode):

* PHYSICAL_TRANSFER_REQUIRED -- capital must move between wallets.
* SHARED_COLLATERAL          -- the same wallet collateralises both
                                purposes (e.g. Bybit UTA spot + derivatives).
* LOGICAL_ALLOCATION_ONLY    -- same wallet; allocation is bookkeeping only.
* UNSUPPORTED                -- no API route between these wallets.

Account-level class (``TopologyClass``): UNIFIED / SEGMENTED / UNKNOWN /
UNSUPPORTED. A broker whose wallet structure depends on the live account mode
(Bybit) is UNKNOWN for a connected account until that mode was read from the
broker (``topology_for_account``) -- shared collateral is never assumed.

Every declaration here is UNVALIDATED until an internal-transfer contract
test passes against the venue (see ``capabilities.py``). Route minimum /
maximum / fee are not published by a verified venue API and are reported
UNAVAILABLE, never 0.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Mapping, Optional, Tuple


class WalletPurpose(str, Enum):
    FUNDING = "FUNDING"
    SPOT = "SPOT"
    DERIVATIVES = "DERIVATIVES"
    UNIFIED = "UNIFIED"
    TRADFI = "TRADFI"
    FX = "FX"
    STOCKS = "STOCKS"
    UNKNOWN = "UNKNOWN"


class TopologyMode(str, Enum):
    PHYSICAL_TRANSFER_REQUIRED = "PHYSICAL_TRANSFER_REQUIRED"
    SHARED_COLLATERAL = "SHARED_COLLATERAL"
    LOGICAL_ALLOCATION_ONLY = "LOGICAL_ALLOCATION_ONLY"
    UNSUPPORTED = "UNSUPPORTED"


class TopologyClass(str, Enum):
    """Account-level classification of how the products this platform trades are collateralised.

    * UNIFIED      a unified account mode: one wallet is shared collateral across purposes
    * SEGMENTED    separate wallets per purpose (a physical internal move may be needed to fund the
                   trading wallet; products collateralised by the SAME wallet still need none)
    * UNKNOWN      the live account mode could not be read -- nothing is assumed
    * UNSUPPORTED  no declared topology for this broker
    """
    UNIFIED = "UNIFIED"
    SEGMENTED = "SEGMENTED"
    UNKNOWN = "UNKNOWN"
    UNSUPPORTED = "UNSUPPORTED"


ACCOUNT_TOPOLOGY_UNKNOWN = "ACCOUNT_TOPOLOGY_UNKNOWN"

#: Route facts no venue publishes through an API this platform has verified. They are reported as
#: UNAVAILABLE -- never as 0 fee / no minimum / instant settlement.
_ROUTE_FACTS_UNAVAILABLE = {"min_amount": None, "max_amount": None, "fee": None,
                            "facts_status": "UNAVAILABLE_FROM_VENUE_API",
                            "permission_required": "INTERNAL_TRANSFER",
                            "settlement": "BROKER_CONFIRMATION_REQUIRED"}


@dataclass(frozen=True)
class BrokerWallet:
    broker: str
    native_type: str                 # the broker's own name for the wallet
    purpose: WalletPurpose
    #: what this wallet can collateralise / hold for trading
    trades: Tuple[str, ...] = ()     # e.g. ("CRYPTO_PERPETUAL", "FX_PERPETUAL")
    note: str = ""

    def to_dict(self) -> dict:
        return {"broker": self.broker, "native_type": self.native_type, "purpose": self.purpose.value,
                "trades": list(self.trades), "note": self.note}


@dataclass(frozen=True)
class BrokerTopology:
    broker: str
    account_mode: str
    wallets: Tuple[BrokerWallet, ...]
    #: (from_native, to_native) -> native transfer code (broker-specific)
    routes: Mapping[Tuple[str, str], str]
    #: purposes collateralised by ONE wallet in this account mode
    shared_purposes: Tuple[Tuple[WalletPurpose, ...], ...] = ()
    validation_status: str = "UNVALIDATED"

    def wallet(self, ref: str) -> Optional[BrokerWallet]:
        """Look up by native type or canonical purpose (case-insensitive)."""
        key = str(ref or "").strip().upper()
        for w in self.wallets:
            if w.native_type.upper() == key:
                return w
        matches = [w for w in self.wallets if w.purpose.value == key]
        return matches[0] if len(matches) == 1 else None

    def route(self, source: BrokerWallet, destination: BrokerWallet) -> Optional[str]:
        return self.routes.get((source.native_type, destination.native_type))

    def mode_between(self, a: WalletPurpose, b: WalletPurpose) -> TopologyMode:
        if a == b:
            return TopologyMode.LOGICAL_ALLOCATION_ONLY
        for group in self.shared_purposes:
            if a in group and b in group:
                return TopologyMode.SHARED_COLLATERAL
        wa = [w for w in self.wallets if w.purpose == a]
        wb = [w for w in self.wallets if w.purpose == b]
        if any(self.routes.get((x.native_type, y.native_type)) for x in wa for y in wb):
            return TopologyMode.PHYSICAL_TRANSFER_REQUIRED
        return TopologyMode.UNSUPPORTED

    def wallet_for_product(self, product: str) -> Optional[BrokerWallet]:
        for w in self.wallets:
            if product in w.trades:
                return w
        return None

    @property
    def topology_class(self) -> TopologyClass:
        # UNIFIED = an account mode whose trading wallet is shared collateral across purposes (Bybit UTA).
        # A classic account whose derivatives wallet happens to hold several product families is SEGMENTED:
        # between those families allocation is logical (same wallet), but funding/spot are separate wallets.
        return TopologyClass.UNIFIED if self.shared_purposes else TopologyClass.SEGMENTED

    def to_dict(self) -> dict:
        return {
            "broker": self.broker,
            "account_mode": self.account_mode,
            "topology_class": self.topology_class.value,
            "validation_status": self.validation_status,
            "wallets": [w.to_dict() for w in self.wallets],
            "routes": [{"from": a, "to": b, "native_code": c, **_ROUTE_FACTS_UNAVAILABLE}
                       for (a, b), c in sorted(self.routes.items())],
            "shared_collateral_groups": [[p.value for p in g] for g in self.shared_purposes],
        }


P = WalletPurpose

# ── Binance (classic account; Portfolio Margin is not detected -> not assumed) ──
# Universal transfer types: POST /sapi/v1/asset/transfer ``type``.
_BINANCE_WALLETS = (
    BrokerWallet("binance", "MAIN", P.SPOT, (), "Spot wallet"),
    BrokerWallet("binance", "FUNDING", P.FUNDING, (), "Funding wallet"),
    BrokerWallet("binance", "UMFUTURE", P.DERIVATIVES, ("CRYPTO_PERPETUAL", "TRADFI_PERPETUAL"),
                 "USD-M futures wallet"),
)
_BINANCE_ROUTES = {
    ("MAIN", "UMFUTURE"): "MAIN_UMFUTURE",
    ("UMFUTURE", "MAIN"): "UMFUTURE_MAIN",
    ("MAIN", "FUNDING"): "MAIN_FUNDING",
    ("FUNDING", "MAIN"): "FUNDING_MAIN",
    ("FUNDING", "UMFUTURE"): "FUNDING_UMFUTURE",
    ("UMFUTURE", "FUNDING"): "UMFUTURE_FUNDING",
}

# ── Bybit V5 ──────────────────────────────────────────────────────────────────
# UTA: the UNIFIED wallet collateralises spot + all derivatives, including the
# FX (symbolType "forex") and stock/ETF/commodity (TradFi) linear perpetuals V5
# instruments-info lists. Classic: CONTRACT and SPOT are separate wallets.
# Bybit MT5/CFD TradFi is a different platform and has no V5 wallet here.
_BYBIT_UTA_WALLETS = (
    BrokerWallet("bybit", "FUND", P.FUNDING, (), "Funding wallet"),
    BrokerWallet("bybit", "UNIFIED", P.UNIFIED, ("CRYPTO_PERPETUAL", "FX_PERPETUAL", "TRADFI_PERPETUAL",
                                                 "CRYPTO_SPOT"), "Unified Trading Account"),
)
_BYBIT_UTA_ROUTES = {("FUND", "UNIFIED"): "FUND->UNIFIED", ("UNIFIED", "FUND"): "UNIFIED->FUND"}
_BYBIT_CLASSIC_WALLETS = (
    BrokerWallet("bybit", "FUND", P.FUNDING, (), "Funding wallet"),
    BrokerWallet("bybit", "CONTRACT", P.DERIVATIVES, ("CRYPTO_PERPETUAL",), "Classic derivatives wallet"),
    BrokerWallet("bybit", "SPOT", P.SPOT, ("CRYPTO_SPOT",), "Classic spot wallet"),
)
_BYBIT_CLASSIC_ROUTES = {
    ("FUND", "CONTRACT"): "FUND->CONTRACT", ("CONTRACT", "FUND"): "CONTRACT->FUND",
    ("FUND", "SPOT"): "FUND->SPOT", ("SPOT", "FUND"): "SPOT->FUND",
    ("SPOT", "CONTRACT"): "SPOT->CONTRACT", ("CONTRACT", "SPOT"): "CONTRACT->SPOT",
}

# ── BingX ─────────────────────────────────────────────────────────────────────
# Asset transfer ``type`` codes (FUND <-> USDT-M perpetual, FUND <-> spot).
# The NCFX/NCSK/NCCO/NCSI TradFi contracts are USDT-M swap contracts in the same
# official swap API, so the perpetual account collateralises them (UNVALIDATED).
_BINGX_WALLETS = (
    BrokerWallet("bingx", "FUND", P.FUNDING, (), "Fund account"),
    BrokerWallet("bingx", "PFUTURES", P.DERIVATIVES, ("CRYPTO_PERPETUAL", "FX_PERPETUAL", "TRADFI_PERPETUAL"),
                 "USDT-M perpetual futures account"),
)
_BINGX_ROUTES = {("FUND", "PFUTURES"): "FUND_PFUTURES", ("PFUTURES", "FUND"): "PFUTURES_FUND"}


def topology_for(broker: str, account_mode: Optional[str] = None) -> Optional[BrokerTopology]:
    b = str(broker or "").strip().lower()
    mode = str(account_mode or "").strip().upper()
    if b == "binance":
        return BrokerTopology("binance", "CLASSIC", _BINANCE_WALLETS, _BINANCE_ROUTES)
    if b == "bybit":
        if mode == "CLASSIC":
            return BrokerTopology("bybit", "CLASSIC", _BYBIT_CLASSIC_WALLETS, _BYBIT_CLASSIC_ROUTES)
        # Default: UTA (Bybit's current account model). The live account mode is
        # read from /v5/account/info by the transfer adapter before any move.
        return BrokerTopology("bybit", "UNIFIED", _BYBIT_UTA_WALLETS, _BYBIT_UTA_ROUTES,
                              shared_purposes=((P.UNIFIED, P.DERIVATIVES, P.SPOT, P.FX),))
    if b == "bingx":
        return BrokerTopology("bingx", "STANDARD", _BINGX_WALLETS, _BINGX_ROUTES)
    return None


def trading_wallet_purpose(broker: str, product: str, account_mode: Optional[str] = None) -> Optional[WalletPurpose]:
    """Which wallet purpose collateralises ``product`` at this broker/account mode."""
    topo = topology_for(broker, account_mode)
    wallet = topo.wallet_for_product(product) if topo else None
    return wallet.purpose if wallet else None


#: Brokers whose wallet structure depends on the LIVE account mode (Bybit UTA vs classic). For them
#: ``topology_for``'s default is a declaration, not a fact about a connected account.
ACCOUNT_MODE_DEPENDENT = frozenset({"bybit"})


def topology_for_account(broker: str, account_mode: Optional[str]) -> Optional[BrokerTopology]:
    """The topology of a CONNECTED account: ``None`` when it depends on an account mode that was not
    read from the broker (never guessed unified). Brokers with a single account model are unaffected."""
    b = str(broker or "").strip().lower()
    if b in ACCOUNT_MODE_DEPENDENT and not str(account_mode or "").strip():
        return None
    return topology_for(b, account_mode)


def topology_class(broker: str, account_mode: Optional[str]) -> TopologyClass:
    if topology_for(broker) is None:
        return TopologyClass.UNSUPPORTED
    topo = topology_for_account(broker, account_mode)
    return TopologyClass.UNKNOWN if topo is None else topo.topology_class


__all__ = ["ACCOUNT_MODE_DEPENDENT", "ACCOUNT_TOPOLOGY_UNKNOWN", "BrokerTopology", "BrokerWallet", "TopologyClass",
           "TopologyMode", "WalletPurpose", "topology_class", "topology_for", "topology_for_account",
           "trading_wallet_purpose"]
