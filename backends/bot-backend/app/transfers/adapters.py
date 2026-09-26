"""Per-broker internal-transfer adapters over the EXISTING exchange clients.

Each adapter speaks canonical terms (BrokerWallet, Decimal amounts,
TransferStatus) and keeps native names/codes inside. None exposes a
withdrawal: the only money-moving call is an intra-account wallet transfer.

Balances are broker-authoritative "transferable" amounts where the broker
publishes one (Binance futures ``maxWithdrawAmount`` = what can leave the
futures wallet given open-position margin; Bybit ``transferBalance``). A
wallet whose transferable balance cannot be read returns ``None`` and the
service blocks the transfer (never treated as zero or as "enough").
"""
from __future__ import annotations

import logging
import time
from dataclasses import replace
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional

from shared_lib.broker.environment import resolve_wallet_base_url
from shared_lib.broker.resolver import BrokerAuth
from shared_lib.broker.wallets import BrokerTopology, BrokerWallet, topology_for

from app.transfers.models import HistoryRow, LookupOutcome, SubmitOutcome, TransferStatus

logger = logging.getLogger(__name__)

S = TransferStatus


class VenueApiUnavailable(RuntimeError):
    """No verified wallet/transfer API for this broker + environment."""


class BrokerRejected(RuntimeError):
    """The broker explicitly refused the request (nothing moved)."""


def _dec(v: Any) -> Optional[Decimal]:
    try:
        return Decimal(str(v)) if v not in (None, "") else None
    except (InvalidOperation, ValueError):
        return None


def _default_build(auth: BrokerAuth) -> Any:
    from shared_lib.broker.client_factory import build_client_from_auth

    return build_client_from_auth(auth)


class TransferAdapter:
    broker = ""
    #: True when the broker de-duplicates on a caller-supplied id (Bybit).
    client_supplied_id = False
    history_window_ms = 10 * 60_000

    def __init__(self, auth: BrokerAuth, build: Callable[[BrokerAuth], Any] = _default_build):
        self.auth = auth
        self._build = build
        self._trading = None

    @property
    def trading(self) -> Any:
        if self._trading is None:
            self._trading = self._build(self.auth)
        return self._trading

    # -- topology -------------------------------------------------------------------
    def account_mode(self) -> Optional[str]:
        return None

    def topology(self) -> Optional[BrokerTopology]:
        return topology_for(self.broker, self.account_mode())

    # -- to implement ---------------------------------------------------------------
    def transferable(self, wallet: BrokerWallet, asset: str) -> Optional[Decimal]:
        raise NotImplementedError

    def submit(self, *, request_id: str, route_code: str, source: BrokerWallet, destination: BrokerWallet,
               asset: str, amount: Decimal) -> SubmitOutcome:
        raise NotImplementedError

    def lookup(self, *, request_id: str, broker_transfer_id: Optional[str], route_code: str, asset: str,
               amount: Decimal, submitted_at_ms: int, claimed_ids: set) -> LookupOutcome:
        raise NotImplementedError

    def history(self, start_ms: int, end_ms: int) -> List[HistoryRow]:
        raise NotImplementedError


def _match_unknown(rows: List[HistoryRow], *, asset: str, amount: Decimal, route: tuple, submitted_at_ms: int,
                   window_ms: int, claimed_ids: set) -> Optional[HistoryRow]:
    """Exactly ONE unclaimed history row with the same asset/amount/route in
    the submission window, or None (ambiguity is never resolved by guessing)."""
    cands = [r for r in rows
             if r.asset.upper() == asset.upper() and r.amount == amount
             and (r.source_native, r.destination_native) == route
             and submitted_at_ms - 60_000 <= r.timestamp_ms <= submitted_at_ms + window_ms
             and r.broker_transfer_id not in claimed_ids]
    return cands[0] if len(cands) == 1 else None


# ── Binance ─────────────────────────────────────────────────────────────────────
_BINANCE_STATUS = {"CONFIRMED": S.COMPLETED, "FAILED": S.FAILED, "PENDING": S.CONFIRMATION_PENDING}


class BinanceTransferAdapter(TransferAdapter):
    broker = "binance"

    @property
    def wallet_client(self) -> Any:
        url = resolve_wallet_base_url("binance", self.auth.environment)
        if not url:
            raise VenueApiUnavailable(f"no verified Binance wallet API for {self.auth.environment.value}")
        if getattr(self, "_wallet", None) is None:
            self._wallet = self._build(replace(self.auth, base_url=url))
        return self._wallet

    def account_mode(self) -> Optional[str]:
        return "CLASSIC"

    def transferable(self, wallet: BrokerWallet, asset: str) -> Optional[Decimal]:
        a = asset.upper()
        if wallet.native_type == "UMFUTURE":
            for row in self.trading.account_balance() or []:
                if str(row.get("asset", "")).upper() == a:
                    return _dec(row.get("maxWithdrawAmount"))
            return Decimal("0")  # broker listed balances and this asset is not among them
        if wallet.native_type == "MAIN":
            for row in (self.wallet_client.spot_account() or {}).get("balances", []):
                if str(row.get("asset", "")).upper() == a:
                    return _dec(row.get("free"))
            return Decimal("0")
        if wallet.native_type == "FUNDING":
            rows = self.wallet_client.funding_assets(a)
            for row in rows:
                if str(row.get("asset", "")).upper() == a:
                    return _dec(row.get("free"))
            return Decimal("0")
        return None

    def submit(self, *, request_id, route_code, source, destination, asset, amount) -> SubmitOutcome:
        resp = self.wallet_client.universal_transfer(route_code, asset, format(amount, "f"))
        tran_id = resp.get("tranId") if isinstance(resp, dict) else None
        if tran_id is None:
            return SubmitOutcome(S.SUBMITTED, None, None, "no tranId in response")
        return SubmitOutcome(S.CONFIRMATION_PENDING, str(tran_id), None, "accepted")

    def _rows(self, route_code: str, start_ms: int, end_ms: int) -> List[HistoryRow]:
        data = self.wallet_client.universal_transfer_history(route_code, start_ms, end_ms, size=100)
        src, dst = route_code.split("_", 1)
        out = []
        for r in (data or {}).get("rows", []) or []:
            out.append(HistoryRow(str(r.get("tranId")), str(r.get("asset", "")), _dec(r.get("amount")) or Decimal("0"),
                                  src, dst, _BINANCE_STATUS.get(str(r.get("status", "")).upper(), S.CONFIRMATION_PENDING),
                                  int(r.get("timestamp") or 0), r))
        return out

    def lookup(self, *, request_id, broker_transfer_id, route_code, asset, amount, submitted_at_ms,
               claimed_ids) -> LookupOutcome:
        now = int(time.time() * 1000)
        rows = self._rows(route_code, submitted_at_ms - 60_000, min(now, submitted_at_ms + self.history_window_ms))
        hit = next((r for r in rows if broker_transfer_id and r.broker_transfer_id == str(broker_transfer_id)), None)
        if hit is None and not broker_transfer_id:
            src, dst = route_code.split("_", 1)
            hit = _match_unknown(rows, asset=asset, amount=amount, route=(src, dst), submitted_at_ms=submitted_at_ms,
                                 window_ms=self.history_window_ms, claimed_ids=claimed_ids)
        if hit is None:
            return LookupOutcome(False)
        return LookupOutcome(True, hit.status, hit.broker_transfer_id, str(hit.raw.get("status")))

    def history(self, start_ms: int, end_ms: int) -> List[HistoryRow]:
        topo = self.topology()
        out: List[HistoryRow] = []
        for code in sorted(set(topo.routes.values())):
            out.extend(self._rows(code, start_ms, end_ms))
        return out


# ── Bybit ───────────────────────────────────────────────────────────────────────
_BYBIT_STATUS = {"SUCCESS": S.COMPLETED, "FAILED": S.FAILED, "PENDING": S.CONFIRMATION_PENDING}


class BybitTransferAdapter(TransferAdapter):
    broker = "bybit"
    client_supplied_id = True

    def account_mode(self) -> Optional[str]:
        if getattr(self, "_mode", None) is None:
            data = self.trading.account_info()
            status = (data or {}).get("result", {}).get("unifiedMarginStatus") if (data or {}).get("retCode") == 0 else None
            try:
                status = int(status)
            except (TypeError, ValueError):
                status = None
            self._mode = None if status is None else ("CLASSIC" if status == 1 else "UNIFIED")
            # Section 7.11: a CHANGED broker-reported mode requests a catalog/capability refresh
            # (observation only; the mode returned here is unaffected)
            try:
                from app.exchange.catalog_refresh import observe_account_mode

                observe_account_mode(self.auth.account_id, "bybit", self.auth.environment, self._mode)
            except Exception:
                pass
        return self._mode

    def topology(self) -> Optional[BrokerTopology]:
        mode = self.account_mode()
        return topology_for("bybit", mode) if mode else None

    def transferable(self, wallet: BrokerWallet, asset: str) -> Optional[Decimal]:
        data = self.trading.account_coin_balance(wallet.native_type, asset)
        if (data or {}).get("retCode") != 0:
            return None
        bal = (data.get("result") or {}).get("balance") or {}
        return _dec(bal.get("transferBalance"))

    def submit(self, *, request_id, route_code, source, destination, asset, amount) -> SubmitOutcome:
        data = self.trading.inter_transfer(request_id, asset, format(amount, "f"), source.native_type,
                                           destination.native_type)
        if (data or {}).get("retCode") != 0:
            raise BrokerRejected(f"bybit retCode={data.get('retCode')} {data.get('retMsg')}")
        res = data.get("result") or {}
        raw = str(res.get("status", "")).upper()
        return SubmitOutcome(_BYBIT_STATUS.get(raw, S.SUBMITTED), str(res.get("transferId") or request_id), raw or None)

    @staticmethod
    def _row(r: Dict[str, Any]) -> HistoryRow:
        return HistoryRow(str(r.get("transferId")), str(r.get("coin", "")), _dec(r.get("amount")) or Decimal("0"),
                          str(r.get("fromAccountType", "")), str(r.get("toAccountType", "")),
                          _BYBIT_STATUS.get(str(r.get("status", "")).upper(), S.CONFIRMATION_PENDING),
                          int(r.get("timestamp") or 0), r)

    def lookup(self, *, request_id, broker_transfer_id, route_code, asset, amount, submitted_at_ms,
               claimed_ids) -> LookupOutcome:
        data = self.trading.query_inter_transfers(transfer_id=broker_transfer_id or request_id)
        if (data or {}).get("retCode") != 0:
            return LookupOutcome(False)
        rows = [self._row(r) for r in ((data.get("result") or {}).get("list") or [])]
        hit = next((r for r in rows if r.broker_transfer_id == str(broker_transfer_id or request_id)), None)
        if hit is None:
            return LookupOutcome(False)
        return LookupOutcome(True, hit.status, hit.broker_transfer_id, str(hit.raw.get("status")))

    def history(self, start_ms: int, end_ms: int) -> List[HistoryRow]:
        out, cursor = [], None
        for _ in range(20):  # bounded pagination
            data = self.trading.query_inter_transfers(start_time_ms=start_ms, end_time_ms=end_ms, limit=50, cursor=cursor)
            if (data or {}).get("retCode") != 0:
                break
            res = data.get("result") or {}
            out.extend(self._row(r) for r in res.get("list") or [])
            cursor = res.get("nextPageCursor")
            if not cursor:
                break
        return out


# ── BingX ───────────────────────────────────────────────────────────────────────
_BINGX_STATUS = {"CONFIRMED": S.COMPLETED, "FAILED": S.FAILED, "PENDING": S.CONFIRMATION_PENDING}


class BingXTransferAdapter(TransferAdapter):
    broker = "bingx"

    def account_mode(self) -> Optional[str]:
        return "STANDARD"

    def _require_wallet_api(self) -> None:
        if not resolve_wallet_base_url("bingx", self.auth.environment):
            raise VenueApiUnavailable(f"no verified BingX wallet API for {self.auth.environment.value}")

    def transferable(self, wallet: BrokerWallet, asset: str) -> Optional[Decimal]:
        if wallet.native_type == "PFUTURES" and asset.upper() == "USDT":
            acc = self.trading.account()
            return _dec(acc.get("availableBalance"))
        # No verified BingX fund-wallet balance endpoint: unknown, never zero.
        return None

    def submit(self, *, request_id, route_code, source, destination, asset, amount) -> SubmitOutcome:
        self._require_wallet_api()
        data = self.trading.asset_transfer(route_code, asset, format(amount, "f"))
        body = data.get("data") if isinstance(data.get("data"), dict) else data
        tran_id = body.get("tranId") if isinstance(body, dict) else None
        if tran_id is None:
            return SubmitOutcome(S.SUBMITTED, None, None, "no tranId in response")
        return SubmitOutcome(S.CONFIRMATION_PENDING, str(tran_id))

    def _rows(self, route_code: str, start_ms: int, end_ms: int) -> List[HistoryRow]:
        data = self.trading.asset_transfer_history(route_code, start_ms, end_ms)
        body = data.get("data") if isinstance(data.get("data"), dict) else data
        src, dst = route_code.split("_", 1)
        return [HistoryRow(str(r.get("tranId")), str(r.get("asset", "")), _dec(r.get("amount")) or Decimal("0"),
                           src, dst, _BINGX_STATUS.get(str(r.get("status", "")).upper(), S.CONFIRMATION_PENDING),
                           int(r.get("timestamp") or 0), r)
                for r in (body or {}).get("rows", []) or []]

    def lookup(self, *, request_id, broker_transfer_id, route_code, asset, amount, submitted_at_ms,
               claimed_ids) -> LookupOutcome:
        self._require_wallet_api()
        now = int(time.time() * 1000)
        rows = self._rows(route_code, submitted_at_ms - 60_000, min(now, submitted_at_ms + self.history_window_ms))
        hit = next((r for r in rows if broker_transfer_id and r.broker_transfer_id == str(broker_transfer_id)), None)
        if hit is None and not broker_transfer_id:
            src, dst = route_code.split("_", 1)
            hit = _match_unknown(rows, asset=asset, amount=amount, route=(src, dst), submitted_at_ms=submitted_at_ms,
                                 window_ms=self.history_window_ms, claimed_ids=claimed_ids)
        if hit is None:
            return LookupOutcome(False)
        return LookupOutcome(True, hit.status, hit.broker_transfer_id, str(hit.raw.get("status")))

    def history(self, start_ms: int, end_ms: int) -> List[HistoryRow]:
        self._require_wallet_api()
        topo = self.topology()
        out: List[HistoryRow] = []
        for code in sorted(set(topo.routes.values())):
            out.extend(self._rows(code, start_ms, end_ms))
        return out


ADAPTERS = {"binance": BinanceTransferAdapter, "bybit": BybitTransferAdapter, "bingx": BingXTransferAdapter}


def adapter_for(auth: BrokerAuth, build: Callable[[BrokerAuth], Any] = _default_build) -> Optional[TransferAdapter]:
    cls = ADAPTERS.get(auth.broker_type.lower())
    return cls(auth, build) if cls else None


__all__ = ["ADAPTERS", "BinanceTransferAdapter", "BingXTransferAdapter", "BrokerRejected", "BybitTransferAdapter",
           "TransferAdapter", "VenueApiUnavailable", "adapter_for"]
