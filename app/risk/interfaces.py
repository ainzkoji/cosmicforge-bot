from typing import List, Dict, Protocol


class BrokerPnLAdapter(Protocol):
    def fetch_user_trades(
        self,
        symbol: str | None,
        since_trade_id: int | None,
    ) -> List[Dict]: ...

    def extract_realized_pnl(self, trades: List[Dict]) -> float: ...
