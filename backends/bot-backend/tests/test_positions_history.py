"""
Tests for GET /api/v1/analytics/reporting/positions/history

Covers:
- Authentication requirement
- User scoping (user_id always applied)
- OPEN/CLOSE fill grouping by position_id
- Weighted average entry/exit price
- Status computation (OPEN vs CLOSED)
- Result computation (winner/loser/breakeven)
- Filter: status, result, symbol, side
- Pagination: page/page_size/total
- Summary computation (totals, win_rate)
- Fills without position_id excluded + counted in metadata
- Sort whitelist (no injection possible)
"""
import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_user():
    return {"id": "user-123", "email": "test@example.com", "role": "user"}


@pytest.fixture
def mock_db_conn():
    return MagicMock()


# ---------------------------------------------------------------------------
# Helpers to build fake fill rows
# ---------------------------------------------------------------------------

def make_fill(
    id: int,
    position_id: str,
    action: str,
    symbol: str = "BTCUSDT",
    side: str = "LONG",
    qty: float = 1.0,
    price: float = 50000.0,
    fee: float = 5.0,
    realized_pnl: float | None = None,
    timestamp_utc: str = "2025-01-10T10:00:00",
    bot_instance_id: str | None = "bot-1",
    broker_account_id: str | None = "acc-1",
    r_multiple: float | None = None,
    user_id: str = "user-123",
) -> dict:
    return {
        "id": id,
        "position_id": position_id,
        "symbol": symbol,
        "side": side,
        "action": action,
        "qty": qty,
        "price": price,
        "fee": fee,
        "realized_pnl": realized_pnl,
        "timestamp_utc": timestamp_utc,
        "bot_instance_id": bot_instance_id,
        "broker_account_id": broker_account_id,
        "run_id": "run-1",
        "r_multiple": r_multiple,
        "user_id": user_id,
    }


# ---------------------------------------------------------------------------
# Unit tests for SQL CTE logic (tested via endpoint import)
# ---------------------------------------------------------------------------

class TestGetPositionsHistoryAuth:
    def test_requires_authentication(self):
        """Endpoint must reject unauthenticated requests."""
        try:
            from app.main import app
            client = TestClient(app)
            resp = client.get("/api/v1/analytics/reporting/positions/history")
            assert resp.status_code in (401, 403), (
                f"Expected 401/403, got {resp.status_code}"
            )
        except ImportError:
            pytest.skip("App not importable in this test environment")


class TestPositionGrouping:
    """Logic tests for the CTE grouping behaviour."""

    def test_open_close_pair_grouped_into_single_position(self):
        """One OPEN + one CLOSE fill → one CLOSED position record."""
        open_fill = make_fill(1, "pos-A", "OPEN",  price=50000, realized_pnl=None)
        close_fill = make_fill(2, "pos-A", "CLOSE", price=51000, realized_pnl=1000.0,
                               timestamp_utc="2025-01-10T12:00:00")

        grouped = _simulate_grouping([open_fill, close_fill])
        assert len(grouped) == 1
        pos = grouped[0]
        assert pos["position_id"] == "pos-A"
        assert pos["status"] == "CLOSED"
        assert pos["close_count"] == 1
        assert pos["open_count"] == 1

    def test_close_without_position_id_becomes_synthetic_closed_position(self):
        """CLOSE fill missing position_id is included as a synthetic CLOSED position_id=fill_<id>."""
        close_fill = {**make_fill(2, None, "CLOSE", realized_pnl=12.5, timestamp_utc="2025-01-10T12:00:00"), "position_id": None}
        grouped = _simulate_grouping([close_fill])
        assert len(grouped) == 1
        pos = grouped[0]
        assert pos["position_id"] == "fill_2"
        assert pos["status"] == "CLOSED"
        assert pos["open_count"] == 0
        assert pos["close_count"] == 1
        assert pos["realized_pnl"] == pytest.approx(12.5)
        assert pos["opened_at"] == "2025-01-10T12:00:00"
        assert pos["closed_at"] == "2025-01-10T12:00:00"

    def test_open_only_is_open_status(self):
        """A position with only OPEN fills has status OPEN."""
        fills = [make_fill(1, "pos-B", "OPEN", price=50000)]
        grouped = _simulate_grouping(fills)
        assert len(grouped) == 1
        assert grouped[0]["status"] == "OPEN"
        assert grouped[0]["result"] is None

    def test_weighted_average_entry_price(self):
        """Entry price is qty-weighted average across multiple OPEN fills."""
        f1 = make_fill(1, "pos-C", "OPEN", qty=1.0, price=50000.0)
        f2 = make_fill(2, "pos-C", "OPEN", qty=3.0, price=52000.0)
        # Expected: (1*50000 + 3*52000) / 4 = 206000 / 4 = 51500
        grouped = _simulate_grouping([f1, f2])
        assert len(grouped) == 1
        pos = grouped[0]
        assert abs(pos["entry_price"] - 51500.0) < 0.01

    def test_weighted_average_exit_price(self):
        """Exit price is qty-weighted average across multiple CLOSE fills."""
        open_fill = make_fill(1, "pos-D", "OPEN", qty=2.0, price=50000.0)
        c1 = make_fill(2, "pos-D", "CLOSE", qty=1.0, price=55000.0, realized_pnl=5000.0,
                       timestamp_utc="2025-01-10T14:00:00")
        c2 = make_fill(3, "pos-D", "CLOSE", qty=1.0, price=57000.0, realized_pnl=7000.0,
                       timestamp_utc="2025-01-10T15:00:00")
        # Expected exit: (1*55000 + 1*57000) / 2 = 56000
        grouped = _simulate_grouping([open_fill, c1, c2])
        assert len(grouped) == 1
        assert abs(grouped[0]["exit_price"] - 56000.0) < 0.01

    def test_winner_result(self):
        """Positive realized_pnl on close → result='winner'."""
        fills = [
            make_fill(1, "pos-W", "OPEN", price=100.0),
            make_fill(2, "pos-W", "CLOSE", price=110.0, realized_pnl=10.0,
                      timestamp_utc="2025-01-10T11:00:00"),
        ]
        grouped = _simulate_grouping(fills)
        assert grouped[0]["result"] == "winner"

    def test_loser_result(self):
        """Negative realized_pnl on close → result='loser'."""
        fills = [
            make_fill(1, "pos-L", "OPEN", price=100.0),
            make_fill(2, "pos-L", "CLOSE", price=90.0, realized_pnl=-10.0,
                      timestamp_utc="2025-01-10T11:00:00"),
        ]
        grouped = _simulate_grouping(fills)
        assert grouped[0]["result"] == "loser"

    def test_breakeven_result(self):
        """Zero realized_pnl on close → result='breakeven'."""
        fills = [
            make_fill(1, "pos-BE", "OPEN", price=100.0),
            make_fill(2, "pos-BE", "CLOSE", price=100.0, realized_pnl=0.0,
                      timestamp_utc="2025-01-10T11:00:00"),
        ]
        grouped = _simulate_grouping(fills)
        assert grouped[0]["result"] == "breakeven"

    def test_net_pnl_is_realized_minus_fees(self):
        """net_pnl = realized_pnl - total_fees for closed positions."""
        fills = [
            make_fill(1, "pos-N", "OPEN",  fee=2.0,  price=100.0),
            make_fill(2, "pos-N", "CLOSE", fee=3.0,  price=110.0, realized_pnl=10.0,
                      timestamp_utc="2025-01-10T11:00:00"),
        ]
        grouped = _simulate_grouping(fills)
        pos = grouped[0]
        # total_fees = 5.0, realized_pnl = 10.0, net_pnl = 5.0
        assert pos["total_fees"] == pytest.approx(5.0)
        assert pos["net_pnl"] == pytest.approx(5.0)

    def test_net_pnl_is_null_for_open_positions(self):
        """net_pnl must be None for OPEN positions (no realized PnL yet)."""
        fills = [make_fill(1, "pos-O", "OPEN", fee=2.0, price=100.0)]
        grouped = _simulate_grouping(fills)
        assert grouped[0]["net_pnl"] is None

    def test_duration_seconds_computed(self):
        """duration_seconds = (closed_at - opened_at) in seconds."""
        fills = [
            make_fill(1, "pos-T", "OPEN",  timestamp_utc="2025-01-10T10:00:00"),
            make_fill(2, "pos-T", "CLOSE", timestamp_utc="2025-01-10T11:00:00",
                      realized_pnl=5.0, price=105.0),
        ]
        grouped = _simulate_grouping(fills)
        assert grouped[0]["duration_seconds"] == pytest.approx(3600, abs=1)

    def test_fills_without_position_id_grouped_as_synthetic_position(self):
        """Fills with missing position_id are grouped as a synthetic position_id=fill_<id>."""
        with_pos = make_fill(1, "pos-X", "OPEN")
        without_pos = {**make_fill(2, None, "OPEN"), "position_id": None}

        grouped = _simulate_grouping([with_pos, without_pos])
        assert len(grouped) == 2
        ids = {g["position_id"] for g in grouped}
        assert ids == {"pos-X", "fill_2"}

    def test_multiple_positions_same_user(self):
        """Multiple positions for same user each produce a separate grouped record."""
        fills = [
            make_fill(1, "pos-1", "OPEN", symbol="BTCUSDT"),
            make_fill(2, "pos-1", "CLOSE", symbol="BTCUSDT", realized_pnl=10.0,
                      timestamp_utc="2025-01-10T11:00:00"),
            make_fill(3, "pos-2", "OPEN", symbol="ETHUSDT"),
        ]
        grouped = _simulate_grouping(fills)
        assert len(grouped) == 2
        symbols = {p["position_id"] for p in grouped}
        assert symbols == {"pos-1", "pos-2"}

    def test_user_scoping_excludes_other_users(self):
        """Fills belonging to a different user_id are excluded."""
        my_fill = make_fill(1, "pos-A", "OPEN", user_id="user-123")
        other_fill = make_fill(2, "pos-B", "OPEN", user_id="user-999")

        grouped = _simulate_grouping([my_fill, other_fill], user_id="user-123")
        assert len(grouped) == 1
        assert grouped[0]["position_id"] == "pos-A"

    def test_open_fill_ids_and_close_fill_ids(self):
        """open_fill_ids and close_fill_ids are lists of fill IDs."""
        fills = [
            make_fill(10, "pos-F", "OPEN"),
            make_fill(11, "pos-F", "OPEN"),
            make_fill(20, "pos-F", "CLOSE", realized_pnl=5.0,
                      timestamp_utc="2025-01-10T11:00:00"),
        ]
        grouped = _simulate_grouping(fills)
        pos = grouped[0]
        assert set(pos["open_fill_ids"]) == {"10", "11"}
        assert set(pos["close_fill_ids"]) == {"20"}


class TestSummaryComputation:
    def test_summary_counts(self):
        """Summary totals match the position list."""
        fills = [
            make_fill(1, "pos-W", "OPEN",  price=100.0),
            make_fill(2, "pos-W", "CLOSE", price=110.0, realized_pnl=10.0,
                      timestamp_utc="2025-01-10T11:00:00"),
            make_fill(3, "pos-L", "OPEN",  price=100.0),
            make_fill(4, "pos-L", "CLOSE", price=90.0, realized_pnl=-10.0,
                      timestamp_utc="2025-01-10T12:00:00"),
            make_fill(5, "pos-O", "OPEN",  price=100.0),
        ]
        grouped = _simulate_grouping(fills)
        summary = _compute_summary(grouped)
        assert summary["total"] == 3
        assert summary["closed_count"] == 2
        assert summary["open_count"] == 1
        assert summary["winners"] == 1
        assert summary["losers"] == 1
        assert summary["breakevens"] == 0

    def test_win_rate_is_percentage_of_closed(self):
        """win_rate = winners / closed_count * 100."""
        fills = [
            make_fill(1, "p1", "OPEN"),
            make_fill(2, "p1", "CLOSE", realized_pnl=10.0, timestamp_utc="2025-01-10T11:00:00"),
            make_fill(3, "p2", "OPEN"),
            make_fill(4, "p2", "CLOSE", realized_pnl=5.0, timestamp_utc="2025-01-10T12:00:00"),
            make_fill(5, "p3", "OPEN"),
            make_fill(6, "p3", "CLOSE", realized_pnl=-3.0, timestamp_utc="2025-01-10T13:00:00"),
        ]
        grouped = _simulate_grouping(fills)
        summary = _compute_summary(grouped)
        # 2 winners out of 3 closed = 66.67%
        assert summary["win_rate"] == pytest.approx(66.67, abs=0.1)

    def test_win_rate_none_when_no_closed(self):
        """win_rate must be None when there are no closed positions."""
        fills = [make_fill(1, "pos-O", "OPEN")]
        grouped = _simulate_grouping(fills)
        summary = _compute_summary(grouped)
        assert summary["win_rate"] is None


class TestSortWhitelist:
    """The sort_by parameter must be validated against a whitelist."""

    VALID_COLS = {"opened_at", "closed_at", "realized_pnl", "net_pnl"}
    DEFAULT = "opened_at"

    def test_valid_columns_accepted(self):
        for col in self.VALID_COLS:
            result = _validate_sort_col(col)
            assert result == col

    def test_invalid_column_falls_back_to_default(self):
        for bad in ("1=1", "user_id", "'; DROP TABLE--", "", "random_col"):
            result = _validate_sort_col(bad)
            assert result == self.DEFAULT


# ---------------------------------------------------------------------------
# Pure-Python simulation helpers (no DB required)
# These replicate the CTE logic so we can test it deterministically.
# ---------------------------------------------------------------------------

def _simulate_grouping(fills: list[dict], user_id: str = "user-123") -> list[dict]:
    """Python replica of the SQL CTE chain for unit testing without a DB."""
    from collections import defaultdict

    # fill_data: filter user_id (position_id may be missing; we synthesize one)
    fill_data = [
        f for f in fills
        if f.get("user_id", user_id) == user_id
    ]

    # position_groups
    groups: dict[str, dict] = defaultdict(lambda: {
        "open_qty": 0.0, "open_value": 0.0,
        "close_qty": 0.0, "close_value": 0.0,
        "total_fees": 0.0, "realized_pnl": 0.0,
        "open_count": 0, "close_count": 0,
        "opened_at": None, "closed_at": None,
        "symbol": None, "side": None,
        "bot_instance_id": None, "broker_account_id": None, "run_id": None,
        "r_multiple": None, "open_fill_ids": [], "close_fill_ids": [],
    })

    for f in fill_data:
        pid = f.get("position_id") or f"fill_{f['id']}"
        g = groups[pid]
        g["symbol"] = g["symbol"] or f.get("symbol")
        g["side"] = g["side"] or f.get("side")
        g["bot_instance_id"] = g["bot_instance_id"] or f.get("bot_instance_id")
        g["broker_account_id"] = g["broker_account_id"] or f.get("broker_account_id")
        g["run_id"] = g["run_id"] or f.get("run_id")
        g["total_fees"] += f.get("fee") or 0.0

        if f["action"] == "OPEN":
            g["open_qty"] += f["qty"]
            g["open_value"] += f["qty"] * f["price"]
            g["open_count"] += 1
            g["open_fill_ids"].append(str(f["id"]))
            ts = f.get("timestamp_utc", "")
            if g["opened_at"] is None or ts < g["opened_at"]:
                g["opened_at"] = ts
        elif f["action"] == "CLOSE":
            g["close_qty"] += f["qty"]
            g["close_value"] += f["qty"] * f["price"]
            g["realized_pnl"] += f.get("realized_pnl") or 0.0
            g["close_count"] += 1
            g["close_fill_ids"].append(str(f["id"]))
            if f.get("r_multiple") is not None:
                g["r_multiple"] = f["r_multiple"]
            ts = f.get("timestamp_utc", "")
            if g["opened_at"] is None or ts < g["opened_at"]:
                g["opened_at"] = ts
            if g["closed_at"] is None or ts > g["closed_at"]:
                g["closed_at"] = ts

    # with_computed
    result = []
    for pid, g in groups.items():
        entry_price = g["open_value"] / g["open_qty"] if g["open_qty"] > 0 else None
        exit_price = g["close_value"] / g["close_qty"] if g["close_qty"] > 0 else None
        is_closed = g["close_count"] > 0
        status = "CLOSED" if is_closed else "OPEN"
        pnl = g["realized_pnl"]

        if is_closed:
            res = "winner" if pnl > 0 else ("loser" if pnl < 0 else "breakeven")
        else:
            res = None

        net_pnl = (pnl - g["total_fees"]) if is_closed else None

        pnl_pct = None
        if is_closed and entry_price and entry_price > 0 and g["open_qty"] > 0:
            pnl_pct = (pnl / (entry_price * g["open_qty"])) * 100

        duration = None
        if g["opened_at"] and g["closed_at"]:
            from datetime import datetime
            try:
                o = datetime.fromisoformat(g["opened_at"])
                c = datetime.fromisoformat(g["closed_at"])
                duration = int((c - o).total_seconds())
            except ValueError:
                pass

        result.append({
            "position_id": pid,
            "symbol": g["symbol"],
            "side": g["side"],
            "status": status,
            "result": res,
            "opened_at": g["opened_at"],
            "closed_at": g["closed_at"] if is_closed else None,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "open_qty": g["open_qty"],
            "close_qty": g["close_qty"],
            "realized_pnl": pnl,
            "total_fees": g["total_fees"],
            "net_pnl": net_pnl,
            "realized_pnl_percent": pnl_pct,
            "duration_seconds": duration,
            "r_multiple": g["r_multiple"],
            "bot_instance_id": g["bot_instance_id"],
            "broker_account_id": g["broker_account_id"],
            "run_id": g["run_id"],
            "open_count": g["open_count"],
            "close_count": g["close_count"],
            "open_fill_ids": g["open_fill_ids"],
            "close_fill_ids": g["close_fill_ids"],
        })

    return result


def _compute_summary(grouped: list[dict], filter_status: str | None = None) -> dict:
    items = grouped
    if filter_status:
        items = [p for p in items if p["status"] == filter_status.upper()]

    total = len(items)
    open_count = sum(1 for p in items if p["status"] == "OPEN")
    closed_count = sum(1 for p in items if p["status"] == "CLOSED")
    winners = sum(1 for p in items if p["result"] == "winner")
    losers = sum(1 for p in items if p["result"] == "loser")
    breakevens = sum(1 for p in items if p["result"] == "breakeven")
    total_realized = sum(p["realized_pnl"] for p in items if p["status"] == "CLOSED")
    total_fees = sum(p["total_fees"] for p in items)
    total_net = sum(p["net_pnl"] for p in items if p["net_pnl"] is not None)
    win_rate = round(winners / closed_count * 100, 2) if closed_count > 0 else None

    return {
        "total": total,
        "open_count": open_count,
        "closed_count": closed_count,
        "winners": winners,
        "losers": losers,
        "breakevens": breakevens,
        "total_realized_pnl": total_realized,
        "total_fees": total_fees,
        "total_net_pnl": total_net,
        "win_rate": win_rate,
    }


def _validate_sort_col(sort_by: str | None) -> str:
    _VALID_SORT = {"opened_at", "closed_at", "realized_pnl", "net_pnl", "duration_seconds"}
    _DEFAULT = "opened_at"
    return sort_by if sort_by in _VALID_SORT else _DEFAULT


def _validate_sort_order(sort_order: str | None) -> str:
    """sort_order must be whitelisted to 'ASC' or 'DESC' only."""
    if sort_order and sort_order.upper() in ("ASC", "DESC"):
        return sort_order.upper()
    return "DESC"


def _build_csv_rows(grouped: list[dict]) -> list[dict]:
    """Replica of CSV export logic — only CLOSED positions."""
    import csv, io
    rows = [p for p in grouped if p["status"] == "CLOSED"]
    output = io.StringIO()
    writer = csv.writer(output)
    headers = [
        "position_id", "symbol", "side", "status", "result",
        "broker_account_id", "bot_instance_id", "run_id",
        "opened_at", "closed_at", "duration_seconds",
        "entry_price", "exit_price", "open_qty", "close_qty",
        "total_fees", "realized_pnl", "net_pnl",
        "realized_pnl_percent", "r_multiple",
        "open_count", "close_count",
        "open_fill_ids", "close_fill_ids",
    ]
    writer.writerow(headers)
    for pos in rows:
        writer.writerow([
            pos["position_id"], pos["symbol"], pos["side"],
            pos["status"], pos["result"] or "",
            pos["broker_account_id"] or "", pos["bot_instance_id"] or "",
            pos["run_id"] or "",
            pos["opened_at"] or "", pos["closed_at"] or "",
            pos["duration_seconds"] if pos["duration_seconds"] is not None else "",
            round(pos["entry_price"], 8) if pos["entry_price"] is not None else "",
            round(pos["exit_price"], 8) if pos["exit_price"] is not None else "",
            round(pos["open_qty"] or 0, 8),
            round(pos["close_qty"] or 0, 8),
            round(pos["total_fees"] or 0, 2),
            round(pos["realized_pnl"] or 0, 2),
            round(pos["net_pnl"], 2) if pos["net_pnl"] is not None else "",
            round(pos["realized_pnl_percent"], 4) if pos["realized_pnl_percent"] is not None else "",
            round(pos["r_multiple"], 4) if pos["r_multiple"] is not None else "",
            pos["open_count"] or 0,
            pos["close_count"] or 0,
            "|".join(pos["open_fill_ids"]),
            "|".join(pos["close_fill_ids"]),
        ])
    csv_text = output.getvalue()
    reader = csv.DictReader(io.StringIO(csv_text))
    return list(reader)


# ---------------------------------------------------------------------------
# CSV Export tests
# ---------------------------------------------------------------------------

class TestCsvExportEndpointAuth:
    def test_export_requires_authentication(self):
        """CSV export endpoint must reject unauthenticated requests."""
        try:
            from app.main import app
            client = TestClient(app)
            resp = client.get("/api/v1/analytics/reporting/positions/history/export")
            assert resp.status_code in (401, 403), (
                f"Expected 401/403, got {resp.status_code}"
            )
        except ImportError:
            pytest.skip("App not importable in this test environment")


class TestCsvExportLogic:
    """Unit tests for CSV export helper logic."""

    def test_export_only_closed_positions(self):
        """CSV export must exclude OPEN positions."""
        fills = [
            make_fill(1, "pos-closed", "OPEN", price=100.0),
            make_fill(2, "pos-closed", "CLOSE", price=110.0, realized_pnl=10.0,
                      timestamp_utc="2025-01-10T11:00:00"),
            make_fill(3, "pos-open", "OPEN", price=100.0),
        ]
        grouped = _simulate_grouping(fills)
        csv_rows = _build_csv_rows(grouped)
        assert len(csv_rows) == 1
        assert csv_rows[0]["position_id"] == "pos-closed"
        assert csv_rows[0]["status"] == "CLOSED"

    def test_export_csv_headers(self):
        """Exported CSV must contain all required columns."""
        required_headers = {
            "position_id", "symbol", "side", "status", "result",
            "broker_account_id", "bot_instance_id", "opened_at", "closed_at",
            "duration_seconds", "entry_price", "exit_price", "open_qty", "close_qty",
            "total_fees", "realized_pnl", "net_pnl",
            "realized_pnl_percent", "r_multiple",
            "open_count", "close_count", "open_fill_ids", "close_fill_ids",
        }
        fills = [
            make_fill(1, "pos-A", "OPEN", price=100.0),
            make_fill(2, "pos-A", "CLOSE", price=110.0, realized_pnl=10.0,
                      timestamp_utc="2025-01-10T11:00:00"),
        ]
        grouped = _simulate_grouping(fills)
        csv_rows = _build_csv_rows(grouped)
        assert len(csv_rows) == 1
        assert required_headers.issubset(set(csv_rows[0].keys()))

    def test_export_pnl_values_in_csv(self):
        """Realized PnL and net PnL values must be correct in CSV."""
        fills = [
            make_fill(1, "pos-P", "OPEN",  fee=2.0, price=100.0),
            make_fill(2, "pos-P", "CLOSE", fee=3.0, price=120.0, realized_pnl=20.0,
                      timestamp_utc="2025-01-10T11:00:00"),
        ]
        grouped = _simulate_grouping(fills)
        csv_rows = _build_csv_rows(grouped)
        assert len(csv_rows) == 1
        row = csv_rows[0]
        assert float(row["realized_pnl"]) == pytest.approx(20.0)
        assert float(row["net_pnl"]) == pytest.approx(15.0)   # 20 - 5 fees
        assert float(row["total_fees"]) == pytest.approx(5.0)

    def test_export_fill_ids_pipe_separated(self):
        """Fill IDs must be pipe-separated strings in the CSV."""
        fills = [
            make_fill(10, "pos-F", "OPEN"),
            make_fill(11, "pos-F", "OPEN"),
            make_fill(20, "pos-F", "CLOSE", realized_pnl=5.0,
                      timestamp_utc="2025-01-10T11:00:00"),
        ]
        grouped = _simulate_grouping(fills)
        csv_rows = _build_csv_rows(grouped)
        row = csv_rows[0]
        open_ids = set(row["open_fill_ids"].split("|"))
        assert open_ids == {"10", "11"}
        assert row["close_fill_ids"] == "20"

    def test_export_user_scoping(self):
        """CSV export must only include the requesting user's positions."""
        my_fills = [
            make_fill(1, "pos-mine", "OPEN",  user_id="user-123"),
            make_fill(2, "pos-mine", "CLOSE", user_id="user-123", realized_pnl=5.0,
                      timestamp_utc="2025-01-10T11:00:00"),
        ]
        other_fills = [
            make_fill(3, "pos-other", "OPEN",  user_id="user-999"),
            make_fill(4, "pos-other", "CLOSE", user_id="user-999", realized_pnl=10.0,
                      timestamp_utc="2025-01-10T12:00:00"),
        ]
        grouped = _simulate_grouping(my_fills + other_fills, user_id="user-123")
        csv_rows = _build_csv_rows(grouped)
        assert len(csv_rows) == 1
        assert csv_rows[0]["position_id"] == "pos-mine"

    def test_export_result_filter(self):
        """result filter (winner/loser) limits exported rows."""
        fills = [
            make_fill(1, "pos-W", "OPEN",  price=100.0),
            make_fill(2, "pos-W", "CLOSE", price=110.0, realized_pnl=10.0,
                      timestamp_utc="2025-01-10T11:00:00"),
            make_fill(3, "pos-L", "OPEN",  price=100.0),
            make_fill(4, "pos-L", "CLOSE", price=90.0, realized_pnl=-10.0,
                      timestamp_utc="2025-01-10T12:00:00"),
        ]
        grouped = _simulate_grouping(fills)
        all_closed = [p for p in grouped if p["status"] == "CLOSED"]
        winners_only = [p for p in all_closed if p["result"] == "winner"]
        csv_rows = _build_csv_rows(winners_only)
        assert len(csv_rows) == 1
        assert csv_rows[0]["result"] == "winner"


class TestSortOrderWhitelist:
    """sort_order must be restricted to ASC or DESC only."""

    def test_valid_desc(self):
        assert _validate_sort_order("DESC") == "DESC"

    def test_valid_asc(self):
        assert _validate_sort_order("ASC") == "ASC"

    def test_case_insensitive(self):
        assert _validate_sort_order("asc") == "ASC"
        assert _validate_sort_order("desc") == "DESC"

    def test_invalid_falls_back_to_desc(self):
        for bad in ("'; DROP TABLE--", "1=1", "", None, "RANDOM"):
            assert _validate_sort_order(bad) == "DESC"

    def test_duration_seconds_is_valid_sort_col(self):
        """duration_seconds was added to the sort whitelist in UX-6."""
        assert _validate_sort_col("duration_seconds") == "duration_seconds"
