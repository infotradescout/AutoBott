from __future__ import annotations

from datetime import UTC, datetime, timedelta

import autobott_v2.trading_cycle as trading_cycle
from autobott_v2.execution_config import AlpacaExecutionConfig
from autobott_v2.execution_models import BrokerEnvironment, ExecutionOrder, ExecutionState
from autobott_v2.position_store import OpenPosition, save_open_positions
from autobott_v2.runtime_control import default_runtime_state, save_runtime_state, set_kill_switch


class FakeDataClient:
    def get_stock_bars(self, symbols, *, start, end, timeframe="1Min", limit=35):
        bars = {}
        base = datetime(2026, 7, 1, 15, 0, tzinfo=UTC)
        for idx, symbol in enumerate(symbols):
            rows = []
            price = 100.0 + idx * 10
            for i in range(limit):
                rows.append(
                    {
                        "t": (base + timedelta(minutes=i)).isoformat().replace("+00:00", "Z"),
                        "o": price + i * 0.1,
                        "h": price + i * 0.15,
                        "l": price + i * 0.05,
                        "c": price + i * 0.12,
                        "v": 100000 + i,
                    }
                )
            bars[symbol] = rows
        return bars

    def get_latest_stock_quotes(self, symbols):
        return {
            symbol: {"bp": 104.9 + idx, "ap": 105.1 + idx, "t": "2026-07-01T15:35:00Z"}
            for idx, symbol in enumerate(symbols)
        }

    def get_option_chain_snapshots(self, symbol):
        return {
            f"{symbol}260703C00105000": {
                "latestQuote": {"bp": 2.4, "ap": 2.6, "t": "2026-07-01T15:35:00Z"},
                "greeks": {"delta": 0.55, "theta": -0.05, "vega": 0.10, "iv": 0.22},
                "details": {"expiration_date": "2026-07-03", "strike_price": 105, "type": "call"},
                "dailyBar": {"v": 500},
                "open_interest": 1000,
            },
            f"{symbol}260703P00105000": {
                "latestQuote": {"bp": 2.2, "ap": 2.4, "t": "2026-07-01T15:35:00Z"},
                "greeks": {"delta": -0.45, "theta": -0.05, "vega": 0.09, "iv": 0.24},
                "details": {"expiration_date": "2026-07-03", "strike_price": 105, "type": "put"},
                "dailyBar": {"v": 500},
                "open_interest": 1000,
            },
        }


class WideSpreadDataClient(FakeDataClient):
    def get_option_chain_snapshots(self, symbol):
        payload = super().get_option_chain_snapshots(symbol)
        for row in payload.values():
            row["latestQuote"]["bp"] = 2.0
            row["latestQuote"]["ap"] = 3.0
        return payload


class FakeBroker:
    def __init__(self, **config_overrides) -> None:
        base = AlpacaExecutionConfig(
            environment=BrokerEnvironment.PAPER,
            api_key="paper-key",
            secret_key="paper-secret",
            trading_base_url="https://paper-api.alpaca.markets",
            data_base_url="https://data.alpaca.markets",
            allow_live_trading=False,
            allow_order_placement=True,
            max_position_cost=1000.0,
            max_daily_loss=500.0,
            max_open_positions=3,
        )
        self.config = AlpacaExecutionConfig(**(base.__dict__ | config_overrides))
        self.submitted = []
        self.open_positions_seen = []

    def submit_order(self, intent, *, current_daily_realized_pnl=0.0, open_positions=0):
        self.submitted.append(intent)
        self.open_positions_seen.append(open_positions)
        return ExecutionOrder(
            order_id=f"order-{len(self.submitted)}",
            client_order_id=f"client-{len(self.submitted)}",
            intent=intent,
            state=ExecutionState.SUBMITTED,
            submitted_at=datetime(2026, 7, 1, 15, 36, tzinfo=UTC),
            broker_order_id=f"alpaca-order-{len(self.submitted)}",
        )


class FakeBrokerWithLivePositions(FakeBroker):
    def __init__(self, positions, **config_overrides) -> None:
        super().__init__(**config_overrides)
        self.positions = positions

    def list_open_positions(self):
        return self.positions


class FakeBrokerWithLivePositionsAndOrders(FakeBrokerWithLivePositions):
    def __init__(self, positions, orders, **config_overrides) -> None:
        super().__init__(positions, **config_overrides)
        self.orders = orders

    def list_orders(self, *, status="open", limit=100, direction="desc"):
        return self.orders


class FakeBrokerWithAllOrders(FakeBroker):
    def __init__(self, orders, **config_overrides) -> None:
        super().__init__(**config_overrides)
        self.orders = orders

    def list_orders(self, *, status="open", limit=100, direction="desc"):
        return self.orders


def test_run_trading_cycle_captures_decides_and_submits(tmp_path) -> None:
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    original = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    trading_cycle.load_runtime_state = lambda: original(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: []
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL"],
            broker=FakeBroker(),
            data_client=FakeDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original
        trading_cycle.load_open_positions = original_positions

    assert result.symbols == ["AAPL"]
    assert len(result.snapshot_paths) == 1
    assert len(result.decisions) == 1
    assert len(result.orders_submitted) == 1
    assert result.scanner_candidates_count == 1
    assert result.trade_attempted_count == 1
    assert result.zero_trade_cycle is False
    dispositions = [outcome["disposition"] for outcome in result.execution_outcomes]
    assert "position_monitor_summary" in dispositions
    assert "scanner_candidate" in dispositions
    assert "pass_trade_attempted" in dispositions


def test_run_trading_cycle_skips_when_kill_switch_enabled(tmp_path) -> None:
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    set_kill_switch(True, reason="manual_stop", state_path=tmp_path / "runtime_state.json")
    original = trading_cycle.load_runtime_state
    trading_cycle.load_runtime_state = lambda: original(state_path=tmp_path / "runtime_state.json")
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL"],
            broker=FakeBroker(),
            data_client=FakeDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original

    assert result.orders_submitted == []
    assert result.skipped[0]["reason"] == "kill_switch_enabled"
    assert result.execution_rejected_count_by_reason == {"kill_switch_enabled": 1}
    assert result.trade_attempted_count == 0
    assert result.zero_trade_cycle is False


def test_run_trading_cycle_uses_persisted_open_positions_for_risk_count(tmp_path) -> None:
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    save_open_positions(
        [
            OpenPosition(
                broker_order_id="alpaca-order-1",
                decision_id="decision-1",
                symbol="MSFT",
                option_symbol="MSFT260703C00105000",
                quantity=1,
                entry_limit_price=2.5,
                entry_submitted_at=datetime(2026, 7, 1, 15, 31, tzinfo=UTC),
                take_profit_price=3.75,
                stop_loss_price=1.75,
                status="filled",
            )
        ],
        store_path=tmp_path / "open_positions.json",
    )
    original_runtime = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    original_reconcile = trading_cycle.reconcile_open_positions
    trading_cycle.load_runtime_state = lambda: original_runtime(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: original_positions(store_path=tmp_path / "open_positions.json")
    trading_cycle.reconcile_open_positions = lambda *args, **kwargs: None
    broker = FakeBroker()
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL"],
            broker=broker,
            data_client=FakeDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original_runtime
        trading_cycle.load_open_positions = original_positions
        trading_cycle.reconcile_open_positions = original_reconcile

    assert len(result.orders_submitted) == 1
    assert broker.submitted
    assert broker.open_positions_seen == [1]


def test_run_trading_cycle_prefers_live_broker_position_count_over_stale_store(tmp_path) -> None:
    # The local open_positions.json store never removes entries when a
    # position is closed by the monitor, so it drifts upward forever. The
    # risk-count used for sizing new entries must come from the broker's
    # live truth instead, not that ever-growing local file.
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    save_open_positions(
        [
            OpenPosition(
                broker_order_id=f"alpaca-order-{i}",
                decision_id=f"decision-{i}",
                symbol="MSFT",
                option_symbol="MSFT260703C00105000",
                quantity=1,
                entry_limit_price=2.5,
                entry_submitted_at=datetime(2026, 7, 1, 15, 31, tzinfo=UTC),
                take_profit_price=3.75,
                stop_loss_price=1.75,
                status="filled",
            )
            for i in range(3)
        ],
        store_path=tmp_path / "open_positions.json",
    )
    original_runtime = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    original_reconcile = trading_cycle.reconcile_open_positions
    trading_cycle.load_runtime_state = lambda: original_runtime(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: original_positions(store_path=tmp_path / "open_positions.json")
    trading_cycle.reconcile_open_positions = lambda *args, **kwargs: None
    broker = FakeBrokerWithLivePositions([{"symbol": "MSFT260703C00105000", "side": "long", "qty": "1"}])
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL"],
            broker=broker,
            data_client=FakeDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original_runtime
        trading_cycle.load_open_positions = original_positions
        trading_cycle.reconcile_open_positions = original_reconcile

    assert len(result.orders_submitted) == 1
    assert broker.open_positions_seen == [1]


def test_run_trading_cycle_skips_symbol_with_existing_active_underlying(tmp_path) -> None:
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    save_open_positions(
        [
            OpenPosition(
                broker_order_id="alpaca-order-1",
                decision_id="decision-1",
                symbol="AAPL",
                option_symbol="AAPL260703C00105000",
                quantity=1,
                entry_limit_price=2.5,
                entry_submitted_at=datetime(2026, 7, 1, 15, 31, tzinfo=UTC),
                take_profit_price=3.75,
                stop_loss_price=1.75,
                status="filled",
            )
        ],
        store_path=tmp_path / "open_positions.json",
    )
    original_runtime = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    original_reconcile = trading_cycle.reconcile_open_positions
    trading_cycle.load_runtime_state = lambda: original_runtime(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: original_positions(store_path=tmp_path / "open_positions.json")
    trading_cycle.reconcile_open_positions = lambda *args, **kwargs: None
    broker = FakeBroker()
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL", "MSFT"],
            broker=broker,
            data_client=FakeDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original_runtime
        trading_cycle.load_open_positions = original_positions
        trading_cycle.reconcile_open_positions = original_reconcile

    assert len(result.orders_submitted) == 1
    assert result.orders_submitted[0]["symbol"] == "MSFT"
    assert result.skipped[0]["symbol"] == "AAPL"
    assert result.skipped[0]["reason"] == "underlying_exposure_already_open"
    assert result.execution_rejected_count_by_reason == {"underlying_exposure_already_open": 1}


def test_run_trading_cycle_uses_live_broker_positions_for_underlying_guard(tmp_path) -> None:
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    original_runtime = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    original_reconcile = trading_cycle.reconcile_open_positions
    trading_cycle.load_runtime_state = lambda: original_runtime(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: []
    trading_cycle.reconcile_open_positions = lambda *args, **kwargs: None
    broker = FakeBrokerWithLivePositions(
        [
            {
                "symbol": "AAPL260703C00105000",
                "side": "long",
                "qty": "1",
                "current_price": "2.50",
                "avg_entry_price": "2.50",
                "unrealized_plpc": "0.00",
            }
        ]
    )
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL", "MSFT"],
            broker=broker,
            data_client=FakeDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original_runtime
        trading_cycle.load_open_positions = original_positions
        trading_cycle.reconcile_open_positions = original_reconcile

    assert len(result.orders_submitted) == 1
    assert result.orders_submitted[0]["symbol"] == "MSFT"
    assert result.skipped[0]["symbol"] == "AAPL"
    assert result.skipped[0]["reason"] == "underlying_exposure_already_open"
    assert result.execution_rejected_count_by_reason == {"underlying_exposure_already_open": 1}


def test_run_trading_cycle_uses_pending_buy_orders_for_underlying_guard(tmp_path) -> None:
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    original_runtime = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    original_reconcile = trading_cycle.reconcile_open_positions
    trading_cycle.load_runtime_state = lambda: original_runtime(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: []
    trading_cycle.reconcile_open_positions = lambda *args, **kwargs: None
    broker = FakeBrokerWithLivePositionsAndOrders(
        [],
        [
            {
                "symbol": "AAPL260703C00105000",
                "side": "buy",
                "qty": "1",
                "filled_qty": "0",
                "status": "new",
            }
        ],
    )
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL", "MSFT"],
            broker=broker,
            data_client=FakeDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original_runtime
        trading_cycle.load_open_positions = original_positions
        trading_cycle.reconcile_open_positions = original_reconcile

    assert len(result.orders_submitted) == 1
    assert result.orders_submitted[0]["symbol"] == "MSFT"
    assert result.skipped[0]["symbol"] == "AAPL"
    assert result.skipped[0]["reason"] == "underlying_exposure_already_open"
    assert result.execution_rejected_count_by_reason == {"underlying_exposure_already_open": 1}


def test_run_trading_cycle_records_exact_execution_rejection_reason(tmp_path) -> None:
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    original = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    trading_cycle.load_runtime_state = lambda: original(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: []
    broker = FakeBroker(allow_order_placement=False)
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL"],
            broker=broker,
            data_client=FakeDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original
        trading_cycle.load_open_positions = original_positions

    assert result.orders_submitted == []
    assert result.trade_attempted_count == 0
    assert result.execution_rejected_count_by_reason == {"order_placement_disabled": 1}
    assert result.skipped[0]["reason"] == "order_placement_disabled"
    assert result.zero_trade_cycle is True


def test_run_trading_cycle_paper_trade_through_allows_multiple_attempts(tmp_path) -> None:
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    original = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    trading_cycle.load_runtime_state = lambda: original(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: []
    broker = FakeBroker(
        max_open_positions=1,
        paper_trade_all_passed_signals=True,
        paper_max_open_entry_buy_orders=25,
        paper_max_new_entry_attempts_per_loop=25,
    )
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL", "MSFT", "NVDA", "QQQ"],
            broker=broker,
            data_client=FakeDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original
        trading_cycle.load_open_positions = original_positions

    assert result.scanner_candidates_count == 4
    assert result.trade_attempted_count == 4
    assert len(result.orders_submitted) == 4
    assert broker.open_positions_seen == [0, 1, 2, 3]


def test_run_trading_cycle_uses_recent_loss_guard(tmp_path) -> None:
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    original = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    trading_cycle.load_runtime_state = lambda: original(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: []
    broker = FakeBrokerWithAllOrders(
        [
            {
                "symbol": "AAPL260703C00105000",
                "side": "buy",
                "qty": "1",
                "filled_qty": "1",
                "filled_avg_price": "2.00",
                "status": "filled",
                "submitted_at": "2026-07-01T14:00:00Z",
                "filled_at": "2026-07-01T14:00:00Z",
            },
            {
                "symbol": "AAPL260703C00105000",
                "side": "sell",
                "qty": "1",
                "filled_qty": "1",
                "filled_avg_price": "1.00",
                "status": "filled",
                "submitted_at": "2026-07-01T14:15:00Z",
                "filled_at": "2026-07-01T14:15:00Z",
            },
            {
                "symbol": "AAPL260703C00105000",
                "side": "buy",
                "qty": "1",
                "filled_qty": "1",
                "filled_avg_price": "2.00",
                "status": "filled",
                "submitted_at": "2026-07-01T14:30:00Z",
                "filled_at": "2026-07-01T14:30:00Z",
            },
            {
                "symbol": "AAPL260703C00105000",
                "side": "sell",
                "qty": "1",
                "filled_qty": "1",
                "filled_avg_price": "1.20",
                "status": "filled",
                "submitted_at": "2026-07-01T14:45:00Z",
                "filled_at": "2026-07-01T14:45:00Z",
            },
        ]
    )
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL"],
            broker=broker,
            data_client=FakeDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original
        trading_cycle.load_open_positions = original_positions

    assert result.scanner_candidates_count == 1
    assert result.trade_attempted_count == 0
    assert result.orders_submitted == []
    assert result.skipped[0]["reason"] == "recent_loss_guard"
    assert result.execution_rejected_count_by_reason == {"recent_loss_guard": 1}


def test_run_trading_cycle_splits_expensive_candidate_to_ghost_lane(tmp_path) -> None:
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    original = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    trading_cycle.load_runtime_state = lambda: original(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: []
    broker = FakeBroker(max_position_cost=100.0)
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL"],
            broker=broker,
            data_client=FakeDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original
        trading_cycle.load_open_positions = original_positions

    ghost_rows = (tmp_path / "ghost_trades.jsonl").read_text(encoding="utf-8")
    assert result.scanner_candidates_count == 1
    assert result.trade_attempted_count == 0
    assert result.orders_submitted == []
    assert result.skipped[0]["reason"] == "contract_cost_cap_ghost_tracked"
    assert "ghost_entry" in ghost_rows


def test_run_trading_cycle_prioritizes_recent_winners(tmp_path) -> None:
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    original = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    trading_cycle.load_runtime_state = lambda: original(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: []
    broker = FakeBrokerWithAllOrders(
        [
            {
                "symbol": "MSFT260703C00105000",
                "side": "buy",
                "qty": "1",
                "filled_qty": "1",
                "filled_avg_price": "2.00",
                "status": "filled",
                "submitted_at": "2026-07-01T14:00:00Z",
                "filled_at": "2026-07-01T14:00:00Z",
            },
            {
                "symbol": "MSFT260703C00105000",
                "side": "sell",
                "qty": "1",
                "filled_qty": "1",
                "filled_avg_price": "3.00",
                "status": "filled",
                "submitted_at": "2026-07-01T14:15:00Z",
                "filled_at": "2026-07-01T14:15:00Z",
            },
            {
                "symbol": "MSFT260703C00105000",
                "side": "buy",
                "qty": "1",
                "filled_qty": "1",
                "filled_avg_price": "2.00",
                "status": "filled",
                "submitted_at": "2026-07-01T14:30:00Z",
                "filled_at": "2026-07-01T14:30:00Z",
            },
            {
                "symbol": "MSFT260703C00105000",
                "side": "sell",
                "qty": "1",
                "filled_qty": "1",
                "filled_avg_price": "2.80",
                "status": "filled",
                "submitted_at": "2026-07-01T14:45:00Z",
                "filled_at": "2026-07-01T14:45:00Z",
            },
        ]
    )
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL", "MSFT"],
            broker=broker,
            data_client=FakeDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original
        trading_cycle.load_open_positions = original_positions

    assert result.symbols == ["MSFT", "AAPL"]
    learning = next(row for row in result.execution_outcomes if row["disposition"] == "trade_outcome_learning_summary")
    assert learning["winner_bias"]["preferred_underlyings"] == ["MSFT"]


def test_run_trading_cycle_paper_opportunistic_mode_does_not_override_spread_block(tmp_path, monkeypatch) -> None:
    # Liquidity is a hard floor, not a discovery-mode toggle: a contract wide
    # enough to be BLOCKED_BY_SPREAD under the strict engine bleeds the same
    # real cost in paper as it would live, so opportunistic mode must not
    # rescue it. See _paper_opportunistic_rules().
    monkeypatch.delenv("AUTOBOTT_PAPER_OPPORTUNISTIC_ENTRIES", raising=False)
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    original = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    trading_cycle.load_runtime_state = lambda: original(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: []
    broker = FakeBroker()
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL"],
            broker=broker,
            data_client=WideSpreadDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original
        trading_cycle.load_open_positions = original_positions

    assert result.decisions[0]["decision"] == "BLOCKED_BY_SPREAD"
    assert len(result.decisions) == 1
    assert result.scanner_candidates_count == 0
    assert result.trade_attempted_count == 0
    assert result.orders_submitted == []


def test_run_trading_cycle_paper_opportunistic_mode_can_be_disabled(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("AUTOBOTT_PAPER_OPPORTUNISTIC_ENTRIES", "false")
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    original = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    trading_cycle.load_runtime_state = lambda: original(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: []
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL"],
            broker=FakeBroker(),
            data_client=WideSpreadDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original
        trading_cycle.load_open_positions = original_positions

    assert result.decisions[0]["decision"] == "BLOCKED_BY_SPREAD"
    assert result.trade_attempted_count == 0
    assert result.orders_submitted == []
