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


class FakeBroker:
    def __init__(self) -> None:
        self.config = AlpacaExecutionConfig(
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


def test_run_trading_cycle_captures_decides_and_submits(tmp_path) -> None:
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
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

    assert result.symbols == ["AAPL"]
    assert len(result.snapshot_paths) == 1
    assert len(result.decisions) == 1
    assert len(result.orders_submitted) == 1


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
    assert result.skipped[0]["reason"] == "execution_disabled"


def test_run_trading_cycle_uses_persisted_open_positions_for_risk_count(tmp_path) -> None:
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
