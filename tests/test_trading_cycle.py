from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

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


class OverCapWithCheapFallbackDataClient(FakeDataClient):
    def get_option_chain_snapshots(self, symbol):
        payload = super().get_option_chain_snapshots(symbol)
        payload[f"{symbol}260703C00110000"] = {
            "latestQuote": {"bp": 0.77, "ap": 0.83, "t": "2026-07-01T15:35:00Z"},
            "greeks": {"delta": 0.47, "theta": -0.05, "vega": 0.08, "iv": 0.24},
            "details": {"expiration_date": "2026-07-03", "strike_price": 110, "type": "call"},
            "dailyBar": {"v": 120},
            "open_interest": 650,
        }
        return payload


class SpreadBacktestDataClient(FakeDataClient):
    def get_option_chain_snapshots(self, symbol):
        payload = super().get_option_chain_snapshots(symbol)
        payload[f"{symbol}260703P00104000"] = {
            "latestQuote": {"bp": 0.45, "ap": 0.50, "t": "2026-07-01T15:35:00Z"},
            "greeks": {"delta": -0.35, "theta": -0.02, "vega": 0.05, "iv": 0.25},
            "details": {"expiration_date": "2026-07-03", "strike_price": 104, "type": "put"},
            "dailyBar": {"v": 200},
            "open_interest": 1000,
        }
        payload[f"{symbol}260703P00103000"] = {
            "latestQuote": {"bp": 0.18, "ap": 0.22, "t": "2026-07-01T15:35:00Z"},
            "greeks": {"delta": -0.18, "theta": -0.02, "vega": 0.05, "iv": 0.25},
            "details": {"expiration_date": "2026-07-03", "strike_price": 103, "type": "put"},
            "dailyBar": {"v": 200},
            "open_interest": 1000,
        }
        return payload


class CoreRunnerDataClient(FakeDataClient):
    def get_option_chain_snapshots(self, symbol):
        payload = super().get_option_chain_snapshots(symbol)
        payload[f"{symbol}260703C00110000"] = {
            "latestQuote": {"bp": 0.62, "ap": 0.70, "t": "2026-07-01T15:35:00Z"},
            "greeks": {"delta": 0.35, "theta": -0.03, "vega": 0.06, "iv": 0.25},
            "details": {"expiration_date": "2026-07-03", "strike_price": 110, "type": "call"},
            "dailyBar": {"v": 250},
            "open_interest": 800,
        }
        payload[f"{symbol}260703C00115000"] = {
            "latestQuote": {"bp": 0.20, "ap": 0.24, "t": "2026-07-01T15:35:00Z"},
            "greeks": {"delta": 0.12, "theta": -0.01, "vega": 0.03, "iv": 0.28},
            "details": {"expiration_date": "2026-07-03", "strike_price": 115, "type": "call"},
            "dailyBar": {"v": 150},
            "open_interest": 500,
        }
        return payload


class RiskOffVolatilityDataClient(FakeDataClient):
    def get_stock_bars(self, symbols, *, start, end, timeframe="1Min", limit=35):
        bars = {}
        base_time = datetime(2026, 7, 1, 15, 0, tzinfo=UTC)
        for symbol in symbols:
            symbol = symbol.upper()
            start_price, step = {
                "VXX": (15.0, 0.20),
                "UVXY": (10.0, 0.50),
                "SPY": (600.0, -0.30),
                "QQQ": (500.0, -0.20),
            }.get(symbol, (100.0, 0.10))
            rows = []
            for index in range(limit):
                price = start_price + step * index
                rows.append(
                    {
                        "t": (base_time + timedelta(minutes=index)).isoformat().replace("+00:00", "Z"),
                        "o": price - step * 0.2,
                        "h": price + 0.08,
                        "l": price - 0.08,
                        "c": price,
                        "v": 100000 + index * 100,
                    }
                )
            bars[symbol] = rows
        return bars

    def get_latest_stock_quotes(self, symbols):
        prices = {"VXX": 21.8, "UVXY": 27.0, "SPY": 589.8, "QQQ": 493.2, "AAPL": 103.4}
        return {
            symbol.upper(): {
                "bp": prices[symbol.upper()] - 0.02,
                "ap": prices[symbol.upper()] + 0.02,
                "t": "2026-07-01T15:35:00Z",
            }
            for symbol in symbols
        }

    def get_option_chain_snapshots(self, symbol):
        if symbol.upper() == "AAPL":
            return {
                "AAPL260703C00105000": {
                    "latestQuote": {"bp": 2.40, "ap": 2.50, "t": "2026-07-01T15:35:00Z"},
                    "greeks": {"delta": 0.55, "theta": -0.05, "vega": 0.10, "iv": 0.30},
                    "details": {"expiration_date": "2026-07-03", "strike_price": 105, "type": "call"},
                    "open_interest": 5000,
                },
                "AAPL260703C00115000": {
                    "latestQuote": {"bp": 0.65, "ap": 0.70, "t": "2026-07-01T15:35:00Z"},
                    "greeks": {"delta": 0.20, "theta": -0.02, "vega": 0.06, "iv": 0.35},
                    "details": {"expiration_date": "2026-07-03", "strike_price": 115, "type": "call"},
                    "open_interest": 3500,
                },
            }
        return {
            "VXX260703C00022000": {
                "latestQuote": {"bp": 2.40, "ap": 2.50, "t": "2026-07-01T15:35:00Z"},
                "greeks": {"delta": 0.55, "theta": -0.05, "vega": 0.10, "iv": 0.80},
                "details": {"expiration_date": "2026-07-03", "strike_price": 22, "type": "call"},
                "open_interest": 5000,
            },
            "VXX260703C00025000": {
                "latestQuote": {"bp": 0.65, "ap": 0.70, "t": "2026-07-01T15:35:00Z"},
                "greeks": {"delta": 0.20, "theta": -0.02, "vega": 0.06, "iv": 0.90},
                "details": {"expiration_date": "2026-07-03", "strike_price": 25, "type": "call"},
                "open_interest": 3500,
            },
        }


@pytest.fixture(autouse=True)
def _legacy_single_leg_default_for_existing_cycle_contracts(monkeypatch):
    monkeypatch.setenv("AUTOBOTT_CORE_RUNNER_ENABLED", "false")


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
        self.mleg_calls = []

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

    def submit_mleg_order(self, intents, *, current_daily_realized_pnl=0.0, open_positions=0):
        self.mleg_calls.append(tuple(intents))
        return tuple(
            self.submit_order(
                intent,
                current_daily_realized_pnl=current_daily_realized_pnl,
                open_positions=open_positions + index,
            )
            for index, intent in enumerate(intents)
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


def test_unqualified_pair_chain_does_not_force_primary_and_runner(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("AUTOBOTT_CORE_RUNNER_ENABLED", "true")
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
            data_client=CoreRunnerDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original
        trading_cycle.load_open_positions = original_positions

    assert result.trade_attempted_count == 0
    assert result.orders_submitted == []
    assert broker.mleg_calls == []


def test_risk_off_does_not_manufacture_bullish_volatility_hedge(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("AUTOBOTT_CORE_RUNNER_ENABLED", "true")
    monkeypatch.setenv("AUTOBOTT_VOLATILITY_HEDGE_SYMBOLS", "VXX,UVXY")
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    original = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    trading_cycle.load_runtime_state = lambda: original(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: []
    broker = FakeBroker()
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["VXX"],
            broker=broker,
            data_client=RiskOffVolatilityDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original
        trading_cycle.load_open_positions = original_positions

    assert result.decisions[0]["decision"] == "BLOCKED_BY_REGIME"
    assert len(result.decisions) == 1
    assert result.trade_attempted_count == 0
    assert result.orders_submitted == []
    assert broker.mleg_calls == []


def test_risk_off_still_blocks_ordinary_bullish_equity_in_paper(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("AUTOBOTT_CORE_RUNNER_ENABLED", "true")
    monkeypatch.setenv("AUTOBOTT_VOLATILITY_HEDGE_SYMBOLS", "VXX,UVXY")
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    original = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    trading_cycle.load_runtime_state = lambda: original(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: []
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL"],
            broker=FakeBroker(),
            data_client=RiskOffVolatilityDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original
        trading_cycle.load_open_positions = original_positions

    assert len(result.decisions) == 1
    assert result.decisions[0]["decision"] == "BLOCKED_BY_REGIME"
    assert result.orders_submitted == []


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


def test_run_trading_cycle_paper_ignores_real_money_cost_limit(tmp_path) -> None:
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    original = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    trading_cycle.load_runtime_state = lambda: original(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: []
    broker = FakeBroker(max_position_cost=100.0, paper_ignore_position_cost_limit=True)
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
    assert result.trade_attempted_count == 1
    assert len(result.orders_submitted) == 1
    assert broker.submitted[0].option_symbol == "AAPL260703C00105000"
    assert broker.submitted[0].limit_price == 2.6
    assert not (tmp_path / "ghost_trades.jsonl").exists()


def test_run_trading_cycle_keeps_cost_limit_when_paper_bypass_is_off(tmp_path) -> None:
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

    assert result.scanner_candidates_count == 1
    assert result.trade_attempted_count == 0
    assert result.orders_submitted == []
    assert result.skipped[0]["reason"] == "position_cost_exceeds_limit"


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


def test_run_trading_cycle_routes_to_ghost_when_open_basket_drawdown_is_bad(tmp_path) -> None:
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    original = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    trading_cycle.load_runtime_state = lambda: original(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: []
    broker = FakeBrokerWithLivePositions(
        [
            {"symbol": "C260710P00139000", "side": "long", "qty": "1", "current_price": "0.84", "avg_entry_price": "0.96", "unrealized_pl": "-12", "unrealized_plpc": "-0.125"},
            {"symbol": "DIA260710C00526000", "side": "long", "qty": "1", "current_price": "0.69", "avg_entry_price": "0.90", "unrealized_pl": "-21", "unrealized_plpc": "-0.20"},
            {"symbol": "IWM260710C00298000", "side": "long", "qty": "1", "current_price": "0.81", "avg_entry_price": "0.87", "unrealized_pl": "-6", "unrealized_plpc": "-0.07"},
            {"symbol": "TLT260710C00084500", "side": "long", "qty": "1", "current_price": "0.20", "avg_entry_price": "0.18", "unrealized_pl": "2", "unrealized_plpc": "0.11"},
        ],
        max_open_positions=25,
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
    assert result.skipped[0]["reason"] == "open_drawdown_guard"
    assert result.execution_rejected_count_by_reason == {"open_drawdown_guard": 1}
    guard = next(row for row in result.execution_outcomes if row["disposition"] == "open_drawdown_guard_summary")
    assert guard["blocked"] is True
    assert "ghost_entry" in (tmp_path / "ghost_trades.jsonl").read_text(encoding="utf-8")


def test_run_trading_cycle_can_disable_single_leg_real_entries(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("AUTOBOTT_SINGLE_LEG_REAL_ENTRIES_DISABLED", "true")
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

    assert result.scanner_candidates_count == 1
    assert result.trade_attempted_count == 0
    assert result.orders_submitted == []
    assert result.skipped[0]["reason"] == "single_leg_real_entries_disabled"
    assert result.execution_rejected_count_by_reason == {"single_leg_real_entries_disabled": 1}
    assert "ghost_entry" in (tmp_path / "ghost_trades.jsonl").read_text(encoding="utf-8")


def test_run_trading_cycle_records_defined_risk_spread_backtest_candidate(tmp_path) -> None:
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    original = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    trading_cycle.load_runtime_state = lambda: original(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: []
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL"],
            broker=FakeBroker(),
            data_client=SpreadBacktestDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.load_runtime_state = original
        trading_cycle.load_open_positions = original_positions

    spread = next(row for row in result.execution_outcomes if row["disposition"] == "defined_risk_spread_backtest_candidate")
    assert spread["strategy"] == "bull_put_spread"
    assert spread["max_risk"] == 77.0
    assert "defined_risk_spread.v1" in (tmp_path / "defined_risk_spreads.jsonl").read_text(encoding="utf-8")


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

def test_paper_environment_cannot_turn_neutral_cycle_into_order(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("AUTOBOTT_CORE_RUNNER_ENABLED", "true")
    monkeypatch.setenv("AUTOBOTT_PAPER_OPPORTUNISTIC_ENTRIES", "true")
    monkeypatch.setenv("AUTOBOTT_PAPER_DIRECTIONAL_DISCOVERY", "true")
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    original_runtime = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    original_build = trading_cycle.build_decision_card
    trading_cycle.load_runtime_state = lambda: original_runtime(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: []

    def neutral_build(decision_input, rules=None):
        card = original_build(decision_input, rules)
        return replace(
            card,
            direction=replace(
                card.direction,
                bias=trading_cycle.DirectionBias.NEUTRAL,
                score=0.0,
            ),
            selected_contract=None,
            tactical_contract=None,
            rider_contract=None,
            trade_setup=trading_cycle.TradeSetup.NO_TRADE,
            execution_layer=trading_cycle.ExecutionLayer.NONE,
            decision=trading_cycle.DecisionStatus.NO_TRADE,
            blocked_reason="direction_not_strong_enough",
        )

    trading_cycle.build_decision_card = neutral_build
    broker = FakeBroker()
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL"],
            broker=broker,
            data_client=CoreRunnerDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.build_decision_card = original_build
        trading_cycle.load_runtime_state = original_runtime
        trading_cycle.load_open_positions = original_positions

    assert result.decisions[0]["decision"] == "NO_TRADE"
    assert len(result.decisions) == 1
    assert result.trade_attempted_count == 0
    assert result.orders_submitted == []
    assert broker.mleg_calls == []

def test_paper_environment_records_regime_block_without_order(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("AUTOBOTT_CORE_RUNNER_ENABLED", "true")
    monkeypatch.setenv("AUTOBOTT_PAPER_OPPORTUNISTIC_ENTRIES", "true")
    monkeypatch.setenv("AUTOBOTT_PAPER_DIRECTIONAL_DISCOVERY", "true")
    save_runtime_state(default_runtime_state(), state_path=tmp_path / "runtime_state.json")
    original_runtime = trading_cycle.load_runtime_state
    original_positions = trading_cycle.load_open_positions
    original_build = trading_cycle.build_decision_card
    trading_cycle.load_runtime_state = lambda: original_runtime(state_path=tmp_path / "runtime_state.json")
    trading_cycle.load_open_positions = lambda: []

    def regime_blocked_build(decision_input, rules=None):
        card = original_build(decision_input, rules)
        return replace(
            card,
            selected_contract=None,
            tactical_contract=None,
            rider_contract=None,
            trade_setup=trading_cycle.TradeSetup.NO_TRADE,
            execution_layer=trading_cycle.ExecutionLayer.NONE,
            decision=trading_cycle.DecisionStatus.BLOCKED_BY_REGIME,
            blocked_reason="risk_off_regime",
        )

    trading_cycle.build_decision_card = regime_blocked_build
    broker = FakeBroker()
    try:
        result = trading_cycle.run_trading_cycle(
            symbols=["AAPL"],
            broker=broker,
            data_client=CoreRunnerDataClient(),
            scheduled_market_time=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            captured_at_utc=datetime(2026, 7, 1, 15, 35, tzinfo=UTC),
            corpus_root=tmp_path / "corpus",
            decision_log_path=tmp_path / "decision_cards.jsonl",
            execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        )
    finally:
        trading_cycle.build_decision_card = original_build
        trading_cycle.load_runtime_state = original_runtime
        trading_cycle.load_open_positions = original_positions

    assert result.decisions[0]["decision"] == "BLOCKED_BY_REGIME"
    assert len(result.decisions) == 1
    assert len(result.snapshot_paths) == 1
    assert any(row["disposition"] == "trade_outcome_learning_summary" for row in result.execution_outcomes)
    assert not any(row["disposition"] == "pass_trade_attempted" for row in result.execution_outcomes)
    assert result.trade_attempted_count == 0
    assert result.orders_submitted == []
    assert broker.mleg_calls == []
