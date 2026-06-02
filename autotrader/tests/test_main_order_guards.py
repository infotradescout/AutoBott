"""Regression tests for main entry order guardrails."""

from __future__ import annotations

import sys
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytz

_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

import config  # noqa: E402
import main  # noqa: E402


EASTERN = pytz.timezone(config.EASTERN_TZ)


class FakeOrder:
    def __init__(
        self,
        symbol: str,
        side: str,
        status: str,
        submitted_at: datetime,
        *,
        qty: int = 1,
        limit_price: float | None = None,
    ):
        self.id = f"fake-{symbol}-{submitted_at.timestamp()}"
        self.symbol = symbol
        self.side = side
        self.status = status
        self.submitted_at = submitted_at
        self.qty = qty
        self.limit_price = limit_price
        self.filled_qty = 0
        self.filled_avg_price = None


class FakeEnumValue:
    def __init__(self, value: str):
        self.value = value


class FakeRawSubmittedOrder:
    def __init__(
        self,
        symbol: str,
        side: str,
        status: str,
        submitted_at,
        *,
        qty: int = 1,
        limit_price: float | None = None,
        filled_qty: int = 0,
        filled_avg_price: float | None = None,
    ):
        self.id = f"fake-{symbol}-{abs(hash(str(submitted_at)))}"
        self.symbol = symbol
        self.side = side
        self.status = status
        self.submitted_at = submitted_at
        self.qty = qty
        self.limit_price = limit_price
        self.filled_qty = filled_qty
        self.filled_avg_price = filled_avg_price


class FakeBroker:
    def __init__(self, orders):
        self._orders = list(orders)
        self.canceled_order_ids = []

    def get_recent_orders(self, limit: int = 500):
        return self._orders[:limit]

    def cancel_order(self, order_id: str):
        self.canceled_order_ids.append(order_id)


class FakePosition:
    def __init__(self, symbol: str, qty: int = 1, underlying_symbol: str | None = None):
        self.symbol = symbol
        self.qty = str(qty)
        self.underlying_symbol = underlying_symbol


class FakeEntryBroker:
    def __init__(self, status: str = "new"):
        self.order = FakeOrder("ORCL260515C00195000", "buy", status, EASTERN.localize(datetime(2026, 5, 11, 13, 10, 0)))
        self.canceled = 0
        self.submitted = 0

    def place_option_limit_buy(self, option_symbol: str, qty: int, ask_price: float):
        self.submitted += 1
        self.order.symbol = option_symbol
        return self.order

    def get_order_status(self, order_id: str):
        return self.order

    def cancel_order(self, order_id: str):
        self.canceled += 1


class FakeBarsDataClient:
    def __init__(self, closes: list[float]):
        self.closes = closes

    def get_intraday_bars_since_open(self, **_kwargs):
        return pd.DataFrame({"close": self.closes})


class FakeDataClient:
    def get_latest_option_quote(self, option_symbol: str):
        return {"bid": 5.0, "ask": 5.04}


class FakeCloseDataClient(FakeDataClient):
    def __init__(self, bid: float | None = 5.0, ask: float | None = 5.04):
        self.bid = bid
        self.ask = ask

    def get_latest_option_quote(self, option_symbol: str):
        return {"bid": self.bid, "ask": self.ask}


class FakeExitBroker:
    def __init__(self, open_orders: list[FakeOrder] | None = None):
        self._open_orders = list(open_orders or [])
        self._status_by_id: dict[str, object] = {order.id: order for order in self._open_orders}
        self.canceled_order_ids: list[str] = []
        self.actions: list[tuple] = []

    def get_open_orders_for_symbol(self, symbol: str, side: str):
        return [order for order in self._open_orders if str(getattr(order, "side", "")).lower() == str(side).lower()]

    def get_order_status(self, order_id: str):
        if order_id in self._status_by_id:
            return self._status_by_id[order_id]
        fallback_time = EASTERN.localize(datetime(2026, 5, 11, 9, 0, 0))
        return FakeOrder(
            "ORCL260515C00195000",
            "sell",
            "filled",
            fallback_time.replace(tzinfo=None),
            qty=1,
        )

    def cancel_order(self, order_id: str):
        self.canceled_order_ids.append(order_id)
        self.actions.append(("cancel", order_id))
        self._open_orders = [order for order in self._open_orders if str(order.id) != str(order_id)]

    def place_option_limit_sell(self, symbol: str, qty: int, limit_price: float):
        order = FakeOrder(
            symbol,
            "sell",
            "filled",
            EASTERN.localize(datetime(2026, 5, 11, 9, 0, 1)),
            qty=qty,
            limit_price=limit_price,
        )
        order.filled_qty = 0
        self.actions.append(("limit", order.id, symbol, qty, limit_price))
        self._status_by_id[order.id] = order
        self._open_orders.append(order)
        return order

    def close_option_market(self, symbol: str, qty: int):
        order = FakeOrder(
            symbol,
            "sell",
            "filled",
            EASTERN.localize(datetime(2026, 5, 11, 9, 0, 2)),
            qty=qty,
            limit_price=None,
        )
        order.filled_qty = qty
        order.filled_avg_price = 10.0
        self.actions.append(("market", order.id, symbol, qty))
        self._status_by_id[order.id] = order
        return order


class FakeUniverseDataClient:
    def __init__(self):
        self.assets = {
            "MSFT": {"tradable": True, "status": "active"},
            "NVDA": {"tradable": True, "options_enabled": True, "status": "active"},
        }
        self.prices = {"MSFT": 420.0, "NVDA": 900.0}

    def get_top_movers(self, top: int = 20):
        return ["MSFT"], ["NVDA"]

    def get_asset(self, symbol: str):
        return self.assets[symbol]

    def get_latest_stock_price(self, symbol: str):
        return self.prices.get(symbol)

    def get_all_optionable_tickers(self, *, max_count: int | None = None):
        return ["MSFT", "NVDA", "TSLA", "META", "AMZN"]


class MainOrderGuardTests(unittest.TestCase):
    def setUp(self):
        self._config_values = {
            "ALPACA_BUY_ORDER_CAP_COUNTS_CANCELED": config.ALPACA_BUY_ORDER_CAP_COUNTS_CANCELED,
            "ALPACA_CANCELED_BUY_ORDER_COOLDOWN_MINUTES": config.ALPACA_CANCELED_BUY_ORDER_COOLDOWN_MINUTES,
            "ANTI_CHURN_HOLD_MINUTES": config.ANTI_CHURN_HOLD_MINUTES,
            "MIN_HOLD_EXIT_BYPASS_REASONS": config.MIN_HOLD_EXIT_BYPASS_REASONS,
            "ENTRY_ORDER_STATUS_WAIT_SECONDS": config.ENTRY_ORDER_STATUS_WAIT_SECONDS,
            "ENTRY_LIMIT_ATTEMPTS": config.ENTRY_LIMIT_ATTEMPTS,
            "CANCEL_UNFILLED_ENTRY_BEFORE_RETRY": config.CANCEL_UNFILLED_ENTRY_BEFORE_RETRY,
            "ENABLE_ENTRY_MARKET_FALLBACK": config.ENABLE_ENTRY_MARKET_FALLBACK,
            "ENTRY_RESTING_ORDER_MAX_MINUTES": config.ENTRY_RESTING_ORDER_MAX_MINUTES,
            "MIN_HOLD_EXIT_BYPASS_REASONS": config.MIN_HOLD_EXIT_BYPASS_REASONS,
            "UNIVERSE_MODE": config.UNIVERSE_MODE,
            "AUTO_EXPAND_UNIVERSE_WITH_MOVERS": config.AUTO_EXPAND_UNIVERSE_WITH_MOVERS,
            "UNIVERSE_MAX_TICKERS": config.UNIVERSE_MAX_TICKERS,
            "UNIVERSE_MOVER_TOP": config.UNIVERSE_MOVER_TOP,
            "POSITION_SIZE_USD": config.POSITION_SIZE_USD,
            "RISK_PER_TRADE_PCT": config.RISK_PER_TRADE_PCT,
            "MAX_POSITION_SIZE_USD": config.MAX_POSITION_SIZE_USD,
            "MAX_CONTRACTS_PER_ENTRY": config.MAX_CONTRACTS_PER_ENTRY,
            "MAX_CONTRACTS_PER_TICKER": config.MAX_CONTRACTS_PER_TICKER,
            "DRAWDOWN_REDUCE_AFTER_CONSEC_LOSSES": config.DRAWDOWN_REDUCE_AFTER_CONSEC_LOSSES,
            "DRAWDOWN_SIZE_MULTIPLIER": config.DRAWDOWN_SIZE_MULTIPLIER,
            "EXECUTION_MIN_RVOL_AFTER_IGNORE": config.EXECUTION_MIN_RVOL_AFTER_IGNORE,
            "FAST_START_MIN_SIGNAL_SCORE": config.FAST_START_MIN_SIGNAL_SCORE,
            "FAST_START_MIN_DIRECTION_SCORE": config.FAST_START_MIN_DIRECTION_SCORE,
            "FAST_START_MIN_ABS_ROC_PCT": config.FAST_START_MIN_ABS_ROC_PCT,
            "FAST_START_MIN_VWAP_DISTANCE_PCT": config.FAST_START_MIN_VWAP_DISTANCE_PCT,
            "ENABLE_FRESH_TAPE_DIRECTION_GUARD": config.ENABLE_FRESH_TAPE_DIRECTION_GUARD,
            "NORMAL_EXIT_ORDER_MIN_HOLD_SECONDS": getattr(config, "NORMAL_EXIT_ORDER_MIN_HOLD_SECONDS", 30),
            "NORMAL_EXIT_ORDER_STALE_SECONDS": getattr(config, "NORMAL_EXIT_ORDER_STALE_SECONDS", 180),
            "NORMAL_EXIT_REPRICE_DRIFT_PCT": getattr(config, "NORMAL_EXIT_REPRICE_DRIFT_PCT", 0.06),
        }
        config.ALPACA_BUY_ORDER_CAP_COUNTS_CANCELED = False
        config.ALPACA_CANCELED_BUY_ORDER_COOLDOWN_MINUTES = 10
        config.ANTI_CHURN_HOLD_MINUTES = 30
        config.ENTRY_ORDER_STATUS_WAIT_SECONDS = 1
        config.ENTRY_LIMIT_ATTEMPTS = 1
        config.CANCEL_UNFILLED_ENTRY_BEFORE_RETRY = True
        config.ENABLE_ENTRY_MARKET_FALLBACK = False
        config.ENTRY_RESTING_ORDER_MAX_MINUTES = 10
        config.MIN_HOLD_EXIT_BYPASS_REASONS = ("eod_close",)
        config.UNIVERSE_MODE = "movers"
        config.AUTO_EXPAND_UNIVERSE_WITH_MOVERS = True
        config.UNIVERSE_MAX_TICKERS = 15
        config.UNIVERSE_MOVER_TOP = 50
        config.POSITION_SIZE_USD = 1200
        config.RISK_PER_TRADE_PCT = 0.01
        config.MAX_POSITION_SIZE_USD = 1500
        config.MAX_CONTRACTS_PER_ENTRY = 8
        config.MAX_CONTRACTS_PER_TICKER = 8
        config.DRAWDOWN_REDUCE_AFTER_CONSEC_LOSSES = 1
        config.DRAWDOWN_SIZE_MULTIPLIER = 0.5
        config.EXECUTION_MIN_RVOL_AFTER_IGNORE = 0.20
        config.FAST_START_MIN_SIGNAL_SCORE = 8.4
        config.FAST_START_MIN_DIRECTION_SCORE = 0.75
        config.FAST_START_MIN_ABS_ROC_PCT = 0.12
        config.FAST_START_MIN_VWAP_DISTANCE_PCT = 0.08
        config.ENABLE_FRESH_TAPE_DIRECTION_GUARD = True
        config.NORMAL_EXIT_ORDER_MIN_HOLD_SECONDS = 30
        config.NORMAL_EXIT_ORDER_STALE_SECONDS = 180
        config.NORMAL_EXIT_REPRICE_DRIFT_PCT = 0.06
        self.now = EASTERN.localize(datetime(2026, 5, 11, 9, 55, 0))

    def tearDown(self):
        for key, value in self._config_values.items():
            setattr(config, key, value)

    def test_recent_same_day_canceled_buy_counts_during_cooldown(self):
        broker = FakeBroker(
            [
                FakeOrder(
                    "JPM260515P00290000",
                    "buy",
                    "canceled",
                    self.now - timedelta(minutes=5),
                )
            ]
        )

        counts = main._alpaca_option_buy_order_counts_by_ticker_today(broker, self.now)

        self.assertEqual(counts.get("JPM"), 1)

    def test_old_same_day_canceled_buy_does_not_block_all_day(self):
        broker = FakeBroker(
            [
                FakeOrder(
                    "JPM260515P00290000",
                    "buy",
                    "canceled",
                    self.now - timedelta(minutes=30),
                )
            ]
        )

        counts = main._alpaca_option_buy_order_counts_by_ticker_today(broker, self.now)

        self.assertNotIn("JPM", counts)

    def test_same_day_filled_buy_counts_toward_ticker_cap(self):
        broker = FakeBroker(
            [
                FakeOrder(
                    "JPM260515P00290000",
                    "buy",
                    "filled",
                    self.now - timedelta(hours=2),
                )
            ]
        )

        counts = main._alpaca_option_buy_order_counts_by_ticker_today(broker, self.now)

        self.assertEqual(counts.get("JPM"), 1)

    def test_same_day_sell_does_not_count_as_buy_attempt(self):
        broker = FakeBroker(
            [
                FakeOrder(
                    "JPM260515P00290000",
                    "sell",
                    "filled",
                    self.now - timedelta(minutes=1),
                )
            ]
        )

        counts = main._alpaca_option_buy_order_counts_by_ticker_today(broker, self.now)

        self.assertNotIn("JPM", counts)

    def test_filled_buy_contract_counts_accept_enum_style_side_status(self):
        order = FakeOrder(
            "JPM260515P00290000",
            FakeEnumValue("buy"),
            FakeEnumValue("filled"),
            self.now - timedelta(minutes=4),
        )
        order.filled_qty = 2
        broker = FakeBroker([order])

        counts = main._alpaca_filled_buy_contract_counts_by_ticker_recent(
            broker,
            self.now,
            lookback_minutes=15,
        )

        self.assertEqual(counts.get("JPM"), 2)

    def test_prior_day_buy_does_not_count_today(self):
        broker = FakeBroker(
            [
                FakeOrder(
                    "JPM260515P00290000",
                    "buy",
                    "filled",
                    self.now - timedelta(days=1),
                )
            ]
        )

        counts = main._alpaca_option_buy_order_counts_by_ticker_today(broker, self.now)

        self.assertNotIn("JPM", counts)

    def test_minimum_hold_defers_strategy_before_thirty_minutes(self):
        entry_time = self.now - timedelta(minutes=4)

        deferred = main._is_in_anti_churn_window(entry_time, self.now)

        self.assertTrue(deferred)

    def test_minimum_hold_still_defers_after_ten_minutes(self):
        entry_time = self.now - timedelta(minutes=10, seconds=1)

        deferred = main._is_in_anti_churn_window(entry_time, self.now)

        self.assertTrue(deferred)

    def test_minimum_hold_releases_strategy_after_thirty_minutes(self):
        entry_time = self.now - timedelta(minutes=30, seconds=1)

        deferred = main._is_in_anti_churn_window(entry_time, self.now)

        self.assertFalse(deferred)

    def test_minimum_hold_allows_eod_bypass(self):
        entry_time = self.now - timedelta(minutes=4)

        blocked = main._minimum_hold_blocks_exit("eod_close", entry_time, self.now)

        self.assertFalse(blocked)

    def test_minimum_hold_blocks_exposure_normalize(self):
        entry_time = self.now - timedelta(minutes=4)

        blocked = main._minimum_hold_blocks_exit("exposure_normalize", entry_time, self.now)

        self.assertTrue(blocked)

    def test_recent_filled_buy_entry_time_recovers_missing_runtime_meta(self):
        order = FakeOrder(
            "QQQ260512C00710000",
            "buy",
            "filled",
            self.now - timedelta(minutes=2),
        )
        order.filled_qty = 1
        broker = FakeBroker([order])

        entry_time = main._recent_filled_buy_entry_time(broker, "QQQ260512C00710000", self.now)

        self.assertIsNotNone(entry_time)
        self.assertTrue(main._minimum_hold_blocks_exit("profit_target", entry_time, self.now))

    def test_unfilled_active_limit_entry_is_left_resting(self):
        broker = FakeEntryBroker(status="new")

        result = main._execute_limit_entry(
            broker=broker,
            data_client=FakeDataClient(),
            option_symbol="ORCL260515C00195000",
            qty=1,
            now_et=self.now,
            label="ENTRY ORCL",
            initial_quote={"bid": 5.0, "ask": 5.04, "midpoint": 5.02, "spread_pct": 0.8},
        )

        self.assertTrue(result.get("pending_open"))
        self.assertEqual(result.get("status"), "new")
        self.assertEqual(broker.submitted, 1)
        self.assertEqual(broker.canceled, 0)

    def test_unfilled_limit_entry_can_cancel_and_retry(self):
        config.ENTRY_LIMIT_ATTEMPTS = 2
        config.CANCEL_UNFILLED_ENTRY_BEFORE_RETRY = True
        broker = FakeEntryBroker(status="new")

        result = main._execute_limit_entry(
            broker=broker,
            data_client=FakeDataClient(),
            option_symbol="ORCL260515C00195000",
            qty=1,
            now_et=self.now,
            label="ENTRY ORCL",
            initial_quote={"bid": 5.0, "ask": 5.04, "midpoint": 5.02, "spread_pct": 0.8},
        )

        self.assertTrue(result.get("pending_open"))
        self.assertEqual(result.get("status"), "new")
        self.assertEqual(broker.submitted, 2)
        self.assertEqual(broker.canceled, 1)

    def test_entry_qty_uses_budget_and_contract_cap(self):
        qty = main._entry_qty_for_budget(
            ask_price=0.52,
            equity=150000.0,
            consecutive_losses=0,
            max_trade_premium=1500.0,
        )

        self.assertEqual(qty, 8)

    def test_entry_qty_reduces_after_loss(self):
        qty = main._entry_qty_for_budget(
            ask_price=1.0,
            equity=150000.0,
            consecutive_losses=1,
            max_trade_premium=1500.0,
        )

        self.assertEqual(qty, 7)

    def test_fast_start_midday_still_rejects_dead_rvol(self):
        now = EASTERN.localize(datetime(2026, 5, 13, 11, 31, 0))

        ok, reason = main._fast_start_entry_quality_ok(
            {
                "signal_score": 18.0,
                "direction_score": 1.0,
                "rvol": 0.02,
                "roc": -0.30,
                "price": 99.0,
                "vwap": 100.0,
            },
            now,
        )

        self.assertFalse(ok)
        self.assertIn("RVOL too weak", reason)

    def test_fresh_tape_guard_rejects_call_when_latest_bars_roll_over(self):
        now = EASTERN.localize(datetime(2026, 5, 15, 10, 46, 0))

        ok, reason = main._fresh_tape_direction_guard(
            FakeBarsDataClient([100.0, 100.2, 99.8]),
            "CRM",
            "call",
            now,
        )

        self.assertFalse(ok)
        self.assertIn("disagrees with CALL", reason)

    def test_fresh_tape_guard_rejects_put_when_latest_bars_squeeze_up(self):
        now = EASTERN.localize(datetime(2026, 5, 15, 10, 46, 0))

        ok, reason = main._fresh_tape_direction_guard(
            FakeBarsDataClient([100.0, 99.8, 100.3]),
            "CRM",
            "put",
            now,
        )

        self.assertFalse(ok)
        self.assertIn("disagrees with PUT", reason)

    def test_fresh_tape_guard_accepts_aligned_call(self):
        now = EASTERN.localize(datetime(2026, 5, 15, 10, 46, 0))

        ok, reason = main._fresh_tape_direction_guard(
            FakeBarsDataClient([100.0, 100.1, 100.2]),
            "CRM",
            "call",
            now,
        )

        self.assertTrue(ok, reason)

    def test_normal_profit_exits_do_not_use_market_fallback(self):
        self.assertFalse(main._exit_reason_allows_market_fallback("profit_target"))
        self.assertFalse(main._exit_reason_allows_market_fallback("base_win_bank"))
        self.assertFalse(main._exit_reason_allows_market_fallback("protected_floor_breach"))
        self.assertFalse(main._exit_reason_allows_market_fallback("reversal_detected(ema=1,roc=1,vwap=0)"))

    def test_urgent_exits_can_use_market_fallback(self):
        self.assertTrue(main._exit_reason_allows_market_fallback("stop_loss"))
        self.assertTrue(main._exit_reason_allows_market_fallback("stop_loss_pct"))
        self.assertTrue(main._exit_reason_allows_market_fallback("eod_close"))

    def test_execution_sort_prefers_rvol_before_raw_score(self):
        weak_high_score = {"symbol": "CRM", "signal_score": 17.26, "direction_score": 1.0, "rvol": 0.02, "roc": -0.30}
        active_lower_score = {"symbol": "IWM", "signal_score": 22.29, "direction_score": 0.50, "rvol": 7.13, "roc": 0.01}
        signals = [weak_high_score, active_lower_score]

        signals.sort(key=main._execution_signal_sort_key, reverse=True)

        self.assertEqual(signals[0]["symbol"], "IWM")

    def test_high_spread_requires_strong_direction(self):
        signal = {"signal_score": 11.9, "direction_score": 0.9, "roc": 1.0, "atr_pct": 2.0}

        ok, reason = main._high_spread_direction_strength_gate(signal, 2.6)

        self.assertFalse(ok)
        self.assertIn("high_spread_requires_strong_direction", reason)

    def test_low_spread_keeps_current_direction_thresholds(self):
        signal = {"signal_score": 6.0, "direction_score": 0.4, "roc": 0.05, "atr_pct": 0.2}

        ok, reason = main._high_spread_direction_strength_gate(signal, 2.5)

        self.assertTrue(ok, reason)

    def test_spread_to_move_gate_rejects_bad_contract(self):
        signal = {"signal_score": 12.0, "direction_score": 0.9, "roc": 0.05, "roc_fast": 0.04, "atr_pct": 0.12}

        ok, reason = main._entry_spread_to_move_gate(signal, 4.0)

        self.assertFalse(ok)
        self.assertIn("spread_to_move_gate", reason)

    def test_spread_to_move_gate_accepts_quality_move(self):
        signal = {"signal_score": 14.0, "direction_score": 0.95, "roc": 1.1, "roc_fast": 0.9, "atr_pct": 1.8}

        ok, reason = main._entry_spread_to_move_gate(signal, 3.5)

        self.assertTrue(ok, reason)

    def test_ticker_open_qty_counts_live_positions_and_meta(self):
        positions = [FakePosition("IWM260515C00282000", qty=3)]
        meta = {
            "IWM260515C00282000": {"ticker": "IWM", "qty": 3},
            "IWM260515C00283000": {"ticker": "IWM", "qty": 2},
            "QQQ260515P00450000": {"ticker": "QQQ", "qty": 4},
        }

        self.assertEqual(main._ticker_open_qty(positions, meta, "IWM"), 5)

    def test_ticker_open_qty_uses_meta_when_live_position_not_seen_yet(self):
        meta = {"IWM260515C00282000": {"ticker": "IWM", "qty": 4}}

        self.assertEqual(main._ticker_open_qty([], meta, "IWM"), 4)

    def test_option_exposure_bucket_classifies_0dte_etf_and_weekly_single_name(self):
        now = EASTERN.localize(datetime(2026, 5, 18, 10, 0, 0))

        self.assertEqual(main._option_exposure_bucket("SPY", date(2026, 5, 18), now), "0dte_index_etf")
        self.assertEqual(main._option_exposure_bucket("AAPL", date(2026, 5, 18), now), "0dte_other")
        self.assertEqual(main._option_exposure_bucket("AMD", date(2026, 5, 22), now), "weekly_single_name")

    def test_open_premium_by_exposure_bucket_uses_live_and_meta_positions(self):
        now = EASTERN.localize(datetime(2026, 5, 18, 10, 0, 0))
        positions = [FakePosition("SPY260518C00741000", qty=2)]
        meta = {
            "SPY260518C00741000": {"ticker": "SPY", "qty": 2, "entry_price": 1.25, "expiry": "2026-05-18"},
            "AMD260522P00432500": {"ticker": "AMD", "qty": 1, "entry_price": 19.80, "expiry": "2026-05-22"},
        }

        bucket_premium, ticker_premium = main._open_premium_by_exposure_bucket(positions, meta, now)

        self.assertEqual(bucket_premium["0dte_index_etf"], 250.0)
        self.assertEqual(bucket_premium["weekly_single_name"], 1980.0)
        self.assertEqual(ticker_premium["SPY"], 250.0)
        self.assertEqual(ticker_premium["AMD"], 1980.0)

    def test_same_direction_live_pnl_detects_red_scale_in(self):
        class PnlPosition(FakePosition):
            def __init__(self):
                super().__init__("INTC260515P00116000", qty=4)
                self.unrealized_pl = "-96.0"

        qty, pnl = main._ticker_same_direction_live_pnl(
            [PnlPosition()],
            {"INTC260515P00116000": {"ticker": "INTC", "direction": "put"}},
            "INTC",
            "put",
        )

        self.assertEqual(qty, 4)
        self.assertEqual(pnl, -96.0)

    def test_confirmed_long_option_qty_requires_live_position(self):
        class PositionBroker:
            def get_open_option_positions(self):
                return [FakePosition("ORCL260515C00195000", qty=2)]

        qty = main._confirmed_long_option_qty(PositionBroker(), "ORCL260515C00195000")

        self.assertEqual(qty, 2)

    def test_confirmed_long_option_qty_returns_zero_without_live_position(self):
        class PositionBroker:
            def get_open_option_positions(self):
                return []

        qty = main._confirmed_long_option_qty(PositionBroker(), "ORCL260515C00195000")

        self.assertEqual(qty, 0)

    def test_wait_confirmed_long_option_qty_returns_immediately_when_live(self):
        class PositionBroker:
            def get_open_option_positions(self):
                return [FakePosition("ORCL260515C00195000", qty=3)]

        qty = main._wait_confirmed_long_option_qty(
            PositionBroker(),
            "ORCL260515C00195000",
            min_qty=2,
            wait_seconds=0,
        )

        self.assertEqual(qty, 3)

    def test_active_buy_order_counts_as_resting_entry(self):
        broker = FakeBroker(
            [
                FakeOrder(
                    "ORCL260515C00195000",
                    "buy",
                    "new",
                    self.now - timedelta(minutes=1),
                )
            ]
        )

        counts = main._alpaca_active_option_buy_order_counts_by_ticker_today(broker, self.now)

        self.assertEqual(counts.get("ORCL"), 1)

    def test_active_buy_order_premium_counts_pending_exposure(self):
        broker = FakeBroker(
            [
                FakeOrder(
                    "ORCL260515C00195000",
                    "buy",
                    "new",
                    self.now - timedelta(minutes=1),
                    qty=2,
                    limit_price=5.12,
                ),
                FakeOrder(
                    "JPM260515P00290000",
                    "buy",
                    "canceled",
                    self.now - timedelta(minutes=1),
                    qty=1,
                    limit_price=0.73,
                ),
            ]
        )

        premium = main._alpaca_active_option_buy_order_premium_usd_today(broker, self.now)

        self.assertEqual(premium, 1024.0)

    def test_stale_active_entry_order_is_canceled_after_max_rest(self):
        order = FakeOrder(
            "ORCL260515C00195000",
            "buy",
            "new",
            self.now - timedelta(minutes=11),
        )
        broker = FakeBroker([order])

        canceled = main._cancel_stale_active_entry_buy_orders(broker, self.now)

        self.assertEqual(canceled, 1)
        self.assertEqual(broker.canceled_order_ids, [order.id])

    def test_recent_active_entry_order_is_left_resting(self):
        order = FakeOrder(
            "ORCL260515C00195000",
            "buy",
            "new",
            self.now - timedelta(minutes=5),
        )
        broker = FakeBroker([order])

        canceled = main._cancel_stale_active_entry_buy_orders(broker, self.now)

        self.assertEqual(canceled, 0)

    def test_active_entry_order_for_open_ticker_is_canceled(self):
        config.MAX_CONTRACTS_PER_TICKER = 1
        order = FakeOrder(
            "ORCL260515C00195000",
            "buy",
            "new",
            self.now - timedelta(minutes=1),
        )
        broker = FakeBroker([order])
        positions = [FakePosition("ORCL260515C00195000", qty=1)]

        canceled = main._cancel_active_entry_buys_for_open_tickers(broker, positions, self.now)

        self.assertEqual(canceled, 1)
        self.assertEqual(broker.canceled_order_ids, [order.id])

    def test_active_scale_in_order_for_open_ticker_is_kept_below_cap(self):
        config.MAX_CONTRACTS_PER_TICKER = 8
        order = FakeOrder(
            "ORCL260515C00195000",
            "buy",
            "new",
            self.now - timedelta(minutes=1),
        )
        broker = FakeBroker([order])
        positions = [FakePosition("ORCL260515C00195000", qty=1)]

        canceled = main._cancel_active_entry_buys_for_open_tickers(broker, positions, self.now)

        self.assertEqual(canceled, 0)
        self.assertEqual(broker.canceled_order_ids, [])

    def test_excess_active_entry_orders_for_same_ticker_are_canceled(self):
        older = FakeOrder(
            "ORCL260515C00195000",
            "buy",
            "new",
            self.now - timedelta(minutes=3),
        )
        newer = FakeOrder(
            "ORCL260515C00195000",
            "buy",
            "new",
            self.now - timedelta(minutes=1),
        )
        other = FakeOrder(
            "JPM260515P00290000",
            "buy",
            "new",
            self.now - timedelta(minutes=1),
        )
        broker = FakeBroker([older, newer, other])

        canceled = main._cancel_excess_active_entry_buys_per_ticker(broker, self.now)

        self.assertEqual(canceled, 1)
        self.assertEqual(broker.canceled_order_ids, [older.id])

    def test_mover_filter_keeps_assets_when_options_enabled_field_is_missing(self):
        data_client = FakeUniverseDataClient()

        kept = main._filter_mover_candidates(data_client, ["MSFT"], protected=set(config.CORE_TICKERS))

        self.assertEqual(kept, ["MSFT"])

    def test_scan_universe_supplements_when_movers_are_too_small(self):
        data_client = FakeUniverseDataClient()

        universe = main._build_scan_universe(data_client)

        self.assertIn("MSFT", universe)
        self.assertIn("NVDA", universe)
        self.assertIn("TSLA", universe)
        self.assertGreater(len(universe), len(config.TICKERS))

    def test_select_matching_exit_order_prefers_exact_qty_match(self):
        now = self.now
        exact = FakeOrder(
            "ORCL260515C00195000",
            "sell",
            "new",
            now - timedelta(seconds=5),
            qty=2,
            limit_price=2.10,
        )
        partial = FakeOrder(
            "ORCL260515C00195000",
            "sell",
            "new",
            now - timedelta(seconds=3),
            qty=4,
            limit_price=2.20,
        )
        partial.filled_qty = 1

        selected, selected_qty, selection_mode = main._select_matching_exit_order(
            [partial, exact],
            2,
            now_et=now,
            tz=EASTERN,
            symbol="ORCL260515C00195000",
            label="TEST",
            execution_meta={},
        )

        self.assertEqual(selected, exact)
        self.assertEqual(selected_qty, 2)
        self.assertEqual(selection_mode, "exact_match")

    def test_select_matching_exit_order_falls_back_to_largest_partial(self):
        now = self.now
        first = FakeOrder(
            "ORCL260515C00195000",
            "sell",
            "new",
            now - timedelta(seconds=2),
            qty=2,
            limit_price=2.10,
        )
        first.filled_qty = 1
        second = FakeOrder(
            "ORCL260515C00195000",
            "sell",
            "new",
            now - timedelta(seconds=3),
            qty=4,
            limit_price=2.15,
        )
        second.filled_qty = 2

        selected, selected_qty, selection_mode = main._select_matching_exit_order(
            [first, second],
            3,
            now_et=now,
            tz=EASTERN,
            symbol="ORCL260515C00195000",
            label="TEST",
            execution_meta={},
        )

        self.assertEqual(selected, second)
        self.assertEqual(selected_qty, 2)
        self.assertEqual(selection_mode, "partial_match")

    def test_exit_order_age_seconds_flags_invalid_timestamp(self):
        bad_order = FakeRawSubmittedOrder(
            "ORCL260515C00195000",
            "sell",
            "new",
            "not-a-real-time",
            qty=1,
        )

        age_seconds, age_error = main._exit_order_age_seconds(self.now, order=bad_order, tz=EASTERN)

        self.assertIsNone(age_seconds)
        self.assertIn("invalid_submitted_at", age_error or "")

    def test_normal_exit_with_full_active_close_coverage_skips_new_limit_order(self):
        coverage_order = FakeOrder(
            "ORCL260515C00195000",
            "sell",
            "new",
            self.now - timedelta(seconds=5),
            qty=100,
            limit_price=5.04,
        )
        broker = FakeExitBroker([coverage_order])
        config.NORMAL_EXIT_ORDER_MIN_HOLD_SECONDS = 0
        config.NORMAL_EXIT_ORDER_STALE_SECONDS = 999

        filled_qty, fill_price, close_meta = main._close_position_with_confirmation(
            broker=broker,
            data_client=FakeCloseDataClient(),
            symbol="ORCL260515C00195000",
            qty=100,
            now_et=self.now,
            label="TEST CLOSE",
            exit_reason="profit_target",
            poll_seconds_override=1,
            max_wait_seconds_override=1,
        )

        self.assertEqual(filled_qty, 0)
        self.assertIsNone(fill_price)
        self.assertEqual(close_meta.get("reason"), "normal_exit_skipped_new_close_due_to_active_coverage")
        self.assertEqual(close_meta.get("active_close_qty"), 100)
        self.assertEqual(close_meta.get("uncovered_close_qty"), 0)
        self.assertEqual(close_meta.get("selected_order_id"), coverage_order.id)
        self.assertEqual(len(broker.actions), 0)

    def test_normal_exit_with_partially_filled_active_close_coverage_skips_new_order(self):
        coverage_order = FakeOrder(
            "ORCL260515C00195000",
            "sell",
            "partially_filled",
            self.now - timedelta(seconds=5),
            qty=80,
            limit_price=5.04,
        )
        coverage_order.filled_qty = 40
        broker = FakeExitBroker([coverage_order])
        config.NORMAL_EXIT_ORDER_MIN_HOLD_SECONDS = 0
        config.NORMAL_EXIT_ORDER_STALE_SECONDS = 999

        filled_qty, fill_price, close_meta = main._close_position_with_confirmation(
            broker=broker,
            data_client=FakeCloseDataClient(),
            symbol="ORCL260515C00195000",
            qty=40,
            now_et=self.now,
            label="TEST CLOSE",
            exit_reason="profit_target",
            poll_seconds_override=1,
            max_wait_seconds_override=1,
        )

        self.assertEqual(filled_qty, 0)
        self.assertIsNone(fill_price)
        self.assertEqual(close_meta.get("reason"), "normal_exit_skipped_new_close_due_to_active_coverage")
        self.assertEqual(close_meta.get("active_close_qty"), 40)
        self.assertEqual(close_meta.get("uncovered_close_qty"), 0)
        self.assertEqual(close_meta.get("selected_order_id"), coverage_order.id)
        self.assertEqual(len(broker.actions), 0)

    def test_normal_exit_with_partial_active_coverage_uses_uncovered_qty_only(self):
        coverage_order = FakeOrder(
            "ORCL260515C00195000",
            "sell",
            "partially_filled",
            self.now - timedelta(seconds=5),
            qty=50,
            limit_price=5.04,
        )
        coverage_order.filled_qty = 20
        broker = FakeExitBroker([coverage_order])
        config.NORMAL_EXIT_ORDER_MIN_HOLD_SECONDS = 0
        config.NORMAL_EXIT_ORDER_STALE_SECONDS = 999

        filled_qty, fill_price, close_meta = main._close_position_with_confirmation(
            broker=broker,
            data_client=FakeCloseDataClient(),
            symbol="ORCL260515C00195000",
            qty=100,
            now_et=self.now,
            label="TEST CLOSE",
            exit_reason="profit_target",
            poll_seconds_override=1,
            max_wait_seconds_override=1,
        )

        self.assertEqual(filled_qty, 0)
        self.assertIsNone(fill_price)
        self.assertEqual(close_meta.get("reason"), "normal_exit_additional_close_needed")
        self.assertEqual(close_meta.get("active_close_qty"), 30)
        self.assertEqual(close_meta.get("uncovered_close_qty"), 70)
        self.assertEqual(close_meta.get("selected_order_id"), coverage_order.id)
        self.assertEqual(broker.actions[0][0], "limit")
        self.assertEqual(broker.actions[0][2], "ORCL260515C00195000")
        self.assertEqual(broker.actions[0][3], 70)
        self.assertEqual(len(broker.actions), 1)

    def test_urgent_exit_cancels_existing_close_orders_before_market(self):
        sell_one = FakeOrder(
            "ORCL260515C00195000",
            "sell",
            "new",
            self.now - timedelta(minutes=2),
        )
        sell_one.filled_qty = 1
        sell_two = FakeOrder(
            "ORCL260515C00195000",
            "sell",
            "new",
            self.now - timedelta(minutes=1),
        )
        sell_two.filled_qty = 0
        broker = FakeExitBroker([sell_one, sell_two])
        data_client = FakeCloseDataClient(bid=None, ask=None)

        filled_qty, fill_price, close_meta = main._close_position_with_confirmation(
            broker=broker,
            data_client=data_client,
            symbol="ORCL260515C00195000",
            qty=2,
            now_et=self.now,
            label="TEST CLOSE",
            exit_reason="stop_loss",
            poll_seconds_override=1,
            max_wait_seconds_override=1,
        )

        self.assertEqual(filled_qty, 2)
        self.assertEqual(fill_price, 10.0)
        self.assertEqual(close_meta.get("reused_existing_exit_order"), False)
        self.assertEqual(len(broker.canceled_order_ids), 2)
        self.assertEqual(close_meta.get("cancellation_count_by_reason", {}).get("urgent_exit_force_cancel", 0), 2)
        self.assertEqual(len(broker.actions), 3)
        self.assertEqual(broker.actions[0][0], "cancel")
        self.assertEqual(broker.actions[2][0], "market")

    def test_position_zero_with_orphan_active_close_orders_canceled(self):
        orphan_order = FakeOrder(
            "ORCL260515C00195000",
            "sell",
            "new",
            self.now - timedelta(minutes=1),
            qty=30,
            limit_price=4.98,
        )
        orphan_order.filled_qty = 10
        broker = FakeExitBroker([orphan_order])

        filled_qty, fill_price, close_meta = main._close_position_with_confirmation(
            broker=broker,
            data_client=FakeCloseDataClient(),
            symbol="ORCL260515C00195000",
            qty=0,
            now_et=self.now,
            label="TEST CLOSE",
            exit_reason="profit_target",
            poll_seconds_override=1,
            max_wait_seconds_override=1,
        )

        self.assertEqual(filled_qty, 0)
        self.assertIsNone(fill_price)
        self.assertEqual(close_meta.get("reason"), "normal_exit_canceling_orphan_close_order")
        self.assertEqual(close_meta.get("position_qty"), 0)
        self.assertEqual(close_meta.get("active_close_qty"), 20)
        self.assertEqual(close_meta.get("selected_order_id"), orphan_order.id)
        self.assertEqual(close_meta.get("order_status"), "new")
        self.assertEqual(close_meta.get("remaining_qty"), 20)
        self.assertEqual(close_meta.get("uncovered_close_qty"), 0)
        self.assertTrue(any(
            reason.startswith("normal_exit_canceling_orphan_close_order")
            for reason in close_meta.get("cancellation_reasons", [])
        ))
        self.assertEqual(close_meta.get("cancellation_count_by_reason", {}).get("normal_exit_canceling_orphan_close_order", 0), 1)
        self.assertEqual(len(broker.actions), 1)
        self.assertEqual(broker.actions[0][0], "cancel")
        self.assertEqual(broker.actions[0][1], orphan_order.id)
        self.assertEqual(len(broker.canceled_order_ids), 1)
        self.assertEqual(broker.canceled_order_ids[0], orphan_order.id)

    def test_position_zero_with_orphan_active_close_orders_detected_and_not_canceled_when_zero_remaining(self):
        orphan_order = FakeOrder(
            "ORCL260515C00195000",
            "sell",
            "new",
            self.now - timedelta(minutes=1),
            qty=30,
            limit_price=4.98,
        )
        orphan_order.filled_qty = 30
        broker = FakeExitBroker([orphan_order])

        filled_qty, fill_price, close_meta = main._close_position_with_confirmation(
            broker=broker,
            data_client=FakeCloseDataClient(),
            symbol="ORCL260515C00195000",
            qty=0,
            now_et=self.now,
            label="TEST CLOSE",
            exit_reason="profit_target",
            poll_seconds_override=1,
            max_wait_seconds_override=1,
        )

        self.assertEqual(filled_qty, 0)
        self.assertIsNone(fill_price)
        self.assertEqual(close_meta.get("position_qty"), 0)
        self.assertEqual(close_meta.get("active_close_qty"), 0)
        self.assertEqual(close_meta.get("reason"), "")
        self.assertEqual(len(close_meta.get("cancellation_reasons", [])), 0)
        self.assertEqual(len(broker.actions), 0)

    def test_exit_reprice_drift_normalizes_percent_like_inputs(self):
        self.assertAlmostEqual(main._normalize_exit_reprice_drift_pct(0.06), 0.06)
        self.assertAlmostEqual(main._normalize_exit_reprice_drift_pct(6.0), 0.06)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
