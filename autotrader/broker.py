"""Broker/order and position management."""

from __future__ import annotations

from decimal import Decimal

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import LimitOrderRequest, MarketOrderRequest

import config


class AlpacaBroker:
    def __init__(self, api_key: str, secret_key: str, paper: bool = True):
        self.trading_client = TradingClient(api_key, secret_key, paper=paper)

    def get_clock(self):
        return self.trading_client.get_clock()

    def get_account(self):
        return self.trading_client.get_account()

    def get_all_positions(self):
        return self.trading_client.get_all_positions()

    def get_open_option_positions(self):
        positions = self.get_all_positions()
        return [p for p in positions if str(getattr(p, "asset_class", "")).lower() in ("us_option", "options")]

    def place_option_limit_buy(self, option_symbol: str, qty: int, ask_price: float):
        req = LimitOrderRequest(
            symbol=option_symbol,
            qty=qty,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
            limit_price=Decimal(str(ask_price)),
        )
        return self.trading_client.submit_order(order_data=req)

    def close_option_market(self, option_symbol: str, qty: int):
        req = MarketOrderRequest(
            symbol=option_symbol,
            qty=qty,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
        )
        return self.trading_client.submit_order(order_data=req)

    def cancel_all_open_orders(self):
        return self.trading_client.cancel_orders()

    def get_order_status(self, order_id: str):
        """Fetch a single order by ID."""
        from alpaca.trading.requests import GetOrderByIdRequest

        return self.trading_client.get_order_by_id(order_id)

    def cancel_order(self, order_id: str):
        """Cancel a single order by ID."""
        return self.trading_client.cancel_order_by_id(order_id)

    def pdt_allows_new_day_trade(self) -> tuple[bool, dict]:
        if not config.ENFORCE_PDT_GUARD:
            return True, {"reason": "pdt_guard_disabled", "equity": None, "daytrade_count": None}

        account = self.get_account()
        try:
            equity = float(getattr(account, "equity", 0) or 0)
        except (TypeError, ValueError):
            equity = 0.0
        try:
            daytrade_count = int(getattr(account, "daytrade_count", 0) or 0)
        except (TypeError, ValueError):
            daytrade_count = 0

        if equity >= config.PDT_MIN_EQUITY:
            return True, {"reason": "equity_ok", "equity": equity, "daytrade_count": daytrade_count}
        if daytrade_count >= config.PDT_MAX_DAY_TRADES_5D:
            return (
                False,
                {"reason": "blocked_by_pdt", "equity": equity, "daytrade_count": daytrade_count},
            )
        return True, {"reason": "under_25k_but_available", "equity": equity, "daytrade_count": daytrade_count}
