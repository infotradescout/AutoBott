"""Broker/order and position management."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
import re

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import LimitOrderRequest, MarketOrderRequest

import config

_OPTION_SYMBOL_RE = re.compile(r"^[A-Z.]+\d{6}[CP]\d{8}$")


def _assert_option_symbol(symbol: str) -> None:
    normalized = str(symbol or "").upper().strip()
    if not _OPTION_SYMBOL_RE.match(normalized):
        raise ValueError(f"invalid option symbol: {symbol!r}")


def _limit_price_decimal(value: float) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _normalize_asset_class(raw_asset_class) -> str:
    """Normalize SDK/rest asset_class values into a lowercase label."""
    try:
        value = getattr(raw_asset_class, "value", raw_asset_class)
    except Exception:  # noqa: BLE001
        value = raw_asset_class
    text = str(value or "").strip().lower()
    if "." in text:
        text = text.split(".")[-1]
    return text


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
        try:
            positions = self.get_all_positions()
        except Exception as exc:  # noqa: BLE001
            print(f"[broker] get_all_positions failed: {exc}")
            return []
        option_asset_classes = {"us_option", "option", "options"}
        return [
            p
            for p in positions
            if _normalize_asset_class(getattr(p, "asset_class", "")) in option_asset_classes
        ]

    def _current_option_position_qty(self, option_symbol: str) -> int | None:
        """Return current long/short option quantity, or None when Alpaca lookup fails."""
        _assert_option_symbol(option_symbol)
        try:
            positions = self.get_all_positions()
        except Exception as exc:  # noqa: BLE001
            print(f"[broker] position lookup failed for {option_symbol}: {exc}")
            return None

        option_asset_classes = {"us_option", "option", "options"}
        target = str(option_symbol).upper().strip()
        for pos in positions or []:
            if str(getattr(pos, "symbol", "") or "").upper().strip() != target:
                continue
            if _normalize_asset_class(getattr(pos, "asset_class", "")) not in option_asset_classes:
                continue
            try:
                return int(float(getattr(pos, "qty", 0) or 0))
            except (TypeError, ValueError):
                return 0
        return 0

    def _safe_option_sell_qty(self, option_symbol: str, requested_qty: int) -> int:
        requested_qty = max(0, int(requested_qty or 0))
        if requested_qty <= 0:
            raise ValueError(f"invalid sell qty for {option_symbol}: {requested_qty}")
        if not config.ENFORCE_LONG_ONLY_OPTION_SELLS:
            return requested_qty

        current_qty = self._current_option_position_qty(option_symbol)
        if current_qty is None:
            raise RuntimeError(f"refusing option sell for {option_symbol}: position lookup failed")
        if current_qty <= 0:
            raise ValueError(f"refusing option sell for {option_symbol}: no long position")
        if requested_qty > current_qty:
            print(
                f"[broker] capping option sell for {option_symbol}: requested {requested_qty}, "
                f"long position {current_qty}"
            )
            return current_qty
        return requested_qty

    def place_option_limit_buy(
        self,
        option_symbol: str,
        qty: int,
        ask_price: float,
        *,
        client_order_id: str | None = None,
    ):
        _assert_option_symbol(option_symbol)
        payload = {
            "symbol": option_symbol,
            "qty": qty,
            "side": OrderSide.BUY,
            "time_in_force": TimeInForce.DAY,
            "limit_price": _limit_price_decimal(ask_price),
        }
        if client_order_id:
            payload["client_order_id"] = str(client_order_id).strip()[:48]
        req = LimitOrderRequest(**payload)
        return self.trading_client.submit_order(order_data=req)

    def place_option_limit_sell(self, option_symbol: str, qty: int, limit_price: float):
        _assert_option_symbol(option_symbol)
        sell_qty = self._safe_option_sell_qty(option_symbol, qty)
        req = LimitOrderRequest(
            symbol=option_symbol,
            qty=sell_qty,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
            limit_price=_limit_price_decimal(limit_price),
        )
        return self.trading_client.submit_order(order_data=req)

    def place_option_market_buy(self, option_symbol: str, qty: int):
        _assert_option_symbol(option_symbol)
        req = MarketOrderRequest(
            symbol=option_symbol,
            qty=qty,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
        )
        return self.trading_client.submit_order(order_data=req)

    def cover_option_market(self, option_symbol: str, qty: int):
        """Buy option contracts to cover an accidental short option position."""
        return self.place_option_market_buy(option_symbol, qty)

    def close_option_market(self, option_symbol: str, qty: int):
        _assert_option_symbol(option_symbol)
        sell_qty = self._safe_option_sell_qty(option_symbol, qty)
        req = MarketOrderRequest(
            symbol=option_symbol,
            qty=sell_qty,
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

    def get_recent_orders(self, *, limit: int = 500):
        """Return recent orders, newest first."""
        from alpaca.trading.enums import QueryOrderStatus
        from alpaca.trading.requests import GetOrdersRequest

        req = GetOrdersRequest(status=QueryOrderStatus.ALL, nested=False, limit=max(1, int(limit)))
        orders = self.trading_client.get_orders(filter=req) or []
        orders.sort(key=lambda o: str(getattr(o, "submitted_at", "") or ""), reverse=True)
        return orders

    def cancel_order(self, order_id: str):
        """Cancel a single order by ID."""
        return self.trading_client.cancel_order_by_id(order_id)

    def get_open_orders_for_symbol(self, symbol: str, side: str | None = None) -> list:
        """Return open orders for a symbol (optionally filtered by side)."""
        try:
            from alpaca.trading.enums import QueryOrderStatus
            from alpaca.trading.requests import GetOrdersRequest

            req = GetOrdersRequest(
                status=QueryOrderStatus.OPEN,
                symbols=[symbol],
                nested=False,
                limit=50,
            )
            orders = self.trading_client.get_orders(filter=req) or []
            if side is not None:
                side_lc = str(side).lower()
                orders = [o for o in orders if str(getattr(o, "side", "")).lower() == side_lc]
            # Prefer most recently submitted first when available.
            orders.sort(key=lambda o: str(getattr(o, "submitted_at", "") or ""), reverse=True)
            return orders
        except Exception as exc:  # noqa: BLE001
            print(f"[broker] get_open_orders_for_symbol failed for {symbol}: {exc}")
            return []

    def has_open_order_for_symbol(self, symbol: str, side: str | None = None) -> bool:
        """Return True when there is an open order for the symbol (optionally matching side)."""
        return len(self.get_open_orders_for_symbol(symbol=symbol, side=side)) > 0

    def close_all_positions(self) -> tuple[int, int, list[dict]]:
        """Close all open positions with market sell orders. Returns (total, closed, results)."""
        results = []
        try:
            positions = self.get_open_option_positions()
        except Exception as exc:  # noqa: BLE001
            print(f"[broker] close_all_positions failed to fetch positions: {exc}")
            return 0, 0, [{"error": str(exc)}]
        
        total = len(positions)
        closed = 0
        
        for pos in positions:
            try:
                symbol = str(getattr(pos, "symbol", ""))
                qty = int(float(getattr(pos, "qty", 0) or 0))
                if symbol and qty > 0:
                    order = self.close_option_market(symbol, qty)
                elif symbol and qty < 0:
                    qty = abs(qty)
                    order = self.cover_option_market(symbol, qty)
                else:
                    continue
                order_id = str(getattr(order, "id", "unknown"))
                results.append({
                    "symbol": symbol,
                    "qty": qty,
                    "order_id": order_id,
                    "status": "submitted"
                })
                closed += 1
            except Exception as exc:  # noqa: BLE001
                symbol = str(getattr(pos, "symbol", "unknown"))
                results.append({
                    "symbol": symbol,
                    "error": str(exc),
                    "status": "failed"
                })
        
        return total, closed, results

    def pdt_allows_new_day_trade(self) -> tuple[bool, dict]:
        if not config.ENFORCE_PDT_GUARD:
            return True, {"reason": "pdt_guard_disabled", "equity": None, "daytrade_count": None}

        try:
            account = self.get_account()
        except Exception as exc:  # noqa: BLE001
            print(f"[broker] pdt_allows_new_day_trade account lookup failed: {exc}")
            return True, {"reason": "pdt_check_error", "equity": None, "daytrade_count": None}
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
