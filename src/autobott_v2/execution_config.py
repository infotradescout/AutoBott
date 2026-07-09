from __future__ import annotations

import os
from dataclasses import dataclass

from .execution_models import BrokerEnvironment, ExecutionRiskControls


def _normalize_bool(value: str | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _normalize_optional_int(value: str | None, *, default: int | None = None) -> int | None:
    if value is None or not value.strip():
        return default
    return int(value)


@dataclass(frozen=True)
class AlpacaExecutionConfig:
    environment: BrokerEnvironment
    api_key: str | None
    secret_key: str | None
    trading_base_url: str
    data_base_url: str
    allow_live_trading: bool
    allow_order_placement: bool
    max_position_cost: float
    max_daily_loss: float
    max_open_positions: int
    paper_trade_all_passed_signals: bool = False
    paper_max_new_entry_attempts_per_loop: int | None = None
    paper_max_open_entry_buy_orders: int | None = None

    def validate(self) -> "AlpacaExecutionConfig":
        if not self.api_key or not self.secret_key:
            raise ValueError("alpaca_credentials_missing")
        if self.environment is BrokerEnvironment.LIVE and not self.allow_live_trading:
            raise ValueError("live_trading_disabled")
        if self.environment is BrokerEnvironment.PAPER and "paper-api.alpaca.markets" not in self.trading_base_url.lower():
            raise ValueError("alpaca_trading_base_url_not_paper")
        if self.environment is BrokerEnvironment.LIVE and "api.alpaca.markets" not in self.trading_base_url.lower():
            raise ValueError("alpaca_trading_base_url_not_live")
        if self.max_position_cost <= 0:
            raise ValueError("max_position_cost_invalid")
        if self.max_daily_loss <= 0:
            raise ValueError("max_daily_loss_invalid")
        if self.max_open_positions <= 0:
            raise ValueError("max_open_positions_invalid")
        if self.paper_max_new_entry_attempts_per_loop is not None and self.paper_max_new_entry_attempts_per_loop <= 0:
            raise ValueError("paper_max_new_entry_attempts_per_loop_invalid")
        if self.paper_max_open_entry_buy_orders is not None and self.paper_max_open_entry_buy_orders <= 0:
            raise ValueError("paper_max_open_entry_buy_orders_invalid")
        return self

    def risk_controls(self) -> ExecutionRiskControls:
        return ExecutionRiskControls(
            max_position_cost=self.max_position_cost,
            max_daily_loss=self.max_daily_loss,
            max_open_positions=self.effective_max_open_positions(),
            allow_live_trading=self.allow_live_trading,
            allow_order_placement=self.allow_order_placement,
            allowed_environments=(self.environment,),
        )

    def effective_max_open_positions(self) -> int:
        if (
            self.environment is BrokerEnvironment.PAPER
            and self.paper_trade_all_passed_signals
            and self.paper_max_open_entry_buy_orders is not None
        ):
            return max(self.max_open_positions, self.paper_max_open_entry_buy_orders)
        return self.max_open_positions

    def effective_max_new_entry_attempts_per_loop(self) -> int | None:
        if self.environment is BrokerEnvironment.PAPER and self.paper_trade_all_passed_signals:
            return self.paper_max_new_entry_attempts_per_loop
        return None


def load_alpaca_execution_config() -> AlpacaExecutionConfig:
    env = (os.getenv("ALPACA_ENV") or "paper").strip().lower()
    environment = BrokerEnvironment.LIVE if env == "live" else BrokerEnvironment.PAPER
    default_trading_base = (
        "https://api.alpaca.markets"
        if environment is BrokerEnvironment.LIVE
        else "https://paper-api.alpaca.markets"
    )
    paper_trade_all_passed_signals = _normalize_bool(
        os.getenv("AUTOBOTT_PAPER_TRADE_ALL_PASSED_SIGNALS"),
        default=True,
    )

    return AlpacaExecutionConfig(
        environment=environment,
        api_key=os.getenv("ALPACA_API_KEY_ID"),
        secret_key=os.getenv("ALPACA_API_SECRET_KEY"),
        trading_base_url=(os.getenv("ALPACA_TRADING_BASE_URL") or default_trading_base).rstrip("/"),
        data_base_url=(os.getenv("ALPACA_DATA_BASE_URL") or "https://data.alpaca.markets").rstrip("/"),
        allow_live_trading=_normalize_bool(os.getenv("AUTOBOTT_LIVE_TRADING_ENABLED"), default=False),
        allow_order_placement=_normalize_bool(os.getenv("AUTOBOTT_ALLOW_ORDER_PLACEMENT"), default=False),
        max_position_cost=float(os.getenv("AUTOBOTT_MAX_POSITION_COST", "100")),
        max_daily_loss=float(os.getenv("AUTOBOTT_MAX_DAILY_LOSS", "500")),
        max_open_positions=int(os.getenv("AUTOBOTT_MAX_OPEN_POSITIONS", "3")),
        paper_trade_all_passed_signals=paper_trade_all_passed_signals,
        paper_max_new_entry_attempts_per_loop=_normalize_optional_int(
            os.getenv("AUTOBOTT_PAPER_MAX_NEW_ENTRY_ATTEMPTS_PER_LOOP"),
            default=25 if paper_trade_all_passed_signals else None,
        ),
        paper_max_open_entry_buy_orders=_normalize_optional_int(
            os.getenv("AUTOBOTT_PAPER_MAX_OPEN_ENTRY_BUY_ORDERS"),
            default=25 if paper_trade_all_passed_signals else None,
        ),
    )


def require_alpaca_execution_config() -> AlpacaExecutionConfig:
    return load_alpaca_execution_config().validate()
