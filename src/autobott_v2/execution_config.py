from __future__ import annotations

import math
import os
from dataclasses import dataclass

from .execution_models import BrokerEnvironment, ExecutionRiskControls
from .hosted_policy import (
    HOSTED_MAX_DAILY_LOSS,
    HOSTED_MAX_NEW_PAIRS_PER_CYCLE,
    HOSTED_MAX_OPEN_LEGS,
    HOSTED_MAX_POSITION_COST,
    is_hosted_paper_runtime,
)


# Preserve the prior maximum gross-premium envelope; do not infer a larger
# capital allowance merely because the owner asks for more qualifying entries.
PORTFOLIO_PREMIUM_ENVELOPE = HOSTED_MAX_OPEN_LEGS * HOSTED_MAX_POSITION_COST
PORTFOLIO_OPERATIONAL_MAX_LEGS = 60


def _normalize_bool(value: str | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _normalize_optional_int(value: str | None, *, default: int | None = None) -> int | None:
    if value is None or not value.strip():
        return default
    return int(value)


def portfolio_mode_enabled() -> bool:
    return is_hosted_paper_runtime() and _normalize_bool(os.getenv("AUTOBOTT_PORTFOLIO_BUDGET_ENABLED"))


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
    paper_ignore_position_cost_limit: bool = False
    portfolio_premium_limit: float | None = None

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
        if self.portfolio_premium_limit is not None:
            if (self.environment is not BrokerEnvironment.PAPER or self.allow_live_trading
                    or self.trading_base_url.rstrip("/") != "https://paper-api.alpaca.markets"):
                raise ValueError("portfolio_budget_requires_paper_endpoint")
            if (isinstance(self.portfolio_premium_limit, bool) or not math.isfinite(self.portfolio_premium_limit)
                    or self.portfolio_premium_limit <= 0 or self.portfolio_premium_limit > PORTFOLIO_PREMIUM_ENVELOPE):
                raise ValueError("portfolio_budget_outside_legacy_envelope")
            if self.paper_ignore_position_cost_limit:
                raise ValueError("portfolio_budget_requires_individual_cost_cap")
        return self

    def risk_controls(self) -> ExecutionRiskControls:
        return ExecutionRiskControls(
            max_position_cost=self.effective_max_position_cost(),
            max_daily_loss=self.max_daily_loss,
            max_open_positions=self.effective_max_open_positions(),
            allow_live_trading=self.allow_live_trading,
            allow_order_placement=self.allow_order_placement,
            allowed_environments=(self.environment,),
        )

    def effective_max_position_cost(self) -> float | None:
        """Return the execution cap, or no cap for configured paper testing."""
        if self.environment is BrokerEnvironment.PAPER and self.paper_ignore_position_cost_limit:
            return None
        return self.max_position_cost

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
    hosted_paper = is_hosted_paper_runtime()
    portfolio_mode = portfolio_mode_enabled()
    hosted_max_legs = PORTFOLIO_OPERATIONAL_MAX_LEGS if portfolio_mode else HOSTED_MAX_OPEN_LEGS
    env = "paper" if hosted_paper else (os.getenv("ALPACA_ENV") or "paper").strip().lower()
    environment = BrokerEnvironment.LIVE if env == "live" else BrokerEnvironment.PAPER
    default_trading_base = "https://api.alpaca.markets" if environment is BrokerEnvironment.LIVE else "https://paper-api.alpaca.markets"
    paper_trade_all_passed_signals = _normalize_bool(os.getenv("AUTOBOTT_PAPER_TRADE_ALL_PASSED_SIGNALS"), default=True)
    return AlpacaExecutionConfig(
        environment=environment,
        api_key=os.getenv("ALPACA_API_KEY_ID"),
        secret_key=os.getenv("ALPACA_API_SECRET_KEY"),
        trading_base_url=("https://paper-api.alpaca.markets" if hosted_paper
            else (os.getenv("ALPACA_TRADING_BASE_URL") or default_trading_base).rstrip("/")),
        data_base_url=("https://data.alpaca.markets" if hosted_paper
            else (os.getenv("ALPACA_DATA_BASE_URL") or "https://data.alpaca.markets").rstrip("/")),
        allow_live_trading=False if hosted_paper else _normalize_bool(os.getenv("AUTOBOTT_LIVE_TRADING_ENABLED"), default=False),
        allow_order_placement=_normalize_bool(os.getenv("AUTOBOTT_ALLOW_ORDER_PLACEMENT"), default=hosted_paper),
        max_position_cost=HOSTED_MAX_POSITION_COST if hosted_paper else float(os.getenv("AUTOBOTT_MAX_POSITION_COST", "100")),
        max_daily_loss=HOSTED_MAX_DAILY_LOSS if hosted_paper else float(os.getenv("AUTOBOTT_MAX_DAILY_LOSS", "500")),
        max_open_positions=hosted_max_legs if hosted_paper else int(os.getenv("AUTOBOTT_MAX_OPEN_POSITIONS", "3")),
        paper_trade_all_passed_signals=True if hosted_paper else paper_trade_all_passed_signals,
        paper_max_new_entry_attempts_per_loop=(HOSTED_MAX_NEW_PAIRS_PER_CYCLE if hosted_paper
            else _normalize_optional_int(os.getenv("AUTOBOTT_PAPER_MAX_NEW_ENTRY_ATTEMPTS_PER_LOOP"), default=25 if paper_trade_all_passed_signals else None)),
        paper_max_open_entry_buy_orders=(hosted_max_legs if hosted_paper
            else _normalize_optional_int(os.getenv("AUTOBOTT_PAPER_MAX_OPEN_ENTRY_BUY_ORDERS"), default=25 if paper_trade_all_passed_signals else None)),
        paper_ignore_position_cost_limit=False if hosted_paper else _normalize_bool(os.getenv("AUTOBOTT_PAPER_IGNORE_POSITION_COST_LIMIT"), default=True),
        portfolio_premium_limit=PORTFOLIO_PREMIUM_ENVELOPE if portfolio_mode else None,
    )


def require_alpaca_execution_config() -> AlpacaExecutionConfig:
    return load_alpaca_execution_config().validate()
