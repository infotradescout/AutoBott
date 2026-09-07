from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StrategyPolicy:
    """One code-owned policy for the hosted paper strategy.

    Runtime modules may expose compatibility constants, but strategy-critical
    values originate here so entry, contract selection, pair lifecycle, and
    account protection cannot quietly drift apart.
    """

    version: str = "hosted-core-runner-v2"

    bar_timeframe: str = "1Hour"
    lookback_bars: int = 35
    lookback_calendar_days: int = 14

    tactical_min_dte: int = 5
    tactical_max_dte: int = 10
    rider_min_dte: int = 14
    rider_max_dte: int = 45
    exit_min_dte: int = 2

    runner_max_cost_ratio: float = 0.40
    runner_target_cost_ratio: float = 0.25
    core_max_spread_pct: float = 0.18
    runner_max_spread_pct: float = 0.25
    core_min_open_interest: int = 0
    runner_min_open_interest: int = 0
    core_min_volume: int = 10
    runner_min_volume: int = 1
    core_min_abs_delta: float = 0.25
    runner_min_abs_delta: float = 0.10
    runner_target_abs_delta: float = 0.20
    runner_max_abs_delta: float = 0.35

    pair_max_loss_pct: float = 0.35
    runner_funding_buffer_dollars: float = 0.0
    unfunded_runner_stop_loss_pct: float = 0.70
    funded_runner_trailing_activation_pct: float = 0.75
    funded_runner_trailing_drawdown_pct: float = 0.35
    funded_runner_catastrophic_stop_loss_pct: float = 0.90

    max_new_pairs_per_cycle: int = 3
    max_open_legs: int = 6
    max_position_cost: float = 1000.0
    max_daily_loss: float = 750.0
    open_drawdown_max_loss: float = 750.0
    open_drawdown_min_losers: int = 3
    open_drawdown_loss_rate: float = 0.60

    session_interval_seconds: int = 90
    session_start_time: str = "09:35"
    session_end_time: str = "15:55"
    session_market_timezone: str = "America/New_York"
    session_symbol_batch_size: int = 25

    loss_guard_lookback: int = 30
    loss_guard_consecutive_losses: int = 3
    loss_guard_min_sample: int = 5
    loss_guard_loss_rate: float = 0.70
    winner_bias_lookback: int = 30
    winner_bias_min_sample: int = 5
    winner_bias_win_rate: float = 0.60
    winner_bias_consecutive_wins: int = 3

    def validate(self) -> "StrategyPolicy":
        if not self.version.strip():
            raise ValueError("strategy_policy_version_required")
        if self.bar_timeframe != "1Hour":
            raise ValueError("hosted_strategy_requires_hourly_decision_bars")
        if not 1 <= self.tactical_min_dte <= self.tactical_max_dte < self.rider_min_dte <= self.rider_max_dte:
            raise ValueError("invalid_strategy_dte_windows")
        if not 0 <= self.exit_min_dte < self.tactical_min_dte:
            raise ValueError("exit_min_dte_must_precede_entry_window")
        if not 0 < self.runner_target_cost_ratio <= self.runner_max_cost_ratio < 1:
            raise ValueError("invalid_runner_cost_window")
        if not 0 < self.core_max_spread_pct < 1 or not 0 < self.runner_max_spread_pct < 1:
            raise ValueError("invalid_spread_caps")
        if not 0 < self.core_min_abs_delta < 1:
            raise ValueError("invalid_core_delta_floor")
        if not 0 < self.runner_min_abs_delta <= self.runner_target_abs_delta <= self.runner_max_abs_delta < self.core_min_abs_delta:
            raise ValueError("invalid_runner_delta_window")
        if not 0 < self.pair_max_loss_pct < 1:
            raise ValueError("invalid_pair_max_loss")
        if self.runner_funding_buffer_dollars < 0:
            raise ValueError("invalid_runner_funding_buffer")
        if not 0 < self.unfunded_runner_stop_loss_pct < self.funded_runner_catastrophic_stop_loss_pct < 1:
            raise ValueError("invalid_runner_loss_limits")
        if not 0 < self.funded_runner_trailing_activation_pct < 1:
            raise ValueError("invalid_funded_runner_activation")
        if not 0 < self.funded_runner_trailing_drawdown_pct < 1:
            raise ValueError("invalid_funded_runner_drawdown")
        if self.max_new_pairs_per_cycle <= 0 or self.max_open_legs < 2:
            raise ValueError("invalid_pair_capacity")
        if self.max_position_cost <= 0 or self.max_daily_loss <= 0:
            raise ValueError("invalid_account_limits")
        return self


HOSTED_STRATEGY_POLICY = StrategyPolicy().validate()
