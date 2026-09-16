from __future__ import annotations

import os

from .strategy_policy import HOSTED_STRATEGY_POLICY


# Compatibility constants remain because older modules import these names.
# Their values now originate from one validated policy instead of competing
# module defaults and retained deployment variables.
HOSTED_SESSION_SYMBOL_TOKENS = ("VIX", "VXX", "UVXY", "TOP_OPTIONS_100")
HOSTED_PRIORITY_SYMBOLS = ("VIX", "VXX", "UVXY", "SPY", "QQQ")
HOSTED_VOLATILITY_SYMBOLS = ("VIX", "VIXW", "VXX", "UVXY")
HOSTED_VOLATILITY_EXPOSURE_GROUP = frozenset(HOSTED_VOLATILITY_SYMBOLS)

HOSTED_SESSION_INTERVAL_SECONDS = HOSTED_STRATEGY_POLICY.session_interval_seconds
HOSTED_SESSION_SYMBOL_BATCH_SIZE = HOSTED_STRATEGY_POLICY.session_symbol_batch_size
HOSTED_SESSION_START_TIME = HOSTED_STRATEGY_POLICY.session_start_time
HOSTED_SESSION_END_TIME = HOSTED_STRATEGY_POLICY.session_end_time
HOSTED_SESSION_MARKET_TIMEZONE = HOSTED_STRATEGY_POLICY.session_market_timezone
HOSTED_POSITION_MONITOR_HEARTBEAT_ENABLED = False
HOSTED_POSITION_MONITOR_HEARTBEAT_SECONDS = HOSTED_SESSION_INTERVAL_SECONDS
HOSTED_MAX_NEW_PAIRS_PER_CYCLE = HOSTED_STRATEGY_POLICY.max_new_pairs_per_cycle
HOSTED_MAX_OPEN_LEGS = HOSTED_STRATEGY_POLICY.max_open_legs
HOSTED_MAX_POSITION_COST = HOSTED_STRATEGY_POLICY.max_position_cost
HOSTED_MAX_DAILY_LOSS = HOSTED_STRATEGY_POLICY.max_daily_loss
HOSTED_OPEN_DRAWDOWN_MAX_LOSS = HOSTED_STRATEGY_POLICY.open_drawdown_max_loss
HOSTED_OPEN_DRAWDOWN_MIN_LOSERS = HOSTED_STRATEGY_POLICY.open_drawdown_min_losers
HOSTED_OPEN_DRAWDOWN_LOSS_RATE = HOSTED_STRATEGY_POLICY.open_drawdown_loss_rate

HOSTED_TACTICAL_MIN_DTE = HOSTED_STRATEGY_POLICY.tactical_min_dte
HOSTED_TACTICAL_MAX_DTE = HOSTED_STRATEGY_POLICY.tactical_max_dte
HOSTED_RIDER_MIN_DTE = HOSTED_STRATEGY_POLICY.rider_min_dte
HOSTED_RIDER_MAX_DTE = HOSTED_STRATEGY_POLICY.rider_max_dte
HOSTED_EXIT_MIN_DTE = HOSTED_STRATEGY_POLICY.exit_min_dte

HOSTED_BAR_TIMEFRAME = HOSTED_STRATEGY_POLICY.bar_timeframe
HOSTED_LOOKBACK_BARS = HOSTED_STRATEGY_POLICY.lookback_bars
HOSTED_LOOKBACK_CALENDAR_DAYS = HOSTED_STRATEGY_POLICY.lookback_calendar_days
HOSTED_MIN_OPEN_INTEREST = HOSTED_STRATEGY_POLICY.core_min_open_interest
HOSTED_POLICY_VERSION = HOSTED_STRATEGY_POLICY.version

HOSTED_SNAPSHOT_MAX_BYTES = 128 * 1024 * 1024
HOSTED_MIN_FREE_BYTES = 128 * 1024 * 1024
HOSTED_CAPTURE_OPTION_QUOTE_FILES = False

HOSTED_LOSS_GUARD_LOOKBACK = HOSTED_STRATEGY_POLICY.loss_guard_lookback
HOSTED_LOSS_GUARD_CONSECUTIVE_LOSSES = HOSTED_STRATEGY_POLICY.loss_guard_consecutive_losses
HOSTED_LOSS_GUARD_MIN_SAMPLE = HOSTED_STRATEGY_POLICY.loss_guard_min_sample
HOSTED_LOSS_GUARD_LOSS_RATE = HOSTED_STRATEGY_POLICY.loss_guard_loss_rate
HOSTED_WINNER_BIAS_LOOKBACK = HOSTED_STRATEGY_POLICY.winner_bias_lookback
HOSTED_WINNER_BIAS_MIN_SAMPLE = HOSTED_STRATEGY_POLICY.winner_bias_min_sample
HOSTED_WINNER_BIAS_WIN_RATE = HOSTED_STRATEGY_POLICY.winner_bias_win_rate
HOSTED_WINNER_BIAS_CONSECUTIVE_WINS = HOSTED_STRATEGY_POLICY.winner_bias_consecutive_wins


def active_build_sha() -> str | None:
    value = os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT")
    normalized = (value or "").strip()
    return normalized or None


def is_hosted_runtime() -> bool:
    """Return true for the deployed Render service without user setup."""

    if (os.getenv("RENDER") or "").strip().lower() in {"1", "true", "yes", "on"}:
        return True
    if (os.getenv("RENDER_SERVICE_ID") or "").strip():
        return True
    data_root = (os.getenv("AUTOBOTT_DATA_ROOT") or "").replace("\\", "/").rstrip("/")
    return data_root.startswith("/var/data/autobott")


def is_hosted_paper_runtime() -> bool:
    """Return true for the deployed service, whose broker mode is always paper."""

    return is_hosted_runtime()


def is_volatility_symbol(symbol: str) -> bool:
    return symbol.strip().upper() in HOSTED_VOLATILITY_EXPOSURE_GROUP


def signal_proxy_for(symbol: str) -> str:
    """Map non-equity index underlyings to a tradable signal proxy."""

    normalized = symbol.strip().upper()
    if normalized in {"VIX", "VIXW"}:
        return "VIXY"
    return normalized
