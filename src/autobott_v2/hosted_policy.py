from __future__ import annotations

import os


# Strategy-critical production values live in code. Render retains old
# environment values across deploys, so using env vars for these controls can
# leave a new build running an obsolete trading policy.
HOSTED_SESSION_SYMBOL_TOKENS = ("VIX", "VXX", "UVXY", "TOP_OPTIONS_100")
HOSTED_PRIORITY_SYMBOLS = ("VIX", "VXX", "UVXY", "SPY", "QQQ")
HOSTED_VOLATILITY_SYMBOLS = ("VIX", "VIXW", "VXX", "UVXY")
HOSTED_VOLATILITY_EXPOSURE_GROUP = frozenset(HOSTED_VOLATILITY_SYMBOLS)

HOSTED_SESSION_INTERVAL_SECONDS = 90
HOSTED_SESSION_SYMBOL_BATCH_SIZE = 25
HOSTED_SESSION_START_TIME = "09:35"
HOSTED_SESSION_END_TIME = "15:55"
HOSTED_SESSION_MARKET_TIMEZONE = "America/New_York"
# Position management already runs at the start of every 90-second trading
# cycle. A second retained heartbeat only duplicates broker reads and can push
# the paper account into Alpaca's rate limit.
HOSTED_POSITION_MONITOR_HEARTBEAT_ENABLED = False
HOSTED_POSITION_MONITOR_HEARTBEAT_SECONDS = HOSTED_SESSION_INTERVAL_SECONDS
HOSTED_MAX_NEW_PAIRS_PER_CYCLE = 3
HOSTED_MAX_OPEN_LEGS = 6
HOSTED_MAX_POSITION_COST = 1000.0
HOSTED_MAX_DAILY_LOSS = 750.0
HOSTED_OPEN_DRAWDOWN_MAX_LOSS = 750.0
HOSTED_OPEN_DRAWDOWN_MIN_LOSERS = 3
HOSTED_OPEN_DRAWDOWN_LOSS_RATE = 0.60

HOSTED_TACTICAL_MIN_DTE = 5
HOSTED_TACTICAL_MAX_DTE = 10
HOSTED_RIDER_MIN_DTE = 14
HOSTED_RIDER_MAX_DTE = 45
HOSTED_EXIT_MIN_DTE = 2

# The hosted strategy holds options for days, so one-minute noise is the wrong
# decision horizon. Thirty-five hourly bars cover roughly one trading week and
# still let the 90-second session react to a newly completed bar.
HOSTED_BAR_TIMEFRAME = "1Hour"
HOSTED_LOOKBACK_BARS = 35
HOSTED_LOOKBACK_CALENDAR_DAYS = 14

# Open interest is absent from portions of Alpaca's indicative paper feed.
# Hosted selection still requires a live two-sided quote, bounded spread,
# volume when supplied, delta, vega, theta, DTE, and price. Missing OI must not
# be converted into a false zero-liquidity rejection.
HOSTED_MIN_OPEN_INTEREST = 0
HOSTED_POLICY_VERSION = "hosted-vix-profit-v1"

# The raw market corpus is disposable; orders and outcome journals are stored
# outside this tree. Keep enough headroom for the next cycle even when Render
# retains obsolete environment values from an earlier deployment.
HOSTED_SNAPSHOT_MAX_BYTES = 128 * 1024 * 1024
HOSTED_MIN_FREE_BYTES = 128 * 1024 * 1024
HOSTED_CAPTURE_OPTION_QUOTE_FILES = False

HOSTED_LOSS_GUARD_LOOKBACK = 30
HOSTED_LOSS_GUARD_CONSECUTIVE_LOSSES = 3
HOSTED_LOSS_GUARD_MIN_SAMPLE = 5
HOSTED_LOSS_GUARD_LOSS_RATE = 0.70
HOSTED_WINNER_BIAS_LOOKBACK = 30
HOSTED_WINNER_BIAS_MIN_SAMPLE = 5
HOSTED_WINNER_BIAS_WIN_RATE = 0.60
HOSTED_WINNER_BIAS_CONSECUTIVE_WINS = 3


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
    """Return true for the deployed service, whose broker mode is always paper.

    Render retains old environment values across deploys. In particular, an
    obsolete ``ALPACA_ENV=live`` must not turn off every hosted policy override
    or point this application at the live-money API.
    """

    return is_hosted_runtime()


def is_volatility_symbol(symbol: str) -> bool:
    return symbol.strip().upper() in HOSTED_VOLATILITY_EXPOSURE_GROUP


def signal_proxy_for(symbol: str) -> str:
    """Map non-equity index underlyings to a tradable signal proxy."""

    normalized = symbol.strip().upper()
    if normalized in {"VIX", "VIXW"}:
        return "VIXY"
    return normalized
