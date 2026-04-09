"""Configuration for the intraday options autotrader."""

import os
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _env_csv_dates(name: str, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    value = os.getenv(name)
    if value is None:
        return default
    items = [item.strip() for item in value.split(",")]
    return tuple(item for item in items if item)


def _env_csv_strings(name: str, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    value = os.getenv(name)
    if value is None:
        return default
    items = [item.strip() for item in value.split(",")]
    return tuple(item for item in items if item)


_DEFAULT_DATA_DIR = Path(__file__).resolve().parent
_DATA_DIR = Path(os.getenv("DATA_DIR", str(_DEFAULT_DATA_DIR)))


TICKERS = [
    "SPY",
    "QQQ",
    "IWM",
    "DIA",
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "META",
    "GOOGL",
    "TSLA",
    "AMD",
    "NFLX",
    "CRM",
    "INTC",
]
AUTO_EXPAND_UNIVERSE_WITH_MOVERS = _env_bool("AUTO_EXPAND_UNIVERSE_WITH_MOVERS", True)
UNIVERSE_MOVER_TOP = _env_int("UNIVERSE_MOVER_TOP", 100)
UNIVERSE_MAX_TICKERS = _env_int("UNIVERSE_MAX_TICKERS", 300)
CORE_TICKERS = [
    "SPY",
    "QQQ",
    "IWM",
    "DIA",
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "META",
    "GOOGL",
    "TSLA",
    "AMD",
    "NFLX",
    "CRM",
    "INTC",
    "AVGO",
    "ADBE",
    "ORCL",
    "JPM",
    "XOM",
]
BAR_TIMEFRAME = "5Min"
SIGNAL_LOOKBACK = 20
MAX_POSITIONS = 3
POSITION_SIZE_USD = 500
RISK_PER_TRADE_PCT = _env_float("RISK_PER_TRADE_PCT", 0.01)
MAX_POSITION_SIZE_USD = _env_float("MAX_POSITION_SIZE_USD", 700.0)
DRAWDOWN_REDUCE_AFTER_CONSEC_LOSSES = _env_int("DRAWDOWN_REDUCE_AFTER_CONSEC_LOSSES", 2)
DRAWDOWN_SIZE_MULTIPLIER = _env_float("DRAWDOWN_SIZE_MULTIPLIER", 0.5)
PROFIT_TARGET_PCT = 0.80
STOP_LOSS_PCT = 0.45
DAILY_LOSS_LIMIT_USD = 300.0
WEEKLY_LOSS_LIMIT_USD = _env_float("WEEKLY_LOSS_LIMIT_USD", 900.0)
CONSECUTIVE_LOSS_LIMIT = 3
MARKET_OPEN = "09:30"
PREOPEN_READY_MINUTES = 10
HARD_CLOSE_TIME = "15:45"
NO_NEW_TRADES_BEFORE = "10:00"
NO_NEW_TRADES_AFTER = "15:15"
PAPER = _env_bool("PAPER_TRADING", True)
LOOP_INTERVAL_SECONDS = 60
SCAN_MORNING_TIME = "09:30"
OBSERVATION_END_TIME = "10:00"
OBSERVATION_ENABLED = _env_bool("OBSERVATION_ENABLED", True)
ENABLE_NEWS_EVENT_BLOCK = _env_bool("ENABLE_NEWS_EVENT_BLOCK", True)
NEWS_LOOKBACK_MINUTES = _env_int("NEWS_LOOKBACK_MINUTES", 90)
NEWS_BLOCK_KEYWORDS = _env_csv_strings(
    "NEWS_BLOCK_KEYWORDS",
    default=(
        "earnings",
        "guidance",
        "sec",
        "investigation",
        "lawsuit",
        "fda",
        "downgrade",
        "upgrade",
        "cpi",
        "fomc",
        "fed",
    ),
)
ENABLE_HISTORICAL_REGIME_SCORE = _env_bool("ENABLE_HISTORICAL_REGIME_SCORE", True)
MIN_HISTORICAL_REGIME_SCORE = _env_float("MIN_HISTORICAL_REGIME_SCORE", 2.0)
ENABLE_SIGNAL_SCORING = _env_bool("ENABLE_SIGNAL_SCORING", True)
MIN_SIGNAL_SCORE = _env_float("MIN_SIGNAL_SCORE", 5.0)
MAX_ENTRY_SLIPPAGE_PCT = _env_float("MAX_ENTRY_SLIPPAGE_PCT", 2.0)
MAX_FILL_SLIPPAGE_PCT = _env_float("MAX_FILL_SLIPPAGE_PCT", 5.0)
NEWS_BLOCK_DATES_ET = _env_csv_dates("NEWS_BLOCK_DATES_ET", default=())
MAX_HOLD_MINUTES = _env_int("MAX_HOLD_MINUTES", 75)
ENABLE_VIX_GUARD = _env_bool("ENABLE_VIX_GUARD", True)
VIX_MIN = _env_float("VIX_MIN", 13.0)
VIX_MAX = _env_float("VIX_MAX", 40.0)

MIN_SHARE_PRICE = 10
MAX_SHARE_PRICE = 800
SCREENER_TOP_N = 20
MOVER_SYMBOLS_PER_SIDE = 10
SCAN_INTRADAY_BARS = 60
SCAN_MIN_BARS = _env_int("SCAN_MIN_BARS", 5)
SCAN_DAILY_BARS = 30
RVOL_MIN = 1.5
ATR_PCT_MIN = 1.5
VWAP_NEUTRAL_BAND_PCT = 0.1
ROC_PERIOD = 10
ROC_BULL_MIN = 0.3
ROC_BEAR_MAX = -0.3
IV_RANK_MIN = 20.0
IV_RANK_MAX = 60.0
EARNINGS_LOOKAHEAD_DAYS = 2
EARNINGS_CHECK_STRICT = False

PDT_MIN_EQUITY = 25000.0
PDT_MAX_DAY_TRADES_5D = 3
ENFORCE_PDT_GUARD = True

MIN_OPTION_OPEN_INTEREST = 500
MIN_OPTION_DAILY_VOLUME = 100
MAX_OPTION_SPREAD_PCT = 15.0
MIN_DTE_TRADING_DAYS = 2
MAX_DTE_TRADING_DAYS = 5

EASTERN_TZ = "US/Eastern"
CENTRAL_TZ = "US/Central"
RATE_LIMIT_SLEEP_SECONDS = 0.3
CLOSED_MIN_SLEEP_SECONDS = _env_int("CLOSED_MIN_SLEEP_SECONDS", 60)
CLOSED_MAX_SLEEP_SECONDS = _env_int("CLOSED_MAX_SLEEP_SECONDS", 900)
TRADES_CSV_PATH = _DATA_DIR / "trades.csv"
STATE_JSON_PATH = _DATA_DIR / "runtime_state.json"
SCAN_LOG_CSV_PATH = _DATA_DIR / "scan_log.csv"
OBSERVATION_LOG_CSV_PATH = _DATA_DIR / "observation_log.csv"
TRADING_CONTROL_PATH = _DATA_DIR / "trading_control.json"
MANUAL_PAUSE_SLEEP_SECONDS = _env_int("MANUAL_PAUSE_SLEEP_SECONDS", 30)
HEARTBEAT_SECONDS = _env_int("HEARTBEAT_SECONDS", 300)
ALERT_COOLDOWN_SECONDS = _env_int("ALERT_COOLDOWN_SECONDS", 300)
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")

ALPACA_PAPER_BASE_URL = "https://paper-api.alpaca.markets"
ALPACA_DATA_BASE_URL = "https://data.alpaca.markets"
