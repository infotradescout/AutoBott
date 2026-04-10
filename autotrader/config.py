"""Configuration for the intraday options autotrader."""

import os
from pathlib import Path


def _is_writable_dir(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / ".write_test"
        with probe.open("w", encoding="utf-8") as f:
            f.write("ok")
        probe.unlink(missing_ok=True)
        return True
    except Exception:
        return False


def _resolve_data_dir() -> Path:
    env_path = os.getenv("DATA_DIR")
    candidates: list[Path] = []
    if env_path:
        candidates.append(Path(env_path))
    candidates.append(_DEFAULT_DATA_DIR)
    candidates.append(Path("/tmp/autotrader-data"))

    for candidate in candidates:
        if _is_writable_dir(candidate):
            if env_path and str(candidate) != str(Path(env_path)):
                print(
                    f"[config] DATA_DIR '{env_path}' is not writable. "
                    f"Using fallback '{candidate}'."
                )
            return candidate

    # Final defensive fallback: current working directory.
    cwd = Path.cwd()
    cwd.mkdir(parents=True, exist_ok=True)
    print(f"[config] No writable data directory candidates found. Using '{cwd}'.")
    return cwd


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


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip() or default


_DEFAULT_DATA_DIR = Path(__file__).resolve().parent
_DATA_DIR = _resolve_data_dir()


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
MAX_POSITIONS = _env_int("MAX_POSITIONS", 2)
POSITION_SIZE_USD = 500
RISK_PER_TRADE_PCT = _env_float("RISK_PER_TRADE_PCT", 0.01)
MAX_POSITION_SIZE_USD = _env_float("MAX_POSITION_SIZE_USD", 700.0)
DRAWDOWN_REDUCE_AFTER_CONSEC_LOSSES = _env_int("DRAWDOWN_REDUCE_AFTER_CONSEC_LOSSES", 2)
DRAWDOWN_SIZE_MULTIPLIER = _env_float("DRAWDOWN_SIZE_MULTIPLIER", 0.5)
PROFIT_TARGET_PCT = _env_float("PROFIT_TARGET_PCT", 0.30)
STOP_LOSS_PCT = _env_float("STOP_LOSS_PCT", 0.25)
DAILY_LOSS_LIMIT_USD = _env_float("DAILY_LOSS_LIMIT_USD", 150.0)
WEEKLY_LOSS_LIMIT_USD = _env_float("WEEKLY_LOSS_LIMIT_USD", 900.0)
CONSECUTIVE_LOSS_LIMIT = _env_int("CONSECUTIVE_LOSS_LIMIT", 3)
MARKET_OPEN = "09:30"
PREOPEN_READY_MINUTES = 10
HARD_CLOSE_TIME = _env_str("HARD_CLOSE_TIME", "15:30")
OPTION_EXPIRY_EXIT_TIME = _env_str("OPTION_EXPIRY_EXIT_TIME", "15:00")
OPTION_FORCE_EXIT_DAYS_BEFORE_EXPIRY = _env_int("OPTION_FORCE_EXIT_DAYS_BEFORE_EXPIRY", 1)
NO_NEW_TRADES_BEFORE = _env_str("NO_NEW_TRADES_BEFORE", "09:30")
NO_NEW_TRADES_AFTER = _env_str("NO_NEW_TRADES_AFTER", "15:30")
PAPER = _env_bool("PAPER_TRADING", True)
LOOP_INTERVAL_SECONDS = _env_int("LOOP_INTERVAL_SECONDS", 30)
SCAN_MORNING_TIME = "09:30"
OBSERVATION_END_TIME = "10:00"
OBSERVATION_ENABLED = _env_bool("OBSERVATION_ENABLED", True)
ENABLE_CATALYST_MODE = _env_bool("ENABLE_CATALYST_MODE", False)
CATALYST_WINDOW_MINUTES = _env_int("CATALYST_WINDOW_MINUTES", 90)
CATALYST_INDEX_5M_MOVE_PCT = _env_float("CATALYST_INDEX_5M_MOVE_PCT", 1.2)
CATALYST_BREADTH_MOVE_PCT = _env_float("CATALYST_BREADTH_MOVE_PCT", 1.0)
CATALYST_BREADTH_MIN_COUNT = _env_int("CATALYST_BREADTH_MIN_COUNT", 6)
CATALYST_RELAXED_RVOL_MIN = _env_float("CATALYST_RELAXED_RVOL_MIN", 0.6)
CATALYST_DISABLE_RSI = _env_bool("CATALYST_DISABLE_RSI", True)
CATALYST_ALLOW_IV_FALLBACK = _env_bool("CATALYST_ALLOW_IV_FALLBACK", True)
CATALYST_RELAXED_IV_RANK_MAX = _env_float("CATALYST_RELAXED_IV_RANK_MAX", 90.0)
CATALYST_RELAXED_MIN_SIGNAL_SCORE = _env_float("CATALYST_RELAXED_MIN_SIGNAL_SCORE", 2.5)
ENABLE_HTF_CONFIRM = False
HTF_TIMEFRAME = _env_str("HTF_TIMEFRAME", "15m")
HTF_LOOKBACK_BARS = _env_int("HTF_LOOKBACK_BARS", 30)
ENABLE_ORDER_FLOW_FILTER = False
MIN_FLOW_SCORE = 0.05
ENABLE_NEWS_EVENT_BLOCK = False
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
ENABLE_HISTORICAL_REGIME_SCORE = False
MIN_HISTORICAL_REGIME_SCORE = 2.0
ENABLE_SIGNAL_SCORING = _env_bool("ENABLE_SIGNAL_SCORING", True)
MIN_SIGNAL_SCORE = _env_float("MIN_SIGNAL_SCORE", 5.0)
MAX_ENTRY_SLIPPAGE_PCT = _env_float("MAX_ENTRY_SLIPPAGE_PCT", 5.0)
MAX_FILL_SLIPPAGE_PCT = _env_float("MAX_FILL_SLIPPAGE_PCT", 5.0)
NEWS_BLOCK_DATES_ET = _env_csv_dates("NEWS_BLOCK_DATES_ET", default=())
MAX_HOLD_MINUTES = _env_int("MAX_HOLD_MINUTES", 15)
ENABLE_VIX_GUARD = False
VIX_MIN = 13.0
VIX_MAX = 80.0

MIN_SHARE_PRICE = 10
MAX_SHARE_PRICE = _env_int("MAX_SHARE_PRICE", 2000)
SCREENER_TOP_N = 20
MOVER_SYMBOLS_PER_SIDE = 10
SCAN_INTRADAY_BARS = 60
SCAN_MIN_BARS = _env_int("SCAN_MIN_BARS", 5)
ENABLE_OPENING_ENTRY_RELAX = _env_bool("ENABLE_OPENING_ENTRY_RELAX", True)
OPENING_ENTRY_RELAX_MINUTES = _env_int("OPENING_ENTRY_RELAX_MINUTES", 7)
SCAN_DAILY_BARS = 30

# --- Scanner thresholds (all tunable via env vars) ---
RVOL_MIN = _env_float("RVOL_MIN", 0.9)
OPENING_RVOL_MIN = _env_float("OPENING_RVOL_MIN", 0.35)
RVOL_STRICT_UNTIL = _env_str("RVOL_STRICT_UNTIL", "10:30")
RVOL_RELAX_AFTER = _env_str("RVOL_RELAX_AFTER", "10:00")
RVOL_RELAXED_MIN = _env_float("RVOL_RELAXED_MIN", 0.7)
RVOL_IGNORE_AFTER = _env_str("RVOL_IGNORE_AFTER", "10:30")
ATR_PCT_MIN = _env_float("ATR_PCT_MIN", 1.8)
VWAP_NEUTRAL_BAND_PCT = _env_float("VWAP_NEUTRAL_BAND_PCT", 0.15)
ROC_PERIOD = 10
ROC_BULL_MIN = _env_float("ROC_BULL_MIN", 0.12)
ROC_BEAR_MAX = _env_float("ROC_BEAR_MAX", -0.12)
ENABLE_ROC_FILTER = _env_bool("ENABLE_ROC_FILTER", True)
RSI_EARLY_MIN_PERIOD = _env_int("RSI_EARLY_MIN_PERIOD", 5)
RSI_STRICT_AFTER_TIME = "10:15"
ENABLE_RSI_FILTER = _env_bool("ENABLE_RSI_FILTER", True)

RSI_CALL_MIN = _env_float("RSI_CALL_MIN", 52.0)
RSI_CALL_MAX = _env_float("RSI_CALL_MAX", 75.0)
RSI_PUT_MIN = _env_float("RSI_PUT_MIN", 25.0)
RSI_PUT_MAX = _env_float("RSI_PUT_MAX", 48.0)

IV_RANK_MIN = 20.0
IV_RANK_MAX = _env_float("IV_RANK_MAX", 99.0)
EARNINGS_LOOKAHEAD_DAYS = 2
EARNINGS_CHECK_STRICT = False
EARNINGS_SKIP_SYMBOLS = _env_csv_strings(
    "EARNINGS_SKIP_SYMBOLS",
    default=("SPY", "QQQ", "IWM", "DIA", "VIX", "^VIX"),
)

PDT_MIN_EQUITY = 25000.0
PDT_MAX_DAY_TRADES_5D = 3
ENFORCE_PDT_GUARD = False

# --- Options contract selection ---
MIN_OPTION_OPEN_INTEREST = _env_int("MIN_OPTION_OPEN_INTEREST", 50)
MIN_OPTION_DAILY_VOLUME = _env_int("MIN_OPTION_DAILY_VOLUME", 10)
MAX_OPTION_SPREAD_PCT = _env_float("MAX_OPTION_SPREAD_PCT", 30.0)
MIN_DTE_TRADING_DAYS = _env_int("MIN_DTE_TRADING_DAYS", 1)
MAX_DTE_TRADING_DAYS = _env_int("MAX_DTE_TRADING_DAYS", 2)
MIN_OPTION_OPEN_INTEREST_0DTE = _env_int("MIN_OPTION_OPEN_INTEREST_0DTE", 100)
ENABLE_DELTA_TARGETING = _env_bool("ENABLE_DELTA_TARGETING", True)
TARGET_DELTA_MIN = _env_float("TARGET_DELTA_MIN", 0.40)
TARGET_DELTA_MAX = _env_float("TARGET_DELTA_MAX", 0.55)
TARGET_DELTA_FALLBACK = _env_float("TARGET_DELTA_FALLBACK", 0.50)
EMERGENCY_EXECUTION_MODE = _env_bool("EMERGENCY_EXECUTION_MODE", False)
ALLOW_MARKET_ENTRY_WITHOUT_QUOTE = _env_bool("ALLOW_MARKET_ENTRY_WITHOUT_QUOTE", False)

# --- Entry confirmation / index regime ---
ENABLE_ENTRY_CONFIRMATION = _env_bool("ENABLE_ENTRY_CONFIRMATION", True)
ENTRY_CONFIRM_BARS = _env_int("ENTRY_CONFIRM_BARS", 2)
ENABLE_INDEX_BIAS_FILTER = _env_bool("ENABLE_INDEX_BIAS_FILTER", True)
INDEX_BIAS_TIMEFRAME = _env_str("INDEX_BIAS_TIMEFRAME", "5m")
INDEX_BIAS_LOOKBACK = _env_int("INDEX_BIAS_LOOKBACK", 30)

# --- Exit behavior ---
ENABLE_FIXED_PROFIT_TARGET = _env_bool("ENABLE_FIXED_PROFIT_TARGET", True)
TRAIL_LOCK1_TRIGGER_PCT = _env_float("TRAIL_LOCK1_TRIGGER_PCT", 0.35)
TRAIL_LOCK1_STOP_PCT = _env_float("TRAIL_LOCK1_STOP_PCT", 0.15)
TRAIL_LOCK2_TRIGGER_PCT = _env_float("TRAIL_LOCK2_TRIGGER_PCT", 0.55)
TRAIL_LOCK2_STOP_PCT = _env_float("TRAIL_LOCK2_STOP_PCT", 0.30)

# --- Re-entry ---
MAX_REENTRIES_PER_TICKER = _env_int("MAX_REENTRIES_PER_TICKER", 1)

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
DASHBOARD_CONTROL_TOKEN = _env_str("DASHBOARD_CONTROL_TOKEN", "")
ENTRY_ORDER_STATUS_WAIT_SECONDS = _env_int("ENTRY_ORDER_STATUS_WAIT_SECONDS", 8)
ENTRY_RETRY_STATUS_WAIT_SECONDS = _env_int("ENTRY_RETRY_STATUS_WAIT_SECONDS", 5)
ENTRY_MARKET_FALLBACK_WAIT_SECONDS = _env_int("ENTRY_MARKET_FALLBACK_WAIT_SECONDS", 3)
EXIT_ORDER_STATUS_POLL_SECONDS = _env_int("EXIT_ORDER_STATUS_POLL_SECONDS", 2)
EXIT_ORDER_MAX_WAIT_SECONDS = _env_int("EXIT_ORDER_MAX_WAIT_SECONDS", 20)
EXIT_CLOSE_RETRY_ATTEMPTS = _env_int("EXIT_CLOSE_RETRY_ATTEMPTS", 2)
TRADES_MAX_ROWS = _env_int("TRADES_MAX_ROWS", 5000)

ALPACA_PAPER_BASE_URL = "https://paper-api.alpaca.markets"
ALPACA_DATA_BASE_URL = "https://data.alpaca.markets"
