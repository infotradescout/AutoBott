"""Configuration for the intraday options autotrader.

All trading parameters are hardcoded here — just ask to change any value
and it will be updated and pushed directly. No env vars needed for tuning.

Only secrets (API keys, webhook URLs) and infrastructure paths remain as
env vars since those must stay out of source code.
"""

import os
from pathlib import Path


# ---------------------------------------------------------------------------
# Infrastructure helpers (keep as env vars — secrets / deployment-specific)
# ---------------------------------------------------------------------------

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
    candidates.append(Path("/data"))
    candidates.append(_DEFAULT_DATA_DIR)
    candidates.append(Path("/tmp/autotrader-data"))
    for candidate in candidates:
        if _is_writable_dir(candidate):
            if env_path and str(candidate) != str(Path(env_path)):
                print(f"[config] DATA_DIR '{env_path}' not writable. Using '{candidate}'.")
            return candidate
    cwd = Path.cwd()
    cwd.mkdir(parents=True, exist_ok=True)
    print(f"[config] No writable data directory found. Using '{cwd}'.")
    return cwd


_DEFAULT_DATA_DIR = Path(__file__).resolve().parent
_DATA_DIR = _resolve_data_dir()
DATA_DIR = _DATA_DIR

TICKERS = [
    "SPY", "QQQ", "IWM", "DIA",
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL",
    "TSLA", "AMD", "NFLX", "CRM", "INTC",
    "AVGO", "ADBE", "ORCL", "JPM", "XOM",
    "BAC", "WFC", "GS", "C", "UNH",
    "LLY", "ABBV", "PFE", "MRK", "JNJ",
    "MU", "SMCI", "PLTR", "SHOP", "UBER",
    "COIN", "HOOD", "SNOW", "PANW", "CRWD",
    "DIS", "PYPL", "SQ", "BA", "CAT",
    "CVX", "SLB", "OXY", "GE", "F",
]
CORE_TICKERS = TICKERS[:]

AUTO_EXPAND_UNIVERSE_WITH_MOVERS = True
UNIVERSE_MOVER_TOP                = 50
UNIVERSE_MAX_TICKERS              = 300
SCREENER_TOP_N                    = 100
MOVER_SYMBOLS_PER_SIDE            = 40
MIN_SHARE_PRICE                   = 10
MAX_SHARE_PRICE                   = 2000

BAR_TIMEFRAME       = "5Min"
SIGNAL_LOOKBACK     = 20
SCAN_INTRADAY_BARS  = 60
SCAN_MIN_BARS       = 5
SCAN_DAILY_BARS     = 30

MAX_POSITIONS                       = 12
POSITION_SIZE_USD                   = 600
RISK_PER_TRADE_PCT                  = 0.01
MAX_POSITION_SIZE_USD               = 1200.0
DRAWDOWN_REDUCE_AFTER_CONSEC_LOSSES = 2
DRAWDOWN_SIZE_MULTIPLIER            = 0.5
DAILY_LOSS_LIMIT_USD                = 1500.0
WEEKLY_LOSS_LIMIT_USD               = 5000.0
CONSECUTIVE_LOSS_LIMIT              = 99
INTRADAY_NET_LOSS_LIMIT_USD         = 0.0
ENABLE_ALPACA_TRUTH_LOSS_GUARD      = True
ALPACA_TRUTH_DAILY_LOSS_LIMIT_USD   = 1500.0
ALPACA_TRUTH_ADAPT_AFTER_LOSS_USD   = 1.0
ADAPTIVE_BLOCK_LOSING_TICKERS       = True
ADAPTIVE_LOSS_MIN_SIGNAL_SCORE      = 5.2
ADAPTIVE_LOSS_MIN_DIRECTION_SCORE   = 0.20
ADAPTIVE_LOSS_MAX_SPREAD_PCT        = 8.0
ADAPTIVE_LOSS_CAUSE_WINDOW          = 12
ADAPTIVE_LOSS_SIGNAL_SCORE_ADD_PER_LOSS = 0.0
ADAPTIVE_LOSS_MAX_SIGNAL_SCORE      = 6.2
ADAPTIVE_LOSS_DIRECTION_ADD_PER_WRONG = 0.0
ADAPTIVE_LOSS_MAX_DIRECTION_SCORE   = 0.50
ADAPTIVE_LOSS_EXECUTION_MAX_SPREAD_PCT = 8.0
ADAPTIVE_LOSS_QUICK_SECONDS         = 180
ADAPTIVE_LOSS_WRONG_WAY_UNDERLYING_MOVE_PCT = 0.05
ADAPTIVE_LOSS_SPREAD_CAUSE_PCT      = 4.0
ADAPTIVE_LOSS_SLIPPAGE_CAUSE_PCT    = 2.0
ADAPTIVE_LOSS_REQUIRE_MOMENTUM_AFTER_LOSSES = 999
ADAPTIVE_LOSS_MIN_ABS_ROC_PCT       = 0.0
ADAPTIVE_LOSS_MIN_RVOL              = 0.0
ADAPTIVE_LOSS_BLOCK_TICKER_AFTER_LOSSES = 5
ADAPTIVE_LOSS_WRONG_DIRECTION_COOLDOWN_MINUTES = 90
ADAPTIVE_LOSS_EXECUTION_COOLDOWN_MINUTES = 60
ADAPTIVE_LOSS_QUICK_COOLDOWN_MINUTES = 45
ADAPTIVE_LOSS_CHOP_COOLDOWN_MINUTES = 30
EARLY_RED_GUARD_ENABLED             = False
EARLY_RED_GUARD_MIN_CLOSED_TRADES   = 4
EARLY_RED_GUARD_MAX_NET_PNL_USD     = -0.01

# Paper-mode doctrine: do not let one loser freeze the bot.
LOSS_THROTTLE_AFTER_CONSEC_LOSSES   = 99
LOSS_THROTTLE_SIGNAL_SCORE_ADD      = 0.0
LOSS_THROTTLE_MIN_VOLATILITY_SCORE  = 0.0

MAX_PREMIUM_PER_TRADE_USD           = 600.0
MAX_TOTAL_OPEN_PREMIUM_USD          = 18000.0
OPENING_MAX_FRESH_PREMIUM_USD       = 9000.0
MAX_SAME_DIRECTION_POSITIONS        = 8

ENABLE_PREMIUM_CAP_QUALITY_OVERRIDE = True
EXPENSIVE_TRADE_MIN_SIGNAL_SCORE    = 8.0
EXPENSIVE_TRADE_MIN_DIRECTION_SCORE = 0.75
EXPENSIVE_TRADE_MIN_RVOL            = 1.8
EXPENSIVE_TRADE_MAX_SPREAD_PCT      = 8.0
OPENING_EXPENSIVE_TRADE_MIN_SIGNAL_SCORE = 8.8
EXPENSIVE_PREMIUM_SYMBOLS = ("TSLA", "MSFT", "AVGO", "NFLX", "META", "GOOGL", "AMZN")
PREFERRED_CORE_TICKERS = ("SPY", "QQQ", "IWM", "AAPL", "AMD", "INTC", "JPM", "XOM", "CRM", "ORCL")
MAX_NON_CORE_ENTRIES_PER_DAY        = 999
NON_CORE_MIN_SIGNAL_SCORE           = 6.8

ENABLE_VOLATILITY_ADAPTIVE_RISK     = True
VOL_RISK_ATR_PCT_HIGH               = 2.0
VOL_RISK_ATR_PCT_EXTREME            = 3.0
VOL_RISK_RVOL_HIGH                  = 1.8
VOL_RISK_RVOL_EXTREME               = 2.8
VOL_RISK_IV_RANK_HIGH               = 70.0
VOL_RISK_IV_RANK_EXTREME            = 85.0
VOL_RISK_SCORE_HIGH                 = 3
VOL_RISK_SCORE_EXTREME              = 5
VOL_STOP_LOSS_MULT_HIGH             = 1.20
VOL_STOP_LOSS_MULT_EXTREME          = 1.35
VOL_PREMIUM_CAP_MULT_HIGH           = 0.85
VOL_PREMIUM_CAP_MULT_EXTREME        = 0.70
VOL_OPEN_PREMIUM_CAP_MULT_HIGH      = 0.90
VOL_OPEN_PREMIUM_CAP_MULT_EXTREME   = 0.75

MARKET_OPEN                        = "09:30"
PREOPEN_READY_MINUTES              = 10
HARD_CLOSE_TIME                    = "16:00"
OPTION_EXPIRY_EXIT_TIME            = "15:55"
OPTION_FORCE_EXIT_DAYS_BEFORE_EXPIRY = 1
NO_NEW_TRADES_BEFORE               = "09:30"
NO_NEW_TRADES_AFTER                = "16:00"
SCAN_MORNING_TIME                  = "09:30"
OBSERVATION_END_TIME               = "10:00"
OBSERVATION_ENABLED                = True
ENABLE_PREMARKET_OPENING_SIGNALS   = True
PREMARKET_SIGNAL_WINDOW_START      = "08:00"
PREMARKET_SIGNAL_WINDOW_END        = "09:30"
PREMARKET_REPORT_READY_TIME        = "08:20"
PREMARKET_LOOKBACK_MINUTES         = 75
PREMARKET_MAX_SIGNALS              = 6
PREMARKET_APPLY_UNTIL              = "09:35"
PREMARKET_SCAN_INTERVAL_SECONDS    = 120
PREMARKET_SCAN_MAX_RUNS            = 0
LOOP_INTERVAL_SECONDS              = 15
MAX_HOLD_MINUTES                   = 90
ANTI_CHURN_HOLD_MINUTES            = 3

OPENING_STRICT_WINDOW_MINUTES                = 20
OPENING_STRICT_MIN_SIGNAL_SCORE              = 5.8
OPENING_STRICT_CONFIRM_BARS                  = 2
OPENING_STRICT_CONFIRM_MOMENTUM_THRESHOLD_PCT = 0.12
OPENING_STRICT_MIN_DIRECTION_SCORE           = 0.50
OPENING_STRICT_MIN_RVOL                      = 0.90
OPENING_STRICT_MIN_ROC_PCT                   = 0.12
OPENING_STRICT_MIN_VWAP_DISTANCE_PCT         = 0.07
OPENING_MAX_SIGNAL_CANDIDATES                = 3
OPENING_MAX_FRESH_ENTRIES                    = 999
OPENING_MAX_CONCURRENT_POSITIONS             = 999
OPENING_MAX_NEW_ENTRY_ATTEMPTS_PER_LOOP      = 6
MAX_NEW_ENTRY_ATTEMPTS_PER_LOOP              = 6
OPENING_MAX_EXPENSIVE_ENTRIES                = 999
OPENING_EXPENSIVE_MAX_PREMIUM_USD            = 220.0

STOP_LOSS_USD          = 30.0
STOP_LOSS_PCT          = 0.05
IMMEDIATE_TAKE_PROFIT_PCT = 0.50
TRADE_STATE_PROTECT_TRIGGER_PCT             = 0.05
TRADE_STATE_PROTECTED_STOP_FLOOR_PCT        = 0.001
TRADE_STATE_BANK_OR_QUALIFY_TRIGGER_PCT     = 0.12
TRADE_STATE_RUNNER_PROMOTION_STOP_FLOOR_PCT = 0.05
RUNNER_DISABLE_AFTER_ET                     = "16:00"
ENABLE_FIXED_PROFIT_TARGET = False
PROFIT_TARGET_PCT          = 0.60
TRAIL_LOCK1_TRIGGER_PCT = 0.12
TRAIL_LOCK1_STOP_PCT    = 0.05
TRAIL_LOCK2_TRIGGER_PCT = 0.25
TRAIL_LOCK2_STOP_PCT    = 0.15
TRAIL_LOCK3_TRIGGER_PCT = 0.40
TRAIL_LOCK3_STOP_PCT    = 0.25
TRAIL_PULLBACK_PCT      = 0.10

ENABLE_REVERSAL_EXIT         = True
REVERSAL_EXIT_MIN_PROFIT_PCT = 0.06
RUNNER_REVERSAL_EXIT_MIN_PROFIT_PCT = 0.08
REVERSAL_ROC_THRESHOLD_PCT   = 0.30
REVERSAL_CONFIRM_SIGNALS     = 2

RVOL_MIN                  = 0.9
OPENING_RVOL_MIN          = 0.35
RVOL_STRICT_UNTIL         = "10:30"
RVOL_RELAX_AFTER          = "10:00"
RVOL_RELAXED_MIN          = 0.7
RVOL_IGNORE_AFTER         = "10:30"
ATR_PCT_MIN               = 1.0
VWAP_NEUTRAL_BAND_PCT     = 0.05
MOVEMENT_FORCE_MIN_PCT    = 0.02
MOVEMENT_WEAK_VWAP_MULT   = 1.00

DIRECTION_CONVICTION_MIN  = 0.45  # require directional consensus before risking premium
DIRECTION_MIN_ALIGNED_VOTES = 3   # require at least N directional votes to agree
DIRECTION_FAST_ROC_PERIOD  = 5    # short-horizon ROC used in directional voting

ROC_PERIOD                = 10
ROC_BULL_MIN              = 0.05
ROC_BEAR_MAX              = -0.05
ENABLE_ROC_FILTER         = True
RSI_EARLY_MIN_PERIOD      = 5
RSI_STRICT_AFTER_TIME     = "10:15"
ENABLE_RSI_FILTER         = True
RSI_CALL_MIN              = 45.0
RSI_CALL_MAX              = 85.0
RSI_PUT_MIN               = 15.0
RSI_PUT_MAX               = 55.0
IV_RANK_MIN               = 20.0
IV_RANK_MAX               = 99.0
ENABLE_SIGNAL_SCORING     = True
MIN_SIGNAL_SCORE          = 5.0   # relaxed floor to increase opportunity flow
VOLATILITY_PRIORITY_WEIGHT = 3.0  # make volatility the top signal driver
TREND_PRIORITY_WEIGHT      = 1.0
FLOW_PRIORITY_WEIGHT       = 1.0
ENTRY_BLOCKED_HOURS_ET    = ()
ENTRY_MAX_QUOTE_SPREAD_PCT         = 5.0
OPENING_ENTRY_MAX_QUOTE_SPREAD_PCT = 6.0
MAX_ENTRY_SLIPPAGE_PCT    = 3.0
MAX_FILL_SLIPPAGE_PCT     = 3.0
REENTRY_COOLDOWN_LOSS_MINUTES      = 20
STOP_LOSS_REENTRY_COOLDOWN_MINUTES = 30
ENABLE_OPENING_ENTRY_RELAX    = False
OPENING_ENTRY_RELAX_MINUTES   = 7
REJECT_COOLDOWN_SHORT_MINUTES  = 3
REJECT_COOLDOWN_MEDIUM_MINUTES = 30

ENABLE_CATALYST_MODE              = False
CATALYST_WINDOW_MINUTES           = 90
CATALYST_INDEX_5M_MOVE_PCT        = 1.2
CATALYST_BREADTH_MOVE_PCT         = 1.0
CATALYST_BREADTH_MIN_COUNT        = 6
CATALYST_RELAXED_RVOL_MIN         = 0.6
CATALYST_DISABLE_RSI              = True
CATALYST_ALLOW_IV_FALLBACK        = True
CATALYST_RELAXED_IV_RANK_MAX      = 90.0
CATALYST_RELAXED_MIN_SIGNAL_SCORE = 2.5
ENABLE_HTF_CONFIRM         = False
HTF_TIMEFRAME              = "15m"
HTF_LOOKBACK_BARS          = 30
ENABLE_ORDER_FLOW_FILTER   = False
MIN_FLOW_SCORE             = 0.05
ENABLE_NEWS_EVENT_BLOCK    = False
NEWS_LOOKBACK_MINUTES      = 90
NEWS_BLOCK_KEYWORDS        = ("earnings", "guidance", "sec", "investigation", "lawsuit", "fda", "downgrade", "upgrade", "cpi", "fomc", "fed")
NEWS_BLOCK_DATES_ET        = ()
ENABLE_HISTORICAL_REGIME_SCORE = False
MIN_HISTORICAL_REGIME_SCORE    = 2.0
ENABLE_INDEX_BIAS_FILTER   = True
INDEX_BIAS_TIMEFRAME       = "5m"
INDEX_BIAS_LOOKBACK        = 30
ENABLE_VIX_GUARD           = False
VIX_MIN                    = 13.0
VIX_MAX                    = 80.0

ENABLE_ENTRY_CONFIRMATION              = True
ENTRY_CONFIRM_BARS                     = 2
ENTRY_CONFIRM_BYPASS_MIN_SIGNAL_SCORE  = 7.2
ENTRY_CONFIRM_MOMENTUM_THRESHOLD_PCT   = 0.08

ENABLE_FAST_START_ENTRY_QUALITY        = True
FAST_START_MIN_SIGNAL_SCORE            = 5.6
FAST_START_MIN_DIRECTION_SCORE         = 0.45
# Runtime fast-start entries relax/ignore this after RVOL_RELAX_AFTER/RVOL_IGNORE_AFTER.
FAST_START_MIN_RVOL                    = 0.75
FAST_START_MIN_ABS_ROC_PCT             = 0.06
FAST_START_MIN_VWAP_DISTANCE_PCT       = 0.04
OPENING_FAST_START_MIN_SIGNAL_SCORE    = 6.2
OPENING_FAST_START_MIN_DIRECTION_SCORE = 0.55
OPENING_FAST_START_MIN_RVOL            = 1.00
OPENING_FAST_START_MIN_ABS_ROC_PCT     = 0.12
OPENING_FAST_START_MIN_VWAP_DISTANCE_PCT = 0.08

MIN_OPTION_OPEN_INTEREST          = 10
MIN_OPTION_DAILY_VOLUME           = 3
MAX_OPTION_SPREAD_PCT             = 8.0
ENABLE_OPTION_LIQUIDITY_RELAX     = True
OPTION_CONTRACTS_ALLOW_LIVE_FALLBACK = False
MIN_DTE_TRADING_DAYS              = 0
MAX_DTE_TRADING_DAYS              = 5
MIN_OPTION_OPEN_INTEREST_0DTE     = 25
ENABLE_DELTA_TARGETING            = True
TARGET_DELTA_MIN                  = 0.40
TARGET_DELTA_MAX                  = 0.55
TARGET_DELTA_FALLBACK             = 0.50
EMERGENCY_EXECUTION_MODE          = False
ALLOW_MARKET_ENTRY_WITHOUT_QUOTE  = False

PDT_MIN_EQUITY         = 25000.0
PDT_MAX_DAY_TRADES_5D  = 3
ENFORCE_PDT_GUARD      = False
ENABLE_EARNINGS_GUARD = True
# Block report-risk names before and on the earnings date. The next day is allowed
# again so the bot can trade post-earnings momentum after the outcome is known.
EARNINGS_LOOKAHEAD_DAYS  = 2
EARNINGS_CHECK_STRICT    = False
EARNINGS_SKIP_SYMBOLS    = ("SPY", "QQQ", "IWM", "DIA", "VIX", "^VIX")
MAX_ENTRIES_PER_TICKER_PER_DAY = 999
MAX_REENTRIES_PER_TICKER = 999
QUICK_LOSER_MAX_HOLD_MINUTES         = 4
QUICK_LOSER_REENTRY_COOLDOWN_MINUTES = 45
ENABLE_STOPLOSS_REVERSAL_REENTRY = False

EASTERN_TZ                         = "US/Eastern"
CENTRAL_TZ                         = "US/Central"
RATE_LIMIT_SLEEP_SECONDS           = 0.3
CLOSED_MIN_SLEEP_SECONDS           = 60
CLOSED_MAX_SLEEP_SECONDS           = 900
MANUAL_PAUSE_SLEEP_SECONDS         = 30
HEARTBEAT_SECONDS                  = 300
ALERT_COOLDOWN_SECONDS             = 300
ENTRY_ORDER_STATUS_WAIT_SECONDS    = 8
ENTRY_RETRY_STATUS_WAIT_SECONDS    = 5
ENTRY_MARKET_FALLBACK_WAIT_SECONDS = 3
ENTRY_RETRY_LIMIT_PCT              = 0.02
EXIT_ORDER_STATUS_POLL_SECONDS     = 2
EXIT_ORDER_MAX_WAIT_SECONDS        = 20
EXIT_CLOSE_RETRY_ATTEMPTS          = 2
SMART_EXIT_NORMAL_WAIT_SECONDS     = 6
SMART_EXIT_CRITICAL_WAIT_SECONDS   = 3
SMART_EXIT_NORMAL_REPRICE_PCT      = 0.35
SMART_EXIT_CRITICAL_REPRICE_PCT    = 0.10
STOPLOSS_EXIT_ORDER_STATUS_POLL_SECONDS = 1
STOPLOSS_EXIT_ORDER_MAX_WAIT_SECONDS    = 3
STOPLOSS_EXIT_CLOSE_RETRY_ATTEMPTS      = 1
INDEPENDENT_STOPLOSS_INTERVAL_SECONDS    = 2
INDEPENDENT_STOPLOSS_REQUIRE_STALE_LOOP  = False
PAPER_EXECUTION_FRICTION_PER_CONTRACT    = 1.0
TRADES_MAX_ROWS                    = 5000
PAPER = True

FEATURE_SESSION_GUARDRAIL_PANEL      = False
FEATURE_TRADE_REPLAY                 = False
FEATURE_PREMARKET_OPENING_PLAN_CARD  = False
FEATURE_EXIT_RELIABILITY_METRICS     = False
FEATURE_DRY_RUN_MODE                 = False
FEATURE_SMART_ALERTS                 = False
FEATURE_TICKER_SCORECARDS            = False
FEATURE_STRATEGY_PROFILES            = False
FEATURE_BAD_FILL_DETECTOR            = False
FEATURE_WEEKLY_REVIEW_GENERATOR      = False
AUTO_RESUME_TRADING_ON_BOOT          = False

TRADES_CSV_PATH          = _DATA_DIR / "trades.csv"
STATE_JSON_PATH          = _DATA_DIR / "runtime_state.json"
SCAN_LOG_CSV_PATH        = _DATA_DIR / "scan_log.csv"
OBSERVATION_LOG_CSV_PATH = _DATA_DIR / "observation_log.csv"
TRADING_CONTROL_PATH     = _DATA_DIR / "trading_control.json"
WATCHLIST_CONTROL_PATH   = _DATA_DIR / "watchlist_control.json"

ALPACA_PAPER_BASE_URL = "https://paper-api.alpaca.markets"
ALPACA_DATA_BASE_URL  = "https://data.alpaca.markets"
DISCORD_WEBHOOK_URL   = os.getenv("DISCORD_WEBHOOK_URL", "")
ALERT_WEBHOOK_URL     = os.getenv("ALERT_WEBHOOK_URL", "")
DASHBOARD_CONTROL_TOKEN = os.getenv("DASHBOARD_CONTROL_TOKEN", "")
ALPACA_LIVE_API_KEY   = os.getenv("ALPACA_LIVE_API_KEY", "")
ALPACA_LIVE_SECRET_KEY = os.getenv("ALPACA_LIVE_SECRET_KEY", "")
