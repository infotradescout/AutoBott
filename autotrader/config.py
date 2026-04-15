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


# ---------------------------------------------------------------------------
# Watchlist / universe
# ---------------------------------------------------------------------------

TICKERS = [
    "SPY", "QQQ", "IWM", "DIA",
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL",
    "TSLA", "AMD", "NFLX", "CRM", "INTC",
]

CORE_TICKERS = [
    "SPY", "QQQ", "IWM", "DIA",
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL",
    "TSLA", "AMD", "NFLX", "CRM", "INTC",
    "AVGO", "ADBE", "ORCL", "JPM", "XOM",
]

AUTO_EXPAND_UNIVERSE_WITH_MOVERS = True
UNIVERSE_MOVER_TOP                = 50
UNIVERSE_MAX_TICKERS              = 300
SCREENER_TOP_N                    = 20
MOVER_SYMBOLS_PER_SIDE            = 10
MIN_SHARE_PRICE                   = 10
MAX_SHARE_PRICE                   = 2000


# ---------------------------------------------------------------------------
# Bar / data settings
# ---------------------------------------------------------------------------

BAR_TIMEFRAME       = "5Min"
SIGNAL_LOOKBACK     = 20
SCAN_INTRADAY_BARS  = 60
SCAN_MIN_BARS       = 5
SCAN_DAILY_BARS     = 30


# ---------------------------------------------------------------------------
# Position sizing & risk
# ---------------------------------------------------------------------------

MAX_POSITIONS                       = 25    # hard cap for concurrent option positions
POSITION_SIZE_USD                   = 500
RISK_PER_TRADE_PCT                  = 0.01
MAX_POSITION_SIZE_USD               = 700.0
DRAWDOWN_REDUCE_AFTER_CONSEC_LOSSES = 2
DRAWDOWN_SIZE_MULTIPLIER            = 0.5
DAILY_LOSS_LIMIT_USD                = 150.0
WEEKLY_LOSS_LIMIT_USD               = 900.0
CONSECUTIVE_LOSS_LIMIT              = 3


# ---------------------------------------------------------------------------
# Timing & session windows
# ---------------------------------------------------------------------------

MARKET_OPEN                        = "09:30"
PREOPEN_READY_MINUTES              = 10
HARD_CLOSE_TIME                    = "15:30"   # force-close all positions at this time
OPTION_EXPIRY_EXIT_TIME            = "15:00"   # exit expiring contracts by this time
OPTION_FORCE_EXIT_DAYS_BEFORE_EXPIRY = 1
NO_NEW_TRADES_BEFORE               = "09:30"
NO_NEW_TRADES_AFTER                = "15:00"   # stop new entries 30 min before close
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

# Poll every 15 seconds for fast exit response on scalp trades
LOOP_INTERVAL_SECONDS              = 15

# Allow trades to run up to 90 min — trailing stop exits winners well before this
MAX_HOLD_MINUTES                   = 90

# Anti-churn entry hold: prevent discretionary exits (reversal, immediate take-profit)
# during first N minutes after entry. Stop loss still fires immediately.
# Reduces round-trip losses from early noise; lets winners establish momentum.
ANTI_CHURN_HOLD_MINUTES            = 3


# ---------------------------------------------------------------------------
# Stop loss & trailing exit ladder
# ---------------------------------------------------------------------------

# Slightly wider stop to reduce chop noise exits.
STOP_LOSS_USD          = 12.0   # exit if unrealized P&L is <= -$12 per trade
STOP_LOSS_PCT          = 0.03   # legacy fallback reference for older state/debug fields

# Immediate winner lock: exit as soon as gain reaches this fraction.
# Small-win mode: 0.05 = +5% (bank small frequent wins).
IMMEDIATE_TAKE_PROFIT_PCT = 0.05

# Fixed profit target (disabled — trailing stop rides winners instead)
ENABLE_FIXED_PROFIT_TARGET = False
PROFIT_TARGET_PCT          = 0.60   # only used if ENABLE_FIXED_PROFIT_TARGET = True

# Trailing stop ratchet (stop floor only moves UP, never down):
#   Entry → +8%   : floor = -3%   (cut fast if wrong direction)
#   +8%   → +20%  : floor = +3%   (locked in a small win)
#   +20%  → +35%  : floor = +10%  (locked in a solid gain)
#   +35%+ (deep)  : floor = peak − 6%  (dynamic trail, ride momentum)
TRAIL_LOCK1_TRIGGER_PCT = 0.08
TRAIL_LOCK1_STOP_PCT    = 0.03
TRAIL_LOCK2_TRIGGER_PCT = 0.20
TRAIL_LOCK2_STOP_PCT    = 0.10
TRAIL_LOCK3_TRIGGER_PCT = 0.35
TRAIL_LOCK3_STOP_PCT    = 0.20
TRAIL_PULLBACK_PCT      = 0.06   # trail 6% below peak when deep in profit


# ---------------------------------------------------------------------------
# Reversal detection exit
# ---------------------------------------------------------------------------
# When a trade is in profit, check live bars for confirmed momentum reversal.
# Three signals are evaluated each loop:
#   1. EMA9 crosses against trade direction
#   2. Last 2 bars moved >= REVERSAL_ROC_THRESHOLD_PCT against trade direction
#   3. Price crossed back through VWAP
# Requires REVERSAL_CONFIRM_SIGNALS of 3 to fire an exit.

ENABLE_REVERSAL_EXIT         = True
REVERSAL_EXIT_MIN_PROFIT_PCT = 0.02   # check for reversal once trade is up 2%
REVERSAL_ROC_THRESHOLD_PCT   = 0.30   # 0.3% move in 2 bars counts as reversal signal
REVERSAL_CONFIRM_SIGNALS     = 2      # require 2 of 3 signals to confirm reversal


# ---------------------------------------------------------------------------
# Scanner thresholds
# ---------------------------------------------------------------------------

RVOL_MIN                  = 0.9
OPENING_RVOL_MIN          = 0.35
RVOL_STRICT_UNTIL         = "10:30"
RVOL_RELAX_AFTER          = "10:00"
RVOL_RELAXED_MIN          = 0.7
RVOL_IGNORE_AFTER         = "10:30"
ATR_PCT_MIN               = 1.0   # lowered from 1.8 — ETFs like SPY/QQQ have lower ATR
VWAP_NEUTRAL_BAND_PCT     = 0.05  # tightened from 0.15 — 0.15% was rejecting stocks barely off VWAP
MOVEMENT_FORCE_MIN_PCT    = 0.02  # was 0.03 (scanner default); allow borderline tape to be evaluated
MOVEMENT_WEAK_VWAP_MULT   = 1.00  # was effectively 1.5 in scanner; only block when very close to VWAP

# Direction conviction: minimum weighted-vote score to commit to call/put.
# 0.0 = any majority; 0.5 = strongly one-sided required.
DIRECTION_CONVICTION_MIN  = 0.1   # lowered from 0.2 — allows more split-but-leaning signals

ROC_PERIOD                = 10
ROC_BULL_MIN              = 0.05  # lowered from 0.12 — weak momentum is still momentum
ROC_BEAR_MAX              = -0.05 # loosened from -0.12
ENABLE_ROC_FILTER         = True

RSI_EARLY_MIN_PERIOD      = 5
RSI_STRICT_AFTER_TIME     = "10:15"
ENABLE_RSI_FILTER         = True
RSI_CALL_MIN              = 45.0  # widened from 52 — allow calls when RSI is neutral-to-bullish
RSI_CALL_MAX              = 85.0  # widened from 75 — don't block strong momentum
RSI_PUT_MIN               = 15.0  # widened from 25
RSI_PUT_MAX               = 55.0  # widened from 48 — allow puts when RSI is neutral-to-bearish

IV_RANK_MIN               = 20.0
IV_RANK_MAX               = 99.0

ENABLE_SIGNAL_SCORING     = True
MIN_SIGNAL_SCORE          = 4.2   # was 5.0; keeps quality gate while admitting near-threshold core moves

# Phase 3 enforcement knobs driven by review.py output.
# Use blocked hours after you identify weak entry windows from analytics.
ENTRY_BLOCKED_HOURS_ET    = ()

# Execution-time spread gate using the live quote right before order submission.
# Keep this tighter than MAX_OPTION_SPREAD_PCT, which is only used during chain selection.
ENTRY_MAX_QUOTE_SPREAD_PCT         = 18.0
OPENING_ENTRY_MAX_QUOTE_SPREAD_PCT = 24.0

MAX_ENTRY_SLIPPAGE_PCT    = 5.0
MAX_FILL_SLIPPAGE_PCT     = 5.0

# Churn control: block immediate re-entry on a ticker after a losing exit.
REENTRY_COOLDOWN_LOSS_MINUTES      = 20
STOP_LOSS_REENTRY_COOLDOWN_MINUTES = 30

ENABLE_OPENING_ENTRY_RELAX    = True
OPENING_ENTRY_RELAX_MINUTES   = 7

# Reject cooldowns (scanner control flow)
REJECT_COOLDOWN_SHORT_MINUTES  = 3   # transient data issues: 1-5m range enforced in scanner
REJECT_COOLDOWN_MEDIUM_MINUTES = 30  # tradability/chain issues: 15-60m range enforced in scanner


# ---------------------------------------------------------------------------
# Catalyst mode
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Optional filters (all off by default for simple call/put scalping)
# ---------------------------------------------------------------------------

ENABLE_HTF_CONFIRM         = False
HTF_TIMEFRAME              = "15m"
HTF_LOOKBACK_BARS          = 30

ENABLE_ORDER_FLOW_FILTER   = False
MIN_FLOW_SCORE             = 0.05

ENABLE_NEWS_EVENT_BLOCK    = False
NEWS_LOOKBACK_MINUTES      = 90
NEWS_BLOCK_KEYWORDS        = (
    "earnings", "guidance", "sec", "investigation", "lawsuit",
    "fda", "downgrade", "upgrade", "cpi", "fomc", "fed",
)
NEWS_BLOCK_DATES_ET        = ()

ENABLE_HISTORICAL_REGIME_SCORE = False
MIN_HISTORICAL_REGIME_SCORE    = 2.0

ENABLE_INDEX_BIAS_FILTER   = False
INDEX_BIAS_TIMEFRAME       = "5m"
INDEX_BIAS_LOOKBACK        = 30

ENABLE_VIX_GUARD           = False
VIX_MIN                    = 13.0
VIX_MAX                    = 80.0


# ---------------------------------------------------------------------------
# Entry confirmation
# ---------------------------------------------------------------------------

ENABLE_ENTRY_CONFIRMATION              = True
ENTRY_CONFIRM_BARS                     = 2
ENTRY_CONFIRM_BYPASS_MIN_SIGNAL_SCORE  = 7.5


# ---------------------------------------------------------------------------
# Options contract selection
# ---------------------------------------------------------------------------

MIN_OPTION_OPEN_INTEREST          = 10   # lowered from 25 — TSLA/MSFT were at 24/25 OI
MIN_OPTION_DAILY_VOLUME           = 3    # lowered from 5
MAX_OPTION_SPREAD_PCT             = 30.0
ENABLE_OPTION_LIQUIDITY_RELAX     = True
OPTION_CONTRACTS_ALLOW_LIVE_FALLBACK = False
MIN_DTE_TRADING_DAYS              = 1
MAX_DTE_TRADING_DAYS              = 5    # widened from 2 — on Mondays next expiry is Friday (4 days)
MIN_OPTION_OPEN_INTEREST_0DTE     = 25   # lowered from 50
ENABLE_DELTA_TARGETING            = True
TARGET_DELTA_MIN                  = 0.40
TARGET_DELTA_MAX                  = 0.55
TARGET_DELTA_FALLBACK             = 0.50
EMERGENCY_EXECUTION_MODE          = False
ALLOW_MARKET_ENTRY_WITHOUT_QUOTE  = False


# ---------------------------------------------------------------------------
# PDT guard
# ---------------------------------------------------------------------------

PDT_MIN_EQUITY         = 25000.0
PDT_MAX_DAY_TRADES_5D  = 3
ENFORCE_PDT_GUARD      = False


# ---------------------------------------------------------------------------
# Earnings filter
# ---------------------------------------------------------------------------

EARNINGS_LOOKAHEAD_DAYS  = 2
EARNINGS_CHECK_STRICT    = False
EARNINGS_SKIP_SYMBOLS    = ("SPY", "QQQ", "IWM", "DIA", "VIX", "^VIX")


# ---------------------------------------------------------------------------
# Re-entry
# ---------------------------------------------------------------------------

MAX_ENTRIES_PER_TICKER_PER_DAY = 3
MAX_REENTRIES_PER_TICKER = 1


# ---------------------------------------------------------------------------
# Operational / timing constants
# ---------------------------------------------------------------------------

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
# Faster close profile for hard stop-loss exits.
STOPLOSS_EXIT_ORDER_STATUS_POLL_SECONDS = 1
STOPLOSS_EXIT_ORDER_MAX_WAIT_SECONDS    = 3
STOPLOSS_EXIT_CLOSE_RETRY_ATTEMPTS      = 1
# Independent stop-loss watchdog runs even when trader loop is healthy.
INDEPENDENT_STOPLOSS_INTERVAL_SECONDS    = 2
INDEPENDENT_STOPLOSS_REQUIRE_STALE_LOOP  = False
PAPER_EXECUTION_FRICTION_PER_CONTRACT    = 1.0
TRADES_MAX_ROWS                    = 5000

PAPER = True   # paper trading — set to False only when ready for live


# ---------------------------------------------------------------------------
# Feature flags (safe rollout: all default OFF)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# File paths
# ---------------------------------------------------------------------------

TRADES_CSV_PATH          = _DATA_DIR / "trades.csv"
STATE_JSON_PATH          = _DATA_DIR / "runtime_state.json"
SCAN_LOG_CSV_PATH        = _DATA_DIR / "scan_log.csv"
OBSERVATION_LOG_CSV_PATH = _DATA_DIR / "observation_log.csv"
TRADING_CONTROL_PATH     = _DATA_DIR / "trading_control.json"
WATCHLIST_CONTROL_PATH   = _DATA_DIR / "watchlist_control.json"


# ---------------------------------------------------------------------------
# Alpaca endpoints
# ---------------------------------------------------------------------------

ALPACA_PAPER_BASE_URL = "https://paper-api.alpaca.markets"
ALPACA_DATA_BASE_URL  = "https://data.alpaca.markets"


# ---------------------------------------------------------------------------
# Secrets — must stay as env vars (never hardcode API keys in source)
# ---------------------------------------------------------------------------

DISCORD_WEBHOOK_URL   = os.getenv("DISCORD_WEBHOOK_URL", "")
ALERT_WEBHOOK_URL     = os.getenv("ALERT_WEBHOOK_URL", "")
DASHBOARD_CONTROL_TOKEN = os.getenv("DASHBOARD_CONTROL_TOKEN", "")

# Live-account keys used ONLY for options contract lookups in paper mode.
# Alpaca's live endpoint rejects paper keys with 401.
ALPACA_LIVE_API_KEY   = os.getenv("ALPACA_LIVE_API_KEY", "")
ALPACA_LIVE_SECRET_KEY = os.getenv("ALPACA_LIVE_SECRET_KEY", "")
