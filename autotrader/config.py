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
    if env_path:
        env_candidate = Path(env_path)
        if _is_writable_dir(env_candidate):
            return env_candidate
        print(f"[config] DATA_DIR '{env_path}' not writable. Falling back to defaults.")

    candidates: list[Path] = []
    # Render persistent disk default mount path (only when mounted/present).
    render_data_mount = Path("/data")
    if os.name != "nt" and render_data_mount.exists() and render_data_mount.is_dir():
        candidates.append(render_data_mount)
    candidates.append(_DEFAULT_DATA_DIR)
    candidates.append(Path("/tmp/autotrader-data"))

    for candidate in candidates:
        if _is_writable_dir(candidate):
            return candidate
    cwd = Path.cwd()
    cwd.mkdir(parents=True, exist_ok=True)
    print(f"[config] No writable data directory found. Using '{cwd}'.")
    return cwd


_DEFAULT_DATA_DIR = Path(__file__).resolve().parent
_DATA_DIR = _resolve_data_dir()
DATA_DIR = _DATA_DIR


# ---------------------------------------------------------------------------
# Watchlist / universe
# ---------------------------------------------------------------------------

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

CORE_TICKERS = [
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

AUTO_EXPAND_UNIVERSE_WITH_MOVERS = True
UNIVERSE_MOVER_TOP                = 300
UNIVERSE_MAX_TICKERS              = 500  # broad exchange-wide mover sweep
STRICT_TOP_VOLUME_UNIVERSE        = False
TOP_VOLUME_UNIVERSE_SIZE          = 10
SCREENER_TOP_N                    = 300
MOVER_SYMBOLS_PER_SIDE            = 150
MIN_MOVER_CANDIDATES_BEFORE_BASE_FALLBACK = 40
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

# Risk limits scaled for larger paper balances to avoid premature day/week lockouts.
MAX_POSITIONS                       = 3
POSITION_SIZE_USD                   = 125   # baseline premium budget per trade
RISK_PER_TRADE_PCT                  = 0.017
MAX_POSITION_SIZE_USD               = 150.0  # per-trade premium cap
DRAWDOWN_REDUCE_AFTER_CONSEC_LOSSES = 2
DRAWDOWN_SIZE_MULTIPLIER            = 0.75
DAILY_LOSS_LIMIT_USD                = 450.0
WEEKLY_LOSS_LIMIT_USD               = 1500.0
CONSECUTIVE_LOSS_LIMIT              = 3
# Net P&L circuit breaker (runtime telemetry-based):
# Pause new entries once the day is sufficiently red in realized net P&L.
INTRADAY_NET_LOSS_LIMIT_USD         = 350.0
# Early-red guard: stop new entries if still net red after first few trades.
EARLY_RED_GUARD_ENABLED             = True
EARLY_RED_GUARD_MIN_CLOSED_TRADES   = 3
EARLY_RED_GUARD_MAX_NET_PNL_USD     = -150.0

# Loss throttle: after 2 consecutive losses, require stronger setups.
LOSS_THROTTLE_AFTER_CONSEC_LOSSES   = 2
LOSS_THROTTLE_SIGNAL_SCORE_ADD      = 1.0   # require score 6.5+ after 2 losses
LOSS_THROTTLE_MIN_VOLATILITY_SCORE  = 1.5  # after 2 losses require volatility_score >= 1.5 (low bar but not zero)

# Capital doctrine: $150 max per trade, $450 max total open at once (3 positions × $150).
MAX_PREMIUM_PER_TRADE_USD           = 150.0  # keep single-name premium bounded
MAX_TOTAL_OPEN_PREMIUM_USD          = 1500.0  # scaled for $6k account
OPENING_MAX_FRESH_PREMIUM_USD       = 250.0  # opening-session budget
MAX_CONTRACTS_PER_TRADE             = 4
MAX_SAME_DIRECTION_POSITIONS        = 10

# Premium-cap quality override (legacy) is permanently removed; trades that
# exceed MAX_PREMIUM_PER_TRADE_USD are always rejected.

# Expensive names are allowed only when premium stays inside per-trade budget;
# opening window allows at most one expensive-name fresh entry.
EXPENSIVE_PREMIUM_SYMBOLS = (
    "TSLA", "MSFT", "AVGO", "NFLX", "META", "GOOGL", "AMZN",
)

# Preferred core names for small-account behavior.
# Leave empty to disable core-vs-non-core entry bias and allow exchange-wide movers.
PREFERRED_CORE_TICKERS = ()
MAX_NON_CORE_ENTRIES_PER_DAY        = 9999
NON_CORE_MIN_SIGNAL_SCORE           = 0.0

# Volatility-adaptive risk sizing:
# Uses scanner metrics (ATR%, RVOL, IV Rank) to classify each setup as
# normal/high/extreme volatility and auto-adjust stop and premium exposure.
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


# ---------------------------------------------------------------------------
# Timing & session windows
# ---------------------------------------------------------------------------

MARKET_OPEN                        = "09:30"
PREOPEN_READY_MINUTES              = 10
HARD_CLOSE_TIME                    = "16:00"   # force-close all positions at market close
OPTION_EXPIRY_EXIT_TIME            = "15:45"   # exit expiring 0-2DTE contracts 15 min before close
OPTION_FORCE_EXIT_DAYS_BEFORE_EXPIRY = 0    # exit on expiry day itself (intraday)
NO_NEW_TRADES_BEFORE               = "09:30"
NO_NEW_TRADES_AFTER                = "16:00"   # full regular-session entry window
SCAN_MORNING_TIME                  = "09:30"
OBSERVATION_END_TIME               = "10:00"
OBSERVATION_ENABLED                = False  # disabled: skip observation window, trade from 09:30
ENABLE_PREMARKET_OPENING_SIGNALS   = True
PREMARKET_SIGNAL_WINDOW_START      = "08:00"
PREMARKET_SIGNAL_WINDOW_END        = "09:30"
PREMARKET_REPORT_READY_TIME        = "08:20"
PREMARKET_LOOKBACK_MINUTES         = 75
PREMARKET_MAX_SIGNALS              = 6
PREMARKET_APPLY_UNTIL              = "09:40"
PREMARKET_SCAN_INTERVAL_SECONDS    = 120
PREMARKET_SCAN_MAX_RUNS            = 0

# Poll every 10 seconds for faster scalp exit response.
LOOP_INTERVAL_SECONDS              = 10

# Allow trades to run up to 90 min — trailing stop exits winners well before this
MAX_HOLD_MINUTES                   = 45    # rotate capital sooner when trades fail to prove out

# Anti-churn entry hold: prevent discretionary exits (reversal, immediate take-profit)
# during first N minutes after entry. Stop loss still fires immediately.
# Reduces round-trip losses from early noise; lets winners establish momentum.
ANTI_CHURN_HOLD_MINUTES            = 0

# Opening strict mode (09:30+N minutes): trade fewer, stronger setups only.
OPENING_STRICT_WINDOW_MINUTES                = 20
OPENING_STRICT_MIN_SIGNAL_SCORE              = 6.5
OPENING_STRICT_CONFIRM_BARS                  = 3
OPENING_STRICT_CONFIRM_MOMENTUM_THRESHOLD_PCT = 0.22
OPENING_STRICT_MIN_DIRECTION_SCORE           = 0.65
OPENING_STRICT_MIN_RVOL                      = 1.0
OPENING_STRICT_MIN_ROC_PCT                   = 0.05
OPENING_STRICT_MIN_VWAP_DISTANCE_PCT         = 0.03
OPENING_MAX_SIGNAL_CANDIDATES                = 5
OPENING_MAX_FRESH_ENTRIES                    = 4
OPENING_MAX_CONCURRENT_POSITIONS             = 2
OPENING_MAX_NEW_ENTRY_ATTEMPTS_PER_LOOP      = 1
MAX_NEW_ENTRY_ATTEMPTS_PER_LOOP              = 1
OPENING_MAX_EXPENSIVE_ENTRIES                = 0    # no expensive entries in opening window on $6k account
OPENING_EXPENSIVE_MAX_PREMIUM_USD            = 100.0  # same cap as regular trades


# ---------------------------------------------------------------------------
# Stop loss & trailing exit ladder
# ---------------------------------------------------------------------------

# Stop loss baseline scaled up for larger-account operation while preserving fast exits.
STOP_LOSS_USD          = 45.0
STOP_LOSS_PCT          = 0.35   # 35% hard stop

# Legacy immediate TP knob retained for backward compatibility only.
# Stateful manager (protect -> bank/qualify -> runner) is now primary.
IMMEDIATE_TAKE_PROFIT_PCT = 0.50

# Explicit stateful winner-management transitions.
# Raised thresholds: spread noise is 8-15%, so protect only triggers on real gains.
TRADE_STATE_PROTECT_TRIGGER_PCT             = 0.20   # +20%: move to protected (above spread noise)
TRADE_STATE_PROTECTED_STOP_FLOOR_PCT        = 0.05   # +5% floor once protected (lock in real gain)
TRADE_STATE_BANK_OR_QUALIFY_TRIGGER_PCT     = 0.40   # +40%: bank-or-qualify decision
TRADE_STATE_RUNNER_PROMOTION_STOP_FLOOR_PCT = 0.20   # +20% floor when promoted to runner
RUNNER_DISABLE_AFTER_ET                     = "16:00"
RUNNER_AUTO_PROMOTE_MIN_PCT                 = 0.18
ENABLE_RUNNER_MODE                          = True
RUNNER_PROMOTE_PCT                          = 0.35
RUNNER_PROTECT_FLOOR_PCT                    = 0.20
RUNNER_MAX_CONCURRENT                       = 1
NON_RUNNER_FLATTEN_TIME                     = "15:15"

# Basket-level management: when open PnL is meaningfully green, harvest winners
# and cut laggards instead of waiting for each position independently.
ENABLE_BASKET_MANAGER                       = True
BASKET_HARVEST_UNREALIZED_USD               = 40.0
BASKET_LAGGARD_CUT_PCT                      = -0.12
BASKET_WINNER_HARVEST_MIN_PCT               = 0.25
BASKET_WINNER_LOCK_FLOOR_PCT                = 0.05

# Fixed profit target (disabled — trailing stop rides winners instead)
ENABLE_FIXED_PROFIT_TARGET = False
PROFIT_TARGET_PCT          = 0.60   # only used if ENABLE_FIXED_PROFIT_TARGET = True

# Trailing stop ratchet (stop floor only moves UP, never down):
#   Entry → +25%  : hard stop at -25% (survive spread noise)
#   +25%  → +50%  : floor = +10%  (locked in a real gain)
#   +50%  → +80%  : floor = +30%  (locked in a strong gain)
#   +80%+ (deep)  : floor = peak − 15% (dynamic trail, ride momentum)
TRAIL_LOCK1_TRIGGER_PCT = 0.25
TRAIL_LOCK1_STOP_PCT    = 0.10
TRAIL_LOCK2_TRIGGER_PCT = 0.50
TRAIL_LOCK2_STOP_PCT    = 0.30
TRAIL_LOCK3_TRIGGER_PCT = 0.80
TRAIL_LOCK3_STOP_PCT    = 0.50
TRAIL_PULLBACK_PCT      = 0.15   # trail 15% below peak when deep in profit (let winners run)


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
REVERSAL_EXIT_MIN_PROFIT_PCT = 0.06   # protected-state reversal starts at +6%
RUNNER_REVERSAL_EXIT_MIN_PROFIT_PCT = 0.08
REVERSAL_ROC_THRESHOLD_PCT   = 0.30   # 0.3% move in 2 bars counts as reversal signal
REVERSAL_CONFIRM_SIGNALS     = 2      # require 2 of 3 signals to confirm reversal

# Option-behavior exits (post-entry doctrine):
# underlying sets bias; option premium behavior decides if trade still has edge.
ENABLE_OPTION_BEHAVIOR_EXIT                 = True
OPTION_BEHAVIOR_MIN_HOLD_MINUTES            = 3    # ignore micro-noise in first few minutes
OPTION_BEHAVIOR_NO_PROGRESS_MINUTES         = 5    # tighter no-progress clock for weak/choppy tape
OPTION_BEHAVIOR_MIN_PROGRESS_PLPC           = 0.03 # require at least +3% premium progress
OPTION_BEHAVIOR_MOMENTUM_LOOKBACK_CHECKS    = 4    # loop-history bars for momentum decay check
OPTION_BEHAVIOR_MOMENTUM_MIN_DELTA_PLPC     = -0.01 # if premium trend decays >1% over lookback, exit
OPTION_BEHAVIOR_PEAK_PULLBACK_TRIGGER_PLPC  = 0.08 # once +8% achieved, enforce pullback discipline
OPTION_BEHAVIOR_PEAK_PULLBACK_EXIT_PCT      = 0.30 # exit if giving back >=30% of peak premium gain
OPTION_BEHAVIOR_MAX_SPREAD_PCT              = 25.0 # spread blowout threshold during management
OPTION_BEHAVIOR_SPREAD_GRACE_MINUTES        = 5    # wait a bit before spread-based exits


# ---------------------------------------------------------------------------
# Scanner thresholds
# ---------------------------------------------------------------------------

RVOL_MIN                  = 1.0
OPENING_RVOL_MIN          = 1.0
RVOL_STRICT_UNTIL         = "10:30"
RVOL_RELAX_AFTER          = "10:00"
RVOL_RELAXED_MIN          = 1.0
RVOL_IGNORE_AFTER         = "16:00"  # CRITICAL FIX: was 10:30 — never fully disable RVOL gate
ATR_PCT_MIN               = 0.3   # very low ATR floor — don't filter out ETFs
VWAP_NEUTRAL_BAND_PCT     = 0.15  # wider neutral band: within 0.15% of VWAP = neutral, halve VWAP vote weight
MOVEMENT_FORCE_MIN_PCT    = 0.014  # further relaxed for early-session/transition tape
MOVEMENT_WEAK_VWAP_MULT   = 1.00  # was effectively 1.5 in scanner; only block when very close to VWAP
DIRECTION_PRICE_MIN_PCT   = 0.004  # slightly more permissive to avoid starving entries
DIRECTION_CONFLICT_HARD_REJECT = True
DIRECTION_CONFLICT_SCORE_MULT  = 0.55  # kept for telemetry; hard reject now blocks conflicts
DIRECTION_CONFLICT_ROC_MIN_PCT = 0.008
ROC_ACTIVE_MOVE_MIN_PCT   = 0.006
DIRECTION_CONFIRM_LOOKBACK_BARS = 2
DIRECTION_MIN_CONFIRM_ALIGNED_BARS = 2

# Direction conviction: minimum weighted-vote score to commit to call/put.
DIRECTION_CONVICTION_MIN  = 0.65
DIRECTION_MIN_ALIGNED_VOTES = 4
DIRECTION_FAST_ROC_PERIOD  = 5    # short-horizon ROC used in directional voting

ROC_PERIOD                = 10
ROC_BULL_MIN              = 0.05  # lowered from 0.12 — weak momentum is still momentum
ROC_BEAR_MAX              = -0.05 # loosened from -0.12
ENABLE_ROC_FILTER         = False

RSI_EARLY_MIN_PERIOD      = 5
RSI_STRICT_AFTER_TIME     = "10:15"
ENABLE_RSI_FILTER         = False
RSI_CALL_MIN              = 40.0  # allow continuation entries before RSI fully expands
RSI_CALL_MAX              = 95.0  # avoid rejecting trend continuation in high momentum tape
RSI_PUT_MIN               = 5.0   # allow deeper downside continuation setups
RSI_PUT_MAX               = 60.0  # widen bearish acceptance around transition zones

IV_RANK_MIN               = 0.0   # no minimum IV rank — trade any setup
IV_RANK_MAX               = 99.0

ENABLE_SIGNAL_SCORING     = True
MIN_SIGNAL_SCORE          = 6.5
VOLATILITY_PRIORITY_WEIGHT = 3.0  # make volatility the top signal driver
TREND_PRIORITY_WEIGHT      = 1.0
FLOW_PRIORITY_WEIGHT       = 1.0

# Phase 3 enforcement knobs driven by review.py output.
# Use blocked hours after you identify weak entry windows from analytics.
ENTRY_BLOCKED_HOURS_ET    = ()

# Execution-time spread gate using the live quote right before order submission.
# Keep this tighter than MAX_OPTION_SPREAD_PCT, which is only used during chain selection.
ENTRY_MAX_QUOTE_SPREAD_PCT         = 8.0   # stricter fill-quality gate
OPENING_ENTRY_MAX_QUOTE_SPREAD_PCT = 9.0

MAX_ENTRY_SLIPPAGE_PCT    = 3.0
MAX_FILL_SLIPPAGE_PCT     = 3.0

# Churn control: block immediate re-entry on a ticker after a losing exit.
REENTRY_COOLDOWN_LOSS_MINUTES      = 5    # short cooldown after loss
STOP_LOSS_REENTRY_COOLDOWN_MINUTES = 5    # short cooldown after stop-loss hit

ENABLE_OPENING_ENTRY_RELAX    = False
OPENING_ENTRY_RELAX_MINUTES   = 7

# Reject cooldowns (scanner control flow)
REJECT_COOLDOWN_SHORT_MINUTES  = 2   # transient data issues: keep scanner responsive
REJECT_COOLDOWN_MEDIUM_MINUTES = 15  # shorter tradability cooldown to avoid starving symbols
REJECT_COOLDOWN_EVENT_MINUTES  = 8   # shorter event cooldown for intraday rotation

# Chop/no-trade regime guard (reduce force-trading in dead tape).
ENABLE_CHOP_NO_TRADE_FILTER                   = False
CHOP_NO_TRADE_MIN_SIGNAL_SAMPLE               = 8
CHOP_NO_TRADE_WEAK_SHARE_TRIGGER              = 0.65
CHOP_NO_TRADE_MAX_RVOL                        = 0.35
CHOP_NO_TRADE_MAX_ABS_ROC_PCT                 = 0.12
CHOP_NO_TRADE_MAX_ABS_DIRECTION_SCORE         = 0.30
CHOP_NO_TRADE_REQUIRE_NEUTRAL_INDEX_BIAS      = True
CHOP_NO_TRADE_PAUSE_MINUTES                   = 12
CHOP_NO_TRADE_OPTION_EXIT_CLUSTER_WINDOW_MINUTES = 45
CHOP_NO_TRADE_OPTION_EXIT_CLUSTER_MIN_COUNT   = 3

# Adaptive learning from collected scan bars/results (day/week/month windows).
LEARNING_REFRESH_SECONDS   = 300    # recompute adaptive profile every 5 minutes
LEARNING_SCAN_LOG_MAX_ROWS = 12000  # cap CSV reads for runtime efficiency


# ---------------------------------------------------------------------------
# Catalyst mode
# ---------------------------------------------------------------------------

ENABLE_CATALYST_MODE              = False
CATALYST_WINDOW_MINUTES           = 90
CATALYST_INDEX_5M_MOVE_PCT        = 1.2
CATALYST_BREADTH_MOVE_PCT         = 1.0
CATALYST_BREADTH_MIN_COUNT        = 6
CATALYST_RELAXED_RVOL_MIN         = 1.0
CATALYST_DISABLE_RSI              = True
CATALYST_ALLOW_IV_FALLBACK        = True
CATALYST_RELAXED_IV_RANK_MAX      = 90.0
CATALYST_RELAXED_MIN_SIGNAL_SCORE = 6.5


# ---------------------------------------------------------------------------
# Optional filters (all off by default for simple call/put scalping)
# ---------------------------------------------------------------------------

ENABLE_HTF_CONFIRM         = False  # data-collection mode: disable HTF veto stacking
HTF_MISMATCH_HARD_REJECT   = False  # downgrade mismatches instead of outright rejection
HTF_TIMEFRAME              = "15m"
HTF_LOOKBACK_BARS          = 30
HTF_SLOPE_TOLERANCE_PCT    = 0.12   # allow near-flat HTF without hard reject
HTF_EMA_GAP_TOLERANCE_PCT  = 0.05   # allow tiny EMA overlap noise

ENABLE_ORDER_FLOW_FILTER   = False
MIN_FLOW_SCORE             = 0.20

ENABLE_NEWS_EVENT_BLOCK    = False
NEWS_LOOKBACK_MINUTES      = 90
NEWS_BLOCK_KEYWORDS        = (
    "earnings", "guidance", "sec", "investigation", "lawsuit",
    "fda", "downgrade", "upgrade", "cpi", "fomc", "fed",
)
NEWS_BLOCK_DATES_ET        = ()

ENABLE_HISTORICAL_REGIME_SCORE = False
MIN_HISTORICAL_REGIME_SCORE    = 2.0

ENABLE_INDEX_BIAS_FILTER   = False  # disabled: individual ticker direction is the signal, not market-wide bias
INDEX_BIAS_TIMEFRAME       = "5m"   # 5-minute bars for SPY/QQQ trend check
INDEX_BIAS_LOOKBACK        = 30    # 30 bars = 150 minutes of 5m history
INDEX_BIAS_ROC_PERIODS     = 6     # ROC over last 6 bars (30 min) for fast trend detection
INDEX_BIAS_ROC_THRESHOLD   = 0.10  # SPY/QQQ must move >0.10% in 30 min to declare a bias direction
INDEX_BIAS_REQUIRE_BOTH    = True  # SPY AND QQQ must agree

ENABLE_VIX_GUARD           = False  # data-collection mode: avoid extra global veto gate
VIX_MIN                    = 12.0  # below 12 = complacency, spreads too tight for edge
VIX_MAX                    = 60.0  # above 60 = panic, options too wide and unpredictable


# ---------------------------------------------------------------------------
# Entry confirmation
# ---------------------------------------------------------------------------

ENABLE_ENTRY_CONFIRMATION              = False  # disabled: no bar-confirmation wait, enter immediately
ENTRY_CONFIRM_BARS                     = 3
ENTRY_CONFIRM_BYPASS_MIN_SIGNAL_SCORE  = 0.0   # always bypass if confirmation somehow re-enabled
ENTRY_CONFIRM_MOMENTUM_THRESHOLD_PCT   = 0.14

# Fast-start gate (legacy) is permanently removed; the scanner's own direction
# conviction and RVOL thresholds are the single source of truth for entry quality.

# Feed freshness guardrail for intraday bars (scanner).
# Stock-bar feed (iex/sip) cooldown after a 403 forbidden response.
# After this many seconds the feed is retried automatically; before this it is
# skipped without retry. Prevents permanent disablement from a transient 403.
STOCK_BAR_FEED_FORBIDDEN_TTL_SECONDS   = 600
# If data timestamps are older than this, signals are rejected as stale.
STALE_BAR_MAX_AGE_SECONDS              = 900


# ---------------------------------------------------------------------------
# Options contract selection
# ---------------------------------------------------------------------------

MIN_OPTION_OPEN_INTEREST          = 10   # lowered from 25 — TSLA/MSFT were at 24/25 OI
MIN_OPTION_DAILY_VOLUME           = 3    # lowered from 5
MIN_OPTION_PREMIUM_USD            = 0.60  # avoid cheap options with huge bid/ask spreads (min $60/contract)
MAX_OPTION_SPREAD_PCT             = 15.0  # tighter spread gate: reject wide-spread contracts
ENABLE_OPTION_LIQUIDITY_RELAX     = True
OPTION_CONTRACTS_ALLOW_LIVE_FALLBACK = False
MIN_DTE_TRADING_DAYS              = 0    # allow same-day (0DTE) entries (morning only — see NO_NEW_0DTE_AFTER)
MAX_DTE_TRADING_DAYS              = 5    # expanded: find contracts up to 5 trading days out
NO_NEW_0DTE_AFTER                 = "11:30"  # CRITICAL: no new 0DTE entries after 11:30am — theta decay accelerates
MIN_OPTION_OPEN_INTEREST_0DTE     = 50    # 0DTE needs decent liquidity
ENABLE_DELTA_TARGETING            = True
TARGET_DELTA_MIN                  = 0.35  # slightly OTM: cheaper premium, better R:R for scalping
TARGET_DELTA_MAX                  = 0.50  # ATM max: avoid expensive deep ITM contracts
TARGET_DELTA_FALLBACK             = 0.40  # OTM-leaning fallback
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

EARNINGS_LOOKAHEAD_DAYS  = 1
EARNINGS_CHECK_STRICT    = False
EARNINGS_SKIP_SYMBOLS    = ("SPY", "QQQ", "IWM", "DIA", "VIX", "^VIX")


# ---------------------------------------------------------------------------
# Re-entry
# ---------------------------------------------------------------------------

MAX_ENTRIES_PER_TICKER_PER_DAY = 2   # max 2 entries per ticker per day (1 initial + 1 re-entry)
MAX_REENTRIES_PER_TICKER = 1         # max 1 re-entry per ticker per day — prevent churn on losing tickers

# Hard churn-kill: quick losers get a longer cooldown to avoid repeated tuition
# on the same tape. Applies only when realized loss and short hold-time are both true.
QUICK_LOSER_MAX_HOLD_MINUTES         = 4
QUICK_LOSER_REENTRY_COOLDOWN_MINUTES = 10

# Optional reversal entry after stop-loss. Disable by default to reduce churn.
ENABLE_STOPLOSS_REVERSAL_REENTRY = False


# ---------------------------------------------------------------------------
# Operational / timing constants
# ---------------------------------------------------------------------------

EASTERN_TZ                         = "US/Eastern"
CENTRAL_TZ                         = "US/Central"
RATE_LIMIT_SLEEP_SECONDS           = 0.1   # faster API pacing
CLOSED_MIN_SLEEP_SECONDS           = 60
CLOSED_MAX_SLEEP_SECONDS           = 900
MANUAL_PAUSE_SLEEP_SECONDS         = 30
HEARTBEAT_SECONDS                  = 60    # heartbeat every 60s
ALERT_COOLDOWN_SECONDS             = 300
ENTRY_ORDER_STATUS_WAIT_SECONDS    = 10
ENTRY_RETRY_STATUS_WAIT_SECONDS    = 7
ENTRY_MARKET_FALLBACK_WAIT_SECONDS = 4
ENABLE_ENTRY_MARKET_FALLBACK       = False
ENTRY_MARKET_FALLBACK_MAX_SPREAD_PCT = 12.0
ENTRY_RETRY_LIMIT_PCT              = 0.02
EXIT_ORDER_STATUS_POLL_SECONDS     = 2
EXIT_ORDER_MAX_WAIT_SECONDS        = 20
EXIT_CLOSE_RETRY_ATTEMPTS          = 5
SMART_EXIT_NORMAL_WAIT_SECONDS     = 2
SMART_EXIT_CRITICAL_WAIT_SECONDS   = 3
SMART_EXIT_NORMAL_REPRICE_PCT      = 0.10
SMART_EXIT_CRITICAL_REPRICE_PCT    = 0.10
ENABLE_EXIT_MARKET_FALLBACK        = True
EXIT_MARKET_FALLBACK_WAIT_SECONDS  = 6
# Faster close profile for hard stop-loss exits.
STOPLOSS_EXIT_ORDER_STATUS_POLL_SECONDS = 1
STOPLOSS_EXIT_ORDER_MAX_WAIT_SECONDS    = 3
STOPLOSS_EXIT_CLOSE_RETRY_ATTEMPTS      = 1
# Independent stop-loss watchdog runs even when trader loop is healthy.
INDEPENDENT_STOPLOSS_INTERVAL_SECONDS    = 2
INDEPENDENT_STOPLOSS_REQUIRE_STALE_LOOP  = False
PAPER_EXECUTION_FRICTION_PER_CONTRACT    = 1.0
TRADES_MAX_ROWS                    = 5000

# PAPER may be overridden via the ALPACA_PAPER env var ("0"/"false"/"no" → live).
# Default is paper trading. Switching to live requires confirmed live API keys.
_PAPER_ENV = str(os.getenv("ALPACA_PAPER", "")).strip().lower()
if _PAPER_ENV in {"0", "false", "no", "off", "live"}:
    PAPER = False
else:
    PAPER = True


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

# Service boot behavior:
# If True, any persisted manual_stop latch is cleared at process start so
# trading resumes automatically after deploy/restart.
AUTO_RESUME_TRADING_ON_BOOT          = True


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
ALPACA_STOCK_BAR_FEEDS = ("iex", "sip")  # try IEX first; fall back to SIP when available


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
