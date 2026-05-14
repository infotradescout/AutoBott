from datetime import datetime
from pathlib import Path
import csv
import sys
import tempfile

import pandas as pd
import pytz

_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

import scanner  # noqa: E402
import config  # noqa: E402
import strategy_profiles  # noqa: E402


def test_generic_discovery_profile_accepts_non_core_mover_symbol():
    now_et = pytz.timezone("US/Eastern").localize(datetime(2026, 5, 12, 11, 30))
    bars = pd.DataFrame({"close": [100.0, 100.05, 100.10, 100.14, 100.18]})
    signal = {
        "symbol": "XYZ",
        "direction": "call",
        "direction_score": 0.25,
        "signal_score": 3.2,
        "rvol": 0.2,
        "roc": 0.04,
        "price": 100.18,
        "vwap": 100.05,
        "reason": "discovery candidate",
    }

    passed, rejected = scanner._profile_signals_for_candidate(
        base_signal=signal,
        bars_df=bars,
        now_et=now_et,
        catalyst_mode_active=False,
    )

    assert rejected
    assert len(passed) == 1
    assert passed[0]["symbol"] == "XYZ"
    assert passed[0]["strategy_profile"] == "generic_intraday_continuation"
    assert passed[0]["stop_loss_usd"] == 180.0
    assert passed[0]["immediate_take_profit_pct"] == 0.55
    assert passed[0]["max_hold_minutes"] == 150


def test_intraday_profiles_use_growth_mode_exits():
    for profile in scanner.PROFILES.values():
        assert profile.stop_loss_usd == 180.0
        assert profile.immediate_take_profit_pct == 0.55


def test_runtime_strategy_profiles_do_not_restore_tiny_stops():
    assert strategy_profiles.PROFILE_PRESETS["aggressive"]["stop_loss_usd"] == 180.0
    assert strategy_profiles.PROFILE_PRESETS["balanced"]["stop_loss_usd"] == 150.0
    assert strategy_profiles.PROFILE_PRESETS["conservative"]["stop_loss_usd"] == 120.0


def test_growth_frequency_config_keeps_trading_through_day():
    assert config.ENABLE_INDEX_BIAS_FILTER is False
    assert config.MAX_SAME_DIRECTION_POSITIONS == 0
    assert config.OPENING_MAX_SIGNAL_CANDIDATES == 10
    assert config.MAX_ENTRIES_PER_TICKER_PER_DAY == 4
    assert config.TICKER_ROUNDTRIP_COOLDOWN_MINUTES == 5
    assert config.MAX_ALPACA_TRUTH_ROUNDTRIPS_PER_TICKER_PER_DAY == 4
    assert config.ENTRY_LIMIT_ATTEMPTS == 2
    assert config.CANCEL_UNFILLED_ENTRY_BEFORE_RETRY is True
    assert config.ENABLE_ENTRY_MARKET_FALLBACK is False
    assert config.LOOP_INTERVAL_SECONDS == 5
    assert config.MAX_POSITIONS == 0
    assert config.MAX_POSITION_SIZE_USD == 0.0
    assert config.MAX_CONTRACTS_PER_ENTRY == 0
    assert config.MAX_CONTRACTS_PER_TICKER == 0
    assert config.MAX_PREMIUM_PER_TRADE_USD == 0.0
    assert config.MAX_TOTAL_OPEN_PREMIUM_USD == 0.0
    assert config.OPENING_MAX_FRESH_PREMIUM_USD == 0.0
    assert config.MAX_OPTION_PREMIUM_TO_UNDERLYING_PCT == 8.0
    assert config.MAX_OPTION_STRIKE_DISTANCE_PCT == 8.0
    assert config.EXECUTION_MIN_RVOL_AFTER_IGNORE == 0.50
    assert config.ENABLE_ENTRY_CONFIRMATION is True
    assert config.ENABLE_SIGNAL_PATTERN_MEMORY is True
    assert config.ENABLE_PRE_EXECUTION_HISTORY_CHECK is True
    assert config.ADAPTIVE_BLOCK_LOSING_TICKERS is True
    assert config.ALPACA_TRUTH_ADAPT_AFTER_LOSS_USD == 75.0
    assert config.LOSS_THROTTLE_AFTER_CONSEC_LOSSES == 1
    assert config.EARLY_RED_GUARD_ENABLED is True
    assert config.EARLY_RED_GUARD_MAX_NET_PNL_USD == -150.0
    assert config.INDEPENDENT_STOPLOSS_REQUIRE_STALE_LOOP is True
    assert config.UNIVERSE_MAX_TICKERS == 300
    assert config.UNIVERSE_MOVER_TOP == 120
    assert config.MOVER_SYMBOLS_PER_SIDE == 60
    assert config.RATE_LIMIT_SLEEP_SECONDS == 0.05
    assert config.REJECT_COOLDOWN_MEDIUM_MINUTES == 5


def test_medium_reject_cooldown_can_be_short_for_active_scan():
    now_et = pytz.timezone("US/Eastern").localize(datetime(2026, 5, 13, 12, 0))
    scan = scanner.IntradayScanner(object(), emit_summary=False, write_scan_log=False)

    bucket, until = scan._cooldown_for_reject("option chain unavailable", now_et)

    assert bucket == "medium"
    assert int((until - now_et).total_seconds() / 60) == 5


def test_reversal_snapback_does_not_flip_untuned_core_symbol():
    now_et = pytz.timezone("US/Eastern").localize(datetime(2026, 5, 13, 11, 31))
    bars = pd.DataFrame({"close": [100.9, 100.8, 100.65, 100.45, 100.16]})
    signal = {
        "symbol": "ORCL",
        "direction": "put",
        "direction_score": -1.0,
        "signal_score": 18.25,
        "rvol": 0.02,
        "roc": -0.29,
        "price": 100.16,
        "vwap": 100.49,
        "reason": "Below VWAP | EMA bearish",
    }

    passed, rejected = scanner._profile_signals_for_candidate(
        base_signal=signal,
        bars_df=bars,
        now_et=now_et,
        catalyst_mode_active=False,
    )

    assert "reversal_snapback:symbol" in rejected
    assert passed
    assert passed[0]["direction"] == "put"


def test_reversal_snapback_requires_actual_snapback_momentum():
    now_et = pytz.timezone("US/Eastern").localize(datetime(2026, 5, 13, 11, 31))
    bars = pd.DataFrame({"close": [100.9, 100.8, 100.65, 100.45, 100.16]})
    signal = {
        "symbol": "QQQ",
        "direction": "put",
        "direction_score": -1.0,
        "signal_score": 18.25,
        "rvol": 0.02,
        "roc": -0.29,
        "price": 100.16,
        "vwap": 100.49,
        "reason": "Below VWAP | EMA bearish",
    }

    passed, rejected = scanner._profile_signals_for_candidate(
        base_signal=signal,
        bars_df=bars,
        now_et=now_et,
        catalyst_mode_active=False,
    )

    assert "reversal_snapback:logic" in rejected
    assert passed
    assert passed[0]["direction"] == "put"


def test_scanner_logs_candidates_not_trade_passes():
    now_et = pytz.timezone("US/Eastern").localize(datetime(2026, 5, 12, 11, 30))
    with tempfile.TemporaryDirectory() as tmp:
        old_path = scanner.SCAN_LOG_PATH
        scanner.SCAN_LOG_PATH = Path(tmp) / "scan_log.csv"
        try:
            scan = scanner.IntradayScanner(object(), emit_summary=False, write_scan_log=True)
            scan._write_scan_log(
                now_et,
                passed=[
                    {
                        "symbol": "XYZ",
                        "strategy_profile": "generic_intraday_continuation",
                        "direction": "call",
                        "rvol": 0.2,
                        "rsi": 50,
                        "roc": 0.04,
                        "signal_score": 3.2,
                        "reason": "candidate only",
                    }
                ],
                failed=[],
            )
            with scanner.SCAN_LOG_PATH.open("r", newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            assert rows[0]["result"] == "candidate"
        finally:
            scanner.SCAN_LOG_PATH = old_path
