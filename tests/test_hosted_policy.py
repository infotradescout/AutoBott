from __future__ import annotations

from autobott_v2.core_runner import load_core_runner_rules
from autobott_v2.execution_config import load_alpaca_execution_config
from autobott_v2.defined_risk_spreads import load_defined_risk_spread_rules
from autobott_v2.hosted_policy import is_hosted_paper_runtime, is_hosted_runtime
from autobott_v2.phase1_snapshot_capture import (
    _capture_option_quote_files_enabled,
    _manual_mirror_capture_max_contract_cost,
)
from autobott_v2.session_supervisor import load_session_supervisor_config
from autobott_v2.position_monitor import load_position_monitor_rules
from autobott_v2.trading_cycle import _hosted_capture_rules, _hosted_execution_rules


def _clear_hosted_markers(monkeypatch) -> None:
    for name in ("RENDER", "RENDER_SERVICE_ID", "AUTOBOTT_DATA_ROOT"):
        monkeypatch.delenv(name, raising=False)


def test_hosted_runtime_detection_is_platform_owned(monkeypatch) -> None:
    _clear_hosted_markers(monkeypatch)
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setenv("ALPACA_ENV", "paper")
    assert is_hosted_runtime() is True
    assert is_hosted_paper_runtime() is True

    # Broker mode is part of the hosted code contract. A stale Render value
    # must not switch the deployed service out of its paper policy.
    monkeypatch.setenv("ALPACA_ENV", "live")
    assert is_hosted_runtime() is True
    assert is_hosted_paper_runtime() is True


def test_hosted_session_ignores_stale_render_strategy_values(monkeypatch) -> None:
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setenv("ALPACA_ENV", "live")
    monkeypatch.setenv("AUTOBOTT_SESSION_SYMBOLS", "AAPL")
    monkeypatch.setenv("AUTOBOTT_SESSION_INTERVAL_SECONDS", "999")
    monkeypatch.setenv("AUTOBOTT_SESSION_SYMBOL_BATCH_SIZE", "1")
    monkeypatch.setenv("AUTOBOTT_SESSION_MAX_CYCLES", "1")
    monkeypatch.setenv("AUTOBOTT_SESSION_RUN_FOREVER", "false")
    monkeypatch.setenv("AUTOBOTT_SESSION_QUANTITY", "99")
    monkeypatch.setenv("AUTOBOTT_SESSION_POSITION_COUNT", "100")
    monkeypatch.setenv("AUTOBOTT_SESSION_DAILY_PNL", "-999999")
    monkeypatch.setenv("AUTOBOTT_SESSION_ARM_PAPER_EXECUTION", "false")
    monkeypatch.setenv("AUTOBOTT_SESSION_START_TIME", "23:59")
    monkeypatch.setenv("AUTOBOTT_SESSION_END_TIME", "00:01")
    monkeypatch.setenv("AUTOBOTT_SESSION_MARKET_TIMEZONE", "UTC")
    monkeypatch.setenv("AUTOBOTT_POSITION_MONITOR_HEARTBEAT_ENABLED", "true")
    monkeypatch.setenv("AUTOBOTT_POSITION_MONITOR_HEARTBEAT_SECONDS", "5")

    config = load_session_supervisor_config()

    assert config.symbols[:3] == ["VIX", "VXX", "UVXY"]
    assert "SPY" in config.symbols and "QQQ" in config.symbols
    assert config.interval_seconds == 90
    assert config.symbol_batch_size == 25
    assert config.run_forever is True
    assert config.max_cycles is None
    assert config.quantity == 1
    assert config.position_count == 0
    assert config.daily_pnl == 0.0
    assert config.start_time == "09:35:00"
    assert config.end_time == "15:55:00"
    assert config.market_timezone == "America/New_York"
    assert config.arm_paper_execution_on_start is True
    assert config.position_monitor_heartbeat_enabled is False
    assert config.position_monitor_heartbeat_seconds == 90


def test_hosted_execution_ignores_stale_render_risk_values(monkeypatch) -> None:
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setenv("ALPACA_ENV", "live")
    monkeypatch.setenv("ALPACA_API_KEY_ID", "retained-key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "retained-secret")
    monkeypatch.setenv("ALPACA_TRADING_BASE_URL", "https://api.alpaca.markets")
    monkeypatch.setenv("ALPACA_DATA_BASE_URL", "https://stale.invalid")
    monkeypatch.setenv("AUTOBOTT_LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("AUTOBOTT_ALLOW_ORDER_PLACEMENT", "false")
    monkeypatch.setenv("AUTOBOTT_MAX_POSITION_COST", "999999")
    monkeypatch.setenv("AUTOBOTT_MAX_DAILY_LOSS", "1")
    monkeypatch.setenv("AUTOBOTT_MAX_OPEN_POSITIONS", "100")
    monkeypatch.setenv("AUTOBOTT_PAPER_MAX_NEW_ENTRY_ATTEMPTS_PER_LOOP", "99")
    monkeypatch.setenv("AUTOBOTT_PAPER_MAX_OPEN_ENTRY_BUY_ORDERS", "99")
    monkeypatch.setenv("AUTOBOTT_PAPER_IGNORE_POSITION_COST_LIMIT", "true")

    config = load_alpaca_execution_config().validate()

    assert config.environment.value == "paper"
    assert config.trading_base_url == "https://paper-api.alpaca.markets"
    assert config.data_base_url == "https://data.alpaca.markets"
    assert config.allow_live_trading is False
    assert config.allow_order_placement is True
    assert config.max_position_cost == 1000.0
    assert config.max_daily_loss == 750.0
    assert config.max_open_positions == 6
    assert config.paper_max_new_entry_attempts_per_loop == 3
    assert config.paper_max_open_entry_buy_orders == 6
    assert config.paper_ignore_position_cost_limit is False
    assert load_core_runner_rules().core_min_open_interest == 0
    assert load_core_runner_rules().runner_min_open_interest == 0
    assert load_position_monitor_rules().exit_min_dte == 2


def test_hosted_capture_storage_mode_ignores_stale_duplicate_write_flag(monkeypatch) -> None:
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setenv("ALPACA_ENV", "live")
    monkeypatch.setenv("AUTOBOTT_CAPTURE_OPTION_QUOTE_FILES", "true")

    assert _capture_option_quote_files_enabled() is False


def test_hosted_auxiliary_research_ignores_invalid_retained_environment(monkeypatch) -> None:
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setenv("AUTOBOTT_MANUAL_MIRROR_MAX_CONTRACT_COST", "invalid")
    monkeypatch.setenv("AUTOBOTT_SPREAD_MAX_RISK", "invalid")

    assert _manual_mirror_capture_max_contract_cost() == 100.0
    assert load_defined_risk_spread_rules().enabled is False


def test_hosted_capture_and_execution_dte_windows_match(monkeypatch) -> None:
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setenv("ALPACA_ENV", "paper")
    monkeypatch.setenv("AUTOBOTT_ENTRY_MIN_DTE", "1")
    monkeypatch.setenv("AUTOBOTT_ENTRY_TACTICAL_MAX_DTE", "3")
    monkeypatch.setenv("AUTOBOTT_ENTRY_RIDER_MIN_DTE", "7")
    monkeypatch.setenv("AUTOBOTT_ENTRY_RIDER_MAX_DTE", "30")

    capture = _hosted_capture_rules()
    execution = _hosted_execution_rules()

    assert (capture.tactical_min_dte, capture.tactical_max_dte) == (5, 10)
    assert (capture.rider_min_dte, capture.rider_max_dte) == (14, 45)
    assert (capture.option_chain_min_dte, capture.option_chain_max_dte) == (5, 45)
    assert capture.bar_timeframe == "1Hour"
    assert (execution.intraday_min_dte, execution.intraday_max_dte) == (5, 10)
    assert (execution.rider_min_dte, execution.rider_max_dte) == (14, 45)
    assert execution.min_open_interest == 0
    assert {"VIX", "VIXW", "VXX", "UVXY"}.issubset(execution.risk_off_bullish_exempt_symbols)
