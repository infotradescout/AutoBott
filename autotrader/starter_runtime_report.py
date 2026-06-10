"""Print a deterministic report of Starter-safe runtime controls."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

try:
    from autotrader import config
except ImportError:
    import config  # type: ignore


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "y", "on"}


def _effective_values() -> dict[str, Any]:
    starter_safe = _env_bool(
        "RENDER_STARTER_SAFE_MODE",
        bool(getattr(config, "RENDER_STARTER_SAFE_MODE", False)),
    )
    values: dict[str, Any] = {
        "render_starter_safe_mode": starter_safe,
        "universe_mode": str(getattr(config, "UNIVERSE_MODE", "") or ""),
        "auto_expand_universe_with_movers": bool(getattr(config, "AUTO_EXPAND_UNIVERSE_WITH_MOVERS", False)),
        "enable_yfinance_fallback": _env_bool(
            "ENABLE_YFINANCE_FALLBACK",
            bool(getattr(config, "ENABLE_YFINANCE_FALLBACK", True)),
        ),
        "scan_intraday_bars": int(getattr(config, "SCAN_INTRADAY_BARS", 0) or 0),
        "option_enrichment_max_attempts_per_cycle": int(
            getattr(config, "OPTION_ENRICHMENT_MAX_ATTEMPTS_PER_CYCLE", 0) or 0
        ),
        "liquidity_rank_max_candidates_per_cycle": int(
            getattr(config, "LIQUIDITY_RANK_MAX_CANDIDATES_PER_CYCLE", 0) or 0
        ),
        "liquidity_rank_max_etf_candidates_per_cycle": int(
            getattr(config, "LIQUIDITY_RANK_MAX_ETF_CANDIDATES_PER_CYCLE", 0) or 0
        ),
        "liquidity_rank_max_single_name_candidates_per_cycle": int(
            getattr(config, "LIQUIDITY_RANK_MAX_SINGLE_NAME_CANDIDATES_PER_CYCLE", 0) or 0
        ),
        "max_contracts_per_ticker_per_hour": int(
            getattr(config, "MAX_CONTRACTS_PER_TICKER_PER_HOUR", 0) or 0
        ),
        "loop_interval_seconds": int(getattr(config, "LOOP_INTERVAL_SECONDS", 0) or 0),
        "continuous_entry_search_sleep_seconds": int(
            getattr(config, "CONTINUOUS_ENTRY_SEARCH_SLEEP_SECONDS", 0) or 0
        ),
        "dashboard_truth_cache_seconds": int(
            os.getenv("DASHBOARD_TRUTH_CACHE_SECONDS")
            or getattr(config, "DASHBOARD_TRUTH_CACHE_SECONDS", 0)
            or 0
        ),
        "disable_verbose_market_diagnostics": bool(
            getattr(config, "DISABLE_VERBOSE_MARKET_DIAGNOSTICS", False)
        ),
        "signal_pattern_memory_history_rows": int(
            getattr(config, "SIGNAL_PATTERN_MEMORY_HISTORY_ROWS", 0) or 0
        ),
        "enable_pre_execution_history_check": bool(
            getattr(config, "ENABLE_PRE_EXECUTION_HISTORY_CHECK", False)
        ),
    }
    if starter_safe:
        values.update(
            {
                "universe_mode": "core",
                "auto_expand_universe_with_movers": False,
                "enable_yfinance_fallback": False,
                "scan_intraday_bars": min(25, max(10, int(values["scan_intraday_bars"] or 25))),
                "option_enrichment_max_attempts_per_cycle": min(
                    2,
                    max(0, int(values["option_enrichment_max_attempts_per_cycle"] or 2)),
                ),
                "liquidity_rank_max_candidates_per_cycle": min(
                    4,
                    max(1, int(values["liquidity_rank_max_candidates_per_cycle"] or 4)),
                ),
                "liquidity_rank_max_etf_candidates_per_cycle": min(
                    2,
                    max(1, int(values["liquidity_rank_max_etf_candidates_per_cycle"] or 2)),
                ),
                "liquidity_rank_max_single_name_candidates_per_cycle": min(
                    2,
                    max(1, int(values["liquidity_rank_max_single_name_candidates_per_cycle"] or 2)),
                ),
                "max_contracts_per_ticker_per_hour": min(
                    1,
                    max(0, int(values["max_contracts_per_ticker_per_hour"] or 1)),
                ),
                "loop_interval_seconds": max(60, int(values["loop_interval_seconds"] or 60)),
                "continuous_entry_search_sleep_seconds": max(
                    60,
                    int(values["continuous_entry_search_sleep_seconds"] or 60),
                ),
                "dashboard_truth_cache_seconds": max(
                    30,
                    int(values["dashboard_truth_cache_seconds"] or 30),
                ),
                "disable_verbose_market_diagnostics": True,
                "signal_pattern_memory_history_rows": 0,
                "enable_pre_execution_history_check": False,
            }
        )
    return values


def build_report() -> dict[str, Any]:
    data_dir = Path(getattr(config, "DATA_DIR", ""))
    effective = _effective_values()
    return {
        "current_commit": str(os.getenv("RENDER_GIT_COMMIT", "") or "local"),
        "runtime_mode": {
            "render_starter_safe_mode": bool(effective["render_starter_safe_mode"]),
            "paper": bool(getattr(config, "PAPER", True)),
        },
        "ticker_universe": {
            "universe_mode": str(effective["universe_mode"]),
            "core_ticker_count": len(list(getattr(config, "CORE_TICKERS", []) or [])),
            "auto_expand_universe_with_movers": bool(effective["auto_expand_universe_with_movers"]),
        },
        "providers": {
            "enable_yfinance_fallback": bool(effective["enable_yfinance_fallback"]),
        },
        "runtime_caps": {
            "scan_intraday_bars": int(effective["scan_intraday_bars"]),
            "option_enrichment_max_attempts_per_cycle": int(effective["option_enrichment_max_attempts_per_cycle"]),
            "liquidity_rank_max_candidates_per_cycle": int(effective["liquidity_rank_max_candidates_per_cycle"]),
            "liquidity_rank_max_etf_candidates_per_cycle": int(effective["liquidity_rank_max_etf_candidates_per_cycle"]),
            "liquidity_rank_max_single_name_candidates_per_cycle": int(effective["liquidity_rank_max_single_name_candidates_per_cycle"]),
            "max_contracts_per_ticker_per_hour": int(effective["max_contracts_per_ticker_per_hour"]),
            "loop_interval_seconds": int(effective["loop_interval_seconds"]),
            "continuous_entry_search_sleep_seconds": int(effective["continuous_entry_search_sleep_seconds"]),
        },
        "diagnostics": {
            "write_scan_log": bool(getattr(config, "WRITE_SCAN_LOG", True)),
            "disable_verbose_market_diagnostics": bool(effective["disable_verbose_market_diagnostics"]),
            "dashboard_truth_cache_seconds": int(effective["dashboard_truth_cache_seconds"]),
            "signal_pattern_memory_history_rows": int(effective["signal_pattern_memory_history_rows"]),
            "enable_pre_execution_history_check": bool(effective["enable_pre_execution_history_check"]),
        },
        "memory_env": {
            "data_dir": str(data_dir),
            "pythonmalloc": str(os.getenv("PYTHONMALLOC", "") or "default"),
            "malloc_arena_max": str(os.getenv("MALLOC_ARENA_MAX", "") or "default"),
        },
    }


if __name__ == "__main__":
    print(json.dumps(build_report(), indent=2, sort_keys=True))
