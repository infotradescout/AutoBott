from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from types import FunctionType
from typing import Any

from . import trading_cycle as legacy_cycle
from .execution_config import portfolio_mode_enabled
from .expiry_coverage import ExpiryCoverageClient
from .phase1_engine_v2 import build_decision_card as build_decision_card_v2
from .position_monitor_v2 import run_position_monitor as run_position_monitor_v2
from .portfolio_budget import submit_budgeted_pair
from .ranked_entry_scan import RankedCapturePlan


TradingCycleResult = legacy_cycle.TradingCycleResult
load_decision_cards = legacy_cycle.load_decision_cards


def run_trading_cycle(*, symbols: list[str], **kwargs: Any) -> TradingCycleResult:
    """Use per-call bindings while retaining native guards, journals and locks."""
    shell = legacy_cycle.run_trading_cycle
    if not isinstance(shell, FunctionType):
        raise TypeError("cycle_shell_must_be_python_function")
    namespace = {**shell.__globals__, "build_decision_card": build_decision_card_v2,
                 "run_position_monitor": run_position_monitor_v2}
    coverage: list[dict[str, Any]] = []
    clients: dict[str, Any] = {}
    if kwargs.get("data_client") is not None:
        client = ExpiryCoverageClient(kwargs["data_client"], coverage)
        clients["data"] = client
        kwargs = {**kwargs, "data_client": client}
    elif "AlpacaPaperClient" in namespace:
        client_factory = namespace["AlpacaPaperClient"]
        def observed_client():
            client = ExpiryCoverageClient(client_factory(), coverage)
            clients["data"] = client
            return client
        namespace["AlpacaPaperClient"] = observed_client
    plan = None
    if portfolio_mode_enabled():
        def capture_args():
            observed = datetime.now(UTC)
            return {"corpus_root": kwargs.get("corpus_root") or namespace["phase1_snapshots_root"](),
                "scheduled_market_time": kwargs.get("scheduled_market_time") or observed,
                "captured_at_utc": kwargs.get("captured_at_utc") or observed,
                "corpus_type": "paper_capture", "market_timezone": "America/New_York",
                "volatility_proxy_symbol": "VIXY", "data_client": clients["data"],
                "rules": kwargs.get("rules") or namespace["_hosted_capture_rules"]()}
        plan = RankedCapturePlan(capture=namespace["capture_symbol_snapshot"], load=namespace["_load_snapshot"],
            make_input=namespace["_decision_input_from_snapshot"], build=build_decision_card_v2,
            execution_rules=namespace["_hosted_execution_rules"], capture_args=capture_args,
            original_priority=namespace["_prioritize_symbols_by_winners"])
        namespace.update(_prioritize_symbols_by_winners=plan.rank_symbols,
            capture_symbol_snapshot=plan.capture_for_execution, build_decision_card=plan.build_for_execution,
            submit_core_runner_to_broker=submit_budgeted_pair)
    isolated_shell = FunctionType(shell.__code__, namespace, shell.__name__, shell.__defaults__, shell.__closure__)
    isolated_shell.__kwdefaults__ = shell.__kwdefaults__
    result = isolated_shell(symbols=symbols, **kwargs)
    if isinstance(result, TradingCycleResult):
        extra = []
        if coverage:
            extra.append({"disposition": "option_expiration_coverage", "version": "observed_expirations.v1",
                          "extra_provider_requests": 0, "symbols": coverage})
        if plan is not None:
            extra.append({"disposition": "ranked_entry_scan", "version": "native_score_rank.v1",
                "ranking_is_profitability_evidence": False, "candidate_refresh_required": True,
                "symbols": plan.summary})
        if extra:
            result = replace(result, execution_outcomes=[*result.execution_outcomes, *extra])
    return result
