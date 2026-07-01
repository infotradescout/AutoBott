from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .phase1_alpaca_client import AlpacaPaperClient
from .phase1_alpaca_config import load_alpaca_paper_config
from .phase1_engine import build_decision_card
from .phase1_snapshot_capture import CaptureRules, capture_symbol_snapshot
from .phase1_validate import _decision_input_from_snapshot, _load_snapshot
from .position_store import load_open_positions
from .runtime_control import load_runtime_state
from .runtime_paths import phase1_snapshots_root


def run_paper_readiness_probe(
    *,
    symbol: str = "SPY",
    client: Any | None = None,
    corpus_root: str | Path | None = None,
    scheduled_market_time: datetime | None = None,
    captured_at_utc: datetime | None = None,
    rules: CaptureRules | None = None,
) -> dict[str, Any]:
    config = load_alpaca_paper_config()
    runtime_state = load_runtime_state()
    positions = load_open_positions()
    response: dict[str, Any] = {
        "ok": False,
        "symbol": symbol.upper(),
        "paper_config_valid": False,
        "credentials_present": bool(config.api_key and config.secret_key),
        "runtime_execution_enabled": runtime_state.execution_enabled,
        "runtime_kill_switch_enabled": runtime_state.kill_switch_enabled,
        "runtime_live_mode_enabled": runtime_state.live_mode_enabled,
        "open_position_count": len(positions),
    }

    try:
        validated = config.validate()
        response["paper_config_valid"] = True
    except Exception as exc:
        response["status"] = "config_invalid"
        response["detail"] = str(exc)
        return response

    resolved_client = client or AlpacaPaperClient(validated)
    resolved_symbol = symbol.upper()

    try:
        account = resolved_client.get_account()
        quotes = resolved_client.get_latest_stock_quotes([resolved_symbol, "SPY", "QQQ"])
        option_snapshots = resolved_client.get_option_chain_snapshots(resolved_symbol)
    except Exception as exc:
        response["status"] = "paper_connectivity_failed"
        response["detail"] = str(exc)
        return response

    response["account_status"] = str(account.get("status", "unknown"))
    response["quote_symbols"] = sorted(quotes.keys())
    response["option_snapshot_count"] = len(option_snapshots)

    try:
        snapshot_path = capture_symbol_snapshot(
            symbol=resolved_symbol,
            corpus_root=Path(corpus_root) if corpus_root is not None else phase1_snapshots_root(),
            scheduled_market_time=scheduled_market_time or datetime.now(tz=UTC),
            captured_at_utc=captured_at_utc or datetime.now(tz=UTC),
            corpus_type="paper_capture",
            market_timezone="America/New_York",
            volatility_proxy_symbol="VIXY",
            data_client=resolved_client,
            rules=rules or CaptureRules(),
        )
        snapshot = _load_snapshot(Path(snapshot_path))
        decision = build_decision_card(_decision_input_from_snapshot(snapshot))
    except Exception as exc:
        response["status"] = "snapshot_or_decision_failed"
        response["detail"] = str(exc)
        return response

    response.update(
        {
            "ok": True,
            "status": "paper_ready",
            "snapshot_path": snapshot_path,
            "option_chain_count": len(snapshot.get("option_chain", [])),
            "decision_status": decision.decision.value,
            "trade_setup": decision.trade_setup.value,
            "execution_layer": decision.execution_layer.value,
            "selected_contract": decision.selected_contract.option_symbol if decision.selected_contract else None,
        }
    )
    return response


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Probe AutoBott paper-market readiness with real Alpaca paper data.")
    parser.add_argument("--symbol", default="SPY", help="Primary symbol to probe.")
    parser.add_argument("--corpus-root", help="Optional capture output root.")
    args = parser.parse_args(argv)
    result = run_paper_readiness_probe(symbol=args.symbol, corpus_root=args.corpus_root)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
