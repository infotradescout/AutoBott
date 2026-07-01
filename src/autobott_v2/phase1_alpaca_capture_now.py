from __future__ import annotations

import argparse
import hashlib
import json
import math
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .phase1_alpaca_client import AlpacaPaperClient
from .phase1_alpaca_config import AlpacaPaperConfig, require_alpaca_paper_config
from .runtime_paths import gate_path as default_gate_path
from .runtime_paths import phase1_snapshots_root


def capture_now(
    *,
    symbols: list[str],
    minutes: int,
    interval_seconds: int,
    corpus_root: str | Path | None = None,
    client: Any | None = None,
    config: AlpacaPaperConfig | None = None,
    active_gate_path: str | Path | None = None,
    now_fn: Any | None = None,
    sleep_fn: Any = time.sleep,
) -> dict[str, Any]:
    if not symbols:
        raise ValueError("symbols_required")
    if minutes <= 0:
        raise ValueError("minutes_must_be_positive")
    if interval_seconds <= 0:
        raise ValueError("interval_seconds_must_be_positive")

    resolved_config = (config or require_alpaca_paper_config()).validate()
    resolved_client = client or AlpacaPaperClient(resolved_config)
    target_root = Path(corpus_root) if corpus_root is not None else phase1_snapshots_root()
    gate_target = Path(active_gate_path) if active_gate_path is not None else default_gate_path()
    gate_before = _file_hash(gate_target)
    iterations = max(1, math.ceil((minutes * 60) / interval_seconds))
    snapshots_written: dict[str, int] = {symbol.upper(): 0 for symbol in symbols}
    option_quotes_written: dict[str, int] = {symbol.upper(): 0 for symbol in symbols}
    option_skip_reasons: dict[str, str | None] = {symbol.upper(): None for symbol in symbols}
    quality_flags: dict[str, set[str]] = {symbol.upper(): set() for symbol in symbols}
    capture_started = _current_time(now_fn)
    manifest_paths: dict[str, str] = {}

    for index in range(iterations):
        capture_time = _current_time(now_fn)
        for symbol in symbols:
            symbol_key = symbol.upper()
            quote_payload = resolved_client.get_latest_stock_quotes([symbol_key]).get(symbol_key)
            if quote_payload is None:
                raise ValueError(f"stock_quote_missing:{symbol_key}")
            bars_payload = resolved_client.get_stock_bars(
                [symbol_key],
                start=capture_time - timedelta(minutes=40),
                end=capture_time,
                limit=35,
            ).get(symbol_key, [])
            option_payload: dict[str, Any] = {}
            try:
                option_payload = resolved_client.get_option_chain_snapshots(symbol_key)
            except Exception as exc:  # pragma: no cover
                option_skip_reasons[symbol_key] = f"{type(exc).__name__}:{exc}"
                quality_flags[symbol_key].add("option_quotes_skipped")
            else:
                if option_payload:
                    option_quotes_written[symbol_key] += 1
                else:
                    option_skip_reasons[symbol_key] = "no_option_snapshots_returned"
                    quality_flags[symbol_key].add("option_quotes_skipped")

            _write_capture_files(
                root=target_root,
                capture_time=capture_time,
                symbol=symbol_key,
                quote_payload=quote_payload,
                bars_payload=bars_payload,
                option_payload=option_payload,
                option_skip_reason=option_skip_reasons[symbol_key],
            )
            snapshots_written[symbol_key] += 1

        if index < iterations - 1:
            sleep_fn(interval_seconds)

    capture_finished = _current_time(now_fn)
    for symbol in symbols:
        symbol_key = symbol.upper()
        manifest_path = _write_manifest(
            root=target_root,
            symbol=symbol_key,
            capture_started=capture_started,
            capture_finished=capture_finished,
            interval_seconds=interval_seconds,
            snapshots_captured=snapshots_written[symbol_key],
            option_quotes_captured=option_quotes_written[symbol_key],
            skipped_option_quote_reason=option_skip_reasons[symbol_key],
            data_quality_flags=sorted(quality_flags[symbol_key]),
            active_gate_mutated=False,
            order_placement_enabled=False,
        )
        manifest_paths[symbol_key] = str(manifest_path)

    gate_after = _file_hash(gate_target)
    gate_mutated = gate_before != gate_after
    if gate_mutated:
        raise ValueError("active_gate_mutated_during_capture")

    return {
        "schema_version": "phase1_alpaca_capture_now.v1",
        "source": "alpaca",
        "corpus_type": "paper_capture",
        "corpus_root": str(target_root),
        "symbols": [symbol.upper() for symbol in symbols],
        "minutes": minutes,
        "interval_seconds": interval_seconds,
        "snapshots_captured": snapshots_written,
        "option_quotes_captured": option_quotes_written,
        "option_quote_status": {
            symbol.upper(): (
                "CAPTURED" if option_quotes_written[symbol.upper()] > 0 else f"SKIP_WITH_REASON:{option_skip_reasons[symbol.upper()] or 'unknown'}"
            )
            for symbol in symbols
        },
        "manifest_paths": manifest_paths,
        "active_gate_changed": False,
        "order_placement_exists": False,
        "live_trading_enabled": False,
        "config": resolved_config.redacted_dict(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Capture raw Alpaca paper-market data immediately for Phase 1.")
    parser.add_argument("--symbols", nargs="+", required=True, help="Ticker list, for example: SPY QQQ")
    parser.add_argument("--minutes", type=int, required=True, help="Total capture duration in whole minutes.")
    parser.add_argument("--interval-seconds", type=int, default=60, help="Capture cadence in seconds.")
    parser.add_argument("--corpus-root", default=str(phase1_snapshots_root()), help="Output root for raw and normalized snapshots.")
    args = parser.parse_args(argv)

    result = capture_now(
        symbols=args.symbols,
        minutes=args.minutes,
        interval_seconds=args.interval_seconds,
        corpus_root=args.corpus_root,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _write_capture_files(
    *,
    root: Path,
    capture_time: datetime,
    symbol: str,
    quote_payload: dict[str, Any],
    bars_payload: list[dict[str, Any]],
    option_payload: dict[str, Any],
    option_skip_reason: str | None,
) -> None:
    trading_date = capture_time.astimezone(UTC).date().isoformat()
    symbol_dir = root / trading_date / symbol
    raw_dir = symbol_dir / "raw"
    snapshot_dir = symbol_dir / "snapshots"
    option_dir = symbol_dir / "option_quotes"
    raw_dir.mkdir(parents=True, exist_ok=True)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    option_dir.mkdir(parents=True, exist_ok=True)

    filename = capture_time.astimezone(UTC).strftime("%H%M%S")
    raw_payload = {
        "captured_at": capture_time.astimezone(UTC).isoformat(),
        "symbol": symbol,
        "stock_quote": quote_payload,
        "stock_bars": bars_payload,
        "option_chain_snapshot": option_payload,
        "skipped_option_quote_reason": option_skip_reason,
    }
    (raw_dir / f"{filename}.json").write_text(json.dumps(raw_payload, indent=2, sort_keys=True), encoding="utf-8")
    (snapshot_dir / f"{filename}.json").write_text(
        json.dumps(
            {
                "schema_version": "phase1.alpaca_min_snapshot.v1",
                "source": {
                    "name": "alpaca",
                    "environment": "paper",
                    "corpus_type": "paper_capture",
                },
                "captured_at": capture_time.astimezone(UTC).isoformat(),
                "ticker": symbol,
                "timestamp": capture_time.astimezone(UTC).isoformat(),
                "underlying_quote": quote_payload,
                "market_bars": bars_payload,
                "option_chain_snapshot_available": bool(option_payload),
                "skipped_option_quote_reason": option_skip_reason,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    if option_payload:
        (option_dir / f"{filename}.json").write_text(json.dumps(option_payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_manifest(
    *,
    root: Path,
    symbol: str,
    capture_started: datetime,
    capture_finished: datetime,
    interval_seconds: int,
    snapshots_captured: int,
    option_quotes_captured: int,
    skipped_option_quote_reason: str | None,
    data_quality_flags: list[str],
    active_gate_mutated: bool,
    order_placement_enabled: bool,
) -> Path:
    trading_date = capture_started.astimezone(UTC).date().isoformat()
    symbol_dir = root / trading_date / symbol
    raw_dir = symbol_dir / "raw"
    snapshot_dir = symbol_dir / "snapshots"
    option_dir = symbol_dir / "option_quotes"
    manifest_path = symbol_dir / "manifest.json"
    manifest = {
        "schema_version": "phase1_alpaca_capture_manifest.v1",
        "source": "alpaca",
        "corpus_type": "paper_capture",
        "symbol": symbol,
        "trading_date": trading_date,
        "capture_start": capture_started.astimezone(UTC).isoformat(),
        "capture_end": capture_finished.astimezone(UTC).isoformat(),
        "interval_seconds": interval_seconds,
        "snapshots_captured": snapshots_captured,
        "option_quotes_captured": option_quotes_captured,
        "skipped_option_quote_reason": skipped_option_quote_reason,
        "data_quality_flags": data_quality_flags,
        "active_gate_mutated": active_gate_mutated,
        "order_placement_enabled": order_placement_enabled,
        "file_hashes": _file_hashes(symbol_dir, list(raw_dir.glob("*.json")) + list(snapshot_dir.glob("*.json")) + list(option_dir.glob("*.json"))),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest_path


def _file_hash(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _file_hashes(base_dir: Path, paths: list[Path]) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for path in sorted(paths):
        relative = path.relative_to(base_dir).as_posix()
        hashes[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return hashes


def _current_time(now_fn: Any | None) -> datetime:
    if now_fn is None:
        return datetime.now(UTC)
    return now_fn()


if __name__ == "__main__":
    raise SystemExit(main())
