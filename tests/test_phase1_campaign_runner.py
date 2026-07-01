from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from autobott_v2.phase1_campaign_runner import run_phase1_campaign
from autobott_v2.phase1_replay_campaign import main as replay_campaign_main
from autobott_v2.phase1_snapshot_capture import write_snapshot_day_manifest


BASE_TIME = datetime(2026, 6, 29, 15, 20, tzinfo=timezone.utc)


def _bar(index: int, base_time: datetime, start: float = 200.0, step: float = 0.45) -> dict[str, object]:
    close = start + index * step
    return {
        "timestamp": (base_time - timedelta(minutes=34 - index)).isoformat(),
        "open": close - 0.05,
        "high": close + 0.25,
        "low": close - 0.25,
        "close": close,
        "volume": 1000 + index * 10,
    }


def _bars(base_time: datetime, start: float = 200.0, step: float = 0.45) -> list[dict[str, object]]:
    return [_bar(index, base_time, start, step) for index in range(35)]


def _snapshot(
    timestamp: datetime,
    *,
    ticker: str = "SPY",
    tactical_bid: float = 4.9,
    tactical_ask: float = 5.1,
    rider_bid: float = 4.9,
    rider_ask: float = 5.1,
    quote_age_seconds: int = 0,
) -> dict[str, object]:
    quote_timestamp = (timestamp - timedelta(seconds=quote_age_seconds)).isoformat()
    return {
        "schema_version": "phase1.snapshot.v1",
        "source": {
            "name": "deterministic_fixture",
            "environment": "test",
            "latency_assumption": "retail_api_latency",
        },
        "captured_at": timestamp.isoformat(),
        "market_timezone": "America/New_York",
        "timestamp_utc": timestamp.isoformat().replace("+00:00", "Z"),
        "timestamp_market": timestamp.astimezone(timezone(timedelta(hours=-4))).isoformat(),
        "ticker": ticker,
        "timestamp": timestamp.isoformat(),
        "underlying_quote": {
            "symbol": ticker,
            "bid": 599.9,
            "ask": 600.1,
            "last": 600.0,
            "spread": 0.2,
            "spread_pct": 0.0003,
            "quote_timestamp": quote_timestamp,
        },
        "market_bars": _bars(timestamp, 600.0, 0.25),
        "option_chain": [
            {
                "option_symbol": f"{ticker}260701C00600000",
                "underlying": ticker,
                "expiration": "2026-07-01",
                "strike": 600.0,
                "option_type": "call",
                "bid": tactical_bid,
                "ask": tactical_ask,
                "last": round((tactical_bid + tactical_ask) / 2, 4),
                "spread": round(tactical_ask - tactical_bid, 4),
                "spread_pct": round((tactical_ask - tactical_bid) / ((tactical_bid + tactical_ask) / 2), 4),
                "quote_timestamp": quote_timestamp,
                "volume": 250,
                "open_interest": 900,
                "delta": 0.56,
                "theta": -0.04,
                "vega": 0.08,
                "implied_volatility": 0.01,
                "iv_percentile": 0.25,
                "realized_volatility": 0.01,
            },
            {
                "option_symbol": f"{ticker}260718C00600000",
                "underlying": ticker,
                "expiration": "2026-07-18",
                "strike": 600.0,
                "option_type": "call",
                "bid": rider_bid,
                "ask": rider_ask,
                "last": round((rider_bid + rider_ask) / 2, 4),
                "spread": round(rider_ask - rider_bid, 4),
                "spread_pct": round((rider_ask - rider_bid) / ((rider_bid + rider_ask) / 2), 4),
                "quote_timestamp": quote_timestamp,
                "volume": 250,
                "open_interest": 1200,
                "delta": 0.48,
                "theta": -0.04,
                "vega": 0.08,
                "implied_volatility": 0.01,
                "iv_percentile": 0.25,
                "realized_volatility": 0.01,
            },
        ],
        "context": {
            "spy_bars": _bars(timestamp, 600.0, 0.20),
            "qqq_bars": _bars(timestamp, 520.0, 0.15),
            "vix_bars": _bars(timestamp, 16.0, -0.02),
            "blackout_event": False,
            "event_labels": [],
        },
        "iv_history": [0.01, 0.02, 0.03, 0.04],
        "cycle_profile": {
            "expected_holding_days": 6,
            "cycle_confidence": "high",
            "last_pivot_type": "unknown",
        },
    }


def _write_corpus_day(
    root,
    *,
    trading_date: str = "2026-06-29",
    symbol: str = "SPY",
    option_quote_count: int | None = None,
    data_quality_flags: list[str] | None = None,
    corpus_type: str = "test_fixture",
    quote_age_seconds: int = 0,
):
    symbol_dir = root / trading_date / symbol
    snapshots_dir = symbol_dir / "snapshots"
    option_quotes_dir = symbol_dir / "option_quotes"
    snapshots_dir.mkdir(parents=True)
    option_quotes_dir.mkdir(parents=True)
    timestamps = [BASE_TIME, BASE_TIME + timedelta(minutes=1), BASE_TIME + timedelta(minutes=2)]
    payloads = [
        _snapshot(timestamps[0], ticker=symbol, quote_age_seconds=quote_age_seconds),
        _snapshot(timestamps[1], ticker=symbol, tactical_bid=6.67, tactical_ask=6.85, rider_bid=6.2, rider_ask=6.4, quote_age_seconds=quote_age_seconds),
        _snapshot(timestamps[2], ticker=symbol, tactical_bid=6.6, tactical_ask=6.8, rider_bid=6.3, rider_ask=6.5, quote_age_seconds=quote_age_seconds),
    ]
    for timestamp, payload in zip(timestamps, payloads):
        (snapshots_dir / f"{timestamp.strftime('%H%M%S')}.json").write_text(json.dumps(payload), encoding="utf-8")
    total_option_quotes = len(payloads) if option_quote_count is None else option_quote_count
    for timestamp in timestamps[:total_option_quotes]:
        (option_quotes_dir / f"{timestamp.strftime('%H%M%S')}.json").write_text(json.dumps({"captured_at": timestamp.isoformat()}), encoding="utf-8")
    write_snapshot_day_manifest(
        symbol_dir,
        trading_date=trading_date,
        symbol=symbol,
        source="alpaca",
        capture_interval_seconds=60,
        corpus_type=corpus_type,
        data_quality_flags=data_quality_flags,
    )


def test_campaign_runner_requires_nonempty_snapshot_corpus(tmp_path) -> None:
    with pytest.raises(ValueError, match="snapshot_corpus_empty"):
        run_phase1_campaign(tmp_path, artifacts_root=tmp_path / "artifacts", campaign_run_id="campaign1")


def test_campaign_runner_blocks_low_quality_corpus(tmp_path) -> None:
    _write_corpus_day(tmp_path, option_quote_count=1)

    with pytest.raises(ValueError, match="snapshot_corpus_not_campaign_ready"):
        run_phase1_campaign(tmp_path, artifacts_root=tmp_path / "artifacts", campaign_run_id="campaign1")


def test_campaign_runner_outputs_no_eligible_buckets_when_sample_too_small(tmp_path) -> None:
    _write_corpus_day(tmp_path)

    run_phase1_campaign(tmp_path, artifacts_root=tmp_path / "artifacts", campaign_run_id="campaign1")
    report = json.loads((tmp_path / "artifacts" / "campaign1" / "gate_candidate_report.json").read_text(encoding="utf-8"))

    assert report["corpus_type"] == "test_fixture"
    assert report["bucket_candidates"]
    assert all(not candidate["eligible_for_paper_forward"] for candidate in report["bucket_candidates"].values())
    assert all(not candidate["eligible_for_live_review"] for candidate in report["bucket_candidates"].values())


def test_campaign_runner_preserves_active_gate(tmp_path) -> None:
    _write_corpus_day(tmp_path)
    active_gate = tmp_path / "active_gate.json"
    active_gate.write_text(json.dumps({"sentinel": True}, sort_keys=True), encoding="utf-8")

    run_phase1_campaign(
        tmp_path,
        artifacts_root=tmp_path / "artifacts",
        campaign_run_id="campaign1",
        active_gate_path=active_gate,
    )

    assert json.loads(active_gate.read_text(encoding="utf-8")) == {"sentinel": True}


def test_production_campaign_summary_reports_data_quality_flags(tmp_path) -> None:
    _write_corpus_day(tmp_path, corpus_type="production_capture", data_quality_flags=["manual_review_recommended"])

    run_phase1_campaign(tmp_path, artifacts_root=tmp_path / "artifacts", campaign_run_id="campaign1")
    summary = (tmp_path / "artifacts" / "campaign1" / "replay_campaign_summary.md").read_text(encoding="utf-8")

    assert "Data quality flags: manual_review_recommended" in summary


def test_stale_quote_rate_above_threshold_blocks_campaign(tmp_path) -> None:
    _write_corpus_day(tmp_path, corpus_type="paper_capture", quote_age_seconds=180)

    with pytest.raises(ValueError, match="snapshot_corpus_not_campaign_ready:stale_quote_rate_above_threshold"):
        run_phase1_campaign(tmp_path, artifacts_root=tmp_path / "artifacts", campaign_run_id="campaign1")


def test_campaign_runner_uses_canonical_cli_path(tmp_path) -> None:
    _write_corpus_day(tmp_path)

    with pytest.raises(SystemExit):
        replay_campaign_main(["--snapshot-corpus", str(tmp_path)])
