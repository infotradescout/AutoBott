"""Regression tests for the historical replay trainer."""

from __future__ import annotations

import csv
import json
import math
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz

_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

import config  # noqa: E402
import historical_replay  # noqa: E402
import scanner  # noqa: E402


EASTERN = pytz.timezone(config.EASTERN_TZ)


def _sample_bars() -> pd.DataFrame:
    rows = [
        ("2026-05-04 09:30", 100.00, 100.50, 99.80, 100.10, 250_000),
        ("2026-05-04 09:35", 100.20, 101.20, 100.10, 101.00, 300_000),
        ("2026-05-04 09:40", 101.00, 102.00, 100.80, 101.70, 350_000),
        ("2026-05-04 09:45", 101.70, 102.20, 101.50, 102.00, 325_000),
        ("2026-05-04 09:50", 102.00, 102.50, 101.90, 102.30, 310_000),
    ]
    return pd.DataFrame(
        [
            {
                "timestamp": EASTERN.localize(datetime.strptime(ts_text, "%Y-%m-%d %H:%M")),
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
            for ts_text, open_, high, low, close, volume in rows
        ]
    )


class HistoricalReplayTests(unittest.TestCase):
    def test_scanner_indicators_accept_short_daily_context(self):
        rows = []
        for idx in range(8):
            rows.append(
                {
                    "timestamp": EASTERN.localize(datetime(2026, 4, 20 + idx)),
                    "open": 100 + idx,
                    "high": 102 + idx,
                    "low": 99 + idx,
                    "close": 101 + idx,
                    "volume": 1_000_000 + (idx * 25_000),
                }
            )
        daily = pd.DataFrame(rows)

        old_values = {
            "SCAN_MIN_DAILY_BARS": config.SCAN_MIN_DAILY_BARS,
            "RVOL_AVG_DAILY_BARS": config.RVOL_AVG_DAILY_BARS,
            "ATR_PERIOD": config.ATR_PERIOD,
            "ATR_MIN_PERIOD": config.ATR_MIN_PERIOD,
        }
        try:
            config.SCAN_MIN_DAILY_BARS = 8
            config.RVOL_AVG_DAILY_BARS = 8
            config.ATR_PERIOD = 7
            config.ATR_MIN_PERIOD = 4

            rvol = scanner.calculate_rvol("AAPL", 250_000, daily, minutes_since_open=60)
            atr = scanner.calculate_atr("AAPL", daily)
        finally:
            for key, value in old_values.items():
                setattr(config, key, value)

        self.assertFalse(math.isnan(rvol))
        self.assertFalse(math.isnan(atr))

    def test_simulate_outcome_scores_directional_call_win(self):
        result = historical_replay._simulate_outcome(
            bars=_sample_bars(),
            direction="call",
            entry_time=EASTERN.localize(datetime(2026, 5, 4, 9, 35)),
            horizon_minutes=15,
            take_profit_pct=0.5,
            stop_loss_pct=0.25,
        )

        self.assertTrue(result["evaluated"])
        self.assertEqual(result["verdict"], "win")
        self.assertEqual(result["entry_price"], 101.0)
        self.assertGreaterEqual(result["max_favorable_pct"], 0.5)

    def test_write_rows_handles_empty_and_mixed_outcome_columns(self):
        with tempfile.TemporaryDirectory() as tmp:
            empty_path = Path(tmp) / "empty.csv"
            mixed_path = Path(tmp) / "mixed.csv"

            historical_replay._write_rows(empty_path, [])
            historical_replay._write_rows(
                mixed_path,
                [
                    {
                        "timestamp": "2026-05-04T09:35:00-04:00",
                        "symbol": "AAPL",
                        "direction": "call",
                        "evaluated": False,
                        "verdict": "no_future_bars",
                    },
                    {
                        "timestamp": "2026-05-04T09:40:00-04:00",
                        "symbol": "AAPL",
                        "direction": "call",
                        "evaluated": True,
                        "verdict": "win",
                        "entry_price": 101.0,
                        "exit_price": 101.505,
                        "extra_metric": "kept",
                    },
                ],
            )

            with empty_path.open("r", newline="", encoding="utf-8") as handle:
                empty_reader = csv.DictReader(handle)
                self.assertEqual(empty_reader.fieldnames, historical_replay.REPLAY_RESULT_COLUMNS)
                self.assertEqual(list(empty_reader), [])

            with mixed_path.open("r", newline="", encoding="utf-8") as handle:
                mixed_reader = csv.DictReader(handle)
                rows = list(mixed_reader)

        self.assertIn("entry_price", mixed_reader.fieldnames or [])
        self.assertIn("extra_metric", mixed_reader.fieldnames or [])
        self.assertEqual(rows[0]["entry_price"], "")
        self.assertEqual(rows[1]["entry_price"], "101.0")
        self.assertEqual(rows[1]["extra_metric"], "kept")

    def test_load_or_fetch_bars_prefers_superset_cache_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "historical_cache"
            cache_dir.mkdir()
            rows = []
            for day in range(1, 16):
                rows.append(
                    {
                        "timestamp": historical_replay.EASTERN.localize(datetime(2026, 1, day, 9, 30)),
                        "open": 100 + day,
                        "high": 102 + day,
                        "low": 99 + day,
                        "close": 101 + day,
                        "volume": 1_000_000 + day,
                    }
                )
            wide_path = cache_dir / "AAPL_5m_2026-01-01_2026-01-31.csv"
            import csv

            with wide_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
                writer.writeheader()
                for row in rows:
                    writer.writerow({key: row[key] for key in writer.fieldnames})

            cfg = historical_replay.ReplayConfig(
                symbols=["AAPL"],
                start="2026-01-05",
                end="2026-01-10",
                interval="5m",
                scan_every_minutes=5,
                horizon_minutes=45,
                take_profit_pct=0.35,
                stop_loss_pct=0.2,
                max_signals_per_scan=2,
                output=Path(tmp) / "replay.csv",
                cache_dir=cache_dir,
                daily_lookback_days=90,
                min_daily_bars=5,
                offline=True,
            )
            rows = historical_replay._load_or_fetch_bars("AAPL", cfg)

            self.assertEqual(len(rows), 5)
            self.assertEqual(rows.iloc[0]["timestamp"].date(), datetime(2026, 1, 5).date())
            self.assertEqual(rows.iloc[-1]["timestamp"].date(), datetime(2026, 1, 9).date())

    def test_load_or_fetch_bars_requires_coverage_for_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "historical_cache"
            cache_dir.mkdir()
            rows = [
                {
                    "timestamp": historical_replay.EASTERN.localize(datetime(2026, 1, 15, 9, 30)),
                    "open": 100.0,
                    "high": 102.0,
                    "low": 99.0,
                    "close": 101.0,
                    "volume": 1_000_000,
                }
            ]
            cache_path = cache_dir / "AAPL_5m_2026-01-01_2026-01-31.csv"
            import csv

            with cache_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
                writer.writeheader()
                for row in rows:
                    writer.writerow({key: row[key] for key in writer.fieldnames})
            cfg = historical_replay.ReplayConfig(
                symbols=["AAPL"],
                start="2026-02-01",
                end="2026-02-20",
                interval="5m",
                scan_every_minutes=5,
                horizon_minutes=45,
                take_profit_pct=0.35,
                stop_loss_pct=0.2,
                max_signals_per_scan=2,
                output=Path(tmp) / "replay.csv",
                cache_dir=cache_dir,
                daily_lookback_days=90,
                min_daily_bars=5,
                offline=True,
            )
            with self.assertRaisesRegex(FileNotFoundError, "Missing cache file"):
                historical_replay._load_or_fetch_bars("AAPL", cfg)

    def test_scanner_quiet_mode_does_not_write_scan_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            old_scan_log_path = scanner.SCAN_LOG_PATH
            scan_log_path = Path(tmp) / "scan_log.csv"
            scanner.SCAN_LOG_PATH = scan_log_path
            try:
                quiet_scanner = scanner.IntradayScanner(
                    object(),  # type: ignore[arg-type]
                    emit_summary=False,
                    write_scan_log=False,
                )
                quiet_scanner._print_summary(
                    EASTERN.localize(datetime(2026, 5, 4, 9, 35)),
                    total=1,
                    passed=[],
                    failed=[{"symbol": "AAPL", "reason": "setup_reject: test"}],
                )

                self.assertFalse(scan_log_path.exists())

                logging_scanner = scanner.IntradayScanner(
                    object(),  # type: ignore[arg-type]
                    emit_summary=False,
                    write_scan_log=True,
                )
                logging_scanner._print_summary(
                    EASTERN.localize(datetime(2026, 5, 4, 9, 40)),
                    total=1,
                    passed=[],
                    failed=[{"symbol": "AAPL", "reason": "setup_reject: test"}],
                )

                self.assertTrue(scan_log_path.exists())
            finally:
                scanner.SCAN_LOG_PATH = old_scan_log_path

    def test_run_replay_uses_quiet_scanner_and_restores_rate_limit(self):
        captured: dict[str, object] = {}

        class FakeScanner:
            def __init__(self, data_client, **kwargs):
                captured["scanner_kwargs"] = kwargs
                self.data_client = data_client

            def run_scan(self, watchlist, *, now_et=None, premarket_mode=False):
                captured.setdefault("scan_times", []).append(now_et)
                return [
                    {
                        "symbol": "AAPL",
                        "direction": "call",
                        "strategy_profile": "unit_test_profile",
                        "signal_score": 9.1,
                        "direction_score": 0.8,
                        "rvol": 2.5,
                        "roc": 0.4,
                        "rsi": 61.0,
                        "volatility_score": 8.7,
                        "reason": "unit test signal",
                    }
                ]

        old_loader = historical_replay._load_or_fetch_bars
        old_daily_loader = historical_replay._load_or_fetch_daily_bars
        old_scanner = historical_replay.IntradayScanner
        old_sleep = config.RATE_LIMIT_SLEEP_SECONDS
        try:
            historical_replay._load_or_fetch_bars = lambda symbol, cfg: _sample_bars()
            historical_replay._load_or_fetch_daily_bars = lambda symbol, cfg: pd.DataFrame()
            historical_replay.IntradayScanner = FakeScanner  # type: ignore[assignment]
            config.RATE_LIMIT_SLEEP_SECONDS = 1.234

            with tempfile.TemporaryDirectory() as tmp:
                cfg = historical_replay.ReplayConfig(
                    symbols=["AAPL"],
                    start="2026-05-04",
                    end="2026-05-05",
                    interval="5m",
                    scan_every_minutes=60,
                    horizon_minutes=15,
                    take_profit_pct=0.5,
                    stop_loss_pct=0.25,
                    max_signals_per_scan=1,
                    output=Path(tmp) / "replay.csv",
                    cache_dir=Path(tmp) / "cache",
                    daily_lookback_days=90,
                    min_daily_bars=5,
                )

                result = historical_replay.run_replay(cfg)
                output_path = Path(result["output"])
                summary_path = Path(result["summary_path"])

                self.assertTrue(output_path.exists())
                self.assertTrue(summary_path.exists())
                self.assertEqual(json.loads(summary_path.read_text(encoding="utf-8")), result["summary"])

                with output_path.open("r", newline="", encoding="utf-8") as handle:
                    rows = list(csv.DictReader(handle))

            self.assertEqual(captured["scanner_kwargs"], {"emit_summary": False, "write_scan_log": False})
            self.assertEqual(config.RATE_LIMIT_SLEEP_SECONDS, 1.234)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["symbol"], "AAPL")
            self.assertEqual(rows[0]["verdict"], "win")
        finally:
            historical_replay._load_or_fetch_bars = old_loader
            historical_replay._load_or_fetch_daily_bars = old_daily_loader
            historical_replay.IntradayScanner = old_scanner
            config.RATE_LIMIT_SLEEP_SECONDS = old_sleep


    def test_run_replay_offline_requires_cached_bars(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = historical_replay.ReplayConfig(
                symbols=["AAPL"],
                start="2026-05-04",
                end="2026-05-05",
                interval="5m",
                scan_every_minutes=60,
                horizon_minutes=15,
                take_profit_pct=0.5,
                stop_loss_pct=0.25,
                max_signals_per_scan=1,
                output=Path(tmp) / "replay.csv",
                cache_dir=Path(tmp) / "cache",
                daily_lookback_days=90,
                min_daily_bars=5,
                offline=True,
            )

            with self.assertRaisesRegex(FileNotFoundError, "requires cached bars"):
                historical_replay.run_replay(cfg)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
