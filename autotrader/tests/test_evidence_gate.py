"""Regression tests for conservative-P/L evidence gating."""

from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path

import pytz

_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

import config  # noqa: E402
import evidence_gate  # noqa: E402


EASTERN = pytz.timezone(config.EASTERN_TZ)


class EvidenceGateTests(unittest.TestCase):
    def setUp(self):
        self.original = {
            "ENABLE_EXECUTION_EVIDENCE_GATE": config.ENABLE_EXECUTION_EVIDENCE_GATE,
            "EVIDENCE_GATE_MIN_SAMPLES": config.EVIDENCE_GATE_MIN_SAMPLES,
            "EVIDENCE_GATE_MIN_LOSSES": config.EVIDENCE_GATE_MIN_LOSSES,
            "EVIDENCE_GATE_MAX_CONSERVATIVE_EXPECTANCY_USD": config.EVIDENCE_GATE_MAX_CONSERVATIVE_EXPECTANCY_USD,
        }
        config.ENABLE_EXECUTION_EVIDENCE_GATE = True
        config.EVIDENCE_GATE_MIN_SAMPLES = 3
        config.EVIDENCE_GATE_MIN_LOSSES = 2
        config.EVIDENCE_GATE_MAX_CONSERVATIVE_EXPECTANCY_USD = -0.01
        self.now = EASTERN.localize(datetime(2026, 5, 18, 10, 15, 0))

    def tearDown(self):
        for key, value in self.original.items():
            setattr(config, key, value)

    def test_signal_gate_blocks_losing_ticker_hour_direction_bucket(self):
        rows = [
            {
                "ticker": "QQQ",
                "direction": "put",
                "strategy_profile": "generic",
                "entry_hour": "10",
                "score_bucket": "[7+)",
                "conservative_pnl": -20.0,
            },
            {
                "ticker": "QQQ",
                "direction": "put",
                "strategy_profile": "generic",
                "entry_hour": "10",
                "score_bucket": "[7+)",
                "conservative_pnl": -15.0,
            },
            {
                "ticker": "QQQ",
                "direction": "put",
                "strategy_profile": "generic",
                "entry_hour": "10",
                "score_bucket": "[7+)",
                "conservative_pnl": 5.0,
            },
        ]

        decision = evidence_gate.evaluate_signal(
            signal={"signal_score": 8.0, "strategy_profile": "generic"},
            ticker="QQQ",
            direction="put",
            now_et=self.now,
            rows=rows,
        )

        self.assertFalse(decision.allowed)
        self.assertIn("conservative_exp=$-10.00", decision.reason)

    def test_contract_gate_blocks_losing_spread_score_direction_bucket(self):
        rows = [
            {
                "ticker": "SPY",
                "direction": "call",
                "strategy_profile": "generic",
                "entry_hour": "10",
                "score_bucket": "[7+)",
                "spread_bucket": "2-3",
                "exposure_bucket": "0dte_index_etf",
                "conservative_pnl": -12.0,
            },
            {
                "ticker": "QQQ",
                "direction": "call",
                "strategy_profile": "generic",
                "entry_hour": "10",
                "score_bucket": "[7+)",
                "spread_bucket": "2-3",
                "exposure_bucket": "0dte_index_etf",
                "conservative_pnl": -8.0,
            },
            {
                "ticker": "IWM",
                "direction": "call",
                "strategy_profile": "generic",
                "entry_hour": "10",
                "score_bucket": "[7+)",
                "spread_bucket": "2-3",
                "exposure_bucket": "0dte_index_etf",
                "conservative_pnl": -4.0,
            },
        ]

        decision = evidence_gate.evaluate_contract(
            signal={"signal_score": 8.0, "strategy_profile": "generic"},
            ticker="SPY",
            direction="call",
            now_et=self.now,
            exposure_bucket="0dte_index_etf",
            spread_pct=2.5,
            rows=rows,
        )

        self.assertFalse(decision.allowed)
        self.assertIn("contract bucket", decision.reason)

    def test_gate_fails_open_without_enough_samples(self):
        rows = [
            {
                "ticker": "QQQ",
                "direction": "put",
                "strategy_profile": "generic",
                "entry_hour": "10",
                "score_bucket": "[7+)",
                "conservative_pnl": -50.0,
            }
        ]

        decision = evidence_gate.evaluate_signal(
            signal={"signal_score": 8.0, "strategy_profile": "generic"},
            ticker="QQQ",
            direction="put",
            now_et=self.now,
            rows=rows,
        )

        self.assertTrue(decision.allowed)


if __name__ == "__main__":
    unittest.main()
