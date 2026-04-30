"""Regression tests for scanner profile matching on volatile intraday candidates."""

from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz


_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

from scanner import _profile_signals_for_candidate  # noqa: E402


def _bars() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": [100.0, 100.02, 100.03],
            "high": [100.05, 100.06, 100.07],
            "low": [99.98, 100.0, 100.01],
            "close": [100.0, 100.02, 100.04],
            "volume": [1000, 1200, 1400],
        }
    )


def _now() -> datetime:
    return pytz.timezone("US/Eastern").localize(datetime(2026, 4, 30, 10, 1))


class ScannerProfileMatchingTests(unittest.TestCase):
    def test_named_profile_accepts_small_but_real_intraday_push(self):
        signal = {
            "symbol": "SPY",
            "direction": "call",
            "direction_score": 0.10,
            "rvol": 0.08,
            "atr_pct": 0.30,
            "roc": 0.007,
            "price": 100.01,
            "vwap": 100.0,
            "signal_score": 2.0,
            "volatility_score": 1.0,
            "flat_regime": False,
            "reason": "test signal",
        }

        passed, rejected = _profile_signals_for_candidate(
            base_signal=signal,
            bars_df=_bars(),
            now_et=_now(),
            catalyst_mode_active=False,
        )

        self.assertTrue(passed, rejected)

    def test_generic_profile_catches_volatile_core_candidate_when_named_logic_misses(self):
        signal = {
            "symbol": "SPY",
            "direction": "call",
            "direction_score": 0.10,
            "rvol": 0.04,
            "atr_pct": 0.40,
            "roc": 0.001,
            "price": 100.001,
            "vwap": 100.0,
            "signal_score": 2.0,
            "volatility_score": 1.2,
            "flat_regime": False,
            "reason": "test signal",
        }

        passed, rejected = _profile_signals_for_candidate(
            base_signal=signal,
            bars_df=_bars(),
            now_et=_now(),
            catalyst_mode_active=False,
        )

        self.assertTrue(passed, rejected)
        self.assertEqual(passed[0]["strategy_profile"], "generic_intraday_continuation")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()