"""Unit tests for intraday profile activation and window logic."""

from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path

import pytz

# Allow `python -m unittest` from the repo root to import `intraday_profiles`.
_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

from intraday_profiles import (  # noqa: E402
    PROFILES,
    IntradayProfile,
    is_profile_window_open,
)

ET = pytz.timezone("US/Eastern")


def _et(hour: int, minute: int = 0) -> datetime:
    # 2026-04-28 is a Tuesday — a normal trading day.
    return ET.localize(datetime(2026, 4, 28, hour, minute))


class IntradayProfilesShapeTests(unittest.TestCase):
    def test_required_profiles_exist(self):
        for name in (
            "open_drive_momentum",
            "vwap_continuation",
            "reversal_snapback",
            "catalyst_impulse",
            "generic_intraday_continuation",
        ):
            self.assertIn(name, PROFILES, f"missing profile {name!r}")
            self.assertIsInstance(PROFILES[name], IntradayProfile)

    def test_priority_order_is_unique_and_sorted(self):
        priorities = [p.priority for p in PROFILES.values()]
        self.assertEqual(len(priorities), len(set(priorities)), "duplicate priorities")
        # Generic fallback must have the lowest priority (largest number).
        self.assertEqual(
            PROFILES["generic_intraday_continuation"].priority,
            max(priorities),
            "generic profile must be lowest priority (fallback)",
        )

    def test_all_profiles_have_valid_windows(self):
        for name, profile in PROFILES.items():
            self.assertRegex(profile.window_start, r"^\d{2}:\d{2}$", f"{name} window_start")
            self.assertRegex(profile.window_end, r"^\d{2}:\d{2}$", f"{name} window_end")
            self.assertGreater(profile.max_hold_minutes, 0)
            self.assertGreaterEqual(profile.min_signal_score, 0.0)


class WindowOpenTests(unittest.TestCase):
    def test_open_drive_window_open_at_open(self):
        self.assertTrue(is_profile_window_open(_et(9, 30), PROFILES["open_drive_momentum"]))

    def test_open_drive_window_closed_after_1130(self):
        self.assertFalse(is_profile_window_open(_et(11, 30), PROFILES["open_drive_momentum"]))
        self.assertFalse(is_profile_window_open(_et(13, 0), PROFILES["open_drive_momentum"]))

    def test_vwap_continuation_window_spans_session(self):
        profile = PROFILES["vwap_continuation"]
        self.assertTrue(is_profile_window_open(_et(10, 0), profile))
        self.assertTrue(is_profile_window_open(_et(15, 29), profile))
        self.assertFalse(is_profile_window_open(_et(15, 30), profile))

    def test_generic_fallback_window_spans_full_session(self):
        profile = PROFILES["generic_intraday_continuation"]
        self.assertTrue(is_profile_window_open(_et(9, 30), profile))
        self.assertTrue(is_profile_window_open(_et(15, 59), profile))
        self.assertFalse(is_profile_window_open(_et(16, 0), profile))

    def test_invalid_window_strings_default_to_open(self):
        bad = IntradayProfile(
            name="bad",
            window_start="oops",
            window_end="??",
            symbols=(),
            entry_max_quote_spread_pct=10.0,
            stop_loss_usd=10.0,
            immediate_take_profit_pct=0.05,
            max_hold_minutes=30,
            min_signal_score=4.0,
            priority=99,
        )
        self.assertTrue(is_profile_window_open(_et(12, 0), bad))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
