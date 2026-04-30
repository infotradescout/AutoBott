"""Regression tests for scanner dashboard display data."""

from __future__ import annotations

import csv
import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path


_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

os.environ.setdefault("ALPACA_API_KEY", "test_key")
os.environ.setdefault("ALPACA_SECRET_KEY", "test_secret")
os.environ["DASHBOARD_DISPLAY_TZ"] = "America/Chicago"
os.environ.pop("DASHBOARD_DISPLAY_TZ_LABEL", None)

if "dashboard" in sys.modules:
    dashboard = importlib.reload(sys.modules["dashboard"])
else:
    import dashboard  # noqa: E402


class DashboardScannerDisplayTests(unittest.TestCase):
    def test_to_ct_label_uses_daylight_aware_central_label(self):
        parsed = dashboard._parse_ts("2026-04-30 09:33:12 EDT")

        self.assertEqual(dashboard._to_ct_label(parsed), "2026-04-30 08:33:12 CDT")

    def test_scanlog_api_returns_display_timezone_timestamps(self):
        old_scan_log = dashboard.SCAN_LOG_CSV
        old_load_state = dashboard.load_bot_state
        dashboard._HEAVY_API_CACHE.clear()

        with tempfile.TemporaryDirectory() as tmp:
            scan_log = Path(tmp) / "scan_log.csv"
            columns = [
                "timestamp",
                "symbol",
                "strategy_profile",
                "result",
                "direction",
                "rvol",
                "rsi",
                "roc",
                "iv_rank",
                "volatility_score",
                "regime_score",
                "signal_score",
                "flow_score",
                "htf_reason",
                "reason",
            ]
            with scan_log.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()
                writer.writerow(
                    {
                        "timestamp": "2026-04-30 09:33:12 EDT",
                        "symbol": "INTC",
                        "strategy_profile": "balanced",
                        "result": "pass",
                        "direction": "put",
                        "rvol": "3.59",
                        "rsi": "50.0",
                        "roc": "-0.2",
                        "iv_rank": "",
                        "volatility_score": "1",
                        "regime_score": "1",
                        "signal_score": "4.2",
                        "flow_score": "",
                        "htf_reason": "",
                        "reason": "RVOL 3.6x | Below VWAP",
                    }
                )

            try:
                dashboard.SCAN_LOG_CSV = scan_log
                dashboard.load_bot_state = lambda: {}
                response = dashboard.app.test_client().get("/api/scanlog")
                self.assertEqual(response.status_code, 200)
                payload = response.get_json()
            finally:
                dashboard.SCAN_LOG_CSV = old_scan_log
                dashboard.load_bot_state = old_load_state
                dashboard._HEAVY_API_CACHE.clear()

        self.assertEqual(payload[0]["timestamp"], "2026-04-30 08:33:12 CDT")
        self.assertEqual(payload[0]["final_state"], "setup_pass")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
