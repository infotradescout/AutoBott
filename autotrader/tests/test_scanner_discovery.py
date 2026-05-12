from datetime import datetime
from pathlib import Path
import csv
import sys
import tempfile

import pandas as pd
import pytz

_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

import scanner  # noqa: E402


def test_generic_discovery_profile_accepts_non_core_mover_symbol():
    now_et = pytz.timezone("US/Eastern").localize(datetime(2026, 5, 12, 11, 30))
    bars = pd.DataFrame({"close": [100.0, 100.05, 100.10, 100.14, 100.18]})
    signal = {
        "symbol": "XYZ",
        "direction": "call",
        "direction_score": 0.25,
        "signal_score": 3.2,
        "rvol": 0.2,
        "roc": 0.04,
        "price": 100.18,
        "vwap": 100.05,
        "reason": "discovery candidate",
    }

    passed, rejected = scanner._profile_signals_for_candidate(
        base_signal=signal,
        bars_df=bars,
        now_et=now_et,
        catalyst_mode_active=False,
    )

    assert rejected
    assert len(passed) == 1
    assert passed[0]["symbol"] == "XYZ"
    assert passed[0]["strategy_profile"] == "generic_intraday_continuation"


def test_scanner_logs_candidates_not_trade_passes():
    now_et = pytz.timezone("US/Eastern").localize(datetime(2026, 5, 12, 11, 30))
    with tempfile.TemporaryDirectory() as tmp:
        old_path = scanner.SCAN_LOG_PATH
        scanner.SCAN_LOG_PATH = Path(tmp) / "scan_log.csv"
        try:
            scan = scanner.IntradayScanner(object(), emit_summary=False, write_scan_log=True)
            scan._write_scan_log(
                now_et,
                passed=[
                    {
                        "symbol": "XYZ",
                        "strategy_profile": "generic_intraday_continuation",
                        "direction": "call",
                        "rvol": 0.2,
                        "rsi": 50,
                        "roc": 0.04,
                        "signal_score": 3.2,
                        "reason": "candidate only",
                    }
                ],
                failed=[],
            )
            with scanner.SCAN_LOG_PATH.open("r", newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            assert rows[0]["result"] == "candidate"
        finally:
            scanner.SCAN_LOG_PATH = old_path
