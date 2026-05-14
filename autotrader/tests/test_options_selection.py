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
import options  # noqa: E402


EASTERN = pytz.timezone(config.EASTERN_TZ)


class FakeOptionData:
    def __init__(self, contracts, quotes):
        self.contracts = contracts
        self.quotes = quotes

    def get_option_contracts(self, **_kwargs):
        return list(self.contracts)

    def get_latest_option_quote(self, symbol: str):
        return dict(self.quotes[symbol])


class OptionSelectionTests(unittest.TestCase):
    def setUp(self):
        self._config_values = {
            "MIN_OPTION_OPEN_INTEREST": config.MIN_OPTION_OPEN_INTEREST,
            "MIN_OPTION_DAILY_VOLUME": config.MIN_OPTION_DAILY_VOLUME,
            "MAX_OPTION_SPREAD_PCT": config.MAX_OPTION_SPREAD_PCT,
            "MAX_OPTION_PREMIUM_TO_UNDERLYING_PCT": config.MAX_OPTION_PREMIUM_TO_UNDERLYING_PCT,
            "MAX_OPTION_STRIKE_DISTANCE_PCT": config.MAX_OPTION_STRIKE_DISTANCE_PCT,
            "ENABLE_DELTA_TARGETING": config.ENABLE_DELTA_TARGETING,
            "RATE_LIMIT_SLEEP_SECONDS": config.RATE_LIMIT_SLEEP_SECONDS,
        }
        config.MIN_OPTION_OPEN_INTEREST = 1
        config.MIN_OPTION_DAILY_VOLUME = 1
        config.MAX_OPTION_SPREAD_PCT = 5.0
        config.MAX_OPTION_PREMIUM_TO_UNDERLYING_PCT = 8.0
        config.MAX_OPTION_STRIKE_DISTANCE_PCT = 8.0
        config.ENABLE_DELTA_TARGETING = False
        config.RATE_LIMIT_SLEEP_SECONDS = 0.0
        self.now = EASTERN.localize(datetime(2026, 5, 14, 9, 44, 0))

    def tearDown(self):
        for key, value in self._config_values.items():
            setattr(config, key, value)

    def test_rejects_absurdly_expensive_atm_quote_and_uses_next_contract(self):
        data = FakeOptionData(
            contracts=[
                {
                    "symbol": "JPM260515C00300000",
                    "expiration_date": "2026-05-15",
                    "strike_price": 300.0,
                    "open_interest": 100,
                    "volume": 50,
                },
                {
                    "symbol": "JPM260515C00301000",
                    "expiration_date": "2026-05-15",
                    "strike_price": 301.0,
                    "open_interest": 100,
                    "volume": 50,
                },
            ],
            quotes={
                "JPM260515C00300000": {"bid": 29.8, "ask": 30.0},
                "JPM260515C00301000": {"bid": 5.9, "ask": 6.0},
            },
        )

        contract, reason = options.select_atm_option_contract_with_reason(
            data_client=data,
            underlying_symbol="JPM",
            direction="call",
            underlying_price=300.0,
            now_et=self.now,
        )

        self.assertEqual(reason, "ok(strict)")
        self.assertEqual(contract["symbol"], "JPM260515C00301000")

    def test_rejects_far_itm_contract_when_underlying_quote_is_stale(self):
        data = FakeOptionData(
            contracts=[
                {
                    "symbol": "CRM260515P00230000",
                    "expiration_date": "2026-05-15",
                    "strike_price": 230.0,
                    "open_interest": 100,
                    "volume": 50,
                }
            ],
            quotes={
                "CRM260515P00230000": {"bid": 63.4, "ask": 63.6},
            },
        )

        contract, reason = options.select_atm_option_contract_with_reason(
            data_client=data,
            underlying_symbol="CRM",
            direction="put",
            underlying_price=165.0,
            now_et=self.now,
        )

        self.assertIsNone(contract)
        self.assertIn("strike_too_far=1", reason)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
