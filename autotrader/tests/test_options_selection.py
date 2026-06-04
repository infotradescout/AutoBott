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
        config.MAX_OPTION_PREMIUM_TO_UNDERLYING_PCT = 20.0
        config.MAX_OPTION_STRIKE_DISTANCE_PCT = 15.0
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
                "JPM260515C00300000": {"bid": 74.8, "ask": 75.0},
                "JPM260515C00301000": {"bid": 4.95, "ask": 5.0},
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
                "CRM260515P00230000": {"bid": 70.4, "ask": 70.6},
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
        self.assertIn("contract_quality_strike_too_far=1", reason)

    def test_contract_quality_rejects_wide_spread_contract(self):
        data = FakeOptionData(
            contracts=[
                {
                    "symbol": "AAPL260515C00181000",
                    "expiration_date": "2026-05-15",
                    "strike_price": 181.0,
                    "open_interest": 100,
                    "volume": 50,
                }
            ],
            quotes={
                "AAPL260515C00181000": {"bid": 1.00, "ask": 1.10},
            },
        )

        contract, reason = options.select_atm_option_contract_with_reason(
            data_client=data,
            underlying_symbol="AAPL",
            direction="call",
            underlying_price=180.0,
            now_et=self.now,
        )

        self.assertIsNone(contract)
        self.assertIn("contract_quality_spread_too_wide=1", reason)

    def test_etf_contract_quality_rejects_spread_over_one_point_five(self):
        data = FakeOptionData(
            contracts=[
                {
                    "symbol": "SPY260515C00500000",
                    "expiration_date": "2026-05-15",
                    "strike_price": 500.0,
                    "open_interest": 100,
                    "volume": 50,
                }
            ],
            quotes={
                "SPY260515C00500000": {"bid": 1.00, "ask": 1.02},
            },
        )

        contract, reason = options.select_atm_option_contract_with_reason(
            data_client=data,
            underlying_symbol="SPY",
            direction="call",
            underlying_price=500.0,
            now_et=self.now,
        )

        self.assertIsNone(contract)
        self.assertIn("contract_quality_spread_too_wide=1", reason)

    def test_etf_contract_quality_rejects_far_strike(self):
        data = FakeOptionData(
            contracts=[
                {
                    "symbol": "QQQ260515P00445000",
                    "expiration_date": "2026-05-15",
                    "strike_price": 445.0,
                    "open_interest": 100,
                    "volume": 50,
                }
            ],
            quotes={
                "QQQ260515P00445000": {"bid": 1.00, "ask": 1.01},
            },
        )

        contract, reason = options.select_atm_option_contract_with_reason(
            data_client=data,
            underlying_symbol="QQQ",
            direction="put",
            underlying_price=450.0,
            now_et=self.now,
        )

        self.assertIsNone(contract)
        self.assertIn("contract_quality_strike_too_far=1", reason)

    def test_single_name_contract_quality_rejects_strike_over_two_percent(self):
        data = FakeOptionData(
            contracts=[
                {
                    "symbol": "AMD260515C00113000",
                    "expiration_date": "2026-05-15",
                    "strike_price": 113.0,
                    "open_interest": 100,
                    "volume": 50,
                    "delta": 0.50,
                }
            ],
            quotes={
                "AMD260515C00113000": {"bid": 1.00, "ask": 1.01},
            },
        )

        contract, reason = options.select_atm_option_contract_with_reason(
            data_client=data,
            underlying_symbol="AMD",
            direction="call",
            underlying_price=110.0,
            now_et=self.now,
        )

        self.assertIsNone(contract)
        self.assertIn("contract_quality_strike_too_far=1", reason)

    def test_contract_quality_selects_tight_atm_over_farther_contract(self):
        data = FakeOptionData(
            contracts=[
                {
                    "symbol": "AAPL260515C00185000",
                    "expiration_date": "2026-05-15",
                    "strike_price": 185.0,
                    "open_interest": 300,
                    "volume": 80,
                    "delta": 0.50,
                },
                {
                    "symbol": "AAPL260515C00181000",
                    "expiration_date": "2026-05-15",
                    "strike_price": 181.0,
                    "open_interest": 100,
                    "volume": 50,
                    "delta": 0.50,
                },
            ],
            quotes={
                "AAPL260515C00185000": {"bid": 1.00, "ask": 1.02},
                "AAPL260515C00181000": {"bid": 1.00, "ask": 1.02},
            },
        )

        contract, reason = options.select_atm_option_contract_with_reason(
            data_client=data,
            underlying_symbol="AAPL",
            direction="call",
            underlying_price=180.0,
            now_et=self.now,
        )

        self.assertEqual(reason, "ok(strict)")
        self.assertEqual(contract["symbol"], "AAPL260515C00181000")
        self.assertEqual(contract["contract_quality_reason"], "contract_quality_selected")
        self.assertEqual(contract["selected_contract_rank"], 1)

    def test_etf_contract_quality_selects_tight_atm_over_farther_contract(self):
        data = FakeOptionData(
            contracts=[
                {
                    "symbol": "IWM260515C00202000",
                    "expiration_date": "2026-05-15",
                    "strike_price": 202.0,
                    "open_interest": 300,
                    "volume": 80,
                    "delta": 0.50,
                },
                {
                    "symbol": "IWM260515C00200000",
                    "expiration_date": "2026-05-15",
                    "strike_price": 200.0,
                    "open_interest": 100,
                    "volume": 50,
                    "delta": 0.50,
                },
            ],
            quotes={
                "IWM260515C00202000": {"bid": 1.00, "ask": 1.005},
                "IWM260515C00200000": {"bid": 1.00, "ask": 1.005},
            },
        )

        contract, reason = options.select_atm_option_contract_with_reason(
            data_client=data,
            underlying_symbol="IWM",
            direction="call",
            underlying_price=200.0,
            now_et=self.now,
        )

        self.assertEqual(reason, "ok(strict)")
        self.assertEqual(contract["symbol"], "IWM260515C00200000")

    def test_etf_contract_quality_rejects_stale_quote(self):
        data = FakeOptionData(
            contracts=[
                {
                    "symbol": "SPY260515C00500000",
                    "expiration_date": "2026-05-15",
                    "strike_price": 500.0,
                    "open_interest": 100,
                    "volume": 50,
                }
            ],
            quotes={
                "SPY260515C00500000": {"bid": 1.00, "ask": 1.005, "timestamp": "2026-05-14T09:30:00-04:00"},
            },
        )

        contract, reason = options.select_atm_option_contract_with_reason(
            data_client=data,
            underlying_symbol="SPY",
            direction="call",
            underlying_price=500.0,
            now_et=self.now,
        )

        self.assertIsNone(contract)
        self.assertIn("contract_quality_bad_quote=1", reason)

    def test_contract_quality_allows_missing_liquidity_only_with_excellent_quote(self):
        data = FakeOptionData(
            contracts=[
                {
                    "symbol": "AAPL260515C00181000",
                    "expiration_date": "2026-05-15",
                    "strike_price": 181.0,
                    "open_interest": 0,
                    "volume": 0,
                    "delta": 0.50,
                }
            ],
            quotes={
                "AAPL260515C00181000": {"bid": 1.00, "ask": 1.005},
            },
        )

        contract, reason = options.select_atm_option_contract_with_reason(
            data_client=data,
            underlying_symbol="AAPL",
            direction="call",
            underlying_price=180.0,
            now_et=self.now,
        )

        self.assertEqual(reason, "ok(failopen_liquidity)")
        self.assertEqual(contract["symbol"], "AAPL260515C00181000")

    def test_contract_quality_blocks_late_same_day_non_etf(self):
        data = FakeOptionData(
            contracts=[
                {
                    "symbol": "AMD260514C00110000",
                    "expiration_date": "2026-05-14",
                    "strike_price": 110.0,
                    "open_interest": 100,
                    "volume": 50,
                }
            ],
            quotes={
                "AMD260514C00110000": {"bid": 1.00, "ask": 1.01},
            },
        )
        late = EASTERN.localize(datetime(2026, 5, 14, 13, 45, 0))

        contract, reason = options.select_atm_option_contract_with_reason(
            data_client=data,
            underlying_symbol="AMD",
            direction="call",
            underlying_price=110.0,
            now_et=late,
        )

        self.assertIsNone(contract)
        self.assertIn("contract_quality_late_0dte_block=1", reason)

    def test_contract_quality_allows_late_same_day_etf_when_very_tight(self):
        data = FakeOptionData(
            contracts=[
                {
                    "symbol": "SPY260514C00500000",
                    "expiration_date": "2026-05-14",
                    "strike_price": 500.0,
                    "open_interest": 10,
                    "volume": 10,
                }
            ],
            quotes={
                "SPY260514C00500000": {"bid": 1.00, "ask": 1.005},
            },
        )
        late = EASTERN.localize(datetime(2026, 5, 14, 13, 45, 0))

        contract, reason = options.select_atm_option_contract_with_reason(
            data_client=data,
            underlying_symbol="SPY",
            direction="call",
            underlying_price=500.0,
            now_et=late,
        )

        self.assertEqual(reason, "ok(strict)")
        self.assertEqual(contract["symbol"], "SPY260514C00500000")

    def test_rejects_same_day_contracts_near_option_expiry_cutoff(self):
        data = FakeOptionData(
            contracts=[
                {
                    "symbol": "AMD260514C00110000",
                    "expiration_date": "2026-05-14",
                    "strike_price": 110.0,
                    "open_interest": 100,
                    "volume": 50,
                }
            ],
            quotes={
                "AMD260514C00110000": {"bid": 1.99, "ask": 2.0},
            },
        )
        near_cutoff = EASTERN.localize(datetime(2026, 5, 14, 14, 55, 0))

        contract, reason = options.select_atm_option_contract_with_reason(
            data_client=data,
            underlying_symbol="AMD",
            direction="call",
            underlying_price=110.0,
            now_et=near_cutoff,
        )

        self.assertIsNone(contract)
        self.assertIn("contract_quality_late_0dte_block=1", reason)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
