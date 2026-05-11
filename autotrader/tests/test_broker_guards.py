from types import SimpleNamespace
from pathlib import Path
import sys

import pytest

_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

import config  # noqa: E402
from broker import AlpacaBroker  # noqa: E402


OPTION = "ORCL260515C00195000"


class FakeTradingClient:
    def __init__(self, positions=()):
        self.positions = list(positions)
        self.submitted = []

    def get_all_positions(self):
        return self.positions

    def submit_order(self, order_data):
        self.submitted.append(order_data)
        return SimpleNamespace(id="order-1")


def _broker_with_positions(*positions):
    broker = object.__new__(AlpacaBroker)
    broker.trading_client = FakeTradingClient(positions)
    return broker


def _position(symbol=OPTION, qty=1, asset_class="us_option"):
    return SimpleNamespace(symbol=symbol, qty=str(qty), asset_class=asset_class)


def test_market_sell_is_capped_to_current_long_option_qty():
    previous = config.ENFORCE_LONG_ONLY_OPTION_SELLS
    config.ENFORCE_LONG_ONLY_OPTION_SELLS = True
    try:
        broker = _broker_with_positions(_position(qty=1))

        broker.close_option_market(OPTION, 3)

        assert len(broker.trading_client.submitted) == 1
        assert int(broker.trading_client.submitted[0].qty) == 1
    finally:
        config.ENFORCE_LONG_ONLY_OPTION_SELLS = previous


def test_limit_sell_refuses_without_current_long_option_position():
    previous = config.ENFORCE_LONG_ONLY_OPTION_SELLS
    config.ENFORCE_LONG_ONLY_OPTION_SELLS = True
    try:
        broker = _broker_with_positions()

        with pytest.raises(ValueError, match="no long position"):
            broker.place_option_limit_sell(OPTION, 1, 5.12)

        assert broker.trading_client.submitted == []
    finally:
        config.ENFORCE_LONG_ONLY_OPTION_SELLS = previous


def test_cover_market_buy_still_allows_short_position_repair():
    broker = _broker_with_positions(_position(qty=-1))

    broker.cover_option_market(OPTION, 1)

    assert len(broker.trading_client.submitted) == 1
    assert int(broker.trading_client.submitted[0].qty) == 1
