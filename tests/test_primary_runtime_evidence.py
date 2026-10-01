"""Evidence-continuation tests. No real broker/account/network is used."""
from datetime import timedelta

from autobott_v2.primary_runtime_evidence import poll_primary_runtime_evidence_once
from test_primary_fill_capture import ReadOnlyBroker
from test_primary_fill_linkage import linked_case
from test_primary_followthrough import Quotes, START, watch


def test_empty_runtime_evidence_root_does_not_touch_clients(tmp_path):
    class Poison:
        def __getattr__(self, name):
            raise AssertionError(f"client must not be constructed/touched: {name}")

    result = poll_primary_runtime_evidence_once(
        root=tmp_path / "missing",
        broker=Poison(), data_client=Poison(),
        now_fn=lambda: START,
    )
    assert result["trading_actions"] == 0
    assert result["fills"]["checked"] == 0
    assert result["observations"]["checked"] == 0


def test_existing_watch_is_observed_without_any_trading_action(tmp_path):
    root, _, _, _, _ = watch(tmp_path)
    case = linked_case(tmp_path / "broker")
    broker = ReadOnlyBroker(case["broker_order_observations"][0]["order"])
    now = START + timedelta(seconds=70)
    quotes = Quotes(now)

    result = poll_primary_runtime_evidence_once(
        root=root, broker=broker, data_client=quotes, now_fn=lambda: now,
    )

    assert result["trading_actions"] == 0
    assert result["fills"]["checked"] == 0
    assert broker.account_reads == 0 and broker.order_reads == []
    assert result["observations"]["observed"] == 1
    assert quotes.calls


def test_expired_watch_closes_after_window_without_market_or_broker_request(tmp_path):
    root, _, _, _, _ = watch(tmp_path)
    case = linked_case(tmp_path / "broker")
    broker = ReadOnlyBroker(case["broker_order_observations"][0]["order"])

    class PoisonQuotes:
        option_feed = "poison"
        stock_feed = "poison"
        calls = 0
        def get_latest_option_quotes(self, symbols):
            self.calls += 1
            raise AssertionError("expired watch must not request option quotes")
        def get_latest_stock_quotes(self, symbols):
            self.calls += 1
            raise AssertionError("expired watch must not request stock quotes")

    quotes = PoisonQuotes()
    result = poll_primary_runtime_evidence_once(
        root=root, broker=broker, data_client=quotes,
        now_fn=lambda: START + timedelta(hours=1),
    )

    assert result["trading_actions"] == 0
    assert result["observations"]["window_closed"] == 1
    assert result["observations"]["checked"] == 0
    assert quotes.calls == 0
    assert broker.account_reads == 0 and broker.order_reads == []
