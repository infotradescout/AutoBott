from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from autobott_v2 import execution_broker as native
from autobott_v2 import portfolio_budget as budget
from autobott_v2.execution_config import load_alpaca_execution_config, PORTFOLIO_PREMIUM_ENVELOPE
from autobott_v2.execution_models import OrderSide, OrderType
from test_portfolio_budget import intent, snapshot


def config(monkeypatch, *, enabled=True, cap=1000):
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setenv("AUTOBOTT_PORTFOLIO_BUDGET_ENABLED", "true" if enabled else "false")
    monkeypatch.setenv("ALPACA_API_KEY_ID", "synthetic-key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "synthetic-secret")
    monkeypatch.setenv("AUTOBOTT_ALLOW_ORDER_PLACEMENT", "true")
    result = load_alpaca_execution_config().validate()
    return replace(result, portfolio_premium_limit=cap) if enabled else result


class Transport:
    def __init__(self):
        self.orders = []
        self.posts = []
        self.patches = []
        self.timeout_once = False

    def request(self, method, path, *, payload=None):
        if method == "POST":
            self.posts.append(dict(payload))
            order = {**payload, "id": f"order-{len(self.posts)}", "status": "filled",
                "filled_qty": payload["qty"], "filled_avg_price": payload.get("limit_price", "1"),
                "submitted_at": datetime.now(UTC).isoformat(), "filled_at": datetime.now(UTC).isoformat(),
                "asset_class": "us_option"}
            self.orders.append(order)
            if self.timeout_once:
                self.timeout_once = False
                raise TimeoutError("synthetic uncertain delivery")
            return order
        if method == "PATCH":
            self.patches.append((path, payload))
            return {"status": "pending_replace"}
        if path == "/v2/account":
            return {"id": "test", "status": "ACTIVE", "currency": "USD", "trading_blocked": False,
                    "cash": "10000", "options_buying_power": "10000"}
        if path == "/v2/positions":
            return [{"symbol": o["symbol"], "qty": o["qty"], "side": "long", "asset_class": "us_option",
                     "cost_basis": str(float(o["filled_avg_price"]) * int(o["qty"]) * 100),
                     "market_value": str(float(o["filled_avg_price"]) * int(o["qty"]) * 100)}
                    for o in self.orders if o["side"] == "buy"]
        if path.startswith("/v2/orders:by_client_order_id?"):
            from urllib.parse import parse_qs, urlsplit
            client = parse_qs(urlsplit(path).query)["client_order_id"][0]
            return next(o for o in self.orders if o["client_order_id"] == client)
        if path.startswith("/v2/orders?"):
            return list(self.orders)
        if path.startswith("/v2/orders/"):
            return next(o for o in self.orders if o["id"] == path.rsplit("/", 1)[-1])
        raise AssertionError((method, path))


def setup(monkeypatch, tmp_path, **kwargs):
    broker = native.AlpacaExecutionBroker(config(monkeypatch, **kwargs))
    ledger = budget.PremiumLedger(tmp_path / "budget.sqlite")
    monkeypatch.setattr(budget, "_ledger", lambda: ledger)
    transport = Transport()
    monkeypatch.setattr(broker, "_request_json_once", transport.request)
    monkeypatch.setattr(broker, "_pace_mutation", lambda: None)
    return broker, ledger, transport


def test_twenty_native_small_entries_fit_same_budget_and_next_never_posts(monkeypatch, tmp_path):
    broker, ledger, transport = setup(monkeypatch, tmp_path)
    for index in range(20):
        result = broker.submit_order(replace(intent(index, 50), order_type=OrderType.MARKET), open_positions=index)
        assert result.intent.order_type is OrderType.LIMIT
        assert result.client_order_id == transport.posts[-1]["client_order_id"]
        assert transport.posts[-1]["type"] == "limit"
        assert transport.posts[-1]["limit_price"] == "0.50"
    assert len(transport.posts) == 20
    with pytest.raises(budget.BudgetBlocked, match="portfolio_premium_budget_exceeded"):
        broker.submit_order(intent(21, 1), open_positions=20)
    assert len(transport.posts) == 20
    observed = budget.read_budget_snapshot(broker)
    assert observed.held_cents == 100000


def test_ambiguous_acceptance_uses_reserved_id_without_an_extra_post(monkeypatch, tmp_path):
    broker, ledger, transport = setup(monkeypatch, tmp_path)
    transport.timeout_once = True
    result = broker.submit_order(intent(0, 100))
    assert len(transport.posts) == 1
    assert result.client_order_id == transport.orders[0]["client_order_id"]
    with pytest.raises(budget.BudgetBlocked, match="portfolio_underlying_already_reserved"):
        broker.submit_order(intent(0, 100))
    assert len(transport.posts) == 1


def test_pair_cannot_send_primary_when_full_pair_exceeds_budget(monkeypatch, tmp_path):
    broker, ledger, transport = setup(monkeypatch, tmp_path)
    primary = intent(0, 800)
    runner = replace(primary, option_symbol="TEST0261002C00105000", limit_price=3)
    with pytest.raises(budget.BudgetBlocked, match="portfolio_premium_budget_exceeded"):
        with budget.reserve_pair(broker, (primary, runner)):
            broker.submit_order(primary)
            broker.submit_order(runner)
    assert not transport.posts


def test_native_pair_uses_the_two_precommitted_ids(monkeypatch, tmp_path):
    broker, ledger, transport = setup(monkeypatch, tmp_path)
    primary = intent(0, 600)
    runner = replace(primary, option_symbol="TEST0261002C00105000", limit_price=2)
    with budget.reserve_pair(broker, (primary, runner)) as receipt:
        broker.submit_order(replace(primary, order_type=OrderType.MARKET))
        broker.submit_order(replace(runner, order_type=OrderType.MARKET), open_positions=1)
    assert receipt["reserved_dollars"] == 800
    assert len({p["client_order_id"] for p in transport.posts}) == 2
    assert all(p["type"] == "limit" for p in transport.posts)
    assert budget.read_budget_snapshot(broker).held_cents == 80000


def test_sell_path_does_not_read_or_reserve_budget(monkeypatch, tmp_path):
    broker, ledger, transport = setup(monkeypatch, tmp_path)
    monkeypatch.setattr(budget, "read_budget_snapshot", lambda *_: (_ for _ in ()).throw(AssertionError("sell read budget")))
    sold = broker.submit_order(replace(intent(), side=OrderSide.SELL_TO_CLOSE, order_type=OrderType.MARKET))
    assert sold.intent.side is OrderSide.SELL_TO_CLOSE
    assert transport.posts[0]["type"] == "market"
    assert not ledger.path.exists()


def test_upward_reprice_cannot_expand_reserved_buy(monkeypatch, tmp_path):
    broker, _, transport = setup(monkeypatch, tmp_path)
    result = broker.submit_order(intent(0, 100))
    with pytest.raises(budget.BudgetBlocked, match="portfolio_buy_reprice_exceeds_reserved_cap"):
        broker.replace_order(result.broker_order_id, limit_price=2)
    assert not transport.patches


@pytest.mark.parametrize("path", ["positions", "orders"])
def test_malformed_broker_response_is_not_an_empty_portfolio(monkeypatch, tmp_path, path):
    broker, _, transport = setup(monkeypatch, tmp_path)
    original = transport.request
    def malformed(method, endpoint, **kwargs):
        if endpoint.startswith("/v2/" + path):
            return {"error": "synthetic malformed response"}
        return original(method, endpoint, **kwargs)
    monkeypatch.setattr(broker, "_request_json_once", malformed)
    with pytest.raises(budget.BudgetBlocked):
        broker.submit_order(intent())
    assert not transport.posts


def test_explicit_mode_off_retains_original_six_leg_configuration(monkeypatch):
    disabled = config(monkeypatch, enabled=False)
    assert disabled.portfolio_premium_limit is None
    assert disabled.max_open_positions == 6
    enabled = config(monkeypatch, cap=PORTFOLIO_PREMIUM_ENVELOPE)
    assert enabled.portfolio_premium_limit == 6000
    assert enabled.max_open_positions == 60
    assert enabled.max_position_cost == disabled.max_position_cost == 1000
    assert enabled.max_daily_loss == disabled.max_daily_loss == 750
    assert enabled.paper_max_new_entry_attempts_per_loop == disabled.paper_max_new_entry_attempts_per_loop == 3
    assert not enabled.allow_live_trading


def test_budget_cannot_exceed_prior_gross_premium_envelope(monkeypatch):
    base = config(monkeypatch)
    with pytest.raises(ValueError, match="portfolio_budget_outside_legacy_envelope"):
        replace(base, portfolio_premium_limit=6001).validate()
