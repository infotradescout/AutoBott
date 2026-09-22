"""Synthetic market inputs for the combined restored-entry/budgeted pipeline."""
from datetime import UTC, datetime, timedelta

AT = datetime(2026, 9, 21, 15, 35, tzinfo=UTC)


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return AT.astimezone(tz) if tz else AT.replace(tzinfo=None)


class RankedMarketTape:
    option_feed = "indicative"

    def __init__(self, trace, *, refresh_defect=None):
        self.trace = trace
        self.refresh_defect = refresh_defect

    def get_stock_bars(self, symbols, *, start, end, timeframe="1Hour", limit=35):
        seconds = 3600 if timeframe == "1Hour" else 60
        return {symbol: [
            {"t": (end-timedelta(seconds=seconds*(limit-index))).isoformat(),
             "o": 100.0, "h": 100.1, "l": 99.9, "c": 100.0, "v": 100000}
            for index in range(limit)] for symbol in symbols}

    def get_latest_stock_quotes(self, symbols):
        return {symbol: {"bp": 99.99, "ap": 100.01, "t": AT.isoformat()} for symbol in symbols}

    def _chain(self, symbol):
        return {f"{symbol}261002C{strike*1000:08d}": {
            "latestQuote": {"bp": bid, "ap": ask, "t": AT.isoformat()},
            "greeks": {"delta": delta, "theta": -.002, "vega": .1, "iv": .25},
            "details": {"expiration_date": "2026-10-02", "strike_price": strike, "type": "call"},
            "dailyBar": {"v": 500}, "open_interest": 1000,
        } for strike, bid, ask, delta in [(100, .78, .82, .55), (105, .20, .22, .20)]}

    def get_option_chain_snapshots(self, symbol):
        self.trace.append(("capture", symbol))
        return self._chain(symbol)

    def get_latest_option_quotes(self, symbols):
        self.trace.append(("refresh", tuple(symbols)))
        quotes = {}
        for symbol in symbols:
            root = symbol[:-15]
            quote = dict(self._chain(root)[symbol]["latestQuote"])
            if self.refresh_defect == "stale":
                quote["t"] = (AT-timedelta(minutes=5)).isoformat()
            elif self.refresh_defect == "chased":
                quote["bp"] += 1
                quote["ap"] += 1
            quotes[symbol] = quote
        return quotes


def run_combined(monkeypatch, tmp_path, build_card, *, refresh_defect=None, observe=False):
    from autobott_v2 import trading_cycle as legacy
    from autobott_v2 import trading_cycle_v2 as adapter
    from autobott_v2 import ranked_entry_scan
    from autobott_v2.runtime_control import arm_paper_execution
    from test_budgeted_broker_integration import setup

    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setenv("AUTOBOTT_ARTIFACTS_ROOT", str(tmp_path / "artifacts"))
    monkeypatch.setenv("AUTOBOTT_GATE_PATH", str(tmp_path / "gate.json"))
    if observe:
        monkeypatch.setenv("AUTOBOTT_PRIMARY_OBSERVATION_SECONDS", "1800")
    broker, ledger, transport = setup(monkeypatch, tmp_path, cap=150)
    arm_paper_execution(reason="synthetic combined entry capacity test")
    trace = []
    client = RankedMarketTape(trace, refresh_defect=refresh_defect)
    original_request = transport.request
    def request(method, path, **kwargs):
        if method == "POST":
            trace.append(("POST", kwargs["payload"]["symbol"]))
        result = original_request(method, path, **kwargs)
        if method == "POST":
            result["submitted_at"] = (AT+timedelta(seconds=2)).isoformat()
            result["filled_at"] = (AT+timedelta(seconds=2)).isoformat()
        return result
    monkeypatch.setattr(broker, "_request_json_once", request)
    monkeypatch.setattr(ranked_entry_scan, "datetime", FixedDateTime)
    monkeypatch.setattr(legacy, "_entry_check_now", lambda: AT+timedelta(seconds=2))
    monkeypatch.setattr(adapter, "build_decision_card_v2", build_card)
    monkeypatch.setattr(adapter, "run_position_monitor_v2", lambda **_: {"ok": True, "checked": 0, "actions": []})
    monkeypatch.setattr(legacy, "observe_ghost_trades", lambda *_, **__: [])
    monkeypatch.setattr(legacy, "select_defined_risk_spread", lambda *_, **__: None)
    result = adapter.run_trading_cycle(symbols=["WEAK", "STRONG"], broker=broker, data_client=client,
        scheduled_market_time=AT, captured_at_utc=AT,
        corpus_root=tmp_path / "captures", execution_log_path=str(tmp_path / "execution_orders.jsonl"),
        decision_log_path=tmp_path / "decisions.jsonl")
    return result, broker, ledger, transport, trace
