from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

from autobott_v2.phase1_config import load_alpaca_read_only_config
from autobott_v2.phase1_engine import build_decision_card
from autobott_v2.phase1_ledger import LearningLedger
from autobott_v2.phase1_models import (
    DecisionInput,
    DecisionStatus,
    MarketBar,
    MarketContext,
    OptionContractSnapshot,
    OptionType,
)


BASE_TIME = datetime(2026, 6, 1, 15, 30, tzinfo=timezone.utc)


def _bars(start: float, step: float, volume: int = 1000) -> list[MarketBar]:
    bars: list[MarketBar] = []
    for index in range(35):
        close = start + index * step
        timestamp = BASE_TIME - timedelta(minutes=34 - index)
        bars.append(
            MarketBar(
                timestamp=timestamp,
                open=close - 0.05,
                high=close + 0.25,
                low=close - 0.25,
                close=close,
                volume=volume + index * 10,
            )
        )
    return bars


def _contract(
    *,
    option_symbol: str = "AAPL260619C00215000",
    option_type: OptionType = OptionType.CALL,
    bid: float = 4.90,
    ask: float = 5.10,
    strike: float = 215.0,
    delta: float = 0.48,
    iv: float = 0.25,
) -> OptionContractSnapshot:
    return OptionContractSnapshot(
        option_symbol=option_symbol,
        underlying="AAPL",
        expiration=date(2026, 6, 19),
        strike=strike,
        option_type=option_type,
        bid=bid,
        ask=ask,
        last=5.0,
        volume=50,
        open_interest=500,
        delta=delta,
        theta=-0.04,
        vega=0.08,
        implied_volatility=iv,
    )


def _input(*, chain: list[OptionContractSnapshot] | None = None, blackout: bool = False) -> DecisionInput:
    bars = _bars(200.0, 0.45)
    context = MarketContext(
        spy_bars=_bars(500.0, 0.20),
        qqq_bars=_bars(430.0, 0.15),
        vix_bars=_bars(16.0, -0.02),
        blackout_event=blackout,
        event_labels=["earnings"] if blackout else [],
    )
    return DecisionInput(
        ticker="AAPL",
        timestamp=BASE_TIME,
        market_bars=bars,
        option_chain=chain if chain is not None else [_contract()],
        context=context,
        iv_history=[0.18, 0.20, 0.23, 0.27, 0.31],
    )


def test_phase1_builds_trade_candidate_without_order() -> None:
    card = build_decision_card(_input())

    assert card.decision == DecisionStatus.TRADE_CANDIDATE
    assert card.selected_contract is not None
    payload = json.dumps(card.to_json_dict()).lower()
    assert "paper_order" not in payload
    assert "order_id" not in payload


def test_phase1_blocks_event_iv_crush_risk() -> None:
    card = build_decision_card(_input(chain=[_contract(iv=0.55)], blackout=True))

    assert card.decision == DecisionStatus.BLOCKED_BY_VOLATILITY
    assert card.blocked_reason == "long_option_volatility_unfavorable"
    assert card.volatility.iv_crush_risk is True


def test_phase1_blocks_when_contract_spread_is_too_wide() -> None:
    card = build_decision_card(_input(chain=[_contract(bid=4.0, ask=6.0)]))

    assert card.decision == DecisionStatus.BLOCKED_BY_SPREAD
    assert card.selected_contract is None


def test_phase1_persists_accepted_and_rejected_cards(tmp_path) -> None:
    ledger_path = tmp_path / "ledger.jsonl"
    ledger = LearningLedger(ledger_path)
    ledger.append(build_decision_card(_input()))
    ledger.append(build_decision_card(_input(chain=[_contract(bid=4.0, ask=6.0)])))

    rows = [json.loads(line) for line in ledger_path.read_text(encoding="utf-8").splitlines()]
    assert [row["decision"] for row in rows] == [
        DecisionStatus.TRADE_CANDIDATE.value,
        DecisionStatus.BLOCKED_BY_SPREAD.value,
    ]
    assert all(row["ticker"] == "AAPL" for row in rows)
    assert all(row["forward_outcomes"] == {
        "after_5m": None,
        "after_15m": None,
        "after_30m": None,
        "after_1h": None,
    } for row in rows)


def test_phase1_loads_old_alpaca_env_names_read_only(monkeypatch) -> None:
    monkeypatch.setenv("APCA_API_KEY_ID", "key")
    monkeypatch.setenv("APCA_API_SECRET_KEY", "secret")
    monkeypatch.setenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
    monkeypatch.setenv("APCA_API_DATA_URL", "https://data.alpaca.markets")

    config = load_alpaca_read_only_config()

    assert config.has_credentials is True
    assert config.paper is True
    assert config.base_url == "https://paper-api.alpaca.markets"
