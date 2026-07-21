from __future__ import annotations

from datetime import UTC, date, datetime

from autobott_v2.vix_robinhood_mirror import build_robinhood_mirror_report, paper_vix_operating_config
from autobott_v2.vix_trader import (
    AuthoritativeCboeCalendar,
    SettlementType,
    TradingSession,
    VixContractMetadata,
    VixPreflightRequest,
    VixProduct,
    append_vix_cycle,
    create_vix_cycle,
    vix_strategy_config_from_dict,
)


def test_paper_operating_config_allows_trading_without_evidence_deadlock() -> None:
    operating = paper_vix_operating_config()
    assert operating["paper_only"] is True
    assert operating["robinhood_mirror"] is True
    assert operating["config"]["maximum_combined_debit"] is not None
    assert operating["source"] in {"paper_candidate", "evidence", "evidence_artifact"}


def test_robinhood_mirror_report_emits_copy_actions(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("AUTOBOTT_DATA_ROOT", str(tmp_path))
    operating = paper_vix_operating_config()
    config = vix_strategy_config_from_dict(operating["config"])
    when = datetime(2026, 7, 13, 15, 0, tzinfo=UTC)
    expiration = date(2026, 7, 22)
    product = VixProduct.VIXW
    request = VixPreflightRequest(
        spot_vix=17.5,
        product=product,
        call_product=product,
        put_product=product,
        call_expiration=expiration,
        put_expiration=expiration,
        settlement_type=SettlementType.AM,
        intended_session=TradingSession.REGULAR,
        actual_timestamp=when,
        call_strike=18.0,
        put_strike=17.0,
        call_quantity=1,
        put_quantity=1,
        call_debit=2.0,
        put_debit=2.0,
        client_request_id="rh-mirror-1",
        call_contract=VixContractMetadata(
            "VIXW-CALL", product, "call", expiration, 18.0, SettlementType.AM, "broker", when
        ),
        put_contract=VixContractMetadata(
            "VIXW-PUT", product, "put", expiration, 17.0, SettlementType.AM, "broker", when
        ),
        timestamp_source="server",
    )
    calendar = AuthoritativeCboeCalendar(
        holidays=frozenset(),
        source="cboe_published_schedule",
        source_url="https://www.cboe.com/about/hours/us-options/",
        coverage_start=date(2026, 1, 1),
        coverage_end=date(2026, 12, 31),
        published_at=when,
    )
    cycle = create_vix_cycle(request, config, calendar=calendar)
    append_vix_cycle(cycle, path=tmp_path / "vix_trader" / "cycles.jsonl")
    report = build_robinhood_mirror_report()
    assert report["mode"] == "paper_trading_with_robinhood_reporting"
    assert report["real_money_venue"] == "robinhood_manual"
    assert report["autobott_submits_live_orders"] is False
    assert report["open_count"] == 1
    actions = report["robinhood_action_queue"]
    assert any(row["action"] == "BUY_TO_OPEN" and row["option_type"] == "call" for row in actions)
    assert any(row["action"] == "BUY_TO_OPEN" and row["option_type"] == "put" for row in actions)
    assert "BUY_TO_OPEN 1 VIXW Call 18" in actions[0]["copy_line"]
