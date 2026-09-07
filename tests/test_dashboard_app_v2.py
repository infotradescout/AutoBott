from __future__ import annotations

from autobott_v2 import dashboard_app_v2


def test_pair_cockpit_aggregates_core_runner_and_funding_progress(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_app_v2.legacy,
        "_account_positions_payload",
        lambda: {
            "ok": True,
            "account": {"equity": 10000.0, "cash": 5000.0, "day_pl": 125.0, "day_pl_pct": 1.25},
            "positions": [
                {
                    "symbol": "VIX261016C00017000",
                    "leg_role": "primary",
                    "trade_group_id": "group-1",
                    "qty": "1",
                    "avg_entry_price": "0.70",
                    "current_price": "0.90",
                    "unrealized_pl": "20.0",
                    "unrealized_plpc": "0.2857",
                },
                {
                    "symbol": "VIX261016C00020000",
                    "leg_role": "runner",
                    "trade_group_id": "group-1",
                    "qty": "1",
                    "avg_entry_price": "0.25",
                    "current_price": "0.30",
                    "unrealized_pl": "5.0",
                    "unrealized_plpc": "0.20",
                },
            ],
        },
    )
    monkeypatch.setattr(dashboard_app_v2, "_load_pair_states", lambda: {})

    payload = dashboard_app_v2._pair_cockpit_payload()

    assert payload["ok"] is True
    assert payload["pair_count"] == 1
    pair = payload["pairs"][0]
    assert pair["underlying"] == "VIX"
    assert pair["pair_pnl"] == 25.0
    assert pair["runner_entry_cost"] == 25.0
    assert pair["primary_pnl"] == 20.0
    assert pair["funding_progress"] == 0.8
    assert pair["runner_funded"] is False


def test_pair_cockpit_includes_realized_core_after_runner_is_funded(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_app_v2.legacy,
        "_account_positions_payload",
        lambda: {
            "ok": True,
            "account": {},
            "positions": [
                {
                    "symbol": "VIX261016C00020000",
                    "leg_role": "runner",
                    "trade_group_id": "group-1",
                    "qty": "1",
                    "avg_entry_price": "0.25",
                    "current_price": "0.40",
                    "unrealized_pl": "15.0",
                    "unrealized_plpc": "0.60",
                }
            ],
        },
    )
    monkeypatch.setattr(
        dashboard_app_v2,
        "_load_pair_states",
        lambda: {"group-1": {"runner_funded": True, "primary_realized_pnl_estimate": 30.0}},
    )

    payload = dashboard_app_v2._pair_cockpit_payload()

    pair = payload["pairs"][0]
    assert pair["status"] == "FUNDED RUNNER"
    assert pair["funding_progress"] == 1.0
    assert pair["pair_pnl"] == 45.0
