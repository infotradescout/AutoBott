from __future__ import annotations

from autobott_v2.phase1_bucket_eligibility import BucketEligibilityRules, evaluate_bucket_eligibility


def _metrics(
    *,
    closed_trades: int = 60,
    profit_factor: float = 1.4,
    expectancy: float = 0.05,
    max_drawdown: float = 3.0,
    fill_rate: float = 0.8,
    unresolved_position_rate: float = 0.05,
    largest_win_pct_of_total_net_profit: float = 0.2,
    trading_days_covered: int = 4,
    net_profit: float = 10.0,
) -> dict[str, float | int]:
    return {
        "closed_trades": closed_trades,
        "wins": int(closed_trades * 0.6),
        "losses": int(closed_trades * 0.4),
        "win_rate": 0.6,
        "profit_factor": profit_factor,
        "expectancy": expectancy,
        "max_drawdown": max_drawdown,
        "fill_rate": fill_rate,
        "unresolved_position_rate": unresolved_position_rate,
        "largest_win_pct_of_total_net_profit": largest_win_pct_of_total_net_profit,
        "trading_days_covered": trading_days_covered,
        "net_profit": net_profit,
    }


def _fill_models(primary: dict[str, float | int]) -> dict[str, dict[str, float | int]]:
    return {
        "optimistic_mid": _metrics(expectancy=0.09, profit_factor=1.6),
        "realistic_mid_penalty": primary,
        "conservative": _metrics(expectancy=0.02, profit_factor=1.1),
        "stress": _metrics(expectancy=-0.01, profit_factor=0.9),
    }


def test_optimistic_only_profitable_bucket_not_authorized() -> None:
    metrics = _fill_models(_metrics(expectancy=-0.01, profit_factor=0.9))
    result = evaluate_bucket_eligibility(metrics)

    assert result["eligible_for_paper_forward"] is False
    assert "optimistic_only_profitability" in result["blocking_reasons"]


def test_realistic_profitable_bucket_can_be_marked_paper_eligible() -> None:
    metrics = _fill_models(_metrics())
    result = evaluate_bucket_eligibility(metrics)

    assert result["eligible_for_paper_forward"] is True
    assert result["eligible_for_live_review"] is False


def test_high_unresolved_rate_blocks_bucket() -> None:
    metrics = _fill_models(_metrics(unresolved_position_rate=0.4))
    result = evaluate_bucket_eligibility(metrics)

    assert result["eligible_for_paper_forward"] is False
    assert "high_unresolved_position_rate" in result["blocking_reasons"]


def test_single_outlier_profit_does_not_unlock_bucket() -> None:
    metrics = _fill_models(_metrics(largest_win_pct_of_total_net_profit=0.6, net_profit=12.0))
    result = evaluate_bucket_eligibility(metrics)

    assert result["eligible_for_paper_forward"] is False
    assert "single_outlier_profit_dependency" in result["blocking_reasons"]
