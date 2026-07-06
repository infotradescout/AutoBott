from autobott_v2.options_math import black_scholes_price, implied_volatility, solve_iv_and_greeks


def test_implied_volatility_round_trips_a_known_price() -> None:
    price = black_scholes_price(s=100.0, k=100.0, t=30 / 365, r=0.04, sigma=0.20, option_type="call")

    solved = implied_volatility(price=price, s=100.0, k=100.0, t=30 / 365, r=0.04, option_type="call")

    assert solved is not None
    assert abs(solved - 0.20) < 1e-3


def test_atm_call_delta_is_near_half() -> None:
    result = solve_iv_and_greeks(price=5.0, s=100.0, k=100.0, dte_days=30, option_type="call")

    assert result is not None
    iv, delta, theta, vega = result
    assert 0.4 < delta < 0.7
    assert theta < 0
    assert vega > 0


def test_put_delta_is_negative() -> None:
    result = solve_iv_and_greeks(price=5.0, s=100.0, k=100.0, dte_days=30, option_type="put")

    assert result is not None
    _, delta, _, _ = result
    assert -0.7 < delta < -0.3


def test_deep_itm_call_delta_approaches_one() -> None:
    result = solve_iv_and_greeks(price=51.0, s=150.0, k=100.0, dte_days=10, option_type="call")

    assert result is not None
    _, delta, _, _ = result
    assert delta > 0.9


def test_unsolvable_price_returns_none() -> None:
    assert solve_iv_and_greeks(price=0.0, s=100.0, k=100.0, dte_days=30, option_type="call") is None
