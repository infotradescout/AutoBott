from __future__ import annotations

import math

RISK_FREE_RATE = 0.04


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def black_scholes_price(*, s: float, k: float, t: float, r: float, sigma: float, option_type: str) -> float:
    intrinsic = max(0.0, (s - k) if option_type == "call" else (k - s))
    if t <= 0 or sigma <= 0 or s <= 0 or k <= 0:
        return intrinsic
    sqrt_t = math.sqrt(t)
    d1 = (math.log(s / k) + (r + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    if option_type == "call":
        return s * _norm_cdf(d1) - k * math.exp(-r * t) * _norm_cdf(d2)
    return k * math.exp(-r * t) * _norm_cdf(-d2) - s * _norm_cdf(-d1)


def black_scholes_greeks(*, s: float, k: float, t: float, r: float, sigma: float, option_type: str) -> tuple[float, float, float]:
    """Returns (delta, theta_per_day, vega_per_1pct_vol)."""
    if t <= 0 or sigma <= 0 or s <= 0 or k <= 0:
        delta = 0.0
        if s > k:
            delta = 1.0 if option_type == "call" else 0.0
        elif s < k:
            delta = 0.0 if option_type == "call" else -1.0
        return delta, 0.0, 0.0
    sqrt_t = math.sqrt(t)
    d1 = (math.log(s / k) + (r + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    pdf_d1 = _norm_pdf(d1)
    vega = s * pdf_d1 * sqrt_t / 100.0
    if option_type == "call":
        delta = _norm_cdf(d1)
        theta = (-(s * pdf_d1 * sigma) / (2 * sqrt_t) - r * k * math.exp(-r * t) * _norm_cdf(d2)) / 365.0
    else:
        delta = _norm_cdf(d1) - 1.0
        theta = (-(s * pdf_d1 * sigma) / (2 * sqrt_t) + r * k * math.exp(-r * t) * _norm_cdf(-d2)) / 365.0
    return delta, theta, vega


def implied_volatility(*, price: float, s: float, k: float, t: float, r: float, option_type: str) -> float | None:
    if price <= 0 or t <= 0 or s <= 0 or k <= 0:
        return None
    intrinsic = max(0.0, (s - k) if option_type == "call" else (k - s))
    if price < intrinsic:
        return None
    low, high = 1e-4, 5.0
    mid = high
    for _ in range(100):
        mid = (low + high) / 2.0
        model_price = black_scholes_price(s=s, k=k, t=t, r=r, sigma=mid, option_type=option_type)
        if abs(model_price - price) < 1e-4:
            return mid
        if model_price > price:
            high = mid
        else:
            low = mid
    return mid


def solve_iv_and_greeks(
    *,
    price: float,
    s: float,
    k: float,
    dte_days: int,
    option_type: str,
    r: float = RISK_FREE_RATE,
) -> tuple[float, float, float, float] | None:
    """Returns (iv, delta, theta_per_day, vega_per_1pct_vol) solved from a market price, or None if unsolvable."""
    t = max(dte_days, 0) / 365.0
    iv = implied_volatility(price=price, s=s, k=k, t=t, r=r, option_type=option_type)
    if iv is None:
        return None
    delta, theta, vega = black_scholes_greeks(s=s, k=k, t=t, r=r, sigma=iv, option_type=option_type)
    return iv, delta, theta, vega


def black_76_price(*, forward: float, k: float, t: float, r: float, sigma: float, option_type: str) -> float:
    """Price a European option on an expiry-specific forward."""

    discount = math.exp(-r * max(t, 0.0))
    intrinsic = discount * max(0.0, (forward - k) if option_type == "call" else (k - forward))
    if t <= 0 or sigma <= 0 or forward <= 0 or k <= 0:
        return intrinsic
    sqrt_t = math.sqrt(t)
    d1 = (math.log(forward / k) + 0.5 * sigma * sigma * t) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    if option_type == "call":
        return discount * (forward * _norm_cdf(d1) - k * _norm_cdf(d2))
    return discount * (k * _norm_cdf(-d2) - forward * _norm_cdf(-d1))


def solve_forward_iv_and_greeks(
    *,
    price: float,
    forward: float,
    k: float,
    dte_days: int,
    option_type: str,
    r: float = RISK_FREE_RATE,
) -> tuple[float, float, float, float] | None:
    """Solve Black-76 IV and Greeks from an observed option mid.

    VIX options are European options whose relevant reference varies by
    expiration.  ``forward`` must therefore come from the same expiration,
    rather than from spot VIX or a dollar-priced volatility ETF.
    """

    t = max(dte_days, 0) / 365.0
    if price <= 0 or t <= 0 or forward <= 0 or k <= 0:
        return None
    discount = math.exp(-r * t)
    intrinsic = discount * max(0.0, (forward - k) if option_type == "call" else (k - forward))
    if price + 1e-9 < intrinsic:
        return None
    upper_bound = discount * (forward if option_type == "call" else k)
    if price > upper_bound + 1e-9:
        return None

    low, high = 1e-4, 1.0
    high_price = black_76_price(
        forward=forward,
        k=k,
        t=t,
        r=r,
        sigma=high,
        option_type=option_type,
    )
    while high_price + 1e-4 < price and high < 64.0:
        high *= 2.0
        high_price = black_76_price(
            forward=forward,
            k=k,
            t=t,
            r=r,
            sigma=high,
            option_type=option_type,
        )
    if high_price + 1e-4 < price:
        return None

    iv = high
    for _ in range(100):
        iv = (low + high) / 2.0
        model_price = black_76_price(
            forward=forward,
            k=k,
            t=t,
            r=r,
            sigma=iv,
            option_type=option_type,
        )
        if abs(model_price - price) < 1e-4:
            break
        if model_price > price:
            high = iv
        else:
            low = iv
    if abs(
        black_76_price(
            forward=forward,
            k=k,
            t=t,
            r=r,
            sigma=iv,
            option_type=option_type,
        )
        - price
    ) > 1e-3:
        return None

    sqrt_t = math.sqrt(t)
    d1 = (math.log(forward / k) + 0.5 * iv * iv * t) / (iv * sqrt_t)
    if option_type == "call":
        delta = discount * _norm_cdf(d1)
    else:
        delta = discount * (_norm_cdf(d1) - 1.0)
    vega = discount * forward * _norm_pdf(d1) * sqrt_t / 100.0

    # One-calendar-day decay with the same expiry forward held fixed. This is
    # the useful theta convention for ranking contracts when provider Greeks
    # are missing; it does not pretend to forecast tomorrow's VIX term curve.
    shorter_t = max(0.0, t - 1.0 / 365.0)
    theta = black_76_price(
        forward=forward,
        k=k,
        t=shorter_t,
        r=r,
        sigma=iv,
        option_type=option_type,
    ) - black_76_price(
        forward=forward,
        k=k,
        t=t,
        r=r,
        sigma=iv,
        option_type=option_type,
    )
    return iv, delta, theta, vega
