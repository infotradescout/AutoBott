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
