import math
from dataclasses import dataclass

from .bs import bs_d1_d2, norm_cdf, norm_pdf


@dataclass(frozen=True)
class Greeks:
    delta: float
    gamma: float
    vega: float
    theta: float


def bs_greeks(S: float, K: float, r: float, sigma: float, T: float, is_call: bool) -> Greeks | None:
    """
    Returns Black–Scholes greeks:
      delta: per 1 share
      gamma: per 1 share^2
      vega: per 1 vol point?  -> we return per 1.00 (i.e., per 100% vol). Convert later if needed.
      theta: per year (negative means decay). Convert to per-day outside if desired.
    """
    d1, d2 = bs_d1_d2(S, K, r, sigma, T)
    if d1 is None:
        return None

    pdf = norm_pdf(d1)

    if is_call:
        delta = norm_cdf(d1)
        theta = -(S * pdf * sigma) / (2.0 * math.sqrt(T)) - r * K * math.exp(-r * T) * norm_cdf(d2)
    else:
        delta = norm_cdf(d1) - 1.0
        theta = -(S * pdf * sigma) / (2.0 * math.sqrt(T)) + r * K * math.exp(-r * T) * norm_cdf(-d2)

    gamma = pdf / (S * sigma * math.sqrt(T))
    vega = S * pdf * math.sqrt(T)

    return Greeks(delta=delta, gamma=gamma, vega=vega, theta=theta)


def contract_multiplier() -> int:
    # US equity options are typically 100 shares per contract
    return 100
