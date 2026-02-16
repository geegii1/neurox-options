import math


SQRT_2PI = math.sqrt(2.0 * math.pi)


def norm_cdf(x: float) -> float:
    # Standard normal CDF using erf (fast, stable)
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / SQRT_2PI


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def bs_d1_d2(S: float, K: float, r: float, sigma: float, T: float):
    # Basic parameter guards
    S = float(S)
    K = float(K)
    T = float(T)
    r = float(r)
    sigma = float(sigma)

    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return None, None

    vsqrt = sigma * math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / vsqrt
    d2 = d1 - vsqrt
    return d1, d2


def bs_price(S: float, K: float, r: float, sigma: float, T: float, is_call: bool) -> float | None:
    d1, d2 = bs_d1_d2(S, K, r, sigma, T)
    if d1 is None:
        return None

    if is_call:
        return S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)
