import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone

# ------------------------------------------------------------
# Simple RiskDecision container
# ------------------------------------------------------------

@dataclass
class RiskDecision:
    allow: bool
    max_contracts: int
    reasons: list
    worst_pnl_gap10: float | None
    worst_pnl_combo: float | None


# ------------------------------------------------------------
# Basic Black-Scholes helpers
# ------------------------------------------------------------

def norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_price(S, K, r, T, iv, is_call):
    if T <= 0 or iv <= 0:
        return max(0.0, (S - K) if is_call else (K - S))

    d1 = (math.log(S / K) + (r + 0.5 * iv**2) * T) / (iv * math.sqrt(T))
    d2 = d1 - iv * math.sqrt(T)

    if is_call:
        return S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)


# ------------------------------------------------------------
# Core Risk Logic
# ------------------------------------------------------------

def decide_vertical_from_plan(
    underlier: str,
    is_call: bool,
    K_long: float,
    K_short: float,
    dte_days: int,
    qty_requested: int,
):
    """
    Simplified wrapper that:
    - pulls IV defaults from env
    - computes worst-case loss for 1 contract
    - sizes against policy limits
    """

    # ---- Inputs ----
    S0 = float(os.getenv("RISK_SPOT_DEFAULT", "600"))
    r = float(os.getenv("RISK_FREE_RATE", "0.03"))
    iv_long = float(os.getenv("RISK_IV_LONG_DEFAULT", "0.35"))
    iv_short = float(os.getenv("RISK_IV_SHORT_DEFAULT", "0.30"))

    T = max(dte_days / 365.0, 1e-6)

    # ---- Scenario 1: -10% gap ----
    S_gap = S0 * 0.90

    long_gap = bs_price(S_gap, K_long, r, T, iv_long, is_call)
    short_gap = bs_price(S_gap, K_short, r, T, iv_short, is_call)

    pnl_gap = (long_gap - short_gap) - (bs_price(S0, K_long, r, T, iv_long, is_call)
                                        - bs_price(S0, K_short, r, T, iv_short, is_call))

    # ---- Scenario 2: -7% + IV +10% ----
    S_combo = S0 * 0.93
    iv_long_up = iv_long * 1.10
    iv_short_up = iv_short * 1.10

    long_combo = bs_price(S_combo, K_long, r, T, iv_long_up, is_call)
    short_combo = bs_price(S_combo, K_short, r, T, iv_short_up, is_call)

    pnl_combo = (long_combo - short_combo) - (
        bs_price(S0, K_long, r, T, iv_long, is_call)
        - bs_price(S0, K_short, r, T, iv_short, is_call)
    )

    worst_1 = min(pnl_gap, pnl_combo)

    # ---- Policy Limits ----
    equity = float(os.getenv("RISK_ACCOUNT_EQUITY", "100000"))
    max_trade_pct = float(os.getenv("RISK_MAX_DEFINED_RISK_PCT", "0.02"))

    max_trade_loss_usd = equity * max_trade_pct
    loss_mag = max(0.0, -worst_1)

    reasons = []

    if loss_mag <= 0:
        return RiskDecision(
            allow=True,
            max_contracts=qty_requested,
            reasons=[],
            worst_pnl_gap10=pnl_gap,
            worst_pnl_combo=pnl_combo,
        )

    max_contracts = int(max_trade_loss_usd // loss_mag)

    if max_contracts <= 0:
        reasons.append("SIZING_TO_ZERO_BY_LIMITS")

    allow = max_contracts > 0

    return RiskDecision(
        allow=allow,
        max_contracts=min(max_contracts, qty_requested),
        reasons=reasons,
        worst_pnl_gap10=pnl_gap,
        worst_pnl_combo=pnl_combo,
    )


# ------------------------------------------------------------
# CLI test
# ------------------------------------------------------------

def main():
    d = decide_vertical_from_plan(
        underlier="QQQ",
        is_call=True,
        K_long=600,
        K_short=610,
        dte_days=30,
        qty_requested=10,
    )

    print(vars(d))


if __name__ == "__main__":
    main()
