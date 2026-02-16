from dataclasses import dataclass
from typing import Iterable

from .bs import bs_price
from .greeks import contract_multiplier


@dataclass(frozen=True)
class Leg:
    # qty: positive number of contracts
    # side: +1 for long, -1 for short
    K: float
    is_call: bool
    qty: int
    side: int
    iv: float


@dataclass(frozen=True)
class ScenarioResult:
    spot: float
    iv: float
    pnl: float


def structure_value(S: float, r: float, T: float, legs: Iterable[Leg], iv_shift: float = 0.0) -> float:
    """
    Returns the PV of the structure (in dollars), using BS on each leg.
    iv_shift is additive (e.g. +0.10 means +10 vol points if iv is in decimals).
    """
    mult = contract_multiplier()
    total = 0.0
    for leg in legs:
        sigma = max(1e-6, leg.iv + iv_shift)
        px = bs_price(S=S, K=leg.K, r=r, sigma=sigma, T=T, is_call=leg.is_call)
        if px is None:
            continue
        total += leg.side * leg.qty * mult * px
    return total


def scenario_grid(
    S0: float,
    r: float,
    T: float,
    legs: list[Leg],
    spot_shocks: list[float] = None,
    iv_shocks: list[float] = None,
) -> list[ScenarioResult]:
    """
    spot_shocks: e.g. [-0.10, -0.07, -0.03, -0.01, 0.0, 0.01, 0.03, 0.07, 0.10]
    iv_shocks:   e.g. [0.0, 0.05, 0.10, 0.20]  (additive to iv in decimals)
    """
    if spot_shocks is None:
        spot_shocks = [-0.10, -0.07, -0.03, -0.01, 0.0, 0.01, 0.03, 0.07, 0.10]
    if iv_shocks is None:
        iv_shocks = [0.0, 0.05, 0.10, 0.20]

    v0 = structure_value(S=S0, r=r, T=T, legs=legs, iv_shift=0.0)
    out = []

    for ds in spot_shocks:
        S = S0 * (1.0 + ds)
        for dv in iv_shocks:
            v = structure_value(S=S, r=r, T=T, legs=legs, iv_shift=dv)
            out.append(ScenarioResult(spot=S, iv=dv, pnl=v - v0))

    return out
