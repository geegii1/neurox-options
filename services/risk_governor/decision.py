from dataclasses import dataclass
from typing import List

from services.common.math.scenario_grid import Leg, scenario_grid


@dataclass(frozen=True)
class RiskDecision:
    allow: bool
    max_contracts: int
    reasons: List[str]
    worst_pnl_gap10: float
    worst_pnl_combo: float


def incremental_worst_losses(
    S0: float,
    r: float,
    T: float,
    legs: list[Leg],
) -> tuple[float, float]:
    """
    Returns:
      worst_pnl_gap10: worst PnL over spot shocks +/-10% with iv_shock=0
      worst_pnl_combo: worst PnL over spot shocks +/-7% with iv_shock=+0.10
    """
    # Gap 10% (iv shock 0)
    grid_gap = scenario_grid(
        S0=S0,
        r=r,
        T=T,
        legs=legs,
        spot_shocks=[-0.10, 0.10],
        iv_shocks=[0.0],
    )
    worst_gap10 = min(x.pnl for x in grid_gap)

    # Combo 7% + IV +10 vol points
    grid_combo = scenario_grid(
        S0=S0,
        r=r,
        T=T,
        legs=legs,
        spot_shocks=[-0.07, 0.07],
        iv_shocks=[0.10],
    )
    worst_combo = min(x.pnl for x in grid_combo)

    return worst_gap10, worst_combo
