import os
from dataclasses import dataclass
from typing import Any

import yaml


@dataclass(frozen=True)
class ScenarioLimits:
    max_incremental_loss_gap_10pct: float
    max_incremental_loss_combo_7pct_iv10: float


@dataclass(frozen=True)
class PositionLimits:
    max_defined_risk_pct_equity: float
    max_contracts_per_order: int


@dataclass(frozen=True)
class AccountSpec:
    equity_usd: float


@dataclass(frozen=True)
class RiskPolicy:
    account: AccountSpec
    position_limits: PositionLimits
    scenario_limits: ScenarioLimits


def load_risk_policy(path: str = "configs/risk_policy.yaml") -> RiskPolicy:
    # Resolve relative to repo root (two levels up from this file: services/risk_governor)
    here = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(here, "..", ".."))
    abs_path = path if os.path.isabs(path) else os.path.join(repo_root, path)

    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"risk policy not found: {abs_path}")

    with open(abs_path, "r") as f:
        raw: dict[str, Any] = yaml.safe_load(f)

    eq = float(raw["account"]["equity_usd"])

    pl = raw["position_limits"]["per_trade"]
    max_risk_pct = float(pl["max_defined_risk_pct_equity"])
    max_contracts = int(pl["max_contracts_per_order"])

    sl = raw["scenario_limits"]["incremental_trade"]
    max_gap10 = float(sl["max_incremental_loss_gap_10pct"])
    max_combo = float(sl["max_incremental_loss_combo_7pct_iv10"])

    return RiskPolicy(
        account=AccountSpec(equity_usd=eq),
        position_limits=PositionLimits(
            max_defined_risk_pct_equity=max_risk_pct,
            max_contracts_per_order=max_contracts,
        ),
        scenario_limits=ScenarioLimits(
            max_incremental_loss_gap_10pct=max_gap10,
            max_incremental_loss_combo_7pct_iv10=max_combo,
        ),
    )
