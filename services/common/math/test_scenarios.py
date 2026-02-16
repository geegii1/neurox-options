from services.common.math.scenario_grid import Leg, scenario_grid


def main():
    # Example: QQQ call vertical (debit)
    # Long 1x call K1, Short 1x call K2
    S0 = 601.0
    r = 0.04
    T = 30 / 365  # 30 DTE
    iv = 0.22

    legs = [
        Leg(K=600, is_call=True, qty=1, side=+1, iv=iv),
        Leg(K=610, is_call=True, qty=1, side=-1, iv=iv),
    ]

    grid = scenario_grid(S0=S0, r=r, T=T, legs=legs)
    # Print a few key scenarios
    for sc in grid:
        if abs(sc.spot - S0) < 1e-6 and sc.iv in (0.0, 0.10, 0.20):
            print(f"SPOT={sc.spot:.2f} IV_SHOCK={sc.iv:.2f} PNL={sc.pnl:.2f}")

    worst = min(grid, key=lambda x: x.pnl)
    best = max(grid, key=lambda x: x.pnl)
    print(f"WORST: spot={worst.spot:.2f} iv={worst.iv:.2f} pnl={worst.pnl:.2f}")
    print(f"BEST:  spot={best.spot:.2f} iv={best.iv:.2f} pnl={best.pnl:.2f}")


if __name__ == "__main__":
    main()
