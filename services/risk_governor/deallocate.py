import json
import math
import os
from datetime import datetime, timezone

from services.risk_governor.risk_mode import set_risk_mode


def utc_iso():
    return datetime.now(timezone.utc).isoformat()


def read_json(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def atomic_write(path: str, obj: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)


def max_qty_with_limits(
    base_totals: dict,
    inc: dict,
    limits: dict,
    qty_max: int,
) -> int:
    """
    Find max integer q in [0, qty_max] s.t. abs(base + q*inc) <= limits for delta/gamma/vega.
    Conservative: uses ALL three constraints.
    """
    def ok(q: int) -> bool:
        d = float(base_totals.get("delta", 0.0)) + q * float(inc.get("delta", 0.0))
        g = float(base_totals.get("gamma", 0.0)) + q * float(inc.get("gamma", 0.0))
        v = float(base_totals.get("vega", 0.0)) + q * float(inc.get("vega", 0.0))
        return (
            abs(d) <= limits["max_abs_delta"]
            and abs(g) <= limits["max_abs_gamma"]
            and abs(v) <= limits["max_abs_vega"]
        )

    # binary search
    lo, hi = 0, int(qty_max)
    best = 0
    while lo <= hi:
        mid = (lo + hi) // 2
        if ok(mid):
            best = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return best


def main(
    plan_path: str = "state/final_plan.json",
    greeks_path: str = "state/portfolio_greeks.json",
    out_path: str = "state/dealloc_plan.json",
    limits: dict | None = None,
):
    if limits is None:
        limits = {
            "max_abs_delta": 200.0,
            "max_abs_gamma": 10.0,
            "max_abs_vega": 20000.0,
        }

    plan = read_json(plan_path)
    g = read_json(greeks_path)

    base = g.get("totals", {})
    qty_req = int(plan.get("gate", {}).get("order_plan", {}).get("qty", 0))

    # Incremental greeks per 1 spread:
    # We approximate using the two legs greeks already computed in portfolio_greeks.json
    # (Long leg +1, short leg -1). For the current plan, we find those two symbols in g["positions"].
    long_sym = plan["resolved"]["long_leg"]["symbol"]
    short_sym = plan["resolved"]["short_leg"]["symbol"]

    pos_map = {p["symbol"]: p for p in g.get("positions", [])}

    if long_sym not in pos_map or short_sym not in pos_map:
        out = {
            "ts": utc_iso(),
            "status": "CANNOT_DEALLOC",
            "reason": "MISSING_LEG_GREEKS",
            "need": [long_sym, short_sym],
        }
        atomic_write(out_path, out)
        print(f"Wrote {out_path}")
        return

    # The greeks.py rows are already position-weighted. We need per-contract greeks.
    # So divide by net_qty currently in book. If net_qty is 0, can't infer.
    def per_contract(sym: str) -> dict:
        p = pos_map[sym]
        nq = int(p["net_qty"])
        if nq == 0:
            raise ValueError("net_qty=0 cannot infer per-contract")
        return {
            "delta": float(p["delta"]) / nq,
            "gamma": float(p["gamma"]) / nq,
            "vega": float(p["vega"]) / nq,
        }

    long_pc = per_contract(long_sym)     # per 1 contract (long)
    short_pc = per_contract(short_sym)   # per 1 contract (short)  (note: net_qty negative in book → still divides correctly)

    inc = {
        "delta": long_pc["delta"] + short_pc["delta"],
        "gamma": long_pc["gamma"] + short_pc["gamma"],
        "vega": long_pc["vega"] + short_pc["vega"],
    }

    allowed = max_qty_with_limits(base, inc, limits, qty_req)

    out = {
        "ts": utc_iso(),
        "status": "OK",
        "requested_qty": qty_req,
        "allowed_qty": allowed,
        "limits": limits,
        "base_totals": base,
        "inc_per_spread": inc,
        "action": "SET_QTY_TO_ALLOWED" if allowed < qty_req else "NO_CHANGE",
    }
    atomic_write(out_path, out)

    # If currently HALT, and allowed > 0, we can move to DEGRADED to permit small sizing
    if allowed > 0:
        set_risk_mode("DEGRADED", f"DEALLOC_ALLOWED_QTY={allowed}")
    else:
        set_risk_mode("HALT", "DEALLOC_ZERO_ALLOWED")

    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
