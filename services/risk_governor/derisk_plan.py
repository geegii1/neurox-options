import json
import os
from datetime import datetime, timezone
from typing import Dict


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


def sgn(x: int) -> int:
    return 1 if x > 0 else -1 if x < 0 else 0


def buffered_limits(limits: dict, buffer_pct: float) -> dict:
    # buffer_pct=0.90 means target is 90% of hard limits
    return {
        "max_abs_delta": float(limits["max_abs_delta"]) * buffer_pct,
        "max_abs_gamma": float(limits["max_abs_gamma"]) * buffer_pct,
        "max_abs_vega": float(limits["max_abs_vega"]) * buffer_pct,
    }


def within_limits(t: dict, limits: dict) -> bool:
    return (
        abs(float(t.get("delta", 0.0))) <= limits["max_abs_delta"]
        and abs(float(t.get("gamma", 0.0))) <= limits["max_abs_gamma"]
        and abs(float(t.get("vega", 0.0))) <= limits["max_abs_vega"]
    )


def per_contract_from_row(row: dict) -> Dict[str, float]:
    nq = int(row.get("net_qty", 0))
    if nq == 0:
        return {"delta": 0.0, "gamma": 0.0, "vega": 0.0}
    return {
        "delta": float(row.get("delta", 0.0)) / nq,
        "gamma": float(row.get("gamma", 0.0)) / nq,
        "vega": float(row.get("vega", 0.0)) / nq,
    }


def close_one_contract_effect(row: dict) -> Dict[str, float]:
    nq = int(row.get("net_qty", 0))
    pc = per_contract_from_row(row)
    direction = sgn(nq)
    return {
        "delta": -pc["delta"] * direction,
        "gamma": -pc["gamma"] * direction,
        "vega": -pc["vega"] * direction,
    }


def score_row(row: dict, totals: dict, limits: dict) -> float:
    d = float(totals.get("delta", 0.0))
    g = float(totals.get("gamma", 0.0))
    v = float(totals.get("vega", 0.0))

    d_over = max(0.0, abs(d) - limits["max_abs_delta"])
    g_over = max(0.0, abs(g) - limits["max_abs_gamma"])
    v_over = max(0.0, abs(v) - limits["max_abs_vega"])

    eff = close_one_contract_effect(row)

    def red_amount(x: float, dx: float) -> float:
        return max(0.0, abs(x) - abs(x + dx))

    d_red = red_amount(d, eff["delta"])
    g_red = red_amount(g, eff["gamma"])
    v_red = red_amount(v, eff["vega"])

    return 5.0 * v_over * v_red + 3.0 * g_over * g_red + 1.0 * d_over * d_red


def build_derisk_plan(
    greeks_path: str = "state/portfolio_greeks.json",
    out_path: str = "state/derisk_plan.json",
    hard_limits: dict | None = None,
    buffer_pct: float = 0.90,
    max_contracts_to_close: int = 500,
):
    if hard_limits is None:
        hard_limits = {"max_abs_delta": 200.0, "max_abs_gamma": 10.0, "max_abs_vega": 20000.0}

    target_limits = buffered_limits(hard_limits, buffer_pct)

    g = read_json(greeks_path)
    totals = dict(g.get("totals", {}))
    positions = [p for p in g.get("positions", []) if int(p.get("net_qty", 0)) != 0]

    start_totals = dict(totals)

    if within_limits(totals, target_limits):
        out = {
            "ts": utc_iso(),
            "status": "NO_ACTION",
            "reason": "WITHIN_TARGET_LIMITS",
            "hard_limits": hard_limits,
            "target_limits": target_limits,
            "buffer_pct": buffer_pct,
            "start_totals": start_totals,
            "end_totals": totals,
            "actions": [],
        }
        atomic_write(out_path, out)
        print(f"Wrote {out_path}")
        return out

    work = {p["symbol"]: dict(p) for p in positions}
    actions = {sym: {"symbol": sym, "close_side": None, "qty": 0} for sym in work.keys()}

    closed = 0
    while closed < max_contracts_to_close and not within_limits(totals, target_limits) and work:
        rows = list(work.values())
        rows.sort(key=lambda r: score_row(r, totals, target_limits), reverse=True)

        best = rows[0]
        if score_row(best, totals, target_limits) <= 0:
            break

        sym = best["symbol"]
        nq = int(best["net_qty"])
        direction = sgn(nq)
        if direction == 0:
            work.pop(sym, None)
            continue

        close_side = "SELL" if nq > 0 else "BUY"
        eff = close_one_contract_effect(best)

        totals["delta"] = float(totals.get("delta", 0.0)) + eff["delta"]
        totals["gamma"] = float(totals.get("gamma", 0.0)) + eff["gamma"]
        totals["vega"] = float(totals.get("vega", 0.0)) + eff["vega"]

        best["net_qty"] = nq - direction

        a = actions[sym]
        a["close_side"] = close_side
        a["qty"] += 1

        if int(best["net_qty"]) == 0:
            work.pop(sym, None)

        closed += 1

    action_list = [a for a in actions.values() if a["qty"] > 0]

    out = {
        "ts": utc_iso(),
        "status": "OK" if within_limits(totals, target_limits) else "PARTIAL",
        "hard_limits": hard_limits,
        "target_limits": target_limits,
        "buffer_pct": buffer_pct,
        "start_totals": start_totals,
        "end_totals": totals,
        "actions": action_list,
    }
    atomic_write(out_path, out)
    print(f"Wrote {out_path}")
    return out


def main():
    build_derisk_plan()


if __name__ == "__main__":
    main()
