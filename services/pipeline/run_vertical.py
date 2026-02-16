import json
import os
from dataclasses import asdict

from services.pretrade_gateway.gateway import VerticalIntent, gate_intent, load_market_state_from_snapshot
from services.options_refdata.resolve_vertical import resolve_vertical
from services.execution.option_quotes import get_option_quotes


def write_json(path: str, obj: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)


def main():
    # ---- Configure the "strategy intent" here (MVP) ----
    intent = VerticalIntent(
        underlier="QQQ",
        is_call=True,
        K_long=600,
        K_short=610,
        dte_days=30,
        r=0.04,
        iv_long=0.22,
        iv_short=0.22,
        qty_requested=10,
        tag="PIPELINE_QQQ_600_610C",
    )

    # 1) Gate (reads live state/market_state.json)
    gate_out = gate_intent(intent)
    print(json.dumps(gate_out, indent=2))

    if not gate_out["allow"]:
        write_json("state/final_plan.json", {"status": "DENIED", "gate": gate_out})
        return

    qty = gate_out["order_plan"]["qty"]

    # 2) Resolve contracts
    rv = resolve_vertical(
        underlier=intent.underlier,
        is_call=intent.is_call,
        K_long=intent.K_long,
        K_short=intent.K_short,
        dte_days=intent.dte_days,
    )

    # 3) Get option quotes for both legs
    syms = [rv.long_leg.symbol, rv.short_leg.symbol]
    quotes = get_option_quotes(syms)

    long_q = quotes.get(rv.long_leg.symbol)
    short_q = quotes.get(rv.short_leg.symbol)

    if long_q is None or short_q is None:
        final = {
            "status": "BLOCKED",
            "reason": "MISSING_OPTION_QUOTES",
            "gate": gate_out,
            "resolved": asdict(rv),
        }
        write_json("state/final_plan.json", final)
        print("\nWrote state/final_plan.json")
        return

    final = {
        "status": "PLAN_ONLY",
        "gate": gate_out,
        "resolved": asdict(rv),
        "legs": [
            {"symbol": rv.long_leg.symbol, "side": "BUY", "qty": qty, "limit": round(long_q.mid, 2), "spr_pct": long_q.spread_pct},
            {"symbol": rv.short_leg.symbol, "side": "SELL", "qty": qty, "limit": round(short_q.mid, 2), "spr_pct": short_q.spread_pct},
        ],
        "notes": [
            "Execution is PLAN_ONLY until broker supports atomic spreads / account approved.",
            "When enabled, execution module can submit long leg then short leg with fill-state machine.",
        ],
    }

    write_json("state/final_plan.json", final)
    print("\nWrote state/final_plan.json")


if __name__ == "__main__":
    main()

