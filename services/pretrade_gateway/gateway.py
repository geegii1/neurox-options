import json
import os
from datetime import datetime, timezone


STATE_MARKET = "state/market_state.json"
STATE_GATE_OUT = "state/gate_out.json"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write(path: str, obj) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)


def safe_float(x):
    try:
        if x is None:
            return None
        v = float(x)
        if v != v:
            return None
        return v
    except Exception:
        return None


def read_market_ctx(underlier: str):
    """
    Reads underlier spot/bid/ask from state/market_state.json written by md_ingest.
    """
    if not os.path.exists(STATE_MARKET):
        return {
            "spot": None,
            "spot_src": "NONE",
            "bid": None,
            "ask": None,
            "quote_spread_pct": None,
            "chain_contracts": None,
        }

    with open(STATE_MARKET, "r") as f:
        ms = json.load(f)

    s = ms.get("symbols", {}).get(underlier, {}) or {}

    spot = safe_float(s.get("spot"))
    spot_src = s.get("spot_src", "NONE")
    bid = safe_float(s.get("bid"))
    ask = safe_float(s.get("ask"))
    chain_contracts = s.get("chain_contracts")

    spr_pct = None
    if bid and ask and bid > 0 and ask > 0 and ask >= bid:
        mid = 0.5 * (bid + ask)
        spr_pct = ((ask - bid) / mid) * 100.0 if mid > 0 else None

    return {
        "spot": spot,
        "spot_src": spot_src,
        "bid": bid,
        "ask": ask,
        "quote_spread_pct": spr_pct,
        "chain_contracts": chain_contracts,
    }


def decide_vertical_safe(
    underlier: str,
    is_call: bool,
    K_long: float,
    K_short: float,
    dte_days: int,
    qty_requested: int,
):
    """
    Safe wrapper around risk governor. Never throws.
    """
    try:
        from services.risk_governor.main import decide_vertical_from_plan

        d = decide_vertical_from_plan(
            underlier=underlier,
            is_call=is_call,
            K_long=K_long,
            K_short=K_short,
            dte_days=dte_days,
            qty_requested=qty_requested,
        )

        return {
            "allow": bool(d.allow),
            "max_contracts": int(d.max_contracts),
            "reasons": list(d.reasons),
            "worst_pnl_gap10_1": d.worst_pnl_gap10,
            "worst_pnl_combo_1": d.worst_pnl_combo,
        }

    except Exception as e:
        return {
            "allow": False,
            "max_contracts": 0,
            "reasons": [f"RISK_GOVERNOR_ERROR:{type(e).__name__}"],
            "worst_pnl_gap10_1": None,
            "worst_pnl_combo_1": None,
        }


def validate_underlier_liquidity(ctx, max_spread_pct: float = 1.0):
    """
    Basic underlier sanity filter.
    """
    reasons = []
    bid = ctx.get("bid")
    ask = ctx.get("ask")
    spr = ctx.get("quote_spread_pct")

    if bid is None or ask is None:
        reasons.append("NO_UNDERLIER_QUOTE")
    elif bid <= 0 or ask <= 0 or ask < bid:
        reasons.append("BAD_UNDERLIER_QUOTE")
    elif spr is not None and spr > max_spread_pct:
        reasons.append("WIDE_UNDERLIER_QUOTE_SPREAD")

    return reasons


def build_vertical_plan(underlier: str, is_call: bool, K_long: float, K_short: float, dte_days: int, qty: int, tag: str):
    ctx = read_market_ctx(underlier)
    liq_reasons = validate_underlier_liquidity(ctx, max_spread_pct=float(os.getenv("GATE_MAX_UNDERLIER_SPREAD_PCT", "1.0")))

    if liq_reasons:
        return {
            "allow": False,
            "order_plan": None,
            "decision": {
                "allow": False,
                "max_contracts": 0,
                "reasons": liq_reasons,
                "worst_pnl_gap10_1": None,
                "worst_pnl_combo_1": None,
            },
        }

    d = decide_vertical_safe(
        underlier=underlier,
        is_call=is_call,
        K_long=K_long,
        K_short=K_short,
        dte_days=dte_days,
        qty_requested=qty,
    )

    if not d["allow"]:
        return {
            "allow": False,
            "order_plan": None,
            "decision": d,
        }

    qty_final = min(qty, int(d["max_contracts"]))

    return {
        "allow": True,
        "order_plan": {
            "type": "VERTICAL",
            "underlier": underlier,
            "is_call": is_call,
            "K_long": K_long,
            "K_short": K_short,
            "dte_days": dte_days,
            "qty": qty_final,
            "limit_logic": "MID_THEN_STEP",
            "tag": tag,
            "spot_used": ctx.get("spot"),
            "spot_src": ctx.get("spot_src"),
        },
        "decision": d,
    }


def main():
    # Demo plans (keep these simple & deterministic)
    demo1 = build_vertical_plan(
        underlier="QQQ",
        is_call=True,
        K_long=600,
        K_short=610,
        dte_days=30,
        qty=int(os.getenv("DEMO_QQQ_QTY", "10")),
        tag="LIVE_QQQ_GATE",
    )

    demo2 = build_vertical_plan(
        underlier="SPY",
        is_call=True,
        K_long=680,
        K_short=690,
        dte_days=30,
        qty=int(os.getenv("DEMO_SPY_QTY", "5")),
        tag="LIVE_SPY_GATE",
    )

    payload = {"ts": utc_iso(), "out": {"demo1": demo1, "demo2": demo2}}

    # persist + print (print is what tick scrapes)
    atomic_write(STATE_GATE_OUT, payload)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
