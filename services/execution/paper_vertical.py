import os
import json
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.common.exceptions import APIError

from services.options_refdata.resolve_vertical import resolve_vertical
from services.execution.option_quotes import get_option_quotes


def utc_iso():
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class ExecResult:
    ok: bool
    mode: str                 # PLAN_ONLY | SUBMITTED | EXPECTED_BLOCK
    reasons: list[str]
    plan: dict
    orders: list[dict]


def place_paper_vertical(
    underlier: str,
    is_call: bool,
    K_long: float,
    K_short: float,
    dte_days: int,
    qty: int,
    submit: bool = False,               # default SAFE: plan only
    max_leg_spread_pct: float = 5.0,
) -> ExecResult:
    """
    Execution behavior:
      - Always resolves contracts and builds a 2-leg plan
      - By default DOES NOT submit (submit=False)
      - If submit=True, attempts sequential legs (BUY long then SELL short)
        and handles broker blocks explicitly.
    """
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"), override=True)
    key = os.getenv("ALPACA_API_KEY", "").strip()
    secret = os.getenv("ALPACA_SECRET_KEY", "").strip()
    paper = os.getenv("ALPACA_PAPER", "true").lower() == "true"

    if not key or not secret:
        return ExecResult(False, "PLAN_ONLY", ["MISSING_KEYS"], {}, [])

    # Resolve contracts
    rv = resolve_vertical(underlier, is_call, K_long, K_short, dte_days)
    syms = [rv.long_leg.symbol, rv.short_leg.symbol]

    # Quotes
    quotes = get_option_quotes(syms)
    reasons = []
    if rv.long_leg.symbol not in quotes:
        reasons.append("NO_QUOTE_LONG_LEG")
    if rv.short_leg.symbol not in quotes:
        reasons.append("NO_QUOTE_SHORT_LEG")
    if reasons:
        return ExecResult(False, "PLAN_ONLY", reasons, {}, [])

    qL = quotes[rv.long_leg.symbol]
    qS = quotes[rv.short_leg.symbol]

    if qL.spread_pct > max_leg_spread_pct:
        reasons.append(f"LONG_LEG_WIDE_SPREAD_{qL.spread_pct:.2f}%")
    if qS.spread_pct > max_leg_spread_pct:
        reasons.append(f"SHORT_LEG_WIDE_SPREAD_{qS.spread_pct:.2f}%")
    if reasons:
        return ExecResult(False, "PLAN_ONLY", reasons, {}, [])

    # Build executable plan (even if we don't submit)
    plan = {
        "ts": utc_iso(),
        "type": "VERTICAL",
        "underlier": underlier,
        "expiration": rv.expiration,
        "is_call": is_call,
        "qty": qty,
        "legs": [
            {"symbol": rv.long_leg.symbol, "side": "BUY", "limit": round(qL.mid, 2), "bid": qL.bid, "ask": qL.ask, "spr_pct": qL.spread_pct},
            {"symbol": rv.short_leg.symbol, "side": "SELL", "limit": round(qS.mid, 2), "bid": qS.bid, "ask": qS.ask, "spr_pct": qS.spread_pct},
        ],
        "notes": [
            "MVP uses 2 single-leg orders. Broker may block short leg as 'uncovered' unless account approved for spreads.",
            "Production fix = atomic multi-leg order (broker-supported) or PB/prime OMS.",
        ],
    }

    # Plan-only mode (default)
    if not submit:
        return ExecResult(True, "PLAN_ONLY", [], plan, [])

    # Submit mode (best-effort): BUY long first, then SELL short
    tc = TradingClient(key, secret, paper=paper)
    orders_out = []

    try:
        buy_req = LimitOrderRequest(
            symbol=rv.long_leg.symbol,
            qty=qty,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
            limit_price=round(qL.mid, 2),
        )
        buy_order = tc.submit_order(buy_req)
        orders_out.append({"ts": utc_iso(), "leg": "LONG_BUY", "symbol": rv.long_leg.symbol, "limit": round(qL.mid, 2), "id": str(buy_order.id)})

        # small delay to let positions update in paper; still not atomic
        time.sleep(1.0)

        sell_req = LimitOrderRequest(
            symbol=rv.short_leg.symbol,
            qty=qty,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
            limit_price=round(qS.mid, 2),
        )
        sell_order = tc.submit_order(sell_req)
        orders_out.append({"ts": utc_iso(), "leg": "SHORT_SELL", "symbol": rv.short_leg.symbol, "limit": round(qS.mid, 2), "id": str(sell_order.id)})

        return ExecResult(True, "SUBMITTED", [], plan, orders_out)

    except APIError as e:
        msg = str(e)
        # Detect the specific Alpaca options eligibility block
        if "not eligible to trade uncovered option contracts" in msg:
            return ExecResult(False, "EXPECTED_BLOCK", ["ALPACA_OPTIONS_PERMISSION_BLOCK"], plan, orders_out)
        return ExecResult(False, "EXPECTED_BLOCK", [f"API_ERROR: {msg}"], plan, orders_out)

    except Exception as e:
        return ExecResult(False, "EXPECTED_BLOCK", [f"EXCEPTION: {type(e).__name__}: {e}"], plan, orders_out)


def demo():
    # SAFE default: plan only
    res = place_paper_vertical(
        underlier="QQQ",
        is_call=True,
        K_long=600,
        K_short=610,
        dte_days=30,
        qty=1,
        submit=False,
    )
    print(json.dumps(asdict(res), indent=2))


if __name__ == "__main__":
    demo()
