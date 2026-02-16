"""
Symbol resolution for vertical option spreads.

PLAN:
- Given underlier, call/put flag, strikes, and DTE
- Resolve to OCC option symbols
- Uses Alpaca option chain endpoint in LIVE mode
- In PLAN_ONLY, returns synthetic symbols (safe)

IMPORTANT:
LIVE mode requires:
  ALPACA_API_KEY
  ALPACA_SECRET_KEY
  ALPACA_BASE_URL
"""

import os
import requests
from datetime import datetime, timedelta, timezone


def _nearest_expiration(target_date, expirations):
    """
    Pick expiration closest to target_date
    """
    best = None
    best_diff = None
    for exp in expirations:
        exp_dt = datetime.strptime(exp, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        diff = abs((exp_dt - target_date).days)
        if best is None or diff < best_diff:
            best = exp
            best_diff = diff
    return best


def resolve_vertical_symbols(
    underlier: str,
    is_call: bool,
    K_long: float,
    K_short: float,
    dte_days: int,
    mode: str = "PLAN_ONLY"
):
    """
    Returns:
        {
            "long_symbol": "...",
            "short_symbol": "...",
            "expiration": "YYYY-MM-DD"
        }
    """

    # -------------------------------------------------
    # PLAN_ONLY → safe synthetic symbols
    # -------------------------------------------------
    if mode == "PLAN_ONLY":
        exp = (datetime.utcnow() + timedelta(days=dte_days)).strftime("%Y%m%d")
        callput = "C" if is_call else "P"

        return {
            "long_symbol": f"{underlier}_{exp}_{callput}_{int(K_long)}",
            "short_symbol": f"{underlier}_{exp}_{callput}_{int(K_short)}",
            "expiration": exp,
        }

    # -------------------------------------------------
    # LIVE MODE → Alpaca option chain lookup
    # -------------------------------------------------

    base_url = os.environ.get("ALPACA_BASE_URL")
    key = os.environ.get("ALPACA_API_KEY")
    secret = os.environ.get("ALPACA_SECRET_KEY")

    if not base_url or not key or not secret:
        raise RuntimeError("Missing Alpaca credentials for LIVE mode")

    headers = {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
    }

    # Fetch chain
    url = f"{base_url}/v2/options/contracts?underlying_symbols={underlier}"
    r = requests.get(url, headers=headers, timeout=10)
    r.raise_for_status()
    data = r.json()

    contracts = data.get("option_contracts", [])
    if not contracts:
        raise RuntimeError("No option contracts returned")

    # Target expiration
    target_date = datetime.now(timezone.utc) + timedelta(days=dte_days)
    expirations = sorted({c["expiration_date"] for c in contracts})
    chosen_exp = _nearest_expiration(target_date, expirations)

    # Filter by expiration + call/put
    side = "call" if is_call else "put"
    filtered = [
        c for c in contracts
        if c["expiration_date"] == chosen_exp
        and c["type"] == side
    ]

    if not filtered:
        raise RuntimeError("No contracts match expiration + side")

    # Find exact strikes
    def find_strike(strike):
        for c in filtered:
            if float(c["strike_price"]) == float(strike):
                return c["symbol"]
        return None

    long_symbol = find_strike(K_long)
    short_symbol = find_strike(K_short)

    if not long_symbol or not short_symbol:
        raise RuntimeError("Strike not found in option chain")

    return {
        "long_symbol": long_symbol,
        "short_symbol": short_symbol,
        "expiration": chosen_exp,
    }
