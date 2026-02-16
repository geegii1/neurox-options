import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple


STATE_GREEKS = "state/portfolio_greeks.json"
STATE_BOOK = "state/positions_book.json"
STATE_MARKET = "state/market_state.json"


def utc_iso() -> str:
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


# ----------------------------
# OCC option symbology parsing
# Example: QQQ260320C00600000
# root=QQQ, yymmdd=260320, C/P, strike=00600000 => 600.0
# ----------------------------
@dataclass(frozen=True)
class ParsedOCC:
    underlier: str
    exp: str          # YYYY-MM-DD
    is_call: bool
    strike: float


def parse_occ_symbol(sym: str) -> Optional[ParsedOCC]:
    try:
        # find the 6-digit date chunk: yymmdd
        # OCC format is root + yymmdd + C/P + strike(8)
        # root can be variable length, but date chunk is exactly 6 digits.
        # We'll scan for the first 6-digit run.
        s = sym.strip()
        if len(s) < 6 + 1 + 8:
            return None

        idx = None
        for i in range(0, len(s) - 6):
            chunk = s[i:i+6]
            if chunk.isdigit():
                idx = i
                break
        if idx is None:
            return None

        root = s[:idx]
        yymmdd = s[idx:idx+6]
        cp = s[idx+6:idx+7]
        strike8 = s[idx+7:idx+15]

        if cp not in ("C", "P"):
            return None
        if not strike8.isdigit():
            return None

        yy = int(yymmdd[0:2])
        mm = int(yymmdd[2:4])
        dd = int(yymmdd[4:6])
        yyyy = 2000 + yy
        exp = f"{yyyy:04d}-{mm:02d}-{dd:02d}"

        strike = int(strike8) / 1000.0
        return ParsedOCC(underlier=root, exp=exp, is_call=(cp == "C"), strike=strike)
    except Exception:
        return None


def yearfrac_from_iso(exp_iso: str, now: Optional[datetime] = None) -> float:
    if now is None:
        now = datetime.now(timezone.utc)
    yyyy, mm, dd = exp_iso.split("-")
    exp = datetime(int(yyyy), int(mm), int(dd), 16, 0, 0, tzinfo=timezone.utc)  # approx 4pm ET close
    dt = (exp - now).total_seconds()
    if dt <= 0:
        return 0.0
    return dt / (365.0 * 24.0 * 3600.0)


# ----------------------------
# Normal helpers
# ----------------------------
def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def norm_pdf(x: float) -> float:
    return (1.0 / math.sqrt(2.0 * math.pi)) * math.exp(-0.5 * x * x)


def safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


# ----------------------------
# Black-Scholes (no dividends)
# Contract multiplier = 100
# Vega is per 1.00 vol (not 1%)
# ----------------------------
def bs_d1_d2(S: float, K: float, T: float, r: float, sigma: float) -> Tuple[float, float]:
    if sigma <= 0 or T <= 0 or S <= 0 or K <= 0:
        return 0.0, 0.0
    vsqrt = sigma * math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / vsqrt
    d2 = d1 - vsqrt
    return d1, d2


def bs_price(S: float, K: float, T: float, r: float, sigma: float, is_call: bool) -> float:
    if T <= 0:
        intrinsic = max(0.0, S - K) if is_call else max(0.0, K - S)
        return intrinsic
    if sigma <= 0:
        # near-zero vol -> discounted intrinsic-ish
        intrinsic = max(0.0, S - K) if is_call else max(0.0, K - S)
        return intrinsic

    d1, d2 = bs_d1_d2(S, K, T, r, sigma)
    df = math.exp(-r * T)
    if is_call:
        return S * norm_cdf(d1) - K * df * norm_cdf(d2)
    else:
        return K * df * norm_cdf(-d2) - S * norm_cdf(-d1)


def bs_greeks_per_contract(S: float, K: float, T: float, r: float, sigma: float, is_call: bool) -> Dict[str, float]:
    """
    Returns greeks per 1 contract, with multiplier=100:
      delta in shares equiv
      gamma in shares equiv per $ move
      vega in $ per 1.00 vol
      theta in $ per year
    """
    mult = 100.0
    if T <= 0 or sigma <= 0:
        # crude fallback
        intrinsic = max(0.0, S - K) if is_call else max(0.0, K - S)
        delta = mult * (1.0 if (is_call and S > K) else (-1.0 if (not is_call and S < K) else 0.0))
        return {"delta": delta, "gamma": 0.0, "vega": 0.0, "theta": 0.0}

    d1, d2 = bs_d1_d2(S, K, T, r, sigma)
    pdf1 = norm_pdf(d1)
    df = math.exp(-r * T)

    if is_call:
        delta = mult * norm_cdf(d1)
    else:
        delta = mult * (norm_cdf(d1) - 1.0)

    gamma = mult * (pdf1 / (S * sigma * math.sqrt(T)))
    vega = mult * (S * pdf1 * math.sqrt(T))  # per 1.00 vol

    # theta per year (common in BS). Keep consistent with your existing huge theta values.
    if is_call:
        theta = mult * (-(S * pdf1 * sigma) / (2.0 * math.sqrt(T)) - r * K * df * norm_cdf(d2))
    else:
        theta = mult * (-(S * pdf1 * sigma) / (2.0 * math.sqrt(T)) + r * K * df * norm_cdf(-d2))

    return {"delta": delta, "gamma": gamma, "vega": vega, "theta": theta}


# ----------------------------
# Implied Vol: Newton + Robust Bisection
# ----------------------------
def implied_vol_newton(
    target_price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    is_call: bool,
    x0: float = 0.30,
    iters: int = 20,
) -> Optional[float]:
    if target_price <= 0 or S <= 0 or K <= 0 or T <= 0:
        return None

    sigma = max(1e-6, float(x0))
    for _ in range(iters):
        px = bs_price(S, K, T, r, sigma, is_call)
        diff = px - target_price
        if abs(diff) < 1e-7:
            return sigma

        d1, _ = bs_d1_d2(S, K, T, r, sigma)
        vega = S * norm_pdf(d1) * math.sqrt(T)  # per 1.00 vol (no *100 here)
        if vega <= 1e-10:
            return None

        sigma = sigma - diff / vega
        if sigma <= 1e-6:
            sigma = 1e-6
        if sigma > 8.0:
            sigma = 8.0
    return None


def implied_vol_bisect(
    target_price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    is_call: bool,
    lo: float = 0.01,
    hi: float = 1.0,
    iters: int = 60,
) -> Optional[float]:
    """
    Robust bisection with dynamic bracketing:
    Expand hi until bs_price(hi) >= target_price (or hi cap reached).
    """
    if target_price <= 0 or S <= 0 or K <= 0 or T <= 0:
        return None

    p_lo = bs_price(S, K, T, r, lo, is_call)
    if target_price < p_lo:
        return None

    hi_cap = 8.0
    p_hi = bs_price(S, K, T, r, hi, is_call)
    while p_hi < target_price and hi < hi_cap:
        hi *= 2.0
        p_hi = bs_price(S, K, T, r, hi, is_call)

    if p_hi < target_price:
        return None

    a, b = lo, hi
    for _ in range(iters):
        m = 0.5 * (a + b)
        pm = bs_price(S, K, T, r, m, is_call)
        if abs(pm - target_price) < 1e-7:
            return m
        if pm < target_price:
            a = m
        else:
            b = m
    return 0.5 * (a + b)


# ----------------------------
# Snapshot builder
# ----------------------------
def load_spot(underlier: str) -> Tuple[Optional[float], Optional[str]]:
    """
    Uses state/market_state.json written by md_ingest/heartbeat.py.
    Prefers TRADE, then MID, else None.
    """
    try:
        ms = read_json(STATE_MARKET)
        sym = ms.get("symbols", {}).get(underlier, {})
        spot = safe_float(sym.get("spot"))
        src = sym.get("spot_src")
        return spot, src
    except Exception:
        return None, None


def load_prev_mids() -> Dict[str, Dict[str, float]]:
    """
    Preserve last known mid/spr_pct so greeks can recompute consistently even if we
    don't have a fresh option quote module yet.
    """
    out: Dict[str, Dict[str, float]] = {}
    try:
        prev = read_json(STATE_GREEKS)
        for p in prev.get("positions", []):
            sym = p.get("symbol")
            if not sym:
                continue
            out[sym] = {
                "mid": float(p.get("mid", 0.0)),
                "spr_pct": float(p.get("spr_pct", 0.0)) if p.get("spr_pct") is not None else 0.0,
            }
    except Exception:
        pass
    return out


def build_portfolio_greeks(
    book_path: str = STATE_BOOK,
    out_path: str = STATE_GREEKS,
    r: float = 0.0,
    default_iv: float = 0.25,
) -> dict:
    book = read_json(book_path)
    prev_mids = load_prev_mids()

    rows_out = []
    totals = {"delta": 0.0, "gamma": 0.0, "vega": 0.0, "theta": 0.0}

    for pos in book.get("positions", []):
        sym = pos.get("symbol")
        net_qty = int(pos.get("net_qty", 0))
        if not sym or net_qty == 0:
            continue

        occ = parse_occ_symbol(sym)
        if occ is None:
            continue

        spot, spot_src = load_spot(occ.underlier)
        if spot is None:
            spot = 0.0

        T = yearfrac_from_iso(occ.exp)
        # use last known mid if we don't have fresh quotes
        mid = prev_mids.get(sym, {}).get("mid", 0.0)
        spr_pct = prev_mids.get(sym, {}).get("spr_pct", 0.0)

        target = float(mid)  # per-share option price
        iv = None
        iv_src = None

        if target > 0 and spot > 0 and T > 0:
            # Try Newton first; if fails, robust bisection with dynamic bracketing
            iv_n = implied_vol_newton(target, spot, occ.strike, T, r, occ.is_call, x0=0.30)
            if iv_n is not None and 1e-6 < iv_n <= 8.0:
                iv = iv_n
                iv_src = "NEWTON"
            else:
                iv_b = implied_vol_bisect(target, spot, occ.strike, T, r, occ.is_call, lo=0.01, hi=1.0)
                if iv_b is not None and 1e-6 < iv_b <= 8.0:
                    iv = iv_b
                    iv_src = "BISECT"

        if iv is None:
            iv = float(default_iv)
            iv_src = "FALLBACK_DEFAULT"

        g_pc = bs_greeks_per_contract(spot, occ.strike, T if T > 0 else 1e-9, r, iv, occ.is_call)
        # position-weighted
        delta = g_pc["delta"] * net_qty
        gamma = g_pc["gamma"] * net_qty
        vega = g_pc["vega"] * net_qty
        theta = g_pc["theta"] * net_qty

        row = {
            "symbol": sym,
            "underlier": occ.underlier,
            "exp": occ.exp,
            "is_call": occ.is_call,
            "strike": occ.strike,
            "spot": spot if spot > 0 else None,
            "spot_src": spot_src,
            "net_qty": net_qty,
            "mid": mid,
            "spr_pct": spr_pct,
            "iv": iv,
            "iv_src": iv_src,
            "delta": delta,
            "gamma": gamma,
            "vega": vega,
            "theta": theta,
        }
        rows_out.append(row)

        totals["delta"] += delta
        totals["gamma"] += gamma
        totals["vega"] += vega
        totals["theta"] += theta

    out = {"ts": utc_iso(), "positions": rows_out, "totals": totals}
    atomic_write(out_path, out)
    print(f"Wrote {out_path}")
    return out


def main():
    build_portfolio_greeks()


if __name__ == "__main__":
    main()
