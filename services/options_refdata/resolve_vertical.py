import os
from dataclasses import dataclass
from datetime import date, datetime, timezone, timedelta

from dotenv import load_dotenv
from alpaca.data.historical import OptionHistoricalDataClient
from alpaca.data.requests import OptionChainRequest


def utc_now():
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class ResolvedLeg:
    symbol: str
    strike: float
    is_call: bool


@dataclass(frozen=True)
class ResolvedVertical:
    underlier: str
    expiration: str
    long_leg: ResolvedLeg
    short_leg: ResolvedLeg


def _nearest_expiration(target_days: int) -> str:
    """
    MVP heuristic:
      - Choose the next Friday closest to target_days (US equity options standard)
      - No holiday logic yet (we’ll add later)
    Returns YYYY-MM-DD
    """
    today = date.today()
    target = today + timedelta(days=target_days)

    # find next Friday on/after target
    days_ahead = (4 - target.weekday()) % 7  # Monday=0 ... Friday=4
    exp = target + timedelta(days=days_ahead)
    return exp.isoformat()


def resolve_vertical(
    underlier: str,
    is_call: bool,
    K_long: float,
    K_short: float,
    dte_days: int,
) -> ResolvedVertical:
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"), override=True)
    key = os.getenv("ALPACA_API_KEY", "").strip()
    secret = os.getenv("ALPACA_SECRET_KEY", "").strip()

    if not key or not secret:
        raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY")

    exp = _nearest_expiration(dte_days)

    opt = OptionHistoricalDataClient(key, secret)
    chain = opt.get_option_chain(OptionChainRequest(underlying_symbol=underlier))

    # Filter chain down to that expiration + calls/puts + strike
    long_sym = None
    short_sym = None

    for optsym in chain:
        # optsym is a string option symbol on Alpaca
        # We have to fetch metadata by parsing the OCC symbol format.
        # Alpaca uses OCC-like: e.g., QQQ260320C00600000
        s = str(optsym)

        # quick parse:
        # UNDERLIER + YYMMDD + C/P + strike*1000 padded
        # We'll do best-effort parsing to avoid extra API calls.
        try:
            # find date chunk (6 digits) just before C/P
            # assumes underlier length is 1-6
            ulen = len(underlier)
            yy = int(s[ulen:ulen+2])
            mm = int(s[ulen+2:ulen+4])
            dd = int(s[ulen+4:ulen+6])
            cp = s[ulen+6]
            strike_int = int(s[ulen+7:])
            strike = strike_int / 1000.0
            exp_str = f"20{yy:02d}-{mm:02d}-{dd:02d}"
        except Exception:
            continue

        if exp_str != exp:
            continue
        if (cp == "C") != is_call:
            continue

        if abs(strike - float(K_long)) < 1e-6:
            long_sym = s
        elif abs(strike - float(K_short)) < 1e-6:
            short_sym = s

        if long_sym and short_sym:
            break

    if not long_sym or not short_sym:
        raise RuntimeError(
            f"Could not resolve both legs for {underlier} exp={exp} "
            f"{'C' if is_call else 'P'} K_long={K_long} K_short={K_short}. "
            f"Found long={long_sym} short={short_sym}"
        )

    return ResolvedVertical(
        underlier=underlier,
        expiration=exp,
        long_leg=ResolvedLeg(symbol=long_sym, strike=float(K_long), is_call=is_call),
        short_leg=ResolvedLeg(symbol=short_sym, strike=float(K_short), is_call=is_call),
    )


def demo():
    v = resolve_vertical("QQQ", True, 600, 610, 30)
    print(v)


if __name__ == "__main__":
    demo()
