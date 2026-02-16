import os
from dataclasses import dataclass
from datetime import datetime, timezone

from dotenv import load_dotenv
from alpaca.data.historical import OptionHistoricalDataClient
from alpaca.data.requests import OptionLatestQuoteRequest


def utc_now():
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class OptionQuote:
    symbol: str
    bid: float
    ask: float
    mid: float
    spread_pct: float


def get_option_quotes(symbols: list[str]) -> dict[str, OptionQuote]:
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"), override=True)
    key = os.getenv("ALPACA_API_KEY", "").strip()
    secret = os.getenv("ALPACA_SECRET_KEY", "").strip()
    if not key or not secret:
        raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY")

    client = OptionHistoricalDataClient(key, secret)
    resp = client.get_option_latest_quote(OptionLatestQuoteRequest(symbol_or_symbols=symbols))

    out = {}
    for sym in symbols:
        q = resp.get(sym)
        if q is None:
            continue
        bid = float(q.bid_price) if q.bid_price is not None else 0.0
        ask = float(q.ask_price) if q.ask_price is not None else 0.0
        if bid <= 0 or ask <= 0 or ask < bid:
            continue
        mid = 0.5 * (bid + ask)
        spr = ask - bid
        spr_pct = (spr / mid) * 100 if mid > 0 else 999.0
        out[sym] = OptionQuote(symbol=sym, bid=bid, ask=ask, mid=mid, spread_pct=spr_pct)

    return out
