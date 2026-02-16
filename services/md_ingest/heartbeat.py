import os
import time
import json
import threading
from datetime import datetime, timezone

from dotenv import load_dotenv

from alpaca.trading.client import TradingClient
from alpaca.data.live import StockDataStream
from alpaca.data.historical import StockHistoricalDataClient, OptionHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest, StockLatestTradeRequest, OptionChainRequest
from alpaca.data.enums import DataFeed


def utc_now():
    return datetime.now(timezone.utc)


def ms_since(dt):
    if dt is None:
        return None
    return int((utc_now() - dt).total_seconds() * 1000)


def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None


def parse_feed(feed_str):
    f = (feed_str or "iex").lower().strip()
    return DataFeed.SIP if f == "sip" else DataFeed.IEX


def quote_metrics(bid, ask):
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return None, None, None
    mid = 0.5 * (bid + ask)
    spr = ask - bid
    spr_pct = (spr / mid) * 100 if mid > 0 else None
    return mid, spr, spr_pct


def atomic_write_json(path: str, obj: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


class HB:
    def __init__(self, underliers):
        self.underliers = underliers
        self.latest_quote = {s: None for s in underliers}
        self.latest_quote_recv_ts = {s: None for s in underliers}
        self.latest_trade = {s: None for s in underliers}
        self.latest_trade_recv_ts = {s: None for s in underliers}
        self.chain_last_poll_ts = {s: None for s in underliers}
        self.chain_contract_count = {s: 0 for s in underliers}
        self.stream_ok = False
        self.stream_error = None


def main():
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"), override=True)

    key = os.getenv("ALPACA_API_KEY", "").strip()
    secret = os.getenv("ALPACA_SECRET_KEY", "").strip()
    paper = os.getenv("ALPACA_PAPER", "true").lower() == "true"
    feed = parse_feed(os.getenv("ALPACA_STOCK_FEED", "iex"))

    if not key or not secret:
        raise SystemExit("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY")

    underliers = ["SPY", "QQQ"]
    hb = HB(underliers)

    # Auth check
    tc = TradingClient(key, secret, paper=paper)
    acct = tc.get_account()
    print(f"[REST AUTH OK] paper={paper} account_id={acct.id} status={acct.status}")

    stock_rest = StockHistoricalDataClient(key, secret)
    opt_rest = OptionHistoricalDataClient(key, secret)

    # Streaming quotes (best effort)
    sds = StockDataStream(key, secret, feed=feed)

    async def on_quote(data):
        hb.latest_quote[data.symbol] = data
        hb.latest_quote_recv_ts[data.symbol] = utc_now()

    for s in underliers:
        sds.subscribe_quotes(on_quote, s)

    def run_stream():
        try:
            hb.stream_ok = True
            sds.run()
        except Exception as e:
            hb.stream_ok = False
            hb.stream_error = f"{type(e).__name__}: {e}"
            print(f"[STREAM ERROR] {hb.stream_error}")

    threading.Thread(target=run_stream, daemon=True).start()

    def poll_latest_quotes(symbols):
        return stock_rest.get_stock_latest_quote(StockLatestQuoteRequest(symbol_or_symbols=symbols))

    def poll_latest_trades(symbols):
        return stock_rest.get_stock_latest_trade(StockLatestTradeRequest(symbol_or_symbols=symbols))

    def poll_chain(symbol):
        return opt_rest.get_option_chain(OptionChainRequest(underlying_symbol=symbol))

    print("Heartbeat started. Ctrl+C to stop.\n")

    last_quote_poll = utc_now()
    last_trade_poll = utc_now()

    try:
        while True:
            now = utc_now()

            # chain every 10s
            for s in underliers:
                if hb.chain_last_poll_ts[s] is None or (now - hb.chain_last_poll_ts[s]).total_seconds() >= 10:
                    try:
                        chain = poll_chain(s)
                        hb.chain_contract_count[s] = sum(1 for _ in chain)
                    except Exception as e:
                        print(f"[CHAIN ERROR] {s}: {e}")
                        hb.chain_contract_count[s] = 0
                    hb.chain_last_poll_ts[s] = now

            # trades every 2s (spot anchor)
            if (now - last_trade_poll).total_seconds() >= 2:
                try:
                    tr = poll_latest_trades(underliers)
                    for s in underliers:
                        t = tr.get(s)
                        if t:
                            hb.latest_trade[s] = t
                            hb.latest_trade_recv_ts[s] = now
                except Exception as e:
                    print(f"[TRADE POLL ERROR]: {e}")
                last_trade_poll = now

            # quote poll fallback if stream not updating
            need_quote_poll = (not hb.stream_ok)
            if not need_quote_poll:
                for s in underliers:
                    ts = hb.latest_quote_recv_ts[s]
                    if ts is None or (now - ts).total_seconds() > 5:
                        need_quote_poll = True
                        break

            if need_quote_poll and (now - last_quote_poll).total_seconds() >= 2:
                try:
                    qr = poll_latest_quotes(underliers)
                    for s in underliers:
                        q = qr.get(s)
                        if q:
                            hb.latest_quote[s] = q
                            hb.latest_quote_recv_ts[s] = now
                except Exception as e:
                    print(f"[QUOTE POLL ERROR]: {e}")
                last_quote_poll = now

            # build market_state snapshot
            snapshot = {"ts": now.isoformat(), "symbols": {}}

            # print + snapshot per symbol
            parts = []
            for s in underliers:
                t = hb.latest_trade[s]
                trade_px = safe_float(getattr(t, "price", None)) if t else None
                trade_age = ms_since(hb.latest_trade_recv_ts[s])

                q = hb.latest_quote[s]
                bid = safe_float(getattr(q, "bid_price", None)) if q else None
                ask = safe_float(getattr(q, "ask_price", None)) if q else None
                q_age = ms_since(hb.latest_quote_recv_ts[s])

                mid, spr, spr_pct = quote_metrics(bid, ask)

                # spot router: prefer sane mid, else trade
                spot = None
                spot_src = "NONE"
                spot_age = None
                if mid is not None and spr_pct is not None and spr_pct <= 2.0:
                    spot = mid
                    spot_src = "MID"
                    spot_age = q_age
                elif trade_px is not None:
                    spot = trade_px
                    spot_src = "TRADE"
                    spot_age = trade_age

                chain_age = ms_since(hb.chain_last_poll_ts[s])

                snapshot["symbols"][s] = {
                    "spot": spot,
                    "spot_src": spot_src,
                    "spot_age_ms": spot_age,
                    "trade_px": trade_px,
                    "trade_age_ms": trade_age,
                    "bid": bid,
                    "ask": ask,
                    "quote_age_ms": q_age,
                    "quote_spread_pct": spr_pct,
                    "chain_contracts": hb.chain_contract_count[s],
                    "chain_age_ms": chain_age,
                }

                if spot is None:
                    spot_str = "spot=None(NONE)"
                else:
                    spot_str = f"spot={spot:.2f}({spot_src}) age_ms={spot_age}"

                q_str = "quote=NONE" if q is None else (
                    f"bid={bid} ask={ask} spr%={spr_pct} age_ms={q_age}"
                )

                parts.append(f"{s} | {spot_str} | {q_str} | chain_age_ms={chain_age}")

            atomic_write_json("state/market_state.json", snapshot)

            stream_status = "OK" if hb.stream_ok else f"DOWN({hb.stream_error})"
            print(now.strftime("%H:%M:%S"), f"| feed={feed.name} stream={stream_status} |", " || ".join(parts))

            time.sleep(2)

    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        try:
            sds.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()
