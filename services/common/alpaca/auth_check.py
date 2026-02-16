import os
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient

def mask(s: str) -> str:
    if not s:
        return "EMPTY"
    s = s.strip()
    if len(s) <= 8:
        return f"{s[:2]}...{s[-2:]}"
    return f"{s[:3]}...{s[-3:]}"

def main():
    # ensure we load the .env from project root
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"), override=True)

    key = os.getenv("ALPACA_API_KEY", "")
    secret = os.getenv("ALPACA_SECRET_KEY", "")
    paper = os.getenv("ALPACA_PAPER", "true").lower() == "true"

    print("CWD =", os.getcwd())
    print("paper =", paper)
    print("key(mask) =", mask(key), " key_len =", len(key.strip()))
    print("secret_len =", len(secret.strip()))

    if not key.strip() or not secret.strip():
        raise SystemExit("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY in .env")

    tc = TradingClient(key.strip(), secret.strip(), paper=paper)
    acct = tc.get_account()
    print("AUTH OK:", acct.id, acct.status)

if __name__ == "__main__":
    main()
