import json
import os
from datetime import datetime, timezone
from typing import Any


def utc_iso():
    return datetime.now(timezone.utc).isoformat()


def append_jsonl(path: str, obj: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(obj) + "\n")


def record_fill(symbol: str, qty: int, side: str, price: float, tag: str):
    evt: dict[str, Any] = {
        "ts": utc_iso(),
        "type": "FILL",
        "symbol": symbol,
        "qty": int(qty),
        "side": side,
        "price": float(price),
        "tag": tag,
    }
    append_jsonl("state/positions.jsonl", evt)
    return evt
