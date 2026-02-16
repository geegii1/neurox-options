import json
import os
from datetime import datetime, timezone
from typing import Dict, Any


def utc_iso():
    return datetime.now(timezone.utc).isoformat()


def atomic_write(path: str, obj: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)


def load_positions_book(path_jsonl: str = "state/positions.jsonl") -> Dict[str, int]:
    """
    Build net positions per symbol from append-only fills.
    BUY adds qty, SELL subtracts qty.
    """
    book: Dict[str, int] = {}

    if not os.path.exists(path_jsonl):
        return book

    with open(path_jsonl, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            evt = json.loads(line)
            if evt.get("type") != "FILL":
                continue

            sym = str(evt["symbol"])
            qty = int(evt["qty"])
            side = str(evt["side"]).upper()

            sign = 1 if side == "BUY" else -1 if side == "SELL" else 0
            if sign == 0:
                continue

            book[sym] = book.get(sym, 0) + sign * qty

    # drop flat
    book = {k: v for k, v in book.items() if v != 0}
    return book


def write_positions_book(
    path_jsonl: str = "state/positions.jsonl",
    out_path: str = "state/positions_book.json",
):
    book = load_positions_book(path_jsonl)
    snap: Dict[str, Any] = {
        "ts": utc_iso(),
        "positions": [{"symbol": s, "net_qty": q} for s, q in sorted(book.items())],
    }
    atomic_write(out_path, snap)
    print(f"Wrote {out_path}")
    return snap


def main():
    write_positions_book()


if __name__ == "__main__":
    main()
