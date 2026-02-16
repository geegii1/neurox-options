import json
import os
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

from services.risk_governor.risk_mode import get_risk_mode, allow_close_trades


STATE_CLOSE_INTENT = "state/close_intent.json"
STATE_POSITIONS_BOOK = "state/positions_book.json"
STATE_PORTFOLIO_GREEKS = "state/portfolio_greeks.json"
STATE_OMS_CLOSE = "state/oms_close_state.json"
LOCK_PATH = "state/oms_close.lock"


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


def now_epoch() -> float:
    return time.time()


def parse_iso(ts: str) -> float:
    # Returns epoch seconds
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


def acquire_lock(path: str) -> bool:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        os.close(fd)
        return True
    except FileExistsError:
        return False


def release_lock(path: str):
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def positions_to_map(book: dict) -> Dict[str, int]:
    m: Dict[str, int] = {}
    for p in book.get("positions", []):
        sym = str(p.get("symbol"))
        q = p.get("net_qty")
        try:
            q = int(q)
        except Exception:
            q = 0
        if sym:
            m[sym] = q
    return m


def map_to_positions(m: Dict[str, int]) -> List[dict]:
    out = []
    for sym, q in m.items():
        if q != 0:
            out.append({"symbol": sym, "net_qty": q})
    out.sort(key=lambda x: x["symbol"])
    return out


def infer_underlier_from_occ(occ: str) -> str:
    s = occ.strip()
    for i in range(0, len(s) - 6):
        if s[i:i + 6].isdigit():
            return s[:i]
    return ""


def price_proxy_for_symbol(sym: str) -> Optional[float]:
    """
    PLAN_ONLY: use portfolio_greeks mid as a proxy if available.
    """
    if not os.path.exists(STATE_PORTFOLIO_GREEKS):
        return None
    try:
        g = read_json(STATE_PORTFOLIO_GREEKS)
        for p in g.get("positions", []):
            if p.get("symbol") == sym:
                mid = p.get("mid")
                try:
                    return float(mid)
                except Exception:
                    return None
    except Exception:
        return None
    return None


def normalize_actions(actions: List[dict]) -> List[dict]:
    """
    Aggregate by symbol+side so we don't do 20 tiny actions.
    close_side is BUY or SELL.
    """
    agg: Dict[Tuple[str, str], int] = {}
    for a in actions:
        sym = str(a.get("symbol", "")).strip()
        side = str(a.get("close_side", "")).upper().strip()
        qty = a.get("qty", 0)
        try:
            qty = int(qty)
        except Exception:
            qty = 0
        if not sym or side not in ("BUY", "SELL") or qty <= 0:
            continue
        k = (sym, side)
        agg[k] = agg.get(k, 0) + qty

    out = [{"symbol": k[0], "close_side": k[1], "qty": v} for k, v in agg.items()]
    out.sort(key=lambda x: (x["symbol"], x["close_side"]))
    return out


def validate_reduce_only(actions: List[dict], pos_map: Dict[str, int]) -> List[str]:
    """
    Ensure every action reduces exposure and does not exceed net qty.
    Rules:
      - if net_qty > 0: allowed close_side must be SELL, qty <= net_qty
      - if net_qty < 0: allowed close_side must be BUY,  qty <= abs(net_qty)
      - if net_qty == 0: no close action allowed
    """
    breaches = []
    for a in actions:
        sym = a["symbol"]
        side = a["close_side"]
        qty = int(a["qty"])
        net = int(pos_map.get(sym, 0))

        if net == 0:
            breaches.append(f"REDUCE_ONLY_VIOLATION {sym} net=0 action={side} qty={qty}")
            continue

        if net > 0:
            if side != "SELL":
                breaches.append(f"REDUCE_ONLY_VIOLATION {sym} net={net} requires SELL got {side}")
            if qty > net:
                breaches.append(f"REDUCE_ONLY_VIOLATION {sym} qty {qty} > net {net}")
        else:  # net < 0
            if side != "BUY":
                breaches.append(f"REDUCE_ONLY_VIOLATION {sym} net={net} requires BUY got {side}")
            if qty > abs(net):
                breaches.append(f"REDUCE_ONLY_VIOLATION {sym} qty {qty} > abs(net) {abs(net)}")
    return breaches


def apply_fill(pos_map: Dict[str, int], sym: str, side: str, qty: int):
    """
    Apply filled close:
      - SELL decreases net_qty
      - BUY increases net_qty
    """
    net = int(pos_map.get(sym, 0))
    if side == "SELL":
        net -= qty
    elif side == "BUY":
        net += qty
    pos_map[sym] = net
    if pos_map[sym] == 0:
        # keep map clean
        pos_map.pop(sym, None)


def main():
    if not acquire_lock(LOCK_PATH):
        atomic_write(
            STATE_OMS_CLOSE,
            {
                "ts": utc_iso(),
                "mode": "PLAN_ONLY",
                "state": "LOCKED",
                "reason": "ANOTHER_OMS_CLOSE_RUNNING",
            },
        )
        print(f"Wrote {STATE_OMS_CLOSE}")
        return

    try:
        rm = get_risk_mode()
        mode = "PLAN_ONLY"  # keep consistent with your current system

        if not allow_close_trades():
            out = {
                "ts": utc_iso(),
                "mode": mode,
                "risk_mode": rm.mode.value,
                "state": "HALT",
                "reason": f"RISK_MODE_BLOCKS_CLOSE:{rm.reason}",
                "steps": [],
            }
            atomic_write(STATE_OMS_CLOSE, out)
            print(f"Wrote {STATE_OMS_CLOSE}")
            return

        if not os.path.exists(STATE_CLOSE_INTENT):
            out = {
                "ts": utc_iso(),
                "mode": mode,
                "risk_mode": rm.mode.value,
                "state": "NO_INTENT",
                "reason": "NO_CLOSE_INTENT",
                "steps": [],
            }
            atomic_write(STATE_OMS_CLOSE, out)
            print(f"Wrote {STATE_OMS_CLOSE}")
            return

        intent = read_json(STATE_CLOSE_INTENT)

        # Freshness check
        max_age = int(os.environ.get("OMS_INTENT_MAX_AGE_SEC", "300"))  # 5 minutes default
        intent_ts = str(intent.get("ts", ""))
        age_sec = int(now_epoch() - parse_iso(intent_ts))
        if age_sec < 0:
            age_sec = 0
        if age_sec > max_age:
            out = {
                "ts": utc_iso(),
                "mode": mode,
                "risk_mode": rm.mode.value,
                "state": "REJECT",
                "reason": f"STALE_INTENT age_sec={age_sec} > max_age={max_age}",
                "steps": [],
                "intent_ts": intent_ts,
            }
            atomic_write(STATE_OMS_CLOSE, out)
            print(f"Wrote {STATE_OMS_CLOSE}")
            return

        actions_raw = intent.get("actions", [])
        actions = normalize_actions(actions_raw)

        if not actions:
            out = {
                "ts": utc_iso(),
                "mode": mode,
                "risk_mode": rm.mode.value,
                "state": "DONE",
                "reason": "NO_ACTIONS_IN_INTENT",
                "steps": [],
                "intent_ts": intent_ts,
            }
            # delete empty intent to avoid looping
            try:
                os.remove(STATE_CLOSE_INTENT)
            except Exception:
                pass
            atomic_write(STATE_OMS_CLOSE, out)
            print(f"Wrote {STATE_OMS_CLOSE}")
            return

        # Load positions
        if not os.path.exists(STATE_POSITIONS_BOOK):
            pos_book = {"ts": utc_iso(), "positions": []}
        else:
            pos_book = read_json(STATE_POSITIONS_BOOK)

        pos_map = positions_to_map(pos_book)

        # Reduce-only validation
        breaches = validate_reduce_only(actions, pos_map)
        if breaches:
            out = {
                "ts": utc_iso(),
                "mode": mode,
                "risk_mode": rm.mode.value,
                "state": "REJECT",
                "reason": "REDUCE_ONLY_VIOLATION",
                "breaches": breaches,
                "steps": [],
                "intent_ts": intent_ts,
                "positions_before": map_to_positions(pos_map),
                "actions": actions,
            }
            atomic_write(STATE_OMS_CLOSE, out)
            print(f"Wrote {STATE_OMS_CLOSE}")
            return

        # Execute (PLAN_ONLY sim fills)
        steps = []
        t0 = now_epoch()

        # Execute larger-risk-reduction first: for net longs SELL first; net shorts BUY first.
        # actions already encode correct side; we can just do in sorted order, but we’ll keep deterministic.
        for a in actions:
            sym = a["symbol"]
            side = a["close_side"]
            qty = int(a["qty"])

            px = price_proxy_for_symbol(sym)

            step = {
                "ts": utc_iso(),
                "symbol": sym,
                "side": side,
                "qty": qty,
                "price_proxy": px,
                "result": "SIM_FILLED",
            }

            # apply simulated fill
            apply_fill(pos_map, sym, side, qty)

            steps.append(step)
            # tiny delay for readable timestamps in logs (optional)
            time.sleep(0.05)

        # write updated positions book
        new_book = {"ts": utc_iso(), "positions": map_to_positions(pos_map)}
        atomic_write(STATE_POSITIONS_BOOK, new_book)

        # clear intent after successful consumption
        try:
            os.remove(STATE_CLOSE_INTENT)
        except Exception:
            pass

        out = {
            "ts": utc_iso(),
            "mode": mode,
            "risk_mode": rm.mode.value,
            "state": "DONE",
            "steps": steps,
            "elapsed_sec": int(now_epoch() - t0),
            "intent_ts": intent_ts,
            "intent_age_sec": age_sec,
            "positions_after": new_book["positions"],
        }
        atomic_write(STATE_OMS_CLOSE, out)
        print(f"Wrote {STATE_OMS_CLOSE}")
        return

    finally:
        release_lock(LOCK_PATH)


if __name__ == "__main__":
    main()
