# services/execution/oms_poll.py
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from services.execution.journal import append_event, mk_event

# Alpaca SDK (alpaca-py)
from alpaca.trading.client import TradingClient


STATE_DIR = Path("state")
OPEN_ORDERS_PATH = STATE_DIR / "open_orders.json"
POLL_STATE_PATH = STATE_DIR / "oms_poll_state.json"


# ----------------------------
# IO helpers (self-contained)
# ----------------------------
def utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_jsonable(x: Any) -> Any:
    """Convert Alpaca/pydantic objects, UUID, datetime, enums, etc into JSON-safe types."""
    if x is None:
        return None
    if isinstance(x, (str, int, float, bool)):
        return x
    if isinstance(x, datetime):
        # Always write Z
        return x.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if isinstance(x, dict):
        return {str(k): _to_jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_to_jsonable(v) for v in x]

    # pydantic models (alpaca-py returns pydantic-ish objects)
    if hasattr(x, "model_dump"):
        try:
            return _to_jsonable(x.model_dump())
        except Exception:
            pass

    # enums / UUID / other objects
    try:
        return str(x)
    except Exception:
        return repr(x)


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return default
    except Exception:
        return default


def atomic_write(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=False))
    tmp.replace(path)


# ----------------------------
# Normalization
# ----------------------------
def norm_status(s: Any) -> str:
    """
    Alpaca may return OrderStatus enum or strings like 'OrderStatus.ACCEPTED'.
    Normalize to stable lowercase tokens like: accepted, filled, canceled, rejected, etc.
    """
    if s is None:
        return "unknown"
    txt = str(s).strip()
    # handle enum-ish "OrderStatus.ACCEPTED" or "orderstatus.accepted"
    if "." in txt:
        txt = txt.split(".")[-1]
    return txt.lower()


def need_api_keys() -> Tuple[bool, str]:
    k = os.environ.get("APCA_API_KEY_ID", "")
    s = os.environ.get("APCA_API_SECRET_KEY", "")
    if not k or not s:
        return True, "MISSING_API_KEYS_SET_APCA_API_KEY_ID_AND_APCA_API_SECRET_KEY"
    return False, ""


def broker_mode() -> str:
    return os.environ.get("BROKER_MODE", "PLAN_ONLY").strip().upper() or "PLAN_ONLY"


def is_paper() -> bool:
    """
    Use APCA_API_BASE_URL if present to infer paper, otherwise default paper=True.
    Your setup has been paper-based.
    """
    base = (os.environ.get("APCA_API_BASE_URL") or "").lower()
    if "paper-api.alpaca.markets" in base:
        return True
    # if user explicitly sets APCA_PAPER=0, respect it
    ap = os.environ.get("APCA_PAPER")
    if ap is not None:
        return ap.strip() in ("1", "true", "yes", "y")
    return True


# ----------------------------
# Core poll logic
# ----------------------------
@dataclass
class PollResult:
    ts: str
    mode: str
    ok: bool
    state: str
    n_orders: int
    changed: List[Dict[str, str]]
    errors: List[str]
    elapsed_ms: int


def poll_once() -> PollResult:
    t0 = time.time()
    ts = utc_iso()
    mode = broker_mode()

    store = read_json(OPEN_ORDERS_PATH, default={"ts": ts, "mode": mode, "orders": {}})
    orders: Dict[str, Dict[str, Any]] = store.get("orders", {}) or {}
    order_ids = list(orders.keys())

    append_event(mk_event(
        intent_type="OMS_POLL",
        intent_ts=ts,
        stage="POLL_START",
        ok=True,
        mode=mode,
        data={"n_orders": len(order_ids), "paper": is_paper()},
    ))

    if not order_ids:
        res = PollResult(
            ts=ts,
            mode=mode,
            ok=True,
            state="NO_ORDERS",
            n_orders=0,
            changed=[],
            errors=[],
            elapsed_ms=int((time.time() - t0) * 1000),
        )
        atomic_write(POLL_STATE_PATH, _to_jsonable(res.__dict__))
        append_event(mk_event(
            intent_type="OMS_POLL",
            intent_ts=ts,
            stage="POLL_DONE",
            ok=True,
            mode=mode,
            data=_to_jsonable(res.__dict__),
        ))
        return res

    # LIVE polling uses Alpaca client. PLAN_ONLY still polls if keys exist (safe read-only).
    missing, why = need_api_keys()
    if missing:
        res = PollResult(
            ts=ts,
            mode=mode,
            ok=False,
            state="CLIENT_ERROR",
            n_orders=len(order_ids),
            changed=[],
            errors=[f"CLIENT_ERROR:RuntimeError:{why}"],
            elapsed_ms=int((time.time() - t0) * 1000),
        )
        atomic_write(POLL_STATE_PATH, _to_jsonable(res.__dict__))
        append_event(mk_event(
            intent_type="OMS_POLL",
            intent_ts=ts,
            stage="CLIENT_ERROR",
            ok=False,
            mode=mode,
            msg=res.errors[0],
            data={},
        ))
        return res

    try:
        client = TradingClient(
            os.environ["APCA_API_KEY_ID"],
            os.environ["APCA_API_SECRET_KEY"],
            paper=is_paper(),
        )
    except Exception as e:
        res = PollResult(
            ts=ts,
            mode=mode,
            ok=False,
            state="CLIENT_ERROR",
            n_orders=len(order_ids),
            changed=[],
            errors=[f"CLIENT_ERROR:{type(e).__name__}:{e}"],
            elapsed_ms=int((time.time() - t0) * 1000),
        )
        atomic_write(POLL_STATE_PATH, _to_jsonable(res.__dict__))
        append_event(mk_event(
            intent_type="OMS_POLL",
            intent_ts=ts,
            stage="CLIENT_ERROR",
            ok=False,
            mode=mode,
            msg=res.errors[0],
            data={},
        ))
        return res

    changed: List[Dict[str, str]] = []
    errors: List[str] = []

    for oid in order_ids:
        prev_status = norm_status(orders.get(oid, {}).get("status"))
        try:
            o = client.get_order_by_id(oid)
            new_status = norm_status(getattr(o, "status", None))

            # Persist open_orders.json entry
            entry = orders.get(oid, {}) or {}
            entry["order_id"] = oid
            entry["status"] = new_status
            entry["last_seen"] = ts
            entry["paper"] = is_paper()

            # Keep tag if present
            if "tag" not in entry:
                entry["tag"] = None

            # Store raw snapshot (JSON-safe)
            entry["raw"] = _to_jsonable(o)
            orders[oid] = entry

            if new_status != prev_status:
                changed.append({"order_id": oid, "prev": prev_status, "new": new_status})
                append_event(mk_event(
                    intent_type="OMS_POLL",
                    intent_ts=ts,
                    stage="OPEN_POLL",
                    ok=True,
                    mode=mode,
                    data={"order_id": oid, "prev": prev_status, "new": new_status, "tag": entry.get("tag")},
                ))

        except Exception as e:
            msg = f"ORDER_ERROR:{oid}:{type(e).__name__}:{e}"
            errors.append(msg)
            append_event(mk_event(
                intent_type="OMS_POLL",
                intent_ts=ts,
                stage="ORDER_ERROR",
                ok=False,
                mode=mode,
                msg=msg,
                data={"order_id": oid},
            ))

    # Write updated open_orders.json
    store_out = {
        "ts": ts,
        "mode": mode,
        "orders": orders,
    }
    atomic_write(OPEN_ORDERS_PATH, _to_jsonable(store_out))

    ok = len(errors) == 0
    res = PollResult(
        ts=ts,
        mode=mode,
        ok=ok,
        state="POLL_OK" if ok else "POLL_PARTIAL",
        n_orders=len(order_ids),
        changed=changed,
        errors=errors,
        elapsed_ms=int((time.time() - t0) * 1000),
    )
    atomic_write(POLL_STATE_PATH, _to_jsonable(res.__dict__))

    append_event(mk_event(
        intent_type="OMS_POLL",
        intent_ts=ts,
        stage="POLL_DONE",
        ok=ok,
        mode=mode,
        data=_to_jsonable(res.__dict__),
    ))
    return res


def main() -> None:
    res = poll_once()
    print(f"Wrote {POLL_STATE_PATH}")
    print(json.dumps(_to_jsonable(res.__dict__), indent=2))


if __name__ == "__main__":
    main()
