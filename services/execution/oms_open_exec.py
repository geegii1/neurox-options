# services/execution/oms_open_exec.py
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from services.execution.broker_adapter import BrokerAdapter
from services.execution.journal import append_event, mk_event

STATE_DIR = Path(os.environ.get("STATE_DIR", "state"))
OPEN_INTENT_PATH = STATE_DIR / "open_intent.json"
OUT_STATE_PATH = STATE_DIR / "oms_open_exec_state.json"


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_default(o: Any) -> Any:
    # Keep it simple + safe for state writes (UUID, datetime, enums, Path, etc.)
    try:
        from uuid import UUID
        if isinstance(o, UUID):
            return str(o)
    except Exception:
        pass

    if isinstance(o, datetime):
        if o.tzinfo is None:
            o = o.replace(tzinfo=timezone.utc)
        return o.astimezone(timezone.utc).isoformat()

    try:
        from enum import Enum
        if isinstance(o, Enum):
            return o.value
    except Exception:
        pass

    if isinstance(o, Path):
        return str(o)

    return str(o)


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def atomic_write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    data = json.dumps(obj, indent=2, ensure_ascii=False, default=_json_default)
    with tmp.open("w", encoding="utf-8") as f:
        f.write(data)
    tmp.replace(path)


def rm_if_exists(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return


def main() -> None:
    t0 = time.time()
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    mode = BrokerAdapter().mode

    # If no intent, NOOP.
    if not OPEN_INTENT_PATH.exists():
        out = {
            "ts": utc_iso(),
            "mode": mode,
            "state": "NO_INTENT",
            "reason": "NO_OPEN_INTENT",
            "elapsed_ms": int((time.time() - t0) * 1000),
        }
        atomic_write_json(OUT_STATE_PATH, out)
        print(f"Wrote {OUT_STATE_PATH}")
        return

    intent = read_json(OPEN_INTENT_PATH)
    intent_ts = intent.get("ts", "")
    intent_type = intent.get("type", "OPEN_INTENT")
    candidate = intent.get("candidate")

    # Journal start
    append_event(
        mk_event(
            intent_type=intent_type,
            intent_ts=intent_ts,
            stage="OPEN_EXEC_START",
            ok=True,
            mode=mode,
            data={"candidate": candidate},
        )
    )

    order_plan = intent.get("order_plan")
    decision = intent.get("decision")
    if not isinstance(order_plan, dict):
        msg = "INVALID_INTENT_MISSING_ORDER_PLAN"
        append_event(mk_event(intent_type=intent_type, intent_ts=intent_ts, stage="BROKER_TRANSLATE_SUBMIT", ok=False, mode=mode, msg=msg))
        out = {
            "ts": utc_iso(),
            "mode": mode,
            "state": "INTENT_INVALID",
            "reason": msg,
            "intent_ts": intent_ts,
            "candidate": candidate,
            "elapsed_ms": int((time.time() - t0) * 1000),
        }
        atomic_write_json(OUT_STATE_PATH, out)
        print(f"Wrote {OUT_STATE_PATH}")
        return

    # Enrich request (OMS layer metadata)
    enriched_request = dict(order_plan)
    enriched_request["risk"] = decision
    enriched_request["candidate"] = candidate
    enriched_request["intent_ts"] = intent_ts

    try:
        broker = BrokerAdapter()
        broker_result = broker.submit_open(enriched_request)
        ok = bool(broker_result.get("ok", False))

        append_event(
            mk_event(
                intent_type=intent_type,
                intent_ts=intent_ts,
                stage="BROKER_TRANSLATE_SUBMIT",
                ok=ok,
                mode=mode,
                data={"broker_result": broker_result},
            )
        )

        if ok:
            # delete intent only after journaling success
            rm_if_exists(OPEN_INTENT_PATH)
            append_event(mk_event(intent_type=intent_type, intent_ts=intent_ts, stage="INTENT_CONSUME_OK", ok=True, mode=mode))

        out = {
            "ts": utc_iso(),
            "mode": mode,
            "state": "OPEN_SUBMITTED" if (broker_result.get("submitted") is True) else "PLAN_ONLY_TRANSLATED",
            "reason": broker_result.get("error"),
            "intent_ts": intent_ts,
            "candidate": candidate,
            "order_plan": order_plan,
            "decision": decision,
            "broker_result": broker_result,
            "intent_deleted": ok and (not OPEN_INTENT_PATH.exists()),
            "elapsed_ms": int((time.time() - t0) * 1000),
        }

        atomic_write_json(OUT_STATE_PATH, out)
        print(f"Wrote {OUT_STATE_PATH}")
        return

    except Exception as e:
        # IMPORTANT: journal this without ever crashing due to JSON types
        append_event(
            mk_event(
                intent_type=intent_type,
                intent_ts=intent_ts,
                stage="BROKER_TRANSLATE_SUBMIT",
                ok=False,
                mode=mode,
                msg=f"{type(e).__name__}: {e}",
            )
        )
        out = {
            "ts": utc_iso(),
            "mode": mode,
            "state": "BROKER_ERROR",
            "reason": type(e).__name__,
            "intent_ts": intent_ts,
            "candidate": candidate,
            "order_plan": order_plan,
            "decision": decision,
            "elapsed_ms": int((time.time() - t0) * 1000),
        }
        atomic_write_json(OUT_STATE_PATH, out)
        print(f"Wrote {OUT_STATE_PATH}")
        return


if __name__ == "__main__":
    main()
