import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from services.portfolio.positions_ledger import record_fill


def utc_iso():
    return datetime.now(timezone.utc).isoformat()


class Mode(str, Enum):
    PLAN_ONLY = "PLAN_ONLY"
    LIVE = "LIVE"


class State(str, Enum):
    INIT = "INIT"
    SUBMIT_LONG = "SUBMIT_LONG"
    SUBMIT_SHORT = "SUBMIT_SHORT"
    DONE = "DONE"
    HALT = "HALT"
    FAIL = "FAIL"


@dataclass
class OmsConfig:
    mode: Mode = Mode.PLAN_ONLY
    max_seconds: int = 60


def atomic_write(path: str, obj: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)


def read_json(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def read_risk_mode(path: str = "state/risk_mode.json") -> str:
    try:
        with open(path, "r") as f:
            obj = json.load(f)
        return str(obj.get("mode", "NORMAL")).upper()
    except Exception:
        return "NORMAL"


def run_vertical_oms(
    plan_path: str = "state/final_plan.json",
    out_path: str = "state/oms_state.json",
    cfg: OmsConfig = OmsConfig(),
):
    plan = read_json(plan_path)

    if plan.get("status") != "PLAN_ONLY":
        atomic_write(
            out_path,
            {
                "ts": utc_iso(),
                "state": State.FAIL,
                "reason": "PLAN_NOT_READY",
            },
        )
        return

    long_leg = plan["legs"][0]
    short_leg = plan["legs"][1]

    state = State.INIT
    start = time.time()

    filled_long = 0
    filled_short = 0

    while True:
        elapsed = int(time.time() - start)
        risk_mode = read_risk_mode()

        if risk_mode == "HALT":
            state = State.HALT
            reason = "RISK_MODE_HALT"
        elif elapsed > cfg.max_seconds:
            state = State.FAIL
            reason = "TIMEOUT"
        else:
            reason = None

        snapshot = {
            "ts": utc_iso(),
            "mode": cfg.mode,
            "risk_mode": risk_mode,
            "state": state,
            "elapsed_sec": elapsed,
            "filled_long": filled_long,
            "filled_short": filled_short,
            "working": {
                "long": {
                    "symbol": long_leg["symbol"],
                    "qty": long_leg["qty"],
                    "limit": long_leg["limit"],
                },
                "short": {
                    "symbol": short_leg["symbol"],
                    "qty": short_leg["qty"],
                    "limit": short_leg["limit"],
                },
            },
            "reason": reason,
        }

        atomic_write(out_path, snapshot)

        if state in (State.DONE, State.FAIL, State.HALT):
            return

        # ---- State Machine ----

        if state == State.INIT:
            state = State.SUBMIT_LONG

        elif state == State.SUBMIT_LONG:
            if cfg.mode == Mode.PLAN_ONLY:
                filled_long = int(long_leg["qty"])

                record_fill(
                    symbol=long_leg["symbol"],
                    qty=filled_long,
                    side="BUY",
                    price=float(long_leg["limit"]),
                    tag="OMS_LONG_FILL_SIM",
                )

                state = State.SUBMIT_SHORT
            else:
                state = State.FAIL
                reason = "LIVE_MODE_NOT_ENABLED"

        elif state == State.SUBMIT_SHORT:
            if cfg.mode == Mode.PLAN_ONLY:
                filled_short = filled_long

                record_fill(
                    symbol=short_leg["symbol"],
                    qty=filled_short,
                    side="SELL",
                    price=float(short_leg["limit"]),
                    tag="OMS_SHORT_FILL_SIM",
                )

                state = State.DONE
            else:
                state = State.FAIL
                reason = "LIVE_MODE_NOT_ENABLED"

        time.sleep(1)


def main():
    cfg = OmsConfig(mode=Mode.PLAN_ONLY, max_seconds=30)
    run_vertical_oms(cfg=cfg)
    print("Wrote state/oms_state.json")


if __name__ == "__main__":
    main()
