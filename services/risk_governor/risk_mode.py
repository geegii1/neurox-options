import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum


STATE_RISK_MODE = "state/risk_mode.json"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write(path: str, obj: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)


def read_json(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


class RiskMode(str, Enum):
    NORMAL = "NORMAL"
    DEGRADED = "DEGRADED"
    HALT = "HALT"


@dataclass(frozen=True)
class RiskModeState:
    ts: str
    mode: RiskMode
    reason: str


def ensure_risk_mode_file():
    if not os.path.exists(STATE_RISK_MODE):
        set_risk_mode("NORMAL", "boot")


def get_risk_mode() -> RiskModeState:
    """
    Reads state/risk_mode.json.
    If missing, creates a default NORMAL state.
    """
    ensure_risk_mode_file()
    d = read_json(STATE_RISK_MODE)
    mode = str(d.get("mode", "NORMAL")).upper()
    reason = str(d.get("reason", "OK"))
    ts = str(d.get("ts", utc_iso()))
    try:
        rm = RiskMode(mode)
    except Exception:
        rm = RiskMode.NORMAL
    return RiskModeState(ts=ts, mode=rm, reason=reason)


def set_risk_mode(mode: str, reason: str):
    """
    Writes state/risk_mode.json.
    """
    mode_u = str(mode).upper()
    if mode_u not in ("NORMAL", "DEGRADED", "HALT"):
        mode_u = "DEGRADED"
    obj = {"ts": utc_iso(), "mode": mode_u, "reason": str(reason)}
    atomic_write(STATE_RISK_MODE, obj)
    print(f"cat {STATE_RISK_MODE}")
    print(obj)
    print(json.dumps(obj, indent=2))


def allow_open_trades() -> bool:
    """
    Only NORMAL may open new risk.
    """
    rm = get_risk_mode().mode
    return rm == RiskMode.NORMAL


def allow_close_trades() -> bool:
    """
    NORMAL + DEGRADED may reduce risk.
    HALT blocks (we can add 'emergency close' later if desired).
    """
    rm = get_risk_mode().mode
    return rm in (RiskMode.NORMAL, RiskMode.DEGRADED)


def main():
    st = get_risk_mode()
    print(f"cat {STATE_RISK_MODE}")
    print({"ts": st.ts, "mode": st.mode.value, "reason": st.reason})
    print(json.dumps({"ts": st.ts, "mode": st.mode.value, "reason": st.reason}, indent=2))


if __name__ == "__main__":
    main()
