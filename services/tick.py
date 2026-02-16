# services/tick.py
from __future__ import annotations

import json
import os
import subprocess
import tempfile
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional


LOCK_PATH = "state/tick.lock"
STATE_TICK = "state/tick_state.json"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_epoch() -> float:
    return time.time()


def atomic_write(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_", dir=os.path.dirname(path) or ".")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(obj, f, indent=2, sort_keys=False)
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return default


def acquire_lock(path: str) -> bool:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w") as f:
            f.write(utc_iso())
        return True
    except FileExistsError:
        return False


def release_lock(path: str) -> None:
    try:
        os.remove(path)
    except Exception:
        pass


def step_run(name: str, fn: Callable[[], subprocess.CompletedProcess], critical: bool) -> Dict[str, Any]:
    t0 = now_epoch()
    ok = True
    rc = None
    out = ""
    err = ""
    wrote = None
    out_parsed = None

    try:
        cp = fn()
        rc = cp.returncode
        out = (cp.stdout or "")[-2000:]
        err = (cp.stderr or "")[-2000:]
        ok = (rc == 0)

        # If a step writes a known file, record it (optional)
        if "Wrote state/gate_out.json" in out:
            wrote = "state/gate_out.json"
        if name == "pretrade_gateway.gateway":
            # gateway prints JSON; if it is valid parse, we store it
            try:
                out_parsed = json.loads(cp.stdout)
            except Exception:
                out_parsed = None

    except Exception:
        ok = False
        err = traceback.format_exc()[-2000:]

    return {
        "name": name,
        "critical": critical,
        "ok": ok,
        "returncode": rc,
        "stdout_tail": out.strip(),
        "stderr_tail": err.strip(),
        **({"wrote": wrote} if wrote else {}),
        **({"out_parsed": True} if out_parsed is not None else {}),
        "elapsed_ms": int((now_epoch() - t0) * 1000),
        "result": "OK" if ok else "ERR",
    }


def _call_module(mod: str) -> subprocess.CompletedProcess:
    # Uses current interpreter in venv
    cmd = [os.environ.get("PYTHON", "python"), "-m", mod]
    return subprocess.run(cmd, capture_output=True, text=True)


def call_portfolio_greeks() -> subprocess.CompletedProcess:
    return _call_module("services.portfolio.greeks")


def call_portfolio_risk() -> subprocess.CompletedProcess:
    return _call_module("services.risk_governor.portfolio_risk")


def call_derisk_plan() -> subprocess.CompletedProcess:
    return _call_module("services.risk_governor.derisk_plan")


def call_derisk_execute() -> subprocess.CompletedProcess:
    return _call_module("services.risk_governor.derisk_execute")


def call_gateway() -> subprocess.CompletedProcess:
    return _call_module("services.pretrade_gateway.gateway")


def call_oms_open() -> subprocess.CompletedProcess:
    return _call_module("services.execution.oms_open")


def call_oms_open_exec() -> subprocess.CompletedProcess:
    return _call_module("services.execution.oms_open_exec")


def call_oms_close() -> subprocess.CompletedProcess:
    return _call_module("services.execution.oms_close")


def summarize_state() -> Dict[str, Any]:
    risk_mode = read_json("state/risk_mode.json", default=None)
    gate_out = read_json("state/gate_out.json", default=None)

    return {
        "risk_mode": risk_mode,
        "open_intent_present": os.path.exists("state/open_intent.json"),
        "close_intent_present": os.path.exists("state/close_intent.json"),
        "gate_out_present": gate_out is not None,
        "gate_out": gate_out,
    }


def main() -> None:
    if not acquire_lock(LOCK_PATH):
        atomic_write(
            STATE_TICK,
            {"ts": utc_iso(), "ok": False, "state": "LOCKED", "reason": "ANOTHER_TICK_RUNNING"},
        )
        print(f"Wrote {STATE_TICK}")
        return

    t0 = now_epoch()
    steps: List[Dict[str, Any]] = []
    ok = True
    halted_by: Optional[str] = None

    try:
        steps.append(step_run("portfolio.greeks", call_portfolio_greeks, critical=True))
        if not steps[-1]["ok"]:
            ok = False
            halted_by = "portfolio.greeks"
            raise RuntimeError("tick halted")

        steps.append(step_run("risk_governor.portfolio_risk", call_portfolio_risk, critical=True))
        if not steps[-1]["ok"]:
            ok = False
            halted_by = "risk_governor.portfolio_risk"
            raise RuntimeError("tick halted")

        steps.append(step_run("risk_governor.derisk_plan", call_derisk_plan, critical=True))
        if not steps[-1]["ok"]:
            ok = False
            halted_by = "risk_governor.derisk_plan"
            raise RuntimeError("tick halted")

        steps.append(step_run("risk_governor.derisk_execute", call_derisk_execute, critical=True))
        if not steps[-1]["ok"]:
            ok = False
            halted_by = "risk_governor.derisk_execute"
            raise RuntimeError("tick halted")

        steps.append(step_run("pretrade_gateway.gateway", call_gateway, critical=True))
        if not steps[-1]["ok"]:
            ok = False
            halted_by = "pretrade_gateway.gateway"
            raise RuntimeError("tick halted")

        # OPEN plane (writes open_intent)
        steps.append(step_run("execution.oms_open", call_oms_open, critical=True))
        if not steps[-1]["ok"]:
            ok = False
            halted_by = "execution.oms_open"
            raise RuntimeError("tick halted")

        # OPEN EXEC (consumes open_intent)
        steps.append(step_run("execution.oms_open_exec", call_oms_open_exec, critical=True))
        if not steps[-1]["ok"]:
            ok = False
            halted_by = "execution.oms_open_exec"
            raise RuntimeError("tick halted")

        # CLOSE plane (consumes close_intent if present)
        steps.append(step_run("execution.oms_close", call_oms_close, critical=True))
        if not steps[-1]["ok"]:
            ok = False
            halted_by = "execution.oms_close"
            raise RuntimeError("tick halted")

    except Exception:
        # step_run captured details; stop safely
        pass
    finally:
        summary = summarize_state()
        out = {
            "ts": utc_iso(),
            "ok": ok,
            "halted_by": halted_by,
            "elapsed_ms": int((now_epoch() - t0) * 1000),
            "steps": steps,
            "summary": summary,
        }
        atomic_write(STATE_TICK, out)
        release_lock(LOCK_PATH)
        print(f"Wrote {STATE_TICK}")


if __name__ == "__main__":
    main()
