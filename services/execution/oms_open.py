# services/execution/oms_open.py
from __future__ import annotations

import json
import os
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple


STATE_DIR = "state"
STATE_GATE_OUT = os.path.join(STATE_DIR, "gate_out.json")
STATE_RISK_MODE = os.path.join(STATE_DIR, "risk_mode.json")

STATE_OPEN_PLAN = os.path.join(STATE_DIR, "open_plan.json")
STATE_OPEN_INTENT = os.path.join(STATE_DIR, "open_intent.json")
STATE_OMS_OPEN = os.path.join(STATE_DIR, "oms_open_state.json")


# -------------------------
# IO helpers (self-contained)
# -------------------------
def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception:
        return default


def atomic_write(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=False)
    os.replace(tmp, path)


def safe_unlink(path: str) -> bool:
    try:
        os.unlink(path)
        return True
    except FileNotFoundError:
        return False
    except Exception:
        return False


# -------------------------
# Risk-mode policy
# -------------------------
def load_risk_mode() -> Dict[str, Any]:
    d = read_json(STATE_RISK_MODE, default=None)
    if isinstance(d, dict) and "mode" in d:
        return d
    return {"ts": utc_iso(), "mode": "UNKNOWN", "reason": "missing_or_invalid"}


def is_open_allowed_by_mode(rm: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Institutional safety rule:
      - NORMAL   : opens allowed
      - DEGRADED : reduce-only => opens blocked
      - HALT     : all trading blocked
      - UNKNOWN  : treat as HALT
    """
    mode = str(rm.get("mode", "UNKNOWN")).upper()
    reason = str(rm.get("reason", ""))

    if mode == "NORMAL":
        return True, ""
    if mode == "DEGRADED":
        return False, f"RISK_MODE_DEGRADED_OPEN_BLOCKED:{reason}"
    if mode == "HALT":
        return False, f"RISK_MODE_HALT_OPEN_BLOCKED:{reason}"
    return False, f"RISK_MODE_UNKNOWN_OPEN_BLOCKED:{reason}"


# -------------------------
# Gate-out parsing
# -------------------------
def load_gate_candidates() -> Dict[str, Any]:
    """
    Expected state/gate_out.json format:
      {
        "ts": "...",
        "out": {
          "demo1": { "allow": true, "order_plan": {...}, "decision": {...} },
          "demo2": { ... }
        }
      }

    Some earlier versions nested out.out; we handle both.
    """
    d = read_json(STATE_GATE_OUT, default=None)
    if not isinstance(d, dict):
        return {}

    out = d.get("out")
    # handle accidental nesting: {"out": {"ts":..., "out": {...}}}
    if isinstance(out, dict) and "out" in out and isinstance(out.get("out"), dict):
        out = out["out"]

    if isinstance(out, dict):
        return out
    return {}


def candidate_score(c: Dict[str, Any]) -> float:
    """
    Prefer:
      1) allow==True
      2) fewer decision.reasons
      3) higher max_contracts
    """
    if not isinstance(c, dict):
        return -1e9
    allow = bool(c.get("allow", False))
    decision = c.get("decision", {}) if isinstance(c.get("decision"), dict) else {}
    reasons = decision.get("reasons", [])
    n_reasons = len(reasons) if isinstance(reasons, list) else 99
    max_contracts = decision.get("max_contracts", 0)

    base = 1.0 if allow else 0.0
    return base * 1000.0 + float(max_contracts) * 10.0 - float(n_reasons) * 50.0


def select_best_candidate(cands: Dict[str, Any]) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    best_k = None
    best_v = None
    best_s = -1e18
    for k, v in cands.items():
        s = candidate_score(v)
        if s > best_s:
            best_s = s
            best_k = k
            best_v = v
    return best_k, best_v


# -------------------------
# OMS_OPEN main
# -------------------------
def main() -> None:
    os.makedirs(STATE_DIR, exist_ok=True)

    t0 = time.time()
    rm = load_risk_mode()
    open_allowed, open_block_reason = is_open_allowed_by_mode(rm)

    cands = load_gate_candidates()
    cand_name, cand_obj = select_best_candidate(cands)

    # Build open_plan (audit snapshot)
    open_plan: Dict[str, Any] = {
        "ts": utc_iso(),
        "source": "gateway",
        "candidate": cand_name,
        "risk_mode": rm,
        "selected": cand_obj if isinstance(cand_obj, dict) else None,
        "notes": [
            "OMS_OPEN generates OPEN intent when risk_mode allows and candidate allow==True.",
            "Execution remains PLAN_ONLY unless/ until broker adapter enabled.",
        ],
    }
    atomic_write(STATE_OPEN_PLAN, open_plan)
    print(f"Wrote {STATE_OPEN_PLAN}")

    # Decide whether to emit open_intent
    state: Dict[str, Any] = {
        "ts": utc_iso(),
        "mode": "PLAN_ONLY",
        "risk_mode": rm.get("mode"),
        "state": "DONE",
        "reason": None,
        "opened": False,
        "candidate": cand_name,
        "open_plan_path": STATE_OPEN_PLAN,
        "open_intent_written": False,
        "elapsed_ms": int((time.time() - t0) * 1000),
    }

    # If risk mode blocks open, ensure stale intent is removed
    if not open_allowed:
        deleted = safe_unlink(STATE_OPEN_INTENT)
        state["state"] = "OPEN_BLOCKED"
        state["reason"] = open_block_reason
        state["deleted_stale_intent"] = bool(deleted)
        atomic_write(STATE_OMS_OPEN, state)
        print(f"Wrote {STATE_OMS_OPEN}")
        return

    # If no candidate
    if cand_name is None or not isinstance(cand_obj, dict):
        deleted = safe_unlink(STATE_OPEN_INTENT)
        state["state"] = "NO_CANDIDATE"
        state["reason"] = "NO_GATE_CANDIDATE"
        state["deleted_stale_intent"] = bool(deleted)
        atomic_write(STATE_OMS_OPEN, state)
        print(f"Wrote {STATE_OMS_OPEN}")
        return

    # Candidate must be allowed
    if not bool(cand_obj.get("allow", False)):
        deleted = safe_unlink(STATE_OPEN_INTENT)
        state["state"] = "CANDIDATE_BLOCKED"
        # propagate reasons if present
        decision = cand_obj.get("decision", {}) if isinstance(cand_obj.get("decision"), dict) else {}
        reasons = decision.get("reasons", [])
        state["reason"] = "CANDIDATE_NOT_ALLOWED"
        state["candidate_reasons"] = reasons
        state["deleted_stale_intent"] = bool(deleted)
        atomic_write(STATE_OMS_OPEN, state)
        print(f"Wrote {STATE_OMS_OPEN}")
        return

    # Emit OPEN_INTENT
    intent = {
        "ts": utc_iso(),
        "type": "OPEN_INTENT",
        "mode": "PLAN_ONLY",  # stays plan-only until broker adapter enabled
        "candidate": cand_name,
        "risk_mode": rm,
        "order_plan": cand_obj.get("order_plan"),
        "decision": cand_obj.get("decision"),
        "notes": [
            "PLAN_ONLY intent. Wire broker adapter to consume this file.",
            "Reduce-only / halt modes must delete this intent (safety invariant).",
        ],
    }
    atomic_write(STATE_OPEN_INTENT, intent)
    state["open_intent_written"] = True
    state["open_intent_path"] = STATE_OPEN_INTENT
    atomic_write(STATE_OMS_OPEN, state)
    print(f"Wrote {STATE_OPEN_INTENT}")
    print(f"Wrote {STATE_OMS_OPEN}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        err = {
            "ts": utc_iso(),
            "mode": "PLAN_ONLY",
            "state": "ERROR",
            "opened": False,
            "reason": "EXCEPTION",
            "traceback": traceback.format_exc(),
        }
        atomic_write(STATE_OMS_OPEN, err)
        print(f"Wrote {STATE_OMS_OPEN}")
        raise
