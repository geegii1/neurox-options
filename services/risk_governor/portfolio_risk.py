# services/risk_governor/portfolio_risk.py
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Tuple


STATE_GREEKS = "state/portfolio_greeks.json"
STATE_RISK_EVAL = "state/risk_eval.json"
STATE_RISK_MODE = "state/risk_mode.json"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception:
        return default


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


@dataclass
class Limits:
    max_abs_delta: float = 200.0
    max_abs_gamma: float = 10.0
    max_abs_vega: float = 20000.0


def load_limits() -> Limits:
    """
    Keep this simple for now: hard-coded defaults.
    If you later add a policy file, wire it here.
    """
    return Limits()


def compute_breaches(totals: Dict[str, float], lim: Limits) -> Tuple[str, str, list]:
    abs_delta = float(totals.get("abs_delta", abs(float(totals.get("delta", 0.0)))))
    abs_gamma = float(totals.get("abs_gamma", abs(float(totals.get("gamma", 0.0)))))
    abs_vega = float(totals.get("abs_vega", abs(float(totals.get("vega", 0.0)))))

    breaches = []
    if abs_delta > lim.max_abs_delta:
        breaches.append(f"DELTA_LIMIT {abs_delta:.2f} > {lim.max_abs_delta:.1f}")
    if abs_gamma > lim.max_abs_gamma:
        breaches.append(f"GAMMA_LIMIT {abs_gamma:.2f} > {lim.max_abs_gamma:.1f}")
    if abs_vega > lim.max_abs_vega:
        breaches.append(f"VEGA_LIMIT {abs_vega:.2f} > {lim.max_abs_vega:.1f}")

    if breaches:
        mode = "HALT"
        reason = " | ".join(breaches)
    else:
        mode = "NORMAL"
        reason = "OK"

    return mode, reason, breaches


def has_iv_fallback(greeks: Dict[str, Any]) -> bool:
    """
    If any position uses a fallback IV source, we mark DEGRADED.
    You were using iv_src == "FALLBACK_DEFAULT" earlier.
    """
    for p in greeks.get("positions", []) or []:
        if str(p.get("iv_src", "")).upper() in ("FALLBACK_DEFAULT", "FALLBACK", "DEFAULT"):
            return True
    return False


def main() -> None:
    greeks = read_json(STATE_GREEKS, default={}) or {}
    totals = greeks.get("totals", {}) or {}

    lim = load_limits()
    mode, reason, breaches = compute_breaches(totals, lim)

    iv_fallback_present = has_iv_fallback(greeks)

    # If we are NOT halted but IV fallback exists, downgrade to DEGRADED
    if mode == "NORMAL" and iv_fallback_present:
        mode = "DEGRADED"
        reason = "IV_FALLBACK_DEFAULT_PRESENT"

    risk_eval = {
        "ts": utc_iso(),
        "mode_decision": mode,
        "reason": reason,
        "limits": {
            "max_abs_delta": lim.max_abs_delta,
            "max_abs_gamma": lim.max_abs_gamma,
            "max_abs_vega": lim.max_abs_vega,
        },
        "totals": {
            "abs_delta": float(totals.get("abs_delta", abs(float(totals.get("delta", 0.0))))),
            "abs_gamma": float(totals.get("abs_gamma", abs(float(totals.get("gamma", 0.0))))),
            "abs_vega": float(totals.get("abs_vega", abs(float(totals.get("vega", 0.0))))),
            "delta": float(totals.get("delta", 0.0)),
            "gamma": float(totals.get("gamma", 0.0)),
            "vega": float(totals.get("vega", 0.0)),
            "theta": float(totals.get("theta", 0.0)),
        },
        "breaches": breaches,
        "iv_fallback_present": bool(iv_fallback_present),
    }

    risk_mode = {"ts": utc_iso(), "mode": mode, "reason": reason}

    atomic_write(STATE_RISK_EVAL, risk_eval)
    atomic_write(STATE_RISK_MODE, risk_mode)

    # IMPORTANT: no subprocess(cat ...), no extra prints.
    print(f"Wrote {STATE_RISK_EVAL}")
    print(f"Wrote {STATE_RISK_MODE}")


if __name__ == "__main__":
    main()
