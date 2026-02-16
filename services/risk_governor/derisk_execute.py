import json
import os
from datetime import datetime, timezone


def utc_iso():
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


def normalize_limits(plan: dict) -> dict:
    hard = plan.get("hard_limits") or plan.get("limits") or {}
    target = plan.get("target_limits") or {}
    buffer_pct = plan.get("buffer_pct")

    if not hard:
        hard = {"max_abs_delta": 200.0, "max_abs_gamma": 10.0, "max_abs_vega": 20000.0}
    if not target:
        target = dict(hard)

    out = {"hard_limits": hard, "target_limits": target}
    if buffer_pct is not None:
        out["buffer_pct"] = buffer_pct
    return out


def delete_if_exists(path: str):
    try:
        if os.path.exists(path):
            os.remove(path)
            return True
    except Exception:
        pass
    return False


def main(
    plan_path: str = "state/derisk_plan.json",
    out_path: str = "state/derisk_exec.json",
    intent_path: str = "state/close_intent.json",
):
    plan = read_json(plan_path)
    actions = plan.get("actions", [])
    lim = normalize_limits(plan)

    # If no actions, delete stale intent so gateway/OMS can't act on old instructions
    if plan.get("status") not in ("OK", "PARTIAL") or not actions:
        deleted = delete_if_exists(intent_path)
        out = {
            "ts": utc_iso(),
            "status": "NO_EXEC",
            "reason": "NO_ACTIONS",
            "input_status": plan.get("status"),
            "input_reason": plan.get("reason"),
            "deleted_stale_intent": deleted,
            "intent_path": intent_path,
            "actions": actions,
            "limits": lim,
        }
        atomic_write(out_path, out)
        print(f"Wrote {out_path}")
        if deleted:
            print(f"Deleted stale {intent_path}")
        return

    intent = {
        "ts": utc_iso(),
        "type": "DERISK_CLOSE",
        "mode": "PLAN_ONLY",
        "actions": actions,
        "expected_end_totals": plan.get("end_totals"),
        **lim,
    }

    atomic_write(intent_path, intent)

    out = {
        "ts": utc_iso(),
        "status": "WROTE_INTENT",
        "intent_path": intent_path,
        "actions": actions,
        "limits": lim,
    }
    atomic_write(out_path, out)
    print(f"Wrote {intent_path}")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
