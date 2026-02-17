# services/execution/oms_poll.py
from __future__ import annotations

import json
import os
import smtplib
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from alpaca.trading.client import TradingClient

# Optional Alpaca request/enum imports (different SDK versions differ)
try:
    from alpaca.trading.requests import GetOrdersRequest  # type: ignore
except Exception:
    GetOrdersRequest = None  # type: ignore

try:
    from alpaca.trading.enums import QueryOrderStatus  # type: ignore
except Exception:
    QueryOrderStatus = None  # type: ignore


# ============================================================
# ENV (recommended)
# ============================================================
# APCA_API_KEY_ID, APCA_API_SECRET_KEY
# APCA_PAPER=true/false
#
# Email:
# EMAIL_HOST, EMAIL_PORT(587), EMAIL_TLS(true), EMAIL_USER, EMAIL_PASS, EMAIL_FROM, EMAIL_TO
#
# Alert controls:
# ALERT_MIN_SEVERITY=YELLOW|ORANGE|RED
# ORDER_TAG_PREFIX=LIVE_          # only auto-track orders whose tag starts with this prefix (optional)
# ALERT_SUBJECT_PREFIX=[NeuroX OMS]  # optional
#
# Tag extraction:
# TAG_SOURCE=client_order_id|none  # default client_order_id (best-effort)
# DEFAULT_TAG=UNKNOWN              # used if we can't infer
# ============================================================


# -----------------------------
# Paths / State
# -----------------------------
BASE_DIR = Path(__file__).resolve().parents[2]  # /opt/neurox-options/services -> /opt/neurox-options
STATE_DIR = BASE_DIR / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

OMS_POLL_STATE_PATH = STATE_DIR / "oms_poll_state.json"
OPEN_ORDERS_PATH = STATE_DIR / "open_orders.json"
JOURNAL_PATH = STATE_DIR / "execution_journal.jsonl"
ALERTS_STATE_PATH = STATE_DIR / "alerts_state.json"  # tracks what we already emailed

# -----------------------------
# Config
# -----------------------------
PAPER_DEFAULT = True

# severity ranks
SEV_RANK = {"YELLOW": 1, "ORANGE": 2, "RED": 3}

# status -> severity mapping (tune anytime)
STATUS_TO_SEV = {
    "new": "YELLOW",
    "pending_new": "YELLOW",
    "accepted": "YELLOW",
    "partially_filled": "ORANGE",
    "replaced": "ORANGE",
    "filled": "RED",
    "canceled": "RED",
    "cancelled": "RED",
    "rejected": "RED",
    "expired": "RED",
    "failed": "RED",
}

TERMINAL_STATUSES = {"filled", "canceled", "cancelled", "rejected", "expired", "failed"}


# -----------------------------
# Helpers
# -----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(v.strip())
    except Exception:
        return default


def get_min_sev() -> str:
    v = (os.getenv("ALERT_MIN_SEVERITY") or "YELLOW").strip().upper()
    return v if v in SEV_RANK else "YELLOW"


def severity_for_status(status_norm: str) -> str:
    return STATUS_TO_SEV.get(status_norm, "YELLOW")


def should_alert(sev: str, min_sev: str) -> bool:
    return SEV_RANK.get(sev, 99) >= SEV_RANK.get(min_sev, 1)


def normalize_status(s: Any) -> str:
    """
    Normalize status so we never bounce between:
      'accepted' and 'OrderStatus.ACCEPTED' and 'orderstatus.accepted'
    """
    if s is None:
        return "unknown"
    txt = str(s).strip().lower()
    txt = txt.replace("orderstatus.", "").replace("orderstatus_", "").replace("orderstatus", "")
    txt = txt.strip(". _-")
    return txt or "unknown"


def _json_default(o: Any) -> Any:
    """Robust JSON fallback (prevents UUID/Enum/datetime crashes)."""
    try:
        # pydantic v2
        if hasattr(o, "model_dump"):
            return o.model_dump()
        # pydantic v1 / many SDK models
        if hasattr(o, "dict"):
            return o.dict()
    except Exception:
        pass

    # datetime
    if isinstance(o, datetime):
        return o.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    # Enum-like
    try:
        import enum

        if isinstance(o, enum.Enum):
            return getattr(o, "value", str(o))
    except Exception:
        pass

    # UUID and other objects
    return str(o)


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=False, default=_json_default))


def journal(event: Dict[str, Any]) -> None:
    with JOURNAL_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, separators=(",", ":"), default=_json_default))
        f.write("\n")


def subject_prefix() -> str:
    return (os.getenv("ALERT_SUBJECT_PREFIX") or "[NeuroX OMS]").strip()


def get_tag_prefix_filter() -> str:
    # If set, we only auto-track orders whose inferred tag starts with this prefix (e.g. LIVE_)
    return (os.getenv("ORDER_TAG_PREFIX") or "").strip()


def get_tag_source() -> str:
    return (os.getenv("TAG_SOURCE") or "client_order_id").strip().lower()


def get_default_tag() -> str:
    return (os.getenv("DEFAULT_TAG") or "UNKNOWN").strip()


def best_effort_tag_from_order(order_obj: Any) -> str:
    """
    Best-effort "tag" inference.
    - Many Alpaca order objects don't have a 'tag' field.
    - We try client_order_id if TAG_SOURCE=client_order_id.
    """
    src = get_tag_source()
    if src == "none":
        return get_default_tag()

    if src == "client_order_id":
        cid = getattr(order_obj, "client_order_id", None)
        if cid:
            return str(cid)

    return get_default_tag()


def order_to_raw_dict(order_obj: Any) -> Dict[str, Any]:
    """Convert Alpaca SDK order object into a JSON-friendly dict."""
    try:
        if hasattr(order_obj, "model_dump"):
            return order_obj.model_dump()
        if hasattr(order_obj, "dict"):
            return order_obj.dict()
    except Exception:
        pass

    # fallback: shallow attrs
    out: Dict[str, Any] = {}
    for k in dir(order_obj):
        if k.startswith("_"):
            continue
        if k in ("json", "model_dump", "dict"):
            continue
        try:
            v = getattr(order_obj, k)
        except Exception:
            continue
        # skip callables
        if callable(v):
            continue
        # keep simple-ish fields only
        if isinstance(v, (str, int, float, bool)) or v is None:
            out[k] = v
    # ensure id/status if present
    try:
        out["id"] = str(getattr(order_obj, "id", out.get("id")))
    except Exception:
        pass
    try:
        out["status"] = str(getattr(order_obj, "status", out.get("status")))
    except Exception:
        pass
    return out


# -----------------------------
# Email
# -----------------------------
@dataclass
class EmailCfg:
    host: str
    port: int
    user: str
    password: str
    mail_from: str
    mail_to: str
    tls: bool


def load_email_cfg() -> Optional[EmailCfg]:
    host = (os.getenv("EMAIL_HOST") or "").strip()
    user = (os.getenv("EMAIL_USER") or "").strip()
    password = (os.getenv("EMAIL_PASS") or "").strip()
    mail_from = (os.getenv("EMAIL_FROM") or "").strip()
    mail_to = (os.getenv("EMAIL_TO") or "").strip()

    if not (host and user and password and mail_from and mail_to):
        return None

    port = env_int("EMAIL_PORT", 587)
    tls = env_bool("EMAIL_TLS", True)
    return EmailCfg(host=host, port=port, user=user, password=password, mail_from=mail_from, mail_to=mail_to, tls=tls)


def send_email(subject: str, body: str) -> Tuple[bool, str]:
    cfg = load_email_cfg()
    if cfg is None:
        return False, "EMAIL_NOT_CONFIGURED"

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = cfg.mail_from
    msg["To"] = cfg.mail_to

    try:
        with smtplib.SMTP(cfg.host, cfg.port, timeout=20) as server:
            server.ehlo()
            if cfg.tls:
                server.starttls()
                server.ehlo()
            server.login(cfg.user, cfg.password)
            server.sendmail(cfg.mail_from, [cfg.mail_to], msg.as_string())
        return True, "SENT"
    except Exception as e:
        return False, f"EMAIL_SEND_FAILED:{type(e).__name__}:{e}"


# -----------------------------
# Alpaca client + discovery
# -----------------------------
def build_trading_client(paper: bool = True) -> TradingClient:
    key = os.getenv("APCA_API_KEY_ID")
    secret = os.getenv("APCA_API_SECRET_KEY")
    if not key or not secret:
        raise RuntimeError("MISSING_API_KEYS_SET_APCA_API_KEY_ID_AND_APCA_API_SECRET_KEY")
    return TradingClient(key, secret, paper=paper)


def list_open_orders(client: TradingClient) -> List[Any]:
    """
    Returns list of open orders.
    Uses GetOrdersRequest if available; falls back to simple call.
    """
    # Most reliable in newer SDKs
    if GetOrdersRequest is not None:
        try:
            if QueryOrderStatus is not None:
                req = GetOrdersRequest(status=QueryOrderStatus.OPEN)
            else:
                req = GetOrdersRequest(status="open")
            return client.get_orders(filter=req)  # type: ignore
        except Exception:
            pass

    # Fallbacks for older SDK signatures
    try:
        return client.get_orders(status="open")  # type: ignore
    except Exception:
        # last resort: no filter, then caller filters (not ideal)
        try:
            return client.get_orders()  # type: ignore
        except Exception:
            return []


# -----------------------------
# Main poll logic
# -----------------------------
def poll_once(mode: str = "LIVE") -> Dict[str, Any]:
    ts = utc_now_iso()
    paper = env_bool("APCA_PAPER", PAPER_DEFAULT)

    # Load existing state
    open_orders_doc = read_json(OPEN_ORDERS_PATH, {"ts": ts, "mode": mode, "orders": {}})
    orders_map: Dict[str, Any] = open_orders_doc.get("orders", {}) or {}

    alerts_state = read_json(ALERTS_STATE_PATH, {"ts": ts, "last_alert": {}})
    last_alert: Dict[str, Any] = alerts_state.get("last_alert", {}) or {}

    changed: List[Dict[str, Any]] = []
    errors: List[str] = []

    journal(
        {
            "ts": ts,
            "intent_type": "OMS_POLL",
            "intent_ts": ts,
            "stage": "POLL_START",
            "ok": True,
            "mode": mode,
            "msg": "",
            "data": {"n_orders": len(orders_map), "paper": paper},
        }
    )

    # ---- client + discovery
    t0 = datetime.now(timezone.utc)
    client = build_trading_client(paper=paper)

    discovered_open = list_open_orders(client)
    discovered_ids: Dict[str, Any] = {}
    for o in discovered_open:
        oid = str(getattr(o, "id", "") or "")
        if not oid:
            continue
        discovered_ids[oid] = o

    # Merge discovered open orders into tracking map (auto-discovery)
    tag_prefix = get_tag_prefix_filter()
    for oid, o in discovered_ids.items():
        inferred_tag = best_effort_tag_from_order(o)

        # If user wants only certain tags auto-tracked, enforce here.
        # NOTE: because tag inference is imperfect, you can leave ORDER_TAG_PREFIX unset.
        if tag_prefix and not inferred_tag.startswith(tag_prefix):
            continue

        if oid not in orders_map:
            orders_map[oid] = {
                "order_id": oid,
                "status": "unknown",
                "tag": inferred_tag,
                "last_seen": ts,
                "paper": paper,
                "raw": {},
            }

    # Track ids = anything we already track + anything currently open on broker
    track_ids = set(orders_map.keys()) | set(discovered_ids.keys())

    # If nothing tracked and nothing open, we still write a clean NO_ORDERS state
    if not track_ids:
        out = {
            "ts": ts,
            "mode": mode,
            "ok": True,
            "state": "NO_ORDERS",
            "n_orders": 0,
            "changed": [],
            "errors": [],
            "elapsed_ms": int((datetime.now(timezone.utc) - t0).total_seconds() * 1000),
        }
        write_json(OMS_POLL_STATE_PATH, out)
        write_json(OPEN_ORDERS_PATH, {"ts": ts, "mode": mode, "orders": {}})
        write_json(ALERTS_STATE_PATH, {"ts": ts, "last_alert": last_alert})
        journal({"ts": ts, "intent_type": "OMS_POLL", "intent_ts": ts, "stage": "POLL_DONE", "ok": True, "mode": mode, "msg": "", "data": out})
        return out

    # ---- process each tracked id
    min_sev = get_min_sev()

    # We update orders_map in-place; collect terminal removals
    terminal_remove: List[str] = []

    for oid in sorted(track_ids):
        prev_entry = orders_map.get(oid, {})
        prev_status = normalize_status(prev_entry.get("status", "unknown"))
        prev_tag = prev_entry.get("tag", get_default_tag())

        order_obj = discovered_ids.get(oid)
        if order_obj is None:
            # Not in open list (might be terminal now). Fetch by id to capture terminal transitions.
            try:
                order_obj = client.get_order_by_id(oid)  # type: ignore
            except Exception as e:
                err = f"GET_ORDER_FAILED:{type(e).__name__}:{e}"
                errors.append(err)
                journal({"ts": ts, "intent_type": "OMS_POLL", "intent_ts": ts, "stage": "ORDER_FETCH_ERROR", "ok": False, "mode": mode, "msg": err, "data": {"order_id": oid}})
                continue

        # normalize + raw
        status_norm = normalize_status(getattr(order_obj, "status", None))
        raw = order_to_raw_dict(order_obj)

        # tag: keep existing if already tracked, else infer
        tag = prev_tag if prev_tag and prev_tag != "UNKNOWN" else best_effort_tag_from_order(order_obj)

        # update tracking entry
        orders_map[oid] = {
            "order_id": oid,
            "status": status_norm,
            "tag": tag,
            "last_seen": ts,
            "paper": paper,
            "raw": raw,
        }

        # detect change
        if status_norm != prev_status:
            changed.append({"order_id": oid, "prev": prev_status, "new": status_norm})
            sev = severity_for_status(status_norm)

            # dedupe: only email when the status changes vs what we last alerted for
            last = last_alert.get(oid, {})
            last_status_alerted = normalize_status(last.get("status", ""))
            last_sev_alerted = (last.get("sev", "") or "").upper()

            # Alert only in LIVE mode
            if mode.upper() == "LIVE" and should_alert(sev, min_sev) and (status_norm != last_status_alerted or sev.upper() != last_sev_alerted):
                subject = f"{subject_prefix()} {sev} {tag} {oid[:8]} {prev_status}→{status_norm}"
                body = (
                    f"ts: {ts}\n"
                    f"mode: {mode}\n"
                    f"paper: {paper}\n"
                    f"order_id: {oid}\n"
                    f"tag: {tag}\n"
                    f"severity: {sev}\n"
                    f"prev_status: {prev_status}\n"
                    f"new_status: {status_norm}\n\n"
                    f"raw (excerpt):\n"
                    f"{json.dumps(raw, indent=2, default=_json_default)[:4000]}\n"
                )
                ok, info = send_email(subject, body)
                journal(
                    {
                        "ts": ts,
                        "intent_type": "OMS_POLL",
                        "intent_ts": ts,
                        "stage": "ALERT_EMAIL",
                        "ok": ok,
                        "mode": mode,
                        "msg": info,
                        "data": {"order_id": oid, "prev": prev_status, "new": status_norm, "sev": sev, "tag": tag},
                    }
                )
                last_alert[oid] = {"ts": ts, "status": status_norm, "sev": sev, "email": {"ok": ok, "info": info}}

            # mark terminal removals
            if status_norm in TERMINAL_STATUSES:
                journal(
                    {
                        "ts": ts,
                        "intent_type": "OMS_POLL",
                        "intent_ts": ts,
                        "stage": "TERMINAL",
                        "ok": True,
                        "mode": mode,
                        "msg": "",
                        "data": {"order_id": oid, "status": status_norm, "tag": tag},
                    }
                )
                terminal_remove.append(oid)

        else:
            # still terminal? (in case it was terminal but previously unknown)
            if status_norm in TERMINAL_STATUSES:
                terminal_remove.append(oid)

    # prune terminal
    for oid in terminal_remove:
        orders_map.pop(oid, None)

    # Build output state
    elapsed_ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
    state_name = "POLL_OK"
    if not orders_map:
        state_name = "NO_ORDERS"

    out = {
        "ts": ts,
        "mode": mode,
        "ok": True,
        "state": state_name,
        "n_orders": len(orders_map),
        "changed": changed,
        "errors": errors,
        "elapsed_ms": elapsed_ms,
    }

    # persist files
    write_json(OMS_POLL_STATE_PATH, out)
    write_json(OPEN_ORDERS_PATH, {"ts": ts, "mode": mode, "orders": orders_map})
    write_json(ALERTS_STATE_PATH, {"ts": ts, "last_alert": last_alert})

    journal({"ts": ts, "intent_type": "OMS_POLL", "intent_ts": ts, "stage": "POLL_DONE", "ok": True, "mode": mode, "msg": "", "data": out})
    return out


def poll_main(mode: str) -> None:
    """
    Run a single poll and exit (best for systemd timers).
    If you want a loop, do it at the systemd layer (timer) not here.
    """
    ts = utc_now_iso()
    try:
        poll_once(mode=mode)
    except Exception as e:
        # client error path: write state + optional one-time email
        err = f"CLIENT_ERROR:{type(e).__name__}:{e}"
        tb = traceback.format_exc()

        # load alerts state to dedupe client-error emails
        alerts_state = read_json(ALERTS_STATE_PATH, {"ts": ts, "last_alert": {}})
        last_alert: Dict[str, Any] = alerts_state.get("last_alert", {}) or {}

        out = {
            "ts": ts,
            "mode": mode,
            "ok": False,
            "state": "CLIENT_ERROR",
            "n_orders": 0,
            "changed": [],
            "errors": [err],
            "elapsed_ms": 0,
        }

        write_json(OMS_POLL_STATE_PATH, out)

        journal({"ts": ts, "intent_type": "OMS_POLL", "intent_ts": ts, "stage": "CLIENT_ERROR", "ok": False, "mode": mode, "msg": err, "data": {}})

        # email once per unique error string (LIVE only)
        if mode.upper() == "LIVE":
            prev_err = last_alert.get("_client_error")
            if prev_err != err:
                subject = f"{subject_prefix()} RED CLIENT_ERROR"
                body = f"ts: {ts}\nmode: {mode}\nerror: {err}\n\ntraceback:\n{tb}\n"
                ok, info = send_email(subject, body)
                last_alert["_client_error"] = err
                last_alert["_client_error_email"] = {"ts": ts, "ok": ok, "info": info}
                write_json(ALERTS_STATE_PATH, {"ts": ts, "last_alert": last_alert})


def main() -> None:
    mode = (os.getenv("MODE") or "LIVE").strip().upper()
    if mode not in ("LIVE", "PLAN_ONLY"):
        mode = "LIVE"
    poll_main(mode=mode)


if __name__ == "__main__":
    main()