# services/execution/journal.py
from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from uuid import UUID
except Exception:  # pragma: no cover
    UUID = None  # type: ignore


STATE_DIR = Path(os.environ.get("STATE_DIR", "state"))
JOURNAL_PATH = STATE_DIR / "execution_journal.jsonl"


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_default(o: Any) -> Any:
    """
    Make common non-JSON types safe for json.dumps:
      - UUID -> str
      - datetime -> ISO string
      - Enum -> value
      - Path -> str
      - bytes -> utf-8 (replace)
      - fallback -> str
    """
    # UUID
    if UUID is not None and isinstance(o, UUID):
        return str(o)

    # datetime
    if isinstance(o, datetime):
        if o.tzinfo is None:
            o = o.replace(tzinfo=timezone.utc)
        return o.astimezone(timezone.utc).isoformat()

    # Enum
    if isinstance(o, Enum):
        return o.value

    # Path
    if isinstance(o, Path):
        return str(o)

    # bytes
    if isinstance(o, (bytes, bytearray)):
        try:
            return o.decode("utf-8")
        except Exception:
            return o.decode("utf-8", errors="replace")

    # fallback
    return str(o)


def _ensure_state_dir() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class JournalEvent:
    ts: str
    intent_type: str
    intent_ts: str
    stage: str
    ok: bool
    mode: str
    msg: str = ""
    data: Dict[str, Any] = None  # type: ignore


def mk_event(
    *,
    intent_type: str,
    intent_ts: str,
    stage: str,
    ok: bool,
    mode: str,
    msg: str = "",
    data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    e = JournalEvent(
        ts=utc_iso(),
        intent_type=intent_type,
        intent_ts=intent_ts,
        stage=stage,
        ok=ok,
        mode=mode,
        msg=msg or "",
        data=data or {},
    )
    return asdict(e)


def append_event(event: Dict[str, Any]) -> None:
    """
    Append a single JSON line to state/execution_journal.jsonl.

    IMPORTANT: Must never throw due to non-JSON types (UUID, datetime, Enums, etc).
    """
    _ensure_state_dir()
    line = json.dumps(event, ensure_ascii=False, separators=(",", ":"), default=_json_default)
    with JOURNAL_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
