# services/execution/broker_adapter.py
from __future__ import annotations

import os
import json
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# Alpaca SDK (alpaca-py)
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOptionContractsRequest, OptionLegRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass

STATE_DIR = Path("state")
OPEN_ORDERS_PATH = STATE_DIR / "open_orders.json"


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_default(o: Any):
    # make UUIDs, Enums, datetimes, etc. serializable
    try:
        if isinstance(o, uuid.UUID):
            return str(o)
    except Exception:
        pass

    if hasattr(o, "value"):  # Enum
        return getattr(o, "value")

    if isinstance(o, (datetime, date)):
        return o.isoformat()

    if hasattr(o, "model_dump"):  # pydantic
        return o.model_dump()

    if hasattr(o, "dict"):  # older pydantic
        return o.dict()

    return str(o)

def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def _atomic_write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=_json_default)
    os.replace(tmp, path)

def _is_active_order_status(s: str) -> bool:
    """
    Treat these as 'still alive' for dedupe purposes.
    Adjust if you want to allow duplicates after cancel/expired/filled.
    """
    s = (s or "").lower().strip()
    return s in {
        "new",
        "accepted",
        "pending_new",
        "partially_filled",
        "held",
        "replaced",
        "orderstatus.accepted",
        "orderstatus.new",
    }

def _signature_from_request(req: Dict[str, Any]) -> str:
    r = req.get("resolved") or {}
    # use expiration + strikes + call/put + underlier + qty + tag
    under = str(req.get("underlier"))
    exp = str(r.get("expiration") or "")
    is_call = "C" if req.get("is_call") else "P"
    k1 = str(req.get("K_long"))
    k2 = str(req.get("K_short"))
    qty = str(req.get("qty"))
    tag = str(req.get("tag") or "")
    return f"{under}|{exp}|{is_call}|{k1}|{k2}|{qty}|{tag}"

def _utc_today() -> date:
    # good enough for now (you already stamp UTC in other modules)
    return date.today()


def _as_float(x: Any) -> float:
    return float(x)


def _as_int(x: Any) -> int:
    return int(x)


@dataclass
class BrokerAdapter:
    """
    Modes:
      - PLAN_ONLY: resolve/translate only; never submits to broker
      - LIVE: will submit IF ALLOW_LIVE_ORDERS=1 AND LIVE_LIMIT_PRICE is set
    """
    mode: str = "PLAN_ONLY"

    def __post_init__(self) -> None:
        self.mode = os.environ.get("BROKER_MODE", "PLAN_ONLY").strip().upper() or "PLAN_ONLY"

    # --------------------------
    # Public API used by oms_open_exec
    # --------------------------
    def submit_open(self, enriched_request: Dict[str, Any]) -> Dict[str, Any]:
        """
        enriched_request is a dict (your oms_open_exec builds this).
        Must include:
          type=VERTICAL, underlier, is_call, K_long, K_short, dte_days, qty, tag, limit_logic
        """

        # 1) Resolve option symbols (works in PLAN_ONLY and LIVE)
        try:
            resolved = self._resolve_vertical(enriched_request)
        except Exception as e:
            return {
                "ok": False,
                "mode": self.mode,
                "submitted": False,
                "request": enriched_request,
                "resolved": None,
                "order_id": None,
                "error": f"RESOLVE_FAILED:{type(e).__name__}:{e}",
            }

        # Add resolved into request for journaling/debug
        enriched_request = dict(enriched_request)
        enriched_request["resolved"] = resolved

        # 2) PLAN_ONLY short-circuit
        if self.mode == "PLAN_ONLY":
            return {
                "ok": True,
                "mode": self.mode,
                "submitted": False,
                "request": enriched_request,
                "resolved": resolved,
                "order_id": None,
                "error": None,
            }

        # --- SAFETY BLOCK ---
        if self.mode != "PLAN_ONLY":
            if os.environ.get("ALLOW_LIVE_ORDERS", "0") != "1":
                return {
                    "ok": False,
                    "mode": self.mode,
                    "submitted": False,
                    "request": enriched_request,
                    "resolved": resolved,
                    "order_id": None,
                    "error": "LIVE_BLOCKED_SET_ALLOW_LIVE_ORDERS=1",
                }
        # ---------------------

        limit_price_s = os.environ.get("LIVE_LIMIT_PRICE", "").strip()
        if not limit_price_s:
            return {
                "ok": False,
                "mode": self.mode,
                "submitted": False,
                "request": enriched_request,
                "resolved": resolved,
                "order_id": None,
                "error": "LIVE_NEEDS_LIMIT_PRICE_SET_LIVE_LIMIT_PRICE",
            }

        limit_price = float(limit_price_s)

        # 3) Submit REAL multi-leg order to Alpaca
        # IMPORTANT: For order_class=mleg, DO NOT send a top-level "symbol"
        # (Alpaca SDK docs note symbol is for non-mleg orders)  [oai_citation:4‡Alpaca](https://alpaca.markets/sdks/python/api_reference/trading/requests.html)
        try:
            client = self._alpaca_client()

            qty = _as_int(enriched_request.get("qty", 1))

            # Vertical spread:
            # long leg BUY, short leg SELL (ratio_qty=1 each)
            legs = [
                OptionLegRequest(
                    symbol=resolved["long_symbol"],
                    ratio_qty=1.0,
                    side=OrderSide.BUY,
                ),
                OptionLegRequest(
                    symbol=resolved["short_symbol"],
                    ratio_qty=1.0,
                    side=OrderSide.SELL,
                ),
            ]

            order_req = LimitOrderRequest(
                # symbol intentionally omitted for MLEG
                qty=qty,  # qty required for mleg  [oai_citation:5‡Alpaca](https://alpaca.markets/sdks/python/api_reference/trading/requests.html)
                time_in_force=TimeInForce.DAY,
                limit_price=limit_price,
                order_class=OrderClass.MLEG,
                legs=legs,  # legs for mleg
            )

            order = client.submit_order(order_req)

            return {
                "ok": True,
                "mode": self.mode,
                "submitted": True,
                "request": enriched_request,
                "resolved": resolved,
                "order_id": getattr(order, "id", None),
                "error": None,
            }

        except Exception as e:
            # If you still see "symbol is not allowed for mleg order",
            # it means a symbol is still being sent somewhere in the request payload.
            return {
                "ok": False,
                "mode": self.mode,
                "submitted": False,
                "request": enriched_request,
                "resolved": resolved,
                "order_id": None,
                "error": f"ALPACA_SUBMIT_FAILED:{type(e).__name__}:{e}",
            }

    # --------------------------
    # Internals
    # --------------------------
    def _alpaca_client(self) -> TradingClient:
        key = os.environ.get("APCA_API_KEY_ID", "").strip()
        secret = os.environ.get("APCA_API_SECRET_KEY", "").strip()
        base_url = os.environ.get("APCA_API_BASE_URL", "").strip()

        if not key or not secret:
            raise RuntimeError("MISSING_API_KEYS_SET_APCA_API_KEY_ID_AND_APCA_API_SECRET_KEY")

        # paper base url example: https://paper-api.alpaca.markets
        paper = "paper" in base_url if base_url else False
        return TradingClient(api_key=key, secret_key=secret, paper=paper)

    def _resolve_vertical(self, req: Dict[str, Any]) -> Dict[str, Any]:
        """
        Finds exact option contract symbols for the long/short legs.
        Uses GetOptionContractsRequest strike_price_gte/lte which are Optional[str] in alpaca-py  [oai_citation:6‡Alpaca](https://alpaca.markets/sdks/python/api_reference/trading/requests.html)
        """

        under = str(req["underlier"]).upper()
        is_call = bool(req["is_call"])
        k_long = _as_float(req["K_long"])
        k_short = _as_float(req["K_short"])
        dte_days = _as_int(req["dte_days"])

        # Pick an expiration window around target DTE (you can refine later).
        # We'll search +/- 10 days and then choose closest expiration we find.
        target = _utc_today() + timedelta(days=dte_days)
        start = target - timedelta(days=10)
        end = target + timedelta(days=10)

        # Build strike bounds as STRINGS (alpaca-py expects Optional[str])  [oai_citation:7‡Alpaca](https://alpaca.markets/sdks/python/api_reference/trading/requests.html)
        strike_lo = f"{min(k_long, k_short) - 0.001:.3f}"
        strike_hi = f"{max(k_long, k_short) + 0.001:.3f}"

        client = self._alpaca_client()

        # Fetch contracts in a small strike window
        r = GetOptionContractsRequest(
            underlying_symbols=[under],
            expiration_date_gte=start.isoformat(),
            expiration_date_lte=end.isoformat(),
            type="call" if is_call else "put",
            strike_price_gte=strike_lo,
            strike_price_lte=strike_hi,
            limit=1000,
        )

        contracts = client.get_option_contracts(r)

        # Normalize list (alpaca-py returns objects; we read attributes)
        items = getattr(contracts, "option_contracts", None) or getattr(contracts, "contracts", None) or contracts
        if not items:
            raise RuntimeError("NO_CONTRACTS_FOUND")

        # Choose nearest expiration to target
        def _exp(c: Any) -> str:
            return getattr(c, "expiration_date", None) or getattr(c, "expiration", None)

        def _strike(c: Any) -> float:
            return float(getattr(c, "strike_price", None) or getattr(c, "strike", None))

        def _sym(c: Any) -> str:
            return getattr(c, "symbol", None) or getattr(c, "id", None)

        # Filter to the two strikes we need (exact match within tolerance)
        tol = 1e-6
        need = {k_long: None, k_short: None}

        # pick a single best expiration (closest)
        exps = sorted({str(_exp(c)) for c in items if _exp(c)})
        if not exps:
            raise RuntimeError("NO_EXPIRATIONS")

        # choose closest expiration date
        def _to_date(s: str) -> date:
            # expected YYYY-MM-DD
            y, m, d = s.split("-")
            return date(int(y), int(m), int(d))

        exps_sorted = sorted(exps, key=lambda s: abs((_to_date(s) - target).days))
        best_exp = exps_sorted[0]

        for c in items:
            if str(_exp(c)) != best_exp:
                continue
            st = _strike(c)
            for k in list(need.keys()):
                if need[k] is None and abs(st - k) <= tol:
                    need[k] = _sym(c)

        if not need[k_long] or not need[k_short]:
            raise RuntimeError(f"LEG_SYMBOL_NOT_FOUND exp={best_exp} got={need}")

        # Return long/short based on strikes (long strike = K_long in your plan)
        return {
            "long_symbol": need[k_long],
            "short_symbol": need[k_short],
            "expiration": best_exp.replace("-", ""),
            "dte_days": (_to_date(best_exp) - _utc_today()).days,
        }
