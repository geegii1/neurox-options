import json
from datetime import datetime, timezone

from services.portfolio.greeks import build_portfolio_greeks
from services.risk_governor.portfolio_risk import evaluate_portfolio_greeks
from services.risk_governor.derisk_plan import build_derisk_plan
from services.risk_governor.derisk_execute import main as derisk_execute_main
from services.execution.oms_close import main as oms_close_main


def utc_iso():
    return datetime.now(timezone.utc).isoformat()


def read_json(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def main(max_rounds: int = 5):
    history = []
    for r in range(1, max_rounds + 1):
        build_portfolio_greeks()
        eval_out = evaluate_portfolio_greeks()

        mode = read_json("state/risk_mode.json").get("mode")
        history.append({"round": r, "ts": utc_iso(), "mode": mode, "risk_eval": eval_out.get("breaches", [])})

        if mode != "HALT":
            break

        build_derisk_plan(buffer_pct=0.90)
        derisk_execute_main()
        oms_close_main()

    print(json.dumps({"ts": utc_iso(), "rounds": history}, indent=2))


if __name__ == "__main__":
    main()
