"""Quick monitor for synthetic trainer status."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import config


def _load(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _render(payload: dict) -> str:
    if not payload:
        return "No status yet. Trainer may still be warming up."
    lines = [
        f"Updated: {payload.get('updated_at_et', '')}",
        f"Running: {payload.get('running', False)}",
        f"Total rows: {payload.get('total_rows', 0)}",
        f"Total evaluated: {payload.get('total_evaluated', 0)}",
        f"Total wins/losses: {payload.get('total_wins', 0)}/{payload.get('total_losses', 0)}",
        f"Total win rate: {payload.get('total_win_rate_pct', 0.0)}%",
        f"Pass rows: {payload.get('pass_rows', 0)}",
        f"Pass wins/losses: {payload.get('pass_wins', 0)}/{payload.get('pass_losses', 0)}",
        f"Pass win rate: {payload.get('pass_win_rate_pct', 0.0)}%",
    ]
    recent = list(payload.get("recent_trades", []) or [])[-8:]
    if recent:
        lines.append("Recent trades:")
        for row in recent:
            lines.append(
                f"  {row.get('timestamp','')} {row.get('symbol','')} {row.get('direction','')} "
                f"{row.get('verdict','')} move={row.get('directional_move_pct',0.0)}%"
            )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor synthetic trainer status.")
    parser.add_argument(
        "--status",
        default=str(Path(config.DATA_DIR) / "synthetic_trainer_status.json"),
    )
    parser.add_argument("--watch", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--interval", type=float, default=3.0)
    args = parser.parse_args()
    path = Path(args.status)
    if not args.watch:
        print(_render(_load(path)))
        return
    while True:
        print("\x1bc", end="")
        print(_render(_load(path)))
        time.sleep(max(0.5, float(args.interval)))


if __name__ == "__main__":
    main()

