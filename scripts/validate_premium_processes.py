"""Finite, credential-free process proof for the native premium ledger."""
from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile


def deny_network(event, args):
    if event.startswith("socket.") or event in {"urllib.Request", "http.client.connect"}:
        raise RuntimeError("premium_process_proof_network_forbidden")


sys.addaudithook(deny_network)

from autobott_v2.execution_models import OrderSide, OrderType, TradeIntent
from autobott_v2.portfolio_budget import BudgetBlocked, BudgetSnapshot, PremiumLedger


def empty_snapshot():
    return BudgetSnapshot("alpaca:paper:process-fixture", 0, 0, 1000000, frozenset(), {}, datetime.now(UTC))


def attempt(path, index, dollars=200):
    item = TradeIntent(symbol=f"PROC{index}", option_symbol=f"PROC{index}261002C00100000",
        side=OrderSide.BUY_TO_OPEN, quantity=1, limit_price=dollars/100, generated_at=datetime.now(UTC),
        order_type=OrderType.LIMIT, decision_id=f"process-{index}")
    return PremiumLedger(Path(path)).reserve((item,), limit_dollars=1000, read_snapshot=empty_snapshot, max_legs=60)


def main():
    if len(sys.argv) == 4 and sys.argv[1] in {"--child", "--crash"}:
        try:
            receipt, specs = attempt(sys.argv[2], int(sys.argv[3]))
        except BudgetBlocked as exc:
            if str(exc) != "portfolio_premium_budget_exceeded":
                raise
            print("blocked", flush=True)
            return 0
        if sys.argv[1] == "--crash":
            PremiumLedger(Path(sys.argv[2])).mark_attempted(specs[0])
            os._exit(17)
        print("admitted", flush=True)
        return 0
    if len(sys.argv) != 1:
        raise ValueError("invalid_process_proof_arguments")
    env = {key: value for key, value in os.environ.items() if key in {"PATH", "HOME", "SYSTEMROOT", "TMPDIR", "TEMP", "TMP", "PYTHONPATH"}}
    with tempfile.TemporaryDirectory(prefix="autobott-premium-process-proof-") as directory:
        path = str(Path(directory) / "competition.sqlite")
        children = [subprocess.Popen([sys.executable, __file__, "--child", path, str(index)],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env) for index in range(12)]
        outputs = []
        try:
            for child in children:
                stdout, stderr = child.communicate(timeout=30)
                if child.returncode != 0:
                    raise AssertionError("independent_process_failed:" + stderr[-1000:])
                outputs.append(stdout.strip())
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=5)
        assert outputs.count("admitted") == 5, outputs
        assert outputs.count("blocked") == 7, outputs
        with sqlite3.connect(path) as connection:
            cents, rows = connection.execute("SELECT SUM(cents),COUNT(*) FROM reservations WHERE state='reserved'").fetchone()
        assert (cents, rows) == (100000, 5)
        crash_path = str(Path(directory) / "crash.sqlite")
        crash = subprocess.run([sys.executable, __file__, "--crash", crash_path, "99"], env=env,
                               capture_output=True, text=True, timeout=30)
        assert crash.returncode == 17
        with sqlite3.connect(crash_path) as connection:
            assert connection.execute("SELECT cents,state FROM reservations").fetchone() == (20000, "attempted")
        receipt, _ = attempt(crash_path, 100, 800)
        assert receipt["remaining_dollars"] == 0
        assert receipt["uncertain_submission_dollars"] == 200
        try:
            attempt(crash_path, 101, 1)
        except BudgetBlocked as exc:
            assert str(exc) == "portfolio_premium_budget_exceeded"
        else:
            raise AssertionError("crash_lost_reservation")
    print("AUTOBOTT_PREMIUM_PROCESS_PROOF " + json.dumps({"ok": True, "independent_competitors": 12,
        "admitted": 5, "blocked": 7, "budget_dollars": 1000, "committed_dollars": 1000,
        "forced_exit_reservation_retained": True, "network_allowed": False, "broker_orders": 0}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
