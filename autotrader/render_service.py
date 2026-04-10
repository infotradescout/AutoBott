"""Render single-service runner: starts trader loop + dashboard in one process."""

from __future__ import annotations

import os
import threading
import time
import traceback
from pathlib import Path

from env_config import load_runtime_env

load_runtime_env()


def _force_writable_data_dir() -> None:
    current = (os.getenv("DATA_DIR") or "").strip()
    if not current:
        os.environ["DATA_DIR"] = "/tmp/autotrader-data"
        return

    target = Path(current)
    try:
        target.mkdir(parents=True, exist_ok=True)
        probe = target / ".write_test"
        with probe.open("w", encoding="utf-8") as f:
            f.write("ok")
        probe.unlink(missing_ok=True)
    except Exception:
        fallback = Path("/tmp/autotrader-data")
        fallback.mkdir(parents=True, exist_ok=True)
        os.environ["DATA_DIR"] = str(fallback)
        print(
            f"[render_service] DATA_DIR '{current}' not writable. "
            f"Using '{fallback}'."
        )


_force_writable_data_dir()

from alerts import AlertManager
from dashboard import app
from main import main as trader_main

ALERTS = AlertManager()


def _run_trader_forever() -> None:
    while True:
        try:
            trader_main()
        except Exception as exc:  # noqa: BLE001
            print(f"[render_service] Trader crashed: {exc}")
            traceback.print_exc()
            ALERTS.send(
                "trader_crash",
                f"Trader crashed and will restart in 30 seconds: {exc}",
                level="error",
                dedupe_key=f"trader-crash-{int(time.time() // 60)}",
            )
        # Always restart trader loop so service can stay 24/7.
        time.sleep(30)


if __name__ == "__main__":
    trader_thread = threading.Thread(target=_run_trader_forever, daemon=True)
    trader_thread.start()

    port = int(os.getenv("PORT", "5000"))
    print(f"[render_service] Starting dashboard on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
