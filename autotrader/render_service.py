"""Render single-service runner: starts trader loop + dashboard in one process."""

from __future__ import annotations

import os
import threading
import time
import traceback

from dashboard import app
from main import main as trader_main


def _run_trader_forever() -> None:
    while True:
        try:
            trader_main()
        except Exception as exc:  # noqa: BLE001
            print(f"[render_service] Trader crashed: {exc}")
            traceback.print_exc()
        # Always restart trader loop so service can stay 24/7.
        time.sleep(30)


if __name__ == "__main__":
    trader_thread = threading.Thread(target=_run_trader_forever, daemon=True)
    trader_thread.start()

    port = int(os.getenv("PORT", "5000"))
    print(f"[render_service] Starting dashboard on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
