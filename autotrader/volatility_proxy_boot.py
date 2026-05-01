"""Boot helper for the isolated VIX-derived proxy module."""

from __future__ import annotations

import os
import threading


def start() -> None:
    try:
        import config
        from vixw_regime import run_vixw_regime_forever

        if not bool(getattr(config, "VIXW_HEAVY_MODE", True)):
            print("[vol_proxy] disabled")
            return
        api_key = str(os.getenv("ALPACA_API_KEY") or "").strip()
        secret_key = str(os.getenv("ALPACA_SECRET_KEY") or "").strip()
        if not api_key or not secret_key:
            print("[vol_proxy] missing Alpaca keys")
            return
        worker = threading.Thread(target=run_vixw_regime_forever, args=(api_key, secret_key), daemon=True)
        worker.start()
        print("[vol_proxy] worker started")
    except Exception as exc:  # noqa: BLE001
        print(f"[vol_proxy] boot failed: {exc}")
