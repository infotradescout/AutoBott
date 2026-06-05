"""Boot helper for the isolated VIX-derived proxy module."""

from __future__ import annotations

import os
import threading


def start() -> None:
    try:
        import config
        import runtime_telemetry
        from vixw_regime import run_vixw_regime_forever

        if not bool(getattr(config, "VIXW_HEAVY_MODE", True)):
            runtime_telemetry.set_worker("vixw_regime_sidecar", False, detail="VIXW_HEAVY_MODE=false")
            print("[vol_proxy] disabled")
            return
        api_key = str(os.getenv("ALPACA_API_KEY") or "").strip()
        secret_key = str(os.getenv("ALPACA_SECRET_KEY") or "").strip()
        if not api_key or not secret_key:
            runtime_telemetry.set_worker("vixw_regime_sidecar", False, detail="missing Alpaca keys")
            print("[vol_proxy] missing Alpaca keys")
            return
        worker = threading.Thread(
            target=run_vixw_regime_forever,
            args=(api_key, secret_key),
            daemon=True,
            name="autobott-vixw-regime",
        )
        worker.start()
        runtime_telemetry.set_worker("vixw_regime_sidecar", True)
        print("[vol_proxy] worker started")
    except Exception as exc:  # noqa: BLE001
        print(f"[vol_proxy] boot failed: {exc}")
