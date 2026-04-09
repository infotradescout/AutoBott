"""Alert delivery helpers (Discord + generic webhook)."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import requests

import config


class AlertManager:
    def __init__(self):
        self.discord_webhook_url = (config.DISCORD_WEBHOOK_URL or "").strip()
        self.generic_webhook_url = (config.ALERT_WEBHOOK_URL or "").strip()
        self.cooldown_seconds = max(0, int(config.ALERT_COOLDOWN_SECONDS))
        self._last_sent_by_key: dict[str, float] = {}

    def enabled(self) -> bool:
        return bool(self.discord_webhook_url or self.generic_webhook_url)

    def send(self, event: str, message: str, *, level: str = "info", dedupe_key: str | None = None) -> None:
        if not self.enabled():
            return

        key = dedupe_key or event
        now_ts = time.time()
        if self.cooldown_seconds > 0:
            last_ts = self._last_sent_by_key.get(key, 0)
            if (now_ts - last_ts) < self.cooldown_seconds:
                return

        if self.discord_webhook_url:
            self._post_discord(message)
        if self.generic_webhook_url:
            self._post_generic(event=event, level=level, message=message)
        self._last_sent_by_key[key] = now_ts

    def _post_discord(self, message: str) -> None:
        try:
            requests.post(
                self.discord_webhook_url,
                json={"content": message},
                timeout=10,
            ).raise_for_status()
        except Exception as exc:  # noqa: BLE001
            print(f"[alerts] Discord webhook failed: {exc}")

    def _post_generic(self, *, event: str, level: str, message: str) -> None:
        payload: dict[str, Any] = {
            "event": event,
            "level": level,
            "message": message,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
        try:
            requests.post(
                self.generic_webhook_url,
                json=payload,
                timeout=10,
            ).raise_for_status()
        except Exception as exc:  # noqa: BLE001
            print(f"[alerts] Generic webhook failed: {exc}")

