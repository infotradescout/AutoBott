"""Optional Redis-backed key/value JSON storage."""

from __future__ import annotations

import json
import os
from typing import Any

try:
    import redis
except Exception:  # pragma: no cover
    redis = None


_CLIENT = None
_INIT_FAILED = False


def _redis_url() -> str:
    return (os.getenv("REDIS_URL", "") or "").strip()


def _key_prefix() -> str:
    return (os.getenv("AUTOBOTT_REDIS_PREFIX", "autobott") or "autobott").strip()


def redis_key(name: str) -> str:
    return f"{_key_prefix()}:{name}"


def get_client():
    global _CLIENT, _INIT_FAILED
    if _CLIENT is not None:
        return _CLIENT
    if _INIT_FAILED:
        return None

    if redis is None:
        _INIT_FAILED = True
        return None
    url = _redis_url()
    if not url:
        _INIT_FAILED = True
        return None

    try:
        client = redis.Redis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        client.ping()
        _CLIENT = client
        return _CLIENT
    except Exception as exc:  # noqa: BLE001
        print(f"[kv] Redis unavailable: {exc}")
        _INIT_FAILED = True
        return None


def load_json(key: str) -> dict[str, Any] | None:
    client = get_client()
    if client is None:
        return None
    try:
        raw = client.get(key)
        if not raw:
            return None
        payload = json.loads(raw)
        if isinstance(payload, dict):
            return payload
    except Exception as exc:  # noqa: BLE001
        print(f"[kv] Redis load failed for {key}: {exc}")
    return None


def save_json(key: str, payload: dict[str, Any]) -> bool:
    client = get_client()
    if client is None:
        return False
    try:
        client.set(key, json.dumps(payload, sort_keys=True))
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"[kv] Redis save failed for {key}: {exc}")
        return False

