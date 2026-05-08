"""Environment loading and validation helpers."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

_THIS_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _THIS_DIR.parent
_ENV_CANDIDATES = (
    _PROJECT_ROOT / ".env",
    _THIS_DIR / ".env",
)
_LOADED = False
_REQUIRED_ENV_ALIASES: dict[str, tuple[str, ...]] = {
    "ALPACA_API_KEY": (
        "ALPACA_API_KEY",
        "APCA_API_KEY_ID",
        "APCA_API_KEY",
        "ALPACA_LIVE_API_KEY",
    ),
    "ALPACA_SECRET_KEY": (
        "ALPACA_SECRET_KEY",
        "APCA_API_SECRET_KEY",
        "APCA_API_SECRET",
        "ALPACA_LIVE_SECRET_KEY",
    ),
}


def _clean_env_value(value: str | None) -> str:
    cleaned = str(value or "").strip()
    if (
        len(cleaned) >= 2
        and cleaned[0] == cleaned[-1]
        and cleaned[0] in {"'", '"'}
    ):
        cleaned = cleaned[1:-1].strip()
    return cleaned


def _normalize_env_aliases() -> None:
    for canonical_name, aliases in _REQUIRED_ENV_ALIASES.items():
        if _clean_env_value(os.getenv(canonical_name)):
            continue
        for alias_name in aliases:
            alias_value = _clean_env_value(os.getenv(alias_name))
            if alias_value:
                os.environ[canonical_name] = alias_value
                break


def load_runtime_env() -> Path | None:
    """Load env vars from the first existing known env file."""
    global _LOADED
    if _LOADED:
        return None

    loaded_from: Path | None = None
    for path in _ENV_CANDIDATES:
        if path.exists():
            load_dotenv(dotenv_path=path, override=False)
            loaded_from = path
            break

    _normalize_env_aliases()
    _LOADED = True
    return loaded_from


def get_required_env(name: str) -> str:
    """Return a required env var or raise with a clear startup error."""
    _normalize_env_aliases()
    cleaned = _clean_env_value(os.getenv(name))
    if cleaned:
        return cleaned
    for alias_name in _REQUIRED_ENV_ALIASES.get(name, ()):
        alias_value = _clean_env_value(os.getenv(alias_name))
        if alias_value:
            os.environ[name] = alias_value
            return alias_value
    searched = ", ".join(str(path) for path in _ENV_CANDIDATES)
    aliases = [alias for alias in _REQUIRED_ENV_ALIASES.get(name, ()) if alias != name]
    alias_help = f" Accepted aliases: {', '.join(aliases)}." if aliases else ""
    raise RuntimeError(
        f"Missing required environment variable '{name}'. "
        f"{alias_help}Searched env files: {searched}."
    )
