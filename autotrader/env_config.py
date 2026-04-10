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

    _LOADED = True
    return loaded_from


def get_required_env(name: str) -> str:
    """Return a required env var or raise with a clear startup error."""
    value = os.getenv(name)
    if value:
        cleaned = value.strip()
        if (
            len(cleaned) >= 2
            and cleaned[0] == cleaned[-1]
            and cleaned[0] in {"'", '"'}
        ):
            cleaned = cleaned[1:-1].strip()
        if cleaned:
            return cleaned
    searched = ", ".join(str(path) for path in _ENV_CANDIDATES)
    raise RuntimeError(
        f"Missing required environment variable '{name}'. "
        f"Searched env files: {searched}."
    )
