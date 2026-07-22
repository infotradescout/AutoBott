from __future__ import annotations

import os
from pathlib import Path


DEFAULT_JSONL_MAX_BYTES = 64 * 1024 * 1024
DEFAULT_JSONL_RETAIN_BYTES = 48 * 1024 * 1024


def compact_jsonl_tail(
    path: str | Path,
    *,
    max_bytes: int = DEFAULT_JSONL_MAX_BYTES,
    retain_bytes: int = DEFAULT_JSONL_RETAIN_BYTES,
) -> bool:
    """Atomically retain complete recent rows once a JSONL journal is bounded."""

    target = Path(path)
    if max_bytes <= 0 or retain_bytes <= 0 or retain_bytes >= max_bytes:
        raise ValueError("invalid_jsonl_retention_limits")
    try:
        size = target.stat().st_size
    except FileNotFoundError:
        return False
    if size <= max_bytes:
        return False

    with target.open("rb") as source:
        source.seek(max(0, size - retain_bytes))
        retained = source.read()
    if size > retain_bytes:
        _, separator, retained = retained.partition(b"\n")
        if not separator:
            retained = b""
    temporary = target.with_name(f".{target.name}.compact-{os.getpid()}")
    try:
        with temporary.open("wb") as destination:
            destination.write(retained)
            destination.flush()
            os.fsync(destination.fileno())
        os.replace(temporary, target)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
    return True


def read_jsonl_tail(
    path: str | Path,
    *,
    max_tail_bytes: int | None = None,
) -> list[bytes]:
    """Return complete recent rows without loading an unbounded file."""

    target = Path(path)
    if not target.exists():
        return []
    size = target.stat().st_size
    start = 0 if max_tail_bytes is None else max(0, size - max_tail_bytes)
    with target.open("rb") as source:
        source.seek(start)
        raw = source.read()
    if start:
        _, _, raw = raw.partition(b"\n")
    return raw.splitlines()
