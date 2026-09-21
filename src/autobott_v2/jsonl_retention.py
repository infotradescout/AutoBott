from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Iterator


DEFAULT_JSONL_MAX_BYTES = 64 * 1024 * 1024
DEFAULT_JSONL_RETAIN_BYTES = 48 * 1024 * 1024


def _retained_lines(
    source,
    *,
    start: int,
    preserve_event_types: frozenset[str],
) -> Iterator[bytes]:
    """Keep receipt bytes in source order, plus the complete activity tail.

    Start from the beginning only when a caller explicitly requests durable
    event types. A receipt crossing the tail boundary is included once, not
    discarded as a partial row or duplicated in the tail.
    """
    offset = 0
    for line in source:
        in_tail = start == 0 or offset > start
        offset += len(line)
        if in_tail:
            yield line
            continue
        try:
            row = json.loads(line)
        except (ValueError, UnicodeError):
            continue
        event_type = row.get("event_type") if isinstance(row, dict) else None
        if isinstance(event_type, str) and event_type in preserve_event_types:
            yield line


def compact_jsonl_tail(
    path: str | Path,
    *,
    max_bytes: int = DEFAULT_JSONL_MAX_BYTES,
    retain_bytes: int = DEFAULT_JSONL_RETAIN_BYTES,
    preserve_event_types: frozenset[str] = frozenset(),
) -> bool:
    """Retain recent activity; explicitly protected receipts never age out.

    With no protected event types, the original size-bounded behavior remains
    unchanged. With protected types, the size budget applies to activity, not
    permission to discard the order identity/provenance needed by accounting.
    The caller must serialize appends and compaction for a shared journal.
    """
    target = Path(path)
    if max_bytes <= 0 or retain_bytes <= 0 or retain_bytes >= max_bytes:
        raise ValueError("invalid_jsonl_retention_limits")
    try:
        size = target.stat().st_size
    except FileNotFoundError:
        return False
    if size <= max_bytes:
        return False

    temporary = target.with_name(f".{target.name}.compact-{os.getpid()}")
    try:
        with target.open("rb") as source, temporary.open("wb") as destination:
            if preserve_event_types:
                for line in _retained_lines(
                    source, start=max(0, size - retain_bytes),
                    preserve_event_types=preserve_event_types,
                ):
                    destination.write(line)
            else:
                source.seek(max(0, size - retain_bytes))
                retained = source.read()
                if size > retain_bytes:
                    _, separator, retained = retained.partition(b"\n")
                    if not separator:
                        retained = b""
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
    preserve_event_types: frozenset[str] = frozenset(),
) -> list[bytes]:
    """Read recent activity and, when requested, all retained receipt rows.

    Older non-receipt rows are streamed and discarded, not loaded as a second
    full journal. Reading never compacts or writes files.
    """
    target = Path(path)
    if not target.exists():
        return []
    size = target.stat().st_size
    start = 0 if max_tail_bytes is None else max(0, size - max_tail_bytes)
    with target.open("rb") as source:
        if preserve_event_types and start:
            return [line.rstrip(b"\r\n") for line in _retained_lines(
                source, start=start, preserve_event_types=preserve_event_types,
            )]
        source.seek(start)
        raw = source.read()
    if start:
        _, _, raw = raw.partition(b"\n")
    return raw.splitlines()
