from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any


MIB = 1024 * 1024


def prune_snapshot_storage(
    root: str | Path,
    *,
    max_bytes: int | None = None,
    min_free_bytes: int | None = None,
) -> dict[str, Any]:
    """Bound raw snapshot storage without touching execution/outcome journals."""

    snapshot_root = Path(root)
    resolved_max = max(1, max_bytes if max_bytes is not None else _env_bytes("AUTOBOTT_SNAPSHOT_MAX_BYTES", 128 * MIB))
    resolved_min_free = max(
        0,
        min_free_bytes if min_free_bytes is not None else _env_bytes("AUTOBOTT_MIN_FREE_BYTES", 128 * MIB),
    )
    if not snapshot_root.exists():
        return {
            "ok": True,
            "enabled": True,
            "root": str(snapshot_root),
            "files_deleted": 0,
            "bytes_deleted": 0,
            "snapshot_bytes": 0,
            "free_bytes": None,
        }

    files = _snapshot_files(snapshot_root)
    snapshot_bytes = sum(size for _, size, _ in files)
    free_bytes = _disk_free_bytes(snapshot_root)
    files_deleted = 0
    bytes_deleted = 0

    for path, size, _ in files:
        if snapshot_bytes <= resolved_max and (free_bytes is None or free_bytes >= resolved_min_free):
            break
        try:
            path.unlink()
        except FileNotFoundError:
            continue
        snapshot_bytes = max(0, snapshot_bytes - size)
        bytes_deleted += size
        files_deleted += 1
        if free_bytes is not None:
            free_bytes += size

    _remove_empty_directories(snapshot_root)
    actual_free = _disk_free_bytes(snapshot_root)
    return {
        "ok": snapshot_bytes <= resolved_max and (actual_free is None or actual_free >= resolved_min_free),
        "enabled": True,
        "root": str(snapshot_root),
        "max_bytes": resolved_max,
        "min_free_bytes": resolved_min_free,
        "files_deleted": files_deleted,
        "bytes_deleted": bytes_deleted,
        "snapshot_bytes": snapshot_bytes,
        "free_bytes": actual_free,
    }


def _snapshot_files(root: Path) -> list[tuple[Path, int, float]]:
    rows: list[tuple[Path, int, float]] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        rows.append((path, stat.st_size, stat.st_mtime))
    rows.sort(key=lambda row: (row[2], str(row[0])))
    return rows


def _remove_empty_directories(root: Path) -> None:
    directories = sorted(
        (path for path in root.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    )
    for path in directories:
        try:
            path.rmdir()
        except OSError:
            pass


def _disk_free_bytes(path: Path) -> int | None:
    try:
        return int(shutil.disk_usage(path).free)
    except OSError:
        return None


def _env_bytes(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return int(raw)
