from __future__ import annotations

import os
from pathlib import Path

from autobott_v2.storage_retention import prune_snapshot_storage


def _write(path: Path, size: int, *, mtime: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    os.utime(path, (mtime, mtime))


def test_prune_snapshot_storage_removes_oldest_raw_capture_first(tmp_path: Path) -> None:
    root = tmp_path / "phase1_snapshots"
    oldest = root / "2026-07-20" / "SPY" / "snapshots" / "old.json"
    middle = root / "2026-07-21" / "SPY" / "snapshots" / "middle.json"
    newest = root / "2026-07-22" / "SPY" / "snapshots" / "new.json"
    _write(oldest, 60, mtime=1)
    _write(middle, 60, mtime=2)
    _write(newest, 60, mtime=3)

    result = prune_snapshot_storage(root, max_bytes=120, min_free_bytes=0)

    assert result["ok"] is True
    assert result["files_deleted"] == 1
    assert result["bytes_deleted"] == 60
    assert not oldest.exists()
    assert middle.exists()
    assert newest.exists()


def test_prune_snapshot_storage_never_touches_files_outside_snapshot_root(tmp_path: Path) -> None:
    root = tmp_path / "data" / "phase1_snapshots"
    journal = tmp_path / "data" / "execution" / "execution_orders.jsonl"
    _write(root / "2026-07-20" / "SPY" / "snapshots" / "old.json", 100, mtime=1)
    _write(journal, 100, mtime=1)

    result = prune_snapshot_storage(root, max_bytes=1, min_free_bytes=0)

    assert result["files_deleted"] == 1
    assert journal.exists()
