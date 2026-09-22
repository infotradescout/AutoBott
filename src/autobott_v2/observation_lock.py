"""Process-owned observation-store lock; a leftover file is not lock ownership.

Use local filesystems with OS locking support. Upgrade from the prior O_EXCL
sentinel implementation only while old observers are stopped. Never unlink this
lock file: replacing its inode could let two writers believe they own the store.
"""
from __future__ import annotations

from contextlib import contextmanager
import errno
import os
from pathlib import Path
import stat
from typing import Iterator


@contextmanager
def observation_store_lock(root: Path) -> Iterator[None]:
    root = Path(root)
    if root.is_symlink():
        raise ValueError("primary_observation_root_symlink_refused")
    root.mkdir(parents=True, exist_ok=True)
    path = root / ".primary-observation.lock"
    if path.is_symlink():
        raise ValueError("primary_observation_lock_symlink_refused")
    if os.name not in {"posix", "nt"}:
        raise RuntimeError("primary_observation_lock_platform_unsupported")
    fd = os.open(path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
    acquired = False
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("primary_observation_lock_not_regular")
        try:
            if os.name == "nt":
                import msvcrt
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                raise FileExistsError("primary_observation_store_busy") from exc
            raise
        acquired = True
        yield
    finally:
        try:
            if acquired:
                if os.name == "nt":
                    import msvcrt
                    os.lseek(fd, 0, os.SEEK_SET)
                    msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
