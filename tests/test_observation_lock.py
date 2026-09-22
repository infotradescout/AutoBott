from pathlib import Path

import pytest

from autobott_v2.observation_lock import observation_store_lock
from autobott_v2 import primary_followthrough, primary_fill_capture, primary_quality_runtime


def test_all_primary_observation_paths_share_the_same_lock():
    assert primary_followthrough._locked is observation_store_lock
    assert primary_fill_capture._locked is observation_store_lock
    assert primary_quality_runtime._locked is observation_store_lock


def test_unowned_leftover_file_does_not_permanently_block_the_store(tmp_path):
    path = tmp_path / ".primary-observation.lock"
    path.write_bytes(b"")
    inode = path.stat().st_ino
    with observation_store_lock(tmp_path):
        assert path.exists()
        assert path.stat().st_ino == inode
    with observation_store_lock(tmp_path):
        assert path.stat().st_ino == inode
    assert path.exists()


def test_an_active_owner_cannot_be_stolen_or_unlinked(tmp_path):
    with observation_store_lock(tmp_path):
        inode = (tmp_path / ".primary-observation.lock").stat().st_ino
        with pytest.raises(FileExistsError, match="primary_observation_store_busy"):
            with observation_store_lock(tmp_path):
                raise AssertionError("concurrent owner acquired the store")
        assert (tmp_path / ".primary-observation.lock").stat().st_ino == inode
    with observation_store_lock(tmp_path):
        assert (tmp_path / ".primary-observation.lock").stat().st_ino == inode


def test_exception_releases_ownership_without_deleting_watch_data(tmp_path):
    watch = tmp_path / "evidence.json"
    watch.write_bytes(b'{"synthetic":"preserve"}\n')
    with pytest.raises(ValueError, match="fixture_failure"):
        with observation_store_lock(tmp_path):
            raise ValueError("fixture_failure")
    with observation_store_lock(tmp_path):
        assert watch.read_bytes() == b'{"synthetic":"preserve"}\n'


def test_symlink_lock_target_is_not_opened(tmp_path):
    target = tmp_path / "original"
    target.write_bytes(b"untouched")
    (tmp_path / ".primary-observation.lock").symlink_to(target)
    with pytest.raises(ValueError, match="primary_observation_lock_symlink_refused"):
        with observation_store_lock(tmp_path):
            raise AssertionError("opened symlink")
    assert target.read_bytes() == b"untouched"
