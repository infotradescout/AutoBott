from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _tracked_paths() -> list[str]:
    git_index = (REPO_ROOT / ".git" / "index")
    if not git_index.exists():
        return []
    import subprocess

    result = subprocess.run(
        ["git", "ls-files"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _is_git_ignored(path: Path) -> bool:
    import subprocess

    result = subprocess.run(
        ["git", "check-ignore", "-q", str(path)],
        cwd=REPO_ROOT,
        check=False,
    )
    return result.returncode == 0


def test_forbidden_generated_artifacts_not_tracked() -> None:
    tracked = _tracked_paths()

    forbidden_substrings = (
        ".egg-info/",
        "__pycache__/",
        ".pytest_cache/",
        ".mypy_cache/",
        "build/",
        "dist/",
    )
    forbidden_exact_names = {
        "PKG-INFO",
        "SOURCES.txt",
        "dependency_links.txt",
        "requires.txt",
        "top_level.txt",
    }

    offenders: list[str] = []
    for path in tracked:
        normalized = path.replace("\\", "/")
        if any(token in normalized for token in forbidden_substrings):
            offenders.append(path)
            continue
        if Path(normalized).name in forbidden_exact_names and ".egg-info/" in normalized:
            offenders.append(path)

    assert not offenders, f"Forbidden generated artifacts are tracked: {offenders}"


def test_source_tree_has_no_generated_artifact_dirs() -> None:
    source_roots = [REPO_ROOT / "src", REPO_ROOT / "tests"]
    forbidden_dir_names = {
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        "build",
        "dist",
    }

    offenders: list[str] = []
    for root in source_roots:
        if not root.exists():
            continue
        for directory in root.rglob("*"):
            if not directory.is_dir():
                continue
            matches_forbidden = (
                directory.name in forbidden_dir_names or directory.name.endswith(".egg-info")
            )
            if matches_forbidden and not _is_git_ignored(directory):
                offenders.append(str(directory.relative_to(REPO_ROOT)).replace("\\", "/"))

    assert not offenders, (
        "Generated artifact directories in source paths must be ignored by git. "
        f"Not ignored: {offenders}"
    )
