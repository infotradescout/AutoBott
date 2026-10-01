"""Run Python validation without deployment settings, credentials, or network access."""
from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
import os
import platform
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import time
import xml.etree.ElementTree as ET


REPO_ROOT = Path(__file__).resolve().parents[1]
OS_ENV_KEYS = {"SYSTEMROOT", "WINDIR", "PATH", "PATHEXT", "COMSPEC", "SYSTEMDRIVE"}
NETWORK_EVENTS = {
    "socket.connect", "socket.getaddrinfo", "socket.gethostbyname",
    "socket.gethostbyaddr", "socket.getnameinfo", "socket.sendto", "socket.sendmsg",
}
READ_ONLY_GIT_COMMANDS = {"rev-parse", "status", "ls-files", "check-ignore"}


def isolated_environment(inherited: dict[str, str], run_dir: Path) -> dict[str, str]:
    env = {key: value for key, value in inherited.items() if key.upper() in OS_ENV_KEYS}
    env.update({
        "USERPROFILE": str(run_dir / "profile"),
        "TEMP": str(run_dir / "temp"),
        "TMP": str(run_dir / "temp"),
        "TMPDIR": str(run_dir / "temp"),
        "AUTOBOTT_ENV_FILE": str(run_dir / "empty.env"),
        "AUTOBOTT_DATA_ROOT": str(run_dir / "data"),
        "AUTOBOTT_ARTIFACTS_ROOT": str(run_dir / "runtime-artifacts"),
        "AUTOBOTT_GATE_PATH": str(run_dir / "data" / "PHASE1_CYCLE_GATE.json"),
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "GIT_OPTIONAL_LOCKS": "0",
    })
    return env


def audit_event(event: str, args: tuple, violations: dict[str, int]) -> None:
    if event in NETWORK_EVENTS:
        violations[event] = violations.get(event, 0) + 1
        raise PermissionError("offline_validation_network_denied")
    if event in {"subprocess.Popen", "os.posix_spawn"}:
        executable, argv, *_ = args
        parts = shlex.split(argv, posix=False) if isinstance(argv, str) else list(argv)
        program = executable or (parts[0].strip('"') if parts else "")
        if Path(str(program)).name.lower() in {"git", "git.exe"} and len(parts) >= 2 and parts[1] in READ_ONLY_GIT_COMMANDS:
            return
    elif event not in {"os.system", "os.startfile", "os.startfile/2", "os.exec", "os.spawn"}:
        return
    violations[event] = violations.get(event, 0) + 1
    raise PermissionError("offline_validation_process_denied")


def validation_exit_code(pytest_exit_code: int, violations: dict[str, int]) -> int:
    return pytest_exit_code if pytest_exit_code else (1 if violations else 0)


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=REPO_ROOT, text=True).strip()


def source_identity() -> dict:
    if not (REPO_ROOT / ".git").exists():
        return {"sha": None, "status": "source_export"}
    paths = sorted(set(_git("ls-files", "--cached", "--others", "--exclude-standard").splitlines()))
    manifest = {path: hashlib.sha256((REPO_ROOT / path).read_bytes()).hexdigest() for path in paths if (REPO_ROOT / path).is_file()}
    return {
        "sha": _git("rev-parse", "HEAD"),
        "status": _git("status", "--porcelain"),
        "source_manifest_sha256": hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest(),
    }


def main(argv: list[str] | None = None) -> int:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    run_dir = REPO_ROOT / "artifacts" / "offline-validation" / stamp
    run_dir.mkdir(parents=True)
    for name in ("data", "runtime-artifacts", "temp", "profile"):
        (run_dir / name).mkdir()
    (run_dir / "empty.env").write_text("# Offline validation: no credentials.\n", encoding="utf-8")
    env = isolated_environment(dict(os.environ), run_dir)
    os.environ.clear()
    os.environ.update(env)
    sys.dont_write_bytecode = True
    os.chdir(REPO_ROOT)
    sys.path.insert(0, str(REPO_ROOT / "src"))

    # Pytest's JUnit hostname lookup calls platform.uname(). On Windows,
    # Python 3.11 may probe the OS with local subprocesses. Cache that local
    # metadata before installing the guard; never allow those processes in tests.
    platform.uname()

    violations: dict[str, int] = {}
    sys.addaudithook(lambda event, args: audit_event(event, args, violations))
    with socket.socket() as probe:
        probe.settimeout(0.1)
        try:
            probe.connect(("192.0.2.1", 443))
        except PermissionError as exc:
            if str(exc) != "offline_validation_network_denied":
                raise
        else:
            raise RuntimeError("offline_network_guard_self_test_failed")
    guard_self_test = dict(violations)
    violations.clear()

    before = source_identity()
    import pytest

    junit_path = run_dir / "pytest.xml"
    pytest_args = [*(argv or ["tests"]), "--basetemp", str(run_dir / "pytest-temp"), "-o", f"cache_dir={run_dir / 'pytest-cache'}", "--junitxml", str(junit_path)]
    started = time.perf_counter()
    pytest_result = int(pytest.main(pytest_args))
    elapsed = time.perf_counter() - started
    after = source_identity()
    suite = ET.parse(junit_path).getroot().find("testsuite") if junit_path.exists() else None
    result = validation_exit_code(pytest_result, violations)
    if before != after:
        result = result or 1
    summary = {
        "source_before": before,
        "source_after": after,
        "python": sys.version.split()[0],
        "pytest": pytest.__version__,
        "pytest_args": pytest_args,
        "pytest_exit_code": pytest_result,
        "exit_code": result,
        "elapsed_seconds": round(elapsed, 4),
        "tests": {key: suite.get(key) for key in ("tests", "failures", "errors", "skipped")} if suite is not None else None,
        "guard_self_test": guard_self_test,
        "platform_metadata_cached_before_guard": True,
        "blocked_operations_during_tests": dict(violations),
        "isolation": "OS-only inherited environment; empty env sentinel; dedicated runtime/temp roots; socket/DNS/send operations denied; only read-only local Git subprocesses; pytest plugin autoload disabled",
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"Evidence directory: {run_dir}")
    return result


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
