from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


spec = importlib.util.spec_from_file_location("validate_offline", Path(__file__).resolve().parents[1] / "scripts" / "validate_offline.py")
validator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validator)


def test_offline_environment_removes_hosted_settings_and_credentials(tmp_path: Path) -> None:
    inherited = {
        "PATH": "/usr/bin", "SYSTEMROOT": "C:/Windows",
        "RENDER": "true", "RENDER_SERVICE_ID": "synthetic-service", "PORT": "10000",
        "HOST": "0.0.0.0", "ALPACA_API_KEY_ID": "synthetic-key",
        "ALPACA_API_SECRET_KEY": "synthetic-secret", "UNRELATED_SECRET": "synthetic-secret",
        "AUTOBOTT_ENV_FILE": "/private/operator.env", "AUTOBOTT_SESSION_AUTOSTART": "true",
        "AUTOBOTT_ALLOW_ORDER_PLACEMENT": "true", "AUTOBOTT_DATA_ROOT": "/var/data/autobott/data",
        "PYTEST_ADDOPTS": "--unexpected-plugin", "PYTHONPATH": "/untrusted",
    }
    env = validator.isolated_environment(inherited, tmp_path)

    assert {key for key in inherited if key in env} == {"PATH", "SYSTEMROOT", "AUTOBOTT_ENV_FILE", "AUTOBOTT_DATA_ROOT"}
    assert env["AUTOBOTT_ENV_FILE"] == str(tmp_path / "empty.env")
    assert env["AUTOBOTT_DATA_ROOT"] == str(tmp_path / "data")
    assert env["AUTOBOTT_ARTIFACTS_ROOT"] == str(tmp_path / "runtime-artifacts")
    assert env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"
    assert env["GIT_OPTIONAL_LOCKS"] == "0"


@pytest.mark.parametrize("event", [
    "socket.connect", "socket.getaddrinfo", "socket.gethostbyname",
    "socket.gethostbyaddr", "socket.getnameinfo", "socket.sendto", "socket.sendmsg",
])
def test_offline_guard_blocks_and_records_network_operations(event: str) -> None:
    violations = {}
    with pytest.raises(PermissionError, match="offline_validation_network_denied"):
        validator.audit_event(event, (), violations)
    assert violations == {event: 1}


@pytest.mark.parametrize("event,args", [
    ("subprocess.Popen", (None, "git ls-files", None, None)),
    ("subprocess.Popen", ("git", ["git", "rev-parse", "HEAD"], None, None)),
    ("os.posix_spawn", ("/usr/bin/git", ["git", "status", "--porcelain"], {})),
])
def test_offline_guard_allows_read_only_git_on_windows_and_posix(event: str, args: tuple) -> None:
    violations = {}
    validator.audit_event(event, args, violations)
    assert not violations


@pytest.mark.parametrize("event,args", [
    ("subprocess.Popen", (None, "git push", None, None)),
    ("subprocess.Popen", ("curl", ["curl", "https://broker.invalid"], None, None)),
    ("os.posix_spawn", ("/usr/bin/python", ["python", "-c", "pass"], {})),
    ("os.system", ("git ls-files",)),
    ("os.exec", ("/usr/bin/git", ["git", "ls-files"], {})),
])
def test_offline_guard_blocks_process_escape_paths(event: str, args: tuple) -> None:
    violations = {}
    with pytest.raises(PermissionError, match="offline_validation_process_denied"):
        validator.audit_event(event, args, violations)
    assert violations == {event: 1}


def test_swallowed_network_violation_still_fails_validation() -> None:
    assert validator.validation_exit_code(0, {"socket.connect": 1}) == 1
    assert validator.validation_exit_code(2, {"socket.connect": 1}) == 2
    assert validator.validation_exit_code(0, {}) == 0
