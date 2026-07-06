from __future__ import annotations

import os
from pathlib import Path

from autobott_v2.env_bootstrap import bootstrap_env_file, configure_local_paper_runtime_defaults


def test_bootstrap_env_file_loads_configured_file(monkeypatch, tmp_path: Path) -> None:
    env_file = tmp_path / "AutoBott.env"
    env_file.write_text(
        "\n".join(
            [
                "# comment",
                "ALPACA_API_KEY_ID=test-key",
                "ALPACA_API_SECRET_KEY='test-secret'",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("AUTOBOTT_ENV_FILE", str(env_file))
    monkeypatch.delenv("ALPACA_API_KEY_ID", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET_KEY", raising=False)

    resolved = bootstrap_env_file(repo_root=tmp_path)

    assert resolved == env_file
    assert os.environ["ALPACA_API_KEY_ID"] == "test-key"
    assert os.environ["ALPACA_API_SECRET_KEY"] == "test-secret"


def test_bootstrap_env_file_uses_downloads_candidate(monkeypatch, tmp_path: Path) -> None:
    home = tmp_path / "home"
    downloads = home / "Downloads"
    downloads.mkdir(parents=True)
    env_file = downloads / "AutoBott.env"
    env_file.write_text("ALPACA_API_KEY_ID=download-key\n", encoding="utf-8")
    monkeypatch.setattr(Path, "home", lambda: home)
    monkeypatch.delenv("AUTOBOTT_ENV_FILE", raising=False)
    monkeypatch.delenv("ALPACA_API_KEY_ID", raising=False)

    resolved = bootstrap_env_file(repo_root=tmp_path / "repo")

    assert resolved == env_file
    assert os.environ["ALPACA_API_KEY_ID"] == "download-key"


def test_configure_local_paper_runtime_defaults_sets_expected_values(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("AUTOBOTT_DATA_ROOT", raising=False)
    monkeypatch.delenv("AUTOBOTT_SESSION_AUTOSTART", raising=False)
    monkeypatch.delenv("AUTOBOTT_ALLOW_ORDER_PLACEMENT", raising=False)
    monkeypatch.setenv("ALPACA_LIVE_API_KEY", "remove-me")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "remove-me-too")

    configure_local_paper_runtime_defaults(repo_root=tmp_path)

    assert os.environ["AUTOBOTT_DATA_ROOT"] == str(tmp_path / "data")
    assert os.environ["AUTOBOTT_ARTIFACTS_ROOT"] == str(tmp_path / "artifacts")
    assert os.environ["AUTOBOTT_GATE_PATH"] == str(tmp_path / "data" / "PHASE1_CYCLE_GATE.json")
    assert os.environ["AUTOBOTT_SESSION_AUTOSTART"] == "true"
    assert os.environ["AUTOBOTT_SESSION_ARM_PAPER_EXECUTION"] == "true"
    assert os.environ["AUTOBOTT_ALLOW_ORDER_PLACEMENT"] == "true"
    assert os.environ["AUTOBOTT_DASHBOARD_AUTH_TOKEN"] == "autobott-local"
    assert os.environ["HOST"] == "127.0.0.1"
    assert os.environ["PORT"] == "8000"
    assert "ALPACA_LIVE_API_KEY" not in os.environ
    assert "ALPACA_SECRET_KEY" not in os.environ
