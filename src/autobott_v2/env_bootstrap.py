from __future__ import annotations

import os
from pathlib import Path


def bootstrap_env_file(*, repo_root: Path | None = None) -> Path | None:
    root = repo_root or _repo_root()
    for candidate in _candidate_env_files(root):
        if not candidate.exists():
            continue
        _load_key_value_env_file(candidate)
        os.environ.setdefault("AUTOBOTT_ENV_FILE_RESOLVED", str(candidate))
        return candidate
    return None


def configure_local_paper_runtime_defaults(*, repo_root: Path | None = None) -> None:
    root = repo_root or _repo_root()
    os.environ.setdefault("ALPACA_ENV", "paper")
    os.environ.setdefault("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets")
    os.environ.setdefault("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")
    os.environ.setdefault("AUTOBOTT_PAPER_ONLY", "true")
    os.environ.setdefault("AUTOBOTT_LIVE_TRADING_ENABLED", "false")
    os.environ.setdefault("AUTOBOTT_ALLOW_ORDER_PLACEMENT", "true")
    os.environ.setdefault("AUTOBOTT_PAPER_TRADE_ALL_PASSED_SIGNALS", "true")
    os.environ.setdefault("AUTOBOTT_PAPER_MAX_NEW_ENTRY_ATTEMPTS_PER_LOOP", "25")
    os.environ.setdefault("AUTOBOTT_PAPER_MAX_OPEN_ENTRY_BUY_ORDERS", "25")
    os.environ.setdefault("AUTOBOTT_SESSION_AUTOSTART", "true")
    os.environ.setdefault("AUTOBOTT_SESSION_SYMBOLS", "SPY")
    os.environ.setdefault("AUTOBOTT_SESSION_INTERVAL_SECONDS", "300")
    os.environ.setdefault("AUTOBOTT_SESSION_START_TIME", "09:35")
    os.environ.setdefault("AUTOBOTT_SESSION_END_TIME", "15:55")
    os.environ.setdefault("AUTOBOTT_SESSION_MARKET_TIMEZONE", "America/New_York")
    os.environ.setdefault("AUTOBOTT_SESSION_ARM_PAPER_EXECUTION", "true")
    os.environ.setdefault("AUTOBOTT_DASHBOARD_AUTH_TOKEN", "autobott-local")
    # Hosted env files (downloaded from the Render dashboard) carry Render's
    # disk-mounted /var/data paths. Local runs must always use the repo-relative
    # roots regardless of what the loaded env file says, so these are forced
    # rather than defaulted.
    os.environ["AUTOBOTT_DATA_ROOT"] = str(root / "data")
    os.environ["AUTOBOTT_ARTIFACTS_ROOT"] = str(root / "artifacts")
    os.environ["AUTOBOTT_GATE_PATH"] = str(root / "data" / "PHASE1_CYCLE_GATE.json")
    os.environ.setdefault("HOST", "127.0.0.1")
    os.environ.setdefault("PORT", "8000")
    os.environ.pop("ALPACA_LIVE_API_KEY", None)
    os.environ.pop("ALPACA_SECRET_KEY", None)


def _candidate_env_files(repo_root: Path) -> list[Path]:
    configured = os.getenv("AUTOBOTT_ENV_FILE")
    candidates: list[Path] = []
    if configured:
        candidates.append(Path(configured).expanduser())
    candidates.extend(
        [
            repo_root / "local.env",
            repo_root / ".env",
            repo_root / "AutoBott.env",
            Path.home() / "Downloads" / "AutoBott.env",
        ]
    )
    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        resolved_key = str(candidate)
        if resolved_key in seen:
            continue
        seen.add(resolved_key)
        deduped.append(candidate)
    return deduped


def _load_key_value_env_file(path: Path) -> None:
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        key = name.strip()
        if not key:
            continue
        os.environ.setdefault(key, _strip_optional_quotes(value.strip()))


def _strip_optional_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]
