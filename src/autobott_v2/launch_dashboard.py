from __future__ import annotations

from .dashboard_app import main as dashboard_main
from .env_bootstrap import bootstrap_env_file, configure_local_paper_runtime_defaults


def main() -> int:
    bootstrap_env_file()
    configure_local_paper_runtime_defaults()
    return dashboard_main()


if __name__ == "__main__":
    raise SystemExit(main())
