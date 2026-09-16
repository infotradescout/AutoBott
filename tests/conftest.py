"""Read-only failure evidence for the synthetic cycle regression suite."""
from dataclasses import asdict, is_dataclass
import json
import pytest


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if not report.failed or item.path.name != "test_trading_cycle.py" or call.excinfo is None:
        return
    evidence = []
    for entry in call.excinfo.traceback:
        for name, value in entry.frame.f_locals.items():
            if name in {"result", "first", "second"} and is_dataclass(value) and hasattr(value, "decisions"):
                evidence.append({"name": name, "result": asdict(value)})
    if evidence:
        report.sections.append(("SYNTHETIC_CYCLE_FAILURE", json.dumps(evidence, default=str, sort_keys=True)))
