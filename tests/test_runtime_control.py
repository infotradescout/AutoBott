import json

import pytest

from autobott_v2.runtime_control import (
    arm_paper_execution,
    default_runtime_state,
    disable_execution,
    load_runtime_state,
    save_runtime_state,
    set_execution_mode,
    set_kill_switch,
)


def test_runtime_control_defaults_are_safe() -> None:
    state = default_runtime_state()
    assert state.kill_switch_enabled is False
    assert state.execution_enabled is False
    assert state.live_mode_enabled is False


def test_set_kill_switch_disables_execution_and_live(tmp_path) -> None:
    path = tmp_path / "runtime_state.json"
    save_runtime_state(default_runtime_state(), state_path=path)
    state = set_kill_switch(True, reason="manual_stop", state_path=path)
    assert state.kill_switch_enabled is True
    assert state.execution_enabled is False
    assert state.live_mode_enabled is False
    assert load_runtime_state(state_path=path).kill_switch_enabled is True


def test_set_execution_mode_respects_kill_switch(tmp_path) -> None:
    path = tmp_path / "runtime_state.json"
    set_kill_switch(True, reason="manual_stop", state_path=path)
    state = set_execution_mode(execution_enabled=True, live_mode_enabled=True, reason="attempt_resume", state_path=path)
    assert state.execution_enabled is False
    assert state.live_mode_enabled is False


def test_arm_paper_execution_clears_kill_switch_and_enables_paper(tmp_path) -> None:
    path = tmp_path / "runtime_state.json"
    set_kill_switch(True, reason="manual_stop", state_path=path)
    state = arm_paper_execution(reason="resume_paper", state_path=path)
    assert state.kill_switch_enabled is False
    assert state.execution_enabled is True
    assert state.live_mode_enabled is False


def test_disable_execution_turns_off_entries_without_enabling_live(tmp_path) -> None:
    path = tmp_path / "runtime_state.json"
    save_runtime_state(default_runtime_state(), state_path=path)
    state = disable_execution(reason="pause_entries", state_path=path)
    assert state.execution_enabled is False
    assert state.live_mode_enabled is False


def test_missing_runtime_state_starts_paused(tmp_path) -> None:
    state = load_runtime_state(state_path=tmp_path / "missing.json")
    assert state.execution_enabled is False
    assert state.live_mode_enabled is False


def test_missing_execution_field_starts_paused(tmp_path) -> None:
    import json
    path = tmp_path / "runtime_state.json"
    payload = default_runtime_state().to_json_dict()
    del payload["execution_enabled"]
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert load_runtime_state(state_path=path).execution_enabled is False


@pytest.mark.parametrize("field", ["kill_switch_enabled", "execution_enabled", "live_mode_enabled"])
@pytest.mark.parametrize("value", ["false", "true", 0, 1, None, "missing"])
def test_invalid_safety_field_cannot_arm_and_preserves_file(tmp_path, field, value) -> None:
    path = tmp_path / "runtime_state.json"
    payload = default_runtime_state().to_json_dict()
    payload["execution_enabled"] = True
    if value == "missing":
        del payload[field]
    else:
        payload[field] = value
    original = json.dumps(payload)
    path.write_text(original, encoding="utf-8")
    state = load_runtime_state(state_path=path)
    assert state.kill_switch_enabled is True
    assert state.execution_enabled is False
    assert state.live_mode_enabled is False
    assert state.reason == "invalid_runtime_state"
    assert path.read_text(encoding="utf-8") == original


@pytest.mark.parametrize("content", ["{", "[]", "null"])
def test_malformed_runtime_file_is_held(tmp_path, content) -> None:
    path = tmp_path / "runtime_state.json"
    path.write_text(content, encoding="utf-8")
    assert load_runtime_state(state_path=path).kill_switch_enabled is True
    assert load_runtime_state(state_path=path).execution_enabled is False


def test_persisted_kill_switch_overrides_inconsistent_arm(tmp_path) -> None:
    path = tmp_path / "runtime_state.json"
    payload = default_runtime_state().to_json_dict()
    payload.update(kill_switch_enabled=True, execution_enabled=True, live_mode_enabled=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    state = load_runtime_state(state_path=path)
    assert state.kill_switch_enabled is True
    assert state.execution_enabled is False
    assert state.live_mode_enabled is False
