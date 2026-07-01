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
    assert state.execution_enabled is True
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
