from copy import deepcopy
import pytest
from autobott_v2 import accounting_recovery as recovery
from autobott_v2 import session_supervisor as supervisor


def setup_state(monkeypatch):
    monkeypatch.setattr(supervisor, '_SESSION_STATE', supervisor.SessionSupervisorState())


def test_recovery_default_does_not_run(monkeypatch):
    setup_state(monkeypatch)
    monkeypatch.delenv('AUTOBOTT_ACCOUNTING_RECOVERY_MODE', raising=False)
    def forbidden(_):
        raise AssertionError('recovery must be opt in')
    monkeypatch.setattr(recovery, 'maybe_recover_blocked_cycle', forbidden)
    cycle = {'execution_rejected_count_by_reason': {'daily_pnl_unavailable': 1}}
    supervisor._record_cycle_result(cycle)
    assert supervisor._SESSION_STATE.cycles_completed == 1
    assert 'accounting_recovery' not in supervisor._SESSION_STATE.last_result


@pytest.mark.parametrize('mode', ['plan', 'apply'])
def test_callback_keeps_rejection_and_order_results_unchanged(monkeypatch, capsys, mode):
    setup_state(monkeypatch)
    monkeypatch.setenv('AUTOBOTT_ACCOUNTING_RECOVERY_MODE', mode)
    monkeypatch.setattr(recovery, 'maybe_recover_blocked_cycle', lambda _: {'status': 'prepared', 'broker_writes': 0})
    cycle = {'orders_submitted': [], 'execution_rejected_count_by_reason': {'daily_pnl_unavailable': 1}}
    before = deepcopy(cycle)
    supervisor._record_cycle_result(cycle)
    assert cycle == before
    assert supervisor._SESSION_STATE.last_result['cycle_results'] == [before]
    assert supervisor._SESSION_STATE.last_result['accounting_recovery']['broker_writes'] == 0
    assert 'AUTOBOTT_ACCOUNTING_RECOVERY' in capsys.readouterr().out


def test_callback_failure_does_not_interrupt_session_or_expose_exception_text(monkeypatch, capsys):
    setup_state(monkeypatch)
    monkeypatch.setenv('AUTOBOTT_ACCOUNTING_RECOVERY_MODE', 'plan')
    def fail(_):
        raise ValueError('PRIVATE-CREDENTIAL-TEXT')
    monkeypatch.setattr(recovery, 'maybe_recover_blocked_cycle', fail)
    supervisor._record_cycle_result({'orders_submitted': []})
    assert supervisor._SESSION_STATE.cycles_completed == 1
    log = capsys.readouterr().out
    assert 'recovery_hook_failed' in log
    assert 'PRIVATE-CREDENTIAL-TEXT' not in log
