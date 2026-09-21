"""Proof-only context recording; delegates every event to the unchanged guard."""
import os
import sys
import traceback

contexts = []
runner = sys.modules['__main__']
original = runner.audit_event

def contextual_audit(event, args, violations):
    if event in runner.NETWORK_EVENTS:
        contexts.append({'event': event, 'test': os.getenv('PYTEST_CURRENT_TEST'), 'stack': ''.join(traceback.format_stack(limit=16))})
    return original(event, args, violations)

runner.audit_event = contextual_audit

def pytest_sessionfinish(session, exitstatus):
    reporter = session.config.pluginmanager.getplugin('terminalreporter')
    for item in contexts:
        reporter.write_line('DENIED_OPERATION_CONTEXT ' + str(item))
