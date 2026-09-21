"""Proof-only context recording; delegates every event to the unchanged guard."""
import os
import sys
import threading
import traceback

contexts = []
runner = sys.modules['__main__']
original = runner.audit_event

def contextual_audit(event, args, violations):
    if event in runner.NETWORK_EVENTS:
        frames = [f'{frame.filename}:{frame.lineno}:{frame.name}' for frame in traceback.extract_stack() if '/project/src/' in frame.filename]
        contexts.append({'event': event, 'test': os.getenv('PYTEST_CURRENT_TEST'), 'thread': threading.current_thread().name, 'frames': frames})
    return original(event, args, violations)

runner.audit_event = contextual_audit

def pytest_sessionfinish(session, exitstatus):
    reporter = session.config.pluginmanager.getplugin('terminalreporter')
    for item in contexts:
        reporter.write_line('DENIED_OPERATION_CONTEXT ' + str(item))
