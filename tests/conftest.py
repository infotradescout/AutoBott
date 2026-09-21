"""Keep synthetic test workers inside the lifetime of their mocked providers."""
import threading
import pytest


@pytest.fixture(autouse=True)
def join_test_owned_autobott_workers(monkeypatch):
    original_start = threading.Thread.start
    workers = []

    def tracked_start(thread):
        if thread.name in {'autobott-session', 'autobott-position-monitor'}:
            args = getattr(thread, '_args', ())
            stop_event = args[1] if len(args) > 1 and isinstance(args[1], threading.Event) else None
            workers.append((thread, stop_event))
        return original_start(thread)

    monkeypatch.setattr(threading.Thread, 'start', tracked_start)
    yield
    # This fixture depends on monkeypatch, so providers remain mocked until
    # every worker it observed has finished, including deliberately orphaned
    # references in the consumed-autostart/monitor-replacement regression.
    for _, event in workers:
        if event is not None:
            event.set()
    for thread, _ in workers:
        if thread.ident is not None:
            thread.join(timeout=2)
        assert not thread.is_alive(), 'test-owned AutoBott worker outlived its provider fixtures'
