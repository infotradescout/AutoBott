"""Temporary proof-only attribution of calls already forbidden by offline validation."""
import socket
import traceback
import pytest

@pytest.fixture(autouse=True)
def identify_unmocked_dns(request, monkeypatch):
    original = socket.getaddrinfo
    calls = []
    def traced(*args, **kwargs):
        calls.append(''.join(traceback.format_stack(limit=10)))
        return original(*args, **kwargs)
    monkeypatch.setattr(socket, 'getaddrinfo', traced)
    yield
    if calls:
        pytest.fail('offline DNS attempt in ' + request.node.nodeid + '\n' + calls[0], pytrace=False)
