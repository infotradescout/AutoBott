from __future__ import annotations

import ast
import os
from pathlib import Path


def _load_render_helper():
    path = Path(__file__).resolve().parent.parent / "render_service_dashboard_v2.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    helper = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_is_render_starter_runtime"
    )
    module = ast.Module(body=[helper], type_ignores=[])
    ast.fix_missing_locations(module)
    namespace = {"os": os}
    exec(compile(module, str(path), "exec"), namespace)
    return namespace["_is_render_starter_runtime"]


def test_render_starter_runtime_detects_render_web(monkeypatch):
    monkeypatch.setenv("RENDER", "true")
    monkeypatch.setenv("RENDER_SERVICE_TYPE", "web")

    assert _load_render_helper()() is True


def test_render_starter_runtime_ignores_local(monkeypatch):
    monkeypatch.delenv("RENDER", raising=False)
    monkeypatch.delenv("RENDER_SERVICE_TYPE", raising=False)

    assert _load_render_helper()() is False
