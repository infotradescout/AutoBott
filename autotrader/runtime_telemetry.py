"""Lightweight runtime telemetry for hosted smoke audits."""

from __future__ import annotations

import ctypes
import os
import platform
import threading
import time
from typing import Any

_START_EPOCH = time.time()
_LOCK = threading.Lock()
_STATE: dict[str, Any] = {
    "enabled_workers": {},
    "last_loop_name": "",
    "last_candidate_symbol": "",
    "last_api_path": "",
}


def _rss_mb_proc() -> float | None:
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        with open("/proc/self/statm", "r", encoding="utf-8") as handle:
            parts = handle.read().strip().split()
        if len(parts) < 2:
            return None
        return (int(parts[1]) * page_size) / (1024.0 * 1024.0)
    except Exception:
        return None


def _rss_mb_windows() -> float | None:
    if platform.system().lower() != "windows":
        return None
    try:
        class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("cb", ctypes.c_ulong),
                ("PageFaultCount", ctypes.c_ulong),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = PROCESS_MEMORY_COUNTERS()
        counters.cb = ctypes.sizeof(counters)
        handle = ctypes.windll.kernel32.GetCurrentProcess()
        ok = ctypes.windll.psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb)
        if not ok:
            return None
        return float(counters.WorkingSetSize) / (1024.0 * 1024.0)
    except Exception:
        return None


def rss_mb() -> float | None:
    value = _rss_mb_proc()
    if value is not None:
        return round(value, 2)
    value = _rss_mb_windows()
    if value is not None:
        return round(value, 2)
    return None


def set_worker(name: str, enabled: bool, *, detail: str = "") -> None:
    label = str(name or "").strip()
    if not label:
        return
    with _LOCK:
        _STATE["enabled_workers"][label] = {
            "enabled": bool(enabled),
            "detail": str(detail or ""),
            "updated_epoch": round(time.time(), 3),
        }


def set_last_loop(name: str) -> None:
    with _LOCK:
        _STATE["last_loop_name"] = str(name or "")[:120]


def set_last_candidate(symbol: str) -> None:
    with _LOCK:
        _STATE["last_candidate_symbol"] = str(symbol or "")[:80]


def set_last_api_path(path: str) -> None:
    with _LOCK:
        _STATE["last_api_path"] = str(path or "")[:200]


def snapshot() -> dict[str, Any]:
    with _LOCK:
        state = dict(_STATE)
        state["enabled_workers"] = dict(_STATE.get("enabled_workers", {}))
    threads = threading.enumerate()
    return {
        "rss_mb": rss_mb(),
        "uptime_seconds": round(max(0.0, time.time() - _START_EPOCH), 2),
        "active_thread_count": len(threads),
        "active_thread_names": [thread.name for thread in threads[:50]],
        "enabled_workers": state.get("enabled_workers", {}),
        "last_loop_name": state.get("last_loop_name", ""),
        "last_candidate_symbol": state.get("last_candidate_symbol", ""),
        "last_api_path": state.get("last_api_path", ""),
    }
