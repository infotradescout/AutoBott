"""Explicit source, process and optional public-source restoration checks."""
from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha1, sha256
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from zoneinfo import ZoneInfo


PROTECTED = {
    "execution_broker.py": "2592084287f06476aff654aed1d2f91fa8a6b1d9",
    "execution_config.py": "945d2c64a16307acfec96c2dafb41739b45d4012",
    "portfolio_budget.py": "4a78c8892de174dd85e20c058f1ccb90ccc63b7b",
    "strategy_policy.py": "268687abd8261d75caf01807c08152eb935f0c3b",
    "execution_journal.py": "e27b091b18e36778b29ccd643402fc78725d0e45",
    "jsonl_retention.py": "9a308454f3d17fd1e27843680b5a7d681051154d",
}
REQUIRED = ("bar_timing", "entry_admission", "entry_quality", "entry_fomc_context",
    "entry_market_context", "entry_schedule_context", "entry_sector_context",
    "live_entry_thesis", "primary_development_metrics", "primary_entry_study",
    "primary_fill_capture", "primary_fill_linkage", "primary_followthrough",
    "primary_quality_runtime", "primary_runtime_evidence", "quote_observation", "signal_reference")
PUBLIC_URLS: set[str] = set()


def audit(event, args):
    if event == "urllib.Request" and args[0] not in PUBLIC_URLS:
        raise RuntimeError("entry_restore_probe_url_not_allowed")
    if event.startswith("socket.") and not PUBLIC_URLS:
        raise RuntimeError("entry_restore_process_network_forbidden")


sys.addaudithook(audit)


def child_env():
    return {k: v for k, v in os.environ.items() if k in {
        "PATH", "HOME", "SYSTEMROOT", "TMPDIR", "TEMP", "TMP", "PYTHONPATH"}}


def process_proof():
    from autobott_v2.primary_followthrough import _locked
    from autobott_v2.observation_lock import observation_store_lock
    assert _locked is observation_store_lock
    with tempfile.TemporaryDirectory(prefix="entry-observer-lock-proof-") as directory:
        root = Path(directory)
        preserved = root / "preserved-evidence.txt"
        preserved.write_bytes(b"synthetic-evidence-preserve\n")
        owner = subprocess.Popen([sys.executable, __file__, "--hold", str(root)], env=child_env(),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            # A separate stdout wait is bounded by a helper thread so a failed
            # child cannot leave this proof waiting indefinitely for its marker.
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=1) as pool:
                marker = pool.submit(owner.stdout.readline)
                try:
                    assert marker.result(timeout=10).strip() == "LOCK_HELD"
                except BaseException:
                    owner.kill()
                    owner.wait(timeout=5)
                    raise
            lock_path = root / ".primary-observation.lock"
            inode = lock_path.stat().st_ino
            contender = subprocess.run([sys.executable, __file__, "--try", str(root)], env=child_env(),
                capture_output=True, text=True, timeout=10)
            assert contender.returncode == 23, contender.stderr[-500:]
            owner.kill()
            owner.wait(timeout=5)
            recovered = subprocess.run([sys.executable, __file__, "--try", str(root)], env=child_env(),
                capture_output=True, text=True, timeout=10)
            assert recovered.returncode == 0, recovered.stderr[-500:]
            assert recovered.stdout.strip() == "LOCK_ACQUIRED"
            assert lock_path.stat().st_ino == inode
            assert preserved.read_bytes() == b"synthetic-evidence-preserve\n"
        finally:
            if owner.poll() is None:
                owner.kill()
                owner.wait(timeout=5)
    return {"active_owner_excluded_competitor": True, "killed_owner_recovered": True,
        "same_lock_inode_retained": True, "evidence_bytes_preserved": True,
        "platform": sys.platform, "broker_requests": 0}


def public_probes():
    from autobott_v2 import entry_fomc_context as fomc
    from autobott_v2 import entry_schedule_context as bls
    from autobott_v2 import entry_sector_context as sector
    day = datetime.now(UTC).astimezone(ZoneInfo("America/New_York")).date()
    PUBLIC_URLS.update({fomc.fomc_url(day), bls.BLS_URL, sector.HOLDINGS_URL})
    results = []
    probes = [
        ("bls", bls.BLS_URL, bls.fetch_bls_text, bls.parse_bls_calendar),
        ("fomc", fomc.fomc_url(day), lambda: fomc.fetch_fomc_text(day), lambda text: fomc.parse_fomc_calendar(text, day)),
        ("sector", sector.HOLDINGS_URL, sector.fetch_holdings_text, sector.parse_holdings),
    ]
    try:
        for name, url, fetch, parse in probes:
            started = time.monotonic()
            observed = datetime.now(UTC).isoformat()
            try:
                text = fetch()
                parsed = parse(text)
                count = len(parsed) if name == "bls" else len(parsed["events"] if name == "fomc" else parsed["records"])
                result = {"name": name, "url": url, "status": "parsed", "source_sha256": sha256(text.encode()).hexdigest(),
                    "records": count, "source_date": parsed.get("asof") if name == "sector" else None}
            except Exception as exc:
                # Missing public input is not evidence of an empty calendar.
                result = {"name": name, "url": url, "status": "unavailable", "error_type": type(exc).__name__}
            results.append({**result, "observed_at": observed, "elapsed_seconds": round(time.monotonic()-started, 3)})
    finally:
        PUBLIC_URLS.clear()
    return results


def main():
    if len(sys.argv) == 3 and sys.argv[1] in {"--hold", "--try"}:
        from autobott_v2.primary_followthrough import _locked
        try:
            with _locked(Path(sys.argv[2])):
                if sys.argv[1] == "--hold":
                    print("LOCK_HELD", flush=True)
                    sys.stdin.read(1)
                else:
                    print("LOCK_ACQUIRED", flush=True)
        except FileExistsError:
            return 23
        return 0
    if sys.argv[1:] not in ([], ["--public"]):
        raise ValueError("entry_restore_proof_arguments_invalid")
    root = Path(__file__).resolve().parents[1]
    module_root = root / "src" / "autobott_v2"
    hashes = {}
    for name, expected in PROTECTED.items():
        raw = (module_root / name).read_bytes()
        actual = sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
        assert actual == expected, "protected_runtime_changed:" + name
        hashes[name] = actual
    for name in REQUIRED:
        assert (module_root / (name + ".py")).is_file(), "entry_module_missing:" + name
    result = {"observed_at": datetime.now(UTC).isoformat(), "protected_blobs": hashes,
        "restored_required_modules": list(REQUIRED), "observation_process_proof": process_proof(),
        "production_modified": False, "entry_advantage_established": False}
    if sys.argv[1:] == ["--public"]:
        result["public_source_probes"] = public_probes()
        result["public_sources_ready"] = all(row["status"] == "parsed" for row in result["public_source_probes"])
        result["complete_entry_market_readiness"] = False
    print("AUTOBOTT_ENTRY_RESTORE_ACCEPTANCE " + json.dumps(result, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
