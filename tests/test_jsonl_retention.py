from __future__ import annotations

import json

from autobott_v2.jsonl_retention import compact_jsonl_tail, read_jsonl_tail


def test_compact_jsonl_tail_keeps_only_complete_recent_rows(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    rows = [{"sequence": index, "payload": "x" * 20} for index in range(20)]
    path.write_text("".join(f"{json.dumps(row)}\n" for row in rows), encoding="utf-8")

    changed = compact_jsonl_tail(path, max_bytes=300, retain_bytes=200)
    retained = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]

    assert changed is True
    assert retained
    assert retained[-1] == rows[-1]
    assert retained[0]["sequence"] > 0


def test_read_jsonl_tail_discards_partial_leading_row(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text("".join(f'{{"sequence": {index}}}\n' for index in range(20)), encoding="utf-8")

    rows = [json.loads(line) for line in read_jsonl_tail(path, max_tail_bytes=80)]

    assert rows[-1] == {"sequence": 19}
    assert rows[0]["sequence"] > 0
