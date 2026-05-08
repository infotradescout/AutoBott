"""Regression tests for replay-promotion helper logic."""

from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

import replay_promotion  # noqa: E402


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "worker",
        "candidate",
        "run_timestamp",
        "iteration",
        "window_start",
        "window_end",
        "overrides_json",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class ReplayPromotionTests(unittest.TestCase):
    def test_select_candidate_overrides_prefers_latest_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            farm_runs = Path(tmp) / "farm_runs.csv"
            _write_rows(
                farm_runs,
                [
                    {
                        "worker": "worker_a",
                        "candidate": "quality_score",
                        "run_timestamp": "20260508_090000",
                        "iteration": 10,
                        "window_start": "2026-04-01",
                        "window_end": "2026-05-01",
                        "overrides_json": '{"MIN_SIGNAL_SCORE": 8.1, "IGNORED": 123}',
                    },
                    {
                        "worker": "worker_a",
                        "candidate": "quality_score",
                        "run_timestamp": "20260508_091000",
                        "iteration": 11,
                        "window_start": "2026-04-02",
                        "window_end": "2026-05-02",
                        "overrides_json": '{"MIN_SIGNAL_SCORE": 8.4, "RVOL_MIN": 1.05}',
                    },
                ],
            )

            overrides, source_row = replay_promotion.select_candidate_overrides(
                farm_runs_csv=farm_runs,
                candidate="quality_score",
                worker_names={"worker_a"},
                allowed_keys=("MIN_SIGNAL_SCORE", "RVOL_MIN"),
            )

        self.assertEqual(overrides, {"MIN_SIGNAL_SCORE": 8.4, "RVOL_MIN": 1.05})
        self.assertEqual(source_row["iteration"], "11")

    def test_build_promotion_snapshot_filters_workers(self):
        with tempfile.TemporaryDirectory() as tmp:
            farm_runs = Path(tmp) / "farm_runs.csv"
            _write_rows(
                farm_runs,
                [
                    {
                        "worker": "worker_a",
                        "candidate": "direction_strict",
                        "run_timestamp": "20260508_090000",
                        "iteration": 12,
                        "window_start": "2026-04-01",
                        "window_end": "2026-05-01",
                        "overrides_json": '{"DIRECTION_CONVICTION_MIN": 0.75}',
                    },
                    {
                        "worker": "worker_b",
                        "candidate": "direction_strict",
                        "run_timestamp": "20260508_090500",
                        "iteration": 13,
                        "window_start": "2026-04-01",
                        "window_end": "2026-05-01",
                        "overrides_json": '{"DIRECTION_CONVICTION_MIN": 0.90}',
                    },
                ],
            )
            aggregate_payload = {
                "generated_at": "2026-05-08T09:30:00",
                "farm_runs_csv": str(farm_runs),
                "best": {"candidate": "direction_strict", "promotable": True},
            }

            snapshot = replay_promotion.build_promotion_snapshot(
                aggregate_payload=aggregate_payload,
                worker_names={"worker_a"},
                allowed_override_keys=("DIRECTION_CONVICTION_MIN",),
            )

        self.assertTrue(snapshot["promotable"])
        self.assertEqual(snapshot["candidate"], "direction_strict")
        self.assertEqual(snapshot["overrides"], {"DIRECTION_CONVICTION_MIN": 0.75})
        self.assertEqual(snapshot["override_source"]["worker"], "worker_a")

    def test_build_promotion_snapshot_skips_non_promotable(self):
        snapshot = replay_promotion.build_promotion_snapshot(
            aggregate_payload={
                "generated_at": "2026-05-08T09:30:00",
                "farm_runs_csv": "missing.csv",
                "best": {"candidate": "baseline", "promotable": False},
            },
            worker_names={"worker_a"},
            allowed_override_keys=("MIN_SIGNAL_SCORE",),
        )
        self.assertFalse(snapshot["promotable"])
        self.assertEqual(snapshot["overrides"], {})


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
