"""Regression tests for replay farm aggregation."""

from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

import replay_farm  # noqa: E402


def _write_optimizer_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "candidate",
        "evaluated",
        "wins",
        "losses",
        "win_rate_pct",
        "expectancy_pct",
        "pass_target",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class ReplayFarmTests(unittest.TestCase):
    def test_load_worker_specs_file_supports_symbols_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            symbols_path = root / "symbols.txt"
            symbols_path.write_text("AAPL\nMSFT\nAAPL\n", encoding="utf-8")
            specs_path = root / "workers.json"
            specs_payload = [
                {
                    "name": "dataset_alpha",
                    "symbols_file": str(symbols_path.name),
                    "start": "2026-04-01",
                    "end": "2026-04-20",
                    "interval": "1m",
                    "window_days": 4,
                    "step_days": 2,
                    "scan_bars": 15,
                }
            ]
            specs_path.write_text(json.dumps(specs_payload), encoding="utf-8")

            specs = replay_farm._load_worker_specs(specs_path)

            self.assertIn("dataset_alpha", specs)
            self.assertEqual(specs["dataset_alpha"].symbols, ("AAPL", "MSFT"))
            self.assertEqual(specs["dataset_alpha"].scan_bars, 15)
            self.assertEqual(specs["dataset_alpha"].interval, "1m")

    def test_load_worker_specs_file_supports_rolling_settings(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            specs_path = root / "workers.json"
            specs_payload = {
                "workers": [
                    {
                        "name": "dataset_rolling",
                        "symbols": ["AAPL", "MSFT"],
                        "start": "2026-05-01",
                        "end": "2026-05-12",
                        "window_days": 3,
                        "step_days": 3,
                        "rolling": True,
                        "rolling_step_days": 2,
                        "rolling_end_policy": "cache",
                    }
                ]
            }
            specs_path.write_text(json.dumps(specs_payload), encoding="utf-8")

            specs = replay_farm._load_worker_specs(specs_path)

            self.assertIn("dataset_rolling", specs)
            self.assertTrue(specs["dataset_rolling"].rolling)
            self.assertEqual(specs["dataset_rolling"].rolling_step_days, 2)
            self.assertEqual(specs["dataset_rolling"].rolling_end_policy, "cache")
            self.assertEqual(specs["dataset_rolling"].min_win_loss_ratio, 1.25)

    def test_load_worker_specs_file_supports_min_win_loss_ratio(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            specs_path = root / "workers.json"
            specs_payload = [
                {
                    "name": "ratio_guard",
                    "symbols": ["AAPL", "MSFT"],
                    "start": "2026-05-01",
                    "end": "2026-05-12",
                    "min_win_loss_ratio": 1.4,
                }
            ]
            specs_path.write_text(json.dumps(specs_payload), encoding="utf-8")

            specs = replay_farm._load_worker_specs(specs_path)

            self.assertIn("ratio_guard", specs)
            self.assertEqual(specs["ratio_guard"].min_win_loss_ratio, 1.4)

    def test_aggregate_requires_cross_worker_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            worker_a = root / "indexes_recent"
            worker_b = root / "mega_cap_recent"
            replay_farm._write_registry(
                root,
                {
                    "workers": {
                        "indexes_recent": {"pid": 0, "output_dir": str(worker_a)},
                        "mega_cap_recent": {"pid": 0, "output_dir": str(worker_b)},
                    }
                },
            )
            _write_optimizer_rows(
                worker_a / "optimizer_runs.csv",
                [
                    {
                        "candidate": "edge_candidate",
                        "evaluated": 80,
                        "wins": 52,
                        "losses": 28,
                        "win_rate_pct": 65.0,
                        "expectancy_pct": 0.11,
                        "pass_target": True,
                    },
                    {
                        "candidate": "one_dataset_only",
                        "evaluated": 80,
                        "wins": 56,
                        "losses": 24,
                        "win_rate_pct": 70.0,
                        "expectancy_pct": 0.14,
                        "pass_target": True,
                    },
                ],
            )
            _write_optimizer_rows(
                worker_b / "optimizer_runs.csv",
                [
                    {
                        "candidate": "edge_candidate",
                        "evaluated": 90,
                        "wins": 58,
                        "losses": 32,
                        "win_rate_pct": 64.44,
                        "expectancy_pct": 0.10,
                        "pass_target": True,
                    },
                    {
                        "candidate": "one_dataset_only",
                        "evaluated": 10,
                        "wins": 4,
                        "losses": 6,
                        "win_rate_pct": 40.0,
                        "expectancy_pct": -0.02,
                        "pass_target": False,
                    },
                ],
            )

            result = replay_farm.aggregate_farm(
                output_root=root,
                min_total_trades=100,
                min_workers=2,
                min_passing_workers=2,
                min_passing_window_pct=40.0,
                target_win_rate_pct=55.0,
                target_expectancy_pct=0.05,
                min_win_loss_ratio=1.25,
                min_worker_win_loss_ratio=1.15,
            )
            farm_runs_exists = Path(result["farm_runs_csv"]).exists()

        self.assertEqual(result["best"]["candidate"], "edge_candidate")
        self.assertTrue(result["best"]["promotable"])
        self.assertEqual(result["top_3"][0]["candidate"], "edge_candidate")
        by_candidate = {item["candidate"]: item for item in result["leaderboard"]}
        self.assertFalse(by_candidate["one_dataset_only"]["promotable"])
        self.assertTrue(farm_runs_exists)

    def test_aggregate_respects_global_win_loss_ratio_threshold(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            worker_a = root / "dataset_a"
            worker_b = root / "dataset_b"
            replay_farm._write_registry(
                root,
                {
                    "workers": {
                        "dataset_a": {"pid": 0, "output_dir": str(worker_a)},
                        "dataset_b": {"pid": 0, "output_dir": str(worker_b)},
                    }
                },
            )
            _write_optimizer_rows(
                worker_a / "optimizer_runs.csv",
                [
                    {
                        "candidate": "ratio_limited",
                        "evaluated": 60,
                        "wins": 34,
                        "losses": 26,
                        "win_rate_pct": 56.67,
                        "expectancy_pct": 0.09,
                        "pass_target": True,
                    }
                ],
            )
            _write_optimizer_rows(
                worker_b / "optimizer_runs.csv",
                [
                    {
                        "candidate": "ratio_limited",
                        "evaluated": 60,
                        "wins": 34,
                        "losses": 26,
                        "win_rate_pct": 56.67,
                        "expectancy_pct": 0.09,
                        "pass_target": True,
                    }
                ],
            )

            result = replay_farm.aggregate_farm(
                output_root=root,
                min_total_trades=100,
                min_workers=2,
                min_passing_workers=2,
                min_passing_window_pct=40.0,
                target_win_rate_pct=55.0,
                target_expectancy_pct=0.05,
                min_win_loss_ratio=1.35,
                min_worker_win_loss_ratio=1.15,
            )

        self.assertFalse(result["best"]["promotable"])
        self.assertEqual(result["best"]["candidate"], "ratio_limited")
        self.assertAlmostEqual(float(result["best"]["win_loss_ratio"]), 1.3077, places=4)
        self.assertEqual(len(result["top_3"]), 1)

    def test_optimizer_command_passes_offline_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = replay_farm.FarmWorkerSpec(
                name="unit_worker",
                symbols=("AAPL",),
                start="2026-04-01",
                end="2026-05-01",
            )
            offline_command = replay_farm._optimizer_command(
                python_exe=Path("python"),
                spec=spec,
                output_dir=root / "out_off",
                cache_dir=root / "cache",
                offline=True,
            )
            self.assertIn("--offline", offline_command)
            self.assertNotIn("--no-offline", offline_command)
            ratio_index = offline_command.index("--min-win-loss-ratio")
            self.assertEqual(offline_command[ratio_index + 1], str(spec.min_win_loss_ratio))

            online_command = replay_farm._optimizer_command(
                python_exe=Path("python"),
                spec=spec,
                output_dir=root / "out_on",
                cache_dir=root / "cache",
                offline=False,
            )
            self.assertIn("--no-offline", online_command)
            self.assertNotIn("--offline", online_command)

    def test_stop_workers_supports_custom_worker_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output_dir = root / "custom_worker"
            replay_farm._write_registry(
                root,
                {
                    "workers": {
                        "custom_worker": {"pid": 0, "output_dir": str(output_dir)},
                    }
                },
            )
            specs = {
                "custom_worker": replay_farm.FarmWorkerSpec(
                    name="custom_worker",
                    symbols=("AAPL",),
                    start="2026-04-01",
                    end="2026-05-01",
                )
            }
            result = replay_farm.stop_workers(
                worker_names="custom_worker",
                output_root=root,
                worker_specs=specs,
            )
            self.assertEqual(result["not_running"][0]["worker"], "custom_worker")
            self.assertFalse(result["stopped"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
