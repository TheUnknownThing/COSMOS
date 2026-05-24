import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "benchmarks" / "scripts"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


measure_latency = load_module("measure_latency", SCRIPTS_DIR / "measure_latency.py")
compare = load_module("compare", SCRIPTS_DIR / "compare.py")
harness = load_module("harness", SCRIPTS_DIR / "harness.py")


class Phase6HarnessTests(unittest.TestCase):
    def test_workload_registry_covers_sebs_inspired_shapes(self) -> None:
        self.assertIn("memory_heavy", harness.WORKLOADS)
        self.assertIn("network_heavy", harness.WORKLOADS)
        self.assertIn("compression_mixed", harness.WORKLOADS)
        self.assertIn("graph_bfs", harness.WORKLOADS)
        self.assertIn("120.uploader", harness.WORKLOADS["network_heavy"].inspired_by)
        self.assertIn("503.graph-bfs", harness.WORKLOADS["graph_bfs"].inspired_by)

    def test_timestamped_run_id_is_fine_grained(self) -> None:
        first = harness.timestamped_run_id()
        second = harness.timestamped_run_id()
        self.assertNotEqual(first, second)

    def test_summarize_run_captures_latency_and_scheduler_stats(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            (run_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "config": "cosmos-full",
                        "workload": "cpu_burst",
                        "concurrency": 3,
                        "duration_ms": 10,
                        "deadline_us": 10000,
                        "metadata_mode": "metadata-full",
                        "scheduler_flags": ["--slo-target-us", "10000"],
                    }
                ),
                encoding="utf-8",
            )
            with (run_dir / "client_latency.csv").open("w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(
                    fh,
                    fieldnames=[
                        "invocation_id",
                        "status",
                        "exit_code",
                        "start_monotonic_ns",
                        "end_monotonic_ns",
                        "duration_ms",
                        "deadline_us",
                        "workload",
                        "config",
                        "stderr_path",
                    ],
                )
                writer.writeheader()
                writer.writerows(
                    [
                        {
                            "invocation_id": 1,
                            "status": "ok",
                            "exit_code": 0,
                            "start_monotonic_ns": 0,
                            "end_monotonic_ns": 5_000_000,
                            "duration_ms": 5.0,
                            "deadline_us": 10000,
                            "workload": "cpu_burst",
                            "config": "cosmos-full",
                            "stderr_path": "/tmp/1.stderr",
                        },
                        {
                            "invocation_id": 2,
                            "status": "ok",
                            "exit_code": 0,
                            "start_monotonic_ns": 0,
                            "end_monotonic_ns": 12_000_000,
                            "duration_ms": 12.0,
                            "deadline_us": 10000,
                            "workload": "cpu_burst",
                            "config": "cosmos-full",
                            "stderr_path": "/tmp/2.stderr",
                        },
                        {
                            "invocation_id": 3,
                            "status": "failed",
                            "exit_code": 1,
                            "start_monotonic_ns": 0,
                            "end_monotonic_ns": 8_000_000,
                            "duration_ms": 8.0,
                            "deadline_us": 10000,
                            "workload": "cpu_burst",
                            "config": "cosmos-full",
                            "stderr_path": "/tmp/3.stderr",
                        },
                    ]
                )

            (run_dir / "scheduler_stats.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps({"ts_monotonic_ns": 1, "stats": {"nr_queued": 1, "nr_scheduled": 2}}),
                        json.dumps(
                            {
                                "ts_monotonic_ns": 2,
                                "stats": {
                                    "nr_queued": 3,
                                    "nr_scheduled": 4,
                                    "nr_slo_violations": 2,
                                },
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            summary = measure_latency.summarize_run(run_dir)
            self.assertEqual(summary["latency"]["count"], 3)
            self.assertEqual(summary["latency"]["client_slo_violations"], 1)
            self.assertEqual(summary["scheduler"]["peak"]["nr_queued"], 3)
            self.assertEqual(summary["scheduler"]["last"]["nr_slo_violations"], 2)

    def test_compare_resolves_latest_summary_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline_dir = root / "cfs-default" / "20260524T000000Z"
            baseline_dir.mkdir(parents=True)
            (baseline_dir / "summary.json").write_text(
                json.dumps({"config": "cfs-default", "workload": "cpu_burst", "concurrency": 1, "latency": {"p50_ms": 1, "p95_ms": 1, "p99_ms": 1, "mean_ms": 1, "client_slo_violations": 0}, "scheduler": {"last": {}}}),
                encoding="utf-8",
            )
            latest = root / "cfs-default" / "latest"
            latest.symlink_to("20260524T000000Z")

            resolved = compare.resolve_summary_path(root / "cfs-default")
            self.assertEqual(resolved, baseline_dir / "summary.json")


if __name__ == "__main__":
    unittest.main()
