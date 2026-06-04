import csv
import importlib.util
import json
import socket
import sys
import tempfile
import threading
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
LOCAL_HARNESS_DIR = REPO_ROOT / "benchmarks" / "local_harness"
SCRIPTS_DIR = REPO_ROOT / "benchmarks" / "scripts"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


measure_latency = load_module("measure_latency", LOCAL_HARNESS_DIR / "measure_latency.py")
harness = load_module("harness", LOCAL_HARNESS_DIR / "harness.py")
orchestrate_benchmarks = load_module(
    "orchestrate_benchmarks", SCRIPTS_DIR / "orchestrate_benchmarks.py"
)


class Phase6HarnessTests(unittest.TestCase):
    def _serve_fake_stats_socket(
        self,
        socket_path: Path,
        ready: threading.Event,
        stop: threading.Event,
        connection_count: list[int],
        request_count: list[int],
    ) -> None:
        def serve_client(conn: socket.socket) -> None:
            request_index = 0
            raw = b""
            with conn:
                conn.settimeout(0.05)
                while not stop.is_set():
                    try:
                        chunk = conn.recv(65536)
                    except TimeoutError:
                        continue
                    if not chunk:
                        break
                    raw += chunk
                    while b"\n" in raw:
                        _line, raw = raw.split(b"\n", 1)
                        request_count.append(1)
                        response = {
                            "errno": 0,
                            "args": {
                                "resp": {
                                    "nr_user_dispatches": request_index,
                                    "nr_queued": request_index,
                                }
                            },
                        }
                        conn.sendall(json.dumps(response).encode("utf-8") + b"\n")
                        request_index += 1

        try:
            socket_path.unlink()
        except FileNotFoundError:
            pass
        workers: list[threading.Thread] = []
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(socket_path))
            server.listen()
            server.settimeout(0.05)
            ready.set()
            while not stop.is_set():
                try:
                    conn, _addr = server.accept()
                except TimeoutError:
                    continue
                connection_count.append(1)
                worker = threading.Thread(
                    target=serve_client, args=(conn,), daemon=True
                )
                worker.start()
                workers.append(worker)
        for worker in workers:
            worker.join(timeout=1)

    def test_capture_stats_uses_persistent_connection_after_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            socket_path = root / "stats.sock"
            output_path = root / "scheduler_stats.jsonl"
            ready = threading.Event()
            stop = threading.Event()
            connection_count: list[int] = []
            request_count: list[int] = []
            server = threading.Thread(
                target=self._serve_fake_stats_socket,
                args=(socket_path, ready, stop, connection_count, request_count),
                daemon=True,
            )
            server.start()
            self.assertTrue(ready.wait(timeout=1))

            try:

                def enough_samples() -> bool:
                    if not output_path.exists():
                        return False
                    return (
                        len(output_path.read_text(encoding="utf-8").splitlines()) >= 2
                    )

                measure_latency.capture_stats(
                    output_path,
                    socket_path,
                    1,
                    should_stop=enough_samples,
                    install_signal_handlers=False,
                )
            finally:
                stop.set()
                server.join(timeout=1)
                try:
                    socket_path.unlink()
                except FileNotFoundError:
                    pass

            samples = [
                json.loads(line)
                for line in output_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertGreaterEqual(len(samples), 2)
            self.assertEqual(sum(connection_count), 1)
            self.assertGreaterEqual(sum(request_count), len(samples) + 1)
            self.assertEqual(samples[0]["stats"]["nr_user_dispatches"], 1)
            self.assertTrue(
                all(sample["stats"]["nr_user_dispatches"] > 0 for sample in samples)
            )

    def test_workload_registry_covers_sebs_inspired_shapes(self) -> None:
        self.assertIn("memory_heavy", harness.WORKLOADS)
        self.assertIn("network_heavy", harness.WORKLOADS)
        self.assertIn("compression_mixed", harness.WORKLOADS)
        self.assertIn("graph_bfs", harness.WORKLOADS)
        self.assertIn("120.uploader", harness.WORKLOADS["network_heavy"].inspired_by)
        self.assertIn("503.graph-bfs", harness.WORKLOADS["graph_bfs"].inspired_by)

    def test_default_deadlines_include_workload_runtime_headroom(self) -> None:
        for spec in harness.WORKLOADS.values():
            duration_us = spec.default_duration_ms * 1_000
            self.assertGreater(spec.default_deadline_us, duration_us)

        self.assertEqual(harness.WORKLOADS["cpu_burst"].default_duration_ms, 250)
        self.assertEqual(harness.WORKLOADS["cpu_burst"].default_deadline_us, 500_000)

    def test_duration_override_derives_matching_default_deadline(self) -> None:
        spec = harness.WORKLOADS["cpu_burst"]
        self.assertEqual(harness.resolve_deadline_us(spec, 15, None), 30_000)
        self.assertEqual(harness.resolve_deadline_us(spec, 15, 12_000), 12_000)

    def test_workload_command_uses_compiled_rust_runner(self) -> None:
        command = harness.workload_command("cpu_burst", 250)
        command_text = " ".join(command)
        self.assertIn("cosmos-benchmark-workload", command_text)
        self.assertIn("--workload cpu_burst", command_text)
        self.assertIn("--duration-ms 250", command_text)
        self.assertNotIn("workload.py", command_text)

    def test_load_assessment_rejects_hard_overload(self) -> None:
        fair = measure_latency.assess_load(
            concurrency=3,
            duration_ms=10,
            deadline_us=10_000,
            cpu_cores=4,
        )
        self.assertTrue(fair["fair"])
        self.assertEqual(fair["class"], "underloaded")

        overloaded = measure_latency.assess_load(
            concurrency=4,
            duration_ms=10,
            deadline_us=10_000,
            cpu_cores=4,
        )
        self.assertFalse(overloaded["fair"])
        self.assertEqual(overloaded["class"], "unfair-overloaded")

    def test_timestamped_run_id_is_fine_grained(self) -> None:
        first = harness.timestamped_run_id()
        second = harness.timestamped_run_id()
        self.assertNotEqual(first, second)

    def test_parse_invocation_time_stats_reads_runner_done_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            stderr_path = Path(tmp) / "1.stderr"
            stderr_path.write_text(
                "COSMOS_RUNNER_DONE monotonic_ns=123456789\n"
                "COSMOS_TIME real_s=0.12 user_s=0.03 sys_s=0.04 maxrss_kb=2048\n",
                encoding="utf-8",
            )

            stats = harness.parse_invocation_time_stats_file(stderr_path)

        self.assertIsNotNone(stats)
        assert stats is not None
        self.assertEqual(stats["runner_end_monotonic_ns"], 123456789)
        self.assertAlmostEqual(stats["real_ms"], 120.0)
        self.assertAlmostEqual(stats["cpu_ms"], 70.0)
        self.assertEqual(stats["maxrss_kb"], 2048.0)

    def test_invocation_output_payload_prefers_runner_end_for_slo_duration(self) -> None:
        spec = harness.InvocationSpec(
            invocation_id=1,
            workload="cpu_burst",
            actual_duration_ms=10,
            deadline_us=100_000,
            config="cosmos-full",
        )

        payload = harness.invocation_output_payload(
            spec,
            returncode=0,
            launch_start_ns=500,
            start_ns=1_000,
            end_ns=4_000_000,
            cleanup_end_ns=6_000_000,
            stderr_path=Path("/tmp/1.stderr"),
            metadata_ready_ns=1_000,
            metadata_tgid=123,
            metadata_tgids=[122, 123],
            metadata_key_visible=None,
            time_stats={"runner_end_monotonic_ns": 2_500_000},
        )

        self.assertEqual(payload["end_monotonic_ns"], 2_500_000)
        self.assertEqual(payload["wait_end_monotonic_ns"], 4_000_000)
        self.assertEqual(payload["cleanup_end_monotonic_ns"], 6_000_000)
        self.assertAlmostEqual(payload["duration_ms"], 2.499)
        self.assertAlmostEqual(payload["wait_duration_ms"], 3.999)
        self.assertAlmostEqual(payload["observed_duration_ms"], 5.999)
        self.assertAlmostEqual(payload["runner_to_wait_ms"], 1.5)
        self.assertAlmostEqual(payload["post_wait_cleanup_ms"], 2.0)

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
            with (run_dir / "client_latency.csv").open(
                "w", encoding="utf-8", newline=""
            ) as fh:
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
                        "slo_class",
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
                            "slo_class": 0,
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
                            "slo_class": 2,
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
                            "slo_class": 2,
                            "workload": "cpu_burst",
                            "config": "cosmos-full",
                            "stderr_path": "/tmp/3.stderr",
                        },
                    ]
                )

            (run_dir / "scheduler_stats.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "ts_monotonic_ns": 1,
                                "stats": {"nr_queued": 1, "nr_scheduled": 2},
                            }
                        ),
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
            self.assertEqual(summary["per_slo_class"]["0"]["p50_ms"], 5.0)
            self.assertEqual(summary["per_slo_class"]["2"]["client_slo_violations"], 1)
            self.assertEqual(
                summary["per_slo_class"]["2"]["batch_slowdown_vs_latency_critical"],
                2.0,
            )
            self.assertEqual(summary["scheduler"]["peak"]["nr_queued"], 3)
            self.assertEqual(summary["scheduler"]["last"]["nr_slo_violations"], 2)
            self.assertIn("compute", summary)
            self.assertIn("load", summary)
            self.assertEqual(
                summary["load"]["rule"], "total_compute_ms < deadline_ms * cpu_cores"
            )

    def test_summarize_run_ignores_blank_slo_class_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            (run_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "config": "cfs-default",
                        "workload": "cpu_burst",
                        "concurrency": 1,
                        "duration_ms": 10,
                        "deadline_us": 100000,
                        "metadata_mode": "none",
                        "scheduler_flags": [],
                    }
                ),
                encoding="utf-8",
            )
            with (run_dir / "client_latency.csv").open(
                "w", encoding="utf-8", newline=""
            ) as fh:
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
                        "slo_class",
                        "workload",
                        "config",
                        "stderr_path",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "invocation_id": 1,
                        "status": "ok",
                        "exit_code": 0,
                        "start_monotonic_ns": 0,
                        "end_monotonic_ns": 5_000_000,
                        "duration_ms": 5.0,
                        "deadline_us": 100000,
                        "slo_class": "",
                        "workload": "cpu_burst",
                        "config": "cfs-default",
                        "stderr_path": "/tmp/1.stderr",
                    }
                )

            summary = measure_latency.summarize_run(run_dir)

            self.assertEqual(summary["latency"]["count"], 1)
            self.assertNotIn("per_slo_class", summary)

    def test_orchestrator_aggregates_report_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            direct = root / "azure-2019-direct-balanced"
            direct.mkdir(parents=True)
            (direct / "replay.json").write_text(
                json.dumps(
                    {
                        "schema": "cosmos.azure.2019-direct-synthetic-replay",
                        "window": {
                            "arrival_mode": "evenly-spaced",
                            "workload_mix_mode": "balanced",
                            "deadline_mode": "duration-headroom",
                        },
                        "invocations": [
                            {"at_ms": 0, "workload": "cpu_burst"},
                            {"at_ms": 1000, "workload": "network_heavy"},
                        ],
                    }
                ),
                encoding="utf-8",
            )
            openwhisk = root / "openwhisk-azure-2019-direct-balanced-bounded-2"
            openwhisk.mkdir()
            (openwhisk / "summary.json").write_text(
                json.dumps(
                    {
                        "count": 2,
                        "failures": 0,
                        "ok": True,
                        "slo": {
                            "successes": 2,
                            "slo_success_rate": 0.5,
                            "slo_goodput_per_s": 1.0,
                            "arrival": {
                                "average_arrival_rate_per_s": 2.0,
                                "peak_1s_arrival_rate": 2,
                            },
                            "submit_lag_ms": {"p99": 3.0},
                            "post_submit_latency_ms": {"p99": 4.0},
                            "target_duration_vs_deadline": {
                                "impossible_deadline_count": 1
                            },
                            "action_mix": {"ow_cpu_burst": 2},
                            "per_workload": {
                                "cpu_burst": {
                                    "attempts": 2,
                                    "successes": 2,
                                    "slo_success_rate": 0.5,
                                    "slo_goodput_per_s": 1.0,
                                    "latency_ms": {"p99": 4.0},
                                }
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )

            for config, p99 in (("cfs-default", 100.0), ("cosmos-full", 80.0)):
                run = root / "local_harness" / config / "cpu_burst" / "c64" / "run"
                run.mkdir(parents=True)
                (run / "summary.json").write_text(
                    json.dumps(
                        {
                            "config": config,
                            "workload": "cpu_burst",
                            "concurrency": 64,
                            "duration_ms": 250,
                            "deadline_us": 500000,
                            "latency": {
                                "count": 64,
                                "successes": 64,
                                "failures": 0,
                                "p50_ms": 50.0,
                                "p95_ms": 75.0,
                                "p99_ms": p99,
                                "mean_ms": 60.0,
                                "client_slo_violations": 0,
                            },
                            "load": {"load_ratio": 0.5},
                            "scheduler": {"total": {"nr_slo_boosted": 1}},
                        }
                    ),
                    encoding="utf-8",
                )

            cosched_csv = root / "local_harness" / "coscheduling_summary.csv"
            cosched_csv.parent.mkdir(parents=True, exist_ok=True)
            with cosched_csv.open("w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(
                    fh,
                    fieldnames=[
                        "scenario",
                        "config",
                        "slo_class",
                        "p99_ms",
                        "slo_violations",
                        "goodput_per_s",
                        "batch_slowdown_vs_latency_critical",
                        "scheduler_stall_signals",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "scenario": "lc-cpu_vs_batch-cpu_c96",
                        "config": "cosmos-full",
                        "slo_class": "2",
                        "p99_ms": "200",
                        "slo_violations": "0",
                        "goodput_per_s": "10",
                        "batch_slowdown_vs_latency_critical": "2.5",
                        "scheduler_stall_signals": "0",
                    }
                )

            orchestrator = orchestrate_benchmarks.BenchmarkOrchestrator(
                root,
                skip_remote=False,
                skip_local=False,
                openwhisk_limit=2,
            )
            try:
                aggregate = orchestrator.aggregate_results(
                    {"azure-2019-direct-balanced": direct},
                    None,
                )
                orchestrator.generate_report(aggregate)
            finally:
                orchestrator.log_fp.close()

            self.assertIn("openwhisk", aggregate)
            self.assertEqual(
                aggregate["replay_artifacts"]["azure-2019-direct-balanced"][
                    "workload_mix_mode"
                ],
                "balanced",
            )
            self.assertEqual(
                aggregate["local_harness"]["comparison_table"][0][
                    "full_vs_cfs_p99_pct"
                ],
                -20.0,
            )
            self.assertTrue((root / "local_harness_aggregate.csv").exists())
            report = (root / "COMPREHENSIVE_REPORT.md").read_text(encoding="utf-8")
            self.assertIn("Azure/OpenWhisk Results", report)
            self.assertIn("OpenWhisk Per-Workload SLO", report)
            self.assertIn("Mixed Co-Scheduling", report)
            self.assertIn("Batch slowdown", report)
            self.assertIn("Pool latency", report)
            self.assertIn("SLO boosts", report)

if __name__ == "__main__":
    unittest.main()
