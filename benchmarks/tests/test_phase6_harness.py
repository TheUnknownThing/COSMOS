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
AZURE_TRACE_DIR = REPO_ROOT / "benchmarks" / "azure_trace"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


measure_latency = load_module("measure_latency", LOCAL_HARNESS_DIR / "measure_latency.py")
harness = load_module("harness", LOCAL_HARNESS_DIR / "harness.py")
azure_builder = load_module(
    "build_azure_trace_benchmark", AZURE_TRACE_DIR / "build_azure_trace_benchmark.py"
)
openwhisk_replay = load_module(
    "run_openwhisk_azure_replay", AZURE_TRACE_DIR / "run_openwhisk_azure_replay.py"
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

    def test_azure_2021_row_derives_start_and_function_id(self) -> None:
        event = azure_builder.parse_2021_event(
            {
                "app": "app-a",
                "func": "func-b",
                "end_timestamp": "10.250",
                "duration": "0.125",
            }
        )
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.function_id, "app-a:func-b")
        self.assertEqual(event.duration_ms, 125)
        self.assertEqual(event.source_start_ms, 10125.0)

    def test_openwhisk_replay_loader_maps_profiles_to_actions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            replay = Path(tmp) / "replay.json"
            replay.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "invocations": [
                            {
                                "event_id": "az2021-1",
                                "invocation_id": 7,
                                "at_ms": 12.5,
                                "function_id": "app:func",
                                "profile_id": "azp_000001",
                                "workload": "pipeline",
                                "target_duration_ms": 321,
                                "deadline_us": 642000,
                                "slo_class": 1,
                                "profile_hints": {"cpu_intensity": 0.5},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            invocations = openwhisk_replay.load_replay(
                replay, {"azp_000001": "ow_pipeline"}, None
            )
        self.assertEqual(len(invocations), 1)
        self.assertEqual(invocations[0].action, "ow_pipeline")
        self.assertEqual(invocations[0].target_duration_ms, 321)
        self.assertEqual(invocations[0].profile_hints["cpu_intensity"], 0.5)

    def test_openwhisk_activation_parser_accepts_wsk_prefixed_json(self) -> None:
        stdout = (
            "ok: invoked /_/ow_pipeline with id 567159c9a3664b69b159c9a3663b694d\n"
            '{"activationId":"567159c9a3664b69b159c9a3663b694d"}\n'
        )
        self.assertEqual(
            openwhisk_replay.parse_activation_id(stdout),
            "567159c9a3664b69b159c9a3663b694d",
        )

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
            self.assertEqual(summary["scheduler"]["peak"]["nr_queued"], 3)
            self.assertEqual(summary["scheduler"]["last"]["nr_slo_violations"], 2)
            self.assertIn("compute", summary)
            self.assertIn("load", summary)
            self.assertEqual(
                summary["load"]["rule"], "total_compute_ms < deadline_ms * cpu_cores"
            )

if __name__ == "__main__":
    unittest.main()
