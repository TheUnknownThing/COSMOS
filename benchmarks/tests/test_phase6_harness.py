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

if str(AZURE_TRACE_DIR) not in sys.path:
    sys.path.insert(0, str(AZURE_TRACE_DIR))


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


measure_latency = load_module("measure_latency", LOCAL_HARNESS_DIR / "measure_latency.py")
harness = load_module("harness", LOCAL_HARNESS_DIR / "harness.py")
azure_replay_common = load_module(
    "azure_replay_common", AZURE_TRACE_DIR / "azure_replay_common.py"
)
openwhisk_replay = load_module(
    "run_openwhisk_azure_replay", AZURE_TRACE_DIR / "run_openwhisk_azure_replay.py"
)
workload_classifier_2019 = load_module(
    "workload_classifier_2019", AZURE_TRACE_DIR / "workload_classifier_2019.py"
)
azure_2019_synthetic = load_module(
    "build_azure_trace_2019_synthetic",
    AZURE_TRACE_DIR / "build_azure_trace_2019_synthetic.py",
)
azure_2019_sebs = load_module(
    "build_azure_trace_2019_sebs", AZURE_TRACE_DIR / "build_azure_trace_2019_sebs.py"
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
        event = azure_replay_common.parse_2021_event(
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

    def _write_2019_fixture(self, root: Path) -> None:
        with (root / "invocations_per_function_md.anon.d01.csv").open(
            "w", encoding="utf-8", newline=""
        ) as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "HashOwner",
                    "HashApp",
                    "HashFunction",
                    "Trigger",
                    "1",
                    "2",
                    "3",
                    "4",
                ],
            )
            writer.writeheader()
            writer.writerows(
                [
                    {
                        "HashOwner": "owner",
                        "HashApp": "app-http",
                        "HashFunction": "fn-http",
                        "Trigger": "http",
                        "1": "3",
                        "2": "2",
                        "3": "1",
                        "4": "0",
                    },
                    {
                        "HashOwner": "owner",
                        "HashApp": "app-storage",
                        "HashFunction": "fn-storage",
                        "Trigger": "storage",
                        "1": "0",
                        "2": "4",
                        "3": "0",
                        "4": "4",
                    },
                    {
                        "HashOwner": "owner",
                        "HashApp": "app-memory",
                        "HashFunction": "fn-memory",
                        "Trigger": "queue",
                        "1": "10",
                        "2": "0",
                        "3": "15",
                        "4": "0",
                    },
                    {
                        "HashOwner": "owner",
                        "HashApp": "app-timer",
                        "HashFunction": "fn-timer",
                        "Trigger": "timer",
                        "1": "1",
                        "2": "0",
                        "3": "1",
                        "4": "0",
                    },
                ]
            )

        with (root / "function_durations_percentiles.anon.d01.csv").open(
            "w", encoding="utf-8", newline=""
        ) as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "HashOwner",
                    "HashApp",
                    "HashFunction",
                    "Average",
                    "Count",
                    "Minimum",
                    "Maximum",
                    "percentile_Average_50",
                    "percentile_Average_75",
                    "percentile_Average_99",
                ],
            )
            writer.writeheader()
            writer.writerows(
                [
                    {
                        "HashOwner": "owner",
                        "HashApp": "app-http",
                        "HashFunction": "fn-http",
                        "Average": "160",
                        "Count": "6",
                        "Minimum": "80",
                        "Maximum": "300",
                        "percentile_Average_50": "150",
                        "percentile_Average_75": "180",
                        "percentile_Average_99": "250",
                    },
                    {
                        "HashOwner": "owner",
                        "HashApp": "app-storage",
                        "HashFunction": "fn-storage",
                        "Average": "900",
                        "Count": "8",
                        "Minimum": "300",
                        "Maximum": "1600",
                        "percentile_Average_50": "850",
                        "percentile_Average_75": "1000",
                        "percentile_Average_99": "1500",
                    },
                    {
                        "HashOwner": "owner",
                        "HashApp": "app-memory",
                        "HashFunction": "fn-memory",
                        "Average": "300",
                        "Count": "25",
                        "Minimum": "100",
                        "Maximum": "700",
                        "percentile_Average_50": "300",
                        "percentile_Average_75": "500",
                        "percentile_Average_99": "700",
                    },
                    {
                        "HashOwner": "owner",
                        "HashApp": "app-timer",
                        "HashFunction": "fn-timer",
                        "Average": "25",
                        "Count": "2",
                        "Minimum": "20",
                        "Maximum": "40",
                        "percentile_Average_50": "25",
                        "percentile_Average_75": "30",
                        "percentile_Average_99": "40",
                    },
                ]
            )

        with (root / "app_memory_percentiles.anon.d01.csv").open(
            "w", encoding="utf-8", newline=""
        ) as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "HashOwner",
                    "HashApp",
                    "SampleCount",
                    "AverageAllocatedMb",
                    "AverageAllocatedMb_pct50",
                    "AverageAllocatedMb_pct75",
                ],
            )
            writer.writeheader()
            writer.writerows(
                [
                    {
                        "HashOwner": "owner",
                        "HashApp": "app-http",
                        "SampleCount": "10",
                        "AverageAllocatedMb": "256",
                        "AverageAllocatedMb_pct50": "256",
                        "AverageAllocatedMb_pct75": "300",
                    },
                    {
                        "HashOwner": "owner",
                        "HashApp": "app-storage",
                        "SampleCount": "10",
                        "AverageAllocatedMb": "384",
                        "AverageAllocatedMb_pct50": "384",
                        "AverageAllocatedMb_pct75": "512",
                    },
                    {
                        "HashOwner": "owner",
                        "HashApp": "app-memory",
                        "SampleCount": "10",
                        "AverageAllocatedMb": "1536",
                        "AverageAllocatedMb_pct50": "1536",
                        "AverageAllocatedMb_pct75": "1800",
                    },
                    {
                        "HashOwner": "owner",
                        "HashApp": "app-timer",
                        "SampleCount": "10",
                        "AverageAllocatedMb": "128",
                        "AverageAllocatedMb_pct50": "128",
                        "AverageAllocatedMb_pct75": "160",
                    },
                ]
            )

    def test_2019_classifier_requires_all_file_families(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(workload_classifier_2019.Missing2019DataError):
                workload_classifier_2019.load_classified_profiles(Path(tmp))

    def test_2019_classifier_infers_multiple_workload_families(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_2019_fixture(root)
            profiles = workload_classifier_2019.load_classified_profiles(root)

        by_function = {profile.features.function_id: profile for profile in profiles}
        self.assertEqual(by_function["app-http:fn-http"].family, "network_service")
        self.assertEqual(by_function["app-storage:fn-storage"].family, "storage_io")
        self.assertEqual(by_function["app-memory:fn-memory"].family, "memory_heavy")
        self.assertEqual(by_function["app-timer:fn-timer"].family, "timer_control")
        self.assertGreater(by_function["app-memory:fn-memory"].confidence, 0.40)

    def test_2019_synthetic_builder_refuses_without_2019_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trace = root / "trace.csv"
            trace.write_text(
                "app,func,end_timestamp,duration\napp,fn,1.5,0.5\n",
                encoding="utf-8",
            )
            output = root / "out"
            with self.assertRaises(SystemExit) as ctx:
                azure_2019_synthetic.main(
                    [
                        "--trace-2021",
                        str(trace),
                        "--dataset-2019-dir",
                        str(root / "missing-2019"),
                        "--output-dir",
                        str(output),
                    ]
                )
            self.assertNotEqual(ctx.exception.code, 0)
            self.assertFalse((output / "replay.json").exists())

    def test_2019_synthetic_builder_maps_classifier_to_synthetic_kernels(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset_2019 = root / "dataset2019"
            dataset_2019.mkdir()
            self._write_2019_fixture(dataset_2019)
            trace = root / "trace.csv"
            trace.write_text(
                "\n".join(
                    [
                        "app,func,end_timestamp,duration",
                        "app21-a,fn,1.150,0.150",
                        "app21-b,fn,2.850,0.850",
                        "app21-c,fn,3.300,0.300",
                        "app21-d,fn,4.025,0.025",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            output = root / "out"

            rc = azure_2019_synthetic.main(
                [
                    "--trace-2021",
                    str(trace),
                    "--dataset-2019-dir",
                    str(dataset_2019),
                    "--output-dir",
                    str(output),
                    "--scale",
                    "2",
                ]
            )

            self.assertEqual(rc, 0)
            replay = json.loads((output / "replay.json").read_text(encoding="utf-8"))
            self.assertEqual(
                replay["schema"], "cosmos.azure.2019-classified-synthetic-replay"
            )
            workloads = {item["workload"] for item in replay["invocations"]}
            families = {item["workload_family"] for item in replay["invocations"]}
            self.assertIn("network_heavy", workloads)
            self.assertIn("io_mixed", workloads)
            self.assertIn("memory_heavy", workloads)
            self.assertIn("cpu_burst", workloads)
            self.assertIn("network_service", families)
            self.assertEqual(replay["invocations"][0]["at_ms"], 0.0)
            self.assertEqual(replay["invocations"][1]["duration_ms"], 850)

    def test_2019_sebs_builder_refuses_without_2019_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trace = root / "trace.csv"
            trace.write_text(
                "app,func,end_timestamp,duration\napp,fn,1.5,0.5\n",
                encoding="utf-8",
            )
            output = root / "out"
            with self.assertRaises(SystemExit) as ctx:
                azure_2019_sebs.main(
                    [
                        "--trace-2021",
                        str(trace),
                        "--dataset-2019-dir",
                        str(root / "missing-2019"),
                        "--output-dir",
                        str(output),
                    ]
                )
            self.assertNotEqual(ctx.exception.code, 0)
            self.assertFalse((output / "replay.json").exists())

    def test_2019_sebs_builder_maps_classifier_to_sebs_benchmarks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset_2019 = root / "dataset2019"
            dataset_2019.mkdir()
            self._write_2019_fixture(dataset_2019)
            trace = root / "trace.csv"
            trace.write_text(
                "\n".join(
                    [
                        "app,func,end_timestamp,duration",
                        "app21-a,fn,1.150,0.150",
                        "app21-b,fn,2.850,0.850",
                        "app21-c,fn,3.300,0.300",
                        "app21-d,fn,4.025,0.025",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            output = root / "out"

            rc = azure_2019_sebs.main(
                [
                    "--trace-2021",
                    str(trace),
                    "--dataset-2019-dir",
                    str(dataset_2019),
                    "--output-dir",
                    str(output),
                    "--runtime",
                    "python",
                    "--scale",
                    "2",
                ]
            )

            self.assertEqual(rc, 0)
            replay = json.loads((output / "replay.json").read_text(encoding="utf-8"))
            profiles = json.loads((output / "profiles.json").read_text(encoding="utf-8"))
            self.assertEqual(replay["schema"], "cosmos.azure.2019-classified-sebs-replay")
            workloads = {item["workload"] for item in replay["invocations"]}
            self.assertIn("110.dynamic-html", workloads)
            self.assertIn("210.thumbnailer", workloads)
            self.assertIn("411.image-recognition", workloads)
            self.assertIn("010.sleep", workloads)
            self.assertEqual(replay["invocations"][0]["sebs"]["runtime"], "python")
            self.assertIn("sebs_payload", replay["invocations"][0])
            self.assertEqual(replay["invocations"][1]["duration_ms"], 850)
            self.assertIn("family_to_sebs_candidates", profiles)
            self.assertIn("sebs_benchmark_counts", replay["classifier_summary"])

    def test_openwhisk_replay_uses_explicit_payload_when_present(self) -> None:
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
                                "workload": "010.sleep",
                                "target_duration_ms": 321,
                                "deadline_us": 642000,
                                "slo_class": 1,
                                "sebs_payload": {"sleep": 1},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            invocation = openwhisk_replay.load_replay(replay, {}, None)[0]
            self.assertEqual(openwhisk_replay.make_payload(invocation), {"sleep": 1})

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
