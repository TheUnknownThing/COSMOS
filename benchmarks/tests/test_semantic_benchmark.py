import csv
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SEMANTIC_DIR = REPO_ROOT / "benchmarks" / "semantic_benchmark"

if str(SEMANTIC_DIR) not in sys.path:
    sys.path.insert(0, str(SEMANTIC_DIR))


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


trace_ir = load_module("trace_ir", SEMANTIC_DIR / "trace_ir.py")
semantic_catalog = load_module("semantic_catalog", SEMANTIC_DIR / "semantic_catalog.py")
build_trace_ir = load_module("build_trace_ir", SEMANTIC_DIR / "build_trace_ir.py")
build_semantic_assignments = load_module(
    "build_semantic_assignments", SEMANTIC_DIR / "build_semantic_assignments.py"
)
build_calibration = load_module("build_calibration", SEMANTIC_DIR / "build_calibration.py")
build_replay = load_module("build_replay", SEMANTIC_DIR / "build_replay.py")


class SemanticBenchmarkTests(unittest.TestCase):
    def _write_azure_2021_fixture(self, path: Path) -> None:
        path.write_text(
            "\n".join(
                [
                    "app,func,end_timestamp,duration",
                    "app-a,fn-hot,1.100,0.100",
                    "app-a,fn-hot,1.450,0.050",
                    "app-a,fn-cold,2.000,0.250",
                    "app-b,fn-net,4.000,1.000",
                    "app-c,fn-mem,4.100,0.100",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

    def _calibration_payload(self) -> dict:
        return {
            "version": 1,
            "schema": "cosmos.semantic.duration-calibration",
            "created_at": "fixture",
            "source": {"fixture": True},
            "bucket_targets_ms": {
                bucket: semantic_catalog.DEFAULT_BUCKET_TARGET_MS[bucket]
                for bucket in semantic_catalog.DURATION_BUCKETS
            },
            "measurements": {
                realization_id: {
                    "semantic_source": semantic_catalog.REALIZATIONS[realization_id][
                        "semantic_source"
                    ],
                    "resource_class": semantic_catalog.REALIZATIONS[realization_id][
                        "resource_class"
                    ],
                    "uses_upstream_sebs_directly": False,
                    "buckets": {
                        bucket: {
                            "supported": True,
                            "target_duration_ms": semantic_catalog.DEFAULT_BUCKET_TARGET_MS[
                                bucket
                            ],
                            "isolated_warm_ms": {
                                "count": 2,
                                "min": 1.0,
                                "p50": 1.0,
                                "p90": 1.0,
                                "p99": 1.0,
                                "max": 1.0,
                            },
                            "cold_start_ms": {
                                "count": 1,
                                "min": 1.0,
                                "p50": 1.0,
                                "p90": 1.0,
                                "p99": 1.0,
                                "max": 1.0,
                            },
                            "openwhisk": {
                                "available": False,
                                "submit_lag_ms": None,
                                "post_submit_latency_ms": None,
                            },
                        }
                        for bucket in semantic_catalog.DURATION_BUCKETS
                    },
                }
                for realization_id in semantic_catalog.KERNEL_MODE_BY_REALIZATION
            },
            "unsupported": [],
        }

    def _add_upstream_calibration(
        self,
        calibration: dict,
        anchor_id: str,
        bucket: str,
        input_size: str = "test",
    ) -> dict:
        calibration = json.loads(json.dumps(calibration))
        calibration["measurements"][semantic_catalog.UPSTREAM_SEBS_REALIZATION_ID] = {
            "semantic_source": "upstream-sebs",
            "resource_class": "anchor",
            "uses_upstream_sebs_directly": True,
            "anchors": {
                anchor_id: {
                    "buckets": {
                        bucket: {
                            "supported": True,
                            "sebs_anchor": anchor_id,
                            "input_size": input_size,
                            "runtime": "python",
                            "target_duration_ms": semantic_catalog.DEFAULT_BUCKET_TARGET_MS[
                                bucket
                            ],
                            "isolated_warm_ms": {
                                "count": 3,
                                "min": 90.0,
                                "p50": 100.0,
                                "p90": 110.0,
                                "p99": 115.0,
                                "max": 115.0,
                            },
                            "cold_start_ms": {
                                "count": 1,
                                "min": 500.0,
                                "p50": 500.0,
                                "p90": 500.0,
                                "p99": 500.0,
                                "max": 500.0,
                            },
                            "openwhisk": {
                                "available": True,
                                "submit_lag_ms": {"p50": 3.0, "p99": 8.0},
                                "post_submit_latency_ms": {"p50": 120.0, "p99": 150.0},
                            },
                        }
                    }
                }
            },
        }
        return calibration

    def test_contract_declares_trace_truth_and_supported_mixes(self) -> None:
        contract = trace_ir.load_contract()

        self.assertEqual(contract["schema"], "cosmos.semantic-benchmark.contract")
        self.assertEqual(contract["trace_truth"]["time_unit"], "seconds")
        self.assertIn("duration", contract["trace_truth"]["fields"])
        self.assertIn("cpu-heavy", contract["semantic_mixes"])
        self.assertIn("balanced", contract["semantic_mixes"])
        self.assertNotIn("modeled_assumptions", contract)
        self.assertNotIn("non_claims", contract)
        self.assertNotIn("source_references", contract)

    def test_parse_azure_2021_row_uses_seconds_and_preserves_function_id(self) -> None:
        parsed = trace_ir.parse_azure_2021_row(
            {
                "app": "app-a",
                "func": "fn-b",
                "end_timestamp": "10.250",
                "duration": "0.125",
            }
        )

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.function_id, "app-a:fn-b")
        self.assertEqual(parsed.source_start_ms, 10125.0)
        self.assertEqual(parsed.source_end_ms, 10250.0)
        self.assertEqual(parsed.target_duration_ms, 125)

    def test_build_trace_ir_e2e_preserves_timing_identity_and_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trace = root / "azure2021.csv"
            self._write_azure_2021_fixture(trace)
            output = root / "out"

            rc = build_trace_ir.main(
                [
                    "--trace-2021",
                    str(trace),
                    "--output-dir",
                    str(output),
                    "--scale",
                    "2",
                ]
            )

            self.assertEqual(rc, 0)
            trace_payload = json.loads((output / "trace_ir.json").read_text(encoding="utf-8"))
            fidelity = json.loads((output / "fidelity.json").read_text(encoding="utf-8"))
            with (output / "trace_invocations.csv").open(encoding="utf-8", newline="") as fh:
                rows = list(csv.DictReader(fh))

            self.assertEqual(trace_payload["schema"], "cosmos.semantic.azure-2021-trace-ir")
            self.assertEqual(trace_payload["contract"]["trace_time_unit"], "seconds")
            self.assertFalse(trace_payload["contract"]["modeled_fields_present"])
            self.assertEqual(trace_payload["window"]["scale"], 2.0)
            self.assertEqual(len(trace_payload["invocations"]), 5)
            self.assertEqual(len(rows), 5)

            first = trace_payload["invocations"][0]
            self.assertEqual(first["function_id"], "app-a:fn-hot")
            self.assertEqual(first["source_start_ms"], 1000.0)
            self.assertEqual(first["source_end_ms"], 1100.0)
            self.assertEqual(first["target_duration_ms"], 100)
            self.assertEqual(first["at_ms"], 500.0)

            second = trace_payload["invocations"][1]
            self.assertEqual(second["function_id"], "app-a:fn-hot")
            self.assertEqual(second["at_ms"], 700.0)

            function_ids = [item["function_id"] for item in trace_payload["invocations"]]
            self.assertEqual(function_ids.count("app-a:fn-hot"), 2)
            self.assertEqual(fidelity["source_window"]["invocations"], 5)
            self.assertEqual(fidelity["generated"]["functions"], 4)
            self.assertEqual(fidelity["distances"]["duration_ks"], 0.0)
            self.assertEqual(fidelity["distances"]["iat_ks"], 0.0)

    def test_build_trace_ir_can_normalize_window_to_first_start(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trace = root / "azure2021.csv"
            self._write_azure_2021_fixture(trace)
            output = root / "out"

            rc = build_trace_ir.main(
                [
                    "--trace-2021",
                    str(trace),
                    "--output-dir",
                    str(output),
                    "--scale",
                    "2",
                    "--normalize-to-first-start",
                ]
            )

            self.assertEqual(rc, 0)
            trace_payload = json.loads((output / "trace_ir.json").read_text(encoding="utf-8"))
            self.assertTrue(trace_payload["window"]["normalize_to_first_start"])
            self.assertEqual(trace_payload["window"]["base_ms"], 1000.0)
            self.assertEqual(trace_payload["invocations"][0]["at_ms"], 0.0)

    def test_build_trace_ir_records_downsample_selection_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trace = root / "azure2021.csv"
            self._write_azure_2021_fixture(trace)
            output = root / "out"

            rc = build_trace_ir.main(
                [
                    "--trace-2021",
                    str(trace),
                    "--output-dir",
                    str(output),
                    "--limit",
                    "2",
                    "--downsample-mode",
                    "top-apps",
                ]
            )

            self.assertEqual(rc, 0)
            trace_payload = json.loads((output / "trace_ir.json").read_text(encoding="utf-8"))
            fidelity = json.loads((output / "fidelity.json").read_text(encoding="utf-8"))

            self.assertEqual(trace_payload["selection"]["total_window_invocations"], 5)
            self.assertEqual(trace_payload["selection"]["selected_invocations"], 2)
            self.assertEqual(trace_payload["selection"]["selector_kind"], "app")
            self.assertEqual(trace_payload["selection"]["selected_keys"], ["app-a"])
            self.assertEqual(
                {item["app"] for item in trace_payload["invocations"]},
                {"app-a"},
            )
            self.assertEqual(fidelity["selection"]["limit"], 2)
            self.assertIsNotNone(fidelity["distances"]["popularity_ks"])

    def test_semantic_catalog_covers_sebs_anchors_and_duration_realizations(self) -> None:
        catalog = semantic_catalog.catalog_payload()

        self.assertEqual(catalog["schema"], "cosmos.semantic.sebs-anchor-realization-catalog")
        self.assertGreaterEqual(len(catalog["anchors"]), 15)
        self.assertIn("010.sleep", catalog["anchors"])
        self.assertIn("411.image-recognition", catalog["anchors"])
        self.assertIn("504.dna-visualisation", catalog["anchors"])
        self.assertEqual(
            set(catalog["semantic_mixes"]),
            {"cpu-heavy", "io-heavy", "memory-heavy", "network-heavy", "balanced"},
        )

        for anchor in catalog["anchors"].values():
            for field in (
                "benchmark_id",
                "action_name",
                "runtime_options",
                "input_size_options",
                "resource_hints",
                "cold_warm_assumptions",
                "requires_storage",
                "requires_external_service",
            ):
                self.assertIn(field, anchor)
            self.assertFalse(anchor["resource_hints"]["measured"])
            self.assertEqual(anchor["resource_hints"]["source"], "sebs-anchor-semantics")

        for realization in catalog["realizations"].values():
            if not realization["uses_upstream_sebs_directly"]:
                self.assertNotIn("python", realization["calibration_command"])
                self.assertIn("semantic_kernel", realization["calibration_command"])

        covered = {
            (realization["resource_class"], bucket)
            for realization in catalog["realizations"].values()
            if not realization["uses_upstream_sebs_directly"]
            for bucket in realization["supported_duration_buckets"]
        }
        for resource_class in ("cpu", "io", "memory", "network", "balanced"):
            for bucket in semantic_catalog.DURATION_BUCKETS:
                self.assertIn((resource_class, bucket), covered)

    def test_semantic_assignment_is_seeded_and_function_granular(self) -> None:
        trace_payload = {
            "version": 1,
            "schema": "cosmos.semantic.azure-2021-trace-ir",
            "invocations": [
                {
                    "invocation_id": 1,
                    "event_id": "az2021ir-00000001",
                    "app": "app-a",
                    "func": "fn-hot",
                    "function_id": "app-a:fn-hot",
                    "source_start_ms": 0.0,
                    "source_end_ms": 40.0,
                    "target_duration_ms": 40,
                    "at_ms": 0.0,
                },
                {
                    "invocation_id": 2,
                    "event_id": "az2021ir-00000002",
                    "app": "app-a",
                    "func": "fn-hot",
                    "function_id": "app-a:fn-hot",
                    "source_start_ms": 100.0,
                    "source_end_ms": 350.0,
                    "target_duration_ms": 250,
                    "at_ms": 100.0,
                },
                {
                    "invocation_id": 3,
                    "event_id": "az2021ir-00000003",
                    "app": "app-b",
                    "func": "fn-cold",
                    "function_id": "app-b:fn-cold",
                    "source_start_ms": 1000.0,
                    "source_end_ms": 3500.0,
                    "target_duration_ms": 2500,
                    "at_ms": 1000.0,
                },
            ],
        }

        first = build_semantic_assignments.build_assignments_payload(
            trace_payload,
            Path("trace_ir.json"),
            "balanced",
            42,
        )
        second = build_semantic_assignments.build_assignments_payload(
            trace_payload,
            Path("trace_ir.json"),
            "balanced",
            42,
        )

        self.assertEqual(first["function_mappings"], second["function_mappings"])
        self.assertEqual(first["summary"]["anchor_counts"], second["summary"]["anchor_counts"])
        self.assertEqual(first["policy"]["seed"], 42)
        self.assertEqual(first["policy"]["semantic_mix"], "balanced")
        self.assertEqual(first["summary"]["functions"], 2)
        self.assertEqual(first["summary"]["invocations"], 3)

        hot_anchor = first["function_mappings"]["app-a:fn-hot"]["semantic_anchor"]
        hot_invocations = [
            item for item in first["invocations"] if item["function_id"] == "app-a:fn-hot"
        ]
        self.assertEqual({item["sebs_anchor"] for item in hot_invocations}, {hot_anchor})
        self.assertEqual(
            [item["target_duration_class"] for item in hot_invocations],
            ["0-50ms", "200-400ms"],
        )
        self.assertFalse(
            any(item["duration_realization_uses_upstream_sebs"] for item in first["invocations"])
        )

    def test_build_semantic_assignments_e2e_from_trace_ir_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trace = root / "azure2021.csv"
            self._write_azure_2021_fixture(trace)
            trace_output = root / "trace"
            semantic_output = root / "semantic"

            self.assertEqual(
                build_trace_ir.main(
                    [
                        "--trace-2021",
                        str(trace),
                        "--output-dir",
                        str(trace_output),
                        "--scale",
                        "2",
                    ]
                ),
                0,
            )
            self.assertEqual(
                build_semantic_assignments.main(
                    [
                        "--trace-ir",
                        str(trace_output / "trace_ir.json"),
                        "--output-dir",
                        str(semantic_output),
                        "--semantic-mix",
                        "cpu-heavy",
                        "--seed",
                        "7",
                    ]
                ),
                0,
            )

            catalog = json.loads(
                (semantic_output / "semantic_catalog.json").read_text(encoding="utf-8")
            )
            assignments = json.loads(
                (semantic_output / "semantic_assignments.json").read_text(encoding="utf-8")
            )
            with (semantic_output / "semantic_invocations.csv").open(
                encoding="utf-8", newline=""
            ) as fh:
                rows = list(csv.DictReader(fh))

            self.assertEqual(catalog["schema"], "cosmos.semantic.sebs-anchor-realization-catalog")
            self.assertEqual(assignments["schema"], "cosmos.semantic.azure-2021-sebs-assignment")
            self.assertEqual(assignments["contract"]["modeled_fields_present"], True)
            self.assertEqual(assignments["policy"]["semantic_mix"], "cpu-heavy")
            self.assertEqual(assignments["policy"]["seed"], 7)
            self.assertEqual(assignments["summary"]["invocations"], 5)
            self.assertEqual(assignments["summary"]["functions"], 4)
            self.assertEqual(len(rows), 5)
            self.assertIn("app-a:fn-hot", assignments["function_mappings"])
            self.assertEqual(
                len(
                    {
                        item["sebs_anchor"]
                        for item in assignments["invocations"]
                        if item["function_id"] == "app-a:fn-hot"
                    }
                ),
                1,
            )
            self.assertEqual(
                [item["target_duration_class"] for item in assignments["invocations"]],
                ["50-200ms", "50-200ms", "200-400ms", "400ms-2s", "50-200ms"],
            )
            self.assertEqual(
                sum(assignments["summary"]["realization_counts"].values()),
                5,
            )

    def test_build_calibration_runs_controllable_kernel(self) -> None:
        kernel_dir = SEMANTIC_DIR / "kernels"
        build = subprocess.run(
            ["make", "-C", str(kernel_dir)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(build.returncode, 0, build.stderr)

        payload = build_calibration.build_calibration_payload(
            kernel_path=kernel_dir / "semantic_kernel",
            realizations=["cpu-spin-controllable"],
            buckets=["0-50ms"],
            bucket_targets_ms={"0-50ms": 5},
            repetitions=2,
            tolerance=2.0,
        )

        bucket = payload["measurements"]["cpu-spin-controllable"]["buckets"]["0-50ms"]
        self.assertEqual(payload["schema"], "cosmos.semantic.duration-calibration")
        self.assertTrue(bucket["supported"])
        self.assertEqual(bucket["isolated_warm_ms"]["count"], 1)
        self.assertGreaterEqual(bucket["isolated_warm_ms"]["p50"], 5.0)

    def test_calibration_gates_semantic_assignment(self) -> None:
        trace_payload = {
            "version": 1,
            "schema": "cosmos.semantic.azure-2021-trace-ir",
            "invocations": [
                {
                    "invocation_id": 1,
                    "event_id": "az2021ir-00000001",
                    "app": "app-a",
                    "func": "fn-hot",
                    "function_id": "app-a:fn-hot",
                    "source_start_ms": 0.0,
                    "source_end_ms": 100.0,
                    "target_duration_ms": 100,
                    "at_ms": 0.0,
                }
            ],
        }

        with self.assertRaises(ValueError):
            build_semantic_assignments.build_assignments_payload(
                trace_payload,
                Path("trace_ir.json"),
                "balanced",
                42,
                calibration={
                    "schema": "cosmos.semantic.duration-calibration",
                    "created_at": "fixture",
                    "measurements": {},
                },
            )

        assignments = build_semantic_assignments.build_assignments_payload(
            trace_payload,
            Path("trace_ir.json"),
            "balanced",
            42,
            calibration=self._calibration_payload(),
        )
        self.assertTrue(assignments["invocations"][0]["calibration_supports_slo"])
        self.assertEqual(
            assignments["invocations"][0]["calibration_status"],
            "calibrated-supported",
        )

    def test_assignment_prefers_calibrated_upstream_sebs_before_synthetic_fallback(self) -> None:
        function_id = "app-a:fn-hot"
        anchor_id = semantic_catalog.choose_anchor(function_id, "balanced", 42)
        trace_payload = {
            "version": 1,
            "schema": "cosmos.semantic.azure-2021-trace-ir",
            "invocations": [
                {
                    "invocation_id": 1,
                    "event_id": "az2021ir-00000001",
                    "app": "app-a",
                    "func": "fn-hot",
                    "function_id": function_id,
                    "source_start_ms": 0.0,
                    "source_end_ms": 100.0,
                    "target_duration_ms": 100,
                    "at_ms": 0.0,
                },
                {
                    "invocation_id": 2,
                    "event_id": "az2021ir-00000002",
                    "app": "app-a",
                    "func": "fn-hot",
                    "function_id": function_id,
                    "source_start_ms": 1000.0,
                    "source_end_ms": 1250.0,
                    "target_duration_ms": 250,
                    "at_ms": 1000.0,
                },
            ],
        }
        calibration = self._add_upstream_calibration(
            self._calibration_payload(),
            anchor_id,
            "50-200ms",
        )

        assignments = build_semantic_assignments.build_assignments_payload(
            trace_payload,
            Path("trace_ir.json"),
            "balanced",
            42,
            calibration=calibration,
        )

        first, second = assignments["invocations"]
        self.assertEqual(first["duration_realization"], "upstream-sebs-calibrated")
        self.assertEqual(first["actual_workload"], anchor_id)
        self.assertEqual(first["actual_workload_source"], "upstream-sebs")
        self.assertEqual(first["duration_realization_reason"], "calibrated-upstream-sebs")
        self.assertTrue(first["duration_realization_uses_upstream_sebs"])

        self.assertNotEqual(second["duration_realization"], "upstream-sebs-calibrated")
        self.assertEqual(second["actual_workload"], second["duration_realization"])
        self.assertEqual(second["actual_workload_source"], "synthesized")
        self.assertEqual(
            second["duration_realization_reason"],
            "synthetic-fallback-no-calibrated-upstream-sebs",
        )

    def test_build_replay_e2e_from_calibrated_semantic_assignments(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trace = root / "azure2021.csv"
            self._write_azure_2021_fixture(trace)
            trace_output = root / "trace"
            semantic_output = root / "semantic"
            replay_output = root / "replay"
            calibration_path = root / "calibration.json"
            calibration_path.write_text(
                json.dumps(self._calibration_payload(), indent=2) + "\n",
                encoding="utf-8",
            )

            self.assertEqual(
                build_trace_ir.main(
                    [
                        "--trace-2021",
                        str(trace),
                        "--output-dir",
                        str(trace_output),
                        "--scale",
                        "2",
                    ]
                ),
                0,
            )
            self.assertEqual(
                build_semantic_assignments.main(
                    [
                        "--trace-ir",
                        str(trace_output / "trace_ir.json"),
                        "--output-dir",
                        str(semantic_output),
                        "--semantic-mix",
                        "balanced",
                        "--seed",
                        "9",
                        "--calibration",
                        str(calibration_path),
                    ]
                ),
                0,
            )
            self.assertEqual(
                build_replay.main(
                    [
                        "--semantic-assignments",
                        str(semantic_output / "semantic_assignments.json"),
                        "--output-dir",
                        str(replay_output),
                    ]
                ),
                0,
            )

            profiles = json.loads((replay_output / "profiles.json").read_text("utf-8"))
            replay = json.loads((replay_output / "replay.json").read_text("utf-8"))
            fidelity = json.loads((replay_output / "fidelity.json").read_text("utf-8"))
            with (replay_output / "invocations.csv").open(encoding="utf-8", newline="") as fh:
                rows = list(csv.DictReader(fh))

            self.assertEqual(
                profiles["schema"],
                "cosmos.semantic.azure-2021-openwhisk-profiles",
            )
            self.assertEqual(
                replay["schema"],
                "cosmos.semantic.azure-2021-openwhisk-replay",
            )
            self.assertEqual(fidelity["schema"], "cosmos.semantic.replay-fidelity")
            self.assertEqual(len(replay["invocations"]), 5)
            self.assertEqual(len(rows), 5)
            first = replay["invocations"][0]
            self.assertEqual(first["source_start_ms"], 1000.0)
            self.assertEqual(first["at_ms"], 500.0)
            self.assertEqual(first["target_duration_ms"], 100)
            self.assertEqual(first["duration_ms"], 100)
            self.assertEqual(first["function_id"], "app-a:fn-hot")
            self.assertEqual(first["app"], "app-a")
            self.assertEqual(first["func"], "fn-hot")
            self.assertTrue(first["calibration_supports_slo"])
            self.assertEqual(first["workload"], first["duration_realization"])
            self.assertEqual(first["actual_workload_source"], "synthesized")

    def test_native_controllable_kernel_builds_and_runs_under_50ms_cpu(self) -> None:
        kernel_dir = SEMANTIC_DIR / "kernels"
        build = subprocess.run(
            ["make", "-C", str(kernel_dir)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(build.returncode, 0, build.stderr)

        run = subprocess.run(
            [
                str(kernel_dir / "semantic_kernel"),
                "--mode",
                "cpu",
                "--target-us",
                "5000",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(run.returncode, 0, run.stderr)
        payload = json.loads(run.stdout)
        self.assertEqual(payload["mode"], "cpu")
        self.assertGreaterEqual(payload["elapsed_us"], 5000)
        self.assertGreater(payload["iterations"], 0)


if __name__ == "__main__":
    unittest.main()
