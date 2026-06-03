import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "benchmarks" / "scripts"
LOCAL_HARNESS_DIR = REPO_ROOT / "benchmarks" / "local_harness"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


harness = load_module("harness", LOCAL_HARNESS_DIR / "harness.py")
run_cosmos = load_module("run_cosmos", LOCAL_HARNESS_DIR / "run_cosmos.py")
replay_top_functions = load_module(
    "replay_top_functions", SCRIPTS_DIR / "replay_top_functions.py"
)


class ReplayTopFunctionsTests(unittest.TestCase):
    def test_scheduler_peak_stats_takes_numeric_max(self) -> None:
        peaks = replay_top_functions.scheduler_peak_stats(
            [
                {"stats": {"nr_queued": 0, "nr_running": 5, "mode": "warmup"}},
                {"stats": {"nr_queued": 12, "nr_running": 3, "enabled": True}},
                {"stats": {"nr_queued": 2, "nr_running": 50}},
            ]
        )

        self.assertEqual(peaks["nr_queued"], 12)
        self.assertEqual(peaks["nr_running"], 50)
        self.assertNotIn("mode", peaks)
        self.assertNotIn("enabled", peaks)

    def test_load_profiles_normalizes_frequency_and_extracts_p99(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "top.json"
            config_path.write_text(
                json.dumps(
                    {
                        "schema": "cosmos.azure.top-functions-config",
                        "functions": [
                            {
                                "function_id": "func-a",
                                "frequency": 2.0,
                                "expected_time_ms": 10,
                                "time_distribution": {
                                    "min": 8,
                                    "p25": 9,
                                    "p50": 10,
                                    "p75": 12,
                                    "p99": 20,
                                    "max": 30,
                                },
                            },
                            {
                                "function_id": "func-b",
                                "frequency": 1.0,
                                "expected_time_ms": 40,
                                "time_distribution": {
                                    "min": 35,
                                    "p25": 37,
                                    "p50": 40,
                                    "p75": 45,
                                    "p99": 60,
                                    "max": 80,
                                },
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            profiles = replay_top_functions.load_profiles(config_path)

            self.assertEqual(len(profiles), 2)
            self.assertAlmostEqual(profiles[0].frequency, 2.0 / 3.0)
            self.assertAlmostEqual(profiles[1].frequency, 1.0 / 3.0)
            self.assertEqual(profiles[0].p99_time_ms, 20)
            self.assertEqual(profiles[1].expected_time_ms, 45)
            self.assertAlmostEqual(
                profiles[0].mean_time_ms,
                ((8 + 9) / 2 * 0.25)
                + ((9 + 10) / 2 * 0.25)
                + ((10 + 12) / 2 * 0.25)
                + ((12 + 20) / 2 * 0.24)
                + (20 * 0.01),
            )

    def test_build_invocation_uses_p99_deadline_and_clamped_tail(self) -> None:
        profile = replay_top_functions.FunctionProfile(
            function_id="func-a",
            frequency=1.0,
            expected_time_ms=25,
            p99_time_ms=70,
            mean_time_ms=50.0,
            time_distribution={
                "min": 50.0,
                "p25": 50.0,
                "p50": 50.0,
                "p75": 50.0,
                "p99": 50.0,
                "max": 50.0,
            },
        )

        item = replay_top_functions.build_invocation(
            [profile],
            [1.0],
            invocation_id=3,
            release_offset_s=0.25,
            generated_at_ns=123,
            min_slack_us=5_000,
            deadline_safety_factor=1.0,
            deadline_floor_ms=0.0,
            tail_max_multiplier=10.0,
            rng=replay_top_functions.random.Random(7),
        )

        self.assertEqual(item.invocation_id, 3)
        self.assertEqual(item.release_offset_s, 0.25)
        self.assertEqual(item.generated_at_ns, 123)
        self.assertEqual(item.actual_duration_ms, 50)
        self.assertEqual(item.deadline_us, 140_000)

    def test_build_invocation_honors_deadline_floor(self) -> None:
        profile = replay_top_functions.FunctionProfile(
            function_id="func-a",
            frequency=1.0,
            expected_time_ms=10,
            p99_time_ms=12,
            mean_time_ms=11.0,
            time_distribution={
                "min": 10.0,
                "p25": 10.0,
                "p50": 10.0,
                "p75": 11.0,
                "p99": 12.0,
                "max": 12.0,
            },
        )

        item = replay_top_functions.build_invocation(
            [profile],
            [1.0],
            invocation_id=4,
            release_offset_s=0.5,
            generated_at_ns=456,
            min_slack_us=5_000,
            deadline_safety_factor=1.0,
            deadline_floor_ms=25.0,
            tail_max_multiplier=10.0,
            rng=replay_top_functions.random.Random(1),
        )

        self.assertEqual(item.deadline_us, 25_000)

    def test_load_factor_and_rate_from_load_are_inverse(self) -> None:
        rate = replay_top_functions.rate_from_load(
            load=0.5,
            weighted_mean_ms=200.0,
            cpu_cores=48,
        )

        self.assertEqual(rate, 120.0)
        self.assertAlmostEqual(
            replay_top_functions.load_factor(
                rate_inv_per_sec=rate,
                weighted_mean_ms=200.0,
                cpu_cores=48,
            ),
            0.5,
        )

    def test_write_pool_json_records_actual_mean_and_profiles(self) -> None:
        profile = replay_top_functions.FunctionProfile(
            function_id="func-a",
            frequency=1.0,
            expected_time_ms=10,
            p99_time_ms=20,
            mean_time_ms=12.0,
            time_distribution={
                "min": 8.0,
                "p25": 9.0,
                "p50": 10.0,
                "p75": 11.0,
                "p99": 20.0,
                "max": 25.0,
            },
        )
        invocations = [
            replay_top_functions.PoolInvocation(
                function_id="func-a",
                expected_time_ms=10,
                actual_duration_ms=10,
                p99_time_ms=20,
                deadline_us=40_000,
            ),
            replay_top_functions.PoolInvocation(
                function_id="func-a",
                expected_time_ms=10,
                actual_duration_ms=30,
                p99_time_ms=20,
                deadline_us=40_000,
            ),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            pool_json = Path(tmp) / "pool.json"
            replay_top_functions.write_pool_json(
                pool_json,
                config_json=Path("top.json"),
                profiles=[profile],
                invocations=invocations,
                seed=42,
            )

            payload = json.loads(pool_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["pool_size"], 2)
            self.assertAlmostEqual(payload["actual_mean_time_ms"], 20.0)
            self.assertAlmostEqual(payload["weighted_mean_time_ms"], 12.0)
            self.assertEqual(payload["profiles"][0]["function_id"], "func-a")

            profiles = replay_top_functions.load_profiles_from_pool_json(pool_json)
            self.assertEqual(profiles[0].function_id, "func-a")
            self.assertEqual(profiles[0].p99_time_ms, 20)

    def test_cap_profiles_clamps_distribution_and_recomputes_mean(self) -> None:
        profile = replay_top_functions.FunctionProfile(
            function_id="func-a",
            frequency=1.0,
            expected_time_ms=20_000,
            p99_time_ms=60_000,
            mean_time_ms=10_000.0,
            time_distribution={
                "min": 10.0,
                "p25": 100.0,
                "p50": 1000.0,
                "p75": 20_000.0,
                "p99": 60_000.0,
                "max": 120_000.0,
            },
        )

        capped = replay_top_functions.cap_profiles([profile], 10_000)[0]

        self.assertEqual(capped.expected_time_ms, 10_000)
        self.assertEqual(capped.p99_time_ms, 10_000)
        self.assertEqual(capped.time_distribution["p75"], 10_000.0)
        self.assertEqual(capped.time_distribution["p99"], 10_000.0)
        self.assertEqual(capped.time_distribution["max"], 10_000.0)
        self.assertLess(capped.mean_time_ms, profile.mean_time_ms)

    def test_load_invocation_pool_prefers_actual_mean_with_weighted_fallback(self) -> None:
        profile = replay_top_functions.FunctionProfile(
            function_id="func-a",
            frequency=1.0,
            expected_time_ms=10,
            p99_time_ms=20,
            mean_time_ms=100.0,
            time_distribution={
                "min": 10.0,
                "p25": 10.0,
                "p50": 10.0,
                "p75": 10.0,
                "p99": 20.0,
                "max": 20.0,
            },
        )
        base_payload = {
            "schema": "cosmos.azure.top-functions-pool",
            "weighted_mean_time_ms": 100.0,
            "profiles": [
                {
                    "function_id": "func-a",
                    "frequency": 1.0,
                    "expected_time_ms": 10,
                    "p99_time_ms": 20,
                    "mean_time_ms": 100.0,
                    "time_distribution": profile.time_distribution,
                }
            ],
            "invocations": [
                {
                    "function_id": "func-a",
                    "expected_time_ms": 10,
                    "actual_duration_ms": 40,
                    "deadline_us": 40_000,
                }
            ],
        }

        with tempfile.TemporaryDirectory() as tmp:
            pool_json = Path(tmp) / "pool.json"
            payload = dict(base_payload)
            payload["actual_mean_time_ms"] = 40.0
            pool_json.write_text(json.dumps(payload), encoding="utf-8")
            pool = replay_top_functions.load_invocation_pool(
                pool_json,
                [profile],
                weighted_mean_ms=100.0,
                min_slack_us=5_000,
                deadline_safety_factor=1.0,
                deadline_floor_ms=0.0,
            )
            self.assertAlmostEqual(pool.load_mean_time_ms, 40.0)
            self.assertEqual(pool.load_mean_source, "actual_mean_time_ms")

            fallback_json = Path(tmp) / "fallback.json"
            fallback_json.write_text(json.dumps(base_payload), encoding="utf-8")
            fallback = replay_top_functions.load_invocation_pool(
                fallback_json,
                [profile],
                weighted_mean_ms=100.0,
                min_slack_us=5_000,
                deadline_safety_factor=1.0,
                deadline_floor_ms=0.0,
            )
            self.assertAlmostEqual(fallback.load_mean_time_ms, 100.0)
            self.assertEqual(fallback.load_mean_source, "weighted_mean_time_ms")

    def test_invocation_spec_separates_expected_actual_and_slo(self) -> None:
        profile = replay_top_functions.FunctionProfile(
            function_id="func-a",
            frequency=1.0,
            expected_time_ms=30,
            p99_time_ms=90,
            mean_time_ms=45.0,
            time_distribution={
                "min": 20.0,
                "p25": 25.0,
                "p50": 30.0,
                "p75": 45.0,
                "p99": 90.0,
                "max": 100.0,
            },
        )
        scheduled = replay_top_functions.ScheduledInvocation(
            invocation_id=3,
            release_offset_s=0.125,
            profile=profile,
            actual_duration_ms=44,
            deadline_us=180_000,
        )

        spec = replay_top_functions.invocation_spec_from_schedule(
            "cosmos-full",
            scheduled,
            release_monotonic_ns=123_000_000,
        )

        self.assertIsInstance(spec, harness.InvocationSpec)
        self.assertEqual(spec.workload, "cpu_burst")
        self.assertEqual(spec.actual_duration_ms, 44)
        self.assertEqual(spec.expected_duration_ms, 30)
        self.assertEqual(spec.deadline_us, 180_000)
        self.assertEqual(spec.action_name, "func-a")
        self.assertEqual(spec.slo_class, 0)
        self.assertEqual(spec.record_fields["function_id"], "func-a")
        self.assertEqual(spec.record_fields["p99_time_ms"], 90)
        self.assertEqual(spec.record_fields["estimation_error_ms"], 14)

    def test_run_load_sweep_scans_linear_loads(self) -> None:
        profiles = [
            replay_top_functions.FunctionProfile(
                function_id="func-a",
                frequency=1.0,
                expected_time_ms=10,
                p99_time_ms=20,
                mean_time_ms=100.0,
                time_distribution={
                    "min": 10.0,
                    "p25": 10.0,
                    "p50": 10.0,
                    "p75": 10.0,
                    "p99": 20.0,
                    "max": 20.0,
                },
            )
        ]
        args = type(
            "Args",
            (),
            {
                "config": "cosmos-full",
                "config_json": Path("dummy.json"),
                "arrival_mode": "poisson",
                "run_duration_s": 10.0,
                "warmup_duration_s": 1.0,
                "slo_miss_threshold": 0.05,
                "load_min": 0.5,
                "load_max": 1.0,
                "load_steps": 3,
                "rate": None,
                "seed": 11,
                "out_dir": None,
            },
        )()

        seen_rates: list[float] = []

        def fake_run_single_rate(**kwargs):
            seen_rates.append(kwargs["rate"])
            return {
                "goodput_inv_per_sec": kwargs["rate"] * 0.9,
                "effective_utilization": kwargs["rate"] * 0.09,
                "throughput_inv_per_sec": kwargs["rate"],
                "slo_miss_rate": 0.0,
            }

        original = replay_top_functions.run_single_rate
        with tempfile.TemporaryDirectory() as tmp:
            try:
                replay_top_functions.run_single_rate = fake_run_single_rate  # type: ignore[assignment]
                sweep = replay_top_functions.run_load_sweep(
                    args,
                    sweep_dir=Path(tmp),
                    profiles=profiles,
                )
            finally:
                replay_top_functions.run_single_rate = original  # type: ignore[assignment]

        self.assertEqual(len(seen_rates), 3)
        cpu_cores = replay_top_functions.os.cpu_count() or 1
        self.assertAlmostEqual(seen_rates[0], 0.5 * cpu_cores * 1000.0 / 100.0)
        self.assertAlmostEqual(seen_rates[1], 0.75 * cpu_cores * 1000.0 / 100.0)
        self.assertAlmostEqual(seen_rates[2], 1.0 * cpu_cores * 1000.0 / 100.0)
        self.assertEqual(sweep["single_rate_mode"], False)
        self.assertEqual(len(sweep["candidates"]), 3)
        self.assertAlmostEqual(sweep["optimal_rate_inv_per_sec"], seen_rates[2])

    def test_run_load_sweep_uses_pool_actual_mean_for_load_rates(self) -> None:
        profiles = [
            replay_top_functions.FunctionProfile(
                function_id="func-a",
                frequency=1.0,
                expected_time_ms=10,
                p99_time_ms=20,
                mean_time_ms=100.0,
                time_distribution={
                    "min": 10.0,
                    "p25": 10.0,
                    "p50": 10.0,
                    "p75": 10.0,
                    "p99": 20.0,
                    "max": 20.0,
                },
            )
        ]
        pool = replay_top_functions.InvocationPool(
            invocations=[
                replay_top_functions.PoolInvocation(
                    function_id="func-a",
                    expected_time_ms=10,
                    actual_duration_ms=200,
                    p99_time_ms=20,
                    deadline_us=40_000,
                )
            ],
            actual_mean_time_ms=200.0,
            weighted_mean_time_ms=100.0,
            load_mean_time_ms=200.0,
            load_mean_source="actual_mean_time_ms",
            cpu_cores=replay_top_functions.os.cpu_count() or 1,
        )
        args = type(
            "Args",
            (),
            {
                "config": "cosmos-full",
                "config_json": Path("dummy.json"),
                "arrival_mode": "evenly-spaced",
                "run_duration_s": 10.0,
                "warmup_duration_s": 1.0,
                "slo_miss_threshold": 0.05,
                "load_min": 0.5,
                "load_max": 0.5,
                "load_steps": 1,
                "rate": None,
                "seed": 11,
                "out_dir": None,
                "repeats": 3,
            },
        )()

        seen_rates: list[float] = []

        def fake_run_single_rate(**kwargs):
            seen_rates.append(kwargs["rate"])
            return {
                "goodput_inv_per_sec": kwargs["rate"] * 0.8,
                "effective_utilization": 0.4,
                "throughput_inv_per_sec": kwargs["rate"],
                "slo_miss_rate": 0.01,
            }

        original = replay_top_functions.run_single_rate
        with tempfile.TemporaryDirectory() as tmp:
            try:
                replay_top_functions.run_single_rate = fake_run_single_rate  # type: ignore[assignment]
                sweep = replay_top_functions.run_load_sweep(
                    args,
                    sweep_dir=Path(tmp),
                    profiles=profiles,
                    pool=pool,
                    pool_json=Path("pool.json"),
                )
            finally:
                replay_top_functions.run_single_rate = original  # type: ignore[assignment]

        cpu_cores = replay_top_functions.os.cpu_count() or 1
        expected_rate = 0.5 * cpu_cores * 1000.0 / 200.0
        self.assertEqual(len(seen_rates), 3)
        self.assertTrue(all(abs(rate - expected_rate) < 1e-9 for rate in seen_rates))
        self.assertAlmostEqual(sweep["actual_mean_time_ms"], 200.0)
        self.assertEqual(sweep["load_mean_source"], "actual_mean_time_ms")
        self.assertAlmostEqual(sweep["candidates"][0]["offered_load"], 0.5)
        self.assertEqual(sweep["candidates"][0]["repeats"], 3)

    def test_run_load_sweep_single_rate_mode(self) -> None:
        profiles = [
            replay_top_functions.FunctionProfile(
                function_id="func-a",
                frequency=1.0,
                expected_time_ms=10,
                p99_time_ms=20,
                mean_time_ms=100.0,
                time_distribution={
                    "min": 10.0,
                    "p25": 10.0,
                    "p50": 10.0,
                    "p75": 10.0,
                    "p99": 20.0,
                    "max": 20.0,
                },
            )
        ]
        args = type(
            "Args",
            (),
            {
                "config": "cosmos-full",
                "config_json": Path("dummy.json"),
                "arrival_mode": "poisson",
                "run_duration_s": 10.0,
                "warmup_duration_s": 1.0,
                "slo_miss_threshold": 0.05,
                "load_min": 0.5,
                "load_max": 1.0,
                "load_steps": 3,
                "rate": 123.0,
                "seed": 11,
                "out_dir": None,
            },
        )()

        seen_rates: list[float] = []

        def fake_run_single_rate(**kwargs):
            seen_rates.append(kwargs["rate"])
            return {
                "goodput_inv_per_sec": 100.0,
                "effective_utilization": 0.1,
                "throughput_inv_per_sec": 110.0,
                "slo_miss_rate": 0.0,
            }

        original = replay_top_functions.run_single_rate
        with tempfile.TemporaryDirectory() as tmp:
            try:
                replay_top_functions.run_single_rate = fake_run_single_rate  # type: ignore[assignment]
                sweep = replay_top_functions.run_load_sweep(
                    args,
                    sweep_dir=Path(tmp),
                    profiles=profiles,
                )
            finally:
                replay_top_functions.run_single_rate = original  # type: ignore[assignment]

        self.assertEqual(seen_rates, [123.0])
        self.assertEqual(sweep["single_rate_mode"], True)
        self.assertIsNone(sweep["load_sweep"])


if __name__ == "__main__":
    unittest.main()
