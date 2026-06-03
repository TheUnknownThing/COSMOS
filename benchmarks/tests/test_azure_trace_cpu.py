import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
AZURE_TRACE_DIR = REPO_ROOT / "benchmarks" / "azure_trace"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


cpu_trace_common = load_module(
    "cpu_trace_common",
    AZURE_TRACE_DIR / "cpu_trace_common.py",
)
generate_config = load_module(
    "generate_top_functions_config",
    AZURE_TRACE_DIR / "generate_top_functions_config.py",
)


class AzureTraceCpuTests(unittest.TestCase):
    def write_fixture_dataset(self, root: Path) -> Path:
        dataset = root / "azurefunctions-dataset2019"
        dataset.mkdir(parents=True, exist_ok=True)

        with (dataset / "invocations_per_function_md.anon.d01.csv").open(
            "w", encoding="utf-8", newline=""
        ) as fh:
            writer = csv.writer(fh)
            writer.writerow(
                ["HashOwner", "HashApp", "HashFunction", "Trigger", "1", "2", "3", "4"]
            )
            writer.writerow(["owner-a", "app-a", "func-a", "http", "1", "0", "2", "0"])
            writer.writerow(["owner-b", "app-b", "func-b", "timer", "0", "1", "0", "1"])

        with (dataset / "function_durations_percentiles.anon.d01.csv").open(
            "w", encoding="utf-8", newline=""
        ) as fh:
            writer = csv.writer(fh)
            writer.writerow(
                [
                    "HashOwner",
                    "HashApp",
                    "HashFunction",
                    "Average",
                    "Count",
                    "Minimum",
                    "Maximum",
                    "percentile_Average_25",
                    "percentile_Average_50",
                    "percentile_Average_75",
                    "percentile_Average_99",
                ]
            )
            writer.writerow(
                ["owner-a", "app-a", "func-a", "120", "3", "60", "400", "80", "100", "150", "300"]
            )
            writer.writerow(
                ["owner-b", "app-b", "func-b", "40", "2", "15", "120", "20", "30", "60", "100"]
            )

        return dataset

    def test_common_helpers_find_required_2019_cpu_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset = self.write_fixture_dataset(Path(tmp))

            invocation_files, duration_files = cpu_trace_common.require_2019_cpu_dataset(dataset)

            self.assertEqual(len(invocation_files), 1)
            self.assertEqual(len(duration_files), 1)
            rows = list(cpu_trace_common.open_csv_rows(invocation_files[0]))
            self.assertEqual(rows[0]["HashApp"], "app-a")
            self.assertEqual(cpu_trace_common.day_index_from_path(invocation_files[0], 99), 0)
            self.assertEqual(cpu_trace_common.deadline_us_for_duration(25), 50_000)

    def test_common_helpers_reject_missing_dataset_families(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(FileNotFoundError):
                cpu_trace_common.require_2019_cpu_dataset(Path(tmp))

    def test_generate_top_functions_config_from_distribution_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            distribution = root / "cpu_distribution.json"
            output = root / "top.json"
            distribution.write_text(
                json.dumps(
                    {
                        "schema": "cosmos.azure.2019.cpu-distribution",
                        "cpu_time_distribution": {"total_invocations": 5},
                        "profiles": [
                            {
                                "function_id": "func-a",
                                "invocation_count": 3,
                                "expected_cpu_time_ms": 120,
                                "cpu_time_ms": {
                                    "min": 60,
                                    "p25": 80,
                                    "p50": 100,
                                    "p75": 150,
                                    "p99": 300,
                                    "max": 400,
                                    "mean": 120,
                                },
                                "trigger": "http",
                            },
                            {
                                "function_id": "func-b",
                                "invocation_count": 2,
                                "expected_cpu_time_ms": 40,
                                "cpu_time_ms": {
                                    "min": 15,
                                    "p25": 20,
                                    "p50": 30,
                                    "p75": 60,
                                    "p99": 100,
                                    "max": 120,
                                    "mean": 40,
                                },
                                "trigger": "timer",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            rc = generate_config.main(
                [
                    "--distribution-json",
                    str(distribution),
                    "--output",
                    str(output),
                    "--top-n",
                    "2",
                    "--expected-time-percentile",
                    "p75",
                ]
            )

            self.assertEqual(rc, 0)
            payload = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema"], "cosmos.azure.top-functions-config")
            self.assertEqual(payload["summary"]["function_count"], 2)
            self.assertEqual(payload["summary"]["coverage"], 1.0)
            self.assertEqual(payload["functions"][0]["function_id"], "func-a")
            self.assertEqual(payload["functions"][0]["expected_time_ms"], 150)
            self.assertEqual(payload["functions"][0]["metadata"]["trigger"], "http")
            self.assertEqual(
                payload["functions"][0]["metadata"]["selection_reasons"],
                ["frequency", "total_cpu_time"],
            )


if __name__ == "__main__":
    unittest.main()
