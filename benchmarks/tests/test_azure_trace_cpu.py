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


measure_cpu = load_module(
    "measure_cpu_time_distribution",
    AZURE_TRACE_DIR / "measure_cpu_time_distribution.py",
)
generate_cpu = load_module(
    "generate_cpu_workload_stream",
    AZURE_TRACE_DIR / "generate_cpu_workload_stream.py",
)


class AzureTraceCpuTests(unittest.TestCase):
    def write_fixture_dataset(self, root: Path) -> Path:
        dataset = root / "azurefunctions-dataset2019"
        dataset.mkdir(parents=True, exist_ok=True)

        with (dataset / "invocations_per_function_md.anon.d01.csv").open(
            "w", encoding="utf-8", newline=""
        ) as fh:
            writer = csv.writer(fh)
            writer.writerow(["HashOwner", "HashApp", "HashFunction", "Trigger", "1", "2", "3", "4"])
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
            writer.writerow(["owner-a", "app-a", "func-a", "120", "3", "60", "400", "80", "100", "150", "300"])
            writer.writerow(["owner-b", "app-b", "func-b", "40", "2", "15", "120", "20", "30", "60", "100"])

        return dataset

    def test_measure_cpu_distribution_writes_compact_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = self.write_fixture_dataset(root)
            output = root / "cpu_distribution.json"

            rc = measure_cpu.main(
                [
                    "--dataset-dir",
                    str(dataset),
                    "--output",
                    str(output),
                    "--top-functions",
                    "4",
                ]
            )

            self.assertEqual(rc, 0)
            payload = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema"], "cosmos.azure.2019.cpu-distribution")
            self.assertEqual(payload["arrival_distribution"]["per_minute_counts"], [1, 1, 2, 1])
            self.assertEqual(payload["arrival_distribution"]["total_invocations"], 5)
            self.assertEqual(payload["cpu_time_distribution"]["total_functions"], 2)
            self.assertEqual(len(payload["profiles"]), 2)
            self.assertEqual(payload["profiles"][0]["workload"], "cpu_burst")
            self.assertGreater(payload["cpu_time_distribution"]["weighted_mean_cpu_time_ms"], 0.0)

    def test_generate_cpu_stream_from_distribution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = self.write_fixture_dataset(root)
            distribution = root / "cpu_distribution.json"
            output_dir = root / "stream"

            measure_cpu.main(
                [
                    "--dataset-dir",
                    str(dataset),
                    "--output",
                    str(distribution),
                ]
            )
            rc = generate_cpu.main(
                [
                    "--distribution-json",
                    str(distribution),
                    "--output-dir",
                    str(output_dir),
                    "--window-minutes",
                    "4",
                    "--arrival-mode",
                    "evenly-spaced",
                    "--seed",
                    "7",
                ]
            )

            self.assertEqual(rc, 0)
            replay = json.loads((output_dir / "replay.json").read_text(encoding="utf-8"))
            self.assertEqual(replay["schema"], "cosmos.azure.2019.cpu-stream")
            self.assertEqual(replay["summary"]["generated_invocations"], 5)
            invocations = replay["invocations"]
            self.assertEqual(len(invocations), 5)
            self.assertTrue(all(item["workload"] == "cpu_burst" for item in invocations))
            self.assertTrue(all(item["duration_ms"] > 0 for item in invocations))
            self.assertTrue(all(item["deadline_us"] >= item["duration_ms"] * 1000 for item in invocations))
            self.assertEqual(sorted(item["at_ms"] for item in invocations), [item["at_ms"] for item in invocations])


if __name__ == "__main__":
    unittest.main()
