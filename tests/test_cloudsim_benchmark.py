"""End-to-end checks for measured-profile validation and VM billing."""
import csv
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "code/simulation/run_cloudsim_benchmark.py"
SPEC = importlib.util.spec_from_file_location("run_cloudsim_benchmark", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)
run = MODULE.run
sys.path.insert(0, str(SCRIPT.parent))
from run_cloudsim_plus import container_path


class CloudSimBenchmarkTest(unittest.TestCase):
    def test_nine_measured_tasks_all_configs_and_minute_billing(self):
        with tempfile.TemporaryDirectory(dir=SCRIPT.parents[2] / "output") as temp:
            folder = Path(temp)
            profile = folder / "profile.csv"
            storage = folder / "storage.csv"
            output = folder / "results.csv"
            names = [f"Province_{i}" for i in range(9)]
            with profile.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=["task_stage", "province_name",
                    "total_cpu_time_sec", "output_size_mb", "input_rows", "output_rows", "measurement_source",
                    "selected_provinces"])
                writer.writeheader()
                for i, name in enumerate(names):
                    writer.writerow(dict(task_stage="MAP_DATA_PREP", province_name=name,
                        total_cpu_time_sec=10 + i, output_size_mb=1,
                        input_rows=100, output_rows=100,
                        measurement_source="measured", selected_provinces="|".join(names)))
                writer.writerow(dict(task_stage="REDUCE_ANN_FULL_TRAIN",
                    province_name="ALL_9_PROVINCES", total_cpu_time_sec=20,
                    output_size_mb=0, input_rows=900, output_rows=900,
                    measurement_source="measured",
                    selected_provinces="|".join(names)))
            with storage.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=["scale", "format", "engine",
                    "query_1prov_latency_ms", "bytes_scanned_mb", "row_count",
                    "measurement_source", "selected_provinces", "query_province"])
                writer.writeheader()
                for scale in ("1x", "10x", "50x"):
                    for fmt in ("CSV", "Parquet"):
                        writer.writerow(dict(scale=scale, format=fmt, engine="Spark",
                                             query_1prov_latency_ms=100,
                                             bytes_scanned_mb=10,
                                             row_count=900 * int(scale[:-1]),
                                             measurement_source="measured",
                                             selected_provinces="|".join(names),
                                             query_province=names[0]))
            rows = run(profile, storage, output, 1000, 30, 1)
            self.assertEqual(len(rows), 9)
            self.assertTrue(output.exists())
            self.assertEqual(rows[0]["speedup"], 1)
            self.assertTrue(all(row["simulation_engine"] == "analytic estimate" for row in rows))
            self.assertLess(rows[1]["makespan_sec"], rows[0]["makespan_sec"])
            self.assertLess(rows[2]["makespan_sec"], rows[1]["makespan_sec"])
            self.assertGreaterEqual(rows[2]["simulated_cost_usd"],
                                    9 * 60 * 0.4578 / 3600)
            self.assertEqual({r["scale"] for r in rows}, {"1x", "10x", "50x"})
            with self.assertRaisesRegex(ValueError, "Storage row count differs"):
                MODULE.read_storage(storage, set(names), expected_rows=901)

    def test_network_transfer_only_crosses_vms_for_gold_output(self):
        tasks = [MODULE.Task("A", 10, 10)]
        _, single_loads, _ = MODULE.schedule(tasks, 2, 1, 1, 0, 5, 10)
        self.assertEqual(single_loads, [9])  # 5 CPU + 4 shared-storage ingress
        _, cluster_loads, _ = MODULE.schedule(
            [MODULE.Task("A", 10, 10), MODULE.Task("B", 10, 10)],
            2, 2, 1, 0, 5, 10)
        self.assertEqual(cluster_loads, [9, 17])  # remote worker adds 8 s Gold transfer

    def test_unmeasured_profile_rejected(self):
        with tempfile.TemporaryDirectory(dir=SCRIPT.parents[2] / "output") as temp:
            path = Path(temp) / "old.csv"
            names = [f"Province_{i}" for i in range(9)]
            with path.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=["task_stage", "province_name",
                    "total_cpu_time_sec", "output_size_mb", "input_rows", "output_rows",
                    "measurement_source", "selected_provinces"])
                writer.writeheader()
                for name in names:
                    writer.writerow(dict(task_stage="MAP_DATA_PREP", province_name=name,
                        total_cpu_time_sec=10, output_size_mb=1, input_rows=100,
                        output_rows=100, measurement_source="synthetic",
                        selected_provinces="|".join(names)))
                writer.writerow(dict(task_stage="REDUCE_ANN_FULL_TRAIN",
                    province_name="ALL_9_PROVINCES", total_cpu_time_sec=100,
                    output_size_mb=0, input_rows=900, output_rows=900,
                    measurement_source="synthetic", selected_provinces="|".join(names)))
            with self.assertRaisesRegex(ValueError, "measured provenance"):
                MODULE.read_profile(path)

    def test_pandas_storage_timings_rejected(self):
        with tempfile.TemporaryDirectory(dir=SCRIPT.parents[2] / "output") as temp:
            path = Path(temp) / "storage.csv"
            path.write_text(
                "scale,format,engine,measurement_source,selected_provinces\n"
                "1x,CSV,Pandas,measured,A|B\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Spark measurements"):
                MODULE.read_storage(path, {"A", "B"})

    def test_cloudsim_custom_output_resolves_inside_repository(self):
        self.assertEqual(
            container_path(SCRIPT.parents[2] / "output/custom/results.csv"),
            "/workspace/output/custom/results.csv",
        )
        with self.assertRaises(ValueError):
            container_path(Path(tempfile.gettempdir()) / "outside-results.csv")


if __name__ == "__main__":
    unittest.main()
