"""Run measured province and ANN work in CloudSim Plus (or analytic mode).

The profile is measured on a reference host. CPU seconds are core-seconds;
ideal core scaling is an explicit simulation assumption, not a GCP measurement.
"""
from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "output" / "cloudsim_benchmark"
VM = {
    "single": ("e2-standard-2", 2, 8, 0.0736, 1),
    "balanced": ("e2-standard-4", 4, 16, 0.1472, 3),
    "max_parallel": ("c2-standard-8", 8, 32, 0.4578, 9),
}
CONFIG_NAMES = {
    "single": "Config 1 (Single VM)",
    "balanced": "Config 2 (Balanced Cluster)",
    "max_parallel": "Config 3 (Max-Parallel Cluster)",
}


@dataclass(frozen=True)
class Task:
    province: str
    cpu_seconds: float
    output_mb: float
    input_rows: int = 0


def positive(row: dict, field: str) -> float:
    try:
        value = float(row[field])
    except (KeyError, ValueError) as exc:
        raise ValueError(f"Invalid {field}: {row}") from exc
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"Invalid {field}: {row}")
    return value


def read_profile(path: Path) -> tuple[list[Task], float]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    maps = [row for row in rows if row.get("task_stage") == "MAP_DATA_PREP"]
    if len(maps) != 9 or len({row["province_name"] for row in maps}) != 9:
        raise ValueError("workload_profile.csv must contain exactly nine distinct MAP_DATA_PREP provinces")
    ann = [row for row in rows if row.get("task_stage") == "REDUCE_ANN_FULL_TRAIN"]
    if len(ann) != 1:
        raise ValueError("workload_profile.csv must contain one measured REDUCE_ANN_FULL_TRAIN task")
    if any(row.get("measurement_source") != "measured" for row in maps + ann):
        raise ValueError("Profile lacks measured provenance; run work_load.py first")
    if len({row.get("selected_provinces") for row in maps + ann}) != 1:
        raise ValueError("Profile mixes measurements from different province selections")
    if set(ann[0]["selected_provinces"].split("|")) != {row["province_name"] for row in maps}:
        raise ValueError("Global ANN and MAP tasks must cover the same nine provinces")
    tasks = [Task(row["province_name"], positive(row, "total_cpu_time_sec"),
                  positive(row, "output_size_mb"), int(positive(row, "input_rows")))
             for row in maps]
    if any(task.input_rows <= 0 for task in tasks):
        raise ValueError("Every measured MAP task must have input rows")
    if int(positive(ann[0], "input_rows")) != sum(int(positive(row, "output_rows")) for row in maps):
        raise ValueError("ANN training rows differ from the measured MAP Gold output")
    return tasks, positive(ann[0], "total_cpu_time_sec")


def read_storage(path: Path, provinces: set[str], expected_rows: int | None = None) -> dict[tuple[str, str], dict]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    if not rows or any(r.get("measurement_source") != "measured" or
                       r.get("engine", "").lower() != "spark" or
                       set(r.get("selected_provinces", "").split("|")) != provinces
                       for r in rows):
        raise ValueError("storage_benchmark.csv must contain Spark measurements on the same nine provinces")
    query_provinces = {r.get("query_province") for r in rows}
    if len(query_provinces) != 1 or not query_provinces <= provinces:
        raise ValueError("Storage query measurements must target one of the selected provinces")
    table = {(r["scale"], r["format"].lower()): r for r in rows}
    for scale in ("1x", "10x", "50x"):
        for fmt in ("csv", "parquet"):
            if (scale, fmt) not in table:
                raise ValueError(f"Missing storage benchmark row: {scale} {fmt}")
            if expected_rows is not None:
                observed = int(positive(table[(scale, fmt)], "row_count"))
                factor = int(scale[:-1])
                if observed != expected_rows * factor:
                    raise ValueError(f"Storage row count differs from the MAP workload at {scale} {fmt}")
    return table


def schedule(tasks: list[Task], cores: float, n_vms: int, scale: int,
              io_seconds: float, input_mb: float,
              network_mbps: float) -> tuple[float, list[float], float]:
    # LPT: longest measured CPU demand first, with deterministic province tie-break.
    loads = [0.0] * n_vms
    for task in sorted(tasks, key=lambda t: (-t.cpu_seconds, t.province)):
        node = min(range(n_vms), key=lambda i: (loads[i], i))
        # Shared-storage input crosses the network for every VM. Gold output
        # crosses it only when the worker differs from the ANN reducer (VM 0).
        ingress = input_mb * 8.0 / network_mbps
        reducer_transfer = (task.output_mb * scale * 8.0 / network_mbps
                            if node != 0 else 0.0)
        loads[node] += (task.cpu_seconds * scale / cores + io_seconds + ingress
                        + reducer_transfer)
    return max(loads), loads, sum(t.cpu_seconds * scale for t in tasks)


def run(profile: Path, storage: Path, output: Path, network_mbps: float,
        boot_seconds: float, c2_core_factor: float) -> list[dict]:
    if min(network_mbps, c2_core_factor) <= 0 or boot_seconds < 0:
        raise ValueError("network_mbps and c2_core_factor must be positive; boot_seconds nonnegative")
    tasks, ann_cpu = read_profile(profile)
    io = read_storage(storage, {task.province for task in tasks},
                      expected_rows=sum(task.input_rows for task in tasks))
    results = []
    for scale_name, factor in (("1x", 1), ("10x", 10), ("50x", 50)):
        baseline = None
        for key in ("single", "balanced", "max_parallel"):
            sku, cores, ram_gb, usd_hour, n_vms = VM[key]
            fmt = "csv" if key == "single" else "parquet"
            row = io[(scale_name, fmt)]
            query_sec = positive(row, "query_1prov_latency_ms") / 1000.0
            scanned_mb = positive(row, "bytes_scanned_mb")
            effective_cores = cores * (c2_core_factor if key == "max_parallel" else 1.0)
            # A CSV query scans the whole file for every province; Parquet uses its
            # observed partition-pruned query latency. Both are measured at this scale.
            map_end, loads, map_cpu = schedule(tasks, effective_cores, n_vms, factor,
                                                query_sec, scanned_mb, network_mbps)
            ann_seconds = ann_cpu * factor / effective_cores
            makespan = boot_seconds + map_end + ann_seconds
            if baseline is None:
                baseline = makespan
            # ANN is a global reduction on the first VM after all maps complete.
            active = [boot_seconds + load for load in loads]
            active[0] = makespan
            billed = [max(60.0, math.ceil(seconds)) for seconds in active]
            cost = sum(seconds * usd_hour / 3600.0 for seconds in billed)
            # Core occupancy excludes I/O, transfer and boot. The c2 factor
            # changes required core time, while the denominator stays physical.
            cpu_eff = ((map_cpu + ann_cpu * factor) /
                       (c2_core_factor if key == "max_parallel" else 1.0) /
                       (sum(active) * cores))
            results.append({
                "scale": scale_name, "config": key, "config_name": CONFIG_NAMES[key],
                "region": "asia-southeast1", "vm_type": sku,
                "vm_count": n_vms, "vcpus_per_vm": cores, "ram_gb_per_vm": ram_gb,
                "price_usd_per_hour": usd_hour, "makespan_sec": round(makespan, 4),
                "speedup": round(baseline / makespan, 4),
                "cpu_efficiency_pct": round(100 * cpu_eff, 4),
                "simulated_cost_usd": round(cost, 6),
                "boot_sec": boot_seconds, "network_mbps": network_mbps,
                "c2_core_factor": c2_core_factor, "storage_format": fmt,
                "measured_query_sec_per_province": round(query_sec, 4),
                 "measured_scanned_mb_per_province": round(scanned_mb, 4),
                "ann_cpu_seconds_scaled": round(ann_cpu * factor, 4),
                 "simulation_engine": "analytic estimate",
            })
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, default=OUT / "workload_profile.csv")
    parser.add_argument("--storage", type=Path, default=OUT / "storage_benchmark.csv")
    parser.add_argument("--output", type=Path,
                        help="Default: cloudsim_results.csv for CloudSim, analytic_results.csv for analytic mode")
    parser.add_argument("--network-mbps", type=float, default=1000.0)
    parser.add_argument("--boot-seconds", type=float, default=30.0)
    parser.add_argument("--c2-core-factor", type=float, default=1.0,
                        help="Measured c2/e2 per-core throughput ratio; 1 means equal throughput")
    parser.add_argument("--engine", choices=("cloudsim", "analytic"), default="cloudsim",
                        help="Run Java CloudSim Plus by default; analytic is the earlier Python estimate")
    args = parser.parse_args()
    output = args.output or OUT / ("cloudsim_results.csv" if args.engine == "cloudsim"
                                 else "analytic_results.csv")
    if args.engine == "cloudsim":
        from run_cloudsim_plus import run_cloudsim
        rows = run_cloudsim(args.profile, args.storage, output, args.network_mbps,
                            args.boot_seconds, args.c2_core_factor)
    else:
        rows = run(args.profile, args.storage, output, args.network_mbps,
                   args.boot_seconds, args.c2_core_factor)
    for row in rows:
        print(f"{row['scale']:3} {row['config']:12} {row['makespan_sec']:11.2f}s "
              f"speedup={row['speedup']:.2f} cost=${row['simulated_cost_usd']:.6f}")
    print(output)


if __name__ == "__main__":
    main()
