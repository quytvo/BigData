"""Combine measured storage and CloudSim outputs into a CLO5 cost comparison.

The default deployment keeps a regional Cloud Storage bucket and all VMs in
asia-southeast1, with worker VMs in one zone using private addresses. Network
transfer is therefore free; a cross-zone sensitivity estimate is also shown.
"""
from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "output" / "cloudsim_benchmark"
HOT = 0.020             # coursework USD/decimal GB-month
WARM = 0.010
COLD = 0.0025
CROSS_ZONE = 0.01       # Google Cloud USD/GiB, same-region cross-zone VM traffic
MB_PER_GB = 1000.0
MB_PER_GIB = 1024.0 * 1024.0 / 1000.0
SCALES = {"1x": 1, "10x": 10, "50x": 50}
CONFIGS = {"single": "CSV", "balanced": "Parquet", "max_parallel": "Parquet"}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def number(row: dict[str, str], key: str) -> float:
    value = float(row[key])
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"Invalid {key}: {value}")
    return value


def bytes_under(path: Path) -> int:
    if not path.is_dir():
        raise FileNotFoundError(path)
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file() and not p.name.startswith("."))


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def analyze(storage_path: Path, simulation_path: Path, profile_path: Path,
            comparison_path: Path, lifecycle_path: Path,
            bronze_root: Path, weather_root: Path) -> tuple[list[dict], list[dict]]:
    storage = read_csv(storage_path)
    simulation = read_csv(simulation_path)
    profile = read_csv(profile_path)
    maps = [r for r in profile if r.get("task_stage") == "MAP_DATA_PREP"]
    if len(maps) != 9 or len({r["province_name"] for r in maps}) != 9:
        raise ValueError("Expected nine measured province tasks")
    provinces = {r["province_name"] for r in maps}
    if any(r.get("measurement_source") != "measured" for r in maps):
        raise ValueError("Gold size must come from measured MAP output")
    gold_mb = sum(number(r, "output_size_mb") for r in maps)
    # The monthly raw TEDP files and downloaded ERA5 files are the immutable
    # Bronze source archives. Derived Bronze CSVs are excluded to avoid copies.
    bronze_mb = (bytes_under(bronze_root / "tedp_raw_monthly") +
                 bytes_under(bronze_root / "era5_cds_blh")) / 1e6
    weather_mb = sum(bytes_under(weather_root / f"province_name={name}")
                     for name in provinces) / 1e6

    indexed_storage = {(r["scale"], r["format"].lower()): r for r in storage}
    indexed_sim = {(r["scale"], r["config"]): r for r in simulation}
    if len(indexed_storage) != 6 or len(indexed_sim) != 9:
        raise ValueError("Expected six storage and nine CloudSim scenarios")
    if any(r.get("measurement_source") != "measured" or
           r.get("engine", "").lower() != "spark" or
           set(r.get("selected_provinces", "").split("|")) != provinces
           for r in storage):
        raise ValueError("Storage data must be measured by Spark on the same nine provinces")
    if any(r.get("simulation_engine") != "CloudSim Plus 8.5.7" or
           r.get("region") != "asia-southeast1" for r in simulation):
        raise ValueError("Expected the Singapore CloudSim Plus result")

    comparison: list[dict] = []
    lifecycle: list[dict] = []
    for scale, factor in SCALES.items():
        parquet = indexed_storage[(scale, "parquet")]
        # Silver TEDP uses the physically measured Parquet table. Weather and
        # Gold are actual 1x bytes, linearly sized for hypothetical 10x/50x.
        silver_tedp_mb = number(parquet, "storage_size_mb")
        silver_weather_mb = weather_mb * factor
        gold_scaled_mb = gold_mb * factor
        bronze_scaled_mb = bronze_mb * factor
        hot_gb = (silver_tedp_mb + silver_weather_mb + gold_scaled_mb) / MB_PER_GB
        bronze_gb = bronze_scaled_mb / MB_PER_GB
        hot_month = hot_gb * HOT
        warm_month = bronze_gb * WARM
        cold_month = bronze_gb * COLD
        # The lifecycle is 1 month Nearline, then 13 months Archive. The
        # latter exceeds Archive's 365-day minimum even for 30-day months.
        lifecycle.extend([
            dict(scale=scale, layer="Silver+Gold", class_name="Hot Standard",
                 size_gb=round(hot_gb, 6), rate_usd_gb_month=HOT,
                 months=14, cost_usd=round(14 * hot_month, 8),
                 size_basis="measured TEDP Parquet; 1x weather/Gold bytes scaled"),
            dict(scale=scale, layer="Bronze raw", class_name="Warm Nearline",
                 size_gb=round(bronze_gb, 6), rate_usd_gb_month=WARM,
                 months=1, cost_usd=round(warm_month, 8),
                 size_basis="project-wide raw TEDP+ERA5 bytes scaled"),
            dict(scale=scale, layer="Bronze raw", class_name="Cold Archive",
                 size_gb=round(bronze_gb, 6), rate_usd_gb_month=COLD,
                 months=13, cost_usd=round(13 * cold_month, 8),
                 size_basis="project-wide raw TEDP+ERA5 bytes scaled"),
        ])
        previous: dict | None = None
        for config, expected_fmt in CONFIGS.items():
            sim = indexed_sim[(scale, config)]
            measured = indexed_storage[(scale, expected_fmt.lower())]
            if sim["storage_format"].lower() != expected_fmt.lower():
                raise ValueError(f"Format mismatch for {scale} {config}")
            scan_mb = number(measured, "bytes_scanned_mb") * 9
            # All nine queries read the regional bucket. Same-region GCS to
            # Compute Engine traffic is free. Same-zone VM traffic is free.
            # If workers are spread across zones, an upper bound assumes all
            # Gold output crosses one zone boundary on its way to ANN VM 0.
            gold_transfer_gib_upper = (gold_scaled_mb / MB_PER_GIB if config != "single" else 0.0)
            cross_zone_upper = gold_transfer_gib_upper * CROSS_ZONE
            compute = number(sim, "simulated_cost_usd")
            makespan = number(sim, "makespan_sec")
            if previous is None:
                marginal_usd_minute = 0.0
            else:
                saved_minutes = (float(previous["makespan_sec"]) - makespan) / 60
                marginal_usd_minute = ((compute - float(previous["compute_usd_per_run"])) /
                                       saved_minutes)
            monthly_storage = hot_month + cold_month  # steady state after month 1
            result = dict(
                scale=scale, config=config, vm_type=sim["vm_type"], vm_count=sim["vm_count"],
                format=expected_fmt, makespan_sec=sim["makespan_sec"],
                speedup=sim["speedup"], cpu_efficiency_pct=sim["cpu_efficiency_pct"],
                compute_usd_per_run=round(compute, 6),
                boot_compute_usd=round(number(sim, "boot_sec") *
                                       int(sim["vm_count"]) * number(sim, "price_usd_per_hour") / 3600, 8),
                min_60s_billing_binds=makespan < 60,
                marginal_compute_usd_per_minute_saved=round(marginal_usd_minute, 8),
                gcs_read_gb_per_run=round(scan_mb / MB_PER_GB, 6),
                same_region_same_zone_io_usd_per_run=0.0,
                cross_zone_gold_transfer_gib_upper=round(gold_transfer_gib_upper, 6),
                cross_zone_io_usd_upper=round(cross_zone_upper, 8),
                hot_standard_usd_per_month=round(hot_month, 8),
                bronze_nearline_month1_usd=round(warm_month, 8),
                bronze_archive_usd_per_month=round(cold_month, 8),
                steady_storage_usd_per_month=round(monthly_storage, 8),
                month1_one_run_total_usd=round(compute + hot_month + warm_month, 8),
                steady_month_one_run_total_usd=round(compute + monthly_storage, 8),
                storage_14_month_usd=round(14 * hot_month + warm_month + 13 * cold_month, 8),
            )
            comparison.append(result)
            previous = result
    write_csv(comparison_path, comparison)
    write_csv(lifecycle_path, lifecycle)
    return comparison, lifecycle


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--storage", type=Path, default=OUT / "storage_benchmark.csv")
    parser.add_argument("--simulation", type=Path, default=OUT / "cloudsim_results.csv")
    parser.add_argument("--profile", type=Path, default=OUT / "workload_profile.csv")
    parser.add_argument("--comparison", type=Path, default=OUT / "cost_comparison.csv")
    parser.add_argument("--lifecycle", type=Path, default=OUT / "storage_lifecycle.csv")
    parser.add_argument("--bronze-root", type=Path, default=ROOT / "lakehouse_storage/bronze")
    parser.add_argument("--weather-root", type=Path, default=ROOT / "lakehouse_storage/silver/weather_province")
    args = parser.parse_args()
    rows, _ = analyze(args.storage, args.simulation, args.profile,
                      args.comparison, args.lifecycle, args.bronze_root, args.weather_root)
    for row in rows:
        print(f"{row['scale']:3} {row['config']:12} {float(row['makespan_sec']):9.1f}s "
              f"compute=${row['compute_usd_per_run']:.6f} "
              f"storage=${row['steady_storage_usd_per_month']:.6f}/month")
    print(args.comparison)
    print(args.lifecycle)


if __name__ == "__main__":
    main()
