"""Measure the real Spark Gold ETL and Global ANN training on nine provinces.

Run in the project Docker image (PySpark, PyTorch, pyarrow, psutil installed):
  python3 work_load.py --provinces "HCM,Hà Nội,..." --epochs 250
The profile is written only after every measured operation succeeds.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, str(ROOT / "code" / "lakehouse"))
sys.path.insert(0, str(ROOT / "code" / "training"))


def size_bytes(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*.parquet"))


def parquet_rows(path: Path, parquet) -> int:
    return sum(parquet.ParquetFile(p).metadata.num_rows for p in path.rglob("*.parquet"))


def province_part(root: Path, province: str) -> Path:
    part = root / f"province_name={province}"
    if not part.is_dir():
        raise FileNotFoundError(f"Missing partition: {part}")
    return part


def snapshot_cpu(psutil) -> tuple[float, float]:
    proc = psutil.Process()
    members = [proc] + proc.children(recursive=True)
    cpu, rss = 0.0, 0.0
    for member in members:
        try:
            times = member.cpu_times()
            cpu += times.user + times.system
            rss += member.memory_info().rss
        except psutil.Error:
            pass
    return cpu, rss / 1e6


def container_limits(psutil) -> tuple[float, float | None]:
    """Read effective Linux cgroup limits rather than the host CPU count."""
    allocated_cores = float(psutil.cpu_count(logical=True) or 1)
    cpu_max = Path("/sys/fs/cgroup/cpu.max")
    if cpu_max.is_file():
        quota, period = cpu_max.read_text().split()[:2]
        if quota != "max":
            allocated_cores = int(quota) / int(period)
    memory_limit_mb = None
    memory_max = Path("/sys/fs/cgroup/memory.max")
    if memory_max.is_file():
        limit = memory_max.read_text().strip()
        if limit != "max":
            memory_limit_mb = int(limit) / 1e6
    return allocated_cores, memory_limit_mb


def measured(psutil, operation):
    before_cpu, _ = snapshot_cpu(psutil)
    start = time.perf_counter()
    result = operation()
    wall = time.perf_counter() - start
    after_cpu, rss_mb = snapshot_cpu(psutil)
    cpu = after_cpu - before_cpu
    if cpu <= 0 or wall <= 0:
        raise RuntimeError("CPU or elapsed time could not be measured")
    return result, wall, cpu, rss_mb


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provinces", help="Comma-separated names of exactly nine provinces; default is nine largest TEDP partitions")
    parser.add_argument("--epochs", type=int, default=250)
    parser.add_argument("--patience", type=int, default=30)
    parser.add_argument("--output", type=Path, default=ROOT / "output/cloudsim_benchmark/workload_profile.csv")
    args = parser.parse_args()
    if args.epochs < 1 or args.patience < 1:
        parser.error("epochs and patience must be positive")

    import psutil
    import pyarrow.parquet as pq
    import torch
    from spark_session import get_spark_session
    from spark_feature_engineering import build_gold_features_dataframe
    from train_global_ann import (DEFAULT_BEST_CONFIG, build_spatial_prior_matrix,
                                  load_features_dataset, prepare_spatiotemporal_splits,
                                  train_ann_with_config)

    tedp = ROOT / "lakehouse_storage/silver/tedp_province"
    weather = ROOT / "lakehouse_storage/silver/weather_province"
    available = [p.name.split("=", 1)[1] for p in tedp.glob("province_name=*") if p.is_dir()]
    if args.provinces:
        provinces = [p.strip() for p in args.provinces.split(",")]
    else:
        provinces = sorted(available, key=lambda p: (-size_bytes(province_part(tedp, p)), p))[:9]
    if len(provinces) != 9 or len(set(provinces)) != 9 or any(p not in available for p in provinces):
        parser.error("select nine distinct provinces present in the Silver TEDP table")
    print("Measured provinces:", ", ".join(provinces), flush=True)

    rows = []
    allocated_cores, memory_limit_mb = container_limits(psutil)
    with tempfile.TemporaryDirectory(prefix="profile_gold_") as temp:
        gold = Path(temp) / "gold"
        spark = get_spark_session(app_name="Measured_CloudSim_Workload", master="local[4]",
                                  driver_memory="5g", shuffle_partitions="16")
        try:
            # Repeat the real transformation until JVM startup, Catalyst
            # initialization and generated-code warmup have passed. In the
            # observed run the first three executions were much slower despite
            # identical province row counts; the fourth reached steady state.
            warmup = Path(temp) / "warmup"
            for warmup_index in range(4):
                (build_gold_features_dataframe(spark, tedp, weather,
                                               province_name=provinces[0])
                 .write.mode("overwrite").parquet(str(warmup)))
                if parquet_rows(warmup, pq) == 0:
                    raise RuntimeError("Spark Gold warmup produced no rows")
                print(f"Completed real Spark Gold warmup {warmup_index + 1}/4", flush=True)
            for province in provinces:
                output = gold / f"province_name={province}"

                def run_spark():
                    df = build_gold_features_dataframe(spark, tedp, weather,
                                                       province_name=province)
                    df.write.mode("append").partitionBy("province_name").parquet(str(gold))
                    return parquet_rows(output, pq)

                count, wall, cpu, rss = measured(psutil, run_spark)
                if count == 0:
                    raise RuntimeError(f"Spark generated no Gold rows for {province}")
                input_mb = (size_bytes(province_part(tedp, province)) +
                            size_bytes(province_part(weather, province))) / 1e6
                input_rows = parquet_rows(province_part(tedp, province), pq)
                rows.append({"task_id": f"MAP_{len(rows)+1:02}", "task_stage": "MAP_DATA_PREP",
                             "province_name": province, "input_rows": input_rows, "output_rows": count,
                             "input_size_mb": round(input_mb, 6),
                             "output_size_mb": round(size_bytes(output)/1e6, 6),
                             "wall_time_sec": round(wall, 6), "total_cpu_time_sec": round(cpu, 6),
                             "observed_rss_mb": round(rss, 3), "measurement_source": "measured",
                             "operation": "spark_feature_engineering.build_gold_features_dataframe+parquet_write",
                             "host_cores": psutil.cpu_count(logical=True),
                             "allocated_cores": allocated_cores, "memory_limit_mb": memory_limit_mb,
                             "selected_provinces": "|".join(provinces)})
                print(f"{province}: {count} rows, {wall:.2f}s wall, {cpu:.2f}s CPU", flush=True)
        finally:
            spark.stop()

        def run_ann():
            feats, meta, continuous, _, columns = load_features_dataset(
                use_parquet=True, provinces=provinces, gold_features_dir=gold,
                allow_csv_fallback=False)
            feats = feats[feats.province_name.isin(provinces)].copy()
            meta = meta[meta.province_name.isin(provinces)].copy().sort_values("station_idx")
            ids = meta.station_id.astype(str).tolist()
            meta["station_idx"] = range(len(meta))
            index = dict(zip(ids, meta.station_idx))
            feats["station_idx"] = feats.station_id.astype(str).map(index)
            columns = [c for c in columns if not c.startswith("station_id_") or c[11:] in ids]
            split = prepare_spatiotemporal_splits(feats, meta, continuous, columns)
            prior = build_spatial_prior_matrix(meta)
            model, _, best_epoch, _, history = train_ann_with_config(
                split, prior, dict(DEFAULT_BEST_CONFIG), args.epochs, args.patience,
                torch.device("cpu"), verbose=False)
            if not history:
                raise RuntimeError("Global ANN produced no training epochs")
            del model
            return len(feats), best_epoch, len(history)

        (ann_rows, best_epoch, epochs_run), ann_wall, ann_cpu, ann_rss = measured(psutil, run_ann)
        if ann_rows != sum(row["output_rows"] for row in rows):
            raise RuntimeError("ANN training rows differ from the measured Spark Gold output")
        gold_mb = sum(size_bytes(province_part(gold, p)) for p in provinces) / 1e6
        rows.append({"task_id": "ANN_FULL_TRAIN", "task_stage": "REDUCE_ANN_FULL_TRAIN",
                     "province_name": "ALL_9_PROVINCES", "input_rows": ann_rows,
                     "output_rows": ann_rows, "input_size_mb": round(gold_mb, 6),
                     "output_size_mb": 0, "wall_time_sec": round(ann_wall, 6),
                     "total_cpu_time_sec": round(ann_cpu, 6), "observed_rss_mb": round(ann_rss, 3),
                     "measurement_source": "measured", "operation": f"global_ann_full_train:{epochs_run}_epochs:best_{best_epoch}",
                     "host_cores": psutil.cpu_count(logical=True),
                     "allocated_cores": allocated_cores, "memory_limit_mb": memory_limit_mb,
                     "selected_provinces": "|".join(provinces)})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote measured profile: {args.output}")


if __name__ == "__main__":
    main()
