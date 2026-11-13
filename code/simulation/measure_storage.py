"""Measure CSV full scans and partitioned Parquet reads on the profile's nine provinces.

The 10x and 50x datasets replicate the observed province records, matching the
repository's existing multi-scale storage benchmark method. Every latency is
measured by running a real Spark query against those physical files.
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "code/lakehouse"))
OUT = ROOT / "output/cloudsim_benchmark"


def profile_provinces(path: Path) -> list[str]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    provinces = [r["province_name"] for r in rows if r.get("task_stage") == "MAP_DATA_PREP"]
    if len(provinces) != 9 or len(set(provinces)) != 9:
        raise ValueError("A measured nine-province workload profile is required")
    if "Hà Nội" not in provinces:
        raise ValueError("Storage benchmark requires Hà Nội as its representative query province")
    return provinces


def benchmark_spark_csv(spark, csv_path: Path, expected_rows: int,
                        target_province: str, expected_target_rows: int) -> dict:
    """Measure the full CSV scan with the same PySpark engine as Gold ETL."""
    from pyspark.sql import functions as F

    data = spark.read.option("header", "true").csv(str(csv_path))
    start = time.perf_counter()
    aggregated = (data.withColumn("pm25", F.col("pm25").cast("double"))
                  .filter(F.col("pm25").isNotNull() & (F.col("pm25") >= 0))
                  .groupBy("province_name")
                  .agg(F.count("*").alias("row_count"), F.avg("pm25").alias("mean_pm25")))
    counts = aggregated.collect()
    etl_sec = time.perf_counter() - start
    if sum(row.row_count for row in counts) != expected_rows:
        raise ValueError("Spark CSV scan did not cover every measured Silver row")
    start = time.perf_counter()
    province_count = data.filter(F.col("province_name") == target_province).count()
    query_ms = (time.perf_counter() - start) * 1000.0
    if province_count != expected_target_rows:
        raise ValueError(f"Spark CSV query found an unexpected {target_province} row count")
    size_mb = csv_path.stat().st_size / 1e6
    return {"storage_size_mb": round(size_mb, 2),
            "etl_runtime_sec": round(etl_sec, 4),
            "query_1prov_latency_ms": round(query_ms, 2),
            "bytes_scanned_mb": round(size_mb, 2)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, default=OUT / "workload_profile.csv")
    parser.add_argument("--output", type=Path, default=OUT / "storage_benchmark.csv")
    args = parser.parse_args()
    provinces = profile_provinces(args.profile)
    target_province = "Hà Nội"

    import pandas as pd
    from spark_session import get_spark_session
    from benchmark_spark_vs_pandas import (
        benchmark_spark_workload,
        replicate_csv_dataset, replicate_parquet_dataset,
    )

    source = ROOT / "lakehouse_storage/silver/tedp_province"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    spark = get_spark_session(app_name="NineProvince_Storage_Benchmark",
                              driver_memory="5g", shuffle_partitions="16")
    try:
        with tempfile.TemporaryDirectory(prefix="storage_", dir=args.output.parent) as temp:
            temp = Path(temp)
            base_parquet = temp / "parquet_1x"
            base_parquet.mkdir()
            base_csv = temp / "csv_1x.csv"
            for province in provinces:
                part = source / f"province_name={province}"
                if not part.is_dir():
                    raise FileNotFoundError(part)
                shutil.copytree(part, base_parquet / part.name)
            # This CSV is materialized from the actual nine-province Silver
            # Parquet table, rather than invented or downloaded records.
            frame = pd.read_parquet(base_parquet)
            expected_1x = len(frame)
            if expected_1x == 0 or frame.province_name.nunique() != 9:
                raise ValueError("Selected Silver table does not contain nine populated provinces")
            target_1x = int((frame.province_name == target_province).sum())
            frame.to_csv(base_csv, index=False)
            del frame

            for label, factor in (("1x", 1), ("10x", 10), ("50x", 50)):
                parquet = base_parquet if factor == 1 else temp / f"parquet_{label}"
                csv_path = base_csv if factor == 1 else temp / f"csv_{label}.csv"
                if factor != 1:
                    replicate_parquet_dataset(base_parquet, parquet, factor)
                    replicate_csv_dataset(base_csv, csv_path, factor)
                expected = expected_1x * factor
                measured = {
                    "Parquet": benchmark_spark_workload(spark, parquet, label, expected),
                    "CSV": benchmark_spark_csv(spark, csv_path, expected,
                                               target_province, target_1x * factor),
                }
                target_partition = next((part for part in parquet.iterdir()
                                         if part.is_dir() and part.name.startswith("province_name=")
                                         and unquote(part.name.split("=", 1)[1]) == target_province), None)
                if target_partition is None:
                    raise FileNotFoundError(f"No Parquet partition for {target_province}")
                measured["Parquet"]["bytes_scanned_mb"] = round(
                    sum(part.stat().st_size for part in target_partition.rglob("*.parquet")) / 1e6, 2)
                for fmt, metrics in measured.items():
                    rows.append({
                        "scale": label, "engine": "Spark",
                        "format": fmt, "row_count": expected,
                        "storage_size_mb": metrics["storage_size_mb"],
                        "etl_runtime_sec": metrics["etl_runtime_sec"],
                        "query_1prov_latency_ms": metrics["query_1prov_latency_ms"],
                        "bytes_scanned_mb": metrics["bytes_scanned_mb"],
                        "measurement_source": "measured",
                        "query_province": target_province,
                        "selected_provinces": "|".join(provinces),
                    })
                print(f"{label}: {expected} rows, CSV and Parquet measured", flush=True)
                if factor != 1:
                    shutil.rmtree(parquet)
                    csv_path.unlink()
    finally:
        spark.stop()

    with args.output.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(args.output)


if __name__ == "__main__":
    main()
