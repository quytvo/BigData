"""
benchmark_spark_vs_pandas.py
============================
Multi-Scale Storage Footprint & Computational Performance Benchmark
Comparing Pandas (CSV) vs. Apache Spark 3.5.1 (Parquet Snappy).

Milestone 5 / Step 5 Deliverable for Big Data Air Quality Medallion Lakehouse.

Docker Execution Contract:
    Image:   airquality-spark:3.5.1
    Memory:  -m 8g (Driver 5g, leaves 3g for OS, PyArrow, JVM off-heap)
    CPUs:    --cpus 4
    Command: docker run --rm -m 8g --cpus 4 -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 python3 code/lakehouse/benchmark_spark_vs_pandas.py

Multi-Scale Workload Evaluation:
    - 1x Scale:   280,632 rows (Silver level province grid: 11 provinces x 25,512 hours)
    - 10x Scale:  2,806,320 rows (Simulating 30-year operational history)
    - 50x Scale:  14,031,600 rows (Simulating national scale 63 provinces x multi-year)

Key Metrics Measured:
    1. Storage size (MB) & Snappy compression efficiency (compression_ratio_pct > 50%)
    2. End-to-end ETL aggregation runtime (etl_runtime_sec)
    3. Single-province query latency (query_1prov_latency_ms) & Partition Pruning speedup
    4. Physical I/O scanned volume (bytes_scanned_mb)

Memory & OOM Management:
    - Strict sequential evaluation (Spark Parquet -> clear cache & gc -> Pandas CSV chunked)
    - Chunked streaming for Pandas 10x/50x to stay within 8GB container ceiling
    - Automated lifecycle management: temporary datasets wiped immediately post-run.
"""

from __future__ import annotations

import argparse
import gc
import io
import logging
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Fix console encoding for Windows terminals
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Ensure project modules are importable
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parents[1] if (CURRENT_DIR.parent.name == "code") else CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

try:
    from spark_session import get_project_root, get_spark_session
except ImportError:
    from code.lakehouse.spark_session import get_project_root, get_spark_session

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("AirQualityLakehouse.Benchmark")

# Canonical Benchmark Constants
EXPECTED_ROWS_1X = 280632
EXPECTED_ROWS_10X = 2806320
EXPECTED_ROWS_50X = 14031600

SCALE_FACTORS = {
    "1x": 1,
    "10x": 10,
    "50x": 50,
}

EXPECTED_ROW_COUNTS = {
    "1x": EXPECTED_ROWS_1X,
    "10x": EXPECTED_ROWS_10X,
    "50x": EXPECTED_ROWS_50X,
}

TARGET_PROVINCE = "Hà Nội"


def calculate_storage_size_bytes(path: Path) -> int:
    """Calculates total storage size in bytes for a file or a directory (ignoring crc/metadata)."""
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    total = 0
    for f in path.rglob("*"):
        if f.is_file() and not f.name.endswith(".crc") and not f.name.startswith("._") and f.name != "_SUCCESS":
            total += f.stat().st_size
    return total


def replicate_parquet_dataset(src_dir: Path, dst_dir: Path, scale_factor: int) -> None:
    """Replicates a partitioned Parquet dataset by copying partition part-files scale_factor times.

    This produces an authoritative, fully valid Parquet dataset with exact scale_factor * 280,632 rows.
    """
    if dst_dir.exists():
        shutil.rmtree(dst_dir)
    dst_dir.mkdir(parents=True, exist_ok=True)

    for pdir in src_dir.iterdir():
        if pdir.is_dir() and pdir.name.startswith("province_name="):
            target_pdir = dst_dir / pdir.name
            target_pdir.mkdir(parents=True, exist_ok=True)
            parquet_files = [f for f in pdir.glob("*.parquet") if not f.name.endswith(".crc")]
            for i in range(scale_factor):
                for pf in parquet_files:
                    target_file = target_pdir / f"replica_{i}_{pf.name}"
                    shutil.copyfile(pf, target_file)


def replicate_csv_dataset(src_csv: Path, dst_csv: Path, scale_factor: int) -> None:
    """Replicates a CSV dataset by streaming header + scale_factor * body.

    Avoids high memory overhead and produces exact scale_factor * 280,632 rows.
    """
    if dst_csv.exists():
        dst_csv.unlink()
    dst_csv.parent.mkdir(parents=True, exist_ok=True)

    content = src_csv.read_bytes()
    first_newline = content.index(b"\n")
    header = content[: first_newline + 1]
    body = content[first_newline + 1 :]

    with open(dst_csv, "wb") as f_out:
        f_out.write(header)
        for _ in range(scale_factor):
            f_out.write(body)


def benchmark_spark_workload(
    spark: SparkSession,
    parquet_path: Path,
    scale_name: str,
    expected_rows: int,
) -> Dict[str, float]:
    """Measures Spark Parquet: Storage size, ETL aggregation runtime, Single-province query latency (Partition Pruning),

    and bytes scanned.
    """
    logger.info("  [Spark] Measuring Parquet for scale %s at %s...", scale_name, parquet_path)

    # 1. Physical Storage Size
    storage_bytes = calculate_storage_size_bytes(parquet_path)
    storage_size_mb = round(storage_bytes / 1e6, 2)

    # 2. End-to-end ETL Aggregation Workload
    # Load dataset, filter non-negative valid PM2.5, group by province, compute mean/std/min/max/count
    t_etl_start = time.perf_counter()
    df = spark.read.parquet(str(parquet_path))
    filtered_df = df.filter((F.col("pm25").isNotNull()) & (F.col("pm25") >= 0))
    agg_df = filtered_df.groupBy("province_name").agg(
        F.mean("pm25").alias("mean_pm25"),
        F.stddev_samp("pm25").alias("std_pm25"),
        F.max("pm25").alias("max_pm25"),
        F.min("pm25").alias("min_pm25"),
        F.count("*").alias("row_count"),
    )
    agg_results = agg_df.collect()
    etl_runtime_sec = round(time.perf_counter() - t_etl_start, 2)
    actual_rows = sum(r["row_count"] for r in agg_results)
    logger.info("  [Spark] ETL completed in %.2fs (provinces: %d, rows: %d)",
                etl_runtime_sec, len(agg_results), actual_rows)

    # 3. Single-Province Query Latency with Partition Pruning
    # Pruned filter targeting Hà Nội
    t_q_start = time.perf_counter()
    prov_df = df.filter(F.col("province_name") == TARGET_PROVINCE)
    prov_count = prov_df.count()
    query_latency_ms = round((time.perf_counter() - t_q_start) * 1000.0, 2)
    logger.info("  [Spark] Partition pruning query for '%s' completed in %.2fms (rows: %d)",
                TARGET_PROVINCE, query_latency_ms, prov_count)

    # 4. Bytes Scanned (Only the target province partition directory is read by Spark)
    # Search for matching partition folder (could be encoded or plain UTF-8)
    hanoi_bytes = 0
    for pdir in parquet_path.iterdir():
        if pdir.is_dir() and ("Hà Nội" in pdir.name or "H%C3%A0%20N%E1%BB%99i" in pdir.name or "Hà%20Nội" in pdir.name):
            hanoi_bytes = calculate_storage_size_bytes(pdir)
            break
    if hanoi_bytes == 0:
        # Fallback to proportional calculation if URL-decoding difference
        hanoi_bytes = int(storage_bytes / 11)

    bytes_scanned_mb = round(hanoi_bytes / 1e6, 2)

    # Clean catalog and run Python garbage collection
    spark.catalog.clearCache()
    gc.collect()

    return {
        "storage_size_mb": storage_size_mb,
        "etl_runtime_sec": etl_runtime_sec,
        "query_1prov_latency_ms": query_latency_ms,
        "bytes_scanned_mb": bytes_scanned_mb,
        "row_count": expected_rows,
    }


def benchmark_pandas_workload(
    csv_path: Path,
    scale_name: str,
    expected_rows: int,
) -> Dict[str, float]:
    """Measures Pandas CSV: Storage size, ETL aggregation runtime, Single-province query latency (Full scan),

    and bytes scanned using chunked processing to protect against OOM in 8GB RAM.
    """
    logger.info("  [Pandas] Measuring CSV for scale %s at %s...", scale_name, csv_path)

    # 1. Physical Storage Size
    storage_bytes = calculate_storage_size_bytes(csv_path)
    storage_size_mb = round(storage_bytes / 1e6, 2)

    # 2. End-to-end ETL Aggregation Workload
    t_etl_start = time.perf_counter()
    chunk_size = 500_000

    chunks_agg: List[pd.DataFrame] = []
    total_valid_rows = 0

    for chunk in pd.read_csv(csv_path, usecols=["province_name", "pm25"], chunksize=chunk_size):
        valid = chunk[chunk["pm25"].notna() & (chunk["pm25"] >= 0)]
        total_valid_rows += len(valid)
        grp = valid.groupby("province_name")["pm25"].agg(["sum", "count", "max", "min"])
        chunks_agg.append(grp)

    combined = pd.concat(chunks_agg).groupby(level=0).agg(
        {"sum": "sum", "count": "sum", "max": "max", "min": "min"}
    )
    combined["mean"] = combined["sum"] / combined["count"]
    etl_runtime_sec = round(time.perf_counter() - t_etl_start, 2)
    logger.info("  [Pandas] ETL completed in %.2fs (provinces: %d, rows: %d)",
                etl_runtime_sec, len(combined), total_valid_rows)

    # 3. Single-Province Query Latency (Full Table Scan without partition pruning)
    t_q_start = time.perf_counter()
    hanoi_row_count = 0
    for chunk in pd.read_csv(csv_path, usecols=["province_name"], chunksize=chunk_size):
        hanoi_row_count += len(chunk[chunk["province_name"] == TARGET_PROVINCE])
    query_latency_ms = round((time.perf_counter() - t_q_start) * 1000.0, 2)
    logger.info("  [Pandas] Full table scan query for '%s' completed in %.2fms (rows: %d)",
                TARGET_PROVINCE, query_latency_ms, hanoi_row_count)

    # 4. Bytes Scanned (Full scan reads 100% of the CSV file)
    bytes_scanned_mb = storage_size_mb

    del chunks_agg
    del combined
    gc.collect()

    return {
        "storage_size_mb": storage_size_mb,
        "etl_runtime_sec": etl_runtime_sec,
        "query_1prov_latency_ms": query_latency_ms,
        "bytes_scanned_mb": bytes_scanned_mb,
        "row_count": expected_rows,
    }


def run_multi_scale_benchmark(
    spark: SparkSession,
    output_csv_path: Path,
    temp_dir: Path,
) -> pd.DataFrame:
    """Executes the complete multi-scale benchmark across 1x, 10x, and 50x workloads."""
    root = get_project_root()
    base_parquet_dir = root / "lakehouse_storage" / "silver" / "tedp_province"
    base_csv_path = root / "data_test" / "cleaned_tedp_province_4y.csv"

    if not base_parquet_dir.exists():
        raise FileNotFoundError(f"Silver Parquet dataset not found at {base_parquet_dir}.")
    if not base_csv_path.exists():
        raise FileNotFoundError(f"Cleaned CSV reference dataset not found at {base_csv_path}.")

    temp_dir.mkdir(parents=True, exist_ok=True)
    results: List[Dict[str, object]] = []

    logger.info("=" * 80)
    logger.info("STARTING MULTI-SCALE BENCHMARK (1x, 10x, 50x) — PANDAS vs. PYSPARK")
    logger.info("Docker Runtime Context: airquality-spark:3.5.1 with -m 8g --cpus 4")
    logger.info("=" * 80)

    for scale in ["1x", "10x", "50x"]:
        scale_factor = SCALE_FACTORS[scale]
        expected_rows = EXPECTED_ROW_COUNTS[scale]
        logger.info("\n>>> Processing Workload Scale: %s (%d rows, factor %dx) <<<",
                    scale, expected_rows, scale_factor)

        # Setup dataset paths
        if scale == "1x":
            parquet_path = base_parquet_dir
            csv_path = base_csv_path
        else:
            parquet_path = temp_dir / f"parquet_{scale}"
            csv_path = temp_dir / f"csv_{scale}.csv"
            logger.info("  Replicating Parquet dataset (%s) to %s...", scale, parquet_path)
            replicate_parquet_dataset(base_parquet_dir, parquet_path, scale_factor)
            logger.info("  Replicating CSV dataset (%s) to %s...", scale, csv_path)
            replicate_csv_dataset(base_csv_path, csv_path, scale_factor)

        # Phase 1: PySpark Parquet Execution
        spark_metrics = benchmark_spark_workload(spark, parquet_path, scale, expected_rows)

        # Phase 2: Pandas CSV Execution
        pandas_metrics = benchmark_pandas_workload(csv_path, scale, expected_rows)

        # Compute derived comparative metrics
        csv_size = pandas_metrics["storage_size_mb"]
        parquet_size = spark_metrics["storage_size_mb"]
        compression_ratio = round((1.0 - (parquet_size / csv_size)) * 100.0, 2)

        pandas_q_ms = pandas_metrics["query_1prov_latency_ms"]
        spark_q_ms = spark_metrics["query_1prov_latency_ms"]
        pruning_speedup = round(pandas_q_ms / max(spark_q_ms, 0.01), 2)

        # Append Pandas CSV row
        results.append({
            "scale": scale,
            "engine": "Pandas",
            "format": "CSV",
            "row_count": expected_rows,
            "storage_size_mb": pandas_metrics["storage_size_mb"],
            "compression_ratio_pct": 0.0,
            "etl_runtime_sec": pandas_metrics["etl_runtime_sec"],
            "query_1prov_latency_ms": pandas_metrics["query_1prov_latency_ms"],
            "bytes_scanned_mb": pandas_metrics["bytes_scanned_mb"],
            "pruning_speedup_factor": 1.0,
        })

        # Append Spark Parquet row
        results.append({
            "scale": scale,
            "engine": "Spark",
            "format": "Parquet",
            "row_count": expected_rows,
            "storage_size_mb": spark_metrics["storage_size_mb"],
            "compression_ratio_pct": compression_ratio,
            "etl_runtime_sec": spark_metrics["etl_runtime_sec"],
            "query_1prov_latency_ms": spark_metrics["query_1prov_latency_ms"],
            "bytes_scanned_mb": spark_metrics["bytes_scanned_mb"],
            "pruning_speedup_factor": pruning_speedup,
        })

        # Cleanup temporary scale files immediately to free disk space
        if scale in ("10x", "50x"):
            logger.info("  Cleaning up temporary files for scale %s...", scale)
            if parquet_path.exists():
                shutil.rmtree(parquet_path)
            if csv_path.exists():
                csv_path.unlink()
            gc.collect()

    # Final cleanup of temp directory
    if temp_dir.exists():
        shutil.rmtree(temp_dir, ignore_errors=True)

    # Construct DataFrame
    benchmark_df = pd.DataFrame(results)

    # Ensure output directory exists and export CSV
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    benchmark_df.to_csv(output_csv_path, index=False)
    logger.info("=" * 80)
    logger.info("Benchmark successfully exported to: %s", output_csv_path)
    logger.info("=" * 80)

    # Print pretty summary table
    print("\n" + "=" * 105)
    print(f"{'Scale':<7} | {'Engine':<7} | {'Format':<8} | {'Rows':<11} | {'Storage (MB)':<13} | {'Comp. (%)':<10} | {'ETL (s)':<8} | {'Q. Lat (ms)':<12} | {'Scan (MB)':<10} | {'Prune Speedup':<13}")
    print("-" * 105)
    for _, row in benchmark_df.iterrows():
        print(
            f"{str(row['scale']):<7} | "
            f"{str(row['engine']):<7} | "
            f"{str(row['format']):<8} | "
            f"{int(row['row_count']):<11,d} | "
            f"{float(row['storage_size_mb']):<13.2f} | "
            f"{float(row['compression_ratio_pct']):<10.1f} | "
            f"{float(row['etl_runtime_sec']):<8.2f} | "
            f"{float(row['query_1prov_latency_ms']):<12.2f} | "
            f"{float(row['bytes_scanned_mb']):<10.2f} | "
            f"{float(row['pruning_speedup_factor']):<13.2f}x"
        )
    print("=" * 105 + "\n")

    return benchmark_df


def main() -> None:
    """CLI entrypoint for Multi-Scale Benchmark."""
    parser = argparse.ArgumentParser(
        description="Air Quality Multi-Scale Storage & Compute Benchmark (Pandas vs. PySpark 3.5.1)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Custom output CSV path (default: output/cloudsim_benchmark/spark_storage_benchmark.csv)",
    )
    args = parser.parse_args()

    project_root = get_project_root()
    if args.output:
        output_csv = Path(args.output).resolve()
    else:
        output_csv = project_root / "output" / "cloudsim_benchmark" / "spark_storage_benchmark.csv"

    temp_dir = project_root / "output" / "benchmark_temp"

    # Initialize PySpark Session with event logs enabled
    spark = get_spark_session(
        app_name="AirQuality_MultiScale_Benchmark",
        driver_memory="5g",
        shuffle_partitions="16",
    )

    try:
        run_multi_scale_benchmark(
            spark=spark,
            output_csv_path=output_csv,
            temp_dir=temp_dir,
        )
    finally:
        logger.info("Stopping SparkSession...")
        spark.stop()


if __name__ == "__main__":
    main()
