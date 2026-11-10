"""
spark_preprocess_tedp.py
=========================
Bronze to Silver Station-Level Preprocessing Pipeline using Pure PySpark Catalyst Window Functions.

Key Transformation Responsibilities:
1. Enforces static StructType schema for raw Bronze TEDP hourly data (no inferSchema).
2. Cleans timestamps and removes station_id/datetime duplicates.
3. Applies physical validity thresholds based on QCVN 05:2023 (outliers -> null).
4. Performs pure Catalyst Window linear interpolation (limit=3) per station.
5. Flags and imputes remaining gaps (>3h) using station-level median (col_median_filled=1).
6. Generates rich temporal attributes (year, month, day, hour, day_of_week, time_slot, is_weekend).
7. Derives official AQI category labels (TCVN / US EPA).
8. Standardizes province and station names; filters to target 11 monitoring provinces.
9. Writes Snappy-compressed Parquet partitioned by province_name to lakehouse_storage/silver/tedp_station/.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

# Add lakehouse module path
CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

try:
    from spark_session import get_project_root, get_spark_session, wait_for_webui
except ImportError:
    from code.lakehouse.spark_session import get_project_root, get_spark_session, wait_for_webui

logger = logging.getLogger("AirQualityLakehouse.PreprocessTEDP")

# ------------------------------------------------------------------------------
# CONSTANTS & SCHEMAS
# ------------------------------------------------------------------------------
TARGET_PROVINCES: List[str] = [
    "Bắc Ninh",
    "Gia Lai",
    "HCM",
    "Hà Nội",
    "Hưng Yên",
    "Hải Phòng",
    "Lâm Đồng",
    "Ninh Bình",
    "Quảng Ngãi",
    "Tây Ninh",
    "Đà Nẵng",
]

THRESHOLDS = {
    "pm25": (0.0, 500.0),
    "pm10": (0.0, 600.0),
    "aqi": (0.0, 500.0),
}

POLLUTANT_COLS: List[str] = ["pm25", "pm10", "aqi"]

BRONZE_TEDP_SCHEMA = StructType([
    StructField("province_id", StringType(), True),
    StructField("province_name", StringType(), True),
    StructField("station_id", StringType(), True),
    StructField("station_code", StringType(), True),
    StructField("station_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("datetime", StringType(), True),
    StructField("pm25", DoubleType(), True),
    StructField("aqi", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
])


def load_bronze_data(spark: SparkSession, input_path: Path) -> DataFrame:
    """Loads raw Bronze TEDP hourly CSV dataset with explicit StructType schema."""
    logger.info("Loading Bronze TEDP dataset from: %s", input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Bronze input file not found: {input_path}")

    return (
        spark.read
        .option("header", "true")
        .schema(BRONZE_TEDP_SCHEMA)
        .csv(str(input_path))
    )


def clean_and_filter_qcvn(df: DataFrame) -> DataFrame:
    """Parses timestamps, deduplicates, and filters physical outlier bounds (QCVN 05:2023)."""
    logger.info("Parsing timestamps and deduplicating station records...")
    df_clean = (
        df
        .withColumn("datetime", F.to_timestamp(F.col("datetime"), "yyyy-MM-dd HH:mm:ss"))
        .filter(F.col("datetime").isNotNull())
        .dropDuplicates(["station_id", "datetime"])
    )

    logger.info("Applying QCVN physical outlier thresholds (setting bad measurements to NULL)...")
    for col, (lo, hi) in THRESHOLDS.items():
        if col in df_clean.columns:
            df_clean = df_clean.withColumn(
                col,
                F.when((F.col(col) >= lo) & (F.col(col) <= hi), F.col(col)).otherwise(F.lit(None))
            )

    return df_clean


def normalize_province_station_names(df: DataFrame) -> DataFrame:
    """Normalizes province and station names and filters to the 11 target provinces."""
    logger.info("Standardizing province and station naming conventions...")
    # Station name format in TEDP: "Province: Station Detail (KK)"
    prov_extracted = F.trim(F.split(F.col("station_name"), ":").getItem(0))
    short_name_raw = F.trim(F.split(F.col("station_name"), ":").getItem(1))
    short_name = F.trim(F.regexp_replace(short_name_raw, r"\(KK\)", ""))

    df_norm = (
        df
        .withColumn("province_extracted", prov_extracted)
        .withColumn("station_short_name", short_name)
        .withColumn(
            "province_name",
            F.when(
                F.col("province_name").isNull()
                | F.lower(F.col("province_name")).isin(["unknown", ""]),
                prov_extracted,
            ).otherwise(F.col("province_name"))
        )
        .filter(F.col("province_name").isin(TARGET_PROVINCES))
    )

    return df_norm


def interpolate_and_impute_pollutants(df: DataFrame) -> DataFrame:
    """Performs Catalyst Window linear interpolation (limit=3) and median fill per station.

    Pure Catalyst Window implementation:
    - Preceding bounds use chronological window rowsBetween(unboundedPreceding, -1).
    - Following bounds use reverse chronological window (orderBy desc) rowsBetween(unboundedPreceding, -1)
      achieving O(N) linear streaming evaluation instead of O(N^2) forward buffering.
    - Missing runs of length <= 3 are linearly interpolated between boundary anchors.
    - Trailing missing runs <= 3 are forward-filled with the last known measurement.
    - Runs > 3h receive col_median_filled = 1 and are imputed with the station median.
    """
    logger.info("Executing pure Catalyst Window linear interpolation (limit=3) and median imputation...")

    # Calculate station medians via aggregation and broadcast join for optimal throughput
    median_aggs = [
        F.expr(f"percentile_approx({c}, 0.5)").alias(f"{c}_st_median")
        for c in POLLUTANT_COLS
    ]
    station_medians_df = df.groupBy("station_id").agg(*median_aggs)

    df_proc = df.join(F.broadcast(station_medians_df), on="station_id", how="left")

    w_station = Window.partitionBy("station_id").orderBy(F.col("datetime").asc())
    w_pre = Window.partitionBy("station_id").orderBy(F.col("datetime").asc()).rowsBetween(Window.unboundedPreceding, -1)
    w_desc = Window.partitionBy("station_id").orderBy(F.col("datetime").desc()).rowsBetween(Window.unboundedPreceding, -1)

    df_proc = df_proc.withColumn("row_idx", F.row_number().over(w_station))

    for col in POLLUTANT_COLS:
        is_valid = F.col(col).isNotNull() & (~F.isnan(F.col(col)))
        imputed_flag = F.when(~is_valid, F.lit(1)).otherwise(F.lit(0))

        # Anchor tracking
        val_clean = F.when(is_valid, F.col(col)).otherwise(F.lit(None))
        valid_idx = F.when(is_valid, F.col("row_idx")).otherwise(F.lit(None))

        prev_idx = F.last(valid_idx, ignorenulls=True).over(w_pre)
        prev_val = F.last(val_clean, ignorenulls=True).over(w_pre)
        next_idx = F.last(valid_idx, ignorenulls=True).over(w_desc)
        next_val = F.last(val_clean, ignorenulls=True).over(w_desc)

        offset = F.col("row_idx") - prev_idx
        delta = next_idx - prev_idx

        # Linear interpolation with limit=3 (matches pandas interpolate(method='linear', limit=3))
        interp_expr = (
            F.when(is_valid, F.col(col))
            .when(
                prev_val.isNotNull() & next_val.isNotNull() & (offset <= 3),
                prev_val + (next_val - prev_val) * (offset / delta)
            )
            .when(
                prev_val.isNotNull() & next_val.isNull() & (offset <= 3),
                prev_val
            )
            .otherwise(F.lit(None))
        )

        median_flag = F.when((~is_valid) & interp_expr.isNull(), F.lit(1)).otherwise(F.lit(0))
        st_median_col = F.col(f"{col}_st_median")
        final_val = F.coalesce(interp_expr, st_median_col)

        df_proc = (
            df_proc
            .withColumn(f"{col}_imputed", imputed_flag.cast(IntegerType()))
            .withColumn(f"{col}_median_filled", median_flag.cast(IntegerType()))
            .withColumn(col, final_val.cast(DoubleType()))
            .drop(f"{col}_st_median")
        )

    return df_proc.drop("row_idx")


def derive_temporal_features_and_aqi(df: DataFrame) -> DataFrame:
    """Generates temporal features and AQI category classification."""
    logger.info("Deriving temporal calendar features and AQI categorization...")

    day_of_week_expr = (F.dayofweek(F.col("datetime")) + 5) % 7
    hour_col = F.hour(F.col("datetime"))

    time_slot_expr = (
        F.when(hour_col < 6, F.lit("night (0-6h)"))
        .when(hour_col < 12, F.lit("morning (6-12h)"))
        .when(hour_col < 18, F.lit("afternoon (12-18h)"))
        .otherwise(F.lit("evening (18-24h)"))
    )

    # AQI category (TCVN / US EPA)
    aqi_cat_expr = (
        F.when(F.col("aqi") <= 50.0, F.lit("Tot"))
        .when(F.col("aqi") <= 100.0, F.lit("Trung binh"))
        .when(F.col("aqi") <= 150.0, F.lit("Kem"))
        .when(F.col("aqi") <= 200.0, F.lit("Xau"))
        .when(F.col("aqi") <= 300.0, F.lit("Rat xau"))
        .otherwise(F.lit("Nguy hiem"))
    )

    df_featured = (
        df
        .withColumn("year", F.year(F.col("datetime")).cast(IntegerType()))
        .withColumn("month", F.month(F.col("datetime")).cast(IntegerType()))
        .withColumn("day", F.dayofmonth(F.col("datetime")).cast(IntegerType()))
        .withColumn("hour", hour_col.cast(IntegerType()))
        .withColumn("day_of_week", day_of_week_expr.cast(IntegerType()))
        .withColumn("day_name", F.date_format(F.col("datetime"), "EEEE"))
        .withColumn("is_weekend", F.when(day_of_week_expr >= 5, F.lit(1)).otherwise(F.lit(0)).cast(IntegerType()))
        .withColumn("date", F.date_format(F.col("datetime"), "yyyy-MM-dd"))
        .withColumn("time_slot", time_slot_expr)
        .withColumn("aqi_category", aqi_cat_expr)
    )

    final_columns = [
        "province_id", "province_name", "station_id", "station_code", "station_name",
        "latitude", "longitude", "datetime",
        "pm25", "aqi", "pm10",
        "pm25_imputed", "pm25_median_filled",
        "pm10_imputed", "pm10_median_filled",
        "aqi_imputed", "aqi_median_filled",
        "year", "month", "day", "hour", "day_of_week", "day_name", "is_weekend",
        "date", "time_slot", "aqi_category",
        "province_extracted", "station_short_name",
    ]

    return df_featured.select(final_columns)


def purge_output_directory(target_dir: Path) -> None:
    """Safely and deterministically purges target directory prior to writing partitioned Parquet.

    Prevents cross-platform partition collisions (%20 vs literal space) and
    stale partition accumulation across Windows host and Linux Docker container.
    """
    target_path = Path(target_dir)
    if not target_path.exists():
        return

    logger.info("Purging target directory prior to write: %s", target_path)
    import os
    import shutil
    import stat
    import time

    def _handle_readonly(func, path, exc_info):
        try:
            os.chmod(path, stat.S_IWRITE)
            func(path)
        except Exception as err:
            logger.warning("Failed to reset permissions on %s: %s", path, err)

    for attempt in range(3):
        try:
            shutil.rmtree(str(target_path), onerror=_handle_readonly)
            logger.info("Successfully purged directory: %s", target_path)
            return
        except Exception as exc:
            logger.warning("Purge attempt %d for %s failed: %s", attempt + 1, target_path, exc)
            time.sleep(0.5)

    if target_path.exists():
        raise RuntimeError(f"Failed to cleanly purge target directory: {target_path}")


def process_station_silver(
    spark: SparkSession,
    input_file: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    bronze_path: Optional[Path] = None,
) -> DataFrame:
    """Executes the end-to-end station-level Bronze to Silver pipeline."""
    src_file = input_file or bronze_path
    if src_file is None or output_dir is None:
        raise ValueError("input_file (or bronze_path) and output_dir must be provided")
    df_raw = load_bronze_data(spark, src_file)
    df_cleaned = clean_and_filter_qcvn(df_raw)
    df_norm = normalize_province_station_names(df_cleaned)
    df_interp = interpolate_and_impute_pollutants(df_norm)
    df_final = derive_temporal_features_and_aqi(df_interp)

    purge_output_directory(output_dir)

    logger.info("Writing Silver station dataset to: %s (partitioned by province_name)", output_dir)
    (
        df_final.write
        .mode("overwrite")
        .partitionBy("province_name")
        .parquet(str(output_dir))
    )
    logger.info("Silver station preprocessing successfully completed.")
    return df_final


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PySpark Station-Level Bronze to Silver Preprocessing")
    parser.add_argument("--input", type=str, default=None, help="Path to bronze tedp_aqi_pm25_hourly.csv")
    parser.add_argument("--output", type=str, default=None, help="Output path for silver tedp_station")
    parser.add_argument("--keep-ui", action="store_true", help="Hold Spark Web UI open after execution")
    parser.add_argument("--keep-ui-seconds", type=int, default=0, help="Hold Spark Web UI open for specified seconds")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = get_project_root()

    input_file = Path(args.input) if args.input else root / "lakehouse_storage" / "bronze" / "tedp_aqi_pm25_hourly.csv"
    output_dir = Path(args.output) if args.output else root / "lakehouse_storage" / "silver" / "tedp_station"

    spark = get_spark_session(
        app_name="PreprocessTEDPStation",
        keep_ui=args.keep_ui,
    )

    try:
        process_station_silver(spark, input_file, output_dir)
    finally:
        if args.keep_ui:
            wait_for_webui(spark, keep_ui_seconds=args.keep_ui_seconds)
        spark.stop()


if __name__ == "__main__":
    main()
