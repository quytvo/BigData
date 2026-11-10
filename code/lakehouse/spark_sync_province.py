"""
spark_sync_province.py
======================
Silver Province Synchronization & Time Continuity Pipeline using Pure PySpark Catalyst Window Functions.

Key Transformation Responsibilities:
1. Loads station-level Silver data from lakehouse_storage/silver/tedp_station/.
2. Floors datetime to hourly granularity and deduplicates on (station_code, datetime).
3. Disambiguates node_id (e.g., HCM -> 28429342215451431160939106697_hcm).
4. Masks median-filled records (col_median_filled == 1 -> NULL) to isolate authentic measurements.
5. Filters 4-year synchronized time window: 2023-12-11 00:00:00 to 2026-11-07 23:00:00 (25,512 hours).
6. Eliminates late-installed stations (BDi_KKXQ_KHITPH in Gia Lai) to prevent artificial step changes.
7. Filters 11 core monitoring provinces meeting >= 80% temporal coverage.
8. Aggregates station measurements to province spatial centroids (KnowAir pattern).
9. Generates continuous 25,512-hour grid (11 provinces x 25,512 = 280,632 rows).
10. Implements pure Catalyst bidirectional time interpolation (limit_direction="both") filling 100% missing values.
11. Computes continuous missing run lengths (pm25_gap_hours) using pure Catalyst arithmetic.
12. Formats station_name and station_short_name matching Pandas string representation ("Trung bình 1.0 trạm").
13. Derives time features and standardized TCVN AQI categorization ("Nguy hai").
14. Exports partitioned Snappy Parquet to lakehouse_storage/silver/tedp_province/.
15. Synchronizes Open-Meteo weather features to lakehouse_storage/silver/weather_province/ partitioned by province_name.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional, Tuple

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

logger = logging.getLogger("AirQualityLakehouse.SyncProvince")

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

START_TIMESTAMP = "2023-12-11 00:00:00"
END_TIMESTAMP = "2026-11-07 23:00:00"
EXPECTED_HOURS_PER_PROVINCE = 25512
EXPECTED_TOTAL_ROWS = 280632  # 11 * 25512

POLLUTANT_COLS: List[str] = ["pm25", "pm10", "aqi"]

OUTPUT_COLS: List[str] = [
    "province_id",
    "province_name",
    "station_id",
    "station_code",
    "station_name",
    "latitude",
    "longitude",
    "datetime",
    "pm25",
    "aqi",
    "pm10",
    "pm25_imputed",
    "pm10_imputed",
    "aqi_imputed",
    "year",
    "month",
    "day",
    "hour",
    "day_of_week",
    "day_name",
    "is_weekend",
    "date",
    "time_slot",
    "aqi_category",
    "province_extracted",
    "station_short_name",
    "is_inserted",
    "n_stations_member",
    "n_stations_valid_pm25",
    "pm25_gap_hours",
]

SILVER_WEATHER_PROVINCE_SCHEMA = StructType([
    StructField("station_id", StringType(), False),
    StructField("station_code", StringType(), False),
    StructField("station_name", StringType(), True),
    StructField("province_name", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("grid_latitude", DoubleType(), True),
    StructField("grid_longitude", DoubleType(), True),
    StructField("elevation", DoubleType(), True),
    StructField("datetime", StringType(), False),
    StructField("temperature_2m", DoubleType(), True),
    StructField("relative_humidity_2m", DoubleType(), True),
    StructField("dew_point_2m", DoubleType(), True),
    StructField("surface_pressure", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("wind_speed_10m", DoubleType(), True),
    StructField("wind_direction_10m", DoubleType(), True),
    StructField("boundary_layer_height", DoubleType(), True),
    StructField("weather_source", StringType(), True),
])


def load_station_silver(spark: SparkSession, input_path: Path) -> DataFrame:
    """Loads station-level Silver dataset from Parquet directory."""
    logger.info("Loading Silver station dataset from: %s", input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Silver station directory not found: {input_path}")
    return spark.read.parquet(str(input_path))


def resolve_end_timestamp(
    df: DataFrame,
    end_timestamp: Optional[str] = None,
    include_partial_day: bool = False,
) -> str:
    """Dynamically resolves the synchronization end timestamp from input station records.

    - If `end_timestamp` (or env `SYNC_END_TIMESTAMP`) is provided, uses that timestamp.
    - If `include_partial_day=True` (or env `SYNC_INCLUDE_PARTIAL_DAY=1`), uses `max(datetime)` directly
      so all partial hours of the current day are included.
    - Otherwise (default), snaps `max(datetime)` to `23:00:00` of the latest complete 24-hour day.
    """
    import os
    from datetime import timedelta

    env_end = end_timestamp or os.getenv("SYNC_END_TIMESTAMP")
    if env_end:
        return env_end.strip()

    keep_partial = include_partial_day or (os.getenv("SYNC_INCLUDE_PARTIAL_DAY", "0") == "1")
    row = df.select(F.max(F.date_trunc("hour", F.col("datetime"))).alias("max_dt")).collect()
    max_dt = row[0]["max_dt"] if row else None
    if max_dt is None:
        return END_TIMESTAMP

    if keep_partial or max_dt.hour == 23:
        resolved = max_dt.strftime("%Y-%m-%d %H:00:00")
    else:
        last_full_day_end = max_dt.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(hours=1)
        resolved = last_full_day_end.strftime("%Y-%m-%d %H:00:00")

    logger.info(
        "Dynamically resolved end_timestamp='%s' from max_dt='%s' (include_partial_day=%s)",
        resolved,
        max_dt.strftime("%Y-%m-%d %H:%M:%S"),
        keep_partial,
    )
    return resolved


def preprocess_station_records(
    df: DataFrame,
    start_timestamp: str = START_TIMESTAMP,
    end_timestamp: Optional[str] = None,
    include_partial_day: bool = False,
) -> Tuple[DataFrame, str]:
    """Floors datetime, disambiguates node_id, masks median-filled records, and filters late stations."""
    logger.info("Preprocessing station records (time truncation, node disambiguation, median masking)...")

    # 1. Truncate datetime to hour and deduplicate
    df = df.withColumn("datetime", F.date_trunc("hour", F.col("datetime")))
    df = df.dropDuplicates(["station_code", "datetime"])

    # 2. Disambiguate node_id for shared province_id
    node_id_expr = (
        F.when(F.col("province_name") == "HCM", F.concat(F.col("province_id"), F.lit("_hcm")))
        .when(F.col("province_name") == "HCM KV3", F.concat(F.col("province_id"), F.lit("_hcm_kv3")))
        .otherwise(F.col("province_id"))
    )
    df = df.withColumn("node_id", node_id_expr)

    # 3. Mask median-filled records back to NULL (only genuine measurements are used for province mean)
    for col in POLLUTANT_COLS:
        med_col = f"{col}_median_filled"
        if med_col in df.columns:
            df = df.withColumn(
                col,
                F.when(F.col(med_col) == 1, F.lit(None)).otherwise(F.col(col))
            )

    # 4. Drop late-installed station BDi_KKXQ_KHITPH (31.1% coverage) in Gia Lai & filter 11 provinces first
    df = df.filter(F.col("station_code") != "BDi_KKXQ_KHITPH")
    df = df.filter(F.col("province_name").isin(TARGET_PROVINCES))

    # 5. Cut synchronized time window using dynamically resolved end timestamp
    resolved_end_ts = resolve_end_timestamp(
        df, end_timestamp=end_timestamp, include_partial_day=include_partial_day
    )
    t_start = F.to_timestamp(F.lit(start_timestamp))
    t_end = F.to_timestamp(F.lit(resolved_end_ts))
    df = df.filter((F.col("datetime") >= t_start) & (F.col("datetime") <= t_end))

    return df, resolved_end_ts


def compute_province_members_meta(df: DataFrame) -> DataFrame:
    """Computes spatial centroid coordinates and member metadata per province."""
    logger.info("Computing spatial centroid coordinates and member station metadata...")

    # Deduplicate station records to ensure unbiased centroid calculation
    df_stations = df.select(
        "node_id", "province_id", "province_name", "station_code", "latitude", "longitude"
    ).dropDuplicates(["node_id", "station_code"])

    members_meta = (
        df_stations.groupBy("node_id", "province_id", "province_name")
        .agg(
            F.avg("latitude").alias("latitude"),
            F.avg("longitude").alias("longitude"),
            F.count("station_code").alias("n_stations_member_cnt"),
        )
    )

    # Canonical station code mapping for 11 provinces
    station_code_expr = (
        F.when(F.col("province_name") == "Bắc Ninh", F.lit("PROV_BAC_NINH"))
        .when(F.col("province_name") == "Gia Lai", F.lit("PROV_GIA_LAI"))
        .when(F.col("province_name") == "HCM", F.lit("PROV_HCM"))
        .when(F.col("province_name") == "Hà Nội", F.lit("PROV_HA_NOI"))
        .when(F.col("province_name") == "Hưng Yên", F.lit("PROV_HUNG_YEN"))
        .when(F.col("province_name") == "Hải Phòng", F.lit("PROV_HAI_PHONG"))
        .when(F.col("province_name") == "Lâm Đồng", F.lit("PROV_LAM_DONG"))
        .when(F.col("province_name") == "Ninh Bình", F.lit("PROV_NINH_BINH"))
        .when(F.col("province_name") == "Quảng Ngãi", F.lit("PROV_QUANG_NGAI"))
        .when(F.col("province_name") == "Tây Ninh", F.lit("PROV_TAY_NINH"))
        .when(F.col("province_name") == "Đà Nẵng", F.lit("PROV_DA_NANG"))
        .otherwise(F.concat(F.lit("PROV_"), F.col("province_name")))
    )

    # String format matches Pandas float string representation ("Trung bình 1.0 trạm")
    cnt_double = F.col("n_stations_member_cnt").cast(DoubleType())
    station_name_expr = F.concat(
        F.col("province_name"), F.lit(": Trung bình "), cnt_double, F.lit(" trạm")
    )
    station_short_expr = F.concat(F.lit("TB "), cnt_double, F.lit(" trạm"))

    return (
        members_meta
        .withColumn("station_id", F.col("node_id"))
        .withColumn("station_code", station_code_expr)
        .withColumn("station_name", station_name_expr)
        .withColumn("station_short_name", station_short_expr)
        .withColumn("province_extracted", F.col("province_name"))
        .withColumn("n_stations_member", F.col("n_stations_member_cnt").cast(IntegerType()))
        .drop("n_stations_member_cnt")
    )


def aggregate_and_interpolate_province_grid(
    spark: SparkSession,
    df: DataFrame,
    members_meta: DataFrame,
    start_timestamp: str = START_TIMESTAMP,
    end_timestamp: str = END_TIMESTAMP,
) -> DataFrame:
    """Generates continuous hourly grid per province and applies pure Catalyst bidirectional interpolation."""
    logger.info("Aggregating hourly measurements per province...")

    # Mean calculation across active stations (Catalyst avg automatically skips NULLs)
    agg_df = (
        df.groupBy("node_id", "datetime")
        .agg(
            F.avg("pm25").alias("pm25_raw"),
            F.avg("pm10").alias("pm10_raw"),
            F.avg("aqi").alias("aqi_raw"),
            F.count("pm25").alias("n_stations_valid_pm25"),
        )
    )

    logger.info(
        "Generating continuous hourly grid sequence [%s -> %s] across 11 provinces...",
        start_timestamp,
        end_timestamp,
    )
    time_grid = spark.sql(f"""
        SELECT explode(sequence(
            to_timestamp('{start_timestamp}'),
            to_timestamp('{end_timestamp}'),
            interval 1 hour
        )) AS datetime
    """)

    full_grid = time_grid.crossJoin(members_meta)

    # Left join full grid with hourly aggregated observations
    grid_data = full_grid.join(agg_df, on=["node_id", "datetime"], how="left")

    grid_data = (
        grid_data
        .withColumn(
            "n_stations_valid_pm25",
            F.coalesce(F.col("n_stations_valid_pm25"), F.lit(0)).cast(IntegerType())
        )
        .withColumn(
            "is_inserted",
            F.when(F.col("pm25_raw").isNull(), F.lit(1)).otherwise(F.lit(0)).cast(IntegerType())
        )
    )

    logger.info("Executing pure Catalyst bidirectional time interpolation and pm25_gap_hours calculation...")
    w_pre = Window.partitionBy("province_name").orderBy(F.col("datetime").asc()).rowsBetween(Window.unboundedPreceding, -1)
    w_desc = Window.partitionBy("province_name").orderBy(F.col("datetime").desc()).rowsBetween(Window.unboundedPreceding, -1)

    t_cur = F.unix_timestamp(F.col("datetime"))
    t_start = F.unix_timestamp(F.lit(start_timestamp))
    t_end = F.unix_timestamp(F.lit(end_timestamp))

    # 1. Compute continuous gap lengths for PM2.5
    is_valid_pm25 = F.col("pm25_raw").isNotNull() & (~F.isnan(F.col("pm25_raw")))
    valid_t_pm25 = F.when(is_valid_pm25, t_cur)

    t_pre_pm25 = F.last(valid_t_pm25, ignorenulls=True).over(w_pre)
    t_post_pm25 = F.last(valid_t_pm25, ignorenulls=True).over(w_desc)

    pm25_gap_hours_expr = (
        F.when(is_valid_pm25, F.lit(0))
        .when(t_pre_pm25.isNotNull() & t_post_pm25.isNotNull(), ((t_post_pm25 - t_pre_pm25) / 3600 - 1).cast(IntegerType()))
        .when(t_pre_pm25.isNull() & t_post_pm25.isNotNull(), ((t_post_pm25 - t_start) / 3600).cast(IntegerType()))
        .when(t_pre_pm25.isNotNull() & t_post_pm25.isNull(), ((t_end - t_pre_pm25) / 3600).cast(IntegerType()))
        .otherwise(F.lit(0))
    )

    grid_data = grid_data.withColumn("pm25_gap_hours", pm25_gap_hours_expr)

    # 2. Bidirectional interpolation for pm25, pm10, aqi
    for col in POLLUTANT_COLS:
        raw_col = f"{col}_raw"
        is_valid = F.col(raw_col).isNotNull() & (~F.isnan(F.col(raw_col)))
        valid_t = F.when(is_valid, t_cur)

        t_pre = F.last(valid_t, ignorenulls=True).over(w_pre)
        t_post = F.last(valid_t, ignorenulls=True).over(w_desc)

        y_pre = F.last(F.when(is_valid, F.col(raw_col)), ignorenulls=True).over(w_pre)
        y_post = F.last(F.when(is_valid, F.col(raw_col)), ignorenulls=True).over(w_desc)

        interp_time = (
            F.when(is_valid, F.col(raw_col))
            .when(
                y_pre.isNotNull() & y_post.isNotNull(),
                y_pre + (y_post - y_pre) * ((t_cur - t_pre) / (t_post - t_pre))
            )
            .when(y_pre.isNull() & y_post.isNotNull(), y_post)
            .when(y_pre.isNotNull() & y_post.isNull(), y_pre)
            .otherwise(F.lit(None))
        )

        imputed_flag = F.when(is_valid, F.lit(0)).otherwise(F.lit(1))

        grid_data = (
            grid_data
            .withColumn(col, interp_time.cast(DoubleType()))
            .withColumn(f"{col}_imputed", imputed_flag.cast(IntegerType()))
            .drop(raw_col)
        )

    return grid_data


def add_province_time_features(df: DataFrame) -> DataFrame:
    """Adds calendar and AQI category features."""
    logger.info("Deriving calendar features and standardized TCVN AQI categorization...")

    day_of_week_expr = (F.dayofweek(F.col("datetime")) + 5) % 7
    hour_col = F.hour(F.col("datetime"))

    time_slot_expr = (
        F.when(hour_col < 6, F.lit("night (0-6h)"))
        .when(hour_col < 12, F.lit("morning (6-12h)"))
        .when(hour_col < 18, F.lit("afternoon (12-18h)"))
        .otherwise(F.lit("evening (18-24h)"))
    )

    # AQI category (TCVN: [0, 50) Tot, [50, 100) Trung binh, ..., >= 300 Nguy hai)
    aqi_cat_expr = (
        F.when(F.col("aqi") < 50.0, F.lit("Tot"))
        .when(F.col("aqi") < 100.0, F.lit("Trung binh"))
        .when(F.col("aqi") < 150.0, F.lit("Kem"))
        .when(F.col("aqi") < 200.0, F.lit("Xau"))
        .when(F.col("aqi") < 300.0, F.lit("Rat xau"))
        .otherwise(F.lit("Nguy hai"))
    )

    df_out = (
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

    return df_out.select(OUTPUT_COLS)


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


def sync_weather_province(
    spark: SparkSession,
    weather_csv: Path,
    weather_output_dir: Path,
) -> None:
    """Synchronizes and partitions Open-Meteo atmospheric features to Silver weather table."""
    logger.info("Synchronizing Open-Meteo weather dataset from: %s", weather_csv)
    if not weather_csv.exists():
        raise FileNotFoundError(f"Weather reference file not found: {weather_csv}")

    df_weather = (
        spark.read
        .option("header", "true")
        .schema(SILVER_WEATHER_PROVINCE_SCHEMA)
        .csv(str(weather_csv))
    )

    df_weather = (
        df_weather
        .withColumn("datetime", F.to_timestamp(F.col("datetime"), "yyyy-MM-dd HH:mm:ss"))
        .filter(F.col("province_name").isin(TARGET_PROVINCES))
    )

    purge_output_directory(weather_output_dir)

    logger.info("Writing Silver weather dataset to: %s (partitioned by province_name)", weather_output_dir)
    (
        df_weather.write
        .mode("overwrite")
        .partitionBy("province_name")
        .parquet(str(weather_output_dir))
    )
    logger.info("Silver weather synchronization successfully completed.")


def process_province_silver(
    spark: SparkSession,
    station_dir: Path,
    province_dir: Path,
    weather_csv: Path,
    weather_output_dir: Path,
    end_timestamp: Optional[str] = None,
    include_partial_day: bool = False,
) -> DataFrame:
    """Executes the full province-level Silver synchronization pipeline."""
    df_station = load_station_silver(spark, station_dir)
    df_preprocessed, resolved_end_ts = preprocess_station_records(
        df_station,
        end_timestamp=end_timestamp,
        include_partial_day=include_partial_day,
    )
    members_meta = compute_province_members_meta(df_preprocessed)
    df_grid = aggregate_and_interpolate_province_grid(
        spark,
        df_preprocessed,
        members_meta,
        end_timestamp=resolved_end_ts,
    )
    df_final = add_province_time_features(df_grid)

    purge_output_directory(province_dir)

    logger.info("Writing Silver province dataset to: %s (partitioned by province_name)", province_dir)
    (
        df_final.write
        .mode("overwrite")
        .partitionBy("province_name")
        .parquet(str(province_dir))
    )
    logger.info("Silver province table successfully written.")

    sync_weather_province(spark, weather_csv, weather_output_dir)
    return df_final


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PySpark Province-Level Silver Synchronization")
    parser.add_argument("--input", type=str, default=None, help="Path to silver tedp_station Parquet directory")
    parser.add_argument("--output", type=str, default=None, help="Output path for silver tedp_province Parquet")
    parser.add_argument("--weather-input", type=str, default=None, help="Path to weather_openmeteo_province_4y.csv")
    parser.add_argument("--weather-output", type=str, default=None, help="Output path for silver weather_province Parquet")
    parser.add_argument("--end-timestamp", type=str, default=None, help="Optional explicit end timestamp (YYYY-MM-DD HH:00:00)")
    parser.add_argument("--include-partial-day", action="store_true", help="Include partial hours of the current day up to max(datetime)")
    parser.add_argument("--keep-ui", action="store_true", help="Hold Spark Web UI open after execution")
    parser.add_argument("--keep-ui-seconds", type=int, default=0, help="Hold Spark Web UI open for specified seconds")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = get_project_root()

    station_dir = Path(args.input) if args.input else root / "lakehouse_storage" / "silver" / "tedp_station"
    province_dir = Path(args.output) if args.output else root / "lakehouse_storage" / "silver" / "tedp_province"
    weather_csv = Path(args.weather_input) if args.weather_input else root / "data_test" / "weather_openmeteo_province_4y.csv"
    weather_output_dir = Path(args.weather_output) if args.weather_output else root / "lakehouse_storage" / "silver" / "weather_province"

    spark = get_spark_session(
        app_name="SyncTEDPProvince",
        keep_ui=args.keep_ui,
    )

    try:
        process_province_silver(
            spark=spark,
            station_dir=station_dir,
            province_dir=province_dir,
            weather_csv=weather_csv,
            weather_output_dir=weather_output_dir,
            end_timestamp=args.end_timestamp,
            include_partial_day=args.include_partial_day,
        )
    finally:
        if args.keep_ui:
            wait_for_webui(spark, keep_ui_seconds=args.keep_ui_seconds)
        spark.stop()


if __name__ == "__main__":
    main()
