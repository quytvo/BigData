"""
spark_feature_engineering.py
============================
Silver to Gold 49 Atmospheric Features Pipeline using Pure PySpark Catalyst Window Functions.

Key Transformation Responsibilities:
1. Reads Silver TEDP and Weather Parquet datasets with static schemas (no inferSchema).
2. Performs deterministic inner join on ['province_name', 'datetime'].
3. Computes 49 atmospheric features + target label y + target_time using 100% pure Spark SQL
   and Catalyst Window functions (ZERO Python @udf, ZERO applyInPandas, ZERO toPandas).
4. Drops exactly 48 boundary rows per province (24h lookback and 24h lookahead).
5. Supports optional cutoff target date filtering (e.g. 2026-11-06 23:00:00 for 279,840 rows).
6. Enforces static StructType schema for the Gold feature table.
7. Purges output directory with purge_output_directory() before writing Parquet.
8. Writes partitioned Parquet with Snappy compression to lakehouse_storage/gold/global_ann_features/.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import shutil
import stat
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

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

logger = logging.getLogger("AirQualityLakehouse.GoldFeatureEngineering")

# ------------------------------------------------------------------------------
# CONSTANTS & METADATA
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

# Canonical 11 Station Identifiers matching model weights & province nodes
CANONICAL_STATION_IDS: List[str] = [
    "28429342215451431160939106654",       # Bắc Ninh
    "28429342215451431160939106668",       # Gia Lai
    "28429342215451431160939106697_hcm",   # HCM
    "28429342215451431160939106671",       # Hà Nội
    "28429342215451431160939106677",       # Hưng Yên
    "28429342215451431160939106674",       # Hải Phòng
    "28429342215451431160939106682",       # Lâm Đồng
    "28429342215451431160939106688",       # Ninh Bình
    "28429342215451431160939106694",       # Quảng Ngãi
    "28429342215451431160939106700",       # Tây Ninh
    "28429342215451431160939106662",       # Đà Nẵng
]

# Canonical Station Metadata mapping: station_id -> (province_name, station_idx)
STATION_METADATA_MAPPING: List[Tuple[str, str, int]] = [
    ("28429342215451431160939106654", "Bắc Ninh", 0),
    ("28429342215451431160939106668", "Gia Lai", 1),
    ("28429342215451431160939106697_hcm", "HCM", 2),
    ("28429342215451431160939106671", "Hà Nội", 3),
    ("28429342215451431160939106677", "Hưng Yên", 4),
    ("28429342215451431160939106674", "Hải Phòng", 5),
    ("28429342215451431160939106682", "Lâm Đồng", 6),
    ("28429342215451431160939106688", "Ninh Bình", 7),
    ("28429342215451431160939106694", "Quảng Ngãi", 8),
    ("28429342215451431160939106700", "Tây Ninh", 9),
    ("28429342215451431160939106662", "Đà Nẵng", 10),
]

# Spatial coordinate bounding box for normalization to [-1.0, 1.0]
LAT_MIN = 10.5391
LAT_MAX = 21.301503
LON_MIN = 105.81825
LON_MAX = 109.2198

# Raw atmospheric weather variables from Open-Meteo
WEATHER_COLS: List[str] = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "surface_pressure",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "boundary_layer_height",
]

# Canonical 49 Atmospheric Features as defined in train_global_ann.py
CANONICAL_49_FEATURES: List[str] = [
    # 1. PM2.5 Lags (6)
    "pm25_lag0", "pm25_lag1", "pm25_lag3", "pm25_lag6", "pm25_lag12", "pm25_lag24",
    # 2. Rolling Averages (2)
    "pm25_roll6", "pm25_roll24",
    # 3. PM2.5 Dynamics (3)
    "pm25_diff_1h", "pm25_diff_24h", "pm25_std24",
    # 4. Co-pollutants (2)
    "pm10_lag0", "aqi_lag0",
    # 5. Raw Weather (8)
    "temperature_2m", "relative_humidity_2m", "dew_point_2m", "surface_pressure",
    "precipitation", "wind_speed_10m", "wind_direction_10m", "boundary_layer_height",
    # 6. Thermodynamic Derived (8)
    "wind_dir_sin", "wind_dir_cos", "dew_point_deficit", "ventilation_coef",
    "temp_diff_24h", "pressure_diff_24h", "pblh_diff_24h", "precip_sum_24h",
    # 7. Time Targets (2)
    "hour", "day_of_week",
    # 8. Cyclic Time (4)
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    # 9. Spatial Coordinates (2)
    "latitude_norm", "longitude_norm",
    # 10. Binary Indicator (1)
    "is_weekend",
    # 11. One-Hot Station Identifiers (11)
    "station_id_28429342215451431160939106654",
    "station_id_28429342215451431160939106668",
    "station_id_28429342215451431160939106697_hcm",
    "station_id_28429342215451431160939106671",
    "station_id_28429342215451431160939106677",
    "station_id_28429342215451431160939106674",
    "station_id_28429342215451431160939106682",
    "station_id_28429342215451431160939106688",
    "station_id_28429342215451431160939106694",
    "station_id_28429342215451431160939106700",
    "station_id_28429342215451431160939106662",
]

# Static StructType Schema for the Gold feature table
GOLD_FEATURE_SCHEMA = StructType([
    # Identifiers & Metadata
    StructField("station_id", StringType(), False),
    StructField("station_code", StringType(), False),
    StructField("station_name", StringType(), False),
    StructField("province_id", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("origin_time", TimestampType(), False),
    StructField("target_time", TimestampType(), False),
    StructField("datetime", TimestampType(), False),
    StructField("station_idx", IntegerType(), False),
    # Target label
    StructField("y", DoubleType(), False),
    # 49 atmospheric features
    *[StructField(f, DoubleType(), False) for f in CANONICAL_49_FEATURES],
    # Partition column
    StructField("province_name", StringType(), False),
])


# ------------------------------------------------------------------------------
# DIRECTORY MANAGEMENT
# ------------------------------------------------------------------------------
def purge_output_directory(target_dir: Path) -> None:
    """Safely and deterministically purges target directory prior to writing partitioned Parquet.

    Prevents cross-platform partition collisions (%20 vs literal space) and
    stale partition accumulation across Windows host and Linux Docker container.
    """
    target_path = Path(target_dir)
    if not target_path.exists():
        return

    logger.info("Purging target directory prior to write: %s", target_path)

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


# ------------------------------------------------------------------------------
# PURE PYSPARK FEATURE ENGINEERING LOGIC
# ------------------------------------------------------------------------------
def build_gold_features_dataframe(
    spark: SparkSession,
    silver_tedp_dir: Path,
    silver_weather_dir: Path,
    cutoff_target_date: Optional[str] = None,
    province_name: Optional[str] = None,
) -> DataFrame:
    """Executes pure PySpark Catalyst Window transformations to produce the 49 Gold features.

    Parameters
    ----------
    spark : SparkSession
        Active configured SparkSession.
    silver_tedp_dir : Path
        Directory of Silver TEDP province table.
    silver_weather_dir : Path
        Directory of Silver Weather province table.
    cutoff_target_date : Optional[str]
        Optional cutoff date timestamp (e.g. '2026-11-06 23:00:00').

    Returns
    -------
    DataFrame
        Transformed Gold features DataFrame with exactly 49 atmospheric features + label y.
    """
    logger.info("Loading Silver TEDP province dataset from: %s", silver_tedp_dir)
    df_tedp = spark.read.parquet(str(silver_tedp_dir))

    logger.info("Loading Silver Weather province dataset from: %s", silver_weather_dir)
    df_wea = spark.read.parquet(str(silver_weather_dir))
    if province_name is not None:
        # Apply partition pruning before the join and window calculations.
        df_tedp = df_tedp.filter(F.col("province_name") == province_name)
        df_wea = df_wea.filter(F.col("province_name") == province_name)

    # Aliasing weather columns to cur_* to prevent name collision with future weather features
    df_wea_sub = df_wea.select(
        "province_name",
        "datetime",
        *[F.col(c).alias(f"cur_{c}") for c in WEATHER_COLS],
    )

    # Deterministic join on ['province_name', 'datetime']
    df_joined = df_tedp.select(
        "station_id",
        "station_code",
        "station_name",
        "province_id",
        "province_name",
        "latitude",
        "longitude",
        "datetime",
        "pm25",
        "aqi",
        "pm10",
    ).join(df_wea_sub, on=["province_name", "datetime"], how="inner")

    # Define Catalyst Window specifications partitioned by province_name
    w_order = Window.partitionBy("province_name").orderBy("datetime")
    w_roll6 = Window.partitionBy("province_name").orderBy("datetime").rowsBetween(-5, 0)
    w_roll24 = Window.partitionBy("province_name").orderBy("datetime").rowsBetween(-23, 0)
    w_fwd24 = Window.partitionBy("province_name").orderBy("datetime").rowsBetween(1, 24)

    # 1. Base transformations & Lags & Rolling & Leads
    df_feat = df_joined.select(
        "station_id",
        "station_code",
        "station_name",
        "province_id",
        "province_name",
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("datetime").alias("origin_time"),
        F.col("datetime").alias("datetime"),
        F.lead("datetime", 24).over(w_order).alias("target_time"),
        F.lead("pm25", 24).over(w_order).cast("double").alias("y"),

        # 1.1 PM2.5 Lags (6)
        F.col("pm25").cast("double").alias("pm25_lag0"),
        F.lag("pm25", 1).over(w_order).cast("double").alias("pm25_lag1"),
        F.lag("pm25", 3).over(w_order).cast("double").alias("pm25_lag3"),
        F.lag("pm25", 6).over(w_order).cast("double").alias("pm25_lag6"),
        F.lag("pm25", 12).over(w_order).cast("double").alias("pm25_lag12"),
        F.lag("pm25", 24).over(w_order).cast("double").alias("pm25_lag24"),

        # 1.2 Rolling Averages (2) - min_periods guard
        F.when(
            F.count("pm25").over(w_roll6) == 6,
            F.avg("pm25").over(w_roll6)
        ).otherwise(F.lit(None)).cast("double").alias("pm25_roll6"),

        F.when(
            F.count("pm25").over(w_roll24) == 24,
            F.avg("pm25").over(w_roll24)
        ).otherwise(F.lit(None)).cast("double").alias("pm25_roll24"),

        # 1.3 PM2.5 Dynamics (3)
        (F.col("pm25") - F.lag("pm25", 1).over(w_order)).cast("double").alias("pm25_diff_1h"),
        (F.col("pm25") - F.lag("pm25", 24).over(w_order)).cast("double").alias("pm25_diff_24h"),
        F.when(
            F.count("pm25").over(w_roll24) == 24,
            F.coalesce(F.stddev_samp("pm25").over(w_roll24), F.lit(0.0))
        ).otherwise(F.lit(None)).cast("double").alias("pm25_std24"),

        # 1.4 Co-pollutants (2)
        F.col("pm10").cast("double").alias("pm10_lag0"),
        F.col("aqi").cast("double").alias("aqi_lag0"),

        # 1.5 Future Weather at target time t+24h (8)
        F.lead("cur_temperature_2m", 24).over(w_order).cast("double").alias("temperature_2m"),
        F.lead("cur_relative_humidity_2m", 24).over(w_order).cast("double").alias("relative_humidity_2m"),
        F.lead("cur_dew_point_2m", 24).over(w_order).cast("double").alias("dew_point_2m"),
        F.lead("cur_surface_pressure", 24).over(w_order).cast("double").alias("surface_pressure"),
        F.lead("cur_precipitation", 24).over(w_order).cast("double").alias("precipitation"),
        F.lead("cur_wind_speed_10m", 24).over(w_order).cast("double").alias("wind_speed_10m"),
        F.lead("cur_wind_direction_10m", 24).over(w_order).cast("double").alias("wind_direction_10m"),
        F.lead("cur_boundary_layer_height", 24).over(w_order).cast("double").alias("boundary_layer_height"),

        # Intermediate columns for thermodynamic difference
        F.col("cur_temperature_2m").cast("double").alias("_cur_temp"),
        F.col("cur_surface_pressure").cast("double").alias("_cur_pressure"),
        F.col("cur_boundary_layer_height").cast("double").alias("_cur_pblh"),
        F.col("cur_precipitation").cast("double").alias("_cur_precip"),
    )

    # 2. Thermodynamic Derived features (8)
    df_feat = df_feat.withColumn("wind_dir_sin", F.sin(F.radians(F.col("wind_direction_10m"))).cast("double"))
    df_feat = df_feat.withColumn("wind_dir_cos", F.cos(F.radians(F.col("wind_direction_10m"))).cast("double"))
    df_feat = df_feat.withColumn("dew_point_deficit", (F.col("temperature_2m") - F.col("dew_point_2m")).cast("double"))
    df_feat = df_feat.withColumn("ventilation_coef", (F.col("wind_speed_10m") * F.col("boundary_layer_height")).cast("double"))
    df_feat = df_feat.withColumn("temp_diff_24h", (F.col("temperature_2m") - F.col("_cur_temp")).cast("double"))
    df_feat = df_feat.withColumn("pressure_diff_24h", (F.col("surface_pressure") - F.col("_cur_pressure")).cast("double"))
    df_feat = df_feat.withColumn("pblh_diff_24h", (F.col("boundary_layer_height") - F.col("_cur_pblh")).cast("double"))
    df_feat = df_feat.withColumn(
        "precip_sum_24h",
        F.when(
            F.count("_cur_precip").over(w_fwd24) == 24,
            F.sum("_cur_precip").over(w_fwd24)
        ).otherwise(F.lit(None)).cast("double")
    )

    # 3. Time Target & Cyclic Time features
    df_feat = df_feat.withColumn("hour", F.hour("target_time").cast("double"))
    df_feat = df_feat.withColumn("day_of_week", F.weekday("target_time").cast("double"))
    df_feat = df_feat.withColumn("hour_sin", F.sin(F.lit(2.0 * math.pi) * F.col("hour") / F.lit(24.0)).cast("double"))
    df_feat = df_feat.withColumn("hour_cos", F.cos(F.lit(2.0 * math.pi) * F.col("hour") / F.lit(24.0)).cast("double"))
    df_feat = df_feat.withColumn("dow_sin", F.sin(F.lit(2.0 * math.pi) * F.col("day_of_week") / F.lit(7.0)).cast("double"))
    df_feat = df_feat.withColumn("dow_cos", F.cos(F.lit(2.0 * math.pi) * F.col("day_of_week") / F.lit(7.0)).cast("double"))

    # 4. Spatial Coordinate Normalization to [-1.0, 1.0]
    lat_span = LAT_MAX - LAT_MIN
    lon_span = LON_MAX - LON_MIN
    df_feat = df_feat.withColumn(
        "latitude_norm",
        ((F.lit(2.0) * (F.col("latitude") - F.lit(LAT_MIN)) / F.lit(lat_span)) - F.lit(1.0)).cast("double")
    )
    df_feat = df_feat.withColumn(
        "longitude_norm",
        ((F.lit(2.0) * (F.col("longitude") - F.lit(LON_MIN)) / F.lit(lon_span)) - F.lit(1.0)).cast("double")
    )

    # 5. Binary Weekend Indicator
    df_feat = df_feat.withColumn(
        "is_weekend",
        F.when(F.col("day_of_week") >= 5.0, F.lit(1.0)).otherwise(F.lit(0.0)).cast("double")
    )

    # 6. One-Hot Station Identifiers (11)
    for st_id in CANONICAL_STATION_IDS:
        df_feat = df_feat.withColumn(
            f"station_id_{st_id}",
            F.when(F.col("station_id") == F.lit(st_id), F.lit(1.0)).otherwise(F.lit(0.0)).cast("double")
        )

    # 7. Station Index (IntegerType)
    idx_expr = F.when(F.col("station_id") == F.lit(STATION_METADATA_MAPPING[0][0]), F.lit(0))
    for st_id, _, idx in STATION_METADATA_MAPPING[1:]:
        idx_expr = idx_expr.when(F.col("station_id") == F.lit(st_id), F.lit(idx))
    idx_expr = idx_expr.otherwise(F.lit(0)).cast("integer")
    df_feat = df_feat.withColumn("station_idx", idx_expr)

    # 8. Boundary Trimming (Drop 24h lookback and 24h lookahead nulls = 48 boundary rows per province)
    df_valid = df_feat.filter(F.col("pm25_lag24").isNotNull() & F.col("y").isNotNull())

    # 9. Cutoff Target Date Filtering if specified
    if cutoff_target_date:
        logger.info("Applying target_time cutoff filter: target_time <= '%s'", cutoff_target_date)
        df_valid = df_valid.filter(
            F.col("target_time") <= F.to_timestamp(F.lit(cutoff_target_date), "yyyy-MM-dd HH:mm:ss")
        )

    # 10. Select canonical column order matching GOLD_FEATURE_SCHEMA
    final_cols = [
        "station_id", "station_code", "station_name", "province_id",
        "latitude", "longitude", "origin_time", "target_time", "datetime",
        "station_idx", "y"
    ] + CANONICAL_49_FEATURES + ["province_name"]

    df_gold = df_valid.select(*final_cols)
    return df_gold


def process_gold_feature_engineering(
    spark: SparkSession,
    silver_tedp_dir: Path,
    silver_weather_dir: Path,
    output_dir: Path,
    cutoff_target_date: Optional[str] = None,
) -> DataFrame:
    """Executes the complete Gold Feature Engineering transformation and writes partitioned Parquet.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    silver_tedp_dir : Path
        Input path to Silver TEDP dataset.
    silver_weather_dir : Path
        Input path to Silver Weather dataset.
    output_dir : Path
        Target output path for Gold feature dataset.
    cutoff_target_date : Optional[str]
        Optional cutoff date timestamp string.

    Returns
    -------
    DataFrame
        Written Gold feature DataFrame.
    """
    logger.info("Starting Gold 49 Atmospheric Features Pipeline...")
    start_time = time.time()

    df_gold = build_gold_features_dataframe(
        spark=spark,
        silver_tedp_dir=silver_tedp_dir,
        silver_weather_dir=silver_weather_dir,
        cutoff_target_date=cutoff_target_date,
    )

    purge_output_directory(output_dir)

    logger.info("Writing Gold feature dataset to: %s (partitioned by province_name, Snappy compressed)", output_dir)
    (
        df_gold.write
        .mode("overwrite")
        .partitionBy("province_name")
        .parquet(str(output_dir))
    )

    elapsed = time.time() - start_time
    logger.info("Gold Feature Engineering successfully completed in %.2f seconds.", elapsed)
    return df_gold


def main() -> None:
    """CLI Entry point for Step 3: Silver to Gold Feature Engineering Pipeline."""
    parser = argparse.ArgumentParser(description="Step 3: Silver to Gold 49 Features Pipeline (PySpark)")
    parser.add_argument("--silver-tedp-dir", type=str, default="lakehouse_storage/silver/tedp_province",
                        help="Path to Silver TEDP directory (partitioned by province_name)")
    parser.add_argument("--silver-weather-dir", type=str, default="lakehouse_storage/silver/weather_province",
                        help="Path to Silver Weather directory (partitioned by province_name)")
    parser.add_argument("--output-dir", type=str, default="lakehouse_storage/gold/global_ann_features",
                        help="Output directory for Gold feature table")
    parser.add_argument("--cutoff-target-date", type=str, default=None,
                        help="Optional cutoff date (e.g. '2026-11-06 23:00:00' for 279,840 rows)")
    parser.add_argument("--keep-ui", action="store_true", help="Keep Spark Web UI active until interrupted")
    parser.add_argument("--keep-ui-seconds", type=int, default=0, help="Seconds to hold Spark Web UI open")
    args = parser.parse_args()

    project_root = get_project_root()
    silver_tedp_dir = project_root / args.silver_tedp_dir
    silver_weather_dir = project_root / args.silver_weather_dir
    output_dir = project_root / args.output_dir

    spark = get_spark_session(
        app_name="AirQualityLakehouse_Step3_GoldFeatures",
        keep_ui=args.keep_ui,
    )

    try:
        df_gold = process_gold_feature_engineering(
            spark=spark,
            silver_tedp_dir=silver_tedp_dir,
            silver_weather_dir=silver_weather_dir,
            output_dir=output_dir,
            cutoff_target_date=args.cutoff_target_date,
        )

        total_rows = df_gold.count()
        print(f"\nSuccessfully generated Gold Features table at: {output_dir}")
        print(f"Total Rows: {total_rows} (Expected 280,104 full / 279,840 filtered)")

        if args.keep_ui or args.keep_ui_seconds > 0:
            wait_for_webui(spark, keep_ui_seconds=args.keep_ui_seconds)
    finally:
        if not (args.keep_ui or args.keep_ui_seconds > 0):
            spark.stop()


if __name__ == "__main__":
    main()
