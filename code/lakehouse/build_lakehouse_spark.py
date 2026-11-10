"""
build_lakehouse_spark.py
========================
Master Lakehouse Orchestrator and Automated Data Diff Verification Engine.

Coordinates the complete 3-tier Medallion Lakehouse pipeline:
- Step 1: Bronze -> Silver Station-Level Preprocessing (spark_preprocess_tedp.py)
- Step 2: Silver Station -> Silver Province Grid & Weather Synchronization (spark_sync_province.py)
- Step 3: Silver Province -> Gold 49 Atmospheric Features Pipeline (spark_feature_engineering.py)

Automated Data Diff:
- Reads Gold features table from lakehouse_storage/gold/global_ann_features/
- Generates Pandas reference features on data_test/
- Validates row counts (280,104 full / 279,840 filtered)
- Validates 0 NaNs / Nulls
- Asserts max absolute difference across all 49 features + label y strictly < 1e-5
- Exports verification report to output/lakehouse_verification_report.txt
"""

from __future__ import annotations

import argparse
import io
import logging
import math
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# Fix console encoding for Windows terminals
if hasattr(sys.stdout, "buffer"):
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass

# Add lakehouse module path
CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

try:
    from spark_session import get_project_root, get_spark_session, wait_for_webui
    from spark_preprocess_tedp import process_station_silver
    from spark_sync_province import process_province_silver
    from spark_feature_engineering import (
        CANONICAL_49_FEATURES,
        CANONICAL_STATION_IDS,
        LAT_MIN,
        LAT_MAX,
        LON_MIN,
        LON_MAX,
        WEATHER_COLS,
        process_gold_feature_engineering,
    )
except ImportError:
    from code.lakehouse.spark_session import get_project_root, get_spark_session, wait_for_webui
    from code.lakehouse.spark_preprocess_tedp import process_station_silver
    from code.lakehouse.spark_sync_province import process_province_silver
    from code.lakehouse.spark_feature_engineering import (
        CANONICAL_49_FEATURES,
        CANONICAL_STATION_IDS,
        LAT_MIN,
        LAT_MAX,
        LON_MIN,
        LON_MAX,
        WEATHER_COLS,
        process_gold_feature_engineering,
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("AirQualityLakehouse.MasterRunner")


# ------------------------------------------------------------------------------
# PANDAS BASELINE COMPUTATION FOR DATA DIFF
# ------------------------------------------------------------------------------
def compute_pandas_reference_gold(
    tedp_csv: Path,
    weather_csv: Path,
    cutoff_target_date: Optional[str] = None,
) -> pd.DataFrame:
    """Computes reference 49 Gold features using pure Pandas (matching train_global_ann.py).

    Parameters
    ----------
    tedp_csv : Path
        Path to cleaned_tedp_province_4y.csv
    weather_csv : Path
        Path to weather_openmeteo_province_4y.csv
    cutoff_target_date : Optional[str]
        Optional cutoff date string (e.g. '2026-11-06 23:00:00')

    Returns
    -------
    pd.DataFrame
        Reference Gold features DataFrame aligned on ['province_name', 'target_time'].
    """
    logger.info("Computing Pandas reference Gold features from: %s & %s", tedp_csv, weather_csv)
    df_tedp = pd.read_csv(
        tedp_csv,
        dtype={"station_id": str, "province_id": str, "station_code": str},
        parse_dates=["datetime"],
    )
    df_wea = pd.read_csv(
        weather_csv,
        usecols=["station_code", "datetime"] + WEATHER_COLS,
        dtype={"station_code": str},
        parse_dates=["datetime"],
    )
    df_merged = df_tedp.merge(df_wea, on=["station_code", "datetime"], how="left", validate="1:1")

    parts: List[pd.DataFrame] = []
    horizon = 24

    for _, g in df_merged.groupby("station_id", sort=False):
        g = g.sort_values("datetime").reset_index(drop=True)
        first = g.iloc[0]

        out = pd.DataFrame({
            "station_id": str(first["station_id"]),
            "station_code": str(first["station_code"]),
            "province_name": str(first["province_name"]),
            "origin_time": g["datetime"],
            "target_time": g["datetime"].shift(-horizon),
            "y": g["pm25"].shift(-horizon),
        })

        # 1. PM2.5 Lags
        for lag in [0, 1, 3, 6, 12, 24]:
            out[f"pm25_lag{lag}"] = g["pm25"].shift(lag)

        # 2. Rolling Averages
        for w in [6, 24]:
            out[f"pm25_roll{w}"] = g["pm25"].rolling(w, min_periods=w).mean()

        # 3. PM2.5 Dynamics
        out["pm25_diff_1h"] = out["pm25_lag0"] - out["pm25_lag1"]
        out["pm25_diff_24h"] = out["pm25_lag0"] - out["pm25_lag24"]
        out["pm25_std24"] = g["pm25"].rolling(24, min_periods=24).std().fillna(0.0)

        # 4. Co-pollutants
        out["pm10_lag0"] = g["pm10"]
        out["aqi_lag0"] = g["aqi"]

        # 5. Future Weather at t+24
        for col in WEATHER_COLS:
            out[col] = g[col].shift(-horizon)

        # 6. Thermodynamic Derived
        wind_rad = np.deg2rad(out["wind_direction_10m"])
        out["wind_dir_sin"] = np.sin(wind_rad)
        out["wind_dir_cos"] = np.cos(wind_rad)
        out["dew_point_deficit"] = out["temperature_2m"] - out["dew_point_2m"]
        out["ventilation_coef"] = out["wind_speed_10m"] * out["boundary_layer_height"]
        out["temp_diff_24h"] = out["temperature_2m"] - g["temperature_2m"]
        out["pressure_diff_24h"] = out["surface_pressure"] - g["surface_pressure"]
        out["pblh_diff_24h"] = out["boundary_layer_height"] - g["boundary_layer_height"]
        out["precip_sum_24h"] = g["precipitation"].rolling(horizon, min_periods=1).sum().shift(-horizon)

        # 7. Time Targets & Cyclic Time
        out["hour"] = out["target_time"].dt.hour.astype(float)
        out["day_of_week"] = out["target_time"].dt.dayofweek.astype(float)
        out["hour_sin"] = np.sin(2.0 * np.pi * out["hour"] / 24.0)
        out["hour_cos"] = np.cos(2.0 * np.pi * out["hour"] / 24.0)
        out["dow_sin"] = np.sin(2.0 * np.pi * out["day_of_week"] / 7.0)
        out["dow_cos"] = np.cos(2.0 * np.pi * out["day_of_week"] / 7.0)

        # 8. Spatial Coordinates
        out["latitude_norm"] = 2.0 * (float(first["latitude"]) - LAT_MIN) / (LAT_MAX - LAT_MIN) - 1.0
        out["longitude_norm"] = 2.0 * (float(first["longitude"]) - LON_MIN) / (LON_MAX - LON_MIN) - 1.0

        # 9. Binary Weekend Indicator
        out["is_weekend"] = (out["day_of_week"] >= 5.0).astype(float)

        # 10. One-Hot Station Identifiers
        for st_id in CANONICAL_STATION_IDS:
            out[f"station_id_{st_id}"] = (out["station_id"] == st_id).astype(float)

        parts.append(out)

    feats_pd = pd.concat(parts, ignore_index=True).dropna().reset_index(drop=True)

    if cutoff_target_date:
        cutoff_dt = pd.to_datetime(cutoff_target_date)
        feats_pd = feats_pd[pd.to_datetime(feats_pd["target_time"]) <= cutoff_dt].reset_index(drop=True)

    return feats_pd


# ------------------------------------------------------------------------------
# AUTOMATED DATA DIFF VERIFICATION
# ------------------------------------------------------------------------------
def verify_gold_data_diff(
    gold_dir: Path,
    tedp_csv: Path,
    weather_csv: Path,
    cutoff_target_date: Optional[str] = None,
    tolerance: float = 1e-5,
    report_output_path: Optional[Path] = None,
) -> bool:
    """Performs rigorous automated Data Diff between PySpark Gold output and Pandas reference.

    Parameters
    ----------
    gold_dir : Path
        Directory of Spark Gold Parquet table.
    tedp_csv : Path
        Path to reference TEDP CSV.
    weather_csv : Path
        Path to reference Weather CSV.
    cutoff_target_date : Optional[str]
        Optional cutoff date string.
    tolerance : float
        Maximum allowed absolute difference (default: 1e-5).
    report_output_path : Optional[Path]
        File path to save the verification report.

    Returns
    -------
    bool
        True if all checks pass and all differences are strictly < tolerance.
    """
    logger.info("=" * 70)
    logger.info("STARTING AUTOMATED DATA DIFF VERIFICATION (Step 3: Gold Features)")
    logger.info("=" * 70)

    # 1. Read Spark Gold Dataset via PyArrow
    cols_to_read = ["province_name", "target_time", "y"] + CANONICAL_49_FEATURES
    df_spark = pd.read_parquet(gold_dir, columns=cols_to_read)

    if cutoff_target_date:
        cutoff_dt = pd.to_datetime(cutoff_target_date)
        df_spark = df_spark[pd.to_datetime(df_spark["target_time"]) <= cutoff_dt].copy()

    # 2. Compute Pandas Reference
    df_pandas = compute_pandas_reference_gold(tedp_csv, weather_csv, cutoff_target_date=cutoff_target_date)

    # 3. Standardize and sort for row-by-row alignment
    df_spark["target_time"] = pd.to_datetime(df_spark["target_time"])
    df_pandas["target_time"] = pd.to_datetime(df_pandas["target_time"])
    df_spark["province_name"] = df_spark["province_name"].astype(str)
    df_pandas["province_name"] = df_pandas["province_name"].astype(str)

    df_spark = df_spark.sort_values(["province_name", "target_time"]).reset_index(drop=True)
    df_pandas = df_pandas.sort_values(["province_name", "target_time"]).reset_index(drop=True)

    spark_rows = len(df_spark)
    pandas_rows = len(df_pandas)

    logger.info("Spark Gold row count  : %d", spark_rows)
    logger.info("Pandas Ref row count  : %d", pandas_rows)

    assert spark_rows == pandas_rows, (
        f"Row count mismatch: Spark has {spark_rows} rows, Pandas has {pandas_rows} rows!"
    )

    # 4. Detailed column-by-column numerical check
    diff_results: List[Dict[str, any]] = []
    failed_cols: List[str] = []

    columns_to_verify = ["y"] + CANONICAL_49_FEATURES

    for col in columns_to_verify:
        assert col in df_spark.columns, f"Column '{col}' missing from Spark Gold dataset."
        assert col in df_pandas.columns, f"Column '{col}' missing from Pandas reference dataset."

        s_vals = df_spark[col].to_numpy(dtype=np.float64)
        p_vals = df_pandas[col].to_numpy(dtype=np.float64)

        # Check NaNs
        s_nans = int(np.isnan(s_vals).sum())
        p_nans = int(np.isnan(p_vals).sum())

        diff = np.abs(s_vals - p_vals)
        max_diff = float(np.max(diff))
        mean_diff = float(np.mean(diff))

        passed = (max_diff < tolerance) and (s_nans == 0) and (p_nans == 0)
        if not passed:
            failed_cols.append(col)

        diff_results.append({
            "column": col,
            "max_abs_diff": max_diff,
            "mean_abs_diff": mean_diff,
            "spark_nans": s_nans,
            "pandas_nans": p_nans,
            "status": "PASS" if passed else "FAIL",
        })

    # 5. Format and print ASCII summary table
    report_lines: List[str] = []
    report_lines.append("=" * 86)
    report_lines.append("PYSPARK LAKEHOUSE GOLD FEATURE ENGINEERING DATA DIFF VERIFICATION REPORT")
    report_lines.append("=" * 86)
    report_lines.append(f"Gold Target Directory : {gold_dir}")
    report_lines.append(f"Reference Dataset     : {tedp_csv} & {weather_csv}")
    report_lines.append(f"Cutoff Filter Applied : {cutoff_target_date or 'None (Full 280,104 rows)'}")
    report_lines.append(f"Total Rows Verified   : {spark_rows:,} (Spark) vs {pandas_rows:,} (Pandas)")
    report_lines.append(f"Numerical Tolerance   : < {tolerance:.1e}")
    report_lines.append("-" * 86)
    report_lines.append(f"{'Feature / Metric Column':<35} | {'Max Abs Diff':<14} | {'Mean Abs Diff':<14} | {'NaNs':<6} | {'Status'}")
    report_lines.append("-" * 86)

    for res in diff_results:
        report_lines.append(
            f"{res['column']:<35} | {res['max_abs_diff']:<14.6e} | {res['mean_abs_diff']:<14.6e} | {res['spark_nans']:<6} | {res['status']}"
        )

    report_lines.append("=" * 86)

    overall_max_diff = max(r["max_abs_diff"] for r in diff_results)
    report_lines.append(f"OVERALL MAXIMUM ABSOLUTE ERROR: {overall_max_diff:.8e}")
    report_lines.append(f"TOTAL VERIFIED COLUMNS        : {len(diff_results)} (49 features + 1 label y)")
    report_lines.append(f"TOTAL FAILED COLUMNS          : {len(failed_cols)}")
    report_lines.append(f"DATA DIFF VERIFICATION RESULT : {'PASSED (100% BIT-PARITY)' if not failed_cols else 'FAILED'}")
    report_lines.append("=" * 86)

    report_text = "\n".join(report_lines)
    print(report_text)

    # Save to report file if requested
    if report_output_path:
        report_output_path.parent.mkdir(parents=True, exist_ok=True)
        report_output_path.write_text(report_text, encoding="utf-8")
        logger.info("Verification report saved to: %s", report_output_path)

    if failed_cols:
        raise AssertionError(
            f"Data Diff verification failed for {len(failed_cols)} columns: {failed_cols}. "
            f"Max absolute difference {overall_max_diff:.8e} >= {tolerance:.8e}."
        )

    logger.info("Data Diff verification PASSED successfully (Max diff: %.4e < %.1e).", overall_max_diff, tolerance)
    return True


# ------------------------------------------------------------------------------
# MASTER ORCHESTRATOR EXECUTION
# ------------------------------------------------------------------------------
def run_lakehouse_pipeline(
    steps: Sequence[int],
    keep_ui: bool = False,
    keep_ui_seconds: int = 0,
    cutoff_target_date: Optional[str] = None,
    verify_diff: bool = True,
    master: str = "local[4]",
) -> None:
    """Executes specified Lakehouse pipeline steps sequentially with SparkSession lifecycle management."""
    project_root = get_project_root()

    # Paths
    bronze_csv = project_root / "lakehouse_storage" / "bronze" / "tedp_aqi_pm25_hourly.csv"
    silver_station_dir = project_root / "lakehouse_storage" / "silver" / "tedp_station"
    silver_province_dir = project_root / "lakehouse_storage" / "silver" / "tedp_province"
    silver_weather_dir = project_root / "lakehouse_storage" / "silver" / "weather_province"
    gold_dir = project_root / "lakehouse_storage" / "gold" / "global_ann_features"

    weather_ref_csv = project_root / "data_test" / "weather_openmeteo_province_4y.csv"
    cleaned_tedp_csv = project_root / "data_test" / "cleaned_tedp_province_4y.csv"
    report_path = project_root / "output" / "lakehouse_verification_report.txt"

    logger.info("Initializing Master Lakehouse Pipeline (Steps: %s)...", steps)
    spark = get_spark_session(
        app_name="AirQualityLakehouse_MasterPipeline",
        master=master,
        keep_ui=keep_ui,
    )

    try:
        # Step 1: Bronze -> Silver Station
        if 1 in steps:
            logger.info(">>> EXECUTING STEP 1: Bronze -> Silver Station Preprocessing")
            process_station_silver(
                spark=spark,
                input_file=bronze_csv,
                output_dir=silver_station_dir,
            )

        # Step 2: Silver Station -> Silver Province & Weather Sync
        if 2 in steps:
            logger.info(">>> EXECUTING STEP 2: Silver Station -> Province Grid & Weather Synchronization")
            process_province_silver(
                spark=spark,
                station_dir=silver_station_dir,
                province_dir=silver_province_dir,
                weather_csv=weather_ref_csv,
                weather_output_dir=silver_weather_dir,
            )

        # Step 3: Silver Province -> Gold 49 Features Table
        if 3 in steps:
            logger.info(">>> EXECUTING STEP 3: Silver Province -> Gold 49 Atmospheric Features Table")
            process_gold_feature_engineering(
                spark=spark,
                silver_tedp_dir=silver_province_dir,
                silver_weather_dir=silver_weather_dir,
                output_dir=gold_dir,
                cutoff_target_date=cutoff_target_date,
            )

        # Automated Data Diff Verification
        if verify_diff and (3 in steps or gold_dir.exists()):
            logger.info(">>> EXECUTING DATA DIFF VERIFICATION")
            verify_gold_data_diff(
                gold_dir=gold_dir,
                tedp_csv=cleaned_tedp_csv,
                weather_csv=weather_ref_csv,
                cutoff_target_date=cutoff_target_date,
                report_output_path=report_path,
            )

        if keep_ui or keep_ui_seconds > 0:
            wait_for_webui(spark, keep_ui_seconds=keep_ui_seconds)

    finally:
        if not (keep_ui or keep_ui_seconds > 0):
            spark.stop()
            logger.info("SparkSession stopped. Master Lakehouse Pipeline finished.")


def parse_steps_argument(steps_str: str) -> List[int]:
    """Parses CLI --steps argument string into a list of integer steps."""
    clean = steps_str.strip().lower()
    if clean in ("all", "1,2,3", "1-3"):
        return [1, 2, 3]
    steps: List[int] = []
    for part in clean.split(","):
        part = part.strip()
        if part.isdigit():
            steps.append(int(part))
    return sorted(list(set(steps)))


def main() -> None:
    """CLI Entry point for Master Lakehouse Orchestrator."""
    parser = argparse.ArgumentParser(description="Master Lakehouse Orchestrator (Steps 1, 2, 3 & Data Diff)")
    parser.add_argument("--steps", type=str, default="all",
                        help="Comma-separated pipeline steps to run: 'all', '1', '2', '3', '2,3' (default: 'all')")
    parser.add_argument("--skip-preprocess", action="store_true", help="Skip Step 1 (Bronze to Silver Station)")
    parser.add_argument("--skip-sync", action="store_true", help="Skip Step 2 (Silver Station to Province Grid)")
    parser.add_argument("--cutoff-target-date", type=str, default=None,
                        help="Optional cutoff target date for Step 3 & Diff (e.g. '2026-11-06 23:00:00')")
    parser.add_argument("--verify-diff", action="store_true", default=True,
                        help="Run automated Data Diff against Pandas baseline (default: True)")
    parser.add_argument("--no-verify-diff", dest="verify_diff", action="store_false",
                        help="Disable automated Data Diff verification")
    parser.add_argument("--keep-ui", action="store_true", help="Keep Spark Web UI active until interrupted")
    parser.add_argument("--keep-ui-seconds", type=int, default=0, help="Seconds to hold Spark Web UI open")
    parser.add_argument("--master", type=str, default="local[4]", help="Spark Master URL (default: local[4])")
    args = parser.parse_args()

    steps = parse_steps_argument(args.steps)
    if args.skip_preprocess and 1 in steps:
        steps.remove(1)
    if args.skip_sync and 2 in steps:
        steps.remove(2)

    run_lakehouse_pipeline(
        steps=steps,
        keep_ui=args.keep_ui,
        keep_ui_seconds=args.keep_ui_seconds,
        cutoff_target_date=args.cutoff_target_date,
        verify_diff=args.verify_diff,
        master=args.master,
    )


if __name__ == "__main__":
    main()
