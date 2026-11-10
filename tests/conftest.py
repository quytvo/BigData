"""Shared test fixtures, path configurations, and assertion utilities for PySpark Air Quality Lakehouse."""

import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Union
import numpy as np
import pandas as pd
import pytest

# ==============================================================================
# Path Configurations
# ==============================================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TESTS_DIR = Path(__file__).resolve().parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))
CODE_DIR = PROJECT_ROOT / "code"
DATA_TEST_DIR = PROJECT_ROOT / "data_test"
LAKEHOUSE_STORAGE_DIR = PROJECT_ROOT / "lakehouse_storage"

BRONZE_DIR = LAKEHOUSE_STORAGE_DIR / "bronze"
BRONZE_TEDP_CSV = BRONZE_DIR / "tedp_aqi_pm25_hourly.csv"

SILVER_DIR = LAKEHOUSE_STORAGE_DIR / "silver"
SILVER_STATION_DIR = SILVER_DIR / "tedp_station"
SILVER_PROVINCE_DIR = SILVER_DIR / "tedp_province"
SILVER_WEATHER_DIR = SILVER_DIR / "weather_province"

GOLD_DIR = LAKEHOUSE_STORAGE_DIR / "gold"
GOLD_FEATURES_DIR = GOLD_DIR / "global_ann_features"

OUTPUT_DIR = PROJECT_ROOT / "output"
SPARK_EVENT_LOGS_DIR = OUTPUT_DIR / "spark_event_logs"
BENCHMARK_DIR = OUTPUT_DIR / "cloudsim_benchmark"
BENCHMARK_CSV = BENCHMARK_DIR / "spark_storage_benchmark.csv"

CLEANED_TEDP_CSV = DATA_TEST_DIR / "cleaned_tedp_province_4y.csv"
WEATHER_OPENMETEO_CSV = DATA_TEST_DIR / "weather_openmeteo_province_4y.csv"
BEST_ANN_MODEL_PT = OUTPUT_DIR / "global_ann_4y" / "best_ann_model.pt"

DOCKERFILE_SPARK_ROOT = PROJECT_ROOT / "Dockerfile.spark"
DOCKERFILE_SPARK_CODE = CODE_DIR / "lakehouse" / "Dockerfile.spark"

# ==============================================================================
# Ground Truth Constants
# ==============================================================================
EXPECTED_PROVINCE_COUNT = 11
EXPECTED_HOURS_PER_PROVINCE = 25512
EXPECTED_TOTAL_SILVER_ROWS = 280632  # 11 * 25512
START_TIMESTAMP = "2023-12-11 00:00:00"
END_TIMESTAMP = "2026-11-07 23:00:00"

# Gold dataset row counts
EXPECTED_GOLD_ROWS_FULL = 280104  # (25512 - 48 boundary rows) * 11
EXPECTED_GOLD_ROWS_FILTERED = 279840  # (25488 - 48 boundary rows) * 11
CUTOFF_FILTER_TIMESTAMP = "2026-11-06 23:00:00"

NUMERICAL_TOLERANCE = 1e-5

# Canonical 49 Atmospheric Features as defined in train_global_ann.py and best_ann_model.pt
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


# ==============================================================================
# Helper Assertions
# ==============================================================================
def assert_max_abs_diff(
    actual: Union[pd.DataFrame, "pyspark.sql.DataFrame"],
    expected: Union[pd.DataFrame, "pyspark.sql.DataFrame"],
    columns: Sequence[str],
    max_diff: float = NUMERICAL_TOLERANCE,
    key_columns: Sequence[str] = ("province_name", "datetime"),
) -> None:
    """Assert that maximum absolute difference between actual and expected DataFrames is strictly < max_diff.

    Handles alignment by key_columns if present.
    """
    # Convert PySpark DataFrames to Pandas if necessary
    if hasattr(actual, "toPandas"):
        actual_df = actual.toPandas()
    else:
        actual_df = actual.copy()

    if hasattr(expected, "toPandas"):
        expected_df = expected.toPandas()
    else:
        expected_df = expected.copy()

    # Check key columns for alignment
    join_keys = [k for k in key_columns if k in actual_df.columns and k in expected_df.columns]
    if join_keys:
        # Standardize key column types
        for k in join_keys:
            if "time" in k or k == "datetime":
                actual_df[k] = pd.to_datetime(actual_df[k])
                expected_df[k] = pd.to_datetime(expected_df[k])
            else:
                actual_df[k] = actual_df[k].astype(str)
                expected_df[k] = expected_df[k].astype(str)

        actual_df = actual_df.sort_values(join_keys).reset_index(drop=True)
        expected_df = expected_df.sort_values(join_keys).reset_index(drop=True)

    assert len(actual_df) == len(expected_df), (
        f"Row count mismatch: actual has {len(actual_df)} rows, expected has {len(expected_df)} rows."
    )

    for col in columns:
        assert col in actual_df.columns, f"Column '{col}' missing from actual DataFrame."
        assert col in expected_df.columns, f"Column '{col}' missing from expected DataFrame."

        act_vals = actual_df[col].to_numpy(dtype=np.float64)
        exp_vals = expected_df[col].to_numpy(dtype=np.float64)

        # Check NaN alignment
        act_nans = np.isnan(act_vals)
        exp_nans = np.isnan(exp_vals)
        assert np.array_equal(act_nans, exp_nans), (
            f"NaN mask mismatch in column '{col}': actual NaNs={np.sum(act_nans)}, expected NaNs={np.sum(exp_nans)}"
        )

        valid_mask = ~act_nans
        if np.any(valid_mask):
            diffs = np.abs(act_vals[valid_mask] - exp_vals[valid_mask])
            max_val = float(np.max(diffs))
            max_idx = int(np.argmax(diffs))
            assert max_val < max_diff, (
                f"Numerical tolerance exceeded for column '{col}': "
                f"max absolute difference {max_val:.8e} >= {max_diff:.8e} "
                f"(at row index {max_idx}: actual={act_vals[valid_mask][max_idx]}, expected={exp_vals[valid_mask][max_idx]})."
            )


def assert_temporal_continuity(
    df: pd.DataFrame,
    datetime_col: str = "datetime",
    province_col: str = "province_name",
    expected_hours_per_province: int = EXPECTED_HOURS_PER_PROVINCE,
    start_time: str = START_TIMESTAMP,
    end_time: str = END_TIMESTAMP,
) -> None:
    """Verifies each province has exact continuous hourly steps without gaps or duplicates."""
    assert province_col in df.columns, f"Province column '{province_col}' missing from DataFrame."
    assert datetime_col in df.columns, f"Datetime column '{datetime_col}' missing from DataFrame."

    expected_range = pd.date_range(start=start_time, end=end_time, freq="h")
    assert len(expected_range) == expected_hours_per_province, (
        f"Internal error: expected range length {len(expected_range)} != {expected_hours_per_province}"
    )

    provinces = df[province_col].unique()
    assert len(provinces) == EXPECTED_PROVINCE_COUNT, (
        f"Expected {EXPECTED_PROVINCE_COUNT} provinces, but found {len(provinces)}: {sorted(provinces)}"
    )

    for prov in provinces:
        prov_df = df[df[province_col] == prov].sort_values(datetime_col).reset_index(drop=True)
        assert len(prov_df) == expected_hours_per_province, (
            f"Province '{prov}' has {len(prov_df)} rows, expected exactly {expected_hours_per_province} rows."
        )

        actual_times = pd.to_datetime(prov_df[datetime_col])
        # Check start and end
        assert str(actual_times.iloc[0]) == start_time, (
            f"Province '{prov}' start time is {actual_times.iloc[0]}, expected {start_time}."
        )
        assert str(actual_times.iloc[-1]) == end_time, (
            f"Province '{prov}' end time is {actual_times.iloc[-1]}, expected {end_time}."
        )

        # Check exact hourly step
        diffs = actual_times.diff().iloc[1:]
        non_one_hour = diffs[diffs != pd.Timedelta(hours=1)]
        assert len(non_one_hour) == 0, (
            f"Province '{prov}' has {len(non_one_hour)} discontinuous time steps: {non_one_hour.head(3)}"
        )


def assert_struct_schema(schema: Union[dict, "pyspark.sql.types.StructType"], expected_field_types: Dict[str, str]) -> None:
    """Asserts that schema fields match expected types and none are inferred as default StringType incorrectly."""
    if hasattr(schema, "fields"):
        # PySpark StructType
        actual_fields = {field.name: field.dataType.simpleString() for field in schema.fields}
    elif isinstance(schema, dict):
        actual_fields = schema
    else:
        raise TypeError(f"Unsupported schema type: {type(schema)}")

    for field_name, expected_type in expected_field_types.items():
        assert field_name in actual_fields, f"Field '{field_name}' not found in schema. Available: {list(actual_fields.keys())}"
        actual_type = actual_fields[field_name].lower()
        exp_type = expected_type.lower()
        # Accept compatible types (e.g. bigint / long / int, double / float, timestamp)
        compatible = (
            (actual_type == exp_type)
            or ("int" in actual_type and "int" in exp_type)
            or ("long" in actual_type and "int" in exp_type)
            or ("double" in actual_type and "float" in exp_type)
            or ("timestamp" in actual_type and "timestamp" in exp_type)
            or ("string" in actual_type and "string" in exp_type)
        )
        assert compatible, (
            f"Field '{field_name}' has type '{actual_fields[field_name]}', expected '{expected_type}'."
        )


def assert_parquet_snappy_partitioned(dir_path: Path, partition_col: str = "province_name", expected_partitions: int = 11) -> None:
    """Verifies that a directory is a valid Parquet dataset partitioned by partition_col."""
    assert dir_path.exists(), f"Directory not found: {dir_path}"
    assert dir_path.is_dir(), f"Path is not a directory: {dir_path}"

    partition_dirs = [p for p in dir_path.iterdir() if p.is_dir() and p.name.startswith(f"{partition_col}=")]
    assert len(partition_dirs) == expected_partitions, (
        f"Expected {expected_partitions} partition subdirectories matching '{partition_col}=*', "
        f"found {len(partition_dirs)}: {[p.name for p in partition_dirs]}"
    )

    # Check parquet files inside partition subdirectories
    total_parquet_files = 0
    for pdir in partition_dirs:
        parquet_files = list(pdir.glob("*.parquet")) + list(pdir.glob("*.snappy.parquet"))
        assert len(parquet_files) > 0, f"Partition directory '{pdir.name}' contains no parquet files."
        total_parquet_files += len(parquet_files)

    assert total_parquet_files >= expected_partitions, f"Total parquet files {total_parquet_files} < {expected_partitions}."


# ==============================================================================
# Pytest Fixtures
# ==============================================================================
@pytest.fixture(scope="session")
def project_paths() -> Dict[str, Path]:
    """Provides standard project paths."""
    return {
        "root": PROJECT_ROOT,
        "code": CODE_DIR,
        "data_test": DATA_TEST_DIR,
        "bronze": BRONZE_DIR,
        "silver": SILVER_DIR,
        "silver_station": SILVER_STATION_DIR,
        "silver_province": SILVER_PROVINCE_DIR,
        "silver_weather": SILVER_WEATHER_DIR,
        "gold": GOLD_DIR,
        "gold_features": GOLD_FEATURES_DIR,
        "output": OUTPUT_DIR,
        "spark_event_logs": SPARK_EVENT_LOGS_DIR,
        "benchmark": BENCHMARK_DIR,
        "benchmark_csv": BENCHMARK_CSV,
        "cleaned_tedp_csv": CLEANED_TEDP_CSV,
        "weather_openmeteo_csv": WEATHER_OPENMETEO_CSV,
        "best_ann_model_pt": BEST_ANN_MODEL_PT,
        "dockerfile_spark": DOCKERFILE_SPARK_ROOT if DOCKERFILE_SPARK_ROOT.exists() else DOCKERFILE_SPARK_CODE,
    }


@pytest.fixture(scope="session")
def ground_truth_provinces() -> List[str]:
    """Authoritative list of 11 provinces from reference dataset."""
    if CLEANED_TEDP_CSV.exists():
        df = pd.read_csv(CLEANED_TEDP_CSV, usecols=["province_name"])
        provinces = sorted(df["province_name"].unique().tolist())
        return provinces
    return [
        "Bắc Ninh", "Gia Lai", "HCM", "Hà Nội", "Hưng Yên",
        "Hải Phòng", "Lâm Đồng", "Ninh Bình", "Quảng Ngãi", "Tây Ninh", "Đà Nẵng"
    ]


@pytest.fixture(scope="session")
def ground_truth_49_features() -> List[str]:
    """Authoritative list of 49 features loaded from model checkpoint or canonical list."""
    if BEST_ANN_MODEL_PT.exists():
        try:
            import torch
            ckpt = torch.load(BEST_ANN_MODEL_PT, map_location="cpu", weights_only=False)
            if "all_feature_cols" in ckpt:
                return ckpt["all_feature_cols"]
        except Exception:
            pass
    return CANONICAL_49_FEATURES


@pytest.fixture(scope="module")
def spark_session():
    """Provides a local SparkSession with Arrow, UTC, and AQE configured."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("PySpark is not installed in the current environment.")

    # Ensure event log directory exists if configuring event logs
    SPARK_EVENT_LOGS_DIR.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("PyTest_AirQuality_Lakehouse")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    yield spark
    spark.stop()
