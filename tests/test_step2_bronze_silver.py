"""Test Suite for Step 2: Bronze to Silver Preprocessing Pipeline.

Verifies:
- lakehouse_storage/silver/tedp_station/: Station-level cleaning, QCVN filter, Snappy Parquet.
- lakehouse_storage/silver/tedp_province/:
  - Static StructType schema (TimestampType, DoubleType, StringType).
  - Snappy Parquet partitioned by province_name.
  - Exactly 11 provinces, exactly 25,512 continuous hourly timestamps per province.
  - Total rows: exactly 280,632 rows.
  - Zero null values in pm25, pm10, aqi, datetime, province_name.
  - Numerical validation: absolute difference of pm25, pm10, aqi vs
    data_test/cleaned_tedp_province_4y.csv strictly < 1e-5.
- lakehouse_storage/silver/weather_province/:
  - Exactly 280,632 rows x 11 provinces.
  - Numerical validation vs data_test/weather_openmeteo_province_4y.csv (< 1e-5).
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

# Ensure project root and tests directory are in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TESTS_DIR = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

try:
    from tests.conftest import (
        CLEANED_TEDP_CSV,
        END_TIMESTAMP,
        EXPECTED_HOURS_PER_PROVINCE,
        EXPECTED_PROVINCE_COUNT,
        EXPECTED_TOTAL_SILVER_ROWS,
        NUMERICAL_TOLERANCE,
        SILVER_DIR,
        SILVER_PROVINCE_DIR,
        SILVER_STATION_DIR,
        SILVER_WEATHER_DIR,
        START_TIMESTAMP,
        WEATHER_OPENMETEO_CSV,
        assert_max_abs_diff,
        assert_parquet_snappy_partitioned,
        assert_struct_schema,
        assert_temporal_continuity,
    )
except ImportError:
    from conftest import (
        CLEANED_TEDP_CSV,
        END_TIMESTAMP,
        EXPECTED_HOURS_PER_PROVINCE,
        EXPECTED_PROVINCE_COUNT,
        EXPECTED_TOTAL_SILVER_ROWS,
        NUMERICAL_TOLERANCE,
        SILVER_DIR,
        SILVER_PROVINCE_DIR,
        SILVER_STATION_DIR,
        SILVER_WEATHER_DIR,
        START_TIMESTAMP,
        WEATHER_OPENMETEO_CSV,
        assert_max_abs_diff,
        assert_parquet_snappy_partitioned,
        assert_struct_schema,
        assert_temporal_continuity,
    )


class TestStep2BronzeToSilver:
    """Verifies Milestone 2 (Step 2) Bronze to Silver transformation deliverables."""

    def test_silver_station_dataset_integrity(self):
        """Verifies lakehouse_storage/silver/tedp_station/ exists, is Snappy Parquet,

        partitioned by province_name, and respects QCVN physical thresholds.
        """
        assert SILVER_STATION_DIR.exists(), (
            f"Silver station directory not found: {SILVER_STATION_DIR}. "
            "Run Step 2 (code/lakehouse/spark_preprocess_tedp.py) to build station Silver dataset."
        )

        assert_parquet_snappy_partitioned(
            SILVER_STATION_DIR,
            partition_col="province_name",
            expected_partitions=EXPECTED_PROVINCE_COUNT,
        )

        df_station = pd.read_parquet(SILVER_STATION_DIR)
        assert len(df_station) > 0, "Silver station dataset is empty."

        # Verify QCVN / Physical Outlier ranges
        for col, max_limit in [("pm25", 500.0), ("pm10", 600.0), ("aqi", 500.0)]:
            if col in df_station.columns:
                valid_vals = df_station[col].dropna()
                assert (valid_vals >= 0.0).all(), f"Found negative values in station column '{col}'."
                assert (valid_vals <= max_limit).all(), (
                    f"Found values exceeding physical limit {max_limit} in station column '{col}'."
                )

    def test_silver_province_directory_and_partitioning(self, ground_truth_provinces):
        """Verifies lakehouse_storage/silver/tedp_province/ is partitioned by province_name

        with subdirectories for all 11 provinces.
        """
        assert SILVER_PROVINCE_DIR.exists(), (
            f"Silver province directory not found: {SILVER_PROVINCE_DIR}. "
            "Run Step 2 (code/lakehouse/spark_sync_province.py) to build province Silver dataset."
        )

        assert_parquet_snappy_partitioned(
            SILVER_PROVINCE_DIR,
            partition_col="province_name",
            expected_partitions=EXPECTED_PROVINCE_COUNT,
        )

        # Inspect partition directory names
        partition_dirs = [p.name for p in SILVER_PROVINCE_DIR.iterdir() if p.is_dir() and p.name.startswith("province_name=")]
        partition_provinces = sorted([p.replace("province_name=", "") for p in partition_dirs])

        assert len(partition_provinces) == EXPECTED_PROVINCE_COUNT, (
            f"Expected {EXPECTED_PROVINCE_COUNT} partitions, found {len(partition_provinces)}: {partition_provinces}"
        )

    def test_silver_province_static_struct_schema(self):
        """Verifies static StructType schema for Silver province table:

        No inferred types (e.g. numeric columns must be DoubleType/FloatType, not StringType).
        """
        assert SILVER_PROVINCE_DIR.exists(), (
            f"Silver province directory not found: {SILVER_PROVINCE_DIR}."
        )

        # Inspect Parquet schema via pyarrow (faithful representation of Parquet metadata)
        dataset = pq.ParquetDataset(SILVER_PROVINCE_DIR)
        schema = dataset.schema

        schema_dict = {field.name: str(field.type) for field in schema}

        # Check required columns and types
        expected_types = {
            "datetime": "timestamp",
            "pm25": "double",
            "pm10": "double",
            "aqi": "double",
            "station_code": "string",
            "station_name": "string",
        }

        for col, exp_type in expected_types.items():
            assert col in schema_dict, f"Column '{col}' missing from Silver province Parquet schema."
            actual_type = schema_dict[col].lower()
            assert exp_type in actual_type or (exp_type == "double" and "float" in actual_type), (
                f"Column '{col}' has type '{schema_dict[col]}', expected compatible with '{exp_type}'."
            )

        # Ensure no numeric column was wrongly inferred as string
        for num_col in ["pm25", "pm10", "aqi"]:
            assert "string" not in schema_dict[num_col].lower(), (
                f"Column '{num_col}' must be numeric (double/float), but got string type '{schema_dict[num_col]}'."
            )

    def test_silver_province_row_count_and_continuity(self):
        """Verifies exactly 280,632 rows total and 25,512 continuous hourly rows per province."""
        assert SILVER_PROVINCE_DIR.exists(), (
            f"Silver province directory not found: {SILVER_PROVINCE_DIR}."
        )

        df = pd.read_parquet(SILVER_PROVINCE_DIR, columns=["province_name", "datetime", "pm25"])

        # 1. Total row count assertion
        assert len(df) == EXPECTED_TOTAL_SILVER_ROWS, (
            f"Total row count in Silver province table is {len(df)}, expected exactly {EXPECTED_TOTAL_SILVER_ROWS}."
        )

        # 2. Temporal continuity assertion
        assert_temporal_continuity(
            df,
            datetime_col="datetime",
            province_col="province_name",
            expected_hours_per_province=EXPECTED_HOURS_PER_PROVINCE,
            start_time=START_TIMESTAMP,
            end_time=END_TIMESTAMP,
        )

    def test_silver_province_zero_nulls_in_core_metrics(self):
        """Verifies zero unexpected nulls in pm25, pm10, aqi, datetime, and province_name."""
        assert SILVER_PROVINCE_DIR.exists(), (
            f"Silver province directory not found: {SILVER_PROVINCE_DIR}."
        )

        df = pd.read_parquet(SILVER_PROVINCE_DIR, columns=["province_name", "datetime", "pm25", "pm10", "aqi"])

        for col in ["province_name", "datetime", "pm25", "pm10", "aqi"]:
            null_count = int(df[col].isna().sum())
            assert null_count == 0, (
                f"Column '{col}' in Silver province Parquet has {null_count} unexpected null values."
            )

    def test_silver_province_numerical_tolerance_vs_pandas(self):
        """Verifies absolute difference of pm25, pm10, aqi vs data_test/cleaned_tedp_province_4y.csv is < 1e-5."""
        assert SILVER_PROVINCE_DIR.exists(), (
            f"Silver province directory not found: {SILVER_PROVINCE_DIR}."
        )
        assert CLEANED_TEDP_CSV.exists(), (
            f"Reference dataset not found: {CLEANED_TEDP_CSV}."
        )

        # Load Spark Silver Parquet
        cols_to_compare = ["pm25", "pm10", "aqi"]
        df_silver = pd.read_parquet(
            SILVER_PROVINCE_DIR,
            columns=["province_name", "datetime"] + cols_to_compare,
        )

        # Load reference Pandas CSV
        df_ref = pd.read_csv(
            CLEANED_TEDP_CSV,
            usecols=["province_name", "datetime"] + cols_to_compare,
        )

        assert_max_abs_diff(
            actual=df_silver,
            expected=df_ref,
            columns=cols_to_compare,
            max_diff=NUMERICAL_TOLERANCE,
            key_columns=["province_name", "datetime"],
        )

    def test_silver_weather_province_integrity_and_tolerance(self):
        """Verifies lakehouse_storage/silver/weather_province/ has 280,632 rows

        and matches data_test/weather_openmeteo_province_4y.csv with tolerance < 1e-5.
        """
        assert SILVER_WEATHER_DIR.exists(), (
            f"Silver weather directory not found: {SILVER_WEATHER_DIR}."
        )
        assert WEATHER_OPENMETEO_CSV.exists(), (
            f"Reference weather dataset not found: {WEATHER_OPENMETEO_CSV}."
        )

        weather_cols = [
            "temperature_2m", "relative_humidity_2m", "dew_point_2m",
            "surface_pressure", "precipitation", "wind_speed_10m",
            "wind_direction_10m", "boundary_layer_height"
        ]

        df_weather_silver = pd.read_parquet(
            SILVER_WEATHER_DIR,
            columns=["province_name", "datetime"] + weather_cols,
        )

        assert len(df_weather_silver) == EXPECTED_TOTAL_SILVER_ROWS, (
            f"Weather Silver rows {len(df_weather_silver)} != {EXPECTED_TOTAL_SILVER_ROWS}."
        )

        df_weather_ref = pd.read_csv(
            WEATHER_OPENMETEO_CSV,
            usecols=["province_name", "datetime"] + weather_cols,
        )

        assert_max_abs_diff(
            actual=df_weather_silver,
            expected=df_weather_ref,
            columns=weather_cols,
            max_diff=NUMERICAL_TOLERANCE,
            key_columns=["province_name", "datetime"],
        )
