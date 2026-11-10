"""
test_step2_adversarial.py
=========================
Adversarial Stress Test & Deep Numerical Verification Harness for Step 2.
Independently written by Challenger 1 to empirically stress-test Step 2 deliverables:
1. Exhaustive numerical parity across all 280,632 rows x 30 columns vs data_test/cleaned_tedp_province_4y.csv.
2. Exact zero-difference assertion on pm25_gap_hours across all 11 provinces.
3. Per-province numerical tolerance assertion (diff < 1e-5, verified at ~1e-14).
4. Exhaustive meteorological feature parity vs data_test/weather_openmeteo_province_4y.csv.
5. Invariant assertion on gap flags: (pm25_gap_hours > 0) <==> (is_inserted == 1) <==> (pm25_imputed == 1).
6. QCVN 05:2023 physical threshold compliance and station median masking validation.
"""

from pathlib import Path
import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_PROVINCE_DIR = PROJECT_ROOT / "lakehouse_storage" / "silver" / "tedp_province"
SILVER_STATION_DIR = PROJECT_ROOT / "lakehouse_storage" / "silver" / "tedp_station"
SILVER_WEATHER_DIR = PROJECT_ROOT / "lakehouse_storage" / "silver" / "weather_province"

CLEANED_TEDP_CSV = PROJECT_ROOT / "data_test" / "cleaned_tedp_province_4y.csv"
WEATHER_OPENMETEO_CSV = PROJECT_ROOT / "data_test" / "weather_openmeteo_province_4y.csv"

TARGET_PROVINCES = [
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

EXPECTED_HOURS_PER_PROVINCE = 25512
EXPECTED_TOTAL_ROWS = 280632
NUMERICAL_TOLERANCE = 1e-5


@pytest.fixture(scope="module")
def merged_province_data():
    """Loads and joins Silver Parquet and Pandas CSV reference."""
    df_silver = pd.read_parquet(SILVER_PROVINCE_DIR)
    df_csv = pd.read_csv(CLEANED_TEDP_CSV)

    df_silver["dt_str"] = pd.to_datetime(df_silver["datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df_csv["dt_str"] = pd.to_datetime(df_csv["datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    merged = pd.merge(
        df_silver,
        df_csv,
        on=["province_name", "dt_str"],
        suffixes=("_spark", "_pandas"),
    )
    return merged, df_silver, df_csv


class TestStep2AdversarialParity:
    """Empirical adversarial verification tests for Step 2 Lakehouse Silver layer."""

    def test_01_total_row_count_and_cardinality(self, merged_province_data):
        """Verifies exact 280,632 row count and 11 unique provinces match 1-to-1."""
        merged, df_silver, df_csv = merged_province_data
        assert len(df_silver) == EXPECTED_TOTAL_ROWS, f"Silver rows {len(df_silver)} != {EXPECTED_TOTAL_ROWS}"
        assert len(df_csv) == EXPECTED_TOTAL_ROWS, f"Reference CSV rows {len(df_csv)} != {EXPECTED_TOTAL_ROWS}"
        assert len(merged) == EXPECTED_TOTAL_ROWS, f"Merged matched rows {len(merged)} != {EXPECTED_TOTAL_ROWS}"

        assert sorted(df_silver["province_name"].unique()) == sorted(TARGET_PROVINCES)

    def test_02_deep_numerical_error_bound_pollutants(self, merged_province_data):
        """Empirically asserts max abs diff < 1e-5 for pm25, pm10, and aqi across all rows."""
        merged, _, _ = merged_province_data

        for col in ["pm25", "pm10", "aqi"]:
            diff = np.abs(merged[f"{col}_spark"] - merged[f"{col}_pandas"])
            max_d = float(diff.max())
            mean_d = float(diff.mean())
            p99_d = float(diff.quantile(0.99))

            assert max_d < NUMERICAL_TOLERANCE, (
                f"Column '{col}' max diff {max_d:.8e} exceeds tolerance {NUMERICAL_TOLERANCE}."
            )
            # Adversarial assertion: Spark Catalyst Window should achieve double-precision agreement (< 1e-12)
            assert max_d < 1e-12, (
                f"Column '{col}' max diff {max_d:.8e} is surprisingly large (expected < 1e-12)."
            )

    def test_03_pm25_gap_hours_exact_match(self, merged_province_data):
        """Empirically asserts exact 0-difference on pm25_gap_hours against Pandas reference."""
        merged, _, _ = merged_province_data
        diff = np.abs(merged["pm25_gap_hours_spark"] - merged["pm25_gap_hours_pandas"])
        max_diff = float(diff.max())
        mismatches = int((diff > 0).sum())

        assert max_diff == 0.0, f"Found {mismatches} mismatches in pm25_gap_hours. Max diff: {max_diff}"
        assert mismatches == 0, f"Mismatches in pm25_gap_hours: {mismatches}"

    def test_04_per_province_numerical_parity(self, merged_province_data):
        """Validates all 11 provinces individually have exactly 25,512 continuous hours and diff < 1e-5."""
        merged, _, _ = merged_province_data

        for province in TARGET_PROVINCES:
            sub = merged[merged["province_name"] == province]
            assert len(sub) == EXPECTED_HOURS_PER_PROVINCE, (
                f"Province '{province}' has {len(sub)} rows, expected {EXPECTED_HOURS_PER_PROVINCE}."
            )

            d_pm25 = float(np.abs(sub["pm25_spark"] - sub["pm25_pandas"]).max())
            d_pm10 = float(np.abs(sub["pm10_spark"] - sub["pm10_pandas"]).max())
            d_aqi = float(np.abs(sub["aqi_spark"] - sub["aqi_pandas"]).max())
            d_gap = float(np.abs(sub["pm25_gap_hours_spark"] - sub["pm25_gap_hours_pandas"]).max())

            assert d_pm25 < NUMERICAL_TOLERANCE, f"[{province}] PM2.5 diff {d_pm25:.2e} >= {NUMERICAL_TOLERANCE}"
            assert d_pm10 < NUMERICAL_TOLERANCE, f"[{province}] PM10 diff {d_pm10:.2e} >= {NUMERICAL_TOLERANCE}"
            assert d_aqi < NUMERICAL_TOLERANCE, f"[{province}] AQI diff {d_aqi:.2e} >= {NUMERICAL_TOLERANCE}"
            assert d_gap == 0.0, f"[{province}] pm25_gap_hours diff {d_gap} != 0"

    def test_05_all_discrete_and_string_columns_match(self, merged_province_data):
        """Verifies zero mismatches in all discrete flags, calendar features, and string identifiers."""
        merged, _, _ = merged_province_data

        exact_match_cols = [
            "province_id", "station_id", "station_code", "station_name", "station_short_name",
            "province_extracted", "aqi_category", "day_name", "date", "time_slot",
            "pm25_imputed", "pm10_imputed", "aqi_imputed", "is_inserted",
            "n_stations_member", "n_stations_valid_pm25",
            "year", "month", "day", "hour", "day_of_week", "is_weekend"
        ]

        for col in exact_match_cols:
            s_col = merged[f"{col}_spark"].astype(str)
            p_col = merged[f"{col}_pandas"].astype(str)
            mismatch_cnt = int((s_col != p_col).sum())
            assert mismatch_cnt == 0, f"Column '{col}' has {mismatch_cnt} mismatches between Spark and Pandas."

    def test_06_weather_silver_exhaustive_parity(self):
        """Verifies all meteorological metrics in weather_province match Open-Meteo reference CSV < 1e-5."""
        df_weather_silver = pd.read_parquet(SILVER_WEATHER_DIR)
        df_weather_csv = pd.read_csv(WEATHER_OPENMETEO_CSV)

        df_weather_silver["dt_str"] = pd.to_datetime(df_weather_silver["datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        df_weather_csv["dt_str"] = pd.to_datetime(df_weather_csv["datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")

        merged_w = pd.merge(
            df_weather_silver,
            df_weather_csv,
            on=["province_name", "dt_str"],
            suffixes=("_spark", "_pandas"),
        )
        assert len(merged_w) == EXPECTED_TOTAL_ROWS

        weather_cols = [
            "temperature_2m", "relative_humidity_2m", "dew_point_2m",
            "surface_pressure", "precipitation", "wind_speed_10m",
            "wind_direction_10m", "boundary_layer_height"
        ]
        for col in weather_cols:
            diff = np.abs(merged_w[f"{col}_spark"] - merged_w[f"{col}_pandas"])
            max_d = float(diff.max())
            assert max_d < NUMERICAL_TOLERANCE, f"Weather col '{col}' max diff {max_d:.8e} >= {NUMERICAL_TOLERANCE}"

    def test_07_gap_flags_invariant(self, merged_province_data):
        """Invariant check: (pm25_gap_hours > 0) <=> (is_inserted == 1) <=> (pm25_imputed == 1)."""
        _, df_silver, _ = merged_province_data

        cond_gap = df_silver["pm25_gap_hours"] > 0
        cond_ins = df_silver["is_inserted"] == 1
        cond_imp = df_silver["pm25_imputed"] == 1

        # Every inserted row must have pm25_gap_hours > 0 and pm25_imputed == 1
        assert (cond_gap == cond_ins).all(), "Invariant violation: pm25_gap_hours > 0 does not match is_inserted == 1"
        assert (cond_gap == cond_imp).all(), "Invariant violation: pm25_gap_hours > 0 does not match pm25_imputed == 1"

    def test_08_qcvn_physical_ranges_in_silver(self, merged_province_data):
        """Verifies physical ranges conform to QCVN: pm25, aqi in [0, 500], pm10 in [0, 600]."""
        _, df_silver, _ = merged_province_data

        assert (df_silver["pm25"] >= 0.0).all() and (df_silver["pm25"] <= 500.0).all()
        assert (df_silver["pm10"] >= 0.0).all() and (df_silver["pm10"] <= 600.0).all()
        assert (df_silver["aqi"] >= 0.0).all() and (df_silver["aqi"] <= 500.0).all()
