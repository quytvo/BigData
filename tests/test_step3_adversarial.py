"""
test_step3_adversarial.py
=========================
Adversarial Stress Test & Deep Numerical Verification Harness for Step 3.
Independently written by Challenger 1 to empirically stress-test Step 3 deliverables:
1. Strict partition directory integrity & anti-collision verification (11 provinces, zero %20 duplicates).
2. Per-province row count parity (exactly 25,464 rows each, total 280,104).
3. Zero-nulls & zero-NaNs audit across all 61 columns.
4. Temporal continuity, 1-hour delta, and 48-hour boundary trimming (24h lookback + 24h lookahead).
5. Deep numerical parity vs Pandas baseline across all 50 targets (49 features + y) with max error strictly < 1e-5.
6. Domain and mathematical invariants (cyclic identities, normalized coordinates, one-hot exclusivity).
7. Cutoff filter validation (279,840 rows matching ANN checkpoint).
8. Pure Catalyst AST conformance (zero Python UDFs, zero applyInPandas, zero inferSchema=True).
9. Master lakehouse runner CLI interface validation.
"""

import ast
import os
import sys
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_FEATURES_DIR = PROJECT_ROOT / "lakehouse_storage" / "gold" / "global_ann_features"
SILVER_TEDP_DIR = PROJECT_ROOT / "lakehouse_storage" / "silver" / "tedp_province"
CLEANED_TEDP_CSV = PROJECT_ROOT / "data_test" / "cleaned_tedp_province_4y.csv"
WEATHER_CSV = PROJECT_ROOT / "data_test" / "weather_openmeteo_province_4y.csv"

CODE_DIR = PROJECT_ROOT / "code" / "lakehouse"
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

try:
    from build_lakehouse_spark import compute_pandas_reference_gold, CANONICAL_49_FEATURES
except ImportError:
    from code.lakehouse.build_lakehouse_spark import compute_pandas_reference_gold, CANONICAL_49_FEATURES

EXPECTED_PROVINCES = [
    "Bắc Ninh", "Gia Lai", "HCM", "Hà Nội", "Hưng Yên",
    "Hải Phòng", "Lâm Đồng", "Ninh Bình", "Quảng Ngãi", "Tây Ninh", "Đà Nẵng",
]

EXPECTED_ROWS_PER_PROVINCE_FULL = 25464
EXPECTED_TOTAL_ROWS_FULL = 280104
EXPECTED_ROWS_PER_PROVINCE_FILTERED = 25440
EXPECTED_TOTAL_ROWS_FILTERED = 279840
CUTOFF_DATE_STR = "2026-11-06 23:00:00"
NUMERICAL_TOLERANCE = 1e-5


@pytest.fixture(scope="module")
def gold_parquet_data():
    """Loads Gold Parquet dataset once for the test module."""
    assert GOLD_FEATURES_DIR.exists(), f"Gold directory missing: {GOLD_FEATURES_DIR}"
    df_gold = pd.read_parquet(GOLD_FEATURES_DIR)
    return df_gold


class TestStep3AdversarialParity:
    """Adversarial stress test suite for Step 3 Gold feature engineering."""

    def test_01_partition_directory_integrity_and_anti_collision(self):
        """Verifies exactly 11 partition directories exist with zero %20 duplicates or collisions."""
        subdirs = [d for d in os.listdir(GOLD_FEATURES_DIR) if os.path.isdir(GOLD_FEATURES_DIR / d)]
        assert len(subdirs) == 11, f"Expected 11 partition subdirectories, found {len(subdirs)}: {subdirs}"

        # Check for URL encoded collisions
        collision_dirs = [d for d in subdirs if "%20" in d]
        assert len(collision_dirs) == 0, f"Found URL-encoded collision directories in Gold: {collision_dirs}"

        # Verify exact partition directory naming
        expected_partition_names = [f"province_name={p}" for p in EXPECTED_PROVINCES]
        assert sorted(subdirs) == sorted(expected_partition_names), (
            f"Partition subdirectories {sorted(subdirs)} do not match expected {sorted(expected_partition_names)}"
        )

        # Verify each partition contains valid snappy parquet
        for d in subdirs:
            part_path = GOLD_FEATURES_DIR / d
            pq_files = [f for f in os.listdir(part_path) if f.endswith(".parquet")]
            assert len(pq_files) > 0, f"Partition {d} has no .parquet files!"
            for pq_f in pq_files:
                meta = pq.read_metadata(part_path / pq_f)
                assert meta.num_rows > 0, f"Parquet file {pq_f} is empty!"
                codec = str(meta.row_group(0).column(0).compression).upper()
                assert "SNAPPY" in codec, f"Parquet file {pq_f} compression '{codec}' is not Snappy!"

    def test_02_strict_row_counts_per_province_and_total(self, gold_parquet_data):
        """Verifies all 11 provinces have exactly 25,464 rows each and total is 280,104."""
        df = gold_parquet_data
        total_rows = len(df)
        assert total_rows == EXPECTED_TOTAL_ROWS_FULL, (
            f"Total row count {total_rows} != expected {EXPECTED_TOTAL_ROWS_FULL}"
        )

        counts = df["province_name"].value_counts().to_dict()
        assert len(counts) == 11, f"Expected 11 provinces, found {len(counts)}"

        for prov in EXPECTED_PROVINCES:
            assert prov in counts, f"Province '{prov}' missing from Gold table!"
            cnt = counts[prov]
            assert cnt == EXPECTED_ROWS_PER_PROVINCE_FULL, (
                f"Province '{prov}' has {cnt} rows, expected exactly {EXPECTED_ROWS_PER_PROVINCE_FULL}"
            )

    def test_03_zero_nulls_and_nans_across_all_61_columns(self, gold_parquet_data):
        """Verifies exactly 0 nulls / NaNs across all 61 columns in the Gold table."""
        df = gold_parquet_data
        null_counts = df.isnull().sum()
        total_nulls = int(null_counts.sum())
        assert total_nulls == 0, (
            f"Detected {total_nulls} nulls in Gold dataset! Columns with nulls:\n"
            f"{null_counts[null_counts > 0]}"
        )

    def test_04_temporal_continuity_and_boundary_trimming_48h(self, gold_parquet_data):
        """Verifies strictly 1-hour temporal increments and exact 48h boundary trimming."""
        df = gold_parquet_data
        assert SILVER_TEDP_DIR.exists(), f"Silver TEDP dir missing: {SILVER_TEDP_DIR}"
        df_silver = pd.read_parquet(SILVER_TEDP_DIR)

        for prov in EXPECTED_PROVINCES:
            s_prov = df_silver[df_silver["province_name"] == prov].sort_values("datetime").reset_index(drop=True)
            g_prov = df[df["province_name"] == prov].sort_values("origin_time").reset_index(drop=True)

            assert len(s_prov) == 25512, f"Silver rows for {prov} is {len(s_prov)}, expected 25,512"
            assert len(g_prov) == EXPECTED_ROWS_PER_PROVINCE_FULL

            s_start = s_prov["datetime"].iloc[0]
            s_end = s_prov["datetime"].iloc[-1]
            g_start = g_prov["origin_time"].iloc[0]
            g_end = g_prov["origin_time"].iloc[-1]
            g_tgt_end = g_prov["target_time"].iloc[-1]

            # Head trim: exactly 24 hours
            head_trim_hours = (g_start - s_start).total_seconds() / 3600.0
            assert head_trim_hours == 24.0, (
                f"{prov}: Head trimming was {head_trim_hours}h, expected exactly 24.0h"
            )

            # Tail trim: origin_time ends 24 hours before silver end, target_time ends at silver end
            tail_trim_hours = (s_end - g_end).total_seconds() / 3600.0
            assert tail_trim_hours == 24.0, (
                f"{prov}: Tail trimming was {tail_trim_hours}h, expected exactly 24.0h"
            )
            assert g_tgt_end == s_end, f"{prov}: Last target_time does not match Silver last observation!"

            # Strict 1-hour steps
            time_diffs = (g_prov["origin_time"].diff().dropna().dt.total_seconds() / 3600.0).unique()
            assert len(time_diffs) == 1 and time_diffs[0] == 1.0, (
                f"{prov}: Non-continuous time steps detected: {time_diffs}"
            )

            # Target horizon is origin_time + 24h
            horizon_diffs = ((g_prov["target_time"] - g_prov["origin_time"]).dt.total_seconds() / 3600.0).unique()
            assert len(horizon_diffs) == 1 and horizon_diffs[0] == 24.0, (
                f"{prov}: Target horizon not exactly 24.0h: {horizon_diffs}"
            )

            # datetime == origin_time
            assert (g_prov["datetime"] == g_prov["origin_time"]).all(), (
                f"{prov}: datetime column does not match origin_time!"
            )

    def test_05_deep_numerical_error_bound_vs_pandas_all_features(self, gold_parquet_data):
        """Verifies maximum absolute difference vs Pandas reference is strictly < 1e-5 across all 50 targets."""
        df_spark = gold_parquet_data.copy()
        df_pandas = compute_pandas_reference_gold(CLEANED_TEDP_CSV, WEATHER_CSV)

        df_spark["target_time"] = pd.to_datetime(df_spark["target_time"])
        df_pandas["target_time"] = pd.to_datetime(df_pandas["target_time"])
        df_spark["province_name"] = df_spark["province_name"].astype(str)
        df_pandas["province_name"] = df_pandas["province_name"].astype(str)

        df_spark = df_spark.sort_values(["province_name", "target_time"]).reset_index(drop=True)
        df_pandas = df_pandas.sort_values(["province_name", "target_time"]).reset_index(drop=True)

        assert len(df_spark) == len(df_pandas) == EXPECTED_TOTAL_ROWS_FULL

        cols_to_check = ["y"] + CANONICAL_49_FEATURES
        for col in cols_to_check:
            s_vals = df_spark[col].to_numpy(dtype=np.float64)
            p_vals = df_pandas[col].to_numpy(dtype=np.float64)
            diff = np.abs(s_vals - p_vals)
            max_d = float(np.max(diff))

            assert max_d < NUMERICAL_TOLERANCE, (
                f"Feature '{col}' max absolute diff {max_d:.8e} >= {NUMERICAL_TOLERANCE}!"
            )

    def test_06_domain_and_mathematical_invariants(self, gold_parquet_data):
        """Verifies cyclic trigonometric identities, weekend logic, spatial bounds, and one-hot integrity."""
        df = gold_parquet_data

        # 1. Trigonometric identities
        hour_sq = df["hour_sin"] ** 2 + df["hour_cos"] ** 2
        dow_sq = df["dow_sin"] ** 2 + df["dow_cos"] ** 2
        assert np.allclose(hour_sq, 1.0, atol=1e-12), "hour_sin^2 + hour_cos^2 != 1.0"
        assert np.allclose(dow_sq, 1.0, atol=1e-12), "dow_sin^2 + dow_cos^2 != 1.0"

        # 2. Weekend indicator
        expected_weekend = (df["day_of_week"] >= 5.0).astype(float)
        assert (df["is_weekend"] == expected_weekend).all(), "is_weekend does not match day_of_week >= 5"

        # 3. Spatial normalization bounds [-1.0, 1.0]
        assert df["latitude_norm"].min() >= -1.0 - 1e-6 and df["latitude_norm"].max() <= 1.0 + 1e-6
        assert df["longitude_norm"].min() >= -1.0 - 1e-6 and df["longitude_norm"].max() <= 1.0 + 1e-6

        # 4. One-Hot Station Identifiers
        onehot_cols = [c for c in df.columns if c.startswith("station_id_")]
        assert len(onehot_cols) == 11, f"Expected 11 one-hot station columns, found {len(onehot_cols)}"
        assert (df[onehot_cols].sum(axis=1) == 1.0).all(), "One-hot encodings do not sum to 1.0"

        for col in onehot_cols:
            st_id = col.replace("station_id_", "")
            assert ((df[col] == 1.0) & (df["station_id"] != st_id)).sum() == 0

        # 5. Station index range
        assert df["station_idx"].min() == 0 and df["station_idx"].max() == 10

    def test_07_cutoff_filter_subset_validation(self, gold_parquet_data):
        """Verifies filtering by cutoff date yields exactly 279,840 rows (25,440 per province)."""
        df = gold_parquet_data
        cutoff_dt = pd.to_datetime(CUTOFF_DATE_STR)
        df_filtered = df[pd.to_datetime(df["target_time"]) <= cutoff_dt]

        assert len(df_filtered) == EXPECTED_TOTAL_ROWS_FILTERED, (
            f"Filtered row count {len(df_filtered)} != {EXPECTED_TOTAL_ROWS_FILTERED}"
        )

        counts = df_filtered["province_name"].value_counts().to_dict()
        for prov in EXPECTED_PROVINCES:
            assert counts[prov] == EXPECTED_ROWS_PER_PROVINCE_FILTERED, (
                f"{prov} filtered rows {counts[prov]} != {EXPECTED_ROWS_PER_PROVINCE_FILTERED}"
            )

    def test_08_pure_catalyst_ast_inspection(self):
        """Empirically inspects AST of spark_feature_engineering.py to ensure zero prohibited constructs."""
        target_script = CODE_DIR / "spark_feature_engineering.py"
        assert target_script.exists()

        tree = ast.parse(target_script.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    assert node.func.id not in ["udf", "pandas_udf"], (
                        f"Prohibited UDF function '{node.func.id}' found at line {node.lineno}"
                    )
                elif isinstance(node.func, ast.Attribute):
                    assert node.func.attr not in ["applyInPandas", "toPandas"], (
                        f"Prohibited method '{node.func.attr}' found at line {node.lineno}"
                    )
                for kw in node.keywords:
                    if kw.arg == "inferSchema" and getattr(kw.value, "value", None) is True:
                        pytest.fail(f"Prohibited inferSchema=True found at line {node.lineno}")

    def test_09_master_runner_cli_interface(self):
        """Verifies code/lakehouse/build_lakehouse_spark.py exposes complete CLI interface."""
        runner_path = CODE_DIR / "build_lakehouse_spark.py"
        assert runner_path.exists()
        content = runner_path.read_text(encoding="utf-8")

        for flag in ["--steps", "--verify-diff", "--cutoff-target-date", "--keep-ui"]:
            assert flag in content, f"Master runner missing expected CLI flag: {flag}"
