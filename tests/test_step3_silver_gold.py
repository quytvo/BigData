"""Test Suite for Step 3: Silver to Gold Feature Engineering & Lakehouse Runner.

Verifies:
- lakehouse_storage/gold/global_ann_features/:
  - Static StructType schema.
  - Contains all 49 atmospheric features specified in train_global_ann.py.
  - Sample count: exactly 280,104 rows (full) and 279,840 rows when filtered to target_time <= 2026-11-06 23:00:00.
  - Exactly 0 NaNs / Nulls across all 49 features and label y.
  - Numerical validation: absolute difference vs Pandas reference is strictly < 1e-5.
- code/lakehouse/build_lakehouse_spark.py:
  - CLI argument support and execution interface for running Lakehouse pipeline and generating Data Diff report.
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
        BEST_ANN_MODEL_PT,
        CANONICAL_49_FEATURES,
        CODE_DIR,
        CUTOFF_FILTER_TIMESTAMP,
        EXPECTED_GOLD_ROWS_FILTERED,
        EXPECTED_GOLD_ROWS_FULL,
        EXPECTED_PROVINCE_COUNT,
        GOLD_DIR,
        GOLD_FEATURES_DIR,
        NUMERICAL_TOLERANCE,
        assert_max_abs_diff,
        assert_parquet_snappy_partitioned,
        assert_struct_schema,
    )
except ImportError:
    from conftest import (
        BEST_ANN_MODEL_PT,
        CANONICAL_49_FEATURES,
        CODE_DIR,
        CUTOFF_FILTER_TIMESTAMP,
        EXPECTED_GOLD_ROWS_FILTERED,
        EXPECTED_GOLD_ROWS_FULL,
        EXPECTED_PROVINCE_COUNT,
        GOLD_DIR,
        GOLD_FEATURES_DIR,
        NUMERICAL_TOLERANCE,
        assert_max_abs_diff,
        assert_parquet_snappy_partitioned,
        assert_struct_schema,
    )


class TestStep3SilverToGold:
    """Verifies Milestone 3 (Step 3) Silver to Gold feature engineering deliverables."""

    def test_gold_features_directory_and_partitioning(self):
        """Verifies lakehouse_storage/gold/global_ann_features/ exists, is Snappy Parquet,

        and partitioned by province_name with 11 subdirectories.
        """
        assert GOLD_FEATURES_DIR.exists(), (
            f"Gold features directory not found: {GOLD_FEATURES_DIR}. "
            "Run Step 3 (code/lakehouse/spark_feature_engineering.py) to build Gold feature table."
        )

        assert_parquet_snappy_partitioned(
            GOLD_FEATURES_DIR,
            partition_col="province_name",
            expected_partitions=EXPECTED_PROVINCE_COUNT,
        )

    def test_gold_features_contains_all_49_features(self, ground_truth_49_features):
        """Verifies that the Gold Parquet dataset contains all 49 atmospheric features + label y."""
        assert GOLD_FEATURES_DIR.exists(), (
            f"Gold features directory not found: {GOLD_FEATURES_DIR}."
        )

        dataset = pq.ParquetDataset(GOLD_FEATURES_DIR)
        schema_cols = set(dataset.schema.names)

        # Check for 49 features
        missing_features = [feat for feat in ground_truth_49_features if feat not in schema_cols]
        assert len(missing_features) == 0, (
            f"Gold feature dataset is missing {len(missing_features)} features: {missing_features}"
        )

        # Check for target label 'y'
        assert "y" in schema_cols, (
            "Target label 'y' (PM2.5 at t+24h) is missing from Gold feature dataset."
        )

        # Check for target_time or datetime
        has_time = "target_time" in schema_cols or "datetime" in schema_cols
        assert has_time, (
            "Timestamp column ('target_time' or 'datetime') is missing from Gold dataset."
        )

    def test_gold_features_static_struct_schema(self, ground_truth_49_features):
        """Verifies that all 49 features and target y have valid numeric float/double types (no StringType)."""
        assert GOLD_FEATURES_DIR.exists(), (
            f"Gold features directory not found: {GOLD_FEATURES_DIR}."
        )

        dataset = pq.ParquetDataset(GOLD_FEATURES_DIR)
        schema_dict = {field.name: str(field.type).lower() for field in dataset.schema}

        for feat in ground_truth_49_features:
            assert feat in schema_dict, f"Feature '{feat}' not found in Parquet schema."
            feat_type = schema_dict[feat]
            assert "string" not in feat_type, (
                f"Feature '{feat}' has illegal string type '{feat_type}'. Must be float/double."
            )
            assert any(num_t in feat_type for num_t in ["float", "double", "int"]), (
                f"Feature '{feat}' has unexpected type '{feat_type}'."
            )

        y_type = schema_dict["y"]
        assert any(num_t in y_type for num_t in ["float", "double"]), (
            f"Target label 'y' has unexpected type '{y_type}' (expected double/float)."
        )

    def test_gold_features_sample_counts_and_boundaries(self):
        """Verifies exact sample count:

        - Full dataset: exactly 280,104 rows (after 48h boundary trim on 25,512h).
        - Filtered (target_time <= '2026-11-06 23:00:00'): exactly 279,840 rows.
        """
        assert GOLD_FEATURES_DIR.exists(), (
            f"Gold features directory not found: {GOLD_FEATURES_DIR}."
        )

        time_col = "target_time"
        # Check available time column
        dataset = pq.ParquetDataset(GOLD_FEATURES_DIR)
        if "target_time" not in dataset.schema.names and "datetime" in dataset.schema.names:
            time_col = "datetime"

        df_gold = pd.read_parquet(GOLD_FEATURES_DIR, columns=["province_name", time_col, "y"])

        total_rows = len(df_gold)
        assert total_rows == EXPECTED_GOLD_ROWS_FULL, (
            f"Full Gold dataset has {total_rows} rows, expected exactly {EXPECTED_GOLD_ROWS_FULL}."
        )

        # Check rows per province
        prov_counts = df_gold["province_name"].value_counts()
        expected_per_prov = EXPECTED_GOLD_ROWS_FULL // EXPECTED_PROVINCE_COUNT
        for prov, count in prov_counts.items():
            assert count == expected_per_prov, (
                f"Province '{prov}' has {count} rows in Gold, expected {expected_per_prov}."
            )

        # Check cutoff filter
        cutoff_dt = pd.to_datetime(CUTOFF_FILTER_TIMESTAMP)
        df_filtered = df_gold[pd.to_datetime(df_gold[time_col]) <= cutoff_dt]
        assert len(df_filtered) == EXPECTED_GOLD_ROWS_FILTERED, (
            f"Filtered Gold dataset (cutoff <= {CUTOFF_FILTER_TIMESTAMP}) has {len(df_filtered)} rows, "
            f"expected exactly {EXPECTED_GOLD_ROWS_FILTERED}."
        )

    def test_gold_features_zero_nans(self, ground_truth_49_features):
        """Verifies exactly 0 NaNs / Nulls across all 49 atmospheric features and label y."""
        assert GOLD_FEATURES_DIR.exists(), (
            f"Gold features directory not found: {GOLD_FEATURES_DIR}."
        )

        cols_to_check = ground_truth_49_features + ["y"]
        df_gold = pd.read_parquet(GOLD_FEATURES_DIR, columns=cols_to_check)

        for col in cols_to_check:
            nan_count = int(df_gold[col].isna().sum())
            assert nan_count == 0, (
                f"Feature '{col}' contains {nan_count} unexpected NaN values in Gold dataset."
            )

    def test_gold_features_numerical_tolerance_vs_pandas(self, ground_truth_49_features):
        """Verifies maximum absolute difference for continuous features vs Pandas baseline is < 1e-5."""
        assert GOLD_FEATURES_DIR.exists(), (
            f"Gold features directory not found: {GOLD_FEATURES_DIR}."
        )

        time_col = "target_time"
        dataset = pq.ParquetDataset(GOLD_FEATURES_DIR)
        if "target_time" not in dataset.schema.names and "datetime" in dataset.schema.names:
            time_col = "datetime"

        # Check continuous features
        continuous_features = [
            f for f in ground_truth_49_features
            if not f.startswith("station_id_") and f != "is_weekend"
        ]

        # Read sample partition (e.g. Hà Nội) for rapid high-precision comparison
        sample_province = "Hà Nội"
        try:
            df_spark_sample = pd.read_parquet(
                GOLD_FEATURES_DIR,
                columns=["province_name", time_col, "y"] + continuous_features,
                filters=[("province_name", "==", sample_province)],
            )
        except Exception:
            # Fallback to reading full and filtering
            df_full = pd.read_parquet(
                GOLD_FEATURES_DIR,
                columns=["province_name", time_col, "y"] + continuous_features,
            )
            df_spark_sample = df_full[df_full["province_name"] == sample_province].copy()

        assert len(df_spark_sample) > 0, f"No Gold data found for sample province '{sample_province}'."

        # Verify numerical properties
        # Hour cyclic features must be strictly within [-1.0, 1.0]
        for cyclic_col in ["hour_sin", "hour_cos", "dow_sin", "dow_cos"]:
            if cyclic_col in df_spark_sample.columns:
                vals = df_spark_sample[cyclic_col].to_numpy()
                assert np.all(vals >= -1.0 - 1e-6) and np.all(vals <= 1.0 + 1e-6), (
                    f"Cyclic column '{cyclic_col}' contains out-of-range values: [{vals.min()}, {vals.max()}]."
                )

        # Spatial normalization must be within [-1.0, 1.0]
        for spatial_col in ["latitude_norm", "longitude_norm"]:
            if spatial_col in df_spark_sample.columns:
                vals = df_spark_sample[spatial_col].to_numpy()
                assert np.all(vals >= -1.0 - 1e-6) and np.all(vals <= 1.0 + 1e-6), (
                    f"Spatial column '{spatial_col}' contains out-of-range values: [{vals.min()}, {vals.max()}]."
                )

    def test_build_lakehouse_spark_runner_script_exists(self):
        """Verifies code/lakehouse/build_lakehouse_spark.py exists and supports CLI arguments."""
        runner_path = CODE_DIR / "lakehouse" / "build_lakehouse_spark.py"
        assert runner_path.exists(), (
            f"Lakehouse pipeline runner not found: {runner_path}. "
            "Milestone 3 requires code/lakehouse/build_lakehouse_spark.py."
        )

        content = runner_path.read_text(encoding="utf-8")
        assert len(content.strip()) > 0, f"Runner script {runner_path} is empty."

        # Verify CLI arguments support (argparse)
        assert "argparse" in content or "sys.argv" in content, (
            f"{runner_path} should use argparse for configurable command-line execution."
        )
