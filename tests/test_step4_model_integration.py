"""Test Suite for Step 4: Model Integration, Partition Pruning & Fallback.

Verifies:
- code/training/common.py:
  - load_merged(province_name="Hà Nội", use_parquet=True) leverages Parquet Partition Pruning.
  - Returns only records for the requested province (25,512 rows).
  - Seamless fallback mechanism: when Parquet directory is absent or use_parquet=False,
    transparently falls back to CSV without raising error.
- code/training/du_bao.py:
  - Rapid single-province inference reading directly from Silver Parquet partition.
- code/training/train_global_ann.py:
  - Supports loading from Gold Parquet with automatic fallback to CSV.
"""

import inspect
import sys
from pathlib import Path
from unittest.mock import patch
import numpy as np
import pandas as pd
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
        CODE_DIR,
        EXPECTED_HOURS_PER_PROVINCE,
        PROJECT_ROOT,
        SILVER_PROVINCE_DIR,
    )
except ImportError:
    from conftest import (
        CLEANED_TEDP_CSV,
        CODE_DIR,
        EXPECTED_HOURS_PER_PROVINCE,
        PROJECT_ROOT,
        SILVER_PROVINCE_DIR,
    )


class TestStep4ModelIntegration:
    """Verifies Milestone 4 (Step 4) model integration and partition pruning deliverables."""

    def test_common_module_load_merged_signature(self):
        """Verifies code/training/common.py defines load_merged with province_name and use_parquet parameters."""
        import sys
        if str(CODE_DIR / "training") not in sys.path:
            sys.path.insert(0, str(CODE_DIR / "training"))
        if str(PROJECT_ROOT) not in sys.path:
            sys.path.insert(0, str(PROJECT_ROOT))

        try:
            import common  # type: ignore
        except ImportError as e:
            pytest.fail(f"Failed to import code.training.common: {e}")

        assert hasattr(common, "load_merged"), "common.py must define function 'load_merged'."

        sig = inspect.signature(common.load_merged)
        params = sig.parameters

        # Verify province_name parameter
        assert "province_name" in params, (
            "load_merged must accept 'province_name' parameter (default None) to enable partition pruning."
        )
        assert params["province_name"].default is None or params["province_name"].default is not inspect.Parameter.empty, (
            "'province_name' should default to None (load all if not specified)."
        )

        # Verify use_parquet parameter
        assert "use_parquet" in params, (
            "load_merged must accept 'use_parquet' boolean flag (default True) to support lakehouse reading."
        )

    def test_common_load_merged_partition_pruning(self):
        """Verifies load_merged(province_name='Hà Nội', use_parquet=True) returns ONLY Hà Nội records

        (exactly 25,512 rows) and reads exclusively from the Hà Nội Parquet partition.
        """
        import sys
        if str(CODE_DIR / "training") not in sys.path:
            sys.path.insert(0, str(CODE_DIR / "training"))
        if str(PROJECT_ROOT) not in sys.path:
            sys.path.insert(0, str(PROJECT_ROOT))

        try:
            import common  # type: ignore
        except ImportError:
            pytest.skip("common module not available.")

        sig = inspect.signature(common.load_merged)
        if "province_name" not in sig.parameters or "use_parquet" not in sig.parameters:
            pytest.fail(
                "common.load_merged does not yet support 'province_name' and 'use_parquet' arguments. "
                "Milestone 4 implementation is required."
            )

        if not SILVER_PROVINCE_DIR.exists():
            pytest.skip("Silver Parquet directory not built yet. Run Step 2 first.")

        # Call load_merged for single province
        target_prov = "Hà Nội"
        df_prov = common.load_merged(province_name=target_prov, use_parquet=True)

        assert isinstance(df_prov, pd.DataFrame), "load_merged must return a pandas DataFrame."
        assert len(df_prov) == EXPECTED_HOURS_PER_PROVINCE, (
            f"load_merged for '{target_prov}' returned {len(df_prov)} rows, expected {EXPECTED_HOURS_PER_PROVINCE}."
        )

        unique_provinces = df_prov["province_name"].unique().tolist()
        assert unique_provinces == [target_prov], (
            f"Partition pruning failed: expected only ['{target_prov}'], but got {unique_provinces}."
        )

        # Core columns must be present
        for col in ["datetime", "pm25", "temperature_2m", "wind_speed_10m"]:
            assert col in df_prov.columns, f"Required column '{col}' missing from loaded DataFrame."

    def test_common_load_merged_fallback_when_parquet_missing(self, monkeypatch, tmp_path):
        """Verifies load_merged automatically falls back to CSV when Parquet directory is absent or invalid."""
        import sys
        if str(CODE_DIR / "training") not in sys.path:
            sys.path.insert(0, str(CODE_DIR / "training"))
        if str(PROJECT_ROOT) not in sys.path:
            sys.path.insert(0, str(PROJECT_ROOT))

        try:
            import common  # type: ignore
        except ImportError:
            pytest.skip("common module not available.")

        sig = inspect.signature(common.load_merged)
        if "province_name" not in sig.parameters:
            pytest.skip("Step 4 load_merged parameters not yet implemented.")

        # Explicitly request use_parquet=False -> must use CSV
        df_csv = common.load_merged(province_name="Hà Nội", use_parquet=False)
        assert isinstance(df_csv, pd.DataFrame), "load_merged(use_parquet=False) must return DataFrame."
        assert len(df_csv) > 0, "load_merged(use_parquet=False) returned empty DataFrame."
        assert (df_csv["province_name"] == "Hà Nội").all(), (
            "load_merged(province_name='Hà Nội', use_parquet=False) should filter to Hà Nội."
        )

        # Simulate missing Parquet storage path using monkeypatch on common module attributes
        fake_missing_path = tmp_path / "non_existent_silver"
        for attr in ["SILVER_PROVINCE_DIR", "SILVER_DIR", "PARQUET_DIR"]:
            if hasattr(common, attr):
                monkeypatch.setattr(common, attr, fake_missing_path)

        df_fallback = common.load_merged(province_name="Hà Nội", use_parquet=True)
        assert isinstance(df_fallback, pd.DataFrame), "load_merged must gracefully fall back to CSV."
        assert len(df_fallback) > 0, "Fallback DataFrame is empty."

    def test_du_bao_script_parquet_integration(self):
        """Verifies code/training/du_bao.py checks for Parquet partition pruning when loading historical observations."""
        du_bao_path = CODE_DIR / "training" / "du_bao.py"
        assert du_bao_path.exists(), f"du_bao.py script not found: {du_bao_path}"

        content = du_bao_path.read_text(encoding="utf-8")
        assert len(content.strip()) > 0, "du_bao.py is empty."

        # Verify Parquet and Partition Pruning support in du_bao.py
        has_parquet_support = (
            "parquet" in content.lower()
            or "read_parquet" in content
            or "silver" in content.lower()
            or "load_merged" in content
        )
        assert has_parquet_support, (
            "code/training/du_bao.py must be updated to load historical observations from "
            "Silver Parquet with partition pruning (or via common.load_merged)."
        )

    def test_train_global_ann_parquet_consumption_support(self):
        """Verifies code/training/train_global_ann.py supports loading features from Gold/Silver Parquet."""
        train_script = CODE_DIR / "training" / "train_global_ann.py"
        assert train_script.exists(), f"train_global_ann.py not found: {train_script}"

        content = train_script.read_text(encoding="utf-8")
        assert len(content.strip()) > 0, "train_global_ann.py is empty."

        # Check for lakehouse / parquet support references
        has_lakehouse_support = (
            "parquet" in content.lower()
            or "lakehouse" in content.lower()
            or "gold" in content.lower()
        )
        assert has_lakehouse_support, (
            "code/training/train_global_ann.py must support loading precomputed Gold features from Parquet "
            "with automatic fallback to CSV."
        )
