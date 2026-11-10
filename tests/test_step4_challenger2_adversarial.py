"""Empirical Adversarial Benchmark & Verification Suite for Milestone 4 Step 4.

Author: Challenger 2 (Empirical Challenger)
Target:
- code/training/common.py (load_merged Parquet Partition Pruning vs CSV)
- code/training/du_bao.py (CLI inference on multiple provinces, invalid province, CPU device)
- code/training/train_global_ann.py (Gold Parquet feature loader vs CSV feature building)
"""

import os
import subprocess
import sys
import time
from pathlib import Path
import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CODE_TRAINING = PROJECT_ROOT / "code" / "training"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(CODE_TRAINING) not in sys.path:
    sys.path.insert(0, str(CODE_TRAINING))

import common
from train_global_ann import load_features_dataset


class TestAdversarialSpeedupAndEquivalence:
    """Task 1: Quantify execution time and numerical equivalence."""

    PROVINCES = ["Hà Nội", "HCM", "Đà Nẵng", "Gia Lai", "Ninh Bình"]

    def test_benchmark_speedup_and_numerical_equivalence(self):
        results = {}
        for prov in self.PROVINCES:
            # 1. Warm-up
            _ = common.load_merged(province_name=prov, use_parquet=True)
            _ = common.load_merged(province_name=prov, use_parquet=False)

            # 2. Benchmark Parquet Partition Pruning (3 runs)
            pq_times = []
            for _ in range(3):
                t0 = time.perf_counter()
                df_pq = common.load_merged(province_name=prov, use_parquet=True)
                pq_times.append(time.perf_counter() - t0)
            avg_pq_time = float(np.mean(pq_times))

            # 3. Benchmark CSV (3 runs)
            csv_times = []
            for _ in range(3):
                t0 = time.perf_counter()
                df_csv = common.load_merged(province_name=prov, use_parquet=False)
                csv_times.append(time.perf_counter() - t0)
            avg_csv_time = float(np.mean(csv_times))

            speedup = avg_csv_time / max(avg_pq_time, 1e-9)

            # 4. Numerical Equivalence
            assert len(df_pq) == len(df_csv), f"Row count mismatch for {prov}: PQ {len(df_pq)} vs CSV {len(df_csv)}"
            assert (df_pq["province_name"] == prov).all()
            assert (df_csv["province_name"] == prov).all()

            # Align both on station_code and datetime
            df_pq_sorted = df_pq.sort_values(["station_code", "datetime"]).reset_index(drop=True)
            df_csv_sorted = df_csv.sort_values(["station_code", "datetime"]).reset_index(drop=True)

            # Check datetime alignment (normalize unit resolution ns vs us)
            dt_match = (
                df_pq_sorted["datetime"].astype("datetime64[ns]")
                == df_csv_sorted["datetime"].astype("datetime64[ns]")
            ).all()
            assert dt_match, f"Datetime mismatch detected for {prov}"

            # Check numeric columns within 1e-5
            numeric_cols = ["pm25"] + common.WEATHER_COLS
            max_diffs = {}
            for col in numeric_cols:
                diff = np.abs(df_pq_sorted[col].to_numpy() - df_csv_sorted[col].to_numpy())
                max_diff = float(np.nanmax(diff))
                max_diffs[col] = max_diff
                assert max_diff < 1e-5, f"Column {col} differs by {max_diff} (> 1e-5) for province {prov}"

            results[prov] = {
                "avg_pq_time_s": avg_pq_time,
                "avg_csv_time_s": avg_csv_time,
                "speedup": speedup,
                "rows": len(df_pq),
                "max_numeric_diff": max(max_diffs.values()),
            }

            print(
                f"\n[BENCHMARK] Province '{prov}': Parquet={avg_pq_time:.4f}s | "
                f"CSV={avg_csv_time:.4f}s | Speedup={speedup:.2f}x | MaxDiff={results[prov]['max_numeric_diff']:.2e}"
            )

            # Adversarial assertion: Speedup must genuinely be > 10x
            assert speedup > 10.0, f"Speedup for '{prov}' was {speedup:.2f}x, which is not > 10x!"


class TestAdversarialDuBaoCLI:
    """Task 2: In-depth CLI Edge Case Testing on du_bao.py."""

    VALID_PROVINCES = ["Hà Nội", "HCM", "Đà Nẵng", "Gia Lai", "Ninh Bình"]

    @pytest.mark.parametrize("prov", VALID_PROVINCES)
    def test_forecast_valid_province(self, prov):
        cmd = [
            sys.executable,
            str(CODE_TRAINING / "du_bao.py"),
            "--province",
            prov,
            "--device",
            "cpu",
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT)
        assert res.returncode == 0, f"Inference failed for {prov}:\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"

        # Check output file exists
        slug = common.slugify(prov)
        out_csv = PROJECT_ROOT / "data_test" / "du_bao" / f"{slug}.csv"
        assert out_csv.exists(), f"Output forecast file {out_csv} was not generated."

        df_out = pd.read_csv(out_csv)
        assert len(df_out) >= 24, f"Output file has only {len(df_out)} rows, expected >= 24."
        assert "pm25_pred_ann" in df_out.columns
        assert "air_quality_level" in df_out.columns
        assert df_out["pm25_pred_ann"].isna().sum() == 0, f"pm25_pred_ann has NaNs for {prov}"
        assert (df_out["pm25_pred_ann"] >= 0).all(), f"Negative PM2.5 predictions detected for {prov}"

    def test_forecast_invalid_province_handled_gracefully(self):
        cmd = [
            sys.executable,
            str(CODE_TRAINING / "du_bao.py"),
            "--province",
            "Tỉnh Không Tồn Tại",
            "--device",
            "cpu",
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT)
        # Should exit with non-zero code due to descriptive ValueError
        assert res.returncode != 0, "Execution should fail for non-existent province."
        combined_output = res.stdout + res.stderr
        assert (
            "Không tìm thấy tỉnh 'Tỉnh Không Tồn Tại'" in combined_output
            or "Các tỉnh hỗ trợ" in combined_output
        ), f"Error message was not descriptive:\n{combined_output}"


class TestAdversarialGoldParquetLoader:
    """Task 3: Check train_global_ann.py Gold Parquet loader vs CSV Pandas feature building."""

    def test_gold_parquet_loader_vs_csv_building(self):
        t0 = time.perf_counter()
        feats_pq, meta_pq, cont_pq, bin_pq, all_pq = load_features_dataset(use_parquet=True)
        pq_load_time = time.perf_counter() - t0

        t1 = time.perf_counter()
        feats_csv, meta_csv, cont_csv, bin_csv, all_csv = load_features_dataset(use_parquet=False)
        csv_build_time = time.perf_counter() - t1

        print(f"\n[GOLD LOADER BENCHMARK] Parquet Load={pq_load_time:.3f}s | CSV Build={csv_build_time:.3f}s")

        # 1. Check exact feature column definitions (49 features)
        assert len(all_pq) == 49, f"Expected 49 features, got {len(all_pq)}"
        assert all_pq == all_csv, f"Feature columns differ: PQ {len(all_pq)} vs CSV {len(all_csv)}"
        assert cont_pq == cont_csv, "Continuous columns differ between Gold Parquet and CSV"
        assert bin_pq == bin_csv, "Binary & Station columns differ between Gold Parquet and CSV"

        # 2. Check row counts
        assert len(feats_pq) == 280104, f"Gold Parquet has {len(feats_pq)} rows, expected 280,104"
        assert len(feats_pq) == len(feats_csv), (
            f"Row count mismatch: Gold Parquet {len(feats_pq)} vs CSV {len(feats_csv)}"
        )

        # 3. Check 0 NaNs in Gold Parquet
        nan_count = int(feats_pq[all_pq].isna().sum().sum())
        assert nan_count == 0, f"Gold Parquet has {nan_count} NaNs in feature columns!"
        target_nans = int(feats_pq["y"].isna().sum())
        assert target_nans == 0, f"Gold Parquet has {target_nans} NaNs in target y!"

        # 4. Check Station Metadata
        assert len(meta_pq) == len(meta_csv)
        pd.testing.assert_frame_equal(
            meta_pq.sort_values("station_idx").reset_index(drop=True),
            meta_csv.sort_values("station_idx").reset_index(drop=True),
            check_like=True,
            check_dtype=False,
        )

        # 5. Check Numerical Value Alignment
        sort_keys = ["station_id", "target_time"]
        f_pq_sorted = feats_pq.sort_values(sort_keys).reset_index(drop=True)
        f_csv_sorted = feats_csv.sort_values(sort_keys).reset_index(drop=True)

        # Target y difference
        max_y_diff = float(np.nanmax(np.abs(f_pq_sorted["y"].to_numpy() - f_csv_sorted["y"].to_numpy())))
        print(f"[GOLD EQUIVALENCE] Max y difference = {max_y_diff:.2e}")
        assert max_y_diff < 1e-4, f"Target y differs by {max_y_diff} (> 1e-4) between Gold Parquet and CSV"

        # Continuous feature max diff
        max_feature_diff = 0.0
        max_diff_col = ""
        for c in cont_pq:
            diff = np.abs(f_pq_sorted[c].to_numpy() - f_csv_sorted[c].to_numpy())
            curr_diff = float(np.nanmax(diff))
            if curr_diff > max_feature_diff:
                max_feature_diff = curr_diff
                max_diff_col = c

        print(f"[GOLD EQUIVALENCE] Max feature difference across continuous columns = {max_feature_diff:.2e} (col: {max_diff_col})")
        assert max_feature_diff < 1e-4, f"Feature {max_diff_col} differs by {max_feature_diff} (> 1e-4)!"

        # Binary/One-Hot max diff
        max_bin_diff = max(
            float(np.nanmax(np.abs(f_pq_sorted[c].to_numpy() - f_csv_sorted[c].to_numpy())))
            for c in bin_pq
        )
        assert max_bin_diff == 0.0, f"Binary feature differs by {max_bin_diff}!"
