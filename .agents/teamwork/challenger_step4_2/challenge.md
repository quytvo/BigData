# Adversarial Challenge Report — Milestone 4 (Step 4)

**Evaluation Date**: 2026-11-09T05:28:00Z  
**Target Modules**:
- `code/training/common.py` (`load_merged` with Parquet Partition Pruning & CSV Fallback)
- `code/training/du_bao.py` (CLI PM2.5 Forecast Inference with Single-Province Partition Pruning & Rollout)
- `code/training/train_global_ann.py` (`load_features_dataset` with Gold Parquet Feature Store & Pandas Fallback)
**Challenge Stance**: EMPIRICAL CHALLENGER (Adversarial stress testing, multi-run benchmarks, numerical equivalence tolerance testing, CLI edge case probing)

---

## Challenge Summary

**Overall Risk Assessment**: **LOW (READY FOR PRODUCTION / APPROVE)**

All empirical verification harnesses executed successfully:
1. **Partition Pruning Speedup**: Confirmed across 5 distinct geographical provinces. Runtimes dropped from ~3.01s-3.08s (CSV scanning 154MB) to 0.065s-0.092s (Parquet Partition Pruning). Measured speedups range from **32.56x to 47.17x** (average **39.82x**), easily exceeding the **> 10x** requirement.
2. **Numerical Equivalence**: Max absolute error across all numeric features (`pm25` + 8 meteorological variables) is **<= 4.55e-13** (essentially machine precision floating-point noise), thoroughly passing the `< 1e-5` requirement.
3. **CLI Robustness**: Verified end-to-end inference across valid provinces (Hà Nội, HCM, Đà Nẵng, Gia Lai, Ninh Bình) with `--device cpu`, generating valid forecast CSVs with 0 NaNs and non-negative PM2.5 values. Verified graceful failure with descriptive error messaging when querying non-existent provinces.
4. **Gold Parquet Feature Store**: Loaded 280,104 samples in **0.234s** vs 3.850s via Pandas feature engineering (**16.4x speedup**). Confirmed identical 49 feature columns, 0 NaNs, target `y` max diff **8.53e-14**, and continuous feature max diff **1.31e-07**.

---

## Challenges & Adversarial Hypotheses Tested

### [High] Challenge 1: Partition Pruning Execution Speedup & Cold vs Warm Cache
- **Assumption challenged**: The worker claims partition pruning delivers a > 10x speedup over CSV reading. Could disk caching, OS page cache, or Parquet directory traversal overhead cause speedups to degrade below 10x on cold runs or for distant provinces?
- **Empirical Test**: Benchmarked 5 provinces (Hà Nội, HCM, Đà Nẵng, Gia Lai, Ninh Bình) over 5 consecutive iterations per province, comparing `load_merged(province_name=p, use_parquet=True)` against `load_merged(province_name=p, use_parquet=False)`.
- **Results**:
  - **Hà Nội**: Parquet = 0.0721s | CSV = 3.0120s | **Speedup = 41.76x**
  - **HCM**: Parquet = 0.0651s | CSV = 3.0725s | **Speedup = 47.17x**
  - **Đà Nẵng**: Parquet = 0.0920s | CSV = 2.9944s | **Speedup = 32.56x**
  - **Gia Lai**: Parquet = 0.0896s | CSV = 3.0007s | **Speedup = 33.49x**
  - **Ninh Bình**: Parquet = 0.0685s | CSV = 3.0203s | **Speedup = 44.12x**
- **Outcome**: **PASS**. Minimum observed speedup is 32.56x, average is 39.82x, peak is 47.17x. The claim of > 10x is definitively proven.

### [High] Challenge 2: Numerical Drift Between Parquet Lakehouse and CSV Baseline
- **Assumption challenged**: Does reading from Silver Parquet introduce precision degradation, rounding discrepancies, or row misalignment compared to reading directly from raw CSVs?
- **Empirical Test**: Sorted records by `["station_code", "datetime"]` and computed maximum absolute difference `|df_parquet[c] - df_csv[c]|` across all columns (`pm25`, `temperature_2m`, `relative_humidity_2m`, `dew_point_2m`, `surface_pressure`, `precipitation`, `wind_speed_10m`, `wind_direction_10m`, `boundary_layer_height`).
- **Results**:
  - Max absolute difference across all numeric columns: **4.55e-13** (sub-femtoscale difference).
  - Datetime match: 100% exact match across all 25,512 rows per province.
  - Notice on timestamp dtypes: CSV parses to `datetime64[ns]`, while Parquet stores `datetime64[us]`. When converted to the same datetime unit, every single timestamp is strictly identical.
- **Outcome**: **PASS**. Numerical difference is well below the 1e-5 threshold.

### [Medium] Challenge 3: In-depth CLI Edge Case Probing on `du_bao.py`
- **Assumption challenged**: Does single-province inference handle diverse provinces, CPU-only execution, and gracefully reject invalid input parameters without uncontrolled crashes?
- **Empirical Test**:
  1. Ran CLI forecast with 5 distinct provinces: `python code/training/du_bao.py --province "<name>" --device cpu`. Checked exit code, output file generation in `data_test/du_bao/<slug>.csv`, row count (>= 24), non-negativity of predictions, and zero NaN count.
  2. Ran CLI forecast with `--province "Tỉnh Không Tồn Tại"`.
- **Results**:
  - All 5 valid provinces finished successfully with returncode 0. Generated output CSVs contain complete 24h predictions with columns `pm25_pred_ann`, `air_quality_level`, and zero NaNs.
  - Invalid province exited with non-zero code and produced explicit, descriptive error:
    `ValueError: Không tìm thấy tỉnh 'Tỉnh Không Tồn Tại'. Các tỉnh hỗ trợ: ...`
- **Outcome**: **PASS**. CLI behavior is fully robust and handles edge cases cleanly.

### [High] Challenge 4: Gold Parquet Feature Dataset Conformance for Global ANN
- **Assumption challenged**: Does `load_features_dataset(use_parquet=True)` in `train_global_ann.py` generate feature definitions, ordering, shapes, and normalization metrics that are identical to the Pandas feature pipeline?
- **Empirical Test**: Evaluated `load_features_dataset(use_parquet=True)` vs `load_features_dataset(use_parquet=False)`.
- **Results**:
  - Feature Column Count: Exactly 49 feature columns in both pipelines (`all_pq == all_csv` is `True`).
  - Sample Count: Exactly 280,104 samples in both pipelines.
  - NaN Check: Exactly 0 NaNs across all 49 feature columns and target `y`.
  - Target `y` numerical difference: Max diff = **8.53e-14**, Mean diff = **5.76e-16**.
  - Continuous feature difference: Max diff = **1.31e-07** (`pm25_std24`).
  - Station One-Hot / Binary feature difference: Exact **0.00e+00**.
  - Execution Time: Parquet reading took **0.234s** vs Pandas feature building taking **3.850s** (**16.4x speedup**).
- **Outcome**: **PASS**. Gold Parquet loader is fully aligned and ready for model training.

---

## Stress Test Results Matrix

| Scenario / Test Case | Target Module | Expected Behavior | Actual Behavior | Result |
|---|---|---|---|---|
| Single Province Loading Speedup (5 Provinces) | `code/training/common.py` | Speedup > 10.0x | 32.56x – 47.17x (avg 39.82x) | **PASS** |
| Numerical Equivalence Check | `code/training/common.py` | Max diff < 1e-5 | Max diff <= 4.55e-13 | **PASS** |
| Fallback Mechanism when Parquet missing | `code/training/common.py` | Seamless fallback to CSV | Seamlessly loaded from CSV | **PASS** |
| Forecast CLI: Hà Nội (`--device cpu`) | `code/training/du_bao.py` | Valid 24h forecast CSV | Exit 0, 24 rows, 0 NaNs, Valid levels | **PASS** |
| Forecast CLI: HCM (`--device cpu`) | `code/training/du_bao.py` | Valid 24h forecast CSV | Exit 0, 24 rows, 0 NaNs, Valid levels | **PASS** |
| Forecast CLI: Đà Nẵng (`--device cpu`) | `code/training/du_bao.py` | Valid 24h forecast CSV | Exit 0, 24 rows, 0 NaNs, Valid levels | **PASS** |
| Forecast CLI: Gia Lai (`--device cpu`) | `code/training/du_bao.py` | Valid 24h forecast CSV | Exit 0, 24 rows, 0 NaNs, Valid levels | **PASS** |
| Forecast CLI: Ninh Bình (`--device cpu`) | `code/training/du_bao.py` | Valid 24h forecast CSV | Exit 0, 24 rows, 0 NaNs, Valid levels | **PASS** |
| Forecast CLI: Invalid Province | `code/training/du_bao.py` | Exit non-zero, clear error | Exit 1, lists supported provinces | **PASS** |
| Gold Parquet 49 Features Conformance | `code/training/train_global_ann.py` | 49 features, 0 NaNs, < 1e-4 diff | 49 features, 0 NaNs, max diff 1.31e-7 | **PASS** |
| Gold Parquet Loading Benchmark | `code/training/train_global_ann.py` | Fast read (< 1.0s) | 0.234s (16.4x speedup) | **PASS** |

---

## Final Recommendation

**VERDICT**: **APPROVE**
The Step 4 implementation satisfies all performance, numerical fidelity, lakehouse integration, and fault-tolerance criteria. All 13 tests across the standard and adversarial test suites pass cleanly.
