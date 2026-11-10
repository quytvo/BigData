# Adversarial Challenge Report: Milestone 2 (Step 2 - Bronze to Silver Preprocessing)

**Agent**: Challenger 1 (Empirical Challenger)  
**Date**: 2026-11-09T03:59:00Z  
**Verdict**: **REQUEST_CHANGES**

---

## Challenge Summary

**Overall risk assessment**: **HIGH**

While the core numerical calculations in `lakehouse_storage/silver/tedp_province/` achieve extraordinary mathematical precision against the Pandas baseline (absolute errors $< 10^{-13}$ across PM2.5, PM10, AQI, and exact 0.0 difference on `pm25_gap_hours`), an empirical test execution of the official test suite `pytest tests/test_step2_bronze_silver.py -v` failed due to partition corruption in `lakehouse_storage/silver/tedp_station/`.

Specifically, dual-environment execution (Windows host vs. Linux Docker container) caused PySpark's Parquet writer to create 15 partition directories instead of 11. Four provinces (`Gia Lai`, `Lâm Đồng`, `Tây Ninh`, `Đà Nẵng`) exist concurrently with URL percent-encoded spaces (`%20`) and unencoded literal spaces (` `). Downstream consumers reading `tedp_station` ingest duplicated partition records (549,100 rows instead of ~350,000 rows).

Until `lakehouse_storage/silver/tedp_station/` is cleanly purged and rebuilt, and `spark_preprocess_tedp.py` is fortified against partition collision, Milestone 2 cannot be approved.

---

## Challenges

### [Critical] Challenge 1: Cross-Platform Partition Directory Collision & Row Inflation in `tedp_station`

- **Assumption challenged**: Running Spark with `.mode("overwrite").partitionBy("province_name").parquet(output_dir)` safely cleans and overwrites the station-level dataset across execution environments.
- **Attack scenario**: When `spark_preprocess_tedp.py` is executed on Windows, Hadoop's `Path` implementation formats directories with URL-encoded spaces (e.g. `province_name=Gia%20Lai`). When subsequently executed inside the Linux Docker container (`airquality-spark:3.5.1`), Hadoop's `Path` implementation formats directories with unencoded spaces (e.g. `province_name=Gia Lai`). Because Spark's `overwrite` operates per partition key resolved by the active JVM, it does not purge directory paths with differing URI encodings.
- **Empirical Observation**:
  - `lakehouse_storage/silver/tedp_station/` currently contains 15 partition subdirectories:
    - `province_name=Bắc%20Ninh`
    - `province_name=Gia Lai` (created 10:55 AM, 966 KB)
    - `province_name=Gia%20Lai` (created 10:53 AM, 970 KB)
    - `province_name=HCM`
    - `province_name=Hà%20Nội`
    - `province_name=Hưng%20Yên`
    - `province_name=Hải%20Phòng`
    - `province_name=Lâm Đồng` (created 10:55 AM, 788 KB)
    - `province_name=Lâm%20Đồng` (created 10:54 AM, 781 KB)
    - `province_name=Ninh%20Bình`
    - `province_name=Quảng%20Ngãi`
    - `province_name=Tây Ninh` (created 10:55 AM, 711 KB)
    - `province_name=Tây%20Ninh` (created 10:54 AM, 739 KB)
    - `province_name=Đà Nẵng` (created 10:55 AM, 789 KB)
    - `province_name=Đà%20Nẵng` (created 10:54 AM, 1,583 KB)
  - When loaded via standard `pd.read_parquet("lakehouse_storage/silver/tedp_station")` or PyArrow, both directory formats are normalized to the same province string, producing **549,100 rows** (double counting all measurements in Gia Lai, Lâm Đồng, Tây Ninh, Đà Nẵng).
  - Official test suite failure:
    ```
    FAILED tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_station_dataset_integrity
    AssertionError: Expected 11 partition subdirectories matching 'province_name=*', found 15
    assert 15 == 11
    ```
- **Blast radius**: Station-level data consumers, automated tests, and disk storage integrity are compromised. Fortunately, `spark_sync_province.py` escaped data corruption because it explicitly calls `dropDuplicates(["station_code", "datetime"])` at line 150.
- **Mitigation**:
  1. In `code/lakehouse/spark_preprocess_tedp.py`, ensure the destination directory is completely deleted prior to writing (e.g. `shutil.rmtree(output_dir, ignore_errors=True)`), or ensure consistent execution environment (Docker-only or host-only clean).
  2. Re-run `python code/lakehouse/spark_preprocess_tedp.py` to regenerate `lakehouse_storage/silver/tedp_station/` with exactly 11 partitions.

---

### [Low] Challenge 2: Non-Deterministic Station Median Calculation via `percentile_approx`

- **Assumption challenged**: Station-level median values are deterministic across independent Spark runs and partitions.
- **Attack scenario**: In `spark_preprocess_tedp.py` line 166:
  `F.expr(f"percentile_approx({c}, 0.5)").alias(f"{c}_st_median")`
  `percentile_approx` uses an approximation algorithm (t-digest / Greenwald-Khanna) with default accuracy 10,000. Depending on executor partitioning and shuffle splits, median values can fluctuate at the 2nd or 3rd decimal place (empirically observed: PM10 difference of $0.0077$ between two independent runs of Gia Lai station cleaning).
- **Blast radius**: Station-level median imputation can vary slightly between runs. However, since `spark_sync_province.py` line 165 masks median-filled records back to `NULL` prior to province aggregation, this non-determinism does NOT propagate to `tedp_province`.
- **Mitigation**: If exact bit-level determinism is desired at station level, specify higher accuracy (e.g. `percentile_approx(c, 0.5, 100000)`) or document that station median fill is purely a fallback indicator.

---

## Numerical Parity Deep Verification Results

Direct programmatic comparison of `lakehouse_storage/silver/tedp_province/` (Parquet) vs `data_test/cleaned_tedp_province_4y.csv` (Pandas CSV baseline) across all **280,632 rows**:

| Metric / Column | Spark Silver Min / Max | Pandas CSV Min / Max | Max Abs Diff ($\max \|X_S - X_P\|$) | Mean Abs Diff | Status (< 1e-5) |
|:---|:---:|:---:|:---:|:---:|:---:|
| `pm25` | 0.0 / 232.8 | 0.0 / 232.8 | **8.5265e-14** | 5.7529e-16 | **PASS (Flawless)** |
| `pm10` | 0.0 / 370.2 | 0.0 / 370.2 | **2.8422e-14** | 4.5914e-16 | **PASS (Flawless)** |
| `aqi` | 0.0 / 283.0 | 0.0 / 283.0 | **8.5265e-14** | 5.8594e-16 | **PASS (Flawless)** |
| `pm25_gap_hours` | 0 / 805 | 0 / 805 | **0.0000e+00** | 0.0000e+00 | **PASS (Exact Match)** |
| `latitude` | 10.75 / 21.53 | 10.75 / 21.53 | **1.7764e-15** | 1.1272e-16 | **PASS** |
| `longitude` | 105.78 / 108.97 | 105.78 / 108.97 | **0.0000e+00** | 0.0000e+00 | **PASS** |

### Per-Province Parity Breakdown (25,512 rows each)

| Province | Rows | Max Diff PM2.5 | Max Diff PM10 | Max Diff AQI | Max Diff GapHours | Status |
|:---|:---:|:---:|:---:|:---:|:---:|
| Bắc Ninh | 25,512 | 5.68e-14 | 1.42e-14 | 5.68e-14 | 0.00e+00 | PASS |
| Gia Lai | 25,512 | 7.11e-15 | 7.11e-15 | 1.42e-14 | 0.00e+00 | PASS |
| HCM | 25,512 | 4.26e-14 | 2.13e-14 | 2.84e-14 | 0.00e+00 | PASS |
| Hà Nội | 25,512 | 5.68e-14 | 2.84e-14 | 5.68e-14 | 0.00e+00 | PASS |
| Hưng Yên | 25,512 | 8.53e-14 | 2.84e-14 | 8.53e-14 | 0.00e+00 | PASS |
| Hải Phòng | 25,512 | 5.68e-14 | 1.42e-14 | 5.68e-14 | 0.00e+00 | PASS |
| Lâm Đồng | 25,512 | 5.68e-14 | 7.11e-15 | 5.68e-14 | 0.00e+00 | PASS |
| Ninh Bình | 25,512 | 5.68e-14 | 2.84e-14 | 5.68e-14 | 0.00e+00 | PASS |
| Quảng Ngãi | 25,512 | 5.68e-14 | 2.84e-14 | 1.42e-14 | 0.00e+00 | PASS |
| Tây Ninh | 25,512 | 5.68e-14 | 1.42e-14 | 1.42e-14 | 0.00e+00 | PASS |
| Đà Nẵng | 25,512 | 2.84e-14 | 1.42e-14 | 2.84e-14 | 0.00e+00 | PASS |

### Categorical & Discrete Invariant Checks

- `pm25_imputed`, `pm10_imputed`, `aqi_imputed`, `is_inserted`: **0 mismatches** (100% agreement).
- `n_stations_member`, `n_stations_valid_pm25`: **0 mismatches**.
- `year`, `month`, `day`, `hour`, `day_of_week`, `day_name`, `is_weekend`, `date`, `time_slot`: **0 mismatches**.
- `station_code`, `station_name`, `station_short_name`, `aqi_category`, `province_id`, `station_id`: **0 mismatches**.
- Invariant `(pm25_gap_hours > 0) <=> (is_inserted == 1) <=> (pm25_imputed == 1)`: **100% satisfied across all 280,632 rows**.

---

## Stress Test Results

Executed automated test suites:
1. `pytest tests/test_step2_bronze_silver.py -v`:
   - `test_silver_station_dataset_integrity`: **FAIL** (`AssertionError: Expected 11 partition subdirectories, found 15`)
   - `test_silver_province_directory_and_partitioning`: **PASS**
   - `test_silver_province_static_struct_schema`: **PASS**
   - `test_silver_province_row_count_and_continuity`: **PASS**
   - `test_silver_province_zero_nulls_in_core_metrics`: **PASS**
   - `test_silver_province_numerical_tolerance_vs_pandas`: **PASS**
   - `test_silver_weather_province_integrity_and_tolerance`: **PASS**
   - **Result**: 1 FAILED, 6 PASSED.

2. `pytest tests/test_step2_adversarial.py -v`:
   - `test_01_total_row_count_and_cardinality`: **PASS**
   - `test_02_deep_numerical_error_bound_pollutants`: **PASS**
   - `test_03_pm25_gap_hours_exact_match`: **PASS**
   - `test_04_per_province_numerical_parity`: **PASS**
   - `test_05_all_discrete_and_string_columns_match`: **PASS**
   - `test_06_weather_silver_exhaustive_parity`: **PASS**
   - `test_07_gap_flags_invariant`: **PASS**
   - `test_08_qcvn_physical_ranges_in_silver`: **PASS**
   - **Result**: 8 PASSED.

---

## Unchallenged Areas

- **Incremental streaming updates**: Step 2 is designed and validated as a batch 4-year grid synchronization ($2023-12-11$ to $2026-11-07$); streaming micro-batches were not evaluated as they are out of scope for this milestone.
- **Silver to Gold Feature Engineering (Step 3)**: Atmospheric lag features and rolling averages belong to Milestone 3 and will be challenged under Milestone 3 dispatch.

---

## Conclusion & Actionable Recommendations

Step 2 displays exceptional mathematical fidelity in its province-level Catalyst Window transformations, achieving error bounds ten orders of magnitude tighter than the required $10^{-5}$ tolerance. However, the physical disk layout of `lakehouse_storage/silver/tedp_station/` contains duplicate partition directories resulting from cross-environment execution, causing test suite failure and 100% row duplication on read.

**Action Required**:
1. Purge `lakehouse_storage/silver/tedp_station/` completely.
2. Update `spark_preprocess_tedp.py` to clean the destination folder before writing, or re-run `python code/lakehouse/spark_preprocess_tedp.py` in a clean target directory.
3. Re-run `pytest tests/test_step2_bronze_silver.py -v` to achieve 7/7 PASSED.
4. Resubmit for Challenger approval.
