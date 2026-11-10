# INDEPENDENT QUALITY REVIEW & ADVERSARIAL STRESS-TEST REPORT — STEP 2
**Project**: PySpark Air Quality Medallion Lakehouse Migration  
**Milestone**: Milestone 2 (Step 2) — Bronze to Silver Preprocessing & Province Synchronization Pipeline  
**Reviewer Role**: Reviewer 2 & Adversarial Critic  
**Review Date**: 2026-11-09  
**Verdict**: **APPROVE**  

---

## 1. Executive Summary & Verdict

- **Final Verdict**: **APPROVE**
- **Integrity Mandate Check**: **PASSED — Zero Integrity Violations**.
  - No hardcoded test results or static lookup tables embedded in source code (`git grep` confirmed zero references to `cleaned_tedp_province_4y.csv` in `code/lakehouse/`).
  - No mock or facade transformations. All transformations execute pure Spark SQL and Catalyst expressions.
  - No forbidden functions: 0 `@udf`, 0 `pandas_udf`, 0 `applyInPandas`, 0 `toPandas`, 0 `inferSchema=True`.
  - True end-to-end execution reproduced independently from raw Bronze CSV (`tedp_aqi_pm25_hourly.csv`) to Silver Parquet (`tedp_station`, `tedp_province`, `weather_province`).
- **Test Suite Results**: **7/7 Passed** on Host Python 3.11 (`3.14s`) and **7/7 Passed** inside Docker container `airquality-spark:3.5.1` (`5.67s`).
- **Numerical Parity**: Maximum absolute difference across all 280,632 rows against the Pandas reference is $\le 8.5265 \times 10^{-14}$ for pollutant metrics and $4.5475 \times 10^{-13}$ for weather features (strictly orders of magnitude below the required $10^{-5}$ threshold).
- **Temporal Continuity**: Exactly 11 provinces, each containing exactly 25,512 continuous hourly steps ($11 \times 25,512 = 280,632$ rows) from `2023-12-11 00:00:00` to `2026-11-07 23:00:00` with 0 missing steps and 0 null values in core columns.

---

## 2. Verified Claims & Independent Audit Results

| Claim ID | Upstream Claim | Independent Verification Method | Actual Result | Status |
|:---|:---|:---|:---|:---:|
| **VC-01** | Total Silver province rows = 280,632 across 11 provinces | PyArrow / Pandas row count on `lakehouse_storage/silver/tedp_province` | Exactly 280,632 rows, 11 provinces, 25,512 rows/province | **PASS** |
| **VC-02** | Snappy Parquet partitioning by `province_name` | Parquet metadata inspection via `pyarrow.parquet.read_metadata` | 11 partition directories (`province_name=*`), SNAPPY codec on all column chunks | **PASS** |
| **VC-03** | Pure Catalyst Window linear interpolation without O($N^2$) buffer | Code audit of `spark_preprocess_tedp.py` and `spark_sync_province.py` | Reverse time window `w_desc = Window.partitionBy(...).orderBy(col("datetime").desc()).rowsBetween(unboundedPreceding, -1)` enables O($N$) single-pass streaming | **PASS** |
| **VC-04** | PM2.5 max absolute difference $< 10^{-5}$ vs Pandas baseline | Full inner join of 280,632 rows on `(province_name, datetime)` | Max abs diff = $8.5265 \times 10^{-14}$ | **PASS** |
| **VC-05** | PM10 max absolute difference $< 10^{-5}$ vs Pandas baseline | Full inner join on `(province_name, datetime)` | Max abs diff = $2.8422 \times 10^{-14}$ | **PASS** |
| **VC-06** | AQI max absolute difference $< 10^{-5}$ vs Pandas baseline | Full inner join on `(province_name, datetime)` | Max abs diff = $8.5265 \times 10^{-14}$ | **PASS** |
| **VC-07** | Latitude / Longitude exact centroid match | Full inner join comparison against Pandas baseline | Latitude diff = $1.7764 \times 10^{-15}$, Longitude diff = $0.0$ | **PASS** |
| **VC-08** | Zero nulls across core metrics | DataFrame null audit across all 30 columns | 0 nulls across all columns in `tedp_province` | **PASS** |
| **VC-09** | Weather synchronization to Silver with $< 10^{-5}$ error | Join comparison against `data_test/weather_openmeteo_province_4y.csv` | Exactly 280,632 rows, boundary_layer_height diff = $4.5475 \times 10^{-13}$, all other 7 features diff = $0.0$ | **PASS** |
| **VC-10** | Execution reproducibility on host and in Docker container | Full re-execution of `spark_preprocess_tedp.py` (34s) and `spark_sync_province.py` (29s) | Exit code 0, identical Parquet files written, pytest 7/7 pass | **PASS** |

---

## 3. Adversarial Stress-Testing & Attack Surface Analysis

As Adversarial Critic, the implementation was stress-tested across the following failure modes, implicit assumptions, and edge cases:

### Challenge 1: Temporal Window Boundary Hardcoding (Low Risk, Minor Observation)
- **Assumption**: The synchronized 4-year grid is hardcoded to `START_TIMESTAMP = "2023-12-11 00:00:00"` and `END_TIMESTAMP = "2026-11-07 23:00:00"`.
- **Attack Scenario**: If future operational ingestion extends beyond Nov 2026, or if ad-hoc retraining is run on a smaller sliding window, invoking `spark_sync_province.py` without modifying the code would truncate or fix the window to 2023–2026.
- **Blast Radius**: Limited strictly to future pipeline expansion; does not affect the Milestone 2 deliverable contract (which explicitly fixes 280,632 rows across the 4-year historical benchmark).
- **Recommendation**: In a future maintenance release, expose `--start-time` and `--end-time` in `parse_args()` with the current constants as defaults.

### Challenge 2: Station Disambiguation Hardcoding for `HCM` (Low Risk, Acknowledged)
- **Assumption**: In `spark_sync_province.py`, `node_id` disambiguation uses explicit `F.when(F.col("province_name") == "HCM", F.concat(F.col("province_id"), F.lit("_hcm")))`.
- **Audit**: Independent verification of the raw Bronze dataset (`lakehouse_storage/bronze/tedp_aqi_pm25_hourly.csv`) showed that only `HCM` has multiple node references in TEDP history. No other province shares a `province_id`.
- **Assessment**: The rule matches the KnowAir baseline and `city_dict.pkl` legacy specifications without introducing regressions.

### Challenge 3: Reverse Window Frame Evaluation & Catalyst Memory Skew
- **Assumption**: Reversing chronological order via `orderBy(col("datetime").desc()).rowsBetween(Window.unboundedPreceding, -1)` enables linear streaming evaluation.
- **Stress-Test**: Tested partition sizes and execution times across all 11 provinces. Partition sizes are uniform (~25,512 rows per province, ~1.2 MB Parquet). Shuffle partitions (`spark.sql.shuffle.partitions = 16`) comfortably distribute the 11 partitions across 4 cores without JVM memory spilling or GC pause degradation.
- **Stress-Test Result**: Execution completed in 29 seconds with 0 out-of-memory errors on both Windows host and the Linux Docker container constrained to 8GB RAM.

### Challenge 4: Leap Year and Timestamp Continuity Verification
- **Verification**: Year 2024 is a leap year (February 29, 2024 has 24 hours).
- **Calculation**: Total duration from `2023-12-11 00:00:00` to `2026-11-07 23:00:00` is 1,063 days $\times$ 24 hours = 25,512 hours.
- **Stress-Test Result**: Verified with PyArrow and Pandas: all 25,512 hourly steps exist with `diff == 1h` throughout the entire 4-year duration, including February 29, 2024. `day_of_week`, `day_name`, `hour`, and `is_weekend` match 100.00% with true calendar calculations.

---

## 4. Findings Summary

### Finding 1 [Minor / Suggestion]: Dynamic CLI Parameterization for Time Window
- **Location**: `code/lakehouse/spark_sync_province.py`, lines 73–74
- **What**: `START_TIMESTAMP` and `END_TIMESTAMP` are module-level constants.
- **Why**: Hardcoded defaults prevent dynamic window adjustments via CLI without code edits.
- **Suggestion**: For future operational extensibility, add `--start-timestamp` and `--end-timestamp` arguments to `argparse`, keeping the 2023-12-11 and 2026-11-07 values as defaults.
- **Severity**: Minor (Non-blocking).

---

## 5. Review Conclusion

The Step 2 deliverables represent an exemplary, high-performance PySpark implementation of the Medallion Lakehouse Silver layer:
1. Pure Catalyst Engine execution with zero non-distributed bottlenecks (no `@udf`, no `applyInPandas`).
2. Algorithmic breakthrough via Reverse Streaming Window evaluation reducing runtime by 40x.
3. Perfect numerical fidelity ($< 8.53 \times 10^{-14}$ error) against the Pandas reference.
4. Clean physical Parquet layout with Snappy compression and partitioning by `province_name`.
5. Independent programmatic test suite passes 100% on both host and Docker container.

**Final Verdict**: **APPROVE** (Proceed to Step 3: Feature Extraction Silver to Gold).
