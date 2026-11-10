# Technical & Adversarial Review Report: Milestone 3 (Step 3: Silver to Gold 49 Features Pipeline & Master Lakehouse Runner)

**Reviewer**: Reviewer 2 (Roles: Reviewer, Critic)  
**Target Delivery**:
- `code/lakehouse/spark_feature_engineering.py`
- `code/lakehouse/build_lakehouse_spark.py`
- `lakehouse_storage/gold/global_ann_features/`
- `docs/review_checkpoint_step3.md`
- `output/lakehouse_verification_report.txt`
**Verification Date**: 2026-11-09  

---

## 1. Review Summary

**Verdict**: **APPROVE**  
**Integrity Status**: **VERIFIED CLEAN** (No hardcoded test outcomes, no facade implementations, 100% pure Spark Catalyst Window functions, genuine independent verification).

The deliverables for Milestone 3 (Step 3) are technically sound, strictly adhere to requirements, achieve near machine-precision numerical parity with the Pandas reference ($1.314 \times 10^{-7} \ll 10^{-5}$), enforce static schemas without type inference, and execute reliably in the Docker container (`airquality-spark:3.5.1`).

---

## 2. Integrity Verification (Anti-Cheat & Anti-Facade Audit)

A rigorous adversarial inspection of source files and test fixtures was conducted:
1. **Source Code Audit (`spark_feature_engineering.py`)**:
   - Zero Python `@udf`, zero `applyInPandas`, zero `toPandas()` in the core ETL execution path.
   - All 49 atmospheric features and target label $y$ are computed natively on the Spark Catalyst engine using `pyspark.sql.functions` (`lead`, `lag`, `avg`, `stddev_samp`, `sin`, `cos`, `sum`, `when`, `radians`, `hour`, `weekday`) over partitioned `Window` frames.
   - Output directory management uses a deterministic retry loop with permission resets (`purge_output_directory`), eliminating cross-platform artifact remnants.
2. **Master Orchestrator Audit (`build_lakehouse_spark.py`)**:
   - The automated Data Diff engine dynamically parses the written Gold Parquet files via `pyarrow`, computes the ground truth Pandas baseline dynamically from `cleaned_tedp_province_4y.csv` and `weather_openmeteo_province_4y.csv`, and validates differences across all 50 columns.
   - Outputs are written dynamically to `output/lakehouse_verification_report.txt`.
3. **Test Integrity (`test_step3_silver_gold.py`)**:
   - Tests read directly from the physical Parquet storage files (`lakehouse_storage/gold/global_ann_features/`). No mock objects or hardcoded return values are present.

---

## 3. Quality Review Dimensions

### 3.1. Correctness & Mathematical Parity
- **Full Row Count**: Exactly 280,104 rows across 11 provinces ($25,464 \times 11$).
  - Raw Silver hours per province: 25,512.
  - Trimmed boundary rows: exactly 48 hours (24h past lookback for `pm25_lag24`/`pm25_roll24`, and 24h future lookahead for `lead("pm25", 24)` and future weather).
  - $25,512 - 48 = 25,464$ valid rows per province.
- **Filtered Row Count (Cutoff Date)**: Exactly 279,840 rows ($25,440 \times 11$) when `target_time <= '2026-11-06 23:00:00'`.
  - Exactly matches `n_total_samples: 279840` in `output/global_ann_4y/metrics.json`.
- **Numerical Error**: Maximum absolute error across all 50 evaluated columns ($y$ + 49 features) is $1.31437461 \times 10^{-7} \ll 10^{-5}$ (observed on `pm25_std24` due to sample standard deviation floating-point aggregation algorithms between Welford and Two-Pass). For 49 out of 50 columns, error is $\le 1.819 \times 10^{-12}$ or identically $0.0$.
- **Null / NaN Count**: Exactly 0 nulls across all 280,104 rows $\times$ 61 columns in the Gold Parquet dataset.

### 3.2. Static Schema & Layout Conformance
- Static schema `GOLD_FEATURE_SCHEMA` strictly defines data types (`StringType`, `DoubleType`, `IntegerType`, `TimestampType`).
- Snappy-compressed Parquet partitioned by `province_name` with exactly 11 subdirectories (`province_name=Bắc Ninh`, etc.).
- Column ordering is canonical and aligned with model checkpoint `output/global_ann_4y/best_ann_model.pt`.

### 3.3. Container Execution & Performance
- Executed natively inside `airquality-spark:3.5.1` (Linux, Python 3.8.10, PySpark 3.5.1).
- Execution runtime for Step 3 transformation is approximately **14.89s – 15.54s** on 4 CPU cores, well within memory limits (no OOM).

---

## 4. Adversarial Stress-Testing & Edge Case Analysis

| Test Dimension | Stress Test Scenario | Expected Invariant | Observed Result | Status |
|---|---|---|---|:---:|
| **Temporal Invariant** | Evaluate `(target_time - origin_time)` for all rows | Delta must equal exactly 24.0 hours (86,400s) | Min: 24.0h, Max: 24.0h for all 280,104 rows | **PASS** |
| **Hourly Continuity** | Calculate step difference of consecutive timestamps per province | $\Delta t = 1.0$ hour strictly; 0 missing steps, 0 duplicates | Strictly 1.0h across all 11 provinces | **PASS** |
| **Cyclic Ranges** | Check value boundaries for `hour_sin`, `hour_cos`, `dow_sin`, `dow_cos` | All values in $[-1.0, 1.0]$ | `hour_sin/cos`: $[-1.0, 1.0]$<br>`dow_sin`: $[-0.9749, 0.9749]$<br>`dow_cos`: $[-0.9010, 1.0]$ | **PASS** |
| **Cyclic Identity** | Test Pythagorean identity $\sin^2(\theta) + \cos^2(\theta) == 1.0$ | Deviation $< 10^{-12}$ | Max error for `hour`: $1.11 \times 10^{-16}$<br>Max error for `dow`: $2.22 \times 10^{-16}$ | **PASS** |
| **Spatial Normalization** | Check min/max of `latitude_norm`, `longitude_norm` | Normalized values in $[-1.0, 1.0]$ | `latitude_norm`: $[-1.0, 1.0]$<br>`longitude_norm`: $[-1.0, 1.0]$ | **PASS** |
| **Thermodynamic Bounds** | Inspect `ventilation_coef`, `dew_point_deficit`, `precip_sum_24h` | Physically non-negative / consistent | `ventilation_coef`: $[0.0, 43279.52]$<br>`dew_point_deficit`: $[-0.05, 27.90]$<br>`precip_sum_24h`: $[0.0, 275.30]$ | **PASS** |
| **One-Hot Exclusivity** | Sum all 11 `station_id_*` indicator columns row-wise | $\sum \text{station\_id\_*} == 1.0$ strictly | Exactly 1.0 for all 280,104 rows | **PASS** |
| **Station Mapping Alignment** | Match `station_idx` (0..10) against one-hot columns and `province_name` | Exact 1:1 bijection matching `best_ann_model.pt` | Exact match confirmed against `station_meta` in PyTorch model checkpoint | **PASS** |
| **Model Feature Order** | Check sequence of 49 features against PyTorch checkpoint `all_feature_cols` | Exact string match and exact index match | 100% identical match across all 49 features | **PASS** |

---

## 5. Verified Claims Matrix

| Claim from Worker / Docs | Verification Method | Outcome |
|---|---|:---:|
| 7/7 pytest cases pass on host | `pytest tests/test_step3_silver_gold.py -v` | **PASSED** (2.42s) |
| 7/7 pytest cases pass in Docker | `docker run --rm -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 pytest tests/test_step3_silver_gold.py -v` | **PASSED** (2.42s) |
| Master runner executes Step 3 + Data Diff in Docker | `docker run --rm -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 python3 code/lakehouse/build_lakehouse_spark.py --steps 3 --verify-diff` | **PASSED** (15.54s) |
| Numerical difference $< 10^{-5}$ across all 50 columns | Programmatic Data Diff against Pandas ground truth | **PASSED** (Max: $1.314 \times 10^{-7}$) |
| Total samples: 280,104 full / 279,840 filtered | Parquet metadata and DataFrame length evaluation | **PASSED** (Exact matches) |
| Zero NaN / Null values across Gold dataset | Null count query on all 61 Parquet columns | **PASSED** (0 nulls found) |

---

## 6. Coverage Gaps & Caveats

- **Coverage Gaps**: None for Step 3. All components (ETL script, master runner, verification report, review documentation, test suite) were verified in both host and Docker environments.
- **Caveat / Note**: When inspecting Parquet directories in intermediate states (e.g. while Spark jobs are currently writing), readers must wait for Spark to finalize `_SUCCESS` to avoid reading partial partitions. The pipeline handles this robustly by purging target directories prior to write.

---

## 7. Verdict & Recommendation

**VERDICT: APPROVE**

The implementation of Milestone 3 (Step 3: Silver to Gold 49 Features Pipeline & Master Lakehouse Runner) meets all functional, numerical, structural, and infrastructure requirements. The project is completely ready to proceed to **Milestone 4 (Step 4: Model Integration & Parquet Partition Pruning)**.
