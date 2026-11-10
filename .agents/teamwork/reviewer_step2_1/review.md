# Review Report — Step 2: Bronze to Silver Preprocessing Pipeline

**Reviewer**: Reviewer 1 (Adversarial Critic & Quality Reviewer)  
**Date**: 2026-11-09  
**Target Milestone**: Milestone 2 (Step 2: Bronze to Silver Pipeline)  
**Scope**: `code/lakehouse/spark_preprocess_tedp.py`, `code/lakehouse/spark_sync_province.py`, `lakehouse_storage/silver/`, `tests/test_step2_bronze_silver.py`, `docs/review_checkpoint_step2.md`

---

## 1. Review Summary

**Verdict**: **APPROVE**

The deliverables for Milestone 2 (Step 2) fully comply with the technical architecture specifications, integrity mandates, and numerical accuracy requirements. The implementation successfully converts the station-level cleaning and province synchronization pipelines from Pandas to pure Apache Spark Catalyst Window functions with zero forbidden antipatterns (`applyInPandas`, `@udf`, `inferSchema=True`).

All 7 programmatic test cases in `tests/test_step2_bronze_silver.py` pass cleanly in both the host environment and within the standardized Docker container (`airquality-spark:3.5.1`, 8GB RAM, 4 CPUs). Numerical differences against the reference baseline (`data_test/cleaned_tedp_province_4y.csv`) are on the order of $\sim 10^{-14}$ (well below the required $10^{-5}$ threshold). Re-execution of both pipelines from scratch took ~31s and ~35s respectively, confirming complete algorithmic reproducibility.

---

## 2. Technical Compliance Audit

| Requirement / Standard | Rule / Constraint | Actual Implementation Status | Result |
|:---|:---|:---|:---:|
| **Catalyst Engine Execution** | 100% Spark SQL & Catalyst Window functions | Uses `Window.partitionBy`, `orderBy`, `rowsBetween`, `sequence`, `explode`, `crossJoin`, `percentile_approx`, `broadcast` | **PASS** |
| **No Python `@udf`** | ZERO `@udf` or `F.udf` in pipeline code | Static code analysis confirms 0 occurrences across all Step 2 scripts | **PASS** |
| **No `applyInPandas`** | ZERO `applyInPandas` or Pandas UDFs | Static code analysis confirms 0 occurrences across all Step 2 scripts | **PASS** |
| **No `inferSchema=True`** | Explicit static `StructType` schema required | Raw bronze CSV loaded via `BRONZE_TEDP_SCHEMA`; weather CSV loaded via `SILVER_WEATHER_PROVINCE_SCHEMA` | **PASS** |
| **Parquet Storage Format** | Snappy-compressed Parquet partitioned by `province_name` | Verified on `tedp_station/`, `tedp_province/`, and `weather_province/` | **PASS** |
| **Row Count Integrity** | Exactly 280,632 rows (11 provinces x 25,512 hours) | Verified via PyArrow and Pandas: 280,632 rows total, 25,512 rows per province | **PASS** |
| **Temporal Continuity** | Continuous 1-hour interval without gaps or duplicates | Start: `2023-12-11 00:00:00`, End: `2026-11-07 23:00:00`, zero missing hours | **PASS** |
| **Zero Nulls in Core Metrics** | `pm25`, `pm10`, `aqi`, `datetime`, `province_name` must have 0 nulls | Verified 0 null values across all 280,632 records in `tedp_province` | **PASS** |
| **Numerical Tolerance Bound** | Max absolute difference $< 10^{-5}$ vs Pandas | Measured max difference: $8.5265 \times 10^{-14}$ | **PASS** |

---

## 3. Verified Claims & Independent Data Reconciliation

The following verification commands were independently executed during review:

1. **Host Pytest Execution**:
   - Command: `pytest tests/test_step2_bronze_silver.py`
   - Result: `7 passed in 3.23s` (100% passed).
2. **Docker Container Pytest Execution**:
   - Command: `docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest tests/test_step2_bronze_silver.py`
   - Result: `7 passed in 7.55s` (100% passed).
3. **Pipeline End-to-End Reproducibility (From Bronze CSV)**:
   - `python code/lakehouse/spark_preprocess_tedp.py`: completed in 31 seconds.
   - `python code/lakehouse/spark_sync_province.py`: completed in 35 seconds.
   - Newly generated Parquet re-tested with 7/7 tests passing.
4. **Data Diff Audit against Baseline (`data_test/cleaned_tedp_province_4y.csv`)**:
   - Total rows compared: `280,632`
   - Max absolute difference `pm25`: `8.52651283e-14` ($< 10^{-5}$)
   - Max absolute difference `pm10`: `2.84217094e-14` ($< 10^{-5}$)
   - Max absolute difference `aqi`: `8.52651283e-14` ($< 10^{-5}$)
   - Max absolute difference `latitude`: `1.77635684e-15` ($< 10^{-5}$)
   - Max absolute difference `longitude`: `0.00000000e+00` (exact 0)
   - Max absolute difference `n_stations_valid_pm25`: `0.00000000e+00` (exact 0)
   - Max absolute difference `is_inserted`: `0.00000000e+00` (exact 0)
   - Max absolute difference `pm25_gap_hours`: `0.00000000e+00` (exact 0)
   - Exact string matches (`station_name`, `station_short_name`, `aqi_category`): `280,632 / 280,632` ($100.0\%$)
5. **Weather Data Diff Audit (`data_test/weather_openmeteo_province_4y.csv`)**:
   - Max absolute difference across 8 meteorological features: $\le 4.5475 \times 10^{-13}$ ($< 10^{-5}$)

---

## 4. Adversarial Review & Stress-Testing

### Dimension 1: Algorithmic Complexity & Window Buffer Bottlenecks
- **Hypothesis**: Replacing forward unbounded window scans (`rowsBetween(1, unboundedFollowing)`) with reverse streaming windows (`orderBy(desc).rowsBetween(unboundedPreceding, -1)`) might introduce order inconsistency or subtle floating-point drift.
- **Stress-Test Finding**: The reverse window approach preserves strict chronological pairing while reducing time complexity from $O(N^2)$ to $O(N)$ streaming per partition. Total execution time dropped from $>20$ minutes to 31–35 seconds. Numerical differences remained within double-precision rounding epsilon ($10^{-14}$), demonstrating robust mathematical equivalence.

### Dimension 2: Edge-Case Boundary Conditions
- **Leading & Trailing Gaps**: In `spark_sync_province.py`, bidirectional time interpolation handles the case where valid data begins after `START_TIMESTAMP` or ends before `END_TIMESTAMP` via `when(y_pre.isNull() & y_post.isNotNull(), y_post)` and `when(y_pre.isNotNull() & y_post.isNull(), y_pre)`.
- **Gap Length Arithmetic**: `pm25_gap_hours` computation at the start and end of time series matches exact hour offsets from `START_TIMESTAMP` and `END_TIMESTAMP`, yielding exact $0.0$ difference against Pandas.

### Dimension 3: Station Filtering & Spatial Centroids
- **Observation**: Station `BDi_KKXQ_KHITPH` in Gia Lai has low coverage ($31.1\%$) and was excluded before computing province spatial centroids and aggregations.
- **Verification**: Exclusion was verified in `preprocess_station_records()`. Province centroids match the reference coordinates with max diff $1.77 \times 10^{-15}$.

---

## 5. Integrity Audit Attestation

The implementation was examined for common integrity violation vectors:
- **Hardcoded test outputs / Facades**: NEGATIVE. No lookup tables, precomputed tensors, or canned outputs were detected. Transformations are executed entirely through dynamic Catalyst expressions on raw input files.
- **Self-certifying work**: NEGATIVE. The test suite is independent, reads Parquet files from disk, and performs join-based reconciliation against reference files.
- **Shortcuts / Driver Delegation**: NEGATIVE. Zero conversion to Pandas in core logic (`toPandas()` is absent from both pipeline scripts). All operations are pushed down into the JVM Tungsten execution engine.

---

## 6. Conclusion & Recommendation

The Step 2 deliverables (`spark_preprocess_tedp.py`, `spark_sync_province.py`, `lakehouse_storage/silver/`) meet all acceptance criteria with flying colors. Milestone 2 is officially verified and approved.

The project is cleared to transition immediately to **Milestone 3 (Step 3: Silver to Gold Feature Extraction)**.
