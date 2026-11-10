# Adversarial Challenge Report — Milestone 2 Iteration 2 (Step 2 Remediation)

## Challenge Summary

- **Target Component**: Lakehouse Medallion Silver Layer (`lakehouse_storage/silver/tedp_station/`, `tedp_province/`, `weather_province/`)
- **Remediation Under Review**: Partition collision remediation, `purge_output_directory(target_dir)` idempotency, cross-platform URL-encoding vs. literal spaces, deduplication, and row count verification.
- **Overall Risk Assessment**: **LOW** (Empirically verified — zero residual collision, 100% test pass rate across Host and Docker container).
- **Explicit Verdict**: **APPROVE**

---

## Adversarial Challenges & Hypotheses Tested

### [High] Challenge 1: Partition Collision Recurrence & Stale Partition Accumulation
- **Assumption Challenged**: Calling Spark `df.write.mode("overwrite").partitionBy("province_name").parquet(...)` does NOT wipe obsolete or cross-platform partition directories (e.g. `province_name=Gia%20Lai` vs `province_name=Gia Lai`). Does the remediation reliably eliminate collision risk?
- **Attack Scenario**: Execute pipeline across Windows host (URL-encoded `%20`) and Linux Docker container (literal space ` `). In Iteration 1, this caused 15 partition directories and 549,100 rows (+109,088 duplicate rows).
- **Empirical Stress Test**:
  1. Direct filesystem scan of `lakehouse_storage/silver/tedp_station/`:
     - Discovered exactly **11 subdirectories**:
       `['province_name=Bắc%20Ninh', 'province_name=Gia%20Lai', 'province_name=HCM', 'province_name=Hà%20Nội', 'province_name=Hưng%20Yên', 'province_name=Hải%20Phòng', 'province_name=Lâm%20Đồng', 'province_name=Ninh%20Bình', 'province_name=Quảng%20Ngãi', 'province_name=Tây%20Ninh', 'province_name=Đà%20Nẵng']`.
     - Zero unencoded duplicate directories found.
  2. Row count verification via PyArrow dataset and PySpark DataFrame:
     - Exact total row count: **440,012**.
     - Per-partition row counts:
       - HCM: 101,475
       - Hưng Yên: 51,327
       - Đà Nẵng: 51,076
       - Hà Nội: 50,482
       - Gia Lai: 32,638
       - Bắc Ninh: 25,849
       - Hải Phòng: 25,626
       - Quảng Ngãi: 25,617
       - Lâm Đồng: 25,590
       - Tây Ninh: 25,483
       - Ninh Bình: 24,849
       - Sum: **440,012** (100% match, zero duplicates).
  3. Deduplication audit:
     - `df.duplicated(subset=['datetime', 'station_code']).sum()` = **0**.
     - `df.duplicated(subset=['datetime', 'station_id']).sum()` = **0**.
- **Blast Radius**: If broken, downstream aggregates would double-count sensor observations for multi-word provinces (Gia Lai, Lâm Đồng, Tây Ninh, Đà Nẵng).
- **Result**: **PASS** — Defect completely eradicated.

---

### [Medium] Challenge 2: Robustness and Idempotency of `purge_output_directory`
- **Assumption Challenged**: Does `purge_output_directory(target_dir)` handle edge cases such as read-only files (`stat.S_IREAD`), nested directories, special characters, non-existent directories, and retries?
- **Attack Scenario**: In an isolated temporary directory, created mock partition folders simulating cross-platform collision (`province_name=Gia%20Lai` and `province_name=Gia Lai`, `province_name=Hà%20Nội` and `province_name=Hà Nội`) containing read-only mock `.parquet` files (`stat.S_IREAD`).
- **Empirical Test**:
  Executed `purge_output_directory(mock_target)`.
  - Directory completely wiped without permission errors via `_handle_readonly` chmod hook.
  - Subsequent call on non-existent directory returned immediately with zero errors (idempotent).
- **Result**: **PASS**.

---

### [High] Challenge 3: Cross-Platform Execution Consistency (Docker vs Windows Host)
- **Assumption Challenged**: Code running on Windows Host (Python 3.11, Spark 3.4.2) might exhibit numerical drift, timezone skew, or path resolution bugs when run inside standard Docker container (`airquality-spark:3.5.1`, Linux Ubuntu, Python 3.8, Spark 3.5.1).
- **Empirical Test**:
  1. Executed `pytest tests/test_step2_bronze_silver.py -v` on Windows Host: **7 passed in 2.39s**.
  2. Executed `pytest tests/test_step2_adversarial.py -v` on Windows Host: **8 passed in 4.63s**.
  3. Executed inside Docker container `airquality-spark:3.5.1`:
     `docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest /workspace/tests/test_step2_bronze_silver.py /workspace/tests/test_step2_adversarial.py -v`
     - **15 passed in 11.58s** (zero failures, zero warnings).
- **Result**: **PASS**.

---

### [Medium] Challenge 4: Pure Catalyst Conformance & Prohibited Constructs Check
- **Assumption Challenged**: Implementation might have quietly introduced driver-side bottlenecks, Python `@udf`, `applyInPandas`, or `inferSchema=True` to resolve edge cases.
- **Empirical AST Test**:
  Parsed AST trees of `code/lakehouse/spark_preprocess_tedp.py` and `code/lakehouse/spark_sync_province.py`.
  - Occurrences of `@udf`: **0**
  - Occurrences of `applyInPandas`: **0**
  - Occurrences of `inferSchema=True`: **0**
- **Result**: **PASS**.

---

## Stress Test Results Matrix

| Stress Test Scenario | Expected Behavior | Actual Behavior | Verdict |
|:---|:---|:---|:---:|
| 1. Subdirectory scan `lakehouse_storage/silver/tedp_station/` | Exactly 11 subdirectories | Exactly 11 subdirectories (`province_name=*`) | **PASS** |
| 2. Total row count assertion in `tedp_station/` | Exactly 440,012 rows | Exactly 440,012 rows (PyArrow & PySpark verified) | **PASS** |
| 3. Station duplicate check `(station_code, datetime)` | Exactly 0 duplicates | 0 duplicates found across all 440,012 rows | **PASS** |
| 4. Station QCVN physical threshold compliance | All pm25, aqi in [0, 500], pm10 in [0, 600] | 0 negative, 0 exceeding thresholds | **PASS** |
| 5. Core test suite `test_step2_bronze_silver.py` | 7 / 7 test cases pass | 7 passed in 2.39s | **PASS** |
| 6. Adversarial suite `test_step2_adversarial.py` | 8 / 8 test cases pass | 8 passed in 4.63s | **PASS** |
| 7. Containerized execution in `airquality-spark:3.5.1` | 15 / 15 test cases pass | 15 passed in 11.58s | **PASS** |
| 8. `purge_output_directory` read-only collision purge | Cleanly purges collision & read-only files | Successfully purged; idempotent on absent dir | **PASS** |
| 9. Pure Spark SQL AST static analysis | Zero prohibited constructs | Zero `@udf`, zero `applyInPandas`, zero `inferSchema` | **PASS** |

---

## Unchallenged Areas

- **Step 3 (Silver to Gold Feature Extraction)**: Intentionally out of scope for Milestone 2 remediation review.
- **Step 4 (Model Serving Integration)**: Intentionally deferred to Milestone 4.
