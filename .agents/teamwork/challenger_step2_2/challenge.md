# ADVERSARIAL CHALLENGE REPORT — MILESTONE 2 (STEP 2)

**Agent**: Challenger 2 (Step 2 Container Execution & Partition Pruning)  
**Date**: 2026-11-09  
**Evaluation Scope**: Docker Container Execution, Resource Constraints (8GB RAM, 4 CPUs), Partition Pruning on Silver Parquet (`tedp_province`), Cross-Platform Storage Integrity.

---

## Challenge Summary

**Overall risk assessment**: **HIGH**  
**Verdict**: **REQUEST_CHANGES**

While PySpark partition pruning on `lakehouse_storage/silver/tedp_province/` performs exceptionally well (Catalyst pushdown reads strictly 1 fragment and exactly 25,512 rows for Hà Nội and all 11 provinces), an empirical challenge revealed a **CRITICAL cross-platform directory pollution bug** in `lakehouse_storage/silver/tedp_station/`:
1. Stale partition subdirectories from mixed host/Docker execution (`province_name=Gia Lai` vs `province_name=Gia%20Lai`) caused `tests/test_step2_bronze_silver.py` to **FAIL** inside the Docker container (`1 failed, 6 passed in 4.28s`, 15 partitions found instead of 11).
2. PyArrow dataset scan on `tedp_station` silently read duplicate rows (**65,276 rows** instead of 32,638 for Gia Lai).
3. The root cause is that `spark_preprocess_tedp.py` and `spark_sync_province.py` rely exclusively on `.write.mode("overwrite")`, which does NOT remove preexisting partition directories with differing URL-encoding across OS environments.

---

## Challenges

### [High] Challenge 1: Cross-Platform Partition Directory Pollution & Row Duplication in `tedp_station`

- **Assumption challenged**: Spark's `.write.mode("overwrite").partitionBy("province_name")` cleans and guarantees an authoritative, non-redundant Hive partitioned directory structure across development and containerized test environments.
- **Attack scenario**: 
  - When the pipeline or tests are run on a Windows host and then inside a Linux container (or vice-versa), Hadoop's path serialization differs: Linux Docker encodes spaces as `%20` (`province_name=Gia%20Lai`), while Windows host environments may create unescaped directory names (`province_name=Gia Lai`).
  - Because `.mode("overwrite")` only replaces files within partitions matching the exact path name, both directories coexist side-by-side.
- **Blast radius**:
  - `tests/test_step2_bronze_silver.py::test_silver_station_dataset_integrity` FAILS:
    ```
    AssertionError: Expected 11 partition subdirectories matching 'province_name=*', found 15:
    ['province_name=Bắc%20Ninh', 'province_name=Gia Lai', 'province_name=Gia%20Lai', 'province_name=HCM', 'province_name=Hà%20Nội', 'province_name=Hưng%20Yên', 'province_name=Hải%20Phòng', 'province_name=Lâm Đồng', 'province_name=Lâm%20Đồng', 'province_name=Ninh%20Bình', 'province_name=Quảng%20Ngãi', 'province_name=Tây Ninh', 'province_name=Tây%20Ninh', 'province_name=Đà Nẵng', 'province_name=Đà%20Nẵng']
    ```
  - Downstream consumers (PyArrow / Spark / pandas) scanning by partition key union both directories, resulting in silent row duplication:
    `ds.dataset('lakehouse_storage/silver/tedp_station').to_table(filter=ds.field('province_name') == 'Gia Lai').num_rows == 65276` (true row count is 32,638).
- **Mitigation**:
  1. In `code/lakehouse/spark_preprocess_tedp.py` and `code/lakehouse/spark_sync_province.py`, explicitly clean/purge the target directory (e.g., `shutil.rmtree(output_dir, ignore_errors=True)` or delete using Hadoop FileSystem API) before performing `.write.mode("overwrite")`.
  2. Clean up `lakehouse_storage/silver/tedp_station/` by removing the unencoded directories (`province_name=Gia Lai`, `province_name=Lâm Đồng`, `province_name=Tây Ninh`, `province_name=Đà Nẵng`).

---

### [Low] Challenge 2: Partition Pruning on `tedp_province` Under Adversarial Query Predicates

- **Assumption challenged**: Partition pruning reliably skips all unneeded files when querying single provinces or non-existent provinces without memory leaks or full table scans.
- **Attack scenario**:
  - Test query with single province: `province_name == 'Hà Nội'`.
  - Test query with multi-province: `province_name.isin(['Hà Nội', 'HCM'])`.
  - Test query with non-existent province: `province_name == 'Cần Thơ'`.
  - Test query with empty string: `province_name == ''`.
- **Stress-Test Results**:
  - Single province (`Hà Nội`): Scanned strictly 1 fragment (`province_name=Hà%20Nội/part-00003-*.snappy.parquet`), returned exactly 25,512 rows. PySpark physical plan confirmed `PartitionFilters: [isnotnull(province_name#29), (province_name#29 = Hà Nội)]` pushed down to `InMemoryFileIndex`. **PASS**.
  - All 11 target provinces individually: Each scanned strictly 1 fragment and returned exactly 25,512 rows ($11 \times 25,512 = 280,632$ rows total). **PASS**.
  - Multi-province (`Hà Nội` + `HCM`): Scanned strictly 2 fragments, returned exactly 51,024 rows. **PASS**.
  - Non-existent province (`Cần Thơ`): Scanned 0 fragments, returned 0 rows cleanly without errors. **PASS**.
  - Empty string (`""`): Scanned 0 fragments, returned 0 rows cleanly. **PASS**.
- **Blast radius**: None; partition pruning in `tedp_province` is robust.

---

### [Medium] Challenge 3: Docker Container Execution Limits (8GB RAM, 4 CPUs)

- **Assumption challenged**: PySpark station preprocessing and province synchronization run reliably within the constrained Docker container without hitting Out Of Memory (OOM) or container termination.
- **Attack scenario**:
  - Run container with `--memory 8g --cpus 4`:
    - `python3 code/lakehouse/spark_preprocess_tedp.py`: Completed with exit code 0 in 47 seconds. Peak memory within safe bounds.
    - `python3 code/lakehouse/spark_sync_province.py`: Completed with exit code 0 in 32 seconds. Full 25,512-hour grid expansion and bidirectional interpolation executed smoothly.
- **Stress-Test Results**:
  - Execution completed cleanly with zero OOM errors. Driver allocation of 5GB leave 3GB for PyArrow, OS, and JVM off-heap overhead. **PASS**.

---

## Stress Test Results

| Test Scenario | Expected Behavior | Actual Behavior | Result |
|:---|:---|:---|:---:|
| Docker Test Suite (`pytest tests/test_step2_bronze_silver.py`) | 7/7 tests pass cleanly in `airquality-spark:3.5.1` | `test_silver_station_dataset_integrity` FAILED (15 partitions found vs 11 expected) | **FAIL** |
| Partition Pruning Hà Nội (`tedp_province`) | 1 fragment scanned, exactly 25,512 rows | 1 fragment scanned (`part-00003-*.parquet`), exactly 25,512 rows | **PASS** |
| Partition Pruning All 11 Provinces (`tedp_province`) | 11 partitions, each exactly 25,512 rows | All 11 provinces scanned 1 fragment each, exactly 25,512 rows | **PASS** |
| Weather Partition Pruning All 11 Provinces (`weather_province`) | All 11 provinces return exactly 25,512 rows | All 11 provinces returned exactly 25,512 rows | **PASS** |
| Adversarial Predicate Pushdown (`Cần Thơ`, empty string) | 0 fragments, 0 rows returned | 0 fragments scanned, 0 rows returned | **PASS** |
| Station Dataset Row Count (`tedp_station`, Gia Lai) | 32,638 authentic station rows | 65,276 rows returned due to dual partition dirs (`Gia Lai` and `Gia%20Lai`) | **FAIL** |
| Container ETL Execution (`spark_preprocess_tedp.py`) | Completes in Docker (8GB RAM, 4 CPUs) without OOM | Completed in 47s, exit code 0 | **PASS** |
| Container ETL Execution (`spark_sync_province.py`) | Completes in Docker (8GB RAM, 4 CPUs) without OOM | Completed in 32s, exit code 0 | **PASS** |

---

## Unchallenged Areas

- **Step 3 (Silver to Gold Feature Engineering)**: Out of scope for Step 2.
- **Step 4 (Model Training & Inference Integration)**: Out of scope for Step 2.
- **Cluster Distributed Execution (Multi-worker Spark)**: Out of scope; project specification dictates local container execution (`local[4]`).
