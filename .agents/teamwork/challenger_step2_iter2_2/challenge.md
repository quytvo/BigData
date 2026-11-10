# Adversarial Challenge Report — Milestone 2 Iteration 2 (Docker Challenge)

**Evaluator**: Challenger 2 (Empirical Challenger)  
**Execution Environment**: Linux Docker Container `airquality-spark:3.5.1` (8GB RAM, 4 CPUs)  
**Target Milestone**: Milestone 2 (Step 2: Bronze to Silver Preprocessing Pipeline)  
**Date**: 2026-11-09T04:28:30Z  

---

## 1. Challenge Summary

**Overall Risk Assessment**: **LOW**

The Step 2 Bronze to Silver preprocessing pipeline has been empirically tested and challenged within the official Docker container `airquality-spark:3.5.1`. All 7 core tests in `tests/test_step2_bronze_silver.py`, all 8 deep parity adversarial tests in `tests/test_step2_adversarial.py`, and all 8 newly authored empirical partition pruning tests in `tests/test_step2_partition_pruning.py` passed with 0 failures (total 23/23 tests passing).

Partition pruning on both `lakehouse_storage/silver/tedp_province/` and `lakehouse_storage/silver/tedp_station/` has been empirically confirmed at both the physical PyArrow file fragment scanner level and the PySpark Catalyst query execution engine level (`PartitionFilters` pushdown).

---

## 2. Challenges & Empirical Stress Tests

### Challenge 1 (Resolved): Cross-Platform Partition Directory Collision & Purge Mechanism
- **Assumption Challenged**: Spark's `.mode("overwrite").partitionBy("province_name")` was assumed to provide a clean target directory across Windows host and Linux Docker container executions.
- **Attack Scenario**: Windows WinUtils writes URL-encoded partition directories (`province_name=H%C3%A0%20N%E1%BB%99i` or `province_name=Hà%20Nội`), while POSIX Linux environments write unencoded spaces (`province_name=Gia Lai`). In Milestone 2 Iteration 1, this created 15 partition directories and 109,088 duplicate rows in `lakehouse_storage/silver/tedp_station/`.
- **Mitigation Implemented**: Worker added `purge_output_directory(target_dir)` in both `code/lakehouse/spark_preprocess_tedp.py` and `code/lakehouse/spark_sync_province.py` before partitioned write operations.
- **Empirical Verification**:
  - `lakehouse_storage/silver/tedp_station/` contains exactly 11 partition subdirectories.
  - `lakehouse_storage/silver/tedp_province/` contains exactly 11 partition subdirectories.
  - Total station rows = 440,012 (zero duplicate records).
  - Total province rows = 280,632 (25,512 hours x 11 provinces).

### Challenge 2: Partition Pruning on `tedp_province/` (File-Scan Isolation & Physical Pushdown)
- **Assumption Challenged**: Downstream queries filtering on `province_name` actually prune non-target partitions without scanning entire datasets.
- **Attack Scenario**: Hive-style partitioned Parquet with non-ASCII province names might fail to push predicates down or might scan all 11 partitions when decoded/encoded differently.
- **Empirical Verification (`tests/test_step2_partition_pruning.py`)**:
  - **PyArrow Dataset Scan**: Evaluated `dataset.get_fragments(filter=ds.field("province_name") == "Hà Nội")`. Exactly 1 fragment was scanned (`province_name=Hà%20Nội/part-*.parquet`), loading exactly 25,512 rows. Zero other partition files were touched.
  - **PySpark Physical Plan**: Executed `df.filter(F.col("province_name") == "Hà Nội").explain()`. Physical plan confirmed `PartitionFilters: [isnotnull(province_name#...), (province_name#... = Hà Nội)]` directly inside the `FileScan parquet` node.
  - **Multi-Partition Query**: Filtering `province_name.isin(['Hà Nội', 'HCM'])` touched exactly 2 partition directories and yielded 51,024 rows.
  - **Non-Existent Partition**: Filtering for `NonExistentProvince` pruned 100% of files (0 fragments scanned, 0 rows returned).

### Challenge 3: Partition Pruning on `tedp_station/` (Multi-File Partition Pushdown)
- **Assumption Challenged**: Station-level Silver dataset, which has non-uniform file counts and varying row counts per province, correctly prunes partitions and isolates station records per province.
- **Empirical Verification (`tests/test_step2_partition_pruning.py`)**:
  - Tested all 11 provinces individually against known ground truth station counts:
    - HCM: 101,475 rows
    - Hưng Yên: 51,327 rows
    - Đà Nẵng: 51,076 rows
    - Hà Nội: 50,482 rows
    - Gia Lai: 32,638 rows
    - Bắc Ninh: 25,849 rows
    - Hải Phòng: 25,626 rows
    - Quảng Ngãi: 25,617 rows
    - Lâm Đồng: 25,590 rows
    - Tây Ninh: 25,483 rows
    - Ninh Bình: 24,849 rows
  - Sum of pruned loads: exactly 440,012 rows.
  - **PySpark Physical Plan**: Filtering `province_name == 'Đà Nẵng'` confirmed Catalyst `PartitionFilters: [isnotnull(province_name#...), (province_name#... = Đà Nẵng)]`, yielding exactly 51,076 rows.

### Challenge 4: Compression & Code Quality Standards
- **Snappy Compression**: All `.parquet` files across all partition subdirectories in both `tedp_station` and `tedp_province` were scanned and verified to use `SNAPPY` compression codec.
- **Zero Prohibited APIs**: AST parse confirmed 0 occurrences of `toPandas`, `applyInPandas`, `pandas_udf`, or `inferSchema=True` across `spark_preprocess_tedp.py` and `spark_sync_province.py`.

---

## 3. Stress Test Results Summary

| Test Scenario | Target | Expected Result | Actual Result in Docker | Verdict |
|:---|:---|:---|:---|:---:|
| Core Step 2 Suite (`test_step2_bronze_silver.py`) | All Silver deliverables | 7/7 passed | 7 passed in 5.26s | **PASS** |
| Deep Parity Suite (`test_step2_adversarial.py`) | Numerical error < 1e-5 | 8/8 passed | 8 passed in 5.92s | **PASS** |
| Partition Inventory | `tedp_province`, `tedp_station` | 11 subdirs each | Exactly 11 subdirs each | **PASS** |
| PyArrow Pruning `tedp_province` | All 11 provinces | 1 frag/prov, 25,512 rows | 100% isolated, 25,512 rows | **PASS** |
| PyArrow Pruning `tedp_station` | All 11 provinces | Exact station rows per prov | 440,012 total rows isolated | **PASS** |
| Catalyst `PartitionFilters` Province | PySpark physical plan | Pushdown `Hà Nội` | Pushed down in FileScan | **PASS** |
| Catalyst `PartitionFilters` Station | PySpark physical plan | Pushdown `Đà Nẵng` | Pushed down in FileScan | **PASS** |
| Snappy Codec Validation | All partition parquet files | SNAPPY on all columns | 100% Snappy verified | **PASS** |
| Combined Docker Test Run | All 3 test modules | 23/23 passed | 23 passed in 21.08s | **PASS** |

---

## 4. Unchallenged Areas

- **Step 3 (Gold Layer Feature Extraction)**: Not yet generated; scheduled for Milestone 3.
- **Step 4 (Model Serving & Common Integration)**: Scheduled for Milestone 4.
- **Step 5 (Docker Benchmark 1x, 10x, 50x)**: Scheduled for Milestone 5.

---

## 5. Explicit Verdict

**VERDICT**: **APPROVE**  
Milestone 2 (Step 2: Bronze to Silver Preprocessing Pipeline) is robust, idempotent, and completely compliant with all Lakehouse contracts in the Linux Docker container environment.
