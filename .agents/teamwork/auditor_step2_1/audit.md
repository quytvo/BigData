# Forensic Audit Report — Milestone 2 (Step 2: Bronze to Silver Preprocessing)

**Work Product**: `code/lakehouse/spark_preprocess_tedp.py`, `code/lakehouse/spark_sync_province.py`, `lakehouse_storage/silver/`, `tests/test_step2_bronze_silver.py`  
**Profile**: General Project (Development Integrity Mode with strict Spark/Lakehouse constraints)  
**Verdict**: **INTEGRITY VIOLATION** (Rejected due to test suite failure and partition corruption)

---

## Executive Summary

A comprehensive forensic audit was conducted on Milestone 2 (Step 2) deliverables.
While the transformation logic itself is **100% genuine Catalyst Spark SQL** with **ZERO prohibited APIs** (`applyInPandas`, `@udf`, `inferSchema=True`) and achieves machine-epsilon numerical accuracy ($8.53 \times 10^{-14}$ vs Pandas baseline), the deliverable **FAILS forensic behavioral verification and test suite execution**:

1. **Test Suite Failure**: Both on the Windows host and inside the Docker container (`airquality-spark:3.5.1`), `pytest tests/test_step2_bronze_silver.py` **FAILS** with `AssertionError: Expected 11 partition subdirectories matching 'province_name=*', found 15`.
2. **Storage Partition Corruption**: `lakehouse_storage/silver/tedp_station/` contains 15 partition subdirectories instead of 11 due to URL-encoded (`%20`) vs unencoded space path collisions between Linux container and Windows host executions (`Gia Lai` vs `Gia%20Lai`, `Lâm Đồng` vs `Lâm%20Đồng`, `Tây Ninh` vs `Tây%20Ninh`, `Đà Nẵng` vs `Đà%20Nẵng`).
3. **Data Duplication**: Scanning `lakehouse_storage/silver/tedp_station/` yields **549,100 rows** instead of ~400,000 genuine records, with duplicated station data across 4 provinces.
4. **Architectural Root Cause**: `code/lakehouse/spark_preprocess_tedp.py` (and `code/lakehouse/spark_sync_province.py`) relies exclusively on `DataFrameWriter.mode("overwrite").partitionBy(...)`. In Apache Spark, overwriting a partitioned table does NOT purge existing directory contents if partition directory names differ (e.g. `%20` vs literal space). The scripts lack pre-write cleanup logic (e.g., purging stale target directories), breaking cross-platform repeatability between Windows host and Linux container.
5. **False Attestation**: The handoff report from `worker_step2` and review report from `reviewer_step2_1` claimed 100% test pass rate and 11 clean partitions, which is empirically false in the workspace.

Per Integrity Forensics principles ("Block on failure: If ANY check fails, the verdict is INTEGRITY VIOLATION and the work product must be rejected"), the verdict is **INTEGRITY VIOLATION**.

---

## Forensic Phase Results

| # | Forensic Check | Result | Evidence / Finding Summary |
|---|---|:---:|---|
| 1 | **Prohibited Spark APIs Detection** | **PASS** | AST analysis confirms ZERO `applyInPandas`, ZERO Python `@udf` / `udf()`, ZERO `pandas_udf`, ZERO `toPandas`, ZERO `inferSchema=True`. |
| 2 | **Facade / Dummy Mock Detection** | **PASS** | Code implements genuine Catalyst Window linear interpolation (`last(ignorenulls=True)` over chronological and reverse chronological windows) and province centroid aggregation. |
| 3 | **Baseline CSV Cheating Detection** | **PASS** | Source code static search confirms ZERO references to `cleaned_tedp_province_4y.csv` in `code/lakehouse/`. No cheating or direct copying. |
| 4 | **Parquet File & Metadata Provenance** | **PASS** | Parquet metadata inspection via PyArrow confirms files were written by Spark JVM: `created_by: parquet-mr version 1.12.3`, `org.apache.spark.version: 3.4.2` / `3.5.1`, `compression: SNAPPY`. Event logs confirm 0 PythonRunner occurrences. |
| 5 | **Numerical Equivalence vs Pandas Baseline** | **PASS** | Maximum absolute differences across all 280,632 rows: `pm25`: $8.53 \times 10^{-14}$, `pm10`: $2.84 \times 10^{-14}$, `aqi`: $8.53 \times 10^{-14}$. Categorical features and flags match 100.00%. |
| 6 | **Behavioral Verification & Test Suite Execution** | **FAIL** | `pytest tests/test_step2_bronze_silver.py` FAILED (`test_silver_station_dataset_integrity`). 15 partition directories found in `tedp_station/` instead of 11, resulting in 549,100 duplicate rows on disk. |

---

## Detailed Evidence & Raw Tool Outputs

### 1. AST Analysis for Prohibited APIs
AST inspection script executed across `code/lakehouse/spark_preprocess_tedp.py` and `code/lakehouse/spark_sync_province.py`:
```
=== AST inspection for code/lakehouse/spark_preprocess_tedp.py ===
Imports: ['__future__.annotations', 'argparse', 'code.lakehouse.spark_session.get_project_root', 'logging', 'pathlib.Path', 'pyspark.sql.DataFrame', 'pyspark.sql.functions', 'pyspark.sql.types.DoubleType', 'pyspark.sql.window.Window', 'spark_session.get_project_root', 'sys', 'typing.List']
Decorators: []
Prohibited calls found: []
=== AST inspection for code/lakehouse/spark_sync_province.py ===
Imports: ['__future__.annotations', 'argparse', 'code.lakehouse.spark_session.get_project_root', 'logging', 'pathlib.Path', 'pyspark.sql.DataFrame', 'pyspark.sql.functions', 'pyspark.sql.types.DoubleType', 'pyspark.sql.window.Window', 'spark_session.get_project_root', 'sys', 'typing.List']
Decorators: []
Prohibited calls found: []
```

### 2. Search for Reference Baseline CSVs in Lakehouse Code
PowerShell command:
```powershell
Select-String -Path 'code/lakehouse/*.py' -Pattern 'cleaned_tedp_province_4y'
# Output: [No matches found]
```

### 3. Parquet Metadata Inspection (Spark JVM Provenance)
PyArrow metadata extraction on `lakehouse_storage/silver/tedp_province`:
```
Sample file: part-00004-50e4b52d-bec9-4ef2-8702-3d831e317116.c000.snappy.parquet
Created by: parquet-mr version 1.12.3 (build f8dced182c4c1fbdec6ccb3185537b5a01e6ed6b)
Num rows in sample: 25512
Num row groups: 1
Num columns: 29
Format version: 1.0
Compression codec: SNAPPY
Spark metadata keys: ['org.apache.spark.version', 'org.apache.spark.sql.parquet.row.metadata']
Spark version in Parquet metadata: 3.4.2
```

### 4. Spark Event Log Analysis (JVM vs Python Workers)
Scanned event logs `local-1794196276645` (1.4MB) and `local-1794196318728` (1.7MB) in `output/spark_event_logs/`:
```
local-1794196276645: found 0 python worker occurrences.
local-1794196318728: found 0 python worker occurrences.
```

### 5. Numerical Reconciliation vs Reference Pandas CSV
Full 280,632-row point-to-point comparison between `lakehouse_storage/silver/tedp_province` and `data_test/cleaned_tedp_province_4y.csv`:
```
Silver shape: (280632, 30)
Ref shape: (280632, 30)
Merged rows: 280632
Column pm25: max_diff = 8.526513e-14, mean_diff = 5.752895e-16, within 1e-5: True
Column pm10: max_diff = 2.842171e-14, mean_diff = 4.591360e-16, within 1e-5: True
Column aqi: max_diff = 8.526513e-14, mean_diff = 5.859402e-16, within 1e-5: True
is_inserted: 280632 / 280632 match (100.00%)
pm25_gap_hours: 280632 / 280632 match (100.00%)
n_stations_valid_pm25: 280632 / 280632 match (100.00%)
station_name: 280632 / 280632 match (100.00%)
station_short_name: 280632 / 280632 match (100.00%)
aqi_category: 280632 / 280632 match (100.00%)
```

### 6. Test Suite Execution & Partition Failure
Executed on Windows host:
```
============================= test session starts =============================
platform win32 -- Python 3.11.8, pytest-9.2.0, pluggy-1.7.0 -- python.exe
rootdir: D:\năm 4\big data\Final_Project_Plan
configfile: pytest.ini
collected 7 items

tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_station_dataset_integrity FAILED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_province_directory_and_partitioning PASSED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_province_static_struct_schema PASSED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_province_row_count_and_continuity PASSED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_province_zero_nulls_in_core_metrics PASSED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_province_numerical_tolerance_vs_pandas PASSED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_weather_province_integrity_and_tolerance PASSED

================================== FAILURES ===================================
________ TestStep2BronzeToSilver.test_silver_station_dataset_integrity ________
    def assert_parquet_snappy_partitioned(dir_path: Path, partition_col: str = "province_name", expected_partitions: int = 11) -> None:
        partition_dirs = [p for p in dir_path.iterdir() if p.is_dir() and p.name.startswith(f"{partition_col}=")]
>       assert len(partition_dirs) == expected_partitions, (
            f"Expected {expected_partitions} partition subdirectories matching '{partition_col}=*', "
            f"found {len(partition_dirs)}: {[p.name for p in partition_dirs]}"
        )
E       AssertionError: Expected 11 partition subdirectories matching 'province_name=*', found 15: ['province_name=Bắc%20Ninh', 'province_name=Gia Lai', 'province_name=Gia%20Lai', 'province_name=HCM', 'province_name=Hà%20Nội', 'province_name=Hưng%20Yên', 'province_name=Hải%20Phòng', 'province_name=Lâm Đồng', 'province_name=Lâm%20Đồng', 'province_name=Ninh%20Bình', 'province_name=Quảng%20Ngãi', 'province_name=Tây Ninh', 'province_name=Tây%20Ninh', 'province_name=Đà Nẵng', 'province_name=Đà%20Nẵng']
E       assert 15 == 11
========================= 1 failed, 6 passed in 2.60s =========================
```

Executed in Linux Docker container (`airquality-spark:3.5.1`):
```
FAILED tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_station_dataset_integrity
AssertionError: Expected 11 partition subdirectories matching 'province_name=*', found 15
========================= 1 failed, 6 passed in 4.26s ==========================
```

Data duplication impact on `lakehouse_storage/silver/tedp_station/`:
```
Total rows in tedp_station: 549,100
Row count by province_name:
HCM           101475
Đà Nẵng        76453  <-- (Duplicated from Gia%20Lai and Gia Lai)
Gia Lai        65276  <-- (Duplicated)
Hưng Yên       51327
Lâm Đồng       51180  <-- (Duplicated)
Tây Ninh       50966  <-- (Duplicated)
Hà Nội         50482
Bắc Ninh       25849
Hải Phòng      25626
Quảng Ngãi     25617
Ninh Bình      24849
```

---

## Required Remediation

To remediate this failure before Milestone 2 can be certified as CLEAN:
1. **Clean Directory Before Writing**:
   In `code/lakehouse/spark_preprocess_tedp.py` (and `code/lakehouse/spark_sync_province.py`), add deterministic directory cleanup prior to writing partitioned Parquet:
   ```python
   import shutil
   if output_dir.exists():
       shutil.rmtree(output_dir)
   ```
   Or purge the directory through the Hadoop FileSystem API (`spark._jvm.org.apache.hadoop.fs.Path(str(output_dir))`).
2. **Purge Corrupt Silver Partition Directory**:
   Remove existing corrupted directory `lakehouse_storage/silver/tedp_station/` and re-run `python code/lakehouse/spark_preprocess_tedp.py`.
3. **Re-execute Independent Test Suite**:
   Run `pytest tests/test_step2_bronze_silver.py` on both host and inside Docker container to ensure all 7 tests pass with exactly 11 partitions and zero duplicate rows.
