# Forensic Re-Audit Handoff Report — Milestone 2 Iteration 2

**Agent**: Forensic Auditor  
**Task**: Milestone 2 Iteration 2 Forensic Re-Audit  
**Target Deliverables**: `code/lakehouse/spark_preprocess_tedp.py`, `code/lakehouse/spark_sync_province.py`, `lakehouse_storage/silver/`, `tests/test_step2_bronze_silver.py`  
**Verdict**: **CLEAN**

---

## 1. Observation

Direct observations and empirical evidence gathered during the re-audit:
- **Partition Cardinality in `lakehouse_storage/silver/tedp_station/`**:
  `Get-ChildItem -Path 'lakehouse_storage/silver/tedp_station' -Directory` yields exactly 11 directories (`province_name=Bắc%20Ninh`, `province_name=Gia%20Lai`, `province_name=HCM`, `province_name=Hà%20Nội`, `province_name=Hưng%20Yên`, `province_name=Hải%20Phòng`, `province_name=Lâm%20Đồng`, `province_name=Ninh%20Bình`, `province_name=Quảng%20Ngãi`, `province_name=Tây%20Ninh`, `province_name=Đà%20Nẵng`).
  The 4 unencoded colliding directories (`Gia Lai`, `Lâm Đồng`, `Tây Ninh`, `Đà Nẵng`) observed in Iteration 1 have been completely removed.
- **Station Dataset Row Count**:
  PyArrow dataset scan on `lakehouse_storage/silver/tedp_station` counts exactly **440,012 rows** (down from 549,100 duplicate rows in Iteration 1). Per-province counts: HCM (101,475), Hưng Yên (51,327), Đà Nẵng (51,076), Hà Nội (50,482), Gia Lai (32,638), Bắc Ninh (25,849), Hải Phòng (25,626), Quảng Ngãi (25,617), Lâm Đồng (25,590), Tây Ninh (25,483), Ninh Bình (24,849).
- **Province Datasets Row Count & Continuity**:
  `lakehouse_storage/silver/tedp_province`: exactly 280,632 rows (25,512 hours x 11 provinces).
  `lakehouse_storage/silver/weather_province`: exactly 280,632 rows (25,512 hours x 11 provinces).
- **Implementation of `purge_output_directory`**:
  Present in both `code/lakehouse/spark_preprocess_tedp.py` (lines 277–311) and `code/lakehouse/spark_sync_province.py` (lines 382–416). Uses `shutil.rmtree` with `_handle_readonly` (`os.chmod(path, stat.S_IWRITE)`), retries up to 3 times, verifies directory absence before returning, and is called directly before `df.write.parquet(...)`.
- **AST Analysis for Prohibited APIs**:
  Checked `spark_preprocess_tedp.py`, `spark_sync_province.py`, and `spark_session.py`. Found 0 occurrences of `applyInPandas`, 0 `@udf`, 0 `udf()`, 0 `pandas_udf`, 0 `toPandas`, and 0 `inferSchema=True`.
- **Parquet Metadata & Event Logs**:
  All 41 Parquet files in `lakehouse_storage/silver/` show `compression: SNAPPY` across 100% of column chunks, with `created_by: parquet-mr version 1.12.3`.
  Event logs `local-1794197880655` and `local-1794197936327` contain 0 Python worker invocations.
- **Numerical Accuracy vs Pandas Baseline**:
  Full inner-join on 280,632 rows vs `data_test/cleaned_tedp_province_4y.csv`:
  `pm25`: max diff $8.53 \times 10^{-14} < 10^{-5}$
  `pm10`: max diff $2.84 \times 10^{-14} < 10^{-5}$
  `aqi`: max diff $8.53 \times 10^{-14} < 10^{-5}$
- **Test Suite Results**:
  `pytest tests/test_step2_bronze_silver.py -v`:
  - Host: 7/7 PASSED in 2.37s
  - Docker container (`airquality-spark:3.5.1`): 7/7 PASSED in 5.13s
  `pytest tests/test_step2_adversarial.py -v`:
  - Host: 8/8 PASSED in 4.64s
  - Docker container: 8/8 PASSED in 6.09s

---

## 2. Logic Chain

1. In Iteration 1, the test `test_silver_station_dataset_integrity` failed due to partition subdirectory accumulation (15 directories instead of 11, caused by URL-encoded `%20` vs literal space collisions across Windows and Linux Docker executions), inflating row count to 549,100.
2. The root cause was that `DataFrameWriter.mode("overwrite")` only overwrites partitions matching the JVM's current path encoding and does not delete partitions with alternative path representations.
3. Remediation was implemented via `purge_output_directory()`, which clears the entire target directory tree before Spark writes new partitions.
4. Stress testing confirmed that `purge_output_directory()` handles non-existent paths, populated directories, and read-only files safely and idempotently.
5. Inspection of disk storage confirms that all 4 stale partitions have been eliminated, leaving exactly 11 partitions and exactly 440,012 rows in `tedp_station`.
6. AST analysis, Parquet metadata checks, event logs, and numerical reconciliation confirm that the pipeline remains 100% pure Catalyst without any prohibited shortcuts.
7. Both host and container test runs executed cleanly with 100% pass rates.
8. Therefore, the work product satisfies all acceptance criteria and warrants a CLEAN verdict.

---

## 3. Caveats

- `purge_output_directory()` uses local filesystem operations (`shutil.rmtree`). In a distributed HDFS/S3 production environment, equivalent filesystem deletion would use Hadoop `FileSystem.delete()` or S3 delete objects API. For the scope of this local Docker/host Lakehouse project, `shutil.rmtree` with permission handling is optimal.
- No other caveats.

---

## 4. Conclusion

Milestone 2 (Step 2: Bronze to Silver PySpark Preprocessing Pipeline) has fully resolved the partition collision defect and passed all forensic verification checks.
- Binary verdict: **CLEAN**
- All 7 independent automated tests and 8 adversarial tests pass 100% in both host and Docker container environments.
- The project is fully unblocked and ready to advance to Milestone 3 (Step 3: Trích Xuất 49 Đặc Trưng Silver sang Gold & Pipeline Điều Phối).

---

## 5. Verification Method

To independently verify these results:

1. **Verify Partition Cardinality & Row Counts**:
   ```powershell
   Get-ChildItem -Path 'lakehouse_storage/silver/tedp_station' -Directory | Measure-Object | Select-Object Count
   # Expected Count: 11
   
   python -c "import pyarrow.dataset as ds; df = ds.dataset('lakehouse_storage/silver/tedp_station', format='parquet', partitioning='hive').to_table().to_pandas(); print('Rows:', len(df)); assert len(df) == 440012"
   # Expected: Rows: 440012
   ```

2. **Execute Independent Test Suite on Host**:
   ```powershell
   pytest tests/test_step2_bronze_silver.py -v
   # Expected: 7 passed
   ```

3. **Execute Independent Test Suite in Docker**:
   ```powershell
   docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest tests/test_step2_bronze_silver.py -v
   # Expected: 7 passed
   ```

4. **Execute Adversarial Test Suite**:
   ```powershell
   pytest tests/test_step2_adversarial.py -v
   # Expected: 8 passed
   ```
