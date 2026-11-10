# HARD HANDOFF REPORT — FORENSIC AUDIT OF STEP 2

- **Agent**: Forensic Auditor (`auditor_step2_1`)
- **Recipient**: Parent Orchestrator (`3888b5ae-cede-4e9e-8868-75bef4ef4193`)
- **Status**: Hard Handoff — Audit Complete (Verdict: **INTEGRITY VIOLATION**)
- **Date**: 2026-11-09

---

## 1. OBSERVATION

1. **Test Suite Failure**:
   - Command: `pytest tests/test_step2_bronze_silver.py`
   - Command: `docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest tests/test_step2_bronze_silver.py`
   - Result in both environments:
     ```
     FAILED tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_station_dataset_integrity
     AssertionError: Expected 11 partition subdirectories matching 'province_name=*', found 15:
     ['province_name=Bắc%20Ninh', 'province_name=Gia Lai', 'province_name=Gia%20Lai', 'province_name=HCM', 'province_name=Hà%20Nội', 'province_name=Hưng%20Yên', 'province_name=Hải%20Phòng', 'province_name=Lâm Đồng', 'province_name=Lâm%20Đồng', 'province_name=Ninh%20Bình', 'province_name=Quảng%20Ngãi', 'province_name=Tây Ninh', 'province_name=Tây%20Ninh', 'province_name=Đà Nẵng', 'province_name=Đà%20Nẵng']
     ```

2. **Storage Layout Anomaly**:
   - Directory `lakehouse_storage/silver/tedp_station/` contains 15 partition directories instead of 11.
   - For provinces with spaces, both URL-encoded (`%20`) and literal space (` `) directories exist concurrently:
     - `province_name=Gia Lai` (created 10:55 AM) & `province_name=Gia%20Lai` (created 10:53 AM)
     - `province_name=Lâm Đồng` (created 10:55 AM) & `province_name=Lâm%20Đồng` (created 10:54 AM)
     - `province_name=Tây Ninh` (created 10:55 AM) & `province_name=Tây%20Ninh` (created 10:54 AM)
     - `province_name=Đà Nẵng` (created 10:55 AM) & `province_name=Đà%20Nẵng` (created 10:54 AM)

3. **Data Doubling**:
   - Querying `lakehouse_storage/silver/tedp_station/` returns **549,100 records** instead of ~400,000 records.
   - Rows for `Đà Nẵng`: 76,453 (doubled), `Gia Lai`: 65,276 (doubled), `Lâm Đồng`: 51,180 (doubled), `Tây Ninh`: 50,966 (doubled).

4. **Source Code Static Analysis**:
   - `code/lakehouse/spark_preprocess_tedp.py:290-295`:
     ```python
     (
         df_final.write
         .mode("overwrite")
         .partitionBy("province_name")
         .parquet(str(output_dir))
     )
     ```
     No deletion or purging of `output_dir` before writing.
   - AST inspection confirms ZERO `applyInPandas`, ZERO `@udf`, ZERO `inferSchema=True`, and ZERO `toPandas`.

5. **Numerical Accuracy on Province Level**:
   - In `lakehouse_storage/silver/tedp_province/` (which has exactly 11 partitions, all `%20`), maximum absolute error against `data_test/cleaned_tedp_province_4y.csv` is $8.5265 \times 10^{-14}$ across all 280,632 records. Categorical matching is 100.00%.

---

## 2. LOGIC CHAIN

1. Under Apache Spark's `InsertIntoHadoopFsRelationCommand`, calling `.mode("overwrite").partitionBy("province_name")` overwrites files within matching partition paths. If existing partition directories on disk have different names (such as `%20` encoding from Linux Spark 3.5.1 vs literal space from Windows Spark 3.4.2), Spark does NOT delete the unmatched existing directories.
2. When the pipeline was run in Linux Docker, it produced `province_name=Gia%20Lai`, etc.
3. When `spark_preprocess_tedp.py` was executed on the Windows host, it created `province_name=Gia Lai`, etc., leaving the Linux `%20` directories on disk.
4. Parquet readers scan all subdirectories matching the glob/partition layout. Consequently, `pd.read_parquet(SILVER_STATION_DIR)` reads both copies, inflating the dataset to 549,100 rows and duplicating data for 4 provinces.
5. In addition, the programmatic test suite assertion `assert len(partition_dirs) == 11` detects 15 directories and fails.
6. The test failure invalidates the claim of complete milestone success. Per Integrity Forensics rules, failure of any automated test case mandates a verdict of **INTEGRITY VIOLATION**.

---

## 3. CAVEATS

- The transformation logic itself is technically sound and mathematically exact. There is NO cheating, NO hardcoding, and NO mock facades.
- `tedp_province` avoided the partition doubling issue only because it was not re-executed on the Windows host with literal spaces. However, `spark_sync_province.py` shares the same latent vulnerability as `spark_preprocess_tedp.py`.

---

## 4. CONCLUSION

- **Binary Verdict**: **INTEGRITY VIOLATION**
- Work product for Milestone 2 (Step 2) is **REJECTED**.
- **Action Required by Worker**:
  1. Add pre-write directory purging (`shutil.rmtree` or Hadoop `FileSystem.delete`) to `code/lakehouse/spark_preprocess_tedp.py` and `code/lakehouse/spark_sync_province.py`.
  2. Clean `lakehouse_storage/silver/tedp_station/` to remove duplicate `%20` and space partition directories.
  3. Re-run `spark_preprocess_tedp.py` and verify `pytest tests/test_step2_bronze_silver.py` passes 7/7 on both host and inside Docker.

---

## 5. VERIFICATION METHOD

To reproduce and verify this finding:

1. **Verify Test Failure on Host**:
   ```powershell
   pytest tests/test_step2_bronze_silver.py
   ```
   *Observed*: 1 failed, 6 passed (`test_silver_station_dataset_integrity` fails with 15 partitions found vs 11 expected).

2. **Verify Test Failure in Docker**:
   ```powershell
   docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest tests/test_step2_bronze_silver.py
   ```
   *Observed*: Same failure.

3. **Verify Duplicate Rows on Disk**:
   ```powershell
   python -c "import pandas as pd; df = pd.read_parquet('lakehouse_storage/silver/tedp_station'); print(len(df)); print(df['province_name'].value_counts())"
   ```
   *Observed*: 549,100 rows with duplicated counts for Đà Nẵng (76,453), Gia Lai (65,276), Lâm Đồng (51,180), Tây Ninh (50,966).
