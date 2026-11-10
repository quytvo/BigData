# Handoff Report — Worker Step 2 Remediation

**Target Milestone**: Milestone 2 (Step 2: Bronze to Silver Preprocessing Pipeline Remediation)  
**Author**: Worker Step 2 Remediation  
**Date**: 2026-11-09T04:22:00Z  
**Recipient**: Orchestrator (ID: `3888b5ae-cede-4e9e-8868-75bef4ef4193`)  
**Status**: REMEDIATION COMPLETE (100% Tests Passing, Clean 11 Partitions, 440,012 Station Rows)

---

## 1. Observation

1. **Initial Audit Failure Reproduction**:
   - Running `pytest tests/test_step2_bronze_silver.py -v` prior to fix produced:
     ```
     FAILED tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_station_dataset_integrity
     AssertionError: Expected 11 partition subdirectories matching 'province_name=*', found 15:
     ['province_name=Bắc%20Ninh', 'province_name=Gia Lai', 'province_name=Gia%20Lai',
      'province_name=HCM', 'province_name=Hà%20Nội', 'province_name=Hưng%20Yên',
      'province_name=Hải%20Phòng', 'province_name=Lâm Đồng', 'province_name=Lâm%20Đồng',
      'province_name=Ninh%20Bình', 'province_name=Quảng%20Ngãi', 'province_name=Tây Ninh',
      'province_name=Tây%20Ninh', 'province_name=Đà Nẵng', 'province_name=Đà%20Nẵng']
     assert 15 == 11
     ```
   - Prior row count in `lakehouse_storage/silver/tedp_station/`:
     ```
     Current rows: 549,100
     ```

2. **Root Cause Mechanism**:
   - `DataFrameWriter.mode("overwrite").partitionBy("province_name").parquet(...)` does NOT wipe target root folders in Apache Spark; it only writes to and overwrites partition folders whose path strings match the active JVM partition specs.
   - On Windows host, PySpark with Hadoop winutils serialized paths with URL encoding (`%20`), e.g., `province_name=Gia%20Lai`.
   - In Linux Docker container (`airquality-spark:3.5.1`), PySpark serialized paths with literal spaces, e.g., `province_name=Gia Lai`.
   - Running across both environments created co-existing partition directories, resulting in 15 directories and 109,088 duplicate rows.

3. **Code Changes**:
   - `code/lakehouse/spark_preprocess_tedp.py`:
     - Added `purge_output_directory(target_dir: Path)` at line 277 with retry mechanism (3 attempts, 0.5s backoff) and `stat.S_IWRITE` permission reset handler.
     - Called `purge_output_directory(output_dir)` before calling `df_final.write.mode("overwrite").partitionBy("province_name").parquet(str(output_dir))` in `process_station_silver(...)`.
   - `code/lakehouse/spark_sync_province.py`:
     - Added `purge_output_directory(target_dir: Path)` at line 382.
     - Called `purge_output_directory(province_dir)` before partitioned write in `process_province_silver(...)`.
     - Called `purge_output_directory(weather_output_dir)` before partitioned write in `sync_weather_province(...)`.
   - `docs/review_checkpoint_step2.md`:
     - Added Section 6 "Ghi Nhận Khắc Phục Sự Cố Phân Vùng (Remediation Record)" documenting root cause, remediation steps, and cross-platform verification.
     - Updated Section 5 partition inventory with exact per-province row counts (total 440,012 rows).
     - Updated Section 7 Integrity Attestation affirming cross-platform idempotency.

4. **Post-Remediation Storage Verification**:
   - `lakehouse_storage/silver/tedp_station/`:
     - Partition count: exactly **11**
     - Partitions list: `['province_name=Bắc%20Ninh', 'province_name=Gia%20Lai', 'province_name=HCM', 'province_name=Hà%20Nội', 'province_name=Hưng%20Yên', 'province_name=Hải%20Phòng', 'province_name=Lâm%20Đồng', 'province_name=Ninh%20Bình', 'province_name=Quảng%20Ngãi', 'province_name=Tây%20Ninh', 'province_name=Đà%20Nẵng']`
     - Total rows: exactly **440,012**
     - Per-province row counts:
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
   - `lakehouse_storage/silver/tedp_province/`:
     - Partition count: exactly **11**
     - Total rows: exactly **280,632** (11 provinces x 25,512 hours)
   - `lakehouse_storage/silver/weather_province/`:
     - Partition count: exactly **11**
     - Total rows: exactly **280,632**

5. **Test Results**:
   - Windows Host (`pytest tests/test_step2_bronze_silver.py -v`):
     ```
     ============================== 7 passed in 2.48s ==============================
     ```
   - Linux Docker Container (`docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest /workspace/tests/test_step2_bronze_silver.py -v`):
     ```
     ============================== 7 passed in 4.50s ===============================
     ```
   - Adversarial Tests on Windows Host (`pytest tests/test_step2_adversarial.py -v`):
     ```
     ============================== 8 passed in 4.31s ==============================
     ```
   - Adversarial Tests in Docker (`docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest /workspace/tests/test_step2_adversarial.py -v`):
     ```
     ============================== 8 passed in 6.08s ===============================
     ```
   - AST Prohibited Call Analysis:
     ```
     code/lakehouse/spark_preprocess_tedp.py: Prohibited calls found: []
     code/lakehouse/spark_sync_province.py: Prohibited calls found: []
     ```

---

## 2. Logic Chain

1. **Step 1 (Root Cause Identification)**:
   Observation 1 and 2 showed that Spark's `.mode("overwrite").partitionBy(...)` does not purge the root target directory. When tests or scripts ran in Linux Docker, POSIX wrote literal space directories; when ran on Windows, Hadoop winutils wrote `%20` directories. Together they created 15 subdirectories and 109,088 duplicate rows.
2. **Step 2 (Deterministic Remediation)**:
   By introducing `purge_output_directory` directly before Spark write invocations in both pipeline scripts (`spark_preprocess_tedp.py` and `spark_sync_province.py`), any pre-existing directory structure (whether from Docker or Windows host) is completely deleted before Spark partitions are written.
3. **Step 3 (Re-execution & Clean Storage)**:
   Observation 4 confirmed that after purging and re-running, `lakehouse_storage/silver/tedp_station` has exactly 11 partitions and exactly 440,012 genuine rows, while `tedp_province` and `weather_province` have exactly 11 partitions and 280,632 rows.
4. **Step 4 (Cross-Platform Certification)**:
   Observation 5 demonstrated that all 7 tests in `test_step2_bronze_silver.py` and all 8 adversarial tests in `test_step2_adversarial.py` pass 100% on both the Windows host and within the standard `airquality-spark:3.5.1` Linux Docker container.
5. **Step 5 (Documentation Synchronization)**:
   Observation 3 documented the full remediation record in `docs/review_checkpoint_step2.md`.

---

## 3. Caveats

- **No Caveats**: The pipeline operates completely deterministically, achieves zero duplicate records, exhibits cross-platform idempotency across Windows host and Linux Docker, uses 100% genuine Catalyst Spark SQL expressions with zero prohibited APIs, and passes all tests without mocks.

---

## 4. Conclusion

Milestone 2 (Step 2) remediation is **COMPLETE and VERIFIED CLEAN**.
All 5 required tasks specified in the dispatch have been strictly satisfied:
1. `purge_output_directory(target_dir: Path)` is integrated into both `code/lakehouse/spark_preprocess_tedp.py` and `code/lakehouse/spark_sync_province.py`.
2. `lakehouse_storage/silver/tedp_station/` contains exactly 11 partition directories and 440,012 rows.
3. `spark_sync_province.py` output is verified (exactly 11 partitions, 280,632 rows for both tables).
4. `pytest tests/test_step2_bronze_silver.py -v` passes 100% (7/7 passed, 0 failed) on Windows host and inside Docker container `airquality-spark:3.5.1`.
5. `docs/review_checkpoint_step2.md` is updated with complete remediation documentation.

The workspace is now unblocked and ready for Milestone 3 (Step 3: Gold Layer & Feature Engineering).

---

## 5. Verification Method

To independently verify this work:

1. **Verify Partitions & Row Counts**:
   ```powershell
   python -c "import pandas as pd, pathlib; p = pathlib.Path('lakehouse_storage/silver/tedp_station'); dirs = [d.name for d in p.iterdir() if d.is_dir() and d.name.startswith('province_name=')]; print('Partitions:', len(dirs)); df = pd.read_parquet(p); print('Rows:', len(df)); assert len(dirs) == 11; assert len(df) == 440012"
   ```

2. **Run Test Suite on Windows Host**:
   ```powershell
   pytest tests/test_step2_bronze_silver.py -v
   pytest tests/test_step2_adversarial.py -v
   ```

3. **Run Test Suite inside Docker Container**:
   ```powershell
   docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest /workspace/tests/test_step2_bronze_silver.py -v
   docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest /workspace/tests/test_step2_adversarial.py -v
   ```

4. **Verify Zero Prohibited APIs**:
   ```powershell
   python -c "import ast; tree = ast.parse(open('code/lakehouse/spark_preprocess_tedp.py', encoding='utf-8').read()); calls = [n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute) and n.attr in ['applyInPandas', 'toPandas']]; assert len(calls) == 0; print('AST Check Clean')"
   ```

