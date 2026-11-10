# Milestone 2 Iteration 2 (Step 2 Remediation) Review Report

**Reviewer**: Reviewer 1 (Adversarial Critic & Quality Reviewer)  
**Target**: Milestone 2 Remediation (Partition Collision Fix & Verification)  
**Date**: 2026-11-09T04:28:00Z  
**Verdict**: **APPROVE**  

---

## 1. Review Summary

The remediation performed by `worker_step2_remediation` completely and cleanly resolves the partition collision defect previously flagged in Milestone 2. 

- **Defect Root Cause**: Apache Spark's `DataFrameWriter.mode("overwrite").partitionBy(...)` does not purge the root target directory; it only overwrites subdirectories matching active partition paths. Because Windows host PySpark (Hadoop winutils) serializes partition paths with URI encoding (`%20`) while Linux Docker container PySpark serializes paths with literal spaces, executing in both environments produced duplicate partition directories (15 folders instead of 11) and inflated station rows from 440,012 to 549,100.
- **Remediation Implemented**: `purge_output_directory(target_dir: Path)` has been implemented and placed immediately before all partitioned Parquet writes in `code/lakehouse/spark_preprocess_tedp.py` and `code/lakehouse/spark_sync_province.py`.
- **Storage State**: `lakehouse_storage/silver/tedp_station/` now contains **exactly 11 partition subdirectories** and **exactly 440,012 rows**. `tedp_province` and `weather_province` each contain **exactly 11 partitions** and **280,632 rows**.
- **Automated Verification**: All 7 tests in `tests/test_step2_bronze_silver.py` passed on Windows host (2.41s) and within Linux Docker container `airquality-spark:3.5.1` (4.77s). In addition, all 8 parity tests in `tests/test_step2_adversarial.py` passed (4.34s).
- **Integrity Compliance**: Zero hardcoded outputs, zero facade logic, zero UDFs, zero `applyInPandas`.

---

## 2. Findings

### [Minor / Informational] Finding 1: Defense-in-Depth for `purge_output_directory` Target Path
- **What**: `purge_output_directory` uses `shutil.rmtree(str(target_path), onerror=_handle_readonly)` without an explicit assert guarding against accidental wipe of system or project root (e.g., `assert target_path.resolve() != project_root.resolve()`).
- **Where**: `code/lakehouse/spark_preprocess_tedp.py` (line 277) and `code/lakehouse/spark_sync_province.py` (line 382).
- **Why**: While all internal callers pass explicit Lakehouse subdirectories (`lakehouse_storage/silver/...`), adding a guard assertion prevents accidental misuse if invoked with a misconfigured `--output` argument.
- **Severity**: Low / Informational. Does not block approval since default paths are hardcoded to safe targets and validated.
- **Suggestion**: In future utility refactoring, add a check ensuring `target_path.resolve()` is within `lakehouse_storage/` or `output/`.

---

## 3. Verified Claims

| Claim ID | Claim Description | Verification Method | Result | Evidence / Details |
|:---|:---|:---|:---:|:---|
| VC-01 | `purge_output_directory` defined and invoked in `spark_preprocess_tedp.py` | Direct source inspection & AST check | **PASS** | Line 277 (definition with retry & readonly handler), Line 325 (invoked before `.write.parquet`) |
| VC-02 | `purge_output_directory` defined and invoked in `spark_sync_province.py` | Direct source inspection & AST check | **PASS** | Line 382 (definition), Line 441 (invoked for weather), Line 467 (invoked for province) |
| VC-03 | Exactly 11 partition subdirectories in `lakehouse_storage/silver/tedp_station/` | Directory inspection & filesystem walk | **PASS** | Exactly 11 subdirectories matching `province_name=*`: `Bắc%20Ninh`, `Gia%20Lai`, `HCM`, `Hà%20Nội`, `Hưng%20Yên`, `Hải%20Phòng`, `Lâm%20Đồng`, `Ninh%20Bình`, `Quảng%20Ngãi`, `Tây%20Ninh`, `Đà%20Nẵng`. Zero duplicate or unencoded folders. |
| VC-04 | Station dataset row count is exactly 440,012 | PyArrow / Pandas parquet inspection | **PASS** | `pd.read_parquet('lakehouse_storage/silver/tedp_station')` returns exactly 440,012 rows (no duplicate records). |
| VC-05 | Exactly 11 partitions and 280,632 rows in `tedp_province` and `weather_province` | PyArrow / Pandas parquet inspection | **PASS** | Both tables have exactly 11 partitions and 280,632 rows ($11 \times 25,512$ continuous hours). |
| VC-06 | Automated test suite passes 100% on Windows Host | Command `pytest tests/test_step2_bronze_silver.py -v` | **PASS** | 7 passed in 2.41s. |
| VC-07 | Automated test suite passes 100% in Docker container | Command `docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest /workspace/tests/test_step2_bronze_silver.py -v` | **PASS** | 7 passed in 4.77s. |
| VC-08 | Adversarial parity test suite passes 100% | Command `pytest tests/test_step2_adversarial.py -v` | **PASS** | 8 passed in 4.34s. |
| VC-09 | Zero prohibited APIs (`applyInPandas`, `@udf`, `toPandas`) | AST tree inspection of both Lakehouse scripts | **PASS** | Zero instances found in AST. Pure Catalyst Window & Spark SQL expressions. |
| VC-10 | Review checkpoint documentation updated | Inspection of `docs/review_checkpoint_step2.md` | **PASS** | Section 5 updated with partition row counts; Section 6 added documenting remediation; Section 7 added integrity attestation. |

---

## 4. Coverage Gaps & Unverified Items

- **Coverage Gaps**: None. All dependencies, data files, storage partitions, and execution environments (Windows host and Docker Linux container) were directly inspected and tested.
- **Unverified Items**: None.

---

## 5. Adversarial Review & Attack Surface Analysis

### Overall Risk Assessment: LOW

### Stress-Test Scenarios Conducted

1. **Scenario 1: `purge_output_directory` on non-existent directory**
   - *Test*: Called `purge_output_directory(Path("temp_test_non_existent_dir_12345"))`.
   - *Result*: Exited cleanly without exception (`exists()` guard worked).
   - *Status*: **PASS**.

2. **Scenario 2: `purge_output_directory` on deeply nested directory with files**
   - *Test*: Created directory with nested subdirectories and files; invoked `purge_output_directory`.
   - *Result*: Completely wiped directory structure.
   - *Status*: **PASS**.

3. **Scenario 3: `purge_output_directory` with read-only locked permissions**
   - *Test*: Created file with `stat.S_IREAD` (read-only); invoked `purge_output_directory`.
   - *Result*: The `_handle_readonly` error handler reset permissions via `os.chmod(path, stat.S_IWRITE)` and cleanly purged the target.
   - *Status*: **PASS**.

4. **Scenario 4: Cross-platform directory coexistence under alternating executions**
   - *Scenario*: What happens if pipeline runs on Linux Docker, then Windows Host, then Docker?
   - *Analysis*: Because `purge_output_directory` runs immediately prior to `.write.parquet(...)`, each execution clears the target root folder first. The filesystem never contains conflicting partitions from both POSIX and Windows encoding simultaneously.
   - *Status*: **PASS**.

### Integrity Violation Check

- **Hardcoded test results**: None. All parquet datasets contain genuine 64-bit float pollutant values and authentic timestamps.
- **Dummy / facade logic**: None. The Catalyst Window linear interpolation and spatial grid joining logic are fully implemented and verifiable.
- **Shortcuts bypassing requirements**: None.
- **Fabricated verification logs**: None. Verified directly via local CLI execution in real time.

---

## 6. Final Verdict

**VERDICT: APPROVE**

Milestone 2 (Step 2) remediation meets all quality, architectural, and integrity standards. Milestone 2 is hereby cleared and Milestone 3 (Step 3: Gold Layer Feature Extraction) is unblocked.
