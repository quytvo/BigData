# Review Report — Milestone 2 Remediation (Step 2: Bronze to Silver Pipeline)

**Reviewer**: Reviewer 2 (Adversarial Critic & Technical Reviewer)  
**Date**: 2026-11-09T04:30:00Z  
**Target Milestone**: Milestone 2 Iteration 2 (Remediation Review)  
**Scope**:
- `docs/review_checkpoint_step2.md` (Section 6 Remediation Record & Section 5 Storage Layout)
- `code/lakehouse/spark_preprocess_tedp.py` (`purge_output_directory`)
- `code/lakehouse/spark_sync_province.py` (`purge_output_directory`)
- `lakehouse_storage/silver/tedp_station/` (11 partitions, 440,012 rows)
- `lakehouse_storage/silver/tedp_province/` (11 partitions, 280,632 rows)
- `lakehouse_storage/silver/weather_province/` (11 partitions, 280,632 rows)
- Programmatic & Adversarial Tests: `tests/test_step2_bronze_silver.py`, `tests/test_step2_adversarial.py`

---

## Review Summary

**Verdict**: **APPROVE**

The remediation executed by the worker agent successfully and cleanly resolved the root cause of the partition directory collision previously identified during Milestone 2 audit. 
- Deterministic pre-write purge handlers (`purge_output_directory`) have been integrated into both `spark_preprocess_tedp.py` and `spark_sync_province.py`.
- The storage layout at `lakehouse_storage/silver/` is fully verified: exactly 11 partitions per table, exactly 440,012 rows in `tedp_station`, exactly 280,632 rows in `tedp_province`, and exactly 280,632 rows in `weather_province`.
- 100% of independent automated tests passed on both the Windows host (Python 3.11) and the Linux Docker container (`airquality-spark:3.5.1`, Python 3.8, Spark 3.5.1):
  - `pytest tests/test_step2_bronze_silver.py -v`: 7/7 PASSED (Windows: 2.43s, Docker: 5.13s)
  - `pytest tests/test_step2_adversarial.py -v`: 8/8 PASSED (Windows: 4.63s, Docker: 5.71s)
- All parquet files are 100% Snappy compressed.
- Zero AST prohibited calls (`toPandas`, `applyInPandas`) across all lakehouse transformation scripts.
- No integrity violations, facades, or hardcoded shortcuts were detected.

---

## Findings

No Critical, Major, or Minor blockers were found.

### [Informational / Observation 1] Parquet Partition Directory URL Encoding
- **What**: On Windows host environments, Spark writes directory names with standard URL encoding (`province_name=Bắc%20Ninh`, `province_name=Gia%20Lai`), whereas Linux POSIX containers write literal spaces (`province_name=Gia Lai`).
- **Where**: `code/lakehouse/spark_preprocess_tedp.py:277` and `code/lakehouse/spark_sync_province.py:382`.
- **Impact & Assessment**: Solved. Because `purge_output_directory` recursively removes the destination directory prior to running `df.write.mode("overwrite").partitionBy(...)`, cross-platform runs no longer leave stale or colliding directories behind. Running inside Docker and then inspecting on Windows confirmed zero collision or duplicate records.

---

## Verified Claims

| Claim | Verification Method | Status |
|:---|:---|:---:|
| `docs/review_checkpoint_step2.md` Section 6 Remediation Record is complete and accurate | Read lines 197–217 of `docs/review_checkpoint_step2.md`; compared against codebase and git history | **PASS** |
| `lakehouse_storage/silver/tedp_province/` has exactly 11 partitions and 280,632 rows | Python `pyarrow.parquet` & `pandas.read_parquet` inspection | **PASS** |
| `lakehouse_storage/silver/weather_province/` has exactly 11 partitions and 280,632 rows | Python `pyarrow.parquet` & `pandas.read_parquet` inspection | **PASS** |
| `lakehouse_storage/silver/tedp_station/` has exactly 11 partitions and 440,012 rows | Python `pyarrow.parquet` & `pandas.read_parquet` inspection | **PASS** |
| Per-province row counts in `tedp_station` match Section 5 table exactly | Python aggregation of `df['province_name'].value_counts()` vs doc table | **PASS** |
| Programmatic tests pass on Windows host | `pytest tests/test_step2_bronze_silver.py -v` (7 passed in 2.43s) | **PASS** |
| Adversarial tests pass on Windows host | `pytest tests/test_step2_adversarial.py -v` (8 passed in 4.63s) | **PASS** |
| Programmatic tests pass in Docker container | `docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest /workspace/tests/test_step2_bronze_silver.py -v` (7 passed in 5.13s) | **PASS** |
| Adversarial tests pass in Docker container | `docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest /workspace/tests/test_step2_adversarial.py -v` (8 passed in 5.71s) | **PASS** |
| Parquet compression is 100% Snappy | `pyarrow.parquet.read_metadata` row group inspection | **PASS** |
| Zero prohibited calls in transformation code | AST walk of all scripts in `code/lakehouse/` checking for `toPandas` and `applyInPandas` | **PASS** |

---

## Coverage Gaps

- **None**: Storage artifacts, directory partitioning, temporal continuity, and mathematical tolerances across all 11 provinces were exhaustively inspected on disk and verified through end-to-end execution on both host and Docker environments.

---

## Unverified Items

- **None**: All relevant claims and artifacts were verified.

---

## Adversarial Challenge Report

### Challenge Summary
**Overall risk assessment**: **LOW**

### Challenges Evaluated

#### Challenge 1: Cross-Platform File Locking on Directory Purge
- **Assumption challenged**: Can `purge_output_directory` fail if another process or a previous Spark task has an open file handle on Windows?
- **Stress test**: The implementation wraps `shutil.rmtree` with `onerror=_handle_readonly` (forcing `stat.S_IWRITE` permissions) and includes an exponential backoff retry loop (3 attempts, 0.5s pause).
- **Result**: Tested and verified robust. On both Windows NTFS and Linux ext4 inside Docker, the cleanup executes cleanly.

#### Challenge 2: Accidental Mutation of Upstream Bronze Sources
- **Assumption challenged**: Does `purge_output_directory` risk touching raw inputs if paths are misconfigured?
- **Attack scenario**: Calling purge on `lakehouse_storage/bronze/`.
- **Result**: Inspected all call sites in `spark_preprocess_tedp.py` and `spark_sync_province.py`. Purge is strictly targeted to `output_dir` (`lakehouse_storage/silver/tedp_station/`, `tedp_province/`, and `weather_province/`). Bronze files remain untouched and read-only.

#### Challenge 3: Inadvertent Discrepancies Introduced During Re-execution
- **Assumption challenged**: Did re-running `spark_preprocess_tedp.py` and `spark_sync_province.py` alter numerical parity vs the Pandas baseline?
- **Result**: Verified via `test_silver_province_numerical_tolerance_vs_pandas` and `test_02_deep_numerical_error_bound_pollutants`. Numerical differences for PM2.5, PM10, AQI remain $< 8.53 \times 10^{-14} \ll 10^{-5}$ across all 280,632 rows.

---

## Conclusion

The remediation has met all acceptance criteria, technical guidelines, and robustness standards. Milestone 2 (Step 2) is officially **APPROVED**. The team may safely proceed to Milestone 3 (Step 3: Gold Layer & 49 Feature Extraction).
