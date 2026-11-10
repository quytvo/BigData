# Task Assignment: Remediation Explorer (Step 2 Forensic Audit Failure Remediation)

Read ORIGINAL_REQUEST.md at: `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\ORIGINAL_REQUEST.md`
Read PROJECT.md at: `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\orchestrator\PROJECT.md`
Read DEAD_ENDS.md at: `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\orchestrator\DEAD_ENDS.md`
Read TEST_READY.md at: `d:\năm 4\big data\Final_Project_Plan\TEST_READY.md`

## MANDATORY AUDIT EVIDENCE REPORT (UNSUMMARIZED)
The Forensic Auditor reported INTEGRITY VIOLATION for Milestone 2 (Step 2). You MUST review this full evidence report and formulate a comprehensive remediation strategy that directly resolves every issue without circumvention:

```markdown
# Forensic Audit Report — Milestone 2 (Step 2: Bronze to Silver Preprocessing)
Work Product: code/lakehouse/spark_preprocess_tedp.py, code/lakehouse/spark_sync_province.py, lakehouse_storage/silver/, tests/test_step2_bronze_silver.py
Verdict: INTEGRITY VIOLATION (Rejected due to test suite failure and partition corruption)

## Executive Summary
1. Test Suite Failure: Both on the Windows host and inside the Docker container (airquality-spark:3.5.1), pytest tests/test_step2_bronze_silver.py FAILS with AssertionError: Expected 11 partition subdirectories matching 'province_name=*', found 15.
2. Storage Partition Corruption: lakehouse_storage/silver/tedp_station/ contains 15 partition subdirectories instead of 11 due to URL-encoded (%20) vs unencoded space path collisions between Linux container and Windows host executions (Gia Lai vs Gia%20Lai, Lâm Đồng vs Lâm%20Đồng, Tây Ninh vs Tây%20Ninh, Đà Nẵng vs Đà%20Nẵng).
3. Data Duplication: Scanning lakehouse_storage/silver/tedp_station/ yields 549,100 rows instead of ~400,000 genuine records, with duplicated station data across 4 provinces.
4. Architectural Root Cause: code/lakehouse/spark_preprocess_tedp.py (and code/lakehouse/spark_sync_province.py) relies exclusively on DataFrameWriter.mode("overwrite").partitionBy(...). In Apache Spark, overwriting a partitioned table does NOT purge existing directory contents if partition directory names differ (e.g. %20 vs literal space). The scripts lack pre-write cleanup logic (e.g., purging stale target directories), breaking cross-platform repeatability between Windows host and Linux container.
5. Behavioral Verification Failure:
AssertionError: Expected 11 partition subdirectories matching 'province_name=*', found 15: ['province_name=Bắc%20Ninh', 'province_name=Gia Lai', 'province_name=Gia%20Lai', 'province_name=HCM', 'province_name=Hà%20Nội', 'province_name=Hưng%20Yên', 'province_name=Hải%20Phòng', 'province_name=Lâm Đồng', 'province_name=Lâm%20Đồng', 'province_name=Ninh%20Bình', 'province_name=Quảng%20Ngãi', 'province_name=Tây Ninh', 'province_name=Tây%20Ninh', 'province_name=Đà Nẵng', 'province_name=Đà%20Nẵng']
```

## Objectives
1. Investigate `code/lakehouse/spark_preprocess_tedp.py`, `code/lakehouse/spark_sync_province.py`, and `lakehouse_storage/silver/tedp_station/`.
2. Formulate a robust fix strategy:
   - Exactly how destination directories (`lakehouse_storage/silver/tedp_station/`, `lakehouse_storage/silver/tedp_province/`, `lakehouse_storage/silver/weather_province/`) should be safely purged prior to writing partitioned Parquet.
   - How `shutil.rmtree` or Spark filesystem cleanup should be integrated into `spark_preprocess_tedp.py` and `spark_sync_province.py`.
   - Ensure cross-platform compatibility between Windows 11 host and Linux Docker container so that partition directory naming (`%20` vs literal space) never collides.
   - Specify the exact worker implementation instructions to achieve 100% (7/7) passing tests on `pytest tests/test_step2_bronze_silver.py`.
3. Do NOT implement the fix yourself (Explorer is read-only). Write recommendations and specifications.

## Deliverables
- Detailed remediation report: `.agents/teamwork/explorer_step2_remediation/remediation_strategy.md`
- Handoff report: `.agents/teamwork/explorer_step2_remediation/handoff.md`
- Message orchestrator when complete.

