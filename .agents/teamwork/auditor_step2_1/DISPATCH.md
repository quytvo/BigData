# Task Assignment: Forensic Auditor (Step 2 Integrity Forensics)

Read ORIGINAL_REQUEST.md at: `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\ORIGINAL_REQUEST.md`
Read PROJECT.md at: `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\orchestrator\PROJECT.md`
Read TEST_READY.md at: `d:\năm 4\big data\Final_Project_Plan\TEST_READY.md`

## Objectives
Conduct forensic integrity audit on Step 2 implementation:
1. Anti-Cheating & Prohibited Pattern Checks:
   - Inspect `code/lakehouse/spark_preprocess_tedp.py` and `code/lakehouse/spark_sync_province.py`.
   - Verify there is NO cheating, NO hardcoded baseline CSV copying (e.g. simply copying `cleaned_tedp_province_4y.csv` to parquet), NO fake interpolation dummy functions.
   - Verify that all data transformations are 100% genuine Spark SQL Catalyst Window operations executed on the JVM.
   - Verify that there are ZERO `applyInPandas`, ZERO Python `@udf`, and ZERO `inferSchema=True`.
2. Static Analysis & File Provenance:
   - Inspect Parquet metadata in `lakehouse_storage/silver/tedp_province/` and `lakehouse_storage/silver/tedp_station/` to verify genuine Spark/Parquet generation.
   - Verify genuine execution in event logs or container logs.
3. Formulate binary verdict: **CLEAN** or **INTEGRITY VIOLATION**.

Write report to `.agents/teamwork/auditor_step2_1/audit.md` and handoff to `.agents/teamwork/auditor_step2_1/handoff.md`.
Message orchestrator with your verdict.


## 2026-11-09T03:50:07Z
You are Forensic Auditor for Milestone 2 (Step 2).
Working directory: d:\năm 4\big data\Final_Project_Plan
State folder: d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\auditor_step2_1\
Read DISPATCH.md in your state folder, ORIGINAL_REQUEST.md at d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\ORIGINAL_REQUEST.md, PROJECT.md at d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\orchestrator\PROJECT.md, and TEST_READY.md at d:\năm 4\big data\Final_Project_Plan\TEST_READY.md.

Perform forensic integrity verification on Step 2:
- Inspect `code/lakehouse/spark_preprocess_tedp.py` and `code/lakehouse/spark_sync_province.py`.
- Verify there is NO cheating, NO copying cleaned CSV, NO dummy mocks.
- Verify 100% genuine Spark SQL and Catalyst Window transformations.
- Confirm ZERO applyInPandas, ZERO Python @udf, ZERO inferSchema=True.
- Inspect Parquet metadata in `lakehouse_storage/silver/` to confirm genuine Spark Parquet generation.
- Formulate binary verdict: CLEAN or INTEGRITY VIOLATION.

Write report to .agents/teamwork/auditor_step2_1/audit.md and handoff to .agents/teamwork/auditor_step2_1/handoff.md.
Message orchestrator with your verdict.
