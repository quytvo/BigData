# Task Assignment: Challenger 2 (Step 2 Container Execution & Performance Stress)

Read ORIGINAL_REQUEST.md at: `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\ORIGINAL_REQUEST.md`
Read PROJECT.md at: `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\orchestrator\PROJECT.md`
Read TEST_READY.md at: `d:\năm 4\big data\Final_Project_Plan\TEST_READY.md`

## Objectives
Adversarially challenge Step 2 container execution and partition pruning:
1. Docker Container Execution:
   - Run `tests/test_step2_bronze_silver.py` inside the Docker container:
     `docker run --rm -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 pytest tests/test_step2_bronze_silver.py -v`
   - Verify execution completes without OOM within 8GB RAM and 4 CPUs limits.
2. Partition Pruning Verification on Silver Parquet:
   - Test querying a single province (e.g. `province_name = 'Hà Nội'`) from `lakehouse_storage/silver/tedp_province/` using PyArrow or PySpark.
   - Verify that partition pruning successfully reads only the 25,512 rows for Hà Nội and skips the other 10 partitions.
3. Formulate explicit Verdict: **APPROVE** or **REQUEST_CHANGES**.

Write report to `.agents/teamwork/challenger_step2_2/challenge.md` and handoff to `.agents/teamwork/challenger_step2_2/handoff.md`.
Message orchestrator with your verdict.


## 2026-11-09T03:49:58Z
You are Challenger 2 for Milestone 2 (Step 2).
Working directory: d:\năm 4\big data\Final_Project_Plan
State folder: d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\challenger_step2_2\
Read DISPATCH.md in your state folder, ORIGINAL_REQUEST.md at d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\ORIGINAL_REQUEST.md, PROJECT.md at d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\orchestrator\PROJECT.md, and TEST_READY.md at d:\năm 4\big data\Final_Project_Plan\TEST_READY.md.

Adversarially challenge Step 2 container execution and partition pruning:
- Run tests in Docker container:
  docker run --rm -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 pytest tests/test_step2_bronze_silver.py -v
- Test partition pruning on Silver Parquet: query single province (e.g. Hà Nội) and verify only 25,512 rows are scanned.
- Formulate explicit verdict: APPROVE or REQUEST_CHANGES.

Write report to .agents/teamwork/challenger_step2_2/challenge.md and handoff to .agents/teamwork/challenger_step2_2/handoff.md.
Message orchestrator with your verdict.
