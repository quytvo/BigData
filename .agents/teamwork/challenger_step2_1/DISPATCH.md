# Task Assignment: Challenger 1 (Step 2 Numerical Parity & Edge Case Challenge)

Read ORIGINAL_REQUEST.md at: `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\ORIGINAL_REQUEST.md`
Read PROJECT.md at: `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\orchestrator\PROJECT.md`
Read TEST_READY.md at: `d:\năm 4\big data\Final_Project_Plan\TEST_READY.md`

## Objectives
Adversarially challenge Step 2 deliverables:
1. Numerical Parity Deep Check:
   - Compare `lakehouse_storage/silver/tedp_province/` directly with `data_test/cleaned_tedp_province_4y.csv` across all 11 provinces.
   - Compute max absolute difference across `pm25`, `pm10`, `aqi`.
   - Verify that max difference is strictly < 1e-5.
   - Verify `pm25_gap_hours` exact match with Pandas baseline.
2. Catalyst Window Robustness:
   - Check if the Catalyst Window algorithms handle edge cases (beginning/end of series, long gaps > 3h, missing stations) authentically.
3. Verification Execution:
   - Run `pytest tests/test_step2_bronze_silver.py -v`.
4. Formulate explicit Verdict: **APPROVE** or **REQUEST_CHANGES**.

Write report to `.agents/teamwork/challenger_step2_1/challenge.md` and handoff to `.agents/teamwork/challenger_step2_1/handoff.md`.
Message orchestrator with your verdict.


## 2026-11-09T03:49:51Z
You are Challenger 1 for Milestone 2 (Step 2).
Working directory: d:\năm 4\big data\Final_Project_Plan
State folder: d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\challenger_step2_1\
Read DISPATCH.md in your state folder, ORIGINAL_REQUEST.md at d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\ORIGINAL_REQUEST.md, PROJECT.md at d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\orchestrator\PROJECT.md, and TEST_READY.md at d:\năm 4\big data\Final_Project_Plan\TEST_READY.md.

Adversarially challenge Step 2 deliverables:
- Deep numerical comparison between lakehouse_storage/silver/tedp_province/ and data_test/cleaned_tedp_province_4y.csv.
- Compute max absolute difference across pm25, pm10, aqi, and pm25_gap_hours. Assert < 1e-5.
- Run pytest tests/test_step2_bronze_silver.py -v.
- Formulate explicit verdict: APPROVE or REQUEST_CHANGES.

Write report to .agents/teamwork/challenger_step2_1/challenge.md and handoff to .agents/teamwork/challenger_step2_1/handoff.md.
Message orchestrator with your verdict.
