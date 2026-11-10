## 2026-11-09T03:07:20Z
You are Worker Step 2.
Working directory: d:\năm 4\big data\Final_Project_Plan
State folder: d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\worker_step2\
Read DISPATCH.md in your state folder, ORIGINAL_REQUEST.md at d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\ORIGINAL_REQUEST.md, PROJECT.md at d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\orchestrator\PROJECT.md, Explorer 1 survey report at d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\explorer_survey_1\survey_bronze_silver.md, and TEST_READY.md at d:\năm 4\big data\Final_Project_Plan\TEST_READY.md.

Implement Step 2 deliverables:
1. `code/lakehouse/spark_preprocess_tedp.py`:
   - Station-level cleaning on `lakehouse_storage/bronze/tedp_aqi_pm25_hourly.csv`.
   - Pure Catalyst Window functions: QCVN filtering, linear interpolation (limit=3), median imputation for gaps > 3h with col_median_filled.
   - Save station Parquet to `lakehouse_storage/silver/tedp_station/`.
2. `code/lakehouse/spark_sync_province.py`:
   - Province synchronization: mask median-filled records, 4-year grid (2023-12-11 to 2026-11-07, 25,512 hrs x 11 provinces = 280,632 rows).
   - Drop late station BDi_KKXQ_KHITPH, filter 11 provinces with coverage >= 80%.
   - Pure Catalyst bidirectional time interpolation and pm25_gap_hours.
   - Match Pandas station_name string formatting ("Trung bình 1.0 trạm").
   - Save Snappy Parquet partitioned by province_name to `lakehouse_storage/silver/tedp_province/`.
   - Also save weather data to `lakehouse_storage/silver/weather_province/` partitioned by province_name.
3. Verification:
   - Run `pytest tests/test_step2_bronze_silver.py`. Ensure 100% of 7 tests pass.
   - Verify numerical difference vs `data_test/cleaned_tedp_province_4y.csv` is < 1e-5.
   - Write review checkpoint report to `docs/review_checkpoint_step2.md`.

MANDATORY INTEGRITY WARNING:
DO NOT CHEAT. All implementations must be genuine. DO NOT hardcode test results, create dummy/facade implementations, or circumvent the intended task. A teamwork_auditor will independently verify your work. Integrity violations WILL be detected and your work WILL be rejected.

Write your handoff report to `.agents/teamwork/worker_step2/handoff.md` and send a message when complete.


## 2026-11-09T03:38:12Z
**Context**: Liveness check for Milestone 2 (Step 2: Bronze to Silver Preprocessing)
**Content**: Orchestrator checking in on execution progress. Your last visited timestamp was 03:23:30Z (~14 minutes ago) while executing `code/lakehouse/spark_preprocess_tedp.py` in Docker.
**Action**: Please report your current execution status, whether the background Docker task has completed, and your next immediate step.
