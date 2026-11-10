# Task Assignment: Survey Explorer 1 (Bronze to Silver Preprocessing & Data Baseline)

Read ORIGINAL_REQUEST.md at: `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\ORIGINAL_REQUEST.md`

## Objectives
Investigate the existing codebase and data for Step 2 (Bronze to Silver):
1. Analyze `code/crawl_data/preprocess_tedp.py` and `code/crawl_data/sync_tedp_province.py`.
2. Inspect `data_test/cleaned_tedp_province_4y.csv`, `data_test/weather_openmeteo_province_4y.csv`, and any raw data files available in the workspace.
3. Detail all QCVN filtering rules, outlier handling, station-level interpolation (linear interpolation, forward fill, backward fill, window logic), and province-level synchronization.
4. Verify the exact dimensions, time range, hourly continuity (25,512 continuous hours x 11 provinces = 280,632 rows), and schema types.
5. Identify technical challenges in converting Pandas operations to 100% Spark SQL & Catalyst Window functions (NO applyInPandas, NO Python @udf, NO inferSchema).
6. Detail how `lakehouse_storage/silver/` should be structured (Snappy Parquet partitioned by `province_name`).

## Deliverables
- Write detailed survey report to `.agents/teamwork/explorer_survey_1/survey_bronze_silver.md`
- Write handoff report to `.agents/teamwork/explorer_survey_1/handoff.md`
- Send completion message to orchestrator.


## 2026-11-09T02:31:45Z
You are Survey Explorer 1.
Working directory: d:\năm 4\big data\Final_Project_Plan
State folder: d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\explorer_survey_1\
Read your task assignment in DISPATCH.md in your state folder, and read ORIGINAL_REQUEST.md at d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\ORIGINAL_REQUEST.md.

Perform a thorough codebase and data survey for Step 2 (Bronze to Silver Data Preprocessing):
1. Investigate `code/crawl_data/preprocess_tedp.py` and `code/crawl_data/sync_tedp_province.py`.
2. Inspect `data_test/cleaned_tedp_province_4y.csv`, `data_test/weather_openmeteo_province_4y.csv`, and any raw data files available.
3. Detail all QCVN filtering rules, outlier limits, station-level interpolation (linear, forward fill, backward fill, window boundaries), and province-level synchronization.
4. Document the exact dimensions, time range, hourly continuity (25,512 continuous hours x 11 provinces = 280,632 rows), and schema types.
5. Identify technical strategy to convert Pandas operations to 100% Spark SQL & Catalyst Window functions (NO applyInPandas, NO Python @udf, NO inferSchema).
6. Detail how `lakehouse_storage/silver/` must be structured (Snappy Parquet partitioned by `province_name`).

Write your detailed technical report to:
`d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\explorer_survey_1\survey_bronze_silver.md`
And write your handoff report to:
`d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\explorer_survey_1\handoff.md`

Remember to update progress.md in your state folder with heartbeat timestamps.
When complete, send a message to orchestrator.
