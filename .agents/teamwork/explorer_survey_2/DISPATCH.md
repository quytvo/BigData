# Task Assignment: Survey Explorer 2 (Silver to Gold Feature Engineering & Model Consumption)

Read ORIGINAL_REQUEST.md at: `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\ORIGINAL_REQUEST.md`

## Objectives
Investigate the existing codebase and data for Step 3 & Step 4 (Feature Engineering & Model Training/Inference):
1. Analyze `code/training/train_global_ann.py`, `code/training/du_bao.py`, `code/training/common.py`, and any related feature engineering scripts.
2. Enumerate all 49 atmospheric features: how they are defined, calculated, lag periods, rolling windows, meteorological interaction terms, solar angle, AQI, etc.
3. Check target variables, dataset dimensions (279,840 samples x 49 features), input/output schemas, and NaN handling.
4. Detail how `lakehouse_storage/gold/global_ann_features/` must be partitioned and structured.
5. Identify technical challenges in converting feature engineering to pure Spark SQL Catalyst Window functions (NO applyInPandas, NO Python @udf, NO inferSchema).
6. Detail how `common.py`, `train_global_ann.py`, and `du_bao.py` currently load CSV data and how they should be adapted to read Parquet with Partition Pruning (e.g. `spark.read.parquet(...).filter(...)` or pyarrow/fastparquet partition pruning) with automatic fallback to CSV in `data_test/`.
7. Detail requirements for `code/lakehouse/build_lakehouse_spark.py` and Data Diff verification between PySpark and Pandas baseline (< 1e-5 error).

## Deliverables
- Write detailed survey report to `.agents/teamwork/explorer_survey_2/survey_gold_training.md`
- Write handoff report to `.agents/teamwork/explorer_survey_2/handoff.md`
- Send completion message to orchestrator.


## 2026-11-09T02:31:53Z
Sender: 3888b5ae-cede-4e9e-8868-75bef4ef4193
Content:
You are Survey Explorer 2.
Working directory: d:\năm 4\big data\Final_Project_Plan
State folder: d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\explorer_survey_2\
Read your task assignment in DISPATCH.md in your state folder, and read ORIGINAL_REQUEST.md at d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\ORIGINAL_REQUEST.md.

Perform a thorough codebase and data survey for Step 3 & Step 4 (Silver to Gold Feature Engineering & Model Training/Inference):
1. Investigate `code/training/train_global_ann.py`, `code/training/du_bao.py`, `code/training/common.py`, and any existing feature engineering code.
2. Enumerate all 49 atmospheric features: exact formulas, lag periods, rolling windows, meteorological interaction terms, solar elevation/azimuth or temporal encodings, AQI calculations, etc.
3. Check target variables, dataset dimensions (279,840 samples x 49 features), input/output schemas, and NaN handling.
4. Detail how `lakehouse_storage/gold/global_ann_features/` must be partitioned and structured.
5. Identify technical strategy to convert feature engineering to 100% Spark SQL Catalyst Window functions (NO applyInPandas, NO Python @udf, NO inferSchema).
6. Detail how `common.py`, `train_global_ann.py`, and `du_bao.py` currently load CSV data and how they should be adapted to read Parquet with Partition Pruning (by province_name) with automatic fallback to CSV in `data_test/`.
7. Detail requirements for `code/lakehouse/build_lakehouse_spark.py` and Data Diff verification between PySpark and Pandas baseline (< 1e-5 numerical error).

Write your detailed technical report to:
`d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\explorer_survey_2\survey_gold_training.md`
And write your handoff report to:
`d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\explorer_survey_2\handoff.md`

Remember to update progress.md in your state folder with heartbeat timestamps.
When complete, send a message to orchestrator.
