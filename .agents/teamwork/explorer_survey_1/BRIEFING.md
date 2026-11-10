# BRIEFING — 2026-11-09T02:43:00Z

## Mission
Perform comprehensive codebase and data survey for Step 2 (Bronze to Silver Data Preprocessing) to guide PySpark migration.

## 🔒 My Identity
- Archetype: explorer
- Roles: Teamwork explorer (read-only investigation, synthesis, structured reports)
- Working directory: d:\năm 4\big data\Final_Project_Plan
- State folder: d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\explorer_survey_1
- Original parent: 3888b5ae-cede-4e9e-8868-75bef4ef4193
- Milestone: Survey & Architectural Design for Medallion Step 2

## 🔒 Key Constraints
- Read-only investigation — do NOT implement or modify source code
- Strictly NO applyInPandas, NO Python @udf, NO inferSchema in proposed PySpark strategy (pure Catalyst Window and Spark SQL)
- Investigate exact QCVN rules, outlier thresholds, linear/ffill/bfill window operations, province aggregation, dimensions, and schemas
- Keep all outputs in state folder: `survey_bronze_silver.md`, `handoff.md`, `progress.md`, `BRIEFING.md`

## Current Parent
- Conversation ID: 3888b5ae-cede-4e9e-8868-75bef4ef4193
- Updated: 2026-11-09T02:32:00Z

## Investigation State
- **Explored paths**: `code/crawl_data/preprocess_tedp.py`, `code/crawl_data/sync_tedp_province.py`, `lakehouse_storage/bronze/tedp_aqi_pm25_hourly.csv`, `lakehouse_storage/bronze/cleaned_tedp.csv`, `data_test/cleaned_tedp_province_4y.csv`, `data_test/weather_openmeteo_province_4y.csv`, `data_test/province_coverage_4y.csv`, `data_test/province_station_members_4y.csv`, `code/training/common.py`, `code/training/train_global_ann.py`, `code/training/du_bao.py`.
- **Key findings**:
  1. Exact dimensions verified: 280,632 rows = 11 provinces x 25,512 continuous hours (`2023-12-11 00:00:00` to `2026-11-07 23:00:00`).
  2. QCVN outlier limits: `pm25` [0, 500], `pm10` [0, 600], `aqi` [0, 500].
  3. Station-level linear interpolation with limit=3 and median fallback successfully formulated into 100% pure Spark Catalyst Window functions using `F.last(..., ignorenulls=True)` and `F.first(..., ignorenulls=True)`. Verified with empirical test script.
  4. Time grid generation via `sequence` and `explode` cross-joined with province metadata eliminates driver iteration.
  5. `pm25_gap_hours` continuous missing length calculation converted to simple arithmetic on timestamps from Window functions.
  6. Silver storage structure designed as Snappy Parquet partitioned by `province_name` in `lakehouse_storage/silver/tedp_province/` and `lakehouse_storage/silver/tedp_station/`.
- **Unexplored areas**: None for Step 2 survey. All assigned objectives have been thoroughly surveyed and designed.

## Key Decisions Made
- Established pure Spark Catalyst Window strategy for linear interpolation, eliminating `applyInPandas` and Python `@udf` completely.
- Formulated static `StructType` schemas for Bronze and Silver tables.
- Completed comprehensive survey report (`survey_bronze_silver.md`) and handoff report (`handoff.md`).

## Artifact Index
- `.agents/teamwork/explorer_survey_1/survey_bronze_silver.md` — Detailed technical survey report for Step 2
- `.agents/teamwork/explorer_survey_1/handoff.md` — 5-component hard handoff report
- `.agents/teamwork/explorer_survey_1/progress.md` — Liveness heartbeat and progress tracking
- `.agents/teamwork/explorer_survey_1/DISPATCH.md` — Task assignment log
