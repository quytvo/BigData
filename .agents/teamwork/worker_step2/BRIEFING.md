# BRIEFING — 2026-11-09T03:47:30Z

## Mission
Implement Step 2 deliverables: PySpark Bronze to Silver data cleaning pipelines (`spark_preprocess_tedp.py` and `spark_sync_province.py`) using pure Catalyst window operations, passing 100% of 7 tests in `tests/test_step2_bronze_silver.py`, ensuring numerical difference < 1e-5 vs pandas baseline, and generating checkpoint docs.

## 🔒 My Identity
- Archetype: implementer, qa, specialist
- Roles: implementer, qa
- Working directory: d:\năm 4\big data\Final_Project_Plan
- State folder: d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\worker_step2\
- Original parent: 3888b5ae-cede-4e9e-8868-75bef4ef4193
- Milestone: Step 2 - Bronze to Silver PySpark Cleaning Pipeline

## 🔒 Key Constraints
- Pure Catalyst Window functions: NO toPandas(), NO custom UDFs for window interpolation if pure Catalyst expressions can do it, scalable distributed execution.
- Maintain genuine implementation: NO hardcoded test results, facade logic, or cheating.
- Exact schema & numerical accuracy: numerical difference vs `data_test/cleaned_tedp_province_4y.csv` < 1e-5.
- Match Pandas station_name string formatting ("Trung bình X.X trạm" or single station name).
- 4-year grid: 2023-12-11 00:00:00 to 2026-11-07 23:00:00 (25,512 hrs x 11 provinces = 280,632 rows).
- Output paths: `lakehouse_storage/silver/tedp_station/`, `lakehouse_storage/silver/tedp_province/`, `lakehouse_storage/silver/weather_province/`.
- Test suite: 100% of 7 tests in `tests/test_step2_bronze_silver.py` must pass.

## Current Parent
- Conversation ID: 3888b5ae-cede-4e9e-8868-75bef4ef4193
- Updated: 2026-11-09T03:47:30Z

## Task Summary
- **What to build**: `code/lakehouse/spark_preprocess_tedp.py`, `code/lakehouse/spark_sync_province.py`, verification report `docs/review_checkpoint_step2.md`.
- **Success criteria**: 7/7 tests pass in pytest (both Windows host & Docker container), diff < 1e-5 vs pandas benchmark (actual diff < 8.53e-14), Snappy Parquet output properly partitioned by `province_name`.
- **Interface contracts**: `PROJECT.md`, `TEST_READY.md`, `survey_bronze_silver.md`.

## Key Decisions Made
- Implemented O(N) Streaming Reverse Window (`orderBy(datetime.desc()).rowsBetween(unboundedPreceding, -1)`) for backward tracking, eliminating Catalyst's O(N^2) forward buffering (`rowsBetween(1, unboundedFollowing)`).
- Replaced iterative window-level percentile approximation with `groupBy("station_id").agg(percentile_approx)` broadcast join, reducing station preprocessing runtime from > 20 minutes to 32 seconds.
- Ensured WinUtils compatibility on Windows host via automatic path detection, while natively supporting Linux Docker container execution.
- Configured explicit `StructType` schemas for station, province, and weather Silver datasets, enforcing TimestampType for datetime and DoubleType for all numeric measurements.

## Artifact Index
- `code/lakehouse/spark_preprocess_tedp.py` — Station-level QCVN filter, linear interpolation (limit=3), median imputation.
- `code/lakehouse/spark_sync_province.py` — Province synchronization, 4-year continuous grid, bidirectional time interpolation, weather sync.
- `code/lakehouse/spark_session.py` — Augmented SparkSession builder with Windows compatibility flags.
- `lakehouse_storage/silver/tedp_station/` — Parquet dataset partitioned by province_name (11 partitions).
- `lakehouse_storage/silver/tedp_province/` — 280,632 rows x 30 cols partitioned by province_name (11 partitions).
- `lakehouse_storage/silver/weather_province/` — 280,632 rows x 19 cols partitioned by province_name (11 partitions).
- `docs/review_checkpoint_step2.md` — Formal review checkpoint documentation.
- `.agents/teamwork/worker_step2/handoff.md` — Hard handoff report.

## Change Tracker
- **Files modified**:
  - `code/lakehouse/spark_preprocess_tedp.py` (created)
  - `code/lakehouse/spark_sync_province.py` (created)
  - `code/lakehouse/spark_session.py` (augmented)
  - `docs/review_checkpoint_step2.md` (created)
- **Build status**: All pipelines execute cleanly; 7/7 tests pass in pytest.
- **Pending issues**: None.

## Quality Status
- **Build/test result**: 7/7 passed in `tests/test_step2_bronze_silver.py` on both host and Docker container.
- **Lint status**: Clean Python code adhering to PEP 8.
- **Tests added/modified**: `tests/test_step2_bronze_silver.py` validated with 100% pass rate.

## Loaded Skills
- None
