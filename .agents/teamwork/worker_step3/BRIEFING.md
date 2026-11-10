# BRIEFING — 2026-11-09T04:49:00Z

## Mission
Implement Milestone 3 (Step 3: Silver to Gold 49 Features Pipeline & Master Lakehouse Runner) with 100% pure PySpark Catalyst Window operations, zero UDFs, bit-identical/numerical parity (< 1e-5) with Pandas training baseline, and pass all independent tests.

## 🔒 My Identity
- Archetype: implementer / qa / specialist
- Roles: implementer, qa
- Working directory: d:\năm 4\big data\Final_Project_Plan
- State folder: d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\worker_step3
- Original parent: 3888b5ae-cede-4e9e-8868-75bef4ef4193
- Milestone: Milestone 3 (Step 3: Silver to Gold 49 Features & Master Lakehouse Runner)

## 🔒 Key Constraints
- 100% Pure PySpark SQL & Catalyst Window Functions (ZERO Python @udf, ZERO applyInPandas, ZERO toPandas in pipeline, ZERO inferSchema=True).
- Storage: Parquet format with Snappy compression, partitioned by province_name at `lakehouse_storage/gold/global_ann_features/`.
- Must call `purge_output_directory(target_dir)` before writing Parquet.
- Exactly 49 atmospheric features + y + target_time + origin_time + metadata columns (`station_id`, `station_code`, `station_name`, `province_id`, `province_name`, `latitude`, `longitude`).
- All 49 features + y are numeric DoubleType/FloatType, exactly 0 NaNs / Nulls.
- Row counts: 280,104 full dataset (11 x 25,464); 279,840 when target_time <= '2026-11-06 23:00:00'.
- Numerical Precision: Max absolute difference vs Pandas baseline < 1e-5.
- Docker execution via `airquality-spark:3.5.1`. All tests in `tests/test_step3_silver_gold.py` must pass 100%.

## Current Parent
- Conversation ID: 3888b5ae-cede-4e9e-8868-75bef4ef4193
- Updated: 2026-11-09T04:49:00Z

## Task Summary
- **What to build**: `code/lakehouse/spark_feature_engineering.py`, `code/lakehouse/build_lakehouse_spark.py`, and `docs/review_checkpoint_step3.md`.
- **Success criteria**: 7/7 tests pass in `tests/test_step3_silver_gold.py`, full lakehouse runner `--verify-diff` succeeds with max diff < 1e-5, zero NaNs, exact 280,104 rows.
- **Interface contracts**: `tests/test_step3_silver_gold.py`, `.agents/teamwork/explorer_survey_2/survey_gold_training.md`, `code/training/train_global_ann.py`.
- **Code layout**: `code/lakehouse/`

## Key Decisions Made
- Implemented pure Spark Catalyst Window definitions without Python UDFs: `w_order`, `w_roll6`, `w_roll24`, and `w_fwd24`.
- Handled `min_periods` in rolling mean via `F.count().over(w_roll) == N`.
- Used `F.weekday(col).cast("double")` for ISO 0.0 (Monday) to 6.0 (Sunday) parity.
- Implemented `purge_output_directory` with retry and read-only permission resetting for cross-platform robustness.
- Implemented Automated Data Diff Engine in `build_lakehouse_spark.py` that verifies all 50 columns with max diff of 1.31e-7 (< 1e-5).

## Artifact Index
- `.agents/teamwork/worker_step3/DISPATCH.md` — Task assignment
- `.agents/teamwork/worker_step3/BRIEFING.md` — Situational awareness
- `.agents/teamwork/worker_step3/progress.md` — Liveness heartbeat
- `code/lakehouse/spark_feature_engineering.py` — Gold 49 Features Pipeline
- `code/lakehouse/build_lakehouse_spark.py` — Master Lakehouse Orchestrator & Data Diff Engine
- `output/lakehouse_verification_report.txt` — Automated Data Diff Verification Report
- `docs/review_checkpoint_step3.md` — Review Checkpoint Documentation

## Change Tracker
- **Files modified**:
  - `code/lakehouse/spark_feature_engineering.py` (New): Pure Catalyst 49 feature extraction and Snappy Parquet writer.
  - `code/lakehouse/build_lakehouse_spark.py` (New): Master Lakehouse runner and Data Diff verification engine.
  - `code/training/train_global_ann.py`: Made optuna safely imported to prevent crashes in minimal environments.
  - `docs/review_checkpoint_step3.md` (New): Comprehensive Step 3 technical verification report.
- **Build status**: 7/7 tests pass on host (2.31s) and Docker container (2.32s). Data diff verified with max diff 1.31e-7 < 1e-5.
- **Pending issues**: None

## Quality Status
- **Build/test result**: 7/7 PASSED (100%)
- **Lint status**: Clean
- **Tests added/modified**: `tests/test_step3_silver_gold.py` passing 100%
