## 2026-11-09T04:32:41Z
You are the Implementation Worker for Milestone 3 (Step 3: Silver to Gold 49 Features Pipeline & Master Lakehouse Runner) in the PySpark Air Quality Lakehouse project.

## MANDATORY INTEGRITY WARNING
DO NOT CHEAT. All implementations must be genuine. DO NOT hardcode test results, create dummy/facade implementations, or circumvent the intended task. A teamwork_auditor will independently verify your work. Integrity violations WILL be detected and your work WILL be rejected.

## Context & Resources
- Working directory: `d:\năm 4\big data\Final_Project_Plan`
- State folder: `.agents/teamwork/worker_step3/`
- User Request: `.agents/teamwork/ORIGINAL_REQUEST.md`
- Project Architecture & Scope: `.agents/teamwork/orchestrator/PROJECT.md`
- Test Infrastructure: `.agents/teamwork/orchestrator/TEST_INFRA.md`
- Survey report on Gold Features: `.agents/teamwork/explorer_survey_2/survey_gold_training.md`
- Existing training feature reference: `code/training/train_global_ann.py` (`build_global_features` and `add_station_id_Columns`)
- Independent test suite: `tests/test_step3_silver_gold.py` and `tests/conftest.py`
- Reference baseline data:
  - `lakehouse_storage/silver/tedp_province/` (Parquet, Snappy, partitioned by `province_name`, 280,632 rows)
  - `lakehouse_storage/silver/weather_province/` (Parquet, Snappy, partitioned by `province_name`, 280,632 rows)
  - `data_test/cleaned_tedp_province_4y.csv` and `data_test/weather_openmeteo_province_4y.csv`

## Technical Constraints (Strict)
1. 100% Pure PySpark SQL & Catalyst Window Functions.
   - ZERO Python `@udf` or `udf()`.
   - ZERO `applyInPandas`.
   - ZERO `toPandas()` inside the transformation pipeline.
   - ZERO `inferSchema=True`. Define static `StructType` schema.
2. Storage: Parquet format with Snappy compression, partitioned by `province_name`.
   - Output directory: `lakehouse_storage/gold/global_ann_features/`.
   - Call `purge_output_directory(target_dir)` before writing Parquet to eliminate partition collisions and cross-platform path artifacts.
3. Feature Completeness: Exactly 49 atmospheric features + `y` + `target_time` + `origin_time` + metadata columns (`station_id`, `station_code`, `station_name`, `province_id`, `province_name`, `latitude`, `longitude`).
   - All 49 features and `y` must be numeric DoubleType/FloatType.
   - Exactly 0 NaNs / Nulls.
4. Sample counts:
   - Full Gold dataset: Exactly 280,104 rows (11 provinces x 25,464 rows after dropping 48 boundary rows per province: 24h lookback and 24h lookahead).
   - Filtered dataset when `target_time <= '2026-11-06 23:00:00'`: Exactly 279,840 rows (11 provinces x 25,440 rows).
5. Numerical Precision:
   - Max absolute difference vs Pandas baseline (`train_global_ann.py: build_global_features`) strictly < 1e-5.
   - Cyclic features (`hour_sin`, `hour_cos`, `dow_sin`, `dow_cos`) in [-1.0, 1.0].
   - Day of week: 0.0 = Monday ... 6.0 = Sunday (`F.weekday("target_time").cast("double")`).
   - Spatial coords normalized to [-1.0, 1.0] using lat_min=10.5391, lat_max=21.301503, lon_min=105.81825, lon_max=109.2198.
   - 11 One-hot station columns (`station_id_<id>`).
   - `precip_sum_24h` is cumulative future precipitation over next 24 hours: sum of precipitation at t+1 to t+24 (`F.sum("precipitation").over(w_fwd24)`).

## Detailed Tasks
1. **Implement `code/lakehouse/spark_feature_engineering.py`**:
   - Centralized module reading Silver TEDP and Weather Parquet datasets.
   - Join on `['province_name', 'datetime']` (or `['station_id', 'datetime']`).
   - Implement the full Catalyst Window logic for all 49 features + label `y`.
   - Drop the 48 boundary rows per province where lookback or lookahead features are null.
   - Support CLI arguments (e.g. `--output-dir`, `--cutoff-target-date`, `--verify-diff`).
   - Purge destination directory with `purge_output_directory` and write partitioned Parquet with Snappy compression.
2. **Implement `code/lakehouse/build_lakehouse_spark.py`**:
   - Master Lakehouse pipeline orchestrator coordinating Step 1, Step 2, and Step 3 sequentially.
   - CLI support (`--steps all`, `--verify-diff`, `--keep-ui`).
   - Implements automated Data Diff comparing Spark Gold output with Pandas baseline on `data_test/`, asserting max absolute error < 1e-5.
3. **Run Verification & Tests**:
   - Run PySpark write operations via Docker container `airquality-spark:3.5.1`:
     `docker run --rm -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 python code/lakehouse/spark_feature_engineering.py`
     and `docker run --rm -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 python code/lakehouse/build_lakehouse_spark.py --verify-diff`
   - Run test suite: `pytest tests/test_step3_silver_gold.py -v` (on host and inside Docker). All 7 tests must pass 100%.
4. **Draft Review Checkpoint Report `docs/review_checkpoint_step3.md`**:
   - Document architecture, schema definition, window expressions, row counts (280,104 / 279,840), zero-NaN validation, numerical diff comparison table, Docker execution logs, and test results.
5. **Write `handoff.md`** in `.agents/teamwork/worker_step3/handoff.md` and send completion message via `send_message`.
