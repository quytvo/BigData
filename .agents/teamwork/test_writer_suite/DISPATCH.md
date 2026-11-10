# Task Assignment: Independent Test Suite Writer

## 2026-11-09T02:45:32Z
You are the Independent Test Suite Writer.
Working directory: d:\năm 4\big data\Final_Project_Plan
State folder: d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\test_writer_suite\
Read DISPATCH.md in your state folder, ORIGINAL_REQUEST.md at d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\ORIGINAL_REQUEST.md, PROJECT.md at d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\orchestrator\PROJECT.md, and TEST_INFRA.md at d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\orchestrator\TEST_INFRA.md.

Build the independent programmatic test suite in `tests/`:
1. `tests/conftest.py`
2. `tests/test_step1_docker_spark.py`
3. `tests/test_step2_bronze_silver.py`
4. `tests/test_step3_silver_gold.py`
5. `tests/test_step4_model_integration.py`
6. `tests/test_step5_benchmark_docker.py`

Ensure all tests follow pytest standards, have clear assertions for schema (StructType), row counts, temporal continuity, numerical tolerances (< 1e-5), partition pruning, and fallback.
When all test files are written and verified, create `TEST_READY.md` at project root, write `handoff.md` in your state folder, and send a completion message to the orchestrator.

## Detailed Objectives
Build the independent programmatic test suite in `tests/` covering Steps 1 to 5:
1. `tests/conftest.py`:
   - Setup pytest fixtures, path configurations, and helper functions (e.g. comparing numerical tolerance < 1e-5 between DataFrames, checking StructType schemas).
2. `tests/test_step1_docker_spark.py`:
   - Test `code/lakehouse/spark_session.py`:
     - Creates SparkSession with local[4], master, 5g driver memory, shuffle partitions 16.
     - Verifies Arrow enabled (`spark.sql.execution.arrow.pyspark.enabled == "true"`).
     - Verifies AQE enabled (`spark.sql.adaptive.enabled == "true"`).
     - Verifies UTC session timezone (`spark.sql.session.timeZone == "UTC"`).
     - Verifies `output/spark_event_logs/` directory creation and event logging config.
     - Verifies `--keep-ui` flag support.
   - Test `Dockerfile.spark`:
     - Validates Dockerfile syntax, base image `apache/spark:3.5.1-python3`, user permissions, dependencies (pyarrow, pandas, pytest, etc.).
3. `tests/test_step2_bronze_silver.py`:
   - Validates `lakehouse_storage/silver/tedp_province/`:
     - Static StructType schema (no inferred types).
     - Snappy Parquet format partitioned by `province_name`.
     - Exactly 11 provinces (`Bắc Ninh`, `Cao Bằng`, `Đà Nẵng`, `Gia Lai`, `Hà Nội`, `Hưng Yên`, `Phú Thọ`, `Quảng Bình`, `Thành phố Hồ Chí Minh`, `Thừa Thiên Huế`, `Yên Bái`).
     - Exactly 25,512 continuous hourly timestamps per province (2023-12-11 00:00:00 to 2026-11-07 23:00:00), total exactly 280,632 rows.
     - Zero unexpected null values in `pm25`, `pm10`, `aqi`.
     - Numerical validation: absolute difference of `pm25`, `pm10`, `aqi` vs `data_test/cleaned_tedp_province_4y.csv` is strictly < 1e-5.
4. `tests/test_step3_silver_gold.py`:
   - Validates `lakehouse_storage/gold/global_ann_features/`:
     - Static StructType schema.
     - Contains all 49 atmospheric features specified in `train_global_ann.py`.
     - Sample count: exactly 280,104 rows (full) and exactly 279,840 rows when filtered to `target_time <= 2026-11-06 23:00:00`.
     - Exactly 0 NaNs across all 49 features.
     - Numerical validation: absolute difference vs Pandas `build_global_features()` is strictly < 1e-5.
   - Validates `code/lakehouse/build_lakehouse_spark.py` execution and Data Diff report generation.
5. `tests/test_step4_model_integration.py`:
   - Validates `code/training/common.py`:
     - `load_merged(province_name="Hà Nội", use_parquet=True)` leverages Parquet Partition Pruning (reads only Hà Nội partition files).
     - Fallback mechanism: when Parquet directory is absent or invalid, seamlessly falls back to CSV without error.
   - Validates `code/training/du_bao.py`:
     - Runs forecast inference for a given province reading directly from Silver Parquet via partition pruning.
   - Validates `code/training/train_global_ann.py`:
     - Verified to load features from Parquet with fallback to CSV.
6. `tests/test_step5_benchmark_docker.py`:
   - Validates `output/cloudsim_benchmark/spark_storage_benchmark.csv`:
     - Contains benchmark results for 1x, 10x, and 50x scales.
     - Verifies required metrics columns: `scale`, `framework`, `rows`, `ingestion_sec`, `processing_sec`, `throughput_mb_s`, `peak_memory_mb`, `storage_mb`, `format`.
   - Validates Docker container execution command and image tagging `airquality-spark:3.5.1`.
