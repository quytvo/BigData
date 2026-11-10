# E2E Test Infra: PySpark Air Quality Medallion Lakehouse

## Test Philosophy
- Requirement-driven, programmatic automated verification.
- Test suite located under `tests/` executable via `pytest`.
- Verifies schema integrity (`StructType`), row counts, continuous time series (25,512h), numerical tolerance (< 1e-5), Partition Pruning, and Docker container execution without OOM.
- Each milestone (Step 1 to Step 5) is verified by its dedicated test module and accompanied by a detailed Review Checkpoint report in `docs/review_checkpoint_step*.md`.

## Feature & Milestone Test Mapping
| Milestone | Test Module | Verification Focus | Checkpoint Report |
|-----------|-------------|-------------------|-------------------|
| Milestone 1 (Step 1) | `tests/test_step1_docker_spark.py` | SparkSession configs (Arrow, AQE, UTC, event logs, 4 CPUs, 5GB driver memory, --keep-ui, Dockerfile) | `docs/review_checkpoint_step1.md` |
| Milestone 2 (Step 2) | `tests/test_step2_bronze_silver.py` | Silver schema (StructType), 280,632 rows, 11 provinces, 25,512 continuous hrs, Snappy Parquet partition, abs error vs Pandas < 1e-5 | `docs/review_checkpoint_step2.md` |
| Milestone 3 (Step 3) | `tests/test_step3_silver_gold.py` | 49 atmospheric features, 279,840/280,104 samples, zero NaNs, abs error vs Pandas < 1e-5, build_lakehouse_spark.py runner | `docs/review_checkpoint_step3.md` |
| Milestone 4 (Step 4) | `tests/test_step4_model_integration.py` | Partition Pruning on Silver Parquet, automatic CSV fallback, du_bao.py and train_global_ann.py execution | `docs/review_checkpoint_step4.md` |
| Milestone 5 (Step 5) | `tests/test_step5_benchmark_docker.py` | Multi-scale 1x/10x/50x benchmark in Docker container (8GB RAM), spark_storage_benchmark.csv, event logs | `docs/review_checkpoint_step5.md` |

## Test Suite Execution
- **Host / Container Command**: `pytest -v tests/`
- **Single Step Command**: `pytest -v tests/test_step1_docker_spark.py`
- **Docker Full Run**: `docker run --rm -m 8g --cpus 4 -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest -v tests/`

