# Project: PySpark Air Quality Medallion Lakehouse Migration

## Architecture Overview
The project migrates an air quality processing pipeline from Pandas to Apache Spark 3.5.1 on a Medallion Lakehouse architecture (Bronze -> Silver -> Gold) within a resource-constrained Linux Docker container (8GB RAM, 4 CPUs).
- **Compute**: Apache Spark 3.5.1 (PySpark) with JVM Tungsten execution, PyArrow acceleration, Adaptive Query Execution (AQE), UTC timezone.
- **Storage Layer**: Snappy-compressed Apache Parquet partitioned by `province_name` in `lakehouse_storage/`.
- **Transformation**: 100% Spark SQL and Catalyst Window functions (zero Python `@udf`, zero `applyInPandas`, zero `inferSchema`).
- **Serving / Consumption**: Spatio-Temporal Global ANN model training and inference with Parquet Partition Pruning and automatic CSV fallback.
- **Validation**: Multi-scale performance benchmark (1x, 10x, 50x) and independent programmatic test suite with numerical tolerance < 1e-5.

```
[Raw Sources: TEDP CSV + OpenMeteo CSV]
               │
               ▼
   [Bronze Layer: lakehouse_storage/bronze/]
               │
               ▼ (spark_preprocess_tedp.py & spark_sync_province.py)
   [Silver Layer: lakehouse_storage/silver/]
     ├── tedp_station/ (Cleaned Station Parquet)
     ├── tedp_province/ (280,632 rows x 11 provinces, Snappy Parquet, partitioned by province_name)
     └── weather_province/ (280,632 rows x 11 provinces, Snappy Parquet, partitioned by province_name)
               │
               ▼ (spark_feature_engineering.py)
   [Gold Layer: lakehouse_storage/gold/global_ann_features/]
     └── 49 Atmospheric Features (279,840 / 280,104 samples, Snappy Parquet, partitioned by province_name)
               │
               ├──► [Training: code/training/train_global_ann.py] (Parquet + CSV Fallback)
               └──► [Inference: code/training/du_bao.py] (Partition Pruning by province_name)
```

## Feature Inventory
| # | Feature / Deliverable | Description | Milestone | Source |
|---|----------------------|-------------|-----------|--------|
| 1 | Dockerfile.spark & Container Environment | Spark 3.5.1 container config, 8GB RAM, 4 CPUs, dependencies | Milestone 1 (Step 1) | R1 |
| 2 | spark_session.py Configuration Module | Optimized SparkSession builder (Arrow, AQE, UTC, event logs, --keep-ui) | Milestone 1 (Step 1) | R1 |
| 3 | spark_preprocess_tedp.py (Station Cleaning) | QCVN filter, Catalyst Window linear interpolation (limit=3), median fill | Milestone 2 (Step 2) | R2 |
| 4 | spark_sync_province.py (Province Sync) | Full 25,512h grid x 11 prov (280,632 rows), time interpolation, Silver Parquet | Milestone 2 (Step 2) | R2 |
| 5 | spark_feature_engineering.py (49 Features) | Pure Catalyst Window atmospheric feature extraction, Gold Parquet | Milestone 3 (Step 3) | R3 |
| 6 | build_lakehouse_spark.py (Lakehouse Runner) | End-to-end pipeline coordination + Data Diff report vs Pandas (< 1e-5) | Milestone 3 (Step 3) | R3 |
| 7 | Parquet Partition Pruning & CSV Fallback | common.py, train_global_ann.py, du_bao.py integration | Milestone 4 (Step 4) | R4 |
| 8 | benchmark_spark_vs_pandas.py (Multi-Scale) | 1x, 10x, 50x benchmark, spark_storage_benchmark.csv | Milestone 5 (Step 5) | R5 |
| 9 | Full Docker Container Benchmark Run | End-to-end execution in airquality-spark:3.5.1 container | Milestone 5 (Step 5) | R5 |
| 10 | Independent Automated Test Suite | test_step1 to test_step5 programmatic tests (pytest) | E2E Testing Track | R6 |
| 11 | Review Checkpoint Reports | Checkpoint reports for Steps 1 through 5 in docs/ | E2E Testing Track | R6 |

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|------|-------|-------------|--------|
| M1 | Triển khai Môi trường Docker và Cấu hình SparkSession (Step 1) | `Dockerfile.spark`, `code/lakehouse/spark_session.py`, test environment setup | none | DONE |
| M2 | Chuyển đổi Pipeline Tiền Xử Lý Bronze sang Silver (Step 2) | `code/lakehouse/spark_preprocess_tedp.py`, `code/lakehouse/spark_sync_province.py`, `lakehouse_storage/silver/` | M1 | BLOCKED: AUDIT_VIOLATION (tedp_station partition collision) |
| M3 | Trích Xuất 49 Đặc Trưng Silver sang Gold & Điều Phối (Step 3) | `code/lakehouse/spark_feature_engineering.py`, `code/lakehouse/build_lakehouse_spark.py`, Data Diff verification | M2 | PLANNED |
| M4 | Tích Hợp Tiêu Thụ Parquet vào Huấn Luyện & Dự Báo (Step 4) | `code/training/common.py`, `train_global_ann.py`, `du_bao.py` Partition Pruning + Fallback | M3 | PLANNED |
| M5 | Benchmark Hiệu Năng & Kiểm Thử Toàn Trình trong Docker (Step 5) | `code/lakehouse/benchmark_spark_vs_pandas.py`, multi-scale 1x/10x/50x, Docker execution | M4 | PLANNED |
| E2E | Bộ Kiểm Thử Độc Lập và Báo Cáo Nghiệm Thu Từng Bước (Step 6) | `tests/test_step*.py`, `docs/review_checkpoint_step*.md` verification after each step | Runs in parallel / verifies each step | IN_PROGRESS |

## Code Layout
```
code/
├── lakehouse/
│   ├── spark_session.py              # Centralized SparkSession builder (Arrow, AQE, UTC, event logs, UI)
│   ├── spark_preprocess_tedp.py      # Bronze -> Silver station-level cleaning
│   ├── spark_sync_province.py        # Bronze -> Silver province synchronization (280,632 rows)
│   ├── spark_feature_engineering.py  # Silver -> Gold 49 atmospheric features
│   ├── build_lakehouse_spark.py      # End-to-end Lakehouse pipeline runner + Data Diff
│   └── benchmark_spark_vs_pandas.py  # 1x, 10x, 50x benchmark runner
├── training/
│   ├── common.py                     # Data loader with Parquet Partition Pruning + CSV fallback
│   ├── train_global_ann.py           # Global ANN training with Parquet input
│   └── du_bao.py                     # Rapid single-province inference using Partition Pruning
tests/
├── conftest.py                       # Shared test fixtures & Spark session
├── test_step1_docker_spark.py        # Step 1 verification
├── test_step2_bronze_silver.py       # Step 2 verification (schema, rows, continuity, error < 1e-5)
├── test_step3_silver_gold.py         # Step 3 verification (49 features, samples, error < 1e-5)
├── test_step4_model_integration.py   # Step 4 verification (partition pruning & fallback)
└── test_step5_benchmark_docker.py    # Step 5 verification (benchmark csv & docker)
docs/
├── review_checkpoint_step1.md        # Step 1 review report
├── review_checkpoint_step2.md        # Step 2 review report
├── review_checkpoint_step3.md        # Step 3 review report
├── review_checkpoint_step4.md        # Step 4 review report
└── review_checkpoint_step5.md        # Step 5 review report
Dockerfile.spark                      # Spark 3.5.1 Linux Docker container definition
```

## Interface Contracts

### 1. `spark_session.py` ↔ All Lakehouse Modules
- `get_spark_session(app_name: str, keep_ui: bool = False, master: str = "local[4]") -> SparkSession`
- Standard Spark options:
  - `spark.master`: `local[4]`
  - `spark.driver.memory`: `5g`
  - `spark.sql.shuffle.partitions`: `16`
  - `spark.sql.execution.arrow.pyspark.enabled`: `true`
  - `spark.sql.adaptive.enabled`: `true`
  - `spark.sql.session.timeZone`: `UTC`
  - `spark.sql.parquet.compression.codec`: `snappy`
  - `spark.eventLog.enabled`: `true`
  - `spark.eventLog.dir`: `/workspace/output/spark_event_logs` (or relative path on host)

### 2. Silver Storage Schema (`lakehouse_storage/silver/tedp_province/`)
- Format: Apache Parquet (Snappy compressed)
- Partition key: `province_name` (String)
- Key Columns:
  - `datetime`: TimestampType
  - `province_name`: StringType
  - `station_code`: StringType
  - `station_name`: StringType
  - `station_short_name`: StringType
  - `pm25`: DoubleType
  - `pm10`: DoubleType
  - `aqi`: DoubleType
  - `pm25_gap_hours`: DoubleType / LongType
  - `is_interpolated`: IntegerType / LongType
  - Plus normalized metadata columns. Total rows: exactly 280,632.

### 3. Gold Storage Schema (`lakehouse_storage/gold/global_ann_features/`)
- Format: Apache Parquet (Snappy compressed)
- Partition key: `province_name` (String)
- Columns: 49 atmospheric features + `target_time` (Timestamp) + `y` (Double) + `datetime` (Timestamp).
- Row count: exactly 280,104 rows (full) / 279,840 rows (when filtered to `target_time <= 2026-11-06 23:00:00`).

### 4. Training / Inference Consumer Contract (`common.py`)
- `load_merged(province_name=None, use_parquet=True) -> pd.DataFrame`
  - If `use_parquet=True` and `lakehouse_storage/silver/tedp_province` exists:
    Read via PyArrow with partition filter `[("province_name", "==", province_name)]` if `province_name` is provided.
  - If `use_parquet=False` or Parquet directory not found:
    Fallback gracefully to `data_test/cleaned_tedp_province_4y.csv`.

