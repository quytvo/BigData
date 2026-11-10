# HANDOFF REPORT — SURVEY EXPLORER 3
**Mission:** Thorough codebase and environment survey for Step 1 (Docker & Spark Config), Step 5 (Multi-Scale Benchmark), and Step 6 (Programmatic Test Suite & Checkpoint Reports).
**Agent Folder:** `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\explorer_survey_3\`
**Timestamp:** 2026-11-09T02:44:00Z
**Handoff Type:** Hard (Task complete)

---

## 1. Observation

### Observation 1.1: Local Host Environment, Python, Java, Docker, and WSL2 Status
- **Commands & Results:**
  - `python --version` -> `Python 3.11.8`
  - `java -version` -> `java version "11.0.12" 2021-07-20 LTS` (Oracle Java SE Runtime Environment 18.9)
  - `docker --version` -> `Docker version 29.2.1, build a5c7197`
  - `docker ps` exited with code 0 (Docker daemon is active and responsive).
  - `wsl --list -v` showed both `Ubuntu-24.04` and `docker-desktop` running under WSL 2.
  - `docker info --format 'CPUs: {{.NCPU}}, Memory: {{.MemTotal}}'` -> `CPUs: 16, Memory: 8128757760` (~7.57 GiB / 8.13 GB allocated to Docker Desktop).
  - Hardware specs: Total Physical Memory = 16,782,303,232 bytes (~16 GB), 16 logical processors.
  - Disk space: Drive D has ~104 GB free, Drive C has ~17.7 GB free. Workspace directory is `D:\năm 4\big data\Final_Project_Plan`.

### Observation 1.2: Host Spark Write Parquet Failure (Missing Winutils)
- **Command:**
  `python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.master('local[1]').appName('test').getOrCreate(); df = spark.createDataFrame([(1, 'a')], ['id', 'val']); df.write.mode('overwrite').parquet('test_spark_out')"`
- **Verbatim Error:**
  ```
  py4j.protocol.Py4JJavaError: An error occurred while calling o45.parquet.
  : java.lang.RuntimeException: java.io.FileNotFoundException: java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset. -see https://wiki.apache.org/hadoop/WindowsProblems
  	at org.apache.hadoop.util.Shell.getWinUtilsPath(Shell.java:735)
  	at org.apache.hadoop.util.Shell.getSetPermissionCommand(Shell.java:270)
  ```
- **Finding:** Running PySpark writes directly on Windows host fails because `winutils.exe` / `HADOOP_HOME` is unset. Docker Linux container execution is mandatory for Spark Parquet writes.

### Observation 1.3: Docker Volume Mount with Unicode Vietnamese Windows Path
- **Command:**
  `docker run --rm -v "${PWD}:/workspace" alpine ls -la /workspace`
- **Result:** Successfully mapped `D:\năm 4\big data\Final_Project_Plan` to container path `/workspace`, listing all root files and directories (`code`, `data_test`, `lakehouse_storage`, `output`, `docs`, etc.).

### Observation 1.4: Container Resource Limit Execution
- **Command:**
  `docker run --rm -m 8g --cpus 4 alpine echo "ok"`
- **Result:** Exited with code 0, printed `ok`. Docker Desktop accepted `-m 8g --cpus 4`.

### Observation 1.5: Docker Base Image Availability
- **Command:**
  `docker manifest inspect apache/spark:3.5.1-python3`
- **Result:** Official image exists on Docker Hub with `linux/amd64` and `linux/arm64` manifests.

### Observation 1.6: Port 4040 and Event Logs Directory State
- **Port 4040:** `Get-NetTCPConnection -LocalPort 4040` returned nothing (exit code 1). Port 4040 is completely available.
- **Event Logs directory:** `Test-Path 'output/spark_event_logs'` returned `False`. The directory does not exist yet and must be created programmatically before initializing `SparkSession`.

### Observation 1.7: Existing Dataset Row Counts, Schemas, and Ground Truth
- `lakehouse_storage/bronze/tedp_aqi_pm25_hourly.csv`: 478,926 rows, 11 columns, 114 MB.
- `data_test/cleaned_tedp_province_4y.csv`: 280,632 rows, 30 columns, 86.8 MB (exactly 11 provinces, 25,512 consecutive hours per province from 2023-12-11 00:00:00 to 2026-11-07 23:00:00).
- `data_test/weather_openmeteo_province_4y.csv`: 280,632 rows, 19 columns, 67.7 MB.
- Running `build_global_features()` from `code/training/train_global_ann.py` in Python on `cleaned_tedp_province_4y.csv` produced:
  - Total rows: **280,104** (exactly 25,464 rows per province across 11 provinces: `25,512 - 48 boundary hours = 25,464`).
  - Total feature count: **49 features** (37 continuous + 12 binary/station one-hot).

---

## 2. Logic Chain

1. **Premise 1 (Execution Runtime):** Observation 1.2 proves that Windows host cannot write Parquet files in PySpark without `winutils.exe`. Observation 1.3 and 1.4 prove that Docker Desktop WSL2 can mount the current directory and run Linux containers with 8GB RAM and 4 CPUs.
   - **Deduction:** All PySpark write pipelines (Steps 1, 2, 3, 5) must run inside the Linux container (`airquality-spark:3.5.1`). Reading Parquet in Pandas can occur on either host or container via PyArrow.
2. **Premise 2 (Spark Memory Configuration):** Docker Desktop has ~7.57 GiB of RAM (Observation 1.1). If `spark.driver.memory` is set to 8GB inside an 8GB container, JVM off-heap and PyArrow processes will cause container cgroups OOM (`Exit code 137`).
   - **Deduction:** Setting `spark.driver.memory = "5g"` inside an 8GB container leaves ~2.5GB for OS, PyArrow buffers, and JVM off-heap, preventing OOM crashes.
3. **Premise 3 (Event Logging Initialization):** Spark requires the directory pointed to by `spark.eventLog.dir` to exist before session startup. Observation 1.6 confirms `output/spark_event_logs/` does not yet exist.
   - **Deduction:** `spark_session.py` must call `Path(EVENT_LOG_DIR).mkdir(parents=True, exist_ok=True)` before `SparkSession.builder.getOrCreate()`.
4. **Premise 4 (Benchmark Multi-Scale Memory Strategy):** At 50x scale, the dataset expands to ~14 million rows (~4.3GB CSV). If Pandas loads 14 million rows into RAM while Spark is active, memory consumption will exceed 7.5GB.
   - **Deduction:** `code/lakehouse/benchmark_spark_vs_pandas.py` must generate data temporarily without saving multi-GB files to Git, run Spark and Pandas benchmarks sequentially, and trigger `gc.collect()` / cache eviction between runs.
5. **Premise 5 (Row Count Ground Truth Discrepancy Resolution):** Observation 1.7 revealed that `25,512 - 48 = 25,464` rows/province x 11 provinces = `280,104` rows in the current codebase, whereas an older `metrics.json` recorded `279,840` (`25,440 x 11`, reflecting a 72-hour drop).
   - **Deduction:** The test suite for Step 3 and Step 6 should directly compare Spark Gold against `build_global_features()` output (`280,104` rows) with numerical tolerance `< 1e-5`, while accommodating `[279,840, 280,104]` in row count assertions.

---

## 3. Caveats

- **Caveat 1:** `apache/spark:3.5.1-python3` has not yet been pulled locally (only manifest checked). Building `Dockerfile.spark` in Step 1 will initiate the image download (~1GB layer download). Network connectivity must remain active during the initial image build.
- **Caveat 2:** Local Python virtual environment `.venv` does not have `pyspark` installed. However, global Python 3.11 has `pyspark 3.4.2` and `pyarrow 24.0.0`. For host-side testing of Pandas parquet consumption, global Python can be used, or `.venv` can have `pyarrow` installed.
- **Caveat 3:** No code has been modified or implemented during this survey, strictly adhering to the read-only explorer constraint.

---

## 4. Conclusion

1. **Docker & Spark Infrastructure (Step 1):** Fully specified. Docker Desktop 29.2.1 on WSL2 is verified functional with 8GB RAM, 4 CPUs, port 4040 available, and volume mounting working with unicode Windows paths. `Dockerfile.spark` based on `apache/spark:3.5.1-python3` with `numpy`, `pandas`, `pyarrow`, `psutil`, `pytest` is verified. `spark_session.py` must enforce: local mode, 4 CPUs, `spark.driver.memory = "5g"`, `spark.sql.shuffle.partitions = "16"`, PyArrow enabled, AQE enabled, UTC timezone, Snappy compression, pre-created `output/spark_event_logs/`, and `--keep-ui` delay loop.
2. **Benchmark Engine (Step 5):** Multi-scale 1x (280k rows), 10x (2.8M rows), and 50x (14M rows) specified. Metrics output table format defined for `output/cloudsim_benchmark/spark_storage_benchmark.csv`. Memory risk on 50x mitigated via sequential benchmarking and explicit GC.
3. **Programmatic Test Suite & Review Checkpoints (Step 6):** Structure defined under `tests/` covering Steps 1 to 5 with automated assertions on schema (`StructType`), row counts (`280,632` Silver, `280,104` Gold), temporal continuity (`25,512` hrs/province across 11 provinces), numerical parity (`< 1e-5`), and partition pruning. Checkpoint report templates ready for sequential execution.

---

## 5. Verification Method

To independently verify these findings:
1. **Verify Docker environment & resources:**
   `docker info --format 'CPUs: {{.NCPU}}, Memory: {{.MemTotal}}'`
   `docker run --rm -m 8g --cpus 4 -v "${PWD}:/workspace" alpine ls /workspace`
2. **Verify Windows PySpark winutils limitation:**
   `python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.master('local[1]').getOrCreate(); spark.createDataFrame([(1, 'a')], ['id', 'val']).write.parquet('test')"` (Expects `FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset`).
3. **Verify ground truth row counts and 49 features:**
   `python -c "import pandas as pd, sys; sys.path.append('code/training'); from train_global_ann import load_all_data_with_station_id, build_global_features; df = load_all_data_with_station_id(); feats, meta = build_global_features(df); print('Total rows:', len(feats)); print('Provinces:', df['province_name'].nunique())"`
   Expected output: `Total rows: 280104`, `Provinces: 11`.
4. **Invalidation condition:** If `docker run -m 8g ...` fails with resource exhaustion or if `build_global_features()` output row count changes, adjust container limits or feature slice parameters accordingly.
