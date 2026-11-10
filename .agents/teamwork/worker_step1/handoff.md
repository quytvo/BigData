# HANDOFF REPORT — WORKER STEP 1

**Task:** Triển khai Môi trường Docker và Cấu hình SparkSession (Step 1 Deliverables)
**Author:** Worker Step 1
**Timestamp:** 2026-11-09T02:56:00Z
**Status:** COMPLETE (HARD HANDOFF)

---

## 1. Observation

1. **Host Environment & Docker Runtime:**
   - Host OS: Windows 11 x86_64, 16GB RAM, Docker Desktop 29.2.1 running on WSL2 backend.
   - Project directory: `D:\năm 4\big data\Final_Project_Plan`.
   - Mount test: `docker run --rm -v "${PWD}:/workspace" alpine ls -la /workspace` succeeded, resolving all Unicode paths into standard POSIX `/workspace`.

2. **Windows Hadoop Limitation with Event Logs:**
   - Attempting to initialize Spark with `spark.eventLog.enabled=true` on bare Windows host without `winutils.exe` fails with:
     ```
     py4j.protocol.Py4JJavaError: An error occurred while calling o45.parquet.
     : java.lang.RuntimeException: java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset.
       at org.apache.hadoop.util.Shell.getWinUtilsPath(Shell.java:735)
     ```
   - In contrast, inside Linux Docker container (`airquality-spark:3.5.1`), native POSIX file permissions are available, allowing `spark.eventLog.enabled=true` and Parquet writing to succeed with 0 errors.

3. **Deliverables Implemented:**
   - `Dockerfile.spark` (and `code/lakehouse/Dockerfile.spark`):
     - Base image: `apache/spark:3.5.1-python3`.
     - Packages installed: `numpy`, `pandas`, `pyarrow`, `psutil`, `pytest`, `torch` (via CPU index `--extra-index-url https://download.pytorch.org/whl/cpu`).
     - Configured `SPARK_HOME=/opt/spark`, `PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/workspace"`, `WORKDIR /workspace`, `EXPOSE 4040`.
   - `code/lakehouse/spark_session.py`:
     - Function: `get_spark_session(app_name="AirQualityLakehouse", keep_ui=False, master="local[4]") -> SparkSession`.
     - Configurations enforced:
       - `spark.master`: `local[4]`
       - `spark.driver.memory`: `"5g"`
       - `spark.sql.shuffle.partitions`: `"16"`
       - `spark.sql.execution.arrow.pyspark.enabled`: `"true"`
       - `spark.sql.adaptive.enabled`: `"true"`
       - `spark.sql.session.timeZone`: `"UTC"`
       - `spark.sql.parquet.compression.codec`: `"snappy"`
       - `spark.eventLog.enabled`: `"true"` (inside Docker/POSIX, auto-warns and disables on Windows host without winutils to prevent JVM crashes)
       - `spark.eventLog.dir`: `file:///workspace/output/spark_event_logs`
       - Directory `output/spark_event_logs/` is created via `Path.mkdir(parents=True, exist_ok=True)` *before* `builder.getOrCreate()`.
     - Active wait loop: `wait_for_webui(spark, keep_ui_seconds)` and CLI support for `--keep-ui` and `--keep-ui-seconds`.

4. **Test & Verification Outputs:**
   - Image build command: `docker build -t airquality-spark:3.5.1 -f Dockerfile.spark .` -> Exit code 0.
   - In-container execution command: `docker run --rm -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 python3 code/lakehouse/spark_session.py` -> Exit code 0. Output verified:
     ```
     spark.master                                  : local[4]
     spark.app.name                                : AirQualityLakehouse
     spark.driver.memory                           : 5g
     spark.sql.shuffle.partitions                  : 16
     spark.sql.execution.arrow.pyspark.enabled     : true
     spark.sql.adaptive.enabled                    : true
     spark.sql.session.timeZone                    : UTC
     spark.sql.parquet.compression.codec           : snappy
     spark.eventLog.enabled                        : true
     spark.eventLog.dir                            : file:///workspace/output/spark_event_logs
     spark.ui.port                                 : 4040
     ```
   - Event log file verification: `output/spark_event_logs/local-1794192842184` generated with 81,004 bytes.
   - In-container test suite: `docker run --rm -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 pytest tests/test_step1_docker_spark.py`:
     ```
     tests/test_step1_docker_spark.py .........                               [100%]
     ============================== 9 passed in 4.51s ===============================
     ```
   - Host test suite: `python tests/test_step1_docker_spark.py` -> 9 tests passed in 6.077s (OK).

---

## 2. Logic Chain

1. **Requirement R1 & Interface Contracts:**
   - Requirement mandates Apache Spark 3.5.1 on Python 3 with specific packages (`numpy`, `pandas`, `pyarrow`, `psutil`, `pytest`, `torch`), 4040 Web UI, and 8GB RAM / 4 CPU configuration constraints.
   - By creating `Dockerfile.spark` based on `apache/spark:3.5.1-python3`, upgrading pip, and pulling dependencies including CPU PyTorch, the image builds cleanly without downloading unnecessary GPU layers (~200MB vs >900MB).
2. **Environment Variable Configuration:**
   - `apache/spark:3.5.1-python3` bundles `pyspark` inside `/opt/spark/python` and `/opt/spark/python/lib/py4j-0.10.9.7-src.zip`. Setting `PYTHONPATH` in the image allows running both `spark-submit` and direct `python3` invocations without missing module errors.
3. **Deterministic SparkSession Tuning:**
   - In `code/lakehouse/spark_session.py`, `local[4]` matches the container CPU allocation (4 cores).
   - Setting `spark.driver.memory = "5g"` leaves 3GB RAM for OS, PyArrow buffers, and JVM off-heap in the 8GB container.
   - Setting `spark.sql.shuffle.partitions = "16"` (4 per core) prevents the 200 default partitions from causing thousands of tiny task overheads.
   - Setting `spark.sql.session.timeZone = "UTC"` eliminates the 7-hour offset between ERA5 UTC weather data and Vietnam local CEM time.
   - Ensuring `output/spark_event_logs` directory exists prior to `builder.getOrCreate()` prevents JVM file not found crashes.
4. **Platform Compatibility & Host Dev Fallback:**
   - When run in Linux Docker, `_is_winutils_available()` evaluates to True, and event logging is enabled.
   - When imported on Windows host without winutils, it emits a clean warning and sets event logging to false rather than crashing the JVM, allowing tests to run smoothly on both host and Docker.
5. **Programmatic Verification:**
   - 9 unit tests in `tests/test_step1_docker_spark.py` exercise Dockerfile specifications, SparkSession configurations, event log directory creation, and UI wait loop functionality. All 9 pass in both environments.

---

## 3. Caveats

- On Windows host, write operations that go through Hadoop's local filesystem abstraction (e.g., event logs and `DataFrame.write.parquet`) require `winutils.exe`. As designed by the architecture, all heavy Spark pipeline runs (Step 2, Step 3, Step 5) MUST be executed inside the `airquality-spark:3.5.1` Docker container. Reading Parquet from Windows using Pandas / PyArrow works natively without any issues.
- No other caveats.

---

## 4. Conclusion

Step 1 deliverables are 100% complete and verified:
- `Dockerfile.spark` is built and verified as `airquality-spark:3.5.1`.
- `code/lakehouse/spark_session.py` satisfies all interface contracts and performance configurations.
- `tests/test_step1_docker_spark.py` passes 9/9 tests in container and host.
- `docs/review_checkpoint_step1.md` is published with full execution logs.
- The pipeline is fully ready for Step 2 (Bronze to Silver preprocessing).

---

## 5. Verification Method

To independently verify Step 1:

1. **Verify Dockerfile and Image Build:**
   ```powershell
   docker build -t airquality-spark:3.5.1 -f Dockerfile.spark .
   ```
   *Expected:* Exit code 0, successfully builds image.

2. **Verify SparkSession Initialization in Docker:**
   ```powershell
   docker run --rm -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 python3 code/lakehouse/spark_session.py
   ```
   *Expected:* Prints SPARK CONFIGURATION SUMMARY with `master=local[4]`, `driver.memory=5g`, `shuffle.partitions=16`, `arrow=true`, `adaptive=true`, `timeZone=UTC`, `codec=snappy`, `eventLog.enabled=true`.

3. **Verify Independent Test Suite in Docker:**
   ```powershell
   docker run --rm -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 pytest tests/test_step1_docker_spark.py
   ```
   *Expected:* `9 passed in ~4.5s`.

4. **Verify Event Log Generation:**
   ```powershell
   Get-ChildItem -Path output\spark_event_logs
   ```
   *Expected:* Event log files with size > 0 bytes present in `output/spark_event_logs/`.

5. **Invalidation Conditions:**
   - Any test failure in `tests/test_step1_docker_spark.py`.
   - Event log directory not created automatically before SparkSession initialization.
   - Docker build failure or missing dependencies.
