# Adversarial Challenge Report — Step 1: Docker & SparkSession Configuration

**Agent**: Challenger 2 (Empirical Challenger)  
**Target**: Milestone 1 (Step 1 Deliverables)  
**Overall Risk Assessment**: LOW  
**Verdict**: **APPROVE** (with 1 non-blocking CLI lifecycle finding and concrete mitigation)

---

## 1. Executive Summary

As an Empirical Challenger, I conducted adversarial stress tests directly on host (Windows 11, Python 3.11, Spark 3.4.2) and inside the official project Docker container (`airquality-spark:3.5.1`, Linux Ubuntu 22.04, Python 3.8, Spark 3.5.1, 8GB RAM, 4 CPUs).

All empirical tests were executed directly via tool invocations:
1. **Pytest Suite**: `pytest tests/test_step1_docker_spark.py -v` passed **9/9 tests** on host (6.12s) and inside Docker container (4.29s).
2. **Driver Memory Reality Check**: Verified via Py4J `Runtime.getRuntime().maxMemory()`. Confirmed the JVM heap is allocated **exactly 5.0 GB (5,368,709,120 bytes)**.
3. **Web UI Port 4040 Empirical Network Probe**: Started Docker container in background exposing port 4040. HTTP GET to `http://localhost:4040/jobs/` returned HTTP 200 OK; REST endpoint `/api/v1/applications` returned active JSON metadata.
4. **Web UI Port Conflict Stress Test**: Pre-bound port 4040 with a raw Python socket. Spark gracefully degraded and bound to port 4041 without crashing.
5. **Event Log Generation & Inspection**: Inspected event logs in `output/spark_event_logs/`. Verified JSON lines beginning with `SparkListenerLogStart`, `SparkListenerEnvironmentUpdate`, `SparkListenerApplicationStart`, and `SparkListenerApplicationEnd`.
6. **Container OOM Headroom Test**: Ran DataFrame operations and PyArrow conversions in a constrained `-m 6g --cpus 4` container; succeeded without triggering cgroup OOM.
7. **Empirical Defect Uncovered**: Found that running `spark_session.py` via CLI with `--keep-ui` or `--keep-ui-seconds` skips `spark.stop()` due to placement inside the `else` branch, leaving `<app_id>.inprogress` event logs unclosed. Non-blocking for Lakehouse pipelines (which import `get_spark_session()` as a module), with trivial fix provided.

---

## 2. Empirical Challenges & Stress Tests

### [Medium] Challenge 1: Event Log `.inprogress` Abandonment on `--keep-ui` CLI Exit
- **Assumption Challenged**: When a user or test runs `code/lakehouse/spark_session.py --keep-ui` or `--keep-ui-seconds N`, Spark will cleanly close the session and finalize the event log file.
- **Empirical Attack Scenario**:
  Executed:
  ```powershell
  docker run --rm -d -p 4040:4040 -v "${PWD}:/workspace" -w /workspace --name test_spark_ui airquality-spark:3.5.1 python3 code/lakehouse/spark_session.py --keep-ui-seconds 15
  ```
  Waited 15 seconds for timer expiration. Checked `output/spark_event_logs/`.
- **Observed Behavior**:
  The log file remained named `local-1794193344713.inprogress` (size 80,938 bytes) indefinitely!
  Inspected `code/lakehouse/spark_session.py` lines 269–276:
  ```python
  if args.keep_ui or args.keep_ui_seconds > 0:
      wait_for_webui(spark, keep_ui_seconds=args.keep_ui_seconds)
  else:
      logger.info("Shutting down SparkSession.")
      spark.stop()
      logger.info("Done.")
  ```
  Because `spark.stop()` is placed inside the `else:` branch, when `args.keep_ui` or `args.keep_ui_seconds > 0` is True, `spark.stop()` is **NEVER** called! The script exits `main()`, causing the JVM to exit abruptly without renaming `.inprogress` to completed.
- **Blast Radius**:
  Low to Medium for CLI usage. Zero blast radius for downstream pipelines (Step 2 to Step 5) because they import `get_spark_session()` as a library and manage `spark.stop()` independently.
- **Mitigation**:
  Wrap the UI hold in a `try...finally` block in `main()`:
  ```python
  try:
      if args.keep_ui or args.keep_ui_seconds > 0:
          wait_for_webui(spark, keep_ui_seconds=args.keep_ui_seconds)
  finally:
      logger.info("Shutting down SparkSession.")
      spark.stop()
      logger.info("Done.")
  ```

---

### [Low] Challenge 2: Driver Memory 5g — JVM Heap Max Allocation Verification
- **Assumption Challenged**: Does `.config("spark.driver.memory", "5g")` actually configure the underlying JVM `-Xmx` heap when launched from PySpark Python builder, or does it only set a SparkConf metadata string?
- **Empirical Attack Scenario**:
  Executed inline inspection via Py4J Java Runtime:
  ```python
  sc = spark.sparkContext
  jvm = sc._jvm
  max_mem = jvm.java.lang.Runtime.getRuntime().maxMemory()
  ```
- **Observed Results**:
  - Windows Host:
    - `spark.conf.get("spark.driver.memory")`: `5g`
    - `Runtime.getRuntime().maxMemory()`: `5,368,709,120 bytes` (**5.0 GB**)
  - Docker Container (`airquality-spark:3.5.1`):
    - `spark.conf.get("spark.driver.memory")`: `5g`
    - `Runtime.getRuntime().maxMemory()`: `5,368,709,120 bytes` (**5.0 GB**)
- **Verdict**: Fully verified. JVM max heap matches configuration exactly.

---

### [Low] Challenge 3: Spark Web UI Port 4040 Real HTTP Availability & Conflict
- **Assumption Challenged**: Is port 4040 actually bound and serving HTTP traffic? What happens if port 4040 is already occupied?
- **Empirical Attack Scenario**:
  1. Ran container with `-p 4040:4040` and sent HTTP GET requests to `http://localhost:4040/jobs/` and `http://localhost:4040/api/v1/applications`.
  2. Pre-bound port 4040 using a raw Python TCP socket listener and started a new `SparkSession`.
- **Observed Results**:
  1. `http://localhost:4040/jobs/` returned HTTP 200 OK.
  2. `http://localhost:4040/api/v1/applications` returned active JSON containing `appSparkVersion: 3.5.1`, `sparkUser: root`, `name: AirQualityLakehouse`.
  3. Under port conflict, Spark logged:
     `WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.`
     Successfully bound to `http://localhost:4041` without crashing.
- **Verdict**: Fully verified and resilient.

---

### [Low] Challenge 4: Memory Headroom under 6GB Container Constraint
- **Assumption Challenged**: Requirement R1 mentions "Docker 6GB–8GB RAM". If a host runs the container with `-m 6g`, will 5GB driver memory leave enough headroom (1GB) for OS, JVM off-heap, and PyArrow buffers, or trigger Docker cgroup OOM killer?
- **Empirical Attack Scenario**:
  Ran:
  ```powershell
  docker run --rm -m 6g --cpus 4 -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 python3 -c "
  import sys; from pathlib import Path; sys.path.insert(0, str(Path('code').resolve()))
  from lakehouse.spark_session import get_spark_session
  spark = get_spark_session(app_name='6gContainerTest')
  df = spark.range(0, 1000000).toDF('id')
  print('Count:', df.count())
  pdf = df.limit(10000).toPandas()
  print('toPandas shape:', pdf.shape)
  spark.stop()
  "
  ```
- **Observed Results**:
  Successfully completed in 8.3s without OOM kill. Count = 1,000,000, toPandas shape = (10,000, 1).
- **Verdict**: Survives 6GB container limit. However, for large workloads (Step 2 and Step 5), `-m 8g` remains strongly recommended.

---

## 3. Stress Test Matrix

| # | Test Scenario | Command / Method | Expected | Actual | Status |
|:---:|:---|:---|:---|:---|:---:|
| 1 | Pytest Unit Tests (Host) | `pytest tests/test_step1_docker_spark.py -v` | 9 passed | 9 passed in 6.12s | **PASS** |
| 2 | Pytest Unit Tests (Docker) | `docker run ... airquality-spark:3.5.1 pytest tests/test_step1_docker_spark.py -v` | 9 passed | 9 passed in 4.29s | **PASS** |
| 3 | JVM Heap Allocation | Py4J `Runtime.getRuntime().maxMemory()` | 5.0 GB | 5,368,709,120 bytes (5.0 GB) | **PASS** |
| 4 | Web UI HTTP & REST | GET `http://localhost:4040/jobs/` & `/api/v1/applications` | 200 OK + JSON | 200 OK + Valid App JSON | **PASS** |
| 5 | Web UI Port Collision | Socket bind 4040, initialize SparkSession | Retries 4041 | Bound port 4041 with WARN | **PASS** |
| 6 | Event Log Content | Inspect `output/spark_event_logs/local-*` | Valid JSON events | Complete lifecycle events present | **PASS** |
| 7 | Event Log Dir Auto-create | Delete target dir, invoke `get_event_log_dir` | Directory created | Created before SparkSession init | **PASS** |
| 8 | CLI `--keep-ui` Clean Stop | Run with `--keep-ui-seconds 15`, check log status | Renamed from `.inprogress` | Remained `.inprogress` (Defect found) | **WARN / DEFECT** |
| 9 | Explicit `spark.stop()` | Run script with `spark.stop()`, check log status | Renamed to completed | Final file created (80,995 bytes) | **PASS** |
| 10 | 6GB Container Stress | `docker run -m 6g ...` with 1M rows + PyArrow | No OOM kill | Completed successfully (8.3s) | **PASS** |

---

## 4. Unchallenged Areas

- **Step 2 through Step 5 Data Pipelines**: Out of scope for Milestone 1; will be stress-tested during subsequent milestone gates.
- **Physical Multi-Node Cluster**: Out of scope per PROJECT.md design (`local[4]` standalone in single container).

---

## 5. Final Verdict & Actionable Recommendation

**Verdict**: **APPROVE**

**Recommendation**:
Step 1 deliverables (`Dockerfile.spark`, `code/lakehouse/spark_session.py`, `tests/test_step1_docker_spark.py`) meet all functional, architectural, and testability requirements. The project can safely proceed to Step 2.
During Step 2 or routine maintenance, the team should update lines 269-276 of `code/lakehouse/spark_session.py` to wrap `wait_for_webui` in a `try...finally` block so CLI runs cleanly invoke `spark.stop()`.
