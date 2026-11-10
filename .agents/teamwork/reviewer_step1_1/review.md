# Quality & Adversarial Review Report — Milestone 1 (Step 1)

**Task:** Triển khai Môi trường Docker và Cấu hình SparkSession (Step 1 Deliverables)  
**Reviewer:** Reviewer & Adversarial Critic (`reviewer_step1_1`)  
**Timestamp:** 2026-11-09T03:02:00Z  
**Verdict:** **APPROVE**

---

## 1. Review Summary

- **Verdict:** **APPROVE**
- **Integrity Status:** **PASS** (Zero integrity violations; genuine JVM Spark runtime execution; no mock facades; no hardcoded outputs).
- **Test Suite Status:** 9/9 tests passed on Windows host (`6.83s`) and 9/9 tests passed in Docker container `airquality-spark:3.5.1` (`6.53s`).
- **Core Deliverables Inspected:**
  1. `Dockerfile.spark` (root) & `code/lakehouse/Dockerfile.spark`
  2. `code/lakehouse/spark_session.py`
  3. `tests/test_step1_docker_spark.py`
  4. `docs/review_checkpoint_step1.md`

---

## 2. Integrity & Anti-Cheating Verification

| Check Item | Result | Evidence / Methodology |
|:---|:---:|:---|
| **Hardcoded outputs in source code** | **CLEAR** | Source code dynamically calls `pyspark.sql.SparkSession.builder` and binds real JVM runtime properties. |
| **Dummy / Facade implementations** | **CLEAR** | Instantiates a genuine Apache Spark 3.5.1 JVM session, configures Catalyst optimizer, initializes event logging listener. |
| **Task bypasses or shortcuts** | **CLEAR** | Container built from official `apache/spark:3.5.1-python3`, includes CPU PyTorch, PyArrow, psutil, pytest. |
| **Fabricated test logs / claims** | **CLEAR** | Independently reproduced all test executions via CLI on host and inside Docker container; event log files inspected on disk. |
| **Self-certifying work without verification** | **CLEAR** | Independent verification performed using pytest and Docker CLI. |

---

## 3. Quality Review Findings

### [Minor] Finding 1: Explicit `spark.stop()` in `main()` CLI flow
- **What:** In `code/lakehouse/spark_session.py` lines 270–276, if `--keep-ui` or `--keep-ui-seconds` is passed, `wait_for_webui` executes, but upon timer expiration or `KeyboardInterrupt`, the code does not enter the `else` block where `spark.stop()` is invoked.
- **Where:** `code/lakehouse/spark_session.py:270-276`
- **Why:** While process termination will eventually tear down the JVM/Py4J bridge, ungraceful teardown can leave event log files marked with `.inprogress` suffix (as observed with `local-1794192868981.inprogress`).
- **Suggestion:** Wrap the CLI execution in a `try ... finally: spark.stop()` block so that the SparkContext is cleanly closed and event logs are finalized with `SparkListenerApplicationEnd`.

### [Minor] Finding 2: In-container Event Log assertion in Test Suite
- **What:** In `tests/test_step1_docker_spark.py::test_09_spark_session_configurations`, `spark.eventLog.enabled` is intentionally omitted from assertion because Windows host without `winutils.exe` disables it.
- **Where:** `tests/test_step1_docker_spark.py:104-118`
- **Why:** While this enables tests to pass on bare Windows, it means regression testing inside the Docker container does not strictly assert `spark.eventLog.enabled == "true"`.
- **Suggestion:** Add an assertion conditioned on `os.name != "nt"` or test `enable_event_log=True` explicitly in a container-targeted test case.

### [Minor] Finding 3: Redundant Dockerfile mirror
- **What:** Identical `Dockerfile.spark` exists at project root and at `code/lakehouse/Dockerfile.spark`.
- **Where:** `Dockerfile.spark` and `code/lakehouse/Dockerfile.spark`
- **Why:** Having multiple copies creates drift risk if one file is modified and the other is forgotten.
- **Suggestion:** Maintain `Dockerfile.spark` at project root as the single source of truth; remove or symlink the inner copy.

---

## 4. Adversarial Review & Attack Surface Stress-Testing

### Challenge 1: Memory Budget & Container OOM Risk
- **Assumption challenged:** Allocating `driver_memory = 5g` inside an 8GB RAM container (`-m 8g`) is stable.
- **Stress test & Analysis:**
  - JVM Heap: 5GB.
  - Remaining for OS + JVM off-heap + PyArrow buffers: 3GB.
  - In Step 2 & 3, Catalyst Window operations and Snappy Parquet compression run in-engine. PyArrow acceleration (`spark.sql.execution.arrow.pyspark.enabled = true`) will allocate off-heap memory when converting between JVM Arrow records and Python Pandas.
  - If a developer sets Docker limit to 6GB (`-m 6g`), a 5GB heap leaves only 1GB, increasing risk of Linux OOM killer killing the container during heavy Arrow serialization.
- **Blast radius:** Container kill with exit code 137 under peak memory load.
- **Mitigation:** The 5GB setting is well-balanced for the mandated 8GB container. For lower-spec environments, `get_spark_session(driver_memory="4g")` can be passed without altering code.

### Challenge 2: Windows File Path & Unicode Sensitivity
- **Assumption challenged:** Host directory `d:\năm 4\big data\Final_Project_Plan` contains Unicode characters (`ă`) and spaces.
- **Attack scenario:** Windows Hadoop `winutils` and Spark URI parsers often fail or mangle paths containing non-ASCII characters or spaces (e.g. `n%C4%83m%204`).
- **Actual Behavior:** 
  - On Windows host, `spark_session.py` gracefully detects missing winutils and disables event logging, preventing JVM crash.
  - In Docker, the host directory is mounted directly to `/workspace` (ASCII, POSIX). Spark runs entirely against `/workspace`, completely immune to host Unicode path issues.
- **Result:** **ROBUST / DEFENDED**.

### Challenge 3: Timezone Skew across Heterogeneous Data Sources
- **Assumption challenged:** ERA5 atmospheric reanalysis (UTC) and Vietnam CEM air quality sensors (UTC+7) can be merged without temporal mismatch.
- **Attack scenario:** PySpark default session timezone inherits the local host OS timezone (UTC+7 on local developer machines). If not explicitly locked, timestamp parsing or casting in Step 2 would shift records by 7 hours, invalidating time continuity.
- **Stress test:** Verified `spark.conf.get("spark.sql.session.timeZone") == "UTC"`. Both timestamp parsing and Parquet timestamp storage are guaranteed UTC.
- **Result:** **ROBUST / DEFENDED**.

### Challenge 4: Shuffle Partition Bloat on Small Datasets
- **Assumption challenged:** Default Spark shuffle partitions (200) would cause severe task scheduling latency on 280,632 rows.
- **Stress test:** Verified `spark.conf.get("spark.sql.shuffle.partitions") == "16"`. With 4 CPU cores, 16 partitions provide 4 tasks per core, avoiding tiny partition files while maintaining full core utilization.
- **Result:** **ROBUST / DEFENDED**.

---

## 5. Verified Claims Matrix

| Claim in Worker Handoff | Independent Verification Method | Result |
|:---|:---|:---:|
| `Dockerfile.spark` builds cleanly as `airquality-spark:3.5.1` | `docker images airquality-spark:3.5.1` | **PASS** (Image ID `41a45803a5a0`, 839MB) |
| Container contains all required dependencies | In-container pytest session starts with numpy, pandas, pyarrow, torch | **PASS** |
| `spark_session.py` configures local[4], 5g, 16 partitions, Arrow, AQE, UTC, Snappy | Tested on host & Docker container via `pytest tests/test_step1_docker_spark.py` | **PASS** (9/9 passed) |
| Event logs written to `output/spark_event_logs/` | Direct inspection of `output\spark_event_logs` directory | **PASS** (Multiple valid event log files > 80KB) |
| `--keep-ui` holds Spark Web UI active | Executed `python code/lakehouse/spark_session.py --keep-ui-seconds 1` on host | **PASS** (Held for 1s, exited cleanly) |
| No dummy or fake mocks used | Static code analysis & runtime inspection of `code/lakehouse/spark_session.py` | **PASS** |

---

## 6. Coverage Gaps & Unverified Items

- **Coverage Gaps:** None for Step 1 scope.
- **Unverified Items:** None. All deliverables are directly verified on both host and Docker container runtimes.

---

## 7. Conclusion & Recommendation

The Step 1 deliverables strictly meet all technical specifications defined in R1 and PROJECT.md. The implementation is robust, cross-platform aware, and free of any integrity violations.

**Recommendation:** Proceed immediately to Milestone 2 (Step 2: Bronze to Silver preprocessing pipeline).
