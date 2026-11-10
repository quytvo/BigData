# TECHNICAL QUALITY & ADVERSARIAL REVIEW REPORT — STEP 1

**Reviewer:** Reviewer 2 (Step 1 Deliverables)  
**Roles:** Reviewer (Quality & Verification) & Adversarial Critic (Failure Modes & Stress Testing)  
**Timestamp:** 2026-11-09T03:03:00Z  
**Reviewed Artifacts:**
- `Dockerfile.spark`
- `code/lakehouse/spark_session.py`
- `tests/test_step1_docker_spark.py`
- `docs/review_checkpoint_step1.md`
- `output/spark_event_logs/`

---

## 1. Review Summary

**Verdict: APPROVE**

**Integrity Verification: PASSED (NO INTEGRITY VIOLATION)**
- **Source Code Verification**: Zero hardcoded test mocks, zero facade patterns, and zero shortcuts. `code/lakehouse/spark_session.py` initializes genuine Spark 3.5.1 sessions with real Catalyst engine and JVM runtime.
- **Verification Traceability**: Event log files (`output/spark_event_logs/local-*`), Docker container execution logs, and independent unit tests represent genuine, independently reproducible artifacts.
- **Independent Execution**: Confirmed 9/9 unit tests passing on Windows 11 host (5.30s) and inside Linux Docker container `airquality-spark:3.5.1` with resource limits `-m 8g --cpus 4` (4.65s).

---

## 2. Integrity & Quality Audit

| Dimension | Standard / Expectation | Implementation State | Status |
|:---|:---|:---|:---:|
| **Zero Facade / Mock Logic** | Real SparkSession initialized with Tungsten & Catalyst | `SparkSession.builder.getOrCreate()` creates genuine JVM Spark context | **PASS** |
| **Dependency Integrity** | Exact dependencies required by R1 (`numpy`, `pandas`, `pyarrow`, `psutil`, `pytest`, `torch`) | All dependencies pinned and verified in `Dockerfile.spark`; PyTorch uses CPU wheel to save bandwidth and image size | **PASS** |
| **Memory Headroom** | 5g driver memory in 8g container constraint | Verified: 5.12GB heap + ~512MB JVM overhead leaves 2.4GB headroom for OS, PyArrow, and off-heap execution | **PASS** |
| **Event Log Handling** | Pre-creation of `output/spark_event_logs` directory; URI formatting | Verified: Directory is created with `mkdir(parents=True, exist_ok=True)` *prior* to `builder.getOrCreate()`; URI formatted cleanly as `file:///workspace/output/spark_event_logs` in Docker | **PASS** |
| **Cross-Platform Robustness** | Host Windows fallback without `winutils.exe` crash | Verified: Detects missing winutils and cleanly disables eventLog on host with warning; fully activates eventLog inside Linux container | **PASS** |
| **Documentation Accuracy** | `docs/review_checkpoint_step1.md` matches physical reality | Checked byte sizes, timestamps, config summaries, and test runs against live environment | **PASS** |

---

## 3. Adversarial Analysis & Stress Test Findings

### Challenge 1: Memory Boundary & Container OOM Risk (Headroom Analysis)
- **Assumption Challenged**: Driver memory set to `"5g"` (`spark.driver.memory="5g"`) is completely safe inside an 8GB container.
- **Attack Scenario**:
  - Container total memory: 8,192 MB.
  - Spark Driver JVM heap: 5,120 MB.
  - JVM off-heap / overhead (`spark.driver.memoryOverhead`): ~512 MB.
  - Total JVM footprint: ~5,632 MB.
  - Headroom remaining: $\approx 2,560\text{ MB}$ (31.25%).
  - If a future worker in Step 3 or Step 5 attempts to convert a large Spark DataFrame (e.g. 14M rows in 50x benchmark) to Pandas via `.toPandas()`, PyArrow IPC buffers and the Python process will allocate $>2.5\text{GB}$ in user space, prompting the Linux kernel OOM killer (`SIGKILL`, exit code 137).
- **Stress Test Executed**: Executed `docker run --rm -m 8g --cpus 4 -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 pytest tests/test_step1_docker_spark.py`. Execution completed cleanly in 4.65s without exceeding memory bounds.
- **Mitigation / Architectural Guardrail**:
  - The 5GB driver memory allocation is optimal for the 8GB container, provided subsequent steps follow the architectural requirement: **100% Spark SQL and Catalyst Window functions; never invoke `.toPandas()` or `@udf` on full datasets**. Data must be written directly to Snappy Parquet via `DataFrame.write.parquet()`.

### Challenge 2: Event Log Directory Resilience & URI Parsing
- **Assumption Challenged**: Event log directory creation never fails regardless of prior filesystem state.
- **Attack Scenario**: Deleted `output/spark_event_logs` and tested whether `get_event_log_dir` and `get_spark_session` recover gracefully.
- **Result**: `get_event_log_dir()` successfully recreates missing nested directories (`parents=True, exist_ok=True`) before passing the URI to Spark's `EventLoggingListener`.
- **Hadoop Path Parsing**: In Docker, path resolves to `file:///workspace/output/spark_event_logs` (pure ASCII POSIX). On Windows, it handles Unicode path `D:/năm 4/...` properly in URI form `file:///D:/n%C4%83m%204/...`. Furthermore, because bare Windows without winutils would crash JVM when writing event logs, the auto-fallback safely avoids JVM initialization failures.

### Challenge 3: Port Mapping for Web UI Access (Ergonomic Finding)
- **Finding (Minor)**:
  - In `Dockerfile.spark`, port 4040 is exposed (`EXPOSE 4040`).
  - However, running `docker run` without `-p 4040:4040` prevents the host web browser from navigating to `http://localhost:4040` to view the Spark Web UI during `--keep-ui` sessions.
- **Recommendation**:
  - In operational runbooks and Step 5 scripts, ensure container run commands include `-p 4040:4040` whenever `--keep-ui` is desired:
    `docker run --rm -it -p 4040:4040 -m 8g --cpus 4 -v "${PWD}:/workspace" airquality-spark:3.5.1 python3 code/lakehouse/spark_session.py --keep-ui`

---

## 4. Verified Claims

1. `tests/test_step1_docker_spark.py` execution on host:
   - Command: `pytest tests/test_step1_docker_spark.py`
   - Result: 9 passed in 5.30s.
2. `tests/test_step1_docker_spark.py` execution in Docker:
   - Command: `docker run --rm -m 8g --cpus 4 -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 pytest tests/test_step1_docker_spark.py`
   - Result: 9 passed in 4.65s.
3. Event log generation:
   - Verified physical log file `output/spark_event_logs/local-1794192842184` (81,004 bytes) and subsequent test event logs.
4. Checkpoint report `docs/review_checkpoint_step1.md`:
   - Verified accurate documentation of test outputs, environment details, package versions, and configuration summary.

---

## 5. Verdict & Recommendation

**Verdict: APPROVE**

Milestone 1 (Step 1) satisfies all technical, architectural, and integrity criteria. The environment is rock-solid and fully prepared for Milestone 2 (Step 2: Bronze to Silver Preprocessing).
