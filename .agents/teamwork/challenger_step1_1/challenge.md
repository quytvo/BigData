# Adversarial Challenge Report — Step 1: Docker & SparkSession Configuration

## Challenge Summary

**Overall risk assessment**: LOW (Step 1 implementation is highly robust, fully compliant with Lakehouse requirements, and passes all edge cases inside the Docker container).
**Verdict**: **APPROVE** (All core requirements R1 satisfied, 100% test pass rate in container, 0 regressions).

---

## Challenges & Empirical Attack Scenarios

### [Low] Challenge 1: Host vs Container Python Executable Discrepancy (PYSPARK_PYTHON)
- **Assumption challenged**: `get_spark_session()` assumes that if developers run PySpark outside the Docker container (directly on Windows host), `python` is resolvable and callable by PySpark workers during Arrow serialization.
- **Attack scenario**: On Windows hosts with Microsoft Store app execution aliases enabled, PySpark workers invoke `python`, which triggers `Python was not found; run without arguments to install from the Microsoft Store...` and causes `java.net.SocketTimeoutException: Accept timed out` during `spark.createDataFrame(pandas_df)`.
- **Blast radius**: Local execution on Windows without Docker when converting Pandas DataFrames using PyArrow. (Has ZERO impact inside Docker container because `Dockerfile.spark` explicitly sets `ENV PYSPARK_PYTHON=python3`).
- **Mitigation**: Add a guard in `code/lakehouse/spark_session.py`:
  ```python
  if "PYSPARK_PYTHON" not in os.environ:
      os.environ["PYSPARK_PYTHON"] = sys.executable
  if "PYSPARK_DRIVER_PYTHON" not in os.environ:
      os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
  ```

### [Low] Challenge 2: Hadoop FileOutputCommitter on Windows Host
- **Assumption challenged**: Can Parquet files be written directly on the Windows host without Docker?
- **Attack scenario**: Invoking `df.write.parquet(...)` on Windows host without `HADOOP_HOME/winutils.exe` raises `java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset` from Hadoop `RawLocalFileSystem.setPermission`.
- **Blast radius**: Purely host-side Windows filesystem issue. Inside the target Linux container (`airquality-spark:3.5.1`), native POSIX file system handles permissions natively and all Parquet writes succeed.
- **Mitigation**: Confirm and document that all Parquet writing and pipeline steps (Step 2 to Step 5) must be executed inside the Linux Docker container as designed in PROJECT.md.

### [Low] Challenge 3: Session Idempotence and Memory Re-allocation
- **Assumption challenged**: Multiple invocations of `get_spark_session()` with different parameters could fail or leave dirty state.
- **Attack scenario**:
  - Scenario A: Call `get_spark_session("App1")` then `get_spark_session("App2")`.
  - Scenario B: Call `spark.stop()` then call `get_spark_session()` again in the same process.
- **Result**:
  - Scenario A cleanly returns the active SparkSession with a standard Spark warning that existing configuration applies. Both handles execute queries accurately.
  - Scenario B cleanly spins up a brand new SparkSession and SparkContext with a new application ID.
- **Mitigation**: None needed. Behavior is completely safe and conforms to Spark session lifecycle best practices.

### [Low] Challenge 4: Resilience to Deletion of Event Log Directory
- **Assumption challenged**: If `output/spark_event_logs/` is deleted prior to Spark session initialization, does Spark fail?
- **Attack scenario**: Complete directory deletion simulated before `get_spark_session()`.
- **Result**: `get_event_log_dir()` executes `log_dir.mkdir(parents=True, exist_ok=True)` before `builder.getOrCreate()`. Spark successfully initializes and writes event logs.
- **Mitigation**: None needed.

---

## Stress Test Results

| # | Stress Scenario | Expected Behavior | Actual Behavior | Result |
|:---|:---|:---|:---|:---:|
| 1 | Multiple invocations without `stop()` | Reuses active SparkContext, executes queries | Reused session, both sessions ran queries successfully | **PASS** |
| 2 | Stop and recreate session | Initializes fresh SparkContext with new App ID | Clean new SparkContext initialized and ran query | **PASS** |
| 3 | Pre-creation wipe of event log directory | Auto-recreates `output/spark_event_logs` | Recreated directory with full hierarchy | **PASS** |
| 4 | Empirical PyArrow acceleration | Converts Pandas <-> Spark DF without error | PyArrow vectorized conversion verified in Docker | **PASS** |
| 5 | Empirical AQE verification | Physical query plan contains `AdaptiveSparkPlan` | `AdaptiveSparkPlan` verified on joined queries | **PASS** |
| 6 | Empirical UTC timezone guarantee | Session timezone is strictly UTC | `current_timezone() == 'UTC'`, unix timestamp match | **PASS** |
| 7 | Empirical Snappy Parquet codec | Column metadata specifies `SNAPPY` compression | PyArrow inspection verified all columns compressed with `SNAPPY` | **PASS** |
| 8 | Docker container resource constraints | Runs all 16 tests under `-m 8g --cpus 4` limit | Completed all 16 tests in 12.25s without OOM | **PASS** |

---

## Verification Suite Execution Log

### Docker Execution (`airquality-spark:3.5.1` with `-m 8g --cpus 4`):
```
============================= test session starts ==============================
platform linux -- Python 3.8.10, pytest-8.3.5, pluggy-1.5.0 -- /usr/bin/python3
cachedir: .pytest_cache
rootdir: /workspace
configfile: pytest.ini
collecting ... collected 16 items

tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_01_dockerfile_exists PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_02_dockerfile_base_image PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_03_dockerfile_dependencies PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_04_dockerfile_workspace_and_port PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_05_dockerfile_environment_variables PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_06_spark_session_file_exists PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_07_spark_session_importable PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_08_event_log_dir_creation PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_09_spark_session_configurations PASSED
tests/test_step1_adversarial.py::TestStep1Adversarial::test_01_multiple_invocations_reuse_session PASSED
tests/test_step1_adversarial.py::TestStep1Adversarial::test_02_stop_and_recreate_session PASSED
tests/test_step1_adversarial.py::TestStep1Adversarial::test_03_event_log_dir_deletion_resilience PASSED
tests/test_step1_adversarial.py::TestStep1Adversarial::test_04_empirical_pyarrow_acceleration PASSED
tests/test_step1_adversarial.py::TestStep1Adversarial::test_05_empirical_aqe_enabled PASSED
tests/test_step1_adversarial.py::TestStep1Adversarial::test_06_empirical_utc_timezone_guarantee PASSED
tests/test_step1_adversarial.py::TestStep1Adversarial::test_07_empirical_snappy_parquet_codec PASSED

============================= 16 passed in 12.25s ==============================
```

---

## Final Recommendation
**APPROVE**: Step 1 implementation has passed all programmatic and empirical stress tests. The orchestrator may proceed to dispatch Step 2 (Bronze to Silver preprocessing).
