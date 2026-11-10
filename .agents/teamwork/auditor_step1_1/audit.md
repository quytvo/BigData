# Forensic Audit Report: Milestone 1 (Step 1)

**Work Product**: `Dockerfile.spark`, `code/lakehouse/spark_session.py`, `output/spark_event_logs/`, Docker Image `airquality-spark:3.5.1`  
**Auditor**: Forensic Auditor (`auditor_step1_1`)  
**Timestamp**: 2026-11-09T03:02:40Z  
**Integrity Mode**: Development (from `ORIGINAL_REQUEST.md`)  
**Verdict**: **CLEAN**

---

## 1. Executive Summary
A forensic integrity audit was performed on the Step 1 deliverables for the PySpark Air Quality Medallion Lakehouse project. Every requirement specified in `ORIGINAL_REQUEST.md` (R1) and `PROJECT.md` (Interface Contract 1) was empirically scrutinized. Static AST parsing, runtime behavioral analysis, Docker image container inspection, and independent test execution confirmed that the implementation contains zero fake implementations, zero dummy facades, zero hardcoded test outputs, and zero fabricated logs.

The overall forensic verdict is **CLEAN**.

---

## 2. Phase-by-Phase Forensic Results

| # | Forensic Check | Evaluation Method | Result | Details |
|---|----------------|-------------------|:------:|---------|
| 1 | **Hardcoded Output Detection** | Python AST inspection & token scan of `spark_session.py` | **PASS** | No hardcoded return values, constant mocks, or static test return strings. |
| 2 | **Facade & Dummy Detection** | Function AST inspection & runtime JVM verification | **PASS** | `get_spark_session()` initializes a genuine `pyspark.sql.SparkSession` backed by Apache Spark 3.5.1 JVM Catalyst engine. |
| 3 | **Pre-populated Artifact Detection** | Log file directory scan and JSON event stream analysis | **PASS** | Files in `output/spark_event_logs/` are genuine SparkListener JSON streams containing complete event records (`SparkListenerLogStart`, `SparkListenerEnvironmentUpdate`, `SparkListenerBlockManagerAdded`, etc.). |
| 4 | **Docker Image Authenticity** | `docker inspect` & containerized package import | **PASS** | `airquality-spark:3.5.1` exists (ID: `sha256:41a45803a5a0cb6a9d062947a15424c5779589b03a53415b9b59cd6406ffcb17`), built from `apache/spark:3.5.1-python3` with `numpy`, `pandas`, `pyarrow` (17.0.0), `psutil`, `pytest` (8.3.5), and `torch` (2.4.1+cpu). |
| 5 | **Behavioral Test Verification** | Programmatic test suite `tests/test_step1_docker_spark.py` | **PASS** | 9/9 tests passed in Docker container (`docker run --rm -v ... pytest ...`) and 9/9 tests passed on host environment. |
| 6 | **Development Mode Policy Compliance** | Integrity rules mapping | **PASS** | Zero prohibited patterns under Development Mode. |

---

## 3. Adversarial Stress-Testing & Robustness Analysis

### A. Windows Host Winutils Fallback
- **Stress Scenario**: Running PySpark on Windows without `HADOOP_HOME` or `winutils.exe` typically triggers a fatal JVM crash when `spark.eventLog.enabled=true`.
- **Implementation Defense**: `code/lakehouse/spark_session.py` lines 66-79 & 155-168 implement `_is_winutils_available()`, gracefully logging a warning and disabling event logging on Windows hosts while keeping it mandatory in Docker/Linux.
- **Empirical Verification**: Successfully verified on host machine without JVM crashes.

### B. Spark Event Log Directory Race Condition
- **Stress Scenario**: Spark JVM aborts startup if the target URI for `spark.eventLog.dir` does not exist on disk.
- **Implementation Defense**: `get_event_log_dir()` ensures directory creation via `mkdir(parents=True, exist_ok=True)` before `SparkSession.builder.getOrCreate()` is invoked.
- **Empirical Verification**: Directory exists and contains genuine Spark logs.

### C. Container Resource Sizing
- **Stress Scenario**: Running a 5GB driver heap in an 8GB RAM container under heavy PyArrow off-heap allocations could trigger Linux OOM-killer.
- **Implementation Defense**: Driver heap is capped at 5GB, leaving 3GB (~37.5%) dedicated for OS, PyArrow buffers, and Python worker memory. Shuffle partitions are set to 16 (4 partitions per core on 4 CPUs).

---

## 4. Empirical Evidence & Raw Outputs

### Evidence 1: Python AST Analysis of `code/lakehouse/spark_session.py`
```
Total functions defined: 6
Function: get_project_root, args: [], returns: 3
  Returns other expression: <class 'ast.Subscript'>
  Returns Call: Attribute(value=Call(func=Name(id='Path', ctx=Load()), args=[Name(id='env_root', ctx=Load())], keywords=[]), attr='resolve', ctx=Load())
  Returns Call: Attribute(value=Call(func=Name(id='Path', ctx=Load()), args=[Constant(value='/workspace')], keywords=[]), attr='resolve', ctx=Load())
Function: get_event_log_dir, args: ['project_root'], returns: 1
  Returns Name: log_dir
Function: _is_winutils_available, args: [], returns: 3
  Returns Call: Attribute(value=Name(id='winutils_exe', ctx=Load()), attr='is_file', ctx=Load())
  Returns Constant: True
  Returns Constant: False
Function: wait_for_webui, args: ['spark', 'keep_ui_seconds'], returns: 0
Function: get_spark_session, args: ['app_name', 'keep_ui', 'master', 'driver_memory', 'shuffle_partitions', 'enable_event_log'], returns: 1
  Returns Name: spark
Function: main, args: [], returns: 0
```

### Evidence 2: Docker Image Inspection & Python Dependencies
```
$ docker images airquality-spark:3.5.1
IMAGE                    ID             DISK USAGE   CONTENT SIZE   EXTRA
airquality-spark:3.5.1   41a45803a5a0       3.01GB          839MB   U    

$ docker run --rm airquality-spark:3.5.1 python3 -c "import pyspark, numpy, pandas, pyarrow, psutil, pytest, torch; print('Spark:', pyspark.__version__, 'PyTorch:', torch.__version__, 'Arrow:', pyarrow.__version__)"
Spark: 3.5.1 PyTorch: 2.4.1+cpu Arrow: 17.0.0
```

### Evidence 3: Containerized Test Execution
```
$ docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest tests/test_step1_docker_spark.py
============================= test session starts ==============================
platform linux -- Python 3.8.10, pytest-8.3.5, pluggy-1.5.0 -- /usr/bin/python3
cachedir: .pytest_cache
rootdir: /workspace
configfile: pytest.ini
collecting ... collected 9 items

tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_01_dockerfile_exists PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_02_dockerfile_base_image PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_03_dockerfile_dependencies PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_04_dockerfile_workspace_and_port PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_05_dockerfile_environment_variables PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_06_spark_session_file_exists PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_07_spark_session_importable PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_08_event_log_dir_creation PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_09_spark_session_configurations PASSED

============================== 9 passed in 3.98s ===============================
```

### Evidence 4: Host Machine Test Execution
```
$ pytest tests/test_step1_docker_spark.py
============================= test session starts =============================
platform win32 -- Python 3.11.8, pytest-9.2.0, pluggy-1.7.0
collecting ... collected 9 items

tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_01_dockerfile_exists PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_02_dockerfile_base_image PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_03_dockerfile_dependencies PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_04_dockerfile_workspace_and_port PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_05_dockerfile_environment_variables PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_06_spark_session_file_exists PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_07_spark_session_importable PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_08_event_log_dir_creation PASSED
tests/test_step1_docker_spark.py::TestStep1DockerSpark::test_09_spark_session_configurations PASSED

============================== 9 passed in 4.60s ==============================
```

### Evidence 5: Spark Event Log JSON Sample (`output/spark_event_logs/local-1794192842184`)
```json
{"Event":"SparkListenerLogStart","Spark Version":"3.5.1"}
{"Event":"SparkListenerResourceProfileAdded","Resource Profile Id":0,"Executor Resource Requests":{"cores":{"Resource Name":"cores","Amount":1,"Discovery Script":"","Vendor":""},"memory":{"Resource Name":"memory","Amount":1024,"Discovery Script":"","Vendor":""},"offHeap":{"Resource Name":"offHeap","Amount":0,"Discovery Script":"","Vendor":""}},"Task Resource Requests":{"cpus":{"Resource Name":"cpus","Amount":1.0}}}
{"Event":"SparkListenerExecutorAdded","Timestamp":1794192842236,"Executor ID":"driver","Executor Info":{"Host":"0165dd21db04","Total Cores":4,"Log Urls":{},"Attributes":{},"Resources":{},"Resource Profile Id":0}}
{"Event":"SparkListenerBlockManagerAdded","Block Manager ID":{"Executor ID":"driver","Host":"0165dd21db04","Port":36383},"Maximum Memory":3032481792,"Timestamp":1794192842256,"Maximum Onheap Memory":3032481792,"Maximum Offheap Memory":0}
{"Event":"SparkListenerEnvironmentUpdate","JVM Information":{"Java Home":"/opt/java/openjdk","Java Version":"11.0.22 (Eclipse Adoptium)","Scala Version":"version 2.12.18"},"Spark Properties":{"spark.master":"local[4]","spark.app.name":"AirQualityLakehouse","spark.driver.memory":"5g","spark.sql.shuffle.partitions":"16","spark.sql.execution.arrow.pyspark.enabled":"true","spark.sql.adaptive.enabled":"true","spark.sql.session.timeZone":"UTC","spark.sql.parquet.compression.codec":"snappy","spark.eventLog.enabled":"true","spark.eventLog.dir":"file:///workspace/output/spark_event_logs","spark.app.id":"local-1794192842184"}}
```

---

## 5. Conclusion & Recommendations
The Step 1 work product meets all architectural and integrity standards. No cheating, mocking, or facade implementations exist. The code is ready for downstream pipeline ingestion in Milestone 2 (Step 2: Bronze to Silver preprocessing).
