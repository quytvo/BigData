## 2026-11-09T02:46:10Z
You are Worker Step 1.
Working directory: d:\năm 4\big data\Final_Project_Plan
State folder: d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\worker_step1\
Read DISPATCH.md in your state folder, ORIGINAL_REQUEST.md at d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\ORIGINAL_REQUEST.md, PROJECT.md at d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\orchestrator\PROJECT.md, and Explorer 3 survey report at d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\explorer_survey_3\survey_infra_benchmark.md.

Implement Step 1 deliverables:
1. `Dockerfile.spark`:
   - Based on `apache/spark:3.5.1-python3`.
   - Install dependencies: `numpy`, `pandas`, `pyarrow`, `psutil`, `pytest`, `torch`.
   - Setup working directory `/workspace`, environment variables, expose port 4040.
2. `code/lakehouse/spark_session.py`:
   - `get_spark_session(app_name="AirQualityLakehouse", keep_ui=False, master="local[4]") -> SparkSession`
   - Configs: local[4], driver.memory="5g", shuffle.partitions="16", arrow enabled, AQE enabled, UTC timezone, Snappy parquet, event logs to output/spark_event_logs/ (ensure directory exists before builder.getOrCreate()).
   - Support `--keep-ui` flag and active wait loop.
3. Verify your implementation by running verification commands and capturing outputs.

MANDATORY INTEGRITY WARNING:
DO NOT CHEAT. All implementations must be genuine. DO NOT hardcode test results, create dummy/facade implementations, or circumvent the intended task. A teamwork_auditor will independently verify your work. Integrity violations WILL be detected and your work WILL be rejected.

Write your handoff report to `.agents/teamwork/worker_step1/handoff.md` and send a message when complete.
