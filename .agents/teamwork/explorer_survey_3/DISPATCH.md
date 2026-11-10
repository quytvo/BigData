## 2026-11-09T02:32:00Z
# Task Assignment: Survey Explorer 3 (Docker, Spark Infrastructure, Benchmark & Test Suite)

Read ORIGINAL_REQUEST.md at: `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\ORIGINAL_REQUEST.md`

## Objectives
Investigate the existing workspace environment and specifications for Step 1, Step 5, and Step 6:
1. Check repository layout, existing Dockerfile, docker-compose, scripts, requirements.txt, Python/Java versions, and OS environment.
2. Investigate Spark configuration requirements: local mode, 4 CPUs, 6-8GB RAM, PyArrow enabled, AQE (Adaptive Query Execution), UTC timezone, event logging to `output/spark_event_logs/`, Web UI port 4040 with `--keep-ui` flag.
3. Investigate Step 5 benchmark requirements: `code/lakehouse/benchmark_spark_vs_pandas.py`, multi-scale 1x, 10x, 50x, output metrics format in `output/cloudsim_benchmark/spark_storage_benchmark.csv`, Docker execution with image `airquality-spark:3.5.1`.
4. Investigate Step 6 programmatic test suite requirements: independent automated test suite for Step 1 through Step 5 (schema, row counts, partition pruning, temporal continuity 25,512 hrs, numerical tolerance < 1e-5), and review checkpoint reports after each step.
5. Identify any potential resource bottlenecks, Docker volume mounts, Windows vs Linux path issues, and execution risks.

## Deliverables
- Write detailed survey report to `.agents/teamwork/explorer_survey_3/survey_infra_benchmark.md`
- Write handoff report to `.agents/teamwork/explorer_survey_3/handoff.md`
- Send completion message to orchestrator.
