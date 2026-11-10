# Progress — explorer_survey_3

Last visited: 2026-11-09T02:41:00Z

## Status
- [x] Initialized DISPATCH.md and BRIEFING.md
- [x] Task 1: Check repository layout, existing Dockerfile, requirements.txt, environment configs, Python/Java versions, and local execution environment.
  - Confirmed Python 3.11.8 on host, Java 11.0.12 LTS, Docker version 29.2.1 running on WSL2.
  - Confirmed `code/lakehouse` is currently an empty directory. No Dockerfile or requirements.txt exists yet in root.
  - Discovered critical fact: Running PySpark locally on Windows fails on Parquet writes due to missing `winutils.exe` / `HADOOP_HOME`, making Linux Docker execution mandatory.
- [x] Task 2: Investigate Spark configuration requirements: local mode, 4 CPUs, 6-8GB RAM, PyArrow enabled, AQE, UTC timezone, event logging to `output/spark_event_logs/`, Web UI port 4040 with `--keep-ui` flag.
  - Docker Desktop has 16 CPUs and ~7.57 GiB RAM allocated.
  - `-m 8g --cpus 4` verified working.
  - Spark driver memory set to `5g` to leave headroom for OS/PyArrow, avoiding container OOM.
  - Event log dir `output/spark_event_logs/` must be pre-created before SparkSession initializes.
  - Port 4040 verified free on host.
- [x] Task 3: Investigate Step 5 benchmark requirements: `code/lakehouse/benchmark_spark_vs_pandas.py`, multi-scale 1x, 10x, 50x, output metrics format in `output/cloudsim_benchmark/spark_storage_benchmark.csv`, Docker execution with image `airquality-spark:3.5.1`.
  - Analyzed multi-scale data volume: 1x (280k rows, ~86MB CSV / ~15MB Parquet), 10x (2.8M rows), 50x (14M rows, ~4.3GB CSV).
  - Outlined metrics schema: scale, storage size, compression ratio, ETL time, query latency (partition pruning), bytes scanned.
  - Identified RAM pressure risk on 50x in Pandas, established mitigation (in-memory streaming/cleanup, separate GC).
- [x] Task 4: Investigate Step 6 programmatic test suite requirements: independent automated test suite for Step 1 through Step 5, review checkpoint reports after each step.
  - Discovered discrepancy between 279,840 in metrics.json and 280,104 actual rows produced by `build_global_features` (25,464 rows x 11 provinces = 280,104).
  - Designed automated test coverage for schemas (StructType), row counts, temporal continuity (25,512 hrs/prov), numerical precision (< 1e-5), and partition pruning.
  - Specified review checkpoint reports after each step.
- [x] Task 5: Identify potential resource bottlenecks, Docker volume mounts, Windows vs Linux path issues, and execution risks.
  - Verified Docker volume mount `${PWD}:/workspace` works seamlessly despite non-ASCII Windows path `d:\năm 4\big data\Final_Project_Plan`.
  - Confirmed official `apache/spark:3.5.1-python3` available on Docker Hub (amd64/arm64).
- [ ] Produce `survey_infra_benchmark.md` and `handoff.md`.
- [ ] Send completion message to orchestrator.
