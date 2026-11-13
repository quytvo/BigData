# Measured workload and CloudSim Plus benchmark

The profile at `output/cloudsim_benchmark/workload_profile.csv` is generated
from nine real Silver TEDP partitions and Global ANN training. It contains
nine MAP tasks and one ANN task. Its separate nine-province ANN training run
does not replace the project's existing full-model checkpoint.
To repeat the nine-province measurement, run:

```bash
docker build -f Dockerfile.spark -t airquality-spark:3.5.1 .
docker run --rm -m 8g --cpus 4 -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 python3 work_load.py --epochs 250
docker run --rm -m 8g --cpus 4 -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 python3 code/simulation/measure_storage.py
python code/simulation/run_cloudsim_benchmark.py
```

The last command starts the official Java CloudSim Plus 8.5.7 engine in a
Maven/JDK 25 Docker container. It writes `cloudsim_plan.csv` (measured input
converted to Cloudlets), `cloudsim_events.csv` (the engine's actual start,
finish and VM lifecycle timestamps), and `cloudsim_results.csv`. Docker must
be running; the Maven image and dependencies download automatically on first
use. `--engine analytic` runs the earlier Python estimate separately and writes
`analytic_results.csv` by default, keeping the CloudSim result intact.

`work_load.py` selects the nine largest Silver TEDP province partitions unless
`--provinces` supplies exactly nine names. Include Hà Nội when overriding the
selection, since the storage benchmark uses it for the representative query.
The profiler runs the project's real PySpark
Gold feature transformation and Parquet write for each province, then trains
the actual Global ANN on those freshly measured Gold partitions. It records process-tree CPU
seconds, elapsed seconds, row counts, disk bytes, memory observations, and
the container's effective CPU/RAM limits. `host_cores` describes the physical
host and can exceed Docker's `allocated_cores` quota. A
failed operation leaves the previous profile untouched. The ANN uses the project's
default architecture and early stopping; `--epochs` caps the real training run.
Before timing the nine MAP tasks, it executes four real Gold transformations as
a Spark warmup. This keeps JVM and Catalyst initialization from making the
first province look artificially heavy; the scenario's fixed 30-second VM
startup remains a modeling assumption.

`storage_benchmark.csv` was measured on the same nine provinces. The script
materializes a CSV from the real Silver Parquet table, replicates the source
records physically for 10x and 50x, and times actual Spark full CSV scans and
partition-pruned Parquet reads. The simulator checks that both input CSVs name
the same provinces. Measured process-tree CPU seconds are converted to
Cloudlet lengths using an assumed reference rate of 1,000 MIPS per vCPU.
This conversion gives CloudSim a time scale; it is not a measured GCP MIPS
rating. The scenario assumes CPU core-seconds scale ideally with vCPU count;
`--c2-core-factor` can supply a
measured C2/E2 per-core ratio (default 1). It models 30 seconds VM boot,
shared-storage ingress and remote-worker Gold transfer at an assumed 1 Gbps,
LPT scheduling, one global ANN reduction after the maps, and a small 0.2-second
CloudSim event-order margin. Network and I/O waits are represented by Cloudlet
submission delays; CloudSim executes the CPU work on its VMs. It applies
per-second compute billing with a 60-second minimum, and the three hourly
rates specified for this project. Those fixed rates are scenario inputs, not
a current Google Cloud price quote. Network charges and storage charges are
outside the reported compute cost. CPU efficiency is measured core-seconds
divided by the sum of provisioned physical core-seconds while each VM is active,
including boot, I/O and transfer intervals. The broker retains all VMs until
the ANN finishes; billing uses the VM destruction timestamps in the CloudSim
event trace. The 10x and 50x CPU demand is an
ideal linear extrapolation of the measured 1x pipeline; their storage read
latencies were measured against physically replicated source records.

The benchmark executes Java CloudSim Plus VMs and Cloudlets. It does not run
the pipeline on real GCP instances; performance at 10x and 50x and the
C2-to-E2 core ratio remain simulation assumptions.
