"""Run the measured nine-province workload in the CloudSim Plus Java engine."""
from __future__ import annotations

import csv
import math
import subprocess
from pathlib import Path

from run_cloudsim_benchmark import CONFIG_NAMES, ROOT, VM, positive, read_profile, read_storage

OUT = ROOT / "output/cloudsim_benchmark"
PROJECT = ROOT / "code/simulation/cloudsim_plus"
IMAGE = "maven:3.9-eclipse-temurin-25"
EVENT_MARGIN_SEC = 0.2  # CloudSim Plus advances events at about 0.1 s granularity.


def write_plan(profile: Path, storage: Path, path: Path, network_mbps: float,
               boot_seconds: float, c2_core_factor: float) -> dict[str, dict]:
    if min(network_mbps, c2_core_factor) <= 0 or boot_seconds < 0:
        raise ValueError("Network and C2 throughput must be positive; boot must be nonnegative")
    tasks, ann_cpu = read_profile(profile)
    io = read_storage(storage, {task.province for task in tasks},
                      expected_rows=sum(task.input_rows for task in tasks))
    scenarios = {}
    rows = []
    for scale_name, factor in (("1x", 1), ("10x", 10), ("50x", 50)):
        for key in ("single", "balanced", "max_parallel"):
            sku, cores, ram_gb, usd_hour, n_vms = VM[key]
            fmt = "csv" if key == "single" else "parquet"
            sample = io[(scale_name, fmt)]
            io_seconds = positive(sample, "query_1prov_latency_ms") / 1000
            scanned_mb = positive(sample, "bytes_scanned_mb")
            core_factor = c2_core_factor if key == "max_parallel" else 1.0
            name = f"{scale_name}_{key}"
            loads = [0.0] * n_vms
            for task_id, task in enumerate(sorted(tasks, key=lambda t: (-t.cpu_seconds, t.province))):
                vm_id = min(range(n_vms), key=lambda i: (loads[i], i))
                ingress = scanned_mb * 8 / network_mbps
                transfer = task.output_mb * factor * 8 / network_mbps if vm_id else 0.0
                length_mi = max(1, round(task.cpu_seconds * factor * 1000 / (cores * core_factor)))
                # Cloudlet delays are relative to VM creation, which is itself
                # submitted after boot_seconds in the Java simulation.
                submit = loads[vm_id] + io_seconds + ingress
                loads[vm_id] += io_seconds + ingress + length_mi / 1000 + transfer
                rows.append((name, n_vms, cores, ram_gb * 1024, boot_seconds,
                             "MAP", task_id, vm_id, length_mi, submit, transfer))
            ann_length_mi = max(1, round(ann_cpu * factor * 1000 / (cores * core_factor)))
            rows.append((name, n_vms, cores, ram_gb * 1024, boot_seconds,
                         "ANN", 9, 0, ann_length_mi, max(loads) + EVENT_MARGIN_SEC, 0.0))
            scenarios[name] = dict(scale=scale_name, key=key, sku=sku, cores=cores,
                                   ram_gb=ram_gb, usd_hour=usd_hour, n_vms=n_vms,
                                   fmt=fmt, query_sec=io_seconds, scanned_mb=scanned_mb,
                                   map_cpu=sum(t.cpu_seconds for t in tasks) * factor,
                                   ann_cpu=ann_cpu * factor, core_factor=core_factor)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(("scenario", "vm_count", "cores", "ram_mb", "boot_sec",
                         "stage", "task_id", "vm_id", "length_mi", "submit_sec", "transfer_sec"))
        writer.writerows(rows)
    return scenarios


def container_path(path: Path) -> str:
    try:
        relative = path.resolve().relative_to(ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise ValueError("CloudSim output must be inside the repository mounted in Docker") from exc
    return f"/workspace/{relative}"


def invoke_cloudsim(plan: Path, events: Path) -> None:
    cache = PROJECT / ".m2"
    cache.mkdir(parents=True, exist_ok=True)
    arguments = f'"{container_path(plan)}" "{container_path(events)}"'
    subprocess.run([
        "docker", "run", "--rm", "-v", f"{ROOT}:/workspace",
        "-v", f"{cache}:/root/.m2", "-w", "/workspace/code/simulation/cloudsim_plus",
        IMAGE, "mvn", "-q", "clean", "compile", "exec:java",
        "-Dlogback.configurationFile=/workspace/code/simulation/cloudsim_plus/quiet-logback.xml",
        f"-Dexec.args={arguments}",
    ], check=True)
    if not events.is_file():
        raise RuntimeError("CloudSim Plus did not write an event trace")


def summarize(events: Path, scenarios: dict[str, dict], output: Path,
              network_mbps: float, boot_seconds: float, c2_core_factor: float) -> list[dict]:
    with events.open(encoding="utf-8", newline="") as stream:
        trace = list(csv.DictReader(stream))
    grouped: dict[str, list[dict]] = {name: [] for name in scenarios}
    for event in trace:
        grouped[event["scenario"]].append(event)
    results = []
    for scale_name in ("1x", "10x", "50x"):
        baseline = None
        for key in ("single", "balanced", "max_parallel"):
            name = f"{scale_name}_{key}"
            config = scenarios[name]
            tasks = grouped[name]
            maps = [t for t in tasks if t["task_stage"] == "MAP"]
            ann = [t for t in tasks if t["task_stage"] == "ANN"]
            if len(maps) != 9 or len(ann) != 1:
                raise ValueError(f"Incomplete CloudSim trace for {name}")
            ann_start = float(ann[0]["start_sec"])
            for task in maps:
                if float(task["finish_sec"]) + float(task["transfer_sec"]) > ann_start + 0.01:
                    raise ValueError(f"ANN started before a MAP transfer finished in {name}")
            makespan = float(ann[0]["finish_sec"])
            if baseline is None:
                baseline = makespan
            # All VMs remain allocated until the broker finishes the global
            # ANN task. Bill their actual CloudSim destruction timestamps.
            active = [0.0] * config["n_vms"]
            for task in tasks:
                vm_id = int(task["vm_id"])
                active[vm_id] = max(active[vm_id], float(task["vm_finish_sec"]))
            if any(seconds < makespan for seconds in active):
                raise ValueError(f"A VM was destroyed before the global ANN completed in {name}")
            billed = [max(60, math.ceil(seconds)) for seconds in active]
            cost = sum(seconds * config["usd_hour"] / 3600 for seconds in billed)
            cpu_eff = ((config["map_cpu"] + config["ann_cpu"]) / config["core_factor"] /
                       (sum(active) * config["cores"]))
            results.append({
                "scale": scale_name, "config": key, "config_name": CONFIG_NAMES[key],
                "region": "asia-southeast1", "vm_type": config["sku"],
                "vm_count": config["n_vms"], "vcpus_per_vm": config["cores"],
                "ram_gb_per_vm": config["ram_gb"], "price_usd_per_hour": config["usd_hour"],
                "makespan_sec": round(makespan, 4), "speedup": round(baseline / makespan, 4),
                "cpu_efficiency_pct": round(100 * cpu_eff, 4),
                "simulated_cost_usd": round(cost, 6), "boot_sec": boot_seconds,
                "network_mbps": network_mbps, "c2_core_factor": c2_core_factor,
                "storage_format": config["fmt"],
                "measured_query_sec_per_province": round(config["query_sec"], 4),
                "measured_scanned_mb_per_province": round(config["scanned_mb"], 4),
                "ann_cpu_seconds_scaled": round(config["ann_cpu"], 4),
                "simulation_engine": "CloudSim Plus 8.5.7",
            })
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)
    return results


def run_cloudsim(profile: Path, storage: Path, output: Path, network_mbps: float,
                 boot_seconds: float, c2_core_factor: float) -> list[dict]:
    plan = output.with_name("cloudsim_plan.csv")
    events = output.with_name("cloudsim_events.csv")
    container_path(plan)
    container_path(events)
    scenarios = write_plan(profile, storage, plan, network_mbps, boot_seconds, c2_core_factor)
    invoke_cloudsim(plan, events)
    return summarize(events, scenarios, output, network_mbps, boot_seconds, c2_core_factor)
