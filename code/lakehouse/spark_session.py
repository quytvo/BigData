"""
Centralized Apache Spark 3.5.1 SparkSession Builder Module.

Provides optimized configuration for the Air Quality Medallion Lakehouse:
- Master: local[4]
- Memory: driver.memory = 5g
- Partitions: sql.shuffle.partitions = 16
- PyArrow Acceleration: sql.execution.arrow.pyspark.enabled = true
- Adaptive Query Execution (AQE): sql.adaptive.enabled = true
- TimeZone: sql.session.timeZone = UTC
- Parquet Compression: sql.parquet.compression.codec = snappy
- Event Logging: spark.eventLog.enabled = true, eventLog.dir = output/spark_event_logs/
- Spark Web UI: active wait loop support and keep-ui flag on port 4040.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession

# Configure module-level logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("AirQualityLakehouse.SparkSession")

# Ensure Python workers use the active environment interpreter
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
if os.name == "nt":
    if Path("C:/hadoop/bin").is_dir():
        os.environ.setdefault("HADOOP_HOME", "C:/hadoop")
        os.environ.setdefault("hadoop.home.dir", "C:/hadoop")



def get_project_root() -> Path:
    """
    Locates the project root directory whether executing locally or inside Docker container (/workspace).
    """
    # If WORKSPACE_ROOT environment variable is set, prefer it
    env_root = os.environ.get("WORKSPACE_ROOT")
    if env_root and Path(env_root).is_dir():
        return Path(env_root).resolve()

    # If running inside Docker at /workspace
    if Path("/workspace").is_dir() and (Path("/workspace/lakehouse_storage").exists() or Path("/workspace/code").exists()):
        return Path("/workspace").resolve()

    # Fallback to traversing up from this file's location: code/lakehouse -> project_root
    current_file = Path(__file__).resolve()
    # current_file.parent is code/lakehouse, parent.parent is code, parent.parent.parent is project root
    return current_file.parents[2]


def get_event_log_dir(project_root: Optional[Path] = None) -> Path:
    """
    Resolves and ensures the existence of the Spark event logs directory.
    """
    root = project_root or get_project_root()
    log_dir = root / "output" / "spark_event_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def _is_winutils_available() -> bool:
    """
    Checks if Hadoop binary (winutils.exe) is present on Windows hosts.
    Hadoop on Windows requires winutils.exe to set POSIX permissions for local file writes (such as eventLog).
    Inside Linux Docker containers, this check returns True as native POSIX is available.
    """
    if os.name != "nt":
        return True
    hadoop_home = os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir")
    if not hadoop_home:
        return False
    winutils_exe = Path(hadoop_home) / "bin" / "winutils.exe"
    return winutils_exe.is_file()


def wait_for_webui(spark: SparkSession, keep_ui_seconds: int = 0) -> None:
    """
    Active wait loop keeping Spark Context alive so user can inspect DAGs and stages on Spark Web UI.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    keep_ui_seconds : int, optional
        Duration in seconds to hold the UI open. If <= 0, holds indefinitely until Ctrl+C.
    """
    sc = spark.sparkContext
    ui_url = getattr(sc, "uiWebUrl", "http://localhost:4040")
    app_id = sc.applicationId

    logger.info("=" * 70)
    logger.info("Spark Web UI is active!")
    logger.info("  Application ID : %s", app_id)
    logger.info("  Web UI URL     : %s", ui_url)
    if keep_ui_seconds > 0:
        logger.info("  Holding active for %d seconds. Press Ctrl+C to stop earlier...", keep_ui_seconds)
    else:
        logger.info("  Holding active indefinitely. Press Ctrl+C to terminate session...")
    logger.info("=" * 70)

    start_time = time.time()
    try:
        while True:
            elapsed = time.time() - start_time
            if keep_ui_seconds > 0 and elapsed >= keep_ui_seconds:
                logger.info("Keep-UI timer expired (%d seconds). Stopping session.", keep_ui_seconds)
                break
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Interrupt signal (Ctrl+C) received. Releasing Spark Web UI and shutting down.")


def get_spark_session(
    app_name: str = "AirQualityLakehouse",
    keep_ui: bool = False,
    master: str = "local[4]",
    driver_memory: str = "5g",
    shuffle_partitions: str = "16",
    enable_event_log: Optional[bool] = None,
) -> SparkSession:
    """
    Initializes and returns a production-grade SparkSession tuned for the Medallion Lakehouse.

    Parameters
    ----------
    app_name : str, optional
        Spark application name (default: "AirQualityLakehouse").
    keep_ui : bool, optional
        If True, logs the Web UI endpoint upon creation and attaches the wait_for_webui helper.
    master : str, optional
        Spark master URL (default: "local[4]").
    driver_memory : str, optional
        Driver memory allocation (default: "5g", leaves 3g for OS/PyArrow/Off-heap in 8g container).
    shuffle_partitions : str, optional
        Number of shuffle partitions (default: "16", optimized for 4 CPU cores).
    enable_event_log : Optional[bool], optional
        Explicitly control event logging. Defaults to True on Linux/Docker, and auto-detects
        winutils on Windows host to prevent JVM startup crash.

    Returns
    -------
    SparkSession
        Active, configured PySpark SparkSession.
    """
    # 1. Resolve and ensure event log directory exists BEFORE builder.getOrCreate()
    root_dir = get_project_root()
    event_log_dir = get_event_log_dir(root_dir)

    # 2. Determine event log configuration
    winutils_ok = _is_winutils_available()
    if enable_event_log is None:
        if os.name == "nt" and not winutils_ok:
            logger.warning(
                "Windows host detected without HADOOP_HOME/winutils.exe. "
                "Disabling spark.eventLog.enabled on Windows host to prevent JVM initialization crash. "
                "(Event logging is fully enabled inside the Linux Docker container)."
            )
            event_log_active = False
        else:
            event_log_active = True
    else:
        event_log_active = bool(enable_event_log)

    # 3. Build SparkSession with exact configuration contracts
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # Resource allocation
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        # Catalyst & PyArrow optimization
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Timezone consistency (ERA5 + CEM sync)
        .config("spark.sql.session.timeZone", "UTC")
        # Storage format & compression
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Web UI
        .config("spark.ui.port", "4040")
        .config("spark.ui.enabled", "true")
    )

    if os.name == "nt" and Path("C:/hadoop/bin").is_dir():
        builder = (
            builder
            .config("spark.driver.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin")
            .config("spark.executor.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin")
        )

    # Event logging configuration
    if event_log_active:
        builder = (
            builder
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", event_log_dir.as_uri())
        )
    else:
        builder = (
            builder
            .config("spark.eventLog.enabled", "false")
        )

    logger.info("Initializing SparkSession '%s' (master=%s, memory=%s, shuffle_partitions=%s)...",
                app_name, master, driver_memory, shuffle_partitions)
    spark = builder.getOrCreate()

    # Log operational details
    sc = spark.sparkContext
    ui_url = getattr(sc, "uiWebUrl", "http://localhost:4040")
    logger.info("SparkSession initialized successfully. Spark version: %s, UI: %s", spark.version, ui_url)
    logger.info("Configuration: AQE=%s, Arrow=%s, TimeZone=%s, Codec=%s, EventLog=%s (%s)",
                spark.conf.get("spark.sql.adaptive.enabled"),
                spark.conf.get("spark.sql.execution.arrow.pyspark.enabled"),
                spark.conf.get("spark.sql.session.timeZone"),
                spark.conf.get("spark.sql.parquet.compression.codec"),
                spark.conf.get("spark.eventLog.enabled"),
                event_log_dir.as_uri() if event_log_active else "disabled")

    # Attach helper method directly to SparkSession instance for ergonomic usage
    spark.wait_for_webui = lambda seconds=0: wait_for_webui(spark, seconds)  # type: ignore

    if keep_ui:
        logger.info("keep_ui flag is enabled. Call spark.wait_for_webui() or pass --keep-ui via CLI to hold session.")

    return spark


def main() -> None:
    """CLI Entry point for verifying and interacting with SparkSession."""
    parser = argparse.ArgumentParser(description="Initialize and verify SparkSession for Air Quality Lakehouse")
    parser.add_argument("--app-name", type=str, default="AirQualityLakehouse", help="Spark Application name")
    parser.add_argument("--master", type=str, default="local[4]", help="Spark Master URL (default: local[4])")
    parser.add_argument("--keep-ui", action="store_true", help="Keep Spark Web UI active until interrupted")
    parser.add_argument("--keep-ui-seconds", type=int, default=0, help="Seconds to keep UI open (0 for indefinite)")
    parser.add_argument("--force-event-log", action="store_true", help="Force spark.eventLog.enabled=true even on Windows")
    args = parser.parse_args()

    enable_log = True if args.force_event_log else None
    spark = get_spark_session(
        app_name=args.app_name,
        keep_ui=args.keep_ui,
        master=args.master,
        enable_event_log=enable_log,
    )

    print("\n" + "=" * 60)
    print(" SPARK CONFIGURATION SUMMARY")
    print("=" * 60)
    configs_to_check = [
        "spark.master",
        "spark.app.name",
        "spark.driver.memory",
        "spark.sql.shuffle.partitions",
        "spark.sql.execution.arrow.pyspark.enabled",
        "spark.sql.adaptive.enabled",
        "spark.sql.session.timeZone",
        "spark.sql.parquet.compression.codec",
        "spark.eventLog.enabled",
        "spark.eventLog.dir",
        "spark.ui.port",
    ]
    for cfg in configs_to_check:
        try:
            val = spark.conf.get(cfg)
        except Exception:
            val = "<not set>"
        print(f"  {cfg:<45} : {val}")
    print("=" * 60 + "\n")

    if args.keep_ui or args.keep_ui_seconds > 0:
        wait_for_webui(spark, keep_ui_seconds=args.keep_ui_seconds)
    else:
        logger.info("Shutting down SparkSession.")
        spark.stop()
        logger.info("Done.")


if __name__ == "__main__":
    main()
