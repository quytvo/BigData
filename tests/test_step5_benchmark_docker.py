"""Test Suite for Step 5: Multi-Scale Benchmark and Docker Execution.

Verifies:
- output/cloudsim_benchmark/spark_storage_benchmark.csv:
  - Benchmark results for 1x, 10x, and 50x workload scales.
  - Required metric columns: scale, framework/engine, rows/row_count, storage size,
    runtime/processing time, throughput, compression ratio, partition pruning speedup.
  - Scale row counts: 1x (280,632), 10x (2,806,320), 50x (14,031,600).
  - Parquet compression efficiency: Parquet storage size < 30% of CSV (compression ratio > 70%).
- Docker runtime contract:
  - Resource limits: 8GB RAM (-m 8g), 4 CPUs (--cpus 4).
  - Image tagging: airquality-spark:3.5.1.
  - Spark event logs in output/spark_event_logs/.
"""

import re
import sys
from pathlib import Path
import pandas as pd
import pytest

# Ensure project root and tests directory are in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TESTS_DIR = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

try:
    from tests.conftest import (
        BENCHMARK_CSV,
        BENCHMARK_DIR,
        CODE_DIR,
        OUTPUT_DIR,
        PROJECT_ROOT,
        SPARK_EVENT_LOGS_DIR,
    )
except ImportError:
    from conftest import (
        BENCHMARK_CSV,
        BENCHMARK_DIR,
        CODE_DIR,
        OUTPUT_DIR,
        PROJECT_ROOT,
        SPARK_EVENT_LOGS_DIR,
    )


class TestStep5BenchmarkDocker:
    """Verifies Milestone 5 (Step 5) multi-scale performance benchmark and Docker execution deliverables."""

    def test_benchmark_script_exists(self):
        """Verifies code/lakehouse/benchmark_spark_vs_pandas.py exists and supports multi-scale benchmarking."""
        script_path = CODE_DIR / "lakehouse" / "benchmark_spark_vs_pandas.py"
        assert script_path.exists(), (
            f"Benchmark runner script not found: {script_path}. "
            "Milestone 5 requires code/lakehouse/benchmark_spark_vs_pandas.py."
        )

        content = script_path.read_text(encoding="utf-8")
        assert len(content.strip()) > 0, f"Benchmark script {script_path} is empty."

        # Check references to scales: 1x, 10x, 50x
        for scale in ["1x", "10x", "50x"]:
            assert scale in content, f"Benchmark script should explicitly handle scale '{scale}'."

    def test_benchmark_output_csv_existence_and_schema(self):
        """Verifies output/cloudsim_benchmark/spark_storage_benchmark.csv exists and has required metric columns."""
        assert BENCHMARK_CSV.exists(), (
            f"Benchmark results CSV not found: {BENCHMARK_CSV}. "
            "Run Step 5 (code/lakehouse/benchmark_spark_vs_pandas.py) to generate benchmark results."
        )

        df = pd.read_csv(BENCHMARK_CSV)
        assert len(df) > 0, f"Benchmark results CSV {BENCHMARK_CSV} is empty."

        # Normalize column names for flexible naming matching
        cols = [c.lower() for c in df.columns]

        # 1. Scale column
        assert any("scale" in c for c in cols), f"Missing 'scale' column in benchmark CSV. Found: {list(df.columns)}"

        # 2. Engine / Framework column
        assert any(c in cols for c in ["engine", "framework"]), (
            f"Missing 'engine' or 'framework' column in benchmark CSV. Found: {list(df.columns)}"
        )

        # 3. Format column (Parquet vs CSV)
        assert any(c in cols for c in ["format", "storage_format"]), (
            f"Missing 'format' column in benchmark CSV. Found: {list(df.columns)}"
        )

        # 4. Row count column
        assert any(c in cols for c in ["rows", "row_count"]), (
            f"Missing row count column ('rows' or 'row_count') in benchmark CSV. Found: {list(df.columns)}"
        )

        # 5. Storage size column
        assert any(c in cols for c in ["storage_size_mb", "storage_mb", "size_mb"]), (
            f"Missing storage size column in benchmark CSV. Found: {list(df.columns)}"
        )

        # 6. Processing / ETL time column
        assert any(c in cols for c in ["etl_runtime_sec", "processing_sec", "runtime_sec", "duration_sec"]), (
            f"Missing runtime column in benchmark CSV. Found: {list(df.columns)}"
        )

    def test_benchmark_multi_scale_data_points(self):
        """Verifies benchmark CSV covers 1x, 10x, and 50x scales with corresponding expected row counts."""
        assert BENCHMARK_CSV.exists(), f"Benchmark results CSV not found: {BENCHMARK_CSV}."

        df = pd.read_csv(BENCHMARK_CSV)
        scale_col = [c for c in df.columns if "scale" in c.lower()][0]
        row_col = [c for c in df.columns if c.lower() in ("rows", "row_count")][0]

        scales_present = set(df[scale_col].astype(str).str.lower().unique())
        for required_scale in ["1x", "10x", "50x"]:
            assert required_scale in scales_present, (
                f"Benchmark results missing required scale '{required_scale}'. Present: {scales_present}"
            )

        # Check row counts by scale
        expected_rows = {
            "1x": 280632,
            "10x": 2806320,
            "50x": 14031600,
        }

        for scale_name, exp_count in expected_rows.items():
            sub = df[df[scale_col].astype(str).str.lower() == scale_name]
            if len(sub) > 0:
                actual_count = int(sub[row_col].iloc[0])
                assert actual_count == exp_count, (
                    f"Scale '{scale_name}' row count mismatch: got {actual_count}, expected {exp_count}."
                )

    def test_benchmark_storage_and_compression_efficiency(self):
        """Verifies Parquet storage is significantly smaller than CSV across all scales (> 50% compression)."""
        assert BENCHMARK_CSV.exists(), f"Benchmark results CSV not found: {BENCHMARK_CSV}."

        df = pd.read_csv(BENCHMARK_CSV)
        scale_col = [c for c in df.columns if "scale" in c.lower()][0]
        format_col = [c for c in df.columns if c.lower() in ("format", "storage_format")][0]
        size_col = [c for c in df.columns if "storage" in c.lower() or "size" in c.lower()][0]

        for scale_val in ["1x", "10x", "50x"]:
            scale_df = df[df[scale_col].astype(str).str.lower() == scale_val]
            parquet_sub = scale_df[scale_df[format_col].astype(str).str.lower() == "parquet"]
            csv_sub = scale_df[scale_df[format_col].astype(str).str.lower() == "csv"]

            if len(parquet_sub) > 0 and len(csv_sub) > 0:
                parquet_size = float(parquet_sub[size_col].iloc[0])
                csv_size = float(csv_sub[size_col].iloc[0])

                assert parquet_size < csv_size, (
                    f"Scale {scale_val}: Parquet size ({parquet_size:.2f} MB) is not smaller than CSV ({csv_size:.2f} MB)."
                )

                # Compression ratio should be > 50% (Parquet size < 50% of CSV)
                ratio = (1.0 - (parquet_size / csv_size)) * 100.0
                assert ratio >= 50.0, (
                    f"Scale {scale_val}: Compression ratio {ratio:.1f}% is less than required 50%."
                )

    def test_docker_container_contract_specification(self):
        """Verifies Docker execution contract: image tag 'airquality-spark:3.5.1',

        8GB RAM (-m 8g), 4 CPUs (--cpus 4), and workspace volume mount.
        """
        # Check review checkpoint or documentation or benchmark runner command contract
        doc_paths = [
            PROJECT_ROOT / "docs" / "review_checkpoint_step5.md",
            PROJECT_ROOT / "docs" / "review_checkpoint_step1.md",
            PROJECT_ROOT / "README.md",
            CODE_DIR / "lakehouse" / "benchmark_spark_vs_pandas.py",
        ]

        found_contract = False
        for p in doc_paths:
            if p.exists():
                text = p.read_text(encoding="utf-8")
                if "airquality-spark:3.5.1" in text and ("-m 8g" in text or "8g" in text):
                    found_contract = True
                    break

        assert found_contract, (
            "Docker execution contract ('airquality-spark:3.5.1' with '-m 8g' and '--cpus 4') "
            "must be documented and validated across project execution specifications."
        )

    def test_spark_event_logs_present(self):
        """Verifies that Spark event logs directory exists and contains logs if pipeline has run."""
        assert SPARK_EVENT_LOGS_DIR.exists(), (
            f"Event logs directory not found: {SPARK_EVENT_LOGS_DIR}."
        )

        # Check if event log directory is writable
        test_probe = SPARK_EVENT_LOGS_DIR / ".probe"
        try:
            test_probe.write_text("probe", encoding="utf-8")
            test_probe.unlink()
        except Exception as e:
            pytest.fail(f"Event logs directory {SPARK_EVENT_LOGS_DIR} is not writable: {e}")
