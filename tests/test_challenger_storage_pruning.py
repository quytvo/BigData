"""Adversarial Storage & Partition Pruning Verification Test Suite for Step 3.

Empirically tests:
1. Snappy compression codec on 100% of Parquet files and column chunks in Gold table.
2. Strict schema data types: all 49 features + target y are DoubleType/FloatType, no strings.
3. Partition Pruning on Gold table:
   - PyArrow dataset scan pruning when filtering by province_name == 'Hà Nội' (exactly 1 partition).
   - PySpark Catalyst physical plan execution and partition filter pushdown.
   - Multi-province pruning and non-existent partition handling.
"""

from pathlib import Path
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType, IntegerType, StringType

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_DIR = PROJECT_ROOT / "lakehouse_storage" / "gold" / "global_ann_features"

CANONICAL_49_FEATURES = [
    "pm25_lag0", "pm25_lag1", "pm25_lag3", "pm25_lag6", "pm25_lag12", "pm25_lag24",
    "pm25_roll6", "pm25_roll24",
    "pm25_diff_1h", "pm25_diff_24h", "pm25_std24",
    "pm10_lag0", "aqi_lag0",
    "temperature_2m", "relative_humidity_2m", "dew_point_2m", "surface_pressure",
    "precipitation", "wind_speed_10m", "wind_direction_10m", "boundary_layer_height",
    "wind_dir_sin", "wind_dir_cos", "dew_point_deficit", "ventilation_coef",
    "temp_diff_24h", "pressure_diff_24h", "pblh_diff_24h", "precip_sum_24h",
    "hour", "day_of_week",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "latitude_norm", "longitude_norm",
    "is_weekend",
    "station_id_28429342215451431160939106654",
    "station_id_28429342215451431160939106668",
    "station_id_28429342215451431160939106697_hcm",
    "station_id_28429342215451431160939106671",
    "station_id_28429342215451431160939106677",
    "station_id_28429342215451431160939106674",
    "station_id_28429342215451431160939106682",
    "station_id_28429342215451431160939106688",
    "station_id_28429342215451431160939106694",
    "station_id_28429342215451431160939106700",
    "station_id_28429342215451431160939106662",
]


class TestAdversarialStorageAndPruning:
    """Rigorous empirical tests for storage compression, schema compliance, and partition pruning."""

    def test_parquet_snappy_compression_all_files_and_chunks(self):
        """Verifies that every single parquet file in the Gold table has SNAPPY compression on all column chunks."""
        parquet_files = list(GOLD_DIR.glob("**/*.parquet"))
        assert len(parquet_files) >= 11, f"Expected at least 11 Parquet files, found {len(parquet_files)}"

        total_chunks_inspected = 0
        for pfile in parquet_files:
            pf = pq.ParquetFile(pfile)
            meta = pf.metadata
            num_rg = meta.num_row_groups
            for rg_idx in range(num_rg):
                rg = meta.row_group(rg_idx)
                for col_idx in range(rg.num_columns):
                    col = rg.column(col_idx)
                    codec = str(col.compression).upper()
                    assert codec == "SNAPPY", (
                        f"File {pfile.name}, RowGroup {rg_idx}, Column {col.path_in_schema} "
                        f"uses compression '{codec}', expected 'SNAPPY'!"
                    )
                    total_chunks_inspected += 1

        print(f"\n[OK] Inspected {total_chunks_inspected} column chunks across {len(parquet_files)} files: 100% SNAPPY.")

    def test_gold_schema_data_types_no_strings_in_features(self):
        """Verifies that all 49 features + target y are strictly float64/double (no string or object types)."""
        dataset = pq.ParquetDataset(GOLD_DIR)
        schema = dataset.schema

        field_types = {field.name: str(field.type).lower() for field in schema}

        for feat in CANONICAL_49_FEATURES:
            assert feat in field_types, f"Feature '{feat}' missing from schema."
            ftype = field_types[feat]
            assert "string" not in ftype, f"Feature '{feat}' has illegal string type '{ftype}'!"
            assert "double" in ftype or "float" in ftype, f"Feature '{feat}' has unexpected type '{ftype}'!"

        y_type = field_types.get("y", "")
        assert "double" in y_type or "float" in y_type, f"Label 'y' has unexpected type '{y_type}'!"

        # Station metadata fields must have appropriate string / integer types
        assert "string" in field_types.get("station_id", "")
        assert "string" in field_types.get("station_code", "")
        assert "string" in field_types.get("station_name", "")
        assert "string" in field_types.get("province_id", "")
        assert "int" in field_types.get("station_idx", "")

    def test_pyarrow_partition_pruning(self):
        """Verifies PyArrow dataset partition pruning when filtering by province_name == 'Hà Nội'."""
        pyarrow_dataset = ds.dataset(str(GOLD_DIR), partitioning="hive")

        target_prov = "Hà Nội"
        filter_expr = ds.field("province_name") == target_prov
        scanner = pyarrow_dataset.scanner(filter=filter_expr)

        # Inspect scanned fragments and rows
        table = scanner.to_table()
        total_rows = table.num_rows

        # In full 280,104 rows, each province has exactly 25,464 rows
        assert total_rows == 25464, (
            f"Expected exactly 25,464 rows for '{target_prov}', got {total_rows}"
        )

        # Verify only fragments containing Hà Nội were scanned
        matching_fragments = list(pyarrow_dataset.get_fragments(filter=filter_expr))
        assert len(matching_fragments) == 1, (
            f"Expected exactly 1 partition fragment for '{target_prov}', found {len(matching_fragments)}"
        )
        frag_path = str(matching_fragments[0].path)
        assert "H%C3%A0%20N%E1%BB%99i" in frag_path or "Hà Nội" in frag_path, (
            f"Unexpected fragment scanned for filter '{target_prov}': {frag_path}"
        )

    def test_pyspark_catalyst_partition_pruning_and_physical_plan(self):
        """Verifies Catalyst query optimizer pushes down partition filters and prunes physical partition scans."""
        spark = SparkSession.builder \
            .appName("TestCatalystPartitionPruning") \
            .master("local[2]") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()

        try:
            df = spark.read.parquet(str(GOLD_DIR))
            filtered_df = df.filter(F.col("province_name") == "Hà Nội")

            # Check Catalyst Physical Execution Plan
            executed_plan = filtered_df._jdf.queryExecution().executedPlan().toString()
            print("\nCatalyst Executed Physical Plan:\n", executed_plan)

            # Assert partition filter pushdown exists in physical plan
            assert "PartitionFilters" in executed_plan, (
                f"Expected 'PartitionFilters' in physical plan, got: {executed_plan}"
            )
            assert "Hà Nội" in executed_plan, (
                f"Expected 'Hà Nội' in PartitionFilters, got: {executed_plan}"
            )

            # Check row count
            count = filtered_df.count()
            assert count == 25464, f"Expected 25,464 rows for 'Hà Nội', got {count}"

            # Test multi-partition pruning: Hà Nội and Đà Nẵng
            multi_df = df.filter(F.col("province_name").isin("Hà Nội", "Đà Nẵng"))
            multi_plan = multi_df._jdf.queryExecution().executedPlan().toString()
            assert "PartitionFilters" in multi_plan
            assert multi_df.count() == 25464 * 2

            # Test non-existent province returns 0 rows without error
            none_df = df.filter(F.col("province_name") == "NonExistentProvince")
            assert none_df.count() == 0

        finally:
            spark.stop()
