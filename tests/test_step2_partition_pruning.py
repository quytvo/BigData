"""
test_step2_partition_pruning.py
================================
Adversarial Partition Pruning & Physical Plan Verification Suite for Step 2.
Independently written by Challenger 2 to empirically verify partition pruning
and physical plan predicate pushdown on both tedp_province/ and tedp_station/.

Checks:
1. Exact partition directory count (11) and naming convention in both tables.
2. PyArrow partition pruning file-scan isolation (only matching partition scanned).
3. Exact row count preservation under partition filtering for all 11 provinces.
4. PySpark Catalyst physical plan execution check: PartitionFilters pushdown.
5. Zero-leakage across partition boundaries.
6. Non-existent partition filter handling (returns 0 rows, 0 files).
7. Snappy compression validation across all partition files.
"""

from pathlib import Path
import urllib.parse
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_PROVINCE_DIR = PROJECT_ROOT / "lakehouse_storage" / "silver" / "tedp_province"
SILVER_STATION_DIR = PROJECT_ROOT / "lakehouse_storage" / "silver" / "tedp_station"

TARGET_PROVINCES = [
    "Bắc Ninh",
    "Gia Lai",
    "HCM",
    "Hà Nội",
    "Hưng Yên",
    "Hải Phòng",
    "Lâm Đồng",
    "Ninh Bình",
    "Quảng Ngãi",
    "Tây Ninh",
    "Đà Nẵng",
]

EXPECTED_STATION_ROWS = {
    "HCM": 101475,
    "Hưng Yên": 51327,
    "Đà Nẵng": 51076,
    "Hà Nội": 50482,
    "Gia Lai": 32638,
    "Bắc Ninh": 25849,
    "Hải Phòng": 25626,
    "Quảng Ngãi": 25617,
    "Lâm Đồng": 25590,
    "Tây Ninh": 25483,
    "Ninh Bình": 24849,
}

EXPECTED_TOTAL_STATION_ROWS = 440012
EXPECTED_PROVINCE_HOURS = 25512
EXPECTED_TOTAL_PROVINCE_ROWS = 280632


class TestStep2PartitionPruning:
    """Verifies partition layout, pruning effectiveness, and physical pushdown."""

    def test_01_directory_partition_inventory(self):
        """Verifies exactly 11 partition subdirectories exist in both Silver datasets."""
        for name, dir_path in [("tedp_province", SILVER_PROVINCE_DIR), ("tedp_station", SILVER_STATION_DIR)]:
            assert dir_path.exists(), f"Directory not found: {dir_path}"
            partition_dirs = [d.name for d in dir_path.iterdir() if d.is_dir() and d.name.startswith("province_name=")]
            assert len(partition_dirs) == 11, (
                f"[{name}] Expected exactly 11 partition subdirectories, found {len(partition_dirs)}: {partition_dirs}"
            )
            decoded = [urllib.parse.unquote(p.replace("province_name=", "")) for p in partition_dirs]
            assert sorted(decoded) == sorted(TARGET_PROVINCES), (
                f"[{name}] Decoded provinces do not match target: {sorted(decoded)}"
            )

    def test_02_pyarrow_partition_pruning_tedp_province(self):
        """Verifies PyArrow loads exclusively the target partition file for tedp_province."""
        dataset = ds.dataset(str(SILVER_PROVINCE_DIR), format="parquet", partitioning="hive")

        for province in TARGET_PROVINCES:
            filt = ds.field("province_name") == province
            fragments = list(dataset.get_fragments(filter=filt))
            assert len(fragments) > 0, f"No fragments returned for province '{province}'"
            # Verify file fragments visited belong only to this province
            for fragment in fragments:
                frag_path_unquoted = urllib.parse.unquote(fragment.path)
                assert f"province_name={province}" in frag_path_unquoted, (
                    f"Partition pruning leak: scanned fragment '{fragment.path}' when querying '{province}'"
                )

            table = dataset.to_table(filter=filt)
            assert table.num_rows == EXPECTED_PROVINCE_HOURS, (
                f"Partition for '{province}' returned {table.num_rows} rows, expected {EXPECTED_PROVINCE_HOURS}"
            )
            unique_provinces = table["province_name"].unique().to_pylist()
            assert unique_provinces == [province], (
                f"Expected only [{province}], got {unique_provinces}"
            )

    def test_03_pyarrow_partition_pruning_tedp_station(self):
        """Verifies PyArrow loads exclusively the target partition files for tedp_station."""
        dataset = ds.dataset(str(SILVER_STATION_DIR), format="parquet", partitioning="hive")

        total_station_rows_pruned = 0
        for province, exp_count in EXPECTED_STATION_ROWS.items():
            filt = ds.field("province_name") == province
            fragments = list(dataset.get_fragments(filter=filt))
            assert len(fragments) > 0, f"No fragments returned for station province '{province}'"
            for fragment in fragments:
                frag_path_unquoted = urllib.parse.unquote(fragment.path)
                assert f"province_name={province}" in frag_path_unquoted, (
                    f"Partition pruning leak: scanned fragment '{fragment.path}' when querying '{province}'"
                )

            table = dataset.to_table(filter=filt)
            assert table.num_rows == exp_count, (
                f"Station partition '{province}' returned {table.num_rows} rows, expected {exp_count}"
            )
            unique_provinces = table["province_name"].unique().to_pylist()
            assert unique_provinces == [province], (
                f"Expected only [{province}], got {unique_provinces}"
            )
            total_station_rows_pruned += table.num_rows

        assert total_station_rows_pruned == EXPECTED_TOTAL_STATION_ROWS, (
            f"Sum of pruned rows {total_station_rows_pruned} != {EXPECTED_TOTAL_STATION_ROWS}"
        )

    def test_04_pyarrow_non_existent_partition_pruning(self):
        """Verifies filtering by a non-existent province returns 0 fragments and 0 rows."""
        for name, dir_path in [("tedp_province", SILVER_PROVINCE_DIR), ("tedp_station", SILVER_STATION_DIR)]:
            dataset = ds.dataset(str(dir_path), format="parquet", partitioning="hive")
            filt = ds.field("province_name") == "NonExistentProvince"
            fragments = list(dataset.get_fragments(filter=filt))
            assert len(fragments) == 0, f"[{name}] Expected 0 fragments for non-existent province, got {len(fragments)}"
            table = dataset.to_table(filter=filt)
            assert table.num_rows == 0, f"[{name}] Expected 0 rows, got {table.num_rows}"

    def test_05_pyarrow_multi_partition_pruning(self):
        """Verifies filtering by multiple provinces scans only those partitions."""
        target_subset = ["Hà Nội", "HCM"]
        exp_prov_rows = EXPECTED_PROVINCE_HOURS * len(target_subset)
        exp_stat_rows = EXPECTED_STATION_ROWS["Hà Nội"] + EXPECTED_STATION_ROWS["HCM"]

        # tedp_province
        dataset_p = ds.dataset(str(SILVER_PROVINCE_DIR), format="parquet", partitioning="hive")
        filt_p = ds.field("province_name").isin(target_subset)
        frags_p = list(dataset_p.get_fragments(filter=filt_p))
        assert len(frags_p) > 0
        for frag in frags_p:
            frag_unquoted = urllib.parse.unquote(frag.path)
            assert any(f"province_name={p}" in frag_unquoted for p in target_subset), (
                f"Scanned unexpected fragment '{frag.path}'"
            )
        table_p = dataset_p.to_table(filter=filt_p)
        assert table_p.num_rows == exp_prov_rows

        # tedp_station
        dataset_s = ds.dataset(str(SILVER_STATION_DIR), format="parquet", partitioning="hive")
        filt_s = ds.field("province_name").isin(target_subset)
        frags_s = list(dataset_s.get_fragments(filter=filt_s))
        assert len(frags_s) > 0
        for frag in frags_s:
            frag_unquoted = urllib.parse.unquote(frag.path)
            assert any(f"province_name={p}" in frag_unquoted for p in target_subset), (
                f"Scanned unexpected fragment '{frag.path}'"
            )
        table_s = dataset_s.to_table(filter=filt_s)
        assert table_s.num_rows == exp_stat_rows

    def test_06_snappy_compression_all_partitions(self):
        """Verifies all Parquet files in all partitions use snappy compression."""
        for name, dir_path in [("tedp_province", SILVER_PROVINCE_DIR), ("tedp_station", SILVER_STATION_DIR)]:
            parquet_files = list(dir_path.rglob("*.parquet"))
            assert len(parquet_files) > 0, f"[{name}] No parquet files found."
            for pf in parquet_files:
                meta = pq.read_metadata(pf)
                for r in range(meta.num_row_groups):
                    rg = meta.row_group(r)
                    for c in range(rg.num_columns):
                        col = rg.column(c)
                        assert col.compression.upper() == "SNAPPY", (
                            f"[{name}] File {pf.name} column {col.path_in_schema} compressed with {col.compression}, expected SNAPPY"
                        )


class TestStep2PySparkCatalystPruning:
    """Verifies PySpark Catalyst Physical Plan PartitionFilters pushdown in Docker."""

    @pytest.fixture(scope="class")
    def spark(self):
        """Creates or gets an active SparkSession."""
        try:
            from code.lakehouse.spark_session import get_spark_session
            spark = get_spark_session(app_name="TestPartitionPruning")
        except Exception:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.master("local[2]").appName("TestPartitionPruning").getOrCreate()
        yield spark

    def test_07_pyspark_partition_filters_pushdown_province(self, spark):
        """Verifies PySpark physical plan contains PartitionFilters on tedp_province."""
        from pyspark.sql import functions as F

        df = spark.read.parquet(str(SILVER_PROVINCE_DIR))
        filtered = df.filter(F.col("province_name") == "Hà Nội")

        plan = filtered._jdf.queryExecution().executedPlan().toString()
        # Verify PartitionFilters is present in FileScan
        assert "PartitionFilters:" in plan, f"PartitionFilters missing from physical plan:\n{plan}"
        assert "Hà Nội" in plan, f"'Hà Nội' predicate missing from physical plan PartitionFilters:\n{plan}"

        count = filtered.count()
        assert count == EXPECTED_PROVINCE_HOURS, f"Filtered row count {count} != {EXPECTED_PROVINCE_HOURS}"

        distinct_prov = [row["province_name"] for row in filtered.select("province_name").distinct().collect()]
        assert distinct_prov == ["Hà Nội"]

    def test_08_pyspark_partition_filters_pushdown_station(self, spark):
        """Verifies PySpark physical plan contains PartitionFilters on tedp_station."""
        from pyspark.sql import functions as F

        df = spark.read.parquet(str(SILVER_STATION_DIR))
        filtered = df.filter(F.col("province_name") == "Đà Nẵng")

        plan = filtered._jdf.queryExecution().executedPlan().toString()
        assert "PartitionFilters:" in plan, f"PartitionFilters missing from physical plan:\n{plan}"
        assert "Đà Nẵng" in plan, f"'Đà Nẵng' predicate missing from physical plan PartitionFilters:\n{plan}"

        count = filtered.count()
        assert count == EXPECTED_STATION_ROWS["Đà Nẵng"], (
            f"Filtered row count {count} != {EXPECTED_STATION_ROWS['Đà Nẵng']}"
        )

        distinct_prov = [row["province_name"] for row in filtered.select("province_name").distinct().collect()]
        assert distinct_prov == ["Đà Nẵng"]
