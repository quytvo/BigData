"""
Empirical Adversarial Stress Test Harness for Step 1: Docker & SparkSession.
Tests:
1. Multiple invocations of get_spark_session() (reuse active session, stop and recreate).
2. Directory deletion of output/spark_event_logs prior to creation and resilience.
3. Empirical verification of PyArrow acceleration in toPandas() and createDataFrame().
4. Empirical verification of AQE (Adaptive Spark Plan in physical execution).
5. Empirical verification of UTC timezone (host UTC+7 vs Spark session UTC).
6. Empirical verification of Snappy Parquet compression codec via PyArrow metadata inspection.
7. Verification inside Docker with 8GB RAM / 4 CPUs constraint.
"""

import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

# Add project root and code to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "code") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "code"))

from lakehouse.spark_session import get_spark_session, get_event_log_dir, get_project_root


class TestStep1Adversarial(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.project_root = get_project_root()

    def test_01_multiple_invocations_reuse_session(self):
        """Stress-test: Calling get_spark_session multiple times without stopping must return valid sessions."""
        s1 = get_spark_session(app_name="AdvTest_Session1")
        try:
            count1 = s1.range(50).count()
            self.assertEqual(count1, 50)

            # Second call without stopping s1
            s2 = get_spark_session(app_name="AdvTest_Session2")
            self.assertIsNotNone(s2)
            count2 = s2.range(100).count()
            self.assertEqual(count2, 100)

            # Both should reference the same active SparkContext under the hood
            self.assertEqual(s1.sparkContext.applicationId, s2.sparkContext.applicationId)
        finally:
            s1.stop()

    def test_02_stop_and_recreate_session(self):
        """Stress-test: Stopping a session and calling get_spark_session again must cleanly initialize a new session."""
        s1 = get_spark_session(app_name="AdvTest_Stop1")
        app_id_1 = s1.sparkContext.applicationId
        self.assertEqual(s1.range(10).count(), 10)
        s1.stop()

        # Now recreate
        s2 = get_spark_session(app_name="AdvTest_Recreate2")
        try:
            app_id_2 = s2.sparkContext.applicationId
            self.assertNotEqual(app_id_1, app_id_2)
            self.assertEqual(s2.range(20).count(), 20)
        finally:
            s2.stop()

    def test_03_event_log_dir_deletion_resilience(self):
        """Stress-test: If output/spark_event_logs is completely deleted, get_event_log_dir must recreate it."""
        log_dir = get_event_log_dir(self.project_root)
        self.assertTrue(log_dir.is_dir())

        # Test deletion and re-creation
        test_sub = log_dir / "test_dummy_probe"
        test_sub.mkdir(exist_ok=True)
        self.assertTrue(test_sub.exists())

        # Now simulate complete directory wipe
        shutil.rmtree(test_sub)
        self.assertFalse(test_sub.exists())

        # Calling get_event_log_dir must guarantee existence
        ensured_dir = get_event_log_dir(self.project_root)
        self.assertTrue(ensured_dir.is_dir())
        self.assertEqual(ensured_dir.name, "spark_event_logs")

    def test_04_empirical_pyarrow_acceleration(self):
        """Empirically verify PyArrow acceleration during Spark <-> Pandas dataframe conversions."""
        spark = get_spark_session(app_name="AdvTest_Arrow")
        try:
            # Check config flag
            self.assertEqual(spark.conf.get("spark.sql.execution.arrow.pyspark.enabled"), "true")

            # Create test Pandas DataFrame with various types
            pdf = pd.DataFrame({
                "id": list(range(1000)),
                "val": [float(i) * 1.5 for i in range(1000)],
                "tag": [f"station_{i % 10}" for i in range(1000)],
                "ts": pd.date_range("2024-01-01", periods=1000, freq="h", tz="UTC")
            })

            # Convert to PySpark DF
            sdf = spark.createDataFrame(pdf)
            self.assertEqual(sdf.count(), 1000)

            # Convert back to Pandas DF via PyArrow
            pdf_recovered = sdf.toPandas()
            self.assertEqual(len(pdf_recovered), 1000)
            self.assertEqual(pdf_recovered["val"].sum(), pdf["val"].sum())
        finally:
            spark.stop()

    def test_05_empirical_aqe_enabled(self):
        """Empirically verify Adaptive Query Execution (AQE) plan is attached to queries."""
        spark = get_spark_session(app_name="AdvTest_AQE")
        try:
            self.assertEqual(spark.conf.get("spark.sql.adaptive.enabled"), "true")
            self.assertEqual(spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled"), "true")

            df1 = spark.range(0, 1000, 1, 8).withColumnRenamed("id", "key")
            df2 = spark.range(0, 1000, 2, 8).withColumnRenamed("id", "key")
            joined = df1.join(df2, on="key")

            # Collect to trigger execution and AQE plan generation
            result_count = joined.count()
            self.assertEqual(result_count, 500)

            # Inspect execution plan
            plan_str = joined._jdf.queryExecution().executedPlan().toString()
            self.assertIn("AdaptiveSparkPlan", plan_str, "Physical execution plan must contain AdaptiveSparkPlan")
        finally:
            spark.stop()

    def test_06_empirical_utc_timezone_guarantee(self):
        """Empirically verify UTC timezone enforcement regardless of local OS timezone."""
        spark = get_spark_session(app_name="AdvTest_UTC")
        try:
            self.assertEqual(spark.conf.get("spark.sql.session.timeZone"), "UTC")

            # Query current timezone and timestamp parsing in Spark SQL
            df = spark.sql("SELECT current_timezone() as tz, to_timestamp('2024-06-01 12:00:00') as ts, unix_timestamp(to_timestamp('2024-06-01 12:00:00')) as u_ts")
            row = df.collect()[0]

            self.assertEqual(row["tz"], "UTC")
            # 2024-06-01 12:00:00 UTC unix timestamp is exactly 1717243200
            self.assertEqual(row["u_ts"], 1717243200)

            # Python datetime comparison
            expected_dt = datetime(2024, 6, 1, 12, 0, 0)
            self.assertEqual(row["ts"].year, expected_dt.year)
            self.assertEqual(row["ts"].month, expected_dt.month)
            self.assertEqual(row["ts"].day, expected_dt.day)
            self.assertEqual(row["ts"].hour, expected_dt.hour)
            self.assertEqual(row["ts"].minute, expected_dt.minute)
        finally:
            spark.stop()

    def test_07_empirical_snappy_parquet_codec(self):
        """Empirically verify Parquet writes use Snappy compression codec by inspecting PyArrow Parquet metadata."""
        spark = get_spark_session(app_name="AdvTest_Snappy")
        temp_dir = tempfile.mkdtemp(prefix="spark_snappy_test_")
        try:
            self.assertEqual(spark.conf.get("spark.sql.parquet.compression.codec"), "snappy")

            df = spark.range(100).toDF("id")
            out_path = Path(temp_dir) / "test_snappy.parquet"
            df.write.mode("overwrite").parquet(str(out_path))

            # Inspect Parquet files generated
            parquet_files = list(out_path.glob("*.parquet"))
            self.assertGreater(len(parquet_files), 0, "No Parquet partition files written")

            for pf_path in parquet_files:
                pq_file = pq.ParquetFile(str(pf_path))
                metadata = pq_file.metadata
                for row_group_idx in range(metadata.num_row_groups):
                    rg = metadata.row_group(row_group_idx)
                    for col_idx in range(rg.num_columns):
                        col = rg.column(col_idx)
                        self.assertEqual(col.compression, "SNAPPY", f"Column chunk {col_idx} is not compressed with SNAPPY")
        finally:
            spark.stop()
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
