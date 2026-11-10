"""
Independent Verification Test Suite for Step 1:
- Dockerfile.spark specifications
- spark_session.py configuration parameters and constraints
"""

import os
import re
import unittest
from pathlib import Path


class TestStep1DockerSpark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Locate project root
        cls.project_root = Path(__file__).resolve().parents[1]
        cls.dockerfile_path = cls.project_root / "Dockerfile.spark"
        cls.spark_session_module_path = cls.project_root / "code" / "lakehouse" / "spark_session.py"

    def test_01_dockerfile_exists(self):
        """Verify Dockerfile.spark exists in the required location."""
        self.assertTrue(self.dockerfile_path.is_file(), f"Dockerfile.spark not found at {self.dockerfile_path}")

    def test_02_dockerfile_base_image(self):
        """Verify Dockerfile.spark is based on apache/spark:3.5.1-python3."""
        content = self.dockerfile_path.read_text(encoding="utf-8")
        self.assertRegex(
            content,
            r"FROM\s+apache/spark:3\.5\.1-python3",
            "Dockerfile.spark must use apache/spark:3.5.1-python3 base image",
        )

    def test_03_dockerfile_dependencies(self):
        """Verify Dockerfile.spark installs all required dependencies: numpy, pandas, pyarrow, psutil, pytest, torch."""
        content = self.dockerfile_path.read_text(encoding="utf-8")
        required_deps = ["numpy", "pandas", "pyarrow", "psutil", "pytest", "torch"]
        for dep in required_deps:
            self.assertTrue(
                re.search(rf"\b{dep}\b", content),
                f"Dependency '{dep}' missing from Dockerfile.spark",
            )

    def test_04_dockerfile_workspace_and_port(self):
        """Verify Dockerfile.spark sets WORKDIR /workspace and EXPOSE 4040."""
        content = self.dockerfile_path.read_text(encoding="utf-8")
        self.assertRegex(content, r"WORKDIR\s+/workspace", "WORKDIR must be /workspace")
        self.assertRegex(content, r"EXPOSE\s+4040", "Port 4040 must be exposed")

    def test_05_dockerfile_environment_variables(self):
        """Verify Dockerfile.spark sets SPARK_HOME and PYTHONPATH."""
        content = self.dockerfile_path.read_text(encoding="utf-8")
        self.assertIn("SPARK_HOME", content)
        self.assertIn("PYTHONPATH", content)

    def test_06_spark_session_file_exists(self):
        """Verify code/lakehouse/spark_session.py exists."""
        self.assertTrue(
            self.spark_session_module_path.is_file(),
            f"spark_session.py not found at {self.spark_session_module_path}",
        )

    def test_07_spark_session_importable(self):
        """Verify code/lakehouse/spark_session.py can be imported."""
        import sys
        code_dir = str(self.project_root / "code")
        if code_dir not in sys.path:
            sys.path.insert(0, code_dir)
        import lakehouse.spark_session as ss
        self.assertTrue(hasattr(ss, "get_spark_session"))
        self.assertTrue(hasattr(ss, "wait_for_webui"))
        self.assertTrue(hasattr(ss, "get_project_root"))
        self.assertTrue(hasattr(ss, "get_event_log_dir"))

    def test_08_event_log_dir_creation(self):
        """Verify get_event_log_dir ensures the output/spark_event_logs directory exists."""
        import sys
        code_dir = str(self.project_root / "code")
        if code_dir not in sys.path:
            sys.path.insert(0, code_dir)
        import lakehouse.spark_session as ss
        log_dir = ss.get_event_log_dir(self.project_root)
        self.assertTrue(log_dir.is_dir(), f"Event log directory not created at {log_dir}")
        self.assertEqual(log_dir.name, "spark_event_logs")

    def test_09_spark_session_configurations(self):
        """Verify SparkSession created by get_spark_session contains all mandated configuration properties."""
        import sys
        code_dir = str(self.project_root / "code")
        if code_dir not in sys.path:
            sys.path.insert(0, code_dir)
        import lakehouse.spark_session as ss

        # On Windows host without winutils, eventLog is auto-disabled to avoid JVM crash.
        # Inside Docker, it is True. We test the builder output.
        spark = ss.get_spark_session(
            app_name="UnitTest_Step1",
            keep_ui=False,
            master="local[4]",
            driver_memory="5g",
            shuffle_partitions="16",
        )
        try:
            conf = spark.conf
            self.assertEqual(conf.get("spark.master"), "local[4]")
            self.assertEqual(conf.get("spark.app.name"), "UnitTest_Step1")
            self.assertEqual(conf.get("spark.driver.memory"), "5g")
            self.assertEqual(conf.get("spark.sql.shuffle.partitions"), "16")
            self.assertEqual(conf.get("spark.sql.execution.arrow.pyspark.enabled"), "true")
            self.assertEqual(conf.get("spark.sql.adaptive.enabled"), "true")
            self.assertEqual(conf.get("spark.sql.session.timeZone"), "UTC")
            self.assertEqual(conf.get("spark.sql.parquet.compression.codec"), "snappy")
            self.assertEqual(conf.get("spark.ui.port"), "4040")

            # Check that wait_for_webui helper was attached
            self.assertTrue(hasattr(spark, "wait_for_webui"))
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
