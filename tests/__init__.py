"""PyTest test suite package for PySpark Air Quality Lakehouse."""
import sys
from pathlib import Path

# Ensure project root and tests directory are in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TESTS_DIR = Path(__file__).resolve().parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))
