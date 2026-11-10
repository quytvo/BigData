## Forensic Audit Report — Milestone 2 Iteration 2 (Forensic Re-Audit)

**Work Product**: `code/lakehouse/spark_preprocess_tedp.py`, `code/lakehouse/spark_sync_province.py`, `lakehouse_storage/silver/`, `tests/test_step2_bronze_silver.py`  
**Profile**: General Project (Development Integrity Mode)  
**Verdict**: **CLEAN**

---

### Executive Summary

A comprehensive forensic re-audit was executed on Milestone 2 (Step 2: Bronze to Silver Preprocessing Pipeline) following the remediation of the partition collision and purge logic defect identified in Iteration 1.

All previously identified integrity violations have been completely remediated:
1. **Partition Directory Collision Remediated**: `lakehouse_storage/silver/tedp_station/` now contains **exactly 11 partition subdirectories** (matching `province_name=*`). All 4 colliding unencoded directory duplicates (`Gia Lai`, `Lâm Đồng`, `Tây Ninh`, `Đà Nẵng`) have been purged.
2. **Data Duplication Eliminated**: The total row count in `lakehouse_storage/silver/tedp_station/` has decreased from 549,100 rows to **exactly 440,012 genuine rows**, matching station records without duplication.
3. **Deterministic Purge Implementation Verified**: `purge_output_directory` is authentically implemented in both `spark_preprocess_tedp.py` (lines 277–311) and `spark_sync_province.py` (lines 382–416). It features Windows read-only permission reset (`stat.S_IWRITE`), 3-attempt retry loop, and post-deletion existence checks prior to calling Spark Parquet write operations.
4. **Authenticity & Prohibited API Verification**: AST parsing confirms **ZERO `applyInPandas`**, **ZERO Python `@udf` / `udf()`**, and **ZERO `inferSchema=True`**. PyArrow Parquet metadata confirms **100% Snappy compression** and Apache Spark JVM (`parquet-mr version 1.12.3`) provenance. Spark event logs confirm **0 Python worker invocations**.
5. **Test Suite Verification**: `pytest tests/test_step2_bronze_silver.py -v` passes **100% (7/7 PASSED)** on both Windows Host (2.37s) and inside the official Linux Docker container `airquality-spark:3.5.1` (5.13s). Additionally, the adversarial test suite `pytest tests/test_step2_adversarial.py -v` passes **100% (8/8 PASSED)** on both environments.

All forensic acceptance criteria under Development Integrity Mode are fully satisfied with zero defects. The final verdict is **CLEAN**.

---

### Phase Results

| # | Forensic Check | Result | Details |
|---|---|:---:|---|
| 1 | **Partition Directory Cardinality Re-Audit** | **PASS** | Exactly 11 partition subdirectories in `lakehouse_storage/silver/tedp_station/` (11/11 target provinces, 0 unencoded duplicate paths). |
| 2 | **Row Count & De-duplication Verification** | **PASS** | `tedp_station` total rows: exactly 440,012 rows (zero duplicates). `tedp_province`: exactly 280,632 rows (11 provinces x 25,512 continuous hours). `weather_province`: exactly 280,632 rows. |
| 3 | **Deterministic Purge Mechanism (`purge_output_directory`)** | **PASS** | Implemented with `shutil.rmtree`, `stat.S_IWRITE` permission reset, and 3 retries in both `spark_preprocess_tedp.py` and `spark_sync_province.py`. Verified via stress tests. |
| 4 | **Prohibited Spark APIs Detection** | **PASS** | AST analysis reveals ZERO `applyInPandas`, ZERO `@udf` / `udf()`, ZERO `pandas_udf`, ZERO `toPandas`, ZERO `inferSchema=True`. |
| 5 | **Facade & Baseline CSV Cheating Detection** | **PASS** | Real Catalyst Window bidirectional interpolation and median fill. ZERO references to Pandas benchmark CSVs in Lakehouse transformation code. |
| 6 | **Parquet Format & JVM Provenance** | **PASS** | 100% Snappy compression across all column chunks. Metadata confirms `parquet-mr version 1.12.3` and Spark JVM writer. 0 Python worker occurrences in event logs `local-1794197880655` and `local-1794197936327`. |
| 7 | **Numerical Equivalence vs Pandas Baseline** | **PASS** | Maximum absolute difference vs `cleaned_tedp_province_4y.csv`: `pm25` = $8.53 \times 10^{-14}$, `pm10` = $2.84 \times 10^{-14}$, `aqi` = $8.53 \times 10^{-14}$ (all $< 10^{-5}$). |
| 8 | **Test Suite Execution (Host & Docker)** | **PASS** | `tests/test_step2_bronze_silver.py`: 7/7 PASSED on Host (2.37s) and Docker (5.13s). `tests/test_step2_adversarial.py`: 8/8 PASSED on Host (4.64s) and Docker (6.09s). |

---

### Evidence

#### 1. Empirical Partition Directory Listing (`lakehouse_storage/silver/tedp_station/`)
```
Name                      
----                      
province_name=Bắc%20Ninh  
province_name=Gia%20Lai   
province_name=HCM         
province_name=Hà%20Nội    
province_name=Hưng%20Yên  
province_name=Hải%20Phòng 
province_name=Lâm%20Đồng  
province_name=Ninh%20Bình 
province_name=Quảng%20Ngãi
province_name=Tây%20Ninh  
province_name=Đà%20Nẵng   
```
Total partition directory count: **11** (Clean, 0 URL-encoded duplicate collisions).

#### 2. Row Count Verification
PyArrow dataset inspection on `lakehouse_storage/silver/tedp_station`:
```
Total rows in tedp_station: 440,012
Row count by province_name:
HCM           101475
Hưng Yên       51327
Đà Nẵng        51076
Hà Nội         50482
Gia Lai        32638
Bắc Ninh       25849
Hải Phòng      25626
Quảng Ngãi     25617
Lâm Đồng       25590
Tây Ninh       25483
Ninh Bình      24849
Total: 440,012 rows (zero duplicate rows).
```

PyArrow dataset inspection on `lakehouse_storage/silver/tedp_province` and `lakehouse_storage/silver/weather_province`:
```
Total rows in tedp_province: 280,632 (25,512 rows across each of 11 provinces)
Total rows in weather_province: 280,632 (25,512 rows across each of 11 provinces)
```

#### 3. AST Analysis for Prohibited APIs
AST inspection script executed across `spark_preprocess_tedp.py`, `spark_sync_province.py`, and `spark_session.py`:
```
=== AST checking: code\lakehouse\spark_preprocess_tedp.py ===
=== AST checking: code\lakehouse\spark_sync_province.py ===
=== AST checking: code\lakehouse\spark_session.py ===
Prohibited calls/decorators found: []
inferSchema=True violations found: []
AST analysis: CLEAN (0 prohibited APIs, 0 inferSchema=True)
```

#### 4. Parquet Metadata & JVM Provenance
PyArrow metadata verification across all Parquet files:
```
Table: tedp_station, Parquet files count: 16 (100% SNAPPY, created_by: parquet-mr version 1.12.3)
Table: tedp_province, Parquet files count: 11 (100% SNAPPY, created_by: parquet-mr version 1.12.3)
Table: weather_province, Parquet files count: 14 (100% SNAPPY, created_by: parquet-mr version 1.12.3)
```

Spark event log audit (`output/spark_event_logs/local-1794197880655` & `local-1794197936327`):
```
local-1794197880655: 0 Python worker occurrences found.
local-1794197936327: 0 Python worker occurrences found.
```

#### 5. Numerical Reconciliation vs Reference Pandas CSV
Comparison between `lakehouse_storage/silver/tedp_province` and `data_test/cleaned_tedp_province_4y.csv` (280,632 rows):
```
Merged rows: 280,632
Column pm25: max_diff = 8.526513e-14, mean_diff = 5.752895e-16 (within 1e-5: True)
Column pm10: max_diff = 2.842171e-14, mean_diff = 4.591360e-16 (within 1e-5: True)
Column aqi:  max_diff = 8.526513e-14, mean_diff = 5.859402e-16 (within 1e-5: True)
Discrete and string columns match: 100.00%
```

#### 6. Test Suite Execution Logs
**Host (Windows PowerShell, Python 3.11.8, pytest 9.2.0)**:
```
============================= test session starts =============================
platform win32 -- Python 3.11.8, pytest-9.2.0, pluggy-1.7.0 -- python.exe
configfile: pytest.ini
collected 7 items

tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_station_dataset_integrity PASSED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_province_directory_and_partitioning PASSED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_province_static_struct_schema PASSED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_province_row_count_and_continuity PASSED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_province_zero_nulls_in_core_metrics PASSED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_province_numerical_tolerance_vs_pandas PASSED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_weather_province_integrity_and_tolerance PASSED

============================== 7 passed in 2.37s ==============================
```

**Docker Container (`airquality-spark:3.5.1`, Linux Ubuntu, Python 3.8.10, pytest 8.3.5)**:
```
============================= test session starts ==============================
platform linux -- Python 3.8.10, pytest-8.3.5, pluggy-1.5.0 -- /usr/bin/python3
configfile: pytest.ini
collected 7 items

tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_station_dataset_integrity PASSED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_province_directory_and_partitioning PASSED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_province_static_struct_schema PASSED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_province_row_count_and_continuity PASSED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_province_zero_nulls_in_core_metrics PASSED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_province_numerical_tolerance_vs_pandas PASSED
tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_weather_province_integrity_and_tolerance PASSED

============================== 7 passed in 5.13s ===============================
```

**Adversarial Suite Execution (`tests/test_step2_adversarial.py`)**:
- Host: `8 passed in 4.64s`
- Docker Container: `8 passed in 6.09s`
