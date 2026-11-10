# Milestone 4 (Step 4) Adversarial Challenge Report

**Date**: 2026-11-09T05:20:00Z  
**Author**: Challenger 1 (Empirical Challenger)  
**Target Code**: `code/training/common.py`, `code/training/du_bao.py`, `code/training/train_global_ann.py`  
**Test Suite**: `tests/test_step4_model_integration.py`, `tests/test_step4_adversarial.py`  
**Overall Risk Assessment**: **LOW (PASS - ALL CHECKS EMPIRICALLY VERIFIED)**

---

## 1. Executive Summary

Milestone 4 (Step 4) successfully integrates the lakehouse storage architecture with model training and inference pipelines. As Empirical Challenger, rigorous stress tests and physical I/O spies were executed to verify:
1. **Partition Pruning Reality**: Verified via `pyarrow.fs.PyFileSystem` and `FileSystemHandler` instrumented tracing that requesting a single province partition (`"Hà Nội"`) scans strictly the target partition and never opens data fragments of the other 10 provinces.
2. **Data Integrity & Consistency**: All 11 provinces were tested in an exhaustive loop. Each province contains exactly **25,512 rows**, **38 identical columns**, **0 duplicate timestamps**, and **0 nulls** in weather or PM2.5 features.
3. **Transparent Fallback Resilience**: Tested against non-existent silver directories, completely empty silver directories, and missing weather storage. Under all stress scenarios, `common.load_merged()` gracefully and seamlessly fell back to CSV without unhandled exceptions.
4. **Downstream Integration**: Verified `code/training/du_bao.py` rapid inference partition pruning and `code/training/train_global_ann.py` direct Gold Parquet loading (280,104 samples, 49 features).

---

## 2. Empirical Verification & Adversarial Stress Tests

### Task 1: Empirical Partition Pruning & Physical File Scan Isolation

- **Method**: Created a custom `TracedFileSystemHandler` subclassing `pyarrow.fs.FileSystemHandler` to spy on all physical file open calls (`open_input_file`, `open_input_stream`) executed during:
  ```python
  pq.read_table("lakehouse_storage/silver/tedp_province", filesystem=fs, filters=[("province_name", "==", "Hà Nội")])
  ```
- **Observations**:
  - Exactly two files were opened during the operation:
    1. `lakehouse_storage/silver/tedp_province/province_name=Bắc%20Ninh/...snappy.parquet` (Opened exclusively by `FileSystemDatasetFactory.finish()` to inspect the dataset schema and metadata footers, as is standard in PyArrow dataset discovery).
    2. `lakehouse_storage/silver/tedp_province/province_name=Hà%20Nội/part-00003-e7fc3c3d-162a-45d4-ae6e-dbe8ccde4ee2.c000.snappy.parquet` (Opened by `ParquetDataset.to_table` to read data rows).
  - **Zero file access** to the remaining 9 provinces: Gia Lai, HCM, Hưng Yên, Hải Phòng, Lâm Đồng, Ninh Bình, Quảng Ngãi, Tây Ninh, and Đà Nẵng.
  - Returned DataFrame row count: **exactly 25,512 rows**.
  - Province values: **strictly `['Hà Nội']`**.
  - Duplicates on `(station_code, datetime)`: **0**.
  - Null values across all 38 columns: **0**.

### Task 2: Exhaustive 11 Provinces Loop Verification

All 11 provinces were evaluated sequentially in an empirical test loop with strict assertions:

| Province | Status | Rows | Duplicates | Missing Weather | PM2.5 NaNs | Columns |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| Bắc Ninh | PASS | 25,512 | 0 | 0 | 0 | 38 |
| Gia Lai | PASS | 25,512 | 0 | 0 | 0 | 38 |
| HCM | PASS | 25,512 | 0 | 0 | 0 | 38 |
| Hà Nội | PASS | 25,512 | 0 | 0 | 0 | 38 |
| Hưng Yên | PASS | 25,512 | 0 | 0 | 0 | 38 |
| Hải Phòng | PASS | 25,512 | 0 | 0 | 0 | 38 |
| Lâm Đồng | PASS | 25,512 | 0 | 0 | 0 | 38 |
| Ninh Bình | PASS | 25,512 | 0 | 0 | 0 | 38 |
| Quảng Ngãi | PASS | 25,512 | 0 | 0 | 0 | 38 |
| Tây Ninh | PASS | 25,512 | 0 | 0 | 0 | 38 |
| Đà Nẵng | PASS | 25,512 | 0 | 0 | 0 | 38 |
| **TOTAL** | **PASS** | **280,632** | **0** | **0** | **0** | **38** |

All 11 provinces exhibited identical column names, ordering, and data types, confirming zero schema drift.

### Task 3: Fallback Stress Testing

The fallback mechanism was subjected to 6 distinct adversarial edge-case conditions:

1. **Explicit `use_parquet=False` (Single Province)**:
   - Command: `common.load_merged("Hà Nội", use_parquet=False)`
   - Result: Successfully loaded from CSV, 25,512 rows, only 'Hà Nội'.
2. **Explicit `use_parquet=False` (All Provinces)**:
   - Command: `common.load_merged(None, use_parquet=False)`
   - Result: Successfully loaded from CSV, 280,632 rows, 11 provinces.
3. **Full Dataset via Parquet (`use_parquet=True`)**:
   - Command: `common.load_merged(None, use_parquet=True)`
   - Result: Successfully loaded all 280,632 rows across 11 provinces directly from Silver Parquet partitions.
4. **Non-Existent Silver Storage Path**:
   - Simulated: `common.SILVER_PROVINCE_DIR = tmp_path / "non_existent"`
   - Result: Transparently fell back to CSV, returned 25,512 rows for Hà Nội without raising unhandled errors.
5. **Empty Silver Storage Directory**:
   - Simulated: `common.SILVER_PROVINCE_DIR = tmp_path / "empty_dir"` (directory exists but has no parquet files)
   - Result: Transparently caught Parquet read failure and fell back to CSV, returned 25,512 rows.
6. **Empty Silver Weather Directory**:
   - Simulated: `common.SILVER_WEATHER_DIR = tmp_path / "empty_weather"`
   - Result: Transparently fell back to CSV, returned 25,512 rows.

### Task 4: Downstream Script Integration

1. **`code/training/du_bao.py`**:
   - Tested historical 72-hour slice loading via `load_merged(province_name="Hà Nội", use_parquet=True).sort_values("datetime").tail(72)`.
   - Verified that exactly 72 records are loaded spanning `2026-11-05 00:00:00` to `2026-11-07 23:00:00` with 0 NaNs.
2. **`code/training/train_global_ann.py`**:
   - Tested `load_all_data_with_station_id(use_parquet=True)`: returns 280,632 rows, preserving `station_id` and `station_code`.
   - Tested `load_features_dataset(use_parquet=True)`: directly loaded **280,104 samples** and **49 canonical features** from `lakehouse_storage/gold/global_ann_features` without recomputing from raw CSVs.

---

## 3. Test Suite Execution Summary

- `tests/test_step4_model_integration.py`: **5 / 5 PASSED**
- `tests/test_step4_adversarial.py`: **8 / 8 PASSED**
- **Overall Status**: **13 / 13 PASSED (100%)**

---

## 4. Final Recommendation

**Decision: APPROVE**

The Step 4 integration meets and exceeds all requirements for partition pruning, data integrity, and fallback resilience. The implementation is production-ready for the final benchmark phase (Milestone 5).
