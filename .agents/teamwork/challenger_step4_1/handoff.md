# Milestone 4 (Step 4) Challenger 1 Handoff Report

## 1. Observation

- **Pytest Execution**:
  Ran:
  ```powershell
  pytest tests/test_step4_model_integration.py tests/test_step4_adversarial.py -v
  ```
  Result:
  ```
  tests/test_step4_model_integration.py::TestStep4ModelIntegration::test_common_module_load_merged_signature PASSED
  tests/test_step4_model_integration.py::TestStep4ModelIntegration::test_common_load_merged_partition_pruning PASSED
  tests/test_step4_model_integration.py::TestStep4ModelIntegration::test_common_load_merged_fallback_when_parquet_missing PASSED
  tests/test_step4_model_integration.py::TestStep4ModelIntegration::test_du_bao_script_parquet_integration PASSED
  tests/test_step4_model_integration.py::TestStep4ModelIntegration::test_train_global_ann_parquet_consumption_support PASSED
  tests/test_step4_adversarial.py::TestAdversarialPartitionPruning::test_empirical_partition_pruning_physical_isolation PASSED
  tests/test_step4_adversarial.py::TestAdversarialPartitionPruning::test_ha_noi_exact_row_count_and_no_duplicates PASSED
  tests/test_step4_adversarial.py::TestAdversarialPartitionPruning::test_all_11_provinces_loop_integrity PASSED
  tests/test_step4_adversarial.py::TestAdversarialFallbackResilience::test_fallback_when_silver_dir_nonexistent PASSED
  tests/test_step4_adversarial.py::TestAdversarialFallbackResilience::test_fallback_when_silver_dir_empty PASSED
  tests/test_step4_adversarial.py::TestAdversarialFallbackResilience::test_fallback_when_silver_weather_dir_empty PASSED
  tests/test_step4_adversarial.py::TestAdversarialFallbackResilience::test_explicit_use_parquet_false_single_province PASSED
  tests/test_step4_adversarial.py::TestAdversarialFallbackResilience::test_use_parquet_true_all_provinces PASSED
  === 13 passed in 19.38s ===
  ```

- **Physical File Scan Tracing (I/O Spy)**:
  Instrumented `pyarrow.fs.PyFileSystem` using `TracedFileSystemHandler` wrapping `LocalFileSystem`:
  ```python
  table = pq.read_table("lakehouse_storage/silver/tedp_province", filesystem=fs, filters=[("province_name", "==", "Hà Nội")])
  ```
  Observed files opened:
  1. `lakehouse_storage/silver/tedp_province/province_name=Bắc%20Ninh/part-00004-e7fc3c3d-162a-45d4-ae6e-dbe8ccde4ee2.c000.snappy.parquet` (schema inspection by dataset factory).
  2. `lakehouse_storage/silver/tedp_province/province_name=Hà%20Nội/part-00003-e7fc3c3d-162a-45d4-ae6e-dbe8ccde4ee2.c000.snappy.parquet` (target data scan).
  Zero data files opened for the remaining 9 provinces (`Gia Lai`, `HCM`, `Hưng Yên`, `Hải Phòng`, `Lâm Đồng`, `Ninh Bình`, `Quảng Ngãi`, `Tây Ninh`, `Đà Nẵng`).

- **Per-Province Integrity**:
  Iterated through all 11 provinces via `common.load_merged(prov, use_parquet=True)`:
  - Row counts: Exactly 25,512 rows per province (Total 280,632).
  - Province column values: Strictly matching the queried province name.
  - Duplicates on `(station_code, datetime)`: 0 across all 11 provinces.
  - Nulls in weather columns or PM2.5: 0 across all 11 provinces.
  - Column schema: Exactly 38 identical columns across all 11 provinces.

- **Fallback Robustness**:
  - `load_merged("Hà Nội", use_parquet=False)`: Successfully loaded from CSV (25,512 rows, 'Hà Nội').
  - `load_merged(None, use_parquet=False)`: Successfully loaded from CSV (280,632 rows, 11 provinces).
  - Empty `SILVER_PROVINCE_DIR`: Transparently fell back to CSV without unhandled errors (25,512 rows).
  - Non-existent `SILVER_PROVINCE_DIR`: Transparently fell back to CSV (25,512 rows).
  - Empty `SILVER_WEATHER_DIR`: Transparently fell back to CSV (25,512 rows).

- **Downstream Script Verification**:
  - `code/training/du_bao.py`: Loads historical 72-hour slice via `load_merged(province_name="Hà Nội", use_parquet=True).sort_values("datetime").tail(72)`, returning shape `(72, 38)` spanning `2026-11-05 00:00:00` to `2026-11-07 23:00:00`.
  - `code/training/train_global_ann.py`: `load_all_data_with_station_id(use_parquet=True)` returns 280,632 rows (38 columns); `load_features_dataset(use_parquet=True)` directly loads 280,104 samples and 49 features from `lakehouse_storage/gold/global_ann_features` with shape `(280104, 61)` and 11 station records.

---

## 2. Logic Chain

1. **Step 1**: If partition pruning is functional, passing `filters=[("province_name", "==", "Hà Nội")]` to PyArrow Parquet reader must restrict file access such that other province partitions are not opened for reading row data.
2. **Step 2**: The empirical filesystem spy recorded that `lakehouse_storage/silver/tedp_province/province_name=Hà%20Nội/...` was the only target fragment opened for table record reading, while other 9 province folders were never accessed.
3. **Step 3**: Each partition on disk must be syntactically valid and contain uncorrupted Parquet pages. The 11-province loop empirically verified all 11 partitions, showing exactly 25,512 rows per partition with 0 missing values and identical 38-column schemas.
4. **Step 4**: A robust system must degrade gracefully to existing CSVs if lakehouse storage is damaged or deactivated. The 6-scenario fallback stress test verified that whether silver directories are missing, empty, or `use_parquet=False` is requested, `common.load_merged` returns identical row counts and columns from CSV without error.
5. **Step 5**: Therefore, Step 4 satisfies all operational, performance, and reliability requirements.

---

## 3. Caveats

- **PyArrow Schema Resolution**: When reading partitioned Parquet datasets via PyArrow without an explicit pre-supplied schema, PyArrow's `FileSystemDatasetFactory` opens the first file lexicographically (`Bắc Ninh`) to inspect Parquet metadata/footer for schema reconciliation. This is standard PyArrow C++ dataset behavior and does NOT stream record batches from `Bắc Ninh` into memory.

---

## 4. Conclusion

**Verdict**: **APPROVE**

Milestone 4 (Step 4) integration is fully verified, robust, and empirically sound. All 13 unit and adversarial tests pass without flaws. The code is ready for Milestone 5 (End-to-End Docker Lakehouse Benchmark).

---

## 5. Verification Method

To independently verify these findings:
```powershell
pytest tests/test_step4_model_integration.py tests/test_step4_adversarial.py -v
```

All 13 tests must pass with exit code 0.
