# Handoff Report: Milestone 2 (Step 2 - Bronze to Silver Preprocessing)

**Agent**: Challenger 1 (Empirical Challenger)  
**Date**: 2026-11-09T04:00:00Z  
**Verdict**: **REQUEST_CHANGES**

---

## 1. Observation

1. **Test Suite Execution Failure**:
   Command: `pytest tests/test_step2_bronze_silver.py -v`
   Result: 1 failed, 6 passed in 6.61s.
   Verbatim error:
   ```text
   FAILED tests/test_step2_bronze_silver.py::TestStep2BronzeToSilver::test_silver_station_dataset_integrity - AssertionError: Expected 11 partition subdirectories matching 'province_name=*', found 15: ['province_name=Bắc%20Ninh', 'province_name=Gia Lai', 'province_name=Gia%20Lai', 'province_name=HCM', 'province_name=Hà%20Nội', 'province_name=Hưng%20Yên', 'province_name=Hải%20Phòng', 'province_name=Lâm Đồng', 'province_name=Lâm%20Đồng', 'province_name=Ninh%20Bình', 'province_name=Quảng%20Ngãi', 'province_name=Tây Ninh', 'province_name=Tây%20Ninh', 'province_name=Đà Nẵng', 'province_name=Đà%20Nẵng']
   assert 15 == 11
   tests\conftest.py:252: AssertionError
   ```

2. **Partition Directory Discrepancy in `lakehouse_storage/silver/tedp_station/`**:
   Inspection via Python `Path('lakehouse_storage/silver/tedp_station').iterdir()` reveals 15 directories:
   - 11 URL-encoded directories: `province_name=Bắc%20Ninh`, `Gia%20Lai`, `HCM`, `Hà%20Nội`, `Hưng%20Yên`, `Hải%20Phòng`, `Lâm%20Đồng`, `Ninh%20Bình`, `Quảng%20Ngãi`, `Tây%20Ninh`, `Đà%20Nẵng` (mtime ~10:53 - 10:54 AM).
   - 4 unencoded directories with literal spaces: `province_name=Gia Lai`, `province_name=Lâm Đồng`, `province_name=Tây Ninh`, `province_name=Đà Nẵng` (mtime ~10:55 AM).

3. **Data Consumer Duplication**:
   When reading `lakehouse_storage/silver/tedp_station/` via `pd.read_parquet('lakehouse_storage/silver/tedp_station')`:
   - Total rows: **549,100 rows** (expected ~350,000 rows).
   - `Gia Lai` row count: 65,276 rows (exact duplicate: 32,638 rows $\times$ 2).
   - `Lâm Đồng` row count: 51,180 rows (duplicated).
   - `Tây Ninh` row count: 50,966 rows (duplicated).
   - `Đà Nẵng` row count: 76,453 rows (duplicated).

4. **Event Logs Context**:
   `output/spark_event_logs/` shows:
   - `local-1794196413066` / `local-1794196443306`: `PreprocessTEDPStation` ran on Windows host.
   - `local-1794194586315`: `PreprocessTEDPStation` ran inside Linux Docker container (`Host: add76d7cfd87`), generating literal space partition names without percent-encoding.
   - `spark_preprocess_tedp.py` lines 291-295:
     ```python
     (
         df_final.write
         .mode("overwrite")
         .partitionBy("province_name")
         .parquet(str(output_dir))
     )
     ```
     Spark's `.mode("overwrite")` only deletes matching JVM partition paths and does not purge directories with differing URI encodings.

5. **Exhaustive Numerical Parity in `lakehouse_storage/silver/tedp_province/`**:
   Evaluated against `data_test/cleaned_tedp_province_4y.csv` across all 280,632 rows:
   - `pm25`: max absolute difference = $8.5265 \times 10^{-14}$ (well below $10^{-5}$ threshold).
   - `pm10`: max absolute difference = $2.8422 \times 10^{-14}$ (well below $10^{-5}$ threshold).
   - `aqi`: max absolute difference = $8.5265 \times 10^{-14}$ (well below $10^{-5}$ threshold).
   - `pm25_gap_hours`: max absolute difference = $0.0000$ (exact 0.0 match across all 280,632 rows).
   - `latitude`: max absolute difference = $1.7764 \times 10^{-15}$.
   - `longitude`: max absolute difference = $0.0000$.
   - All string/discrete columns (`year`, `month`, `day`, `hour`, `day_of_week`, `day_name`, `is_weekend`, `date`, `time_slot`, `station_code`, `station_name`, `station_short_name`, `aqi_category`, `is_inserted`, `n_stations_member`, `n_stations_valid_pm25`, `pm25_imputed`, `pm10_imputed`, `aqi_imputed`): 0 mismatches.
   - Per-province continuous hours: exactly 25,512 rows for each of the 11 provinces.

6. **Exhaustive Parity in `lakehouse_storage/silver/weather_province/`**:
   Evaluated against `data_test/weather_openmeteo_province_4y.csv` across 280,632 rows:
   - All 8 meteorological measurements match with error $< 10^{-12}$ (max error $4.55 \times 10^{-13}$ on `boundary_layer_height`, $0.0$ on all other 7 features).

7. **Independent Adversarial Test Suite Execution**:
   Command: `pytest tests/test_step2_adversarial.py -v`
   Result: 8 passed in 4.25s (100% pass rate).

---

## 2. Logic Chain

1. From **Observation 1**, `test_silver_station_dataset_integrity` in `tests/test_step2_bronze_silver.py` asserts that `lakehouse_storage/silver/tedp_station/` has exactly 11 partition subdirectories matching `province_name=*`. It found 15 directories.
2. From **Observation 2**, the 15 directories consist of 11 directories with URL-encoded `%20` (e.g. `province_name=Gia%20Lai`) and 4 directories with literal spaces (e.g. `province_name=Gia Lai`).
3. From **Observation 4**, the URL-encoded directories were created by the Windows PySpark runtime, whereas the literal-space directories were created by the Linux Docker PySpark runtime. Because Spark's `overwrite` mode does not wipe out non-matching directory paths, both encodings remain simultaneously on disk.
4. From **Observation 3**, any downstream consumer (Pandas or PyArrow) that loads `lakehouse_storage/silver/tedp_station/` decodes `%20` into spaces, causing it to read both copies of the partition data. This inflates the dataset to 549,100 rows with 100% duplicate records for the 4 affected provinces.
5. From **Observation 5 & 6**, the province-level datasets (`tedp_province` and `weather_province`) are completely unaffected because they were written with a unified naming convention and `spark_sync_province.py` contains `dropDuplicates(["station_code", "datetime"])`. The numerical transformations in Catalyst Window are mathematically exact ($< 10^{-13}$).
6. Therefore, while the analytical logic and province silver dataset are flawless, the deliverable `lakehouse_storage/silver/tedp_station/` is corrupted by orphan partition directories, causing a blocking test failure in the official test suite.

---

## 3. Caveats

- **No caveats regarding numerical accuracy**: Numerical parity across PM2.5, PM10, AQI, and gap hours was validated exhaustively across all 280,632 records with zero tolerance violations.
- **Scope boundary**: This review did not alter or delete files in `lakehouse_storage/silver/tedp_station/` or modify `spark_preprocess_tedp.py`, adhering strictly to the Review-Only constraint.

---

## 4. Conclusion

- **Verdict**: **REQUEST_CHANGES**
- **Root Cause**: Interleaved execution between Windows host and Linux Docker container left orphan partition subdirectories with differing space URI encodings (`%20` vs literal ` `) in `lakehouse_storage/silver/tedp_station/`.
- **Impact**: Fails official test `test_silver_station_dataset_integrity` and causes 100% duplicate record inflation (549,100 rows) when reading `tedp_station`.
- **Required Action for Implementation Worker**:
  1. Delete `lakehouse_storage/silver/tedp_station/` completely:
     `Remove-Item -Recurse -Force lakehouse_storage/silver/tedp_station`
  2. Add directory pre-cleaning to `code/lakehouse/spark_preprocess_tedp.py` before `df_final.write.parquet(...)` or ensure clean overwrite.
  3. Re-run `python code/lakehouse/spark_preprocess_tedp.py`.
  4. Run `pytest tests/test_step2_bronze_silver.py -v` to verify 7/7 PASSED.

---

## 5. Verification Method

To independently reproduce this finding:
```powershell
# 1. Run official test suite
pytest tests/test_step2_bronze_silver.py -v

# 2. Inspect partition directories in tedp_station
python -c "from pathlib import Path; p = Path('lakehouse_storage/silver/tedp_station'); print(sorted([d.name for d in p.iterdir() if d.is_dir()]))"

# 3. Inspect inflated row count
python -c "import pandas as pd; df = pd.read_parquet('lakehouse_storage/silver/tedp_station'); print('Row count:', len(df))"

# 4. Run independent adversarial test suite
pytest tests/test_step2_adversarial.py -v
```

Invalidation Condition:
If `lakehouse_storage/silver/tedp_station/` is cleanly regenerated with exactly 11 partition subdirectories, `pytest tests/test_step2_bronze_silver.py -v` passes 7/7, and `pd.read_parquet('lakehouse_storage/silver/tedp_station')` returns un-duplicated rows, this challenge is resolved and can be converted to **APPROVE**.
