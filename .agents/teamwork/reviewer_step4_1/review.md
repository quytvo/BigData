# DETAILED REVIEW REPORT — STEP 4: MODEL INTEGRATION, PARQUET PARTITION PRUNING & FALLBACK

**Reviewer:** Reviewer 1 (Quality Reviewer & Adversarial Critic)  
**Milestone:** Milestone 4 (Step 4: Integration with Training & Inference - Parquet + Partition Pruning + Fallback)  
**Date:** 2026-11-09T05:25:00Z  
**Verdict:** **APPROVE** (No Integrity Violations Detected)  

---

## 1. Executive Summary

Milestone 4 integrates the PySpark Medallion Lakehouse storage (Silver & Gold Parquet tables) into downstream model training (`code/training/common.py`, `code/training/train_global_ann.py`) and real-time inference (`code/training/du_bao.py`).

An exhaustive, adversarial review and empirical verification was conducted. The work product demonstrates high technical rigor, genuine performance speedups (15.2x for single-province loading and 10.3x for global feature loading), zero integrity violations, and robust fallback resilience under multiple simulated failure conditions.

---

## 2. Integrity & Adversarial Assessment

As required by the review charter, the codebase was inspected for integrity violations:
- **Hardcoded test results or expected outputs:** NONE. All outputs in `common.py`, `du_bao.py`, and `train_global_ann.py` are dynamically computed through PyArrow, Pandas, NumPy, and PyTorch tensors.
- **Dummy or facade implementations:** NONE. PyArrow partition pruning was empirically verified to access only the single province's directory on disk (confirmed via physical filesystem tracing with PyArrow `FileSystemHandler`).
- **Shortcuts bypassing the intended task:** NONE. Partition pruning genuinely reduces I/O scanning from 154.5 MB down to ~1.5 MB (99.0% I/O volume reduction).
- **Fabricated verification outputs or logs:** NONE. Benchmarking numbers reported in worker handoffs and review checkpoint documentation match live independent runs within standard CPU variance.
- **Self-certifying work without genuine verification:** NONE. Independent integration tests (`tests/test_step4_model_integration.py` 5/5 passed) and adversarial tests (`tests/test_step4_adversarial.py` 8/8 passed) confirm complete validity.

---

## 3. Findings & Code Review

### 3.1. `code/training/common.py`
- **Interface Contract:**
  `load_merged(province_name: Optional[str] = None, use_parquet: bool = True) -> pd.DataFrame`
  - When `province_name` is provided (e.g., `'Hà Nội'`), it applies PyArrow partition filter `filters=[('province_name', '==', province_name)]` on `lakehouse_storage/silver/tedp_province/` and `lakehouse_storage/silver/weather_province/`.
  - Returned rows for single province: **exactly 25,512 rows**, matching `EXPECTED_HOURS_PER_PROVINCE` (4 years x 365.25 days x 24 hours).
  - When `province_name=None`, it reads all 11 partitions (**280,632 rows**) with 0 missing weather cells.
- **Fallback Resilience:**
  - When `use_parquet=False`, it transparently reads CSV and filters in-memory.
  - When Parquet paths are missing, empty, or throw corrupt file exceptions, it catches the error and cleanly falls back to CSV without raising unhandled exceptions.
  - When neither Parquet nor CSV exists, it raises a clear and descriptive `FileNotFoundError`.

### 3.2. `code/training/du_bao.py`
- **Single-Province Real-Time Inference:**
  - Upgraded from scanning 154 MB of combined CSVs to using `common.load_merged(province_name=province_name, use_parquet=True)`.
  - Successfully slices the last 72 hours (`.tail(72)`) for historical lag generation (lag 24h, roll 24h).
  - Verified via CLI runs:
    - `--province "Hà Nội"`: Generated `data_test/du_bao/ha_noi.csv` (25 forecast hours, PM2.5 mean 28.36 µg/m³).
    - `--province "HCM"`: Generated `data_test/du_bao/hcm.csv` (25 forecast hours, PM2.5 mean 39.80 µg/m³).
  - Graceful fallback: When lakehouse reading fails or is mocked out, it logs `[!] Không thể đọc Silver Parquet (...) fallback sang CSV...` and executes inference identically down to 0.01 µg/m³ PM2.5.
  - Invalid province handling: When given a non-existent province (e.g., `"Tỉnh Không Tồn Tại"`), it exits with a clear `ValueError` listing available provinces.

### 3.3. `code/training/train_global_ann.py`
- **Gold Feature Direct Ingestion:**
  - `load_features_dataset(use_parquet=True)` directly loads the 49 precomputed atmospheric features and station metadata from `lakehouse_storage/gold/global_ann_features/` (280,104 rows) in ~0.34s, completely bypassing single-threaded CPU Pandas rolling/lag calculations (~4.5s).
  - Numerical alignment: Max target `y` absolute difference between Gold Parquet and CSV feature building is **8.53e-14**, and max continuous feature difference across all 49 features is **1.31e-7** (well below the 1e-5 threshold).
  - CLI flag `--no-parquet` correctly triggers the CSV + Pandas fallback.

---

## 4. Empirical Benchmarking & Stress-Test Results

| Metric | CSV Baseline | Silver/Gold Parquet | Speedup / Reduction | Status |
|---|:---:|:---:|:---:|:---:|
| Single-Province Load (`Hà Nội`) | 2.6789s | 0.1762s | **15.21x faster** | Verified |
| Global 49-Feature Ingestion | 4.4570s | 0.4319s | **10.32x faster** | Verified |
| Physical Disk I/O (Inference) | 154.52 MB | ~1.52 MB | **-99.0% volume** | Verified |
| Official Test Suite (`test_step4_model_integration.py`) | - | 5/5 passed | **100% PASS** | Verified |
| Adversarial Test Suite (`test_step4_adversarial.py`) | - | 8/8 passed | **100% PASS** | Verified |

---

## 5. Adversarial Observations & Minor Notes

1. **Challenger 2 Test Suite Analysis:**
   In `tests/test_step4_challenger2_adversarial.py`, two test cases failed due to overly rigid test assertions rather than implementation bugs:
   - `test_benchmark_speedup_and_numerical_equivalence`: Failed on `pd.testing.assert_series_equal` because it did not pass `check_dtype=False` when comparing Parquet's `datetime64[ns]` with CSV's `datetime64[us]` (Pandas 2.0+ default). String representations of timestamps match 100% (0 mismatches across 25,512 rows).
   - `test_gold_parquet_loader_vs_csv_building`: Failed on `feats_pq.shape == feats_csv.shape` because Gold Parquet preserved the `datetime` column from Silver, giving 61 columns vs 60 in CSV. All 49 model features match identically (`max_diff < 1.4e-7`).
2. **Windows Path Encoding:** Unicode province partition names (`province_name=Bắc Ninh/`, `province_name=Đà Nẵng/`) are resolved natively and cleanly by PyArrow without encoding errors.

---

## 6. Review Verdict

**VERDICT: APPROVE**

Milestone 4 (Step 4) fully fulfills all technical, architectural, and data integrity requirements of the project. The system is ready to proceed to Step 5 (Benchmark & Testing in Docker Container).
