# Forensic Audit Report — Milestone 4 (Step 4: Model Integration, Partition Pruning & Fallback)

**Auditor**: Forensic Integrity Auditor (`auditor_step4_1`)  
**Audit Target**: `code/training/common.py`, `code/training/du_bao.py`, `code/training/train_global_ann.py`, `tests/test_step4_model_integration.py`  
**Profile**: General Project  
**Integrity Mode**: Development (per `ORIGINAL_REQUEST.md`)  
**Timestamp**: 2026-11-09T05:23:00Z  
**Verdict**: **CLEAN**

---

## 1. Executive Summary

A forensic audit was conducted on Milestone 4 (Step 4: Integration with Training & Inference - Parquet + Partition Pruning + Fallback). The objective was to verify:
1. **Authenticity of Parquet Partition Pruning**: Ensure PyArrow dataset filtering prunes Hive partition directories at the storage/filesystem level before memory materialization, rather than reading the entire 280,632-row dataset and doing post-load filtering.
2. **Authenticity of Fallback Mechanism**: Ensure code gracefully reverts to CSV files when Parquet storage is absent or corrupted, without relying on mock return values or fake stubs.
3. **Cheating & Facade Detection**: Verify that `du_bao.py` genuinely executes neural network inference through the trained PyTorch ANN model (`best_ann_model.pt`) and writes dynamic, mathematically sound forecast outputs.
4. **Test Suite Verification**: Execute `tests/test_step4_model_integration.py` and empirical adversarial checks.

**Final Determination**: The work product is **100% CLEAN**. No integrity violations, facades, hardcoded test strings, or fraudulent shortcuts were detected. Parquet partition pruning delivers an empirically verified **29.2x to 42.5x speedup** with zero numerical drift ($\Delta \approx 10^{-14}$).

---

## 2. Phase 1 — Mode-Agnostic Forensic Investigation

### Check 1: Authenticity of Parquet Partition Pruning
- **Files Inspected**: `code/training/common.py` (lines 170-226), `code/training/du_bao.py` (lines 295-304).
- **Static Code Analysis**:
  In `code/training/common.py`:
  ```python
  if province_name is not None:
      filters = [("province_name", "==", province_name)]
      tedp = pd.read_parquet(SILVER_PROVINCE_DIR, filters=filters)
      weather = pd.read_parquet(
          SILVER_WEATHER_DIR,
          columns=["station_code", "datetime"] + WEATHER_COLS,
          filters=filters,
      )
  ```
- **Empirical Reader-Level Verification**:
  We queried PyArrow's underlying `FileSystemDataset` fragments using `pyarrow.dataset`:
  - Total fragments in full Silver TEDP dataset: **11 fragments** (11 provinces).
  - Filtered fragments when `filter=ds.field('province_name') == 'Hà Nội'`: **1 fragment** (`province_name=Hà%20Nội/part-00003-*.snappy.parquet`).
  - Total fragments in full Silver Weather dataset: **14 fragments**.
  - Filtered fragments when filter applied: **1 fragment**.
  - Direct row count returned by `pd.read_parquet(SILVER_PROVINCE_DIR, filters=filters)`: **25,512 rows** (exactly 1 province, not 280,632 rows).
  - **Result**: Pruning is executed natively at the filesystem and reader level by PyArrow before records enter memory.
  - **Status**: **PASS (CLEAN)**

### Check 2: Authenticity of Fallback Mechanism
- **Files Inspected**: `code/training/common.py` (lines 176-246), `code/training/du_bao.py` (lines 305-323), `code/training/train_global_ann.py` (lines 147-169, 180-220).
- **Static Code Analysis**:
  - `common.load_merged` checks path existence and uses `try...except` to catch Parquet I/O or schema errors, automatically loading from `cleaned_tedp_province_4y.csv` and `weather_openmeteo_province_4y.csv`.
  - `du_bao.py` wraps Parquet loading in `try...except` and seamlessly reverts to CSV historical slicing when Parquet is unavailable.
  - `train_global_ann.load_features_dataset` checks `GOLD_FEATURES_DIR.exists()` and falls back to building 49 features from CSV via Pandas.
- **Empirical Verification**:
  - `common.load_merged("Hà Nội", use_parquet=False)` returned exactly 25,512 rows from CSV with identical schema.
  - Monkeypatching `SILVER_PROVINCE_DIR` to a non-existent directory triggered the fallback path without errors.
  - Monkeypatching `du_bao.SILVER_PROVINCE_DIR` caused `du_bao.py` to log:
    `[*] Fallback nạp lịch sử quan trắc từ file CSV cho tỉnh 'Hà Nội'..."`
    and successfully generated the forecast output `data_test/du_bao/ha_noi.csv`.
  - **Status**: **PASS (CLEAN)**

### Check 3: Cheating & Facade Detection
- **Files Inspected**: `code/training/du_bao.py`, `code/training/train_global_ann.py`.
- **Static Code Analysis**:
  - Checked for dummy returns, constant values, or pre-computed lookup tables.
  - Function `predict_single_province_ann` executes full tensor forward pass:
    - `model.station_embedding(st_idx_tensor)`
    - `model.input_proj(h_in)`
    - `model.v_proj(h_local)`
    - `model.blocks(h)`
    - `model.head(h)`
    - `anchor_weight * lag0 + anchor_bias + delta_pred`
- **Empirical Verification**:
  - Executed CLI across multiple distinct provinces:
    - **Hà Nội**: Average PM2.5 = 28.36 µg/m³ (Range: 14.84 - 43.81 µg/m³)
    - **HCM**: Average PM2.5 = 39.80 µg/m³ (Range: 29.49 - 54.66 µg/m³)
  - Executed CLI with non-existent province (`--province "Tỉnh Không Tồn Tại"`): Raised descriptive `ValueError` listing valid provinces.
  - Predictions are dynamic, continuous, province-specific, and reflect real meteorological inputs.
  - **Status**: **PASS (CLEAN)**

### Check 4: Pre-Populated Verification Output Detection
- Workspace search revealed no pre-populated log files or fabricated verification stamps.
- Test suites run dynamically against runtime inputs.
- **Status**: **PASS (CLEAN)**

### Check 5: Test Suite Execution
- Command: `pytest tests/test_step4_model_integration.py -v`
- Results:
  - `test_common_module_load_merged_signature`: **PASSED**
  - `test_common_load_merged_partition_pruning`: **PASSED**
  - `test_common_load_merged_fallback_when_parquet_missing`: **PASSED**
  - `test_du_bao_script_parquet_integration`: **PASSED**
  - `test_train_global_ann_parquet_consumption_support`: **PASSED**
  - **5/5 tests passed** in 7.63 seconds.
- **Status**: **PASS (CLEAN)**

---

## 3. Empirical Benchmark & Adversarial Stress-Testing

We executed empirical benchmarks comparing Parquet Partition Pruning against CSV reading across 5 geographically diverse provinces:

| Province | Parquet (Partition Pruning) | CSV Reading | Speedup | Max Absolute Difference ($\Delta$ PM2.5) |
|---|---|---|---|---|
| **Hà Nội** | 75.7 ms | 3,216.6 ms | **42.5x** | $5.68 \times 10^{-14}$ |
| **Hồ Chí Minh** | 88.7 ms | 3,151.2 ms | **35.5x** | $4.26 \times 10^{-14}$ |
| **Đà Nẵng** | 85.9 ms | 3,063.1 ms | **35.7x** | $2.84 \times 10^{-14}$ |
| **Gia Lai** | 78.1 ms | 3,165.2 ms | **40.5x** | $7.11 \times 10^{-15}$ |
| **Ninh Bình** | 116.1 ms | 3,390.7 ms | **29.2x** | $5.68 \times 10^{-14}$ |

### Gold Parquet Feature Loader vs CSV Feature Engineering
- **Gold Parquet Loading**: 0.254 seconds (280,104 samples x 49 features).
- **CSV Pandas Feature Building**: 4.027 seconds.
- **Speedup**: **15.8x faster**.
- **Numerical Difference**: Target $y$ max diff = $0.00$, continuous features max diff $< 10^{-5}$, exactly 0 NaNs.

---

## 4. Phase 2 — Mode-Specific Flagging

Per `ORIGINAL_REQUEST.md`, the integrity mode is **Development Mode**.

| Audit Check | Dev Mode Standard | Observed Behavior | Flag |
|---|---|---|---|
| Hardcoded test results | Prohibited | No hardcoded outputs; genuine calculation | CLEAN |
| Facade implementation | Prohibited | Full PyArrow dataset filtering & PyTorch inference | CLEAN |
| Fabricated verification outputs | Prohibited | All tests executed live and dynamically | CLEAN |
| Parquet Partition Pruning | Required | Prunes Hive directories at reader level | CLEAN |
| Fallback mechanism | Required | Automatically reverts to CSV on Parquet absence | CLEAN |

---

## 5. Forensic Verdict

**FINAL VERDICT: CLEAN**

The implementation in `code/training/common.py`, `code/training/du_bao.py`, and `code/training/train_global_ann.py` satisfies all integrity constraints and technical requirements of Step 4. All forensic integrity checks have passed unconditionally.
