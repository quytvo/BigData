# REVIEW REPORT — MILESTONE 3 (STEP 3: SILVER TO GOLD 49 FEATURES PIPELINE & MASTER LAKEHOUSE RUNNER)

## Review Summary

**Verdict**: **APPROVE**  
**Integrity Assessment**: PASSED (100% Genuine Implementation, Zero Shortcuts, Zero Integrity Violations)  
**Numerical Accuracy**: PASSED (Max Absolute Error: $1.31437461 \times 10^{-7} < 10^{-5}$)  
**Row Count Validation**: PASSED (280,104 full / 279,840 filtered; exactly 0 NaNs)  
**Catalyst Engine Compliance**: PASSED (Zero `@udf`, Zero `applyInPandas`, Zero `toPandas`, Zero `inferSchema=True`)  
**Test Suite Verification**: PASSED (7/7 tests passed on both Windows Host and Docker Container)

---

## Findings

### Minor Finding 1: Transient WSL2/Host Bind-Mount File Invalidation Latency
- **What**: When Docker finishes writing or purging Parquet files to a host-mounted directory (`lakehouse_storage/gold/global_ann_features/`), host-side Python processes immediately checking `exists()` within milliseconds may experience a transient Windows OS filesystem cache flush latency (1–2 seconds).
- **Where**: `code/lakehouse/spark_feature_engineering.py` (`purge_output_directory()`).
- **Why**: Windows Desktop WSL2 gRPC file sharing caches directory metadata.
- **Suggestion**: The 3-attempt retry loop with `time.sleep(0.5)` in `purge_output_directory()` already mitigates this during Spark runs. In testing, ensuring a brief flush or retry in host assertions ensures rock-solid stability across OS platforms.

---

## Verified Claims

| Claim | Upstream Claim | Reviewer Verification Method | Result |
|---|---|---|---|
| **Catalyst Window Functions** | 100% pure Spark SQL Catalyst, 0 `@udf`, 0 `applyInPandas`, 0 `inferSchema` | Static AST and string regex inspection of `code/lakehouse/spark_feature_engineering.py` | **PASS** (Zero UDFs in execution; AST shows pure `pyspark.sql.functions` & `Window`) |
| **All 49 Features + y + target_time** | Matches `train_global_ann.py` specifications exactly | Schema inspection via PyArrow against `output/global_ann_4y/best_ann_model.pt` checkpoint | **PASS** (Exact 49 features + label `y` + `target_time`, matching PyTorch ANN architecture) |
| **Full Row Count** | Exactly 280,104 rows (11 provinces $\times$ 25,464 valid hours) | `pyarrow.read_table('lakehouse_storage/gold/global_ann_features').num_rows` | **PASS** (280,104 rows confirmed) |
| **Filtered Row Count** | Exactly 279,840 rows with cutoff $\le$ `2026-11-06 23:00:00` | Automated Data Diff & Pandas slice on Parquet table | **PASS** (279,840 rows confirmed) |
| **Zero NaN / Nulls** | 0% NaNs across all 49 features + label `y` | `table[col].null_count` for all 61 columns | **PASS** (0 nulls across 17,086,344 total values) |
| **Numerical Tolerance** | Max absolute difference $< 10^{-5}$ vs Pandas reference | Execution of `verify_gold_data_diff()` on full and filtered datasets | **PASS** ($\Delta_{\max} = 1.31437461 \times 10^{-7} \ll 10^{-5}$) |
| **Partitioning & Codec** | Snappy Parquet partitioned by `province_name` (11 dirs) | Directory inspection & Parquet file metadata check | **PASS** (11 dirs `province_name=*`, Snappy compression, clean purge) |
| **Master Orchestrator** | Coordinates Steps 1–3 with CLI flags (`--steps`, `--verify-diff`, etc.) | Direct execution of `build_lakehouse_spark.py --help` and container run | **PASS** (All CLI flags verified, end-to-end execution verified) |
| **Automated Test Suite** | 7/7 test cases pass | `pytest tests/test_step3_silver_gold.py -v` on host (2.41s) and Docker container (6.42s) | **PASS** (7/7 passed on both platforms) |

---

## Adversarial Stress-Test Results

### 1. Window Boundary Trim Correctness
- **Stress Scenario**: Lookback features require past 24 hours ($t < 24$ incomplete); target $y$ and future weather require future 24 hours ($t \ge N - 24$ incomplete). Does `pm25_lag24.isNotNull() & y.isNotNull()` drop exactly 48 boundary rows per province?
- **Analysis**:
  - $t = 0 \dots 23$: `pm25_lag24` is NULL $\implies$ exactly 24 rows dropped at head.
  - $t = N-24 \dots N-1$: `y` is NULL $\implies$ exactly 24 rows dropped at tail.
  - $(25,512 - 48) \times 11 = 25,464 \times 11 = 280,104$ rows.
  - Intermediate rolling windows: `pm25_roll24` requires 24 past observations ($t \ge 24$). Because $t \ge 24$, all 24 observations exist $\implies$ zero NaNs.
  - Forward precipitation: `precip_sum_24h` requires 24 future observations ($t \le N-25$). Because $t \le N-25$, all 24 observations exist $\implies$ zero NaNs.
- **Result**: **PASS** (Mathematically tight, zero residual nulls).

### 2. Temporal Data Leakage Audit
- **Stress Scenario**: In time series regression, target or future data cannot leak into predictive features at origin time $t$.
- **Audit**:
  - PM2.5 history features: all computed from $t \le 0$ relative to origin time.
  - Current co-pollutants (`pm10_lag0`, `aqi_lag0`): measured at origin time $t$.
  - Target label `y`: future PM2.5 at $t+24$ (lead 24).
  - Target weather: NWP forecasted weather at $t+24$ (lead 24), which is explicitly assumed known from weather forecast.
  - Calendar features: calendar time at target time $t+24$.
  - Spatial coordinates: fixed station coordinates.
- **Result**: **PASS** (No target leakage into features; exact adherence to time-series forecasting formulation).

### 3. Model Compatibility & Feature Ordering
- **Stress Scenario**: Does the output Parquet schema and feature ordering directly align with the pre-trained ANN PyTorch checkpoint (`output/global_ann_4y/best_ann_model.pt`)?
- **Test**: Loaded `best_ann_model.pt` via PyTorch, extracted `ckpt['all_feature_cols']` (49 features), sliced 100 rows from `lakehouse_storage/gold/global_ann_features/`, transformed continuous columns using `scaler_mean` and `scaler_scale`.
- **Result**: **PASS** (100% column name alignment, 0 NaNs, dimension $(100, 49)$ matches neural network input layer).

---

## Coverage Gaps
- None. All functional and non-functional requirements for Step 3 are thoroughly covered.

---

## Recommendation & Next Steps
- Milestone 3 (Step 3) is **APPROVED** without reservations.
- Proceed to Step 4: Model Integration & Parquet Partition Pruning (`spark_train_global_ann.py` & `spark_inference_global_ann.py`).
