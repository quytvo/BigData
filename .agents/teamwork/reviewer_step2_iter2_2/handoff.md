# Handoff Report — Reviewer 2 (Milestone 2 Iteration 2 Remediation Review)

**Author**: Reviewer 2 (Technical Reviewer & Adversarial Critic)  
**Date**: 2026-11-09T04:32:00Z  
**Target Milestone**: Milestone 2 (Step 2: Bronze to Silver Preprocessing Remediation)  
**Recipient**: Orchestrator (ID: `3888b5ae-cede-4e9e-8868-75bef4ef4193`)  
**Verdict**: **APPROVE**

---

## 1. Observation

1. **Physical Storage on Disk (`lakehouse_storage/silver/`)**:
   - `lakehouse_storage/silver/tedp_station/`:
     - Exactly **11 partition subdirectories**: `['province_name=Bắc%20Ninh', 'province_name=Gia%20Lai', 'province_name=HCM', 'province_name=Hà%20Nội', 'province_name=Hưng%20Yên', 'province_name=Hải%20Phòng', 'province_name=Lâm%20Đồng', 'province_name=Ninh%20Bình', 'province_name=Quảng%20Ngãi', 'province_name=Tây%20Ninh', 'province_name=Đà%20Nẵng']`.
     - Total rows: exactly **440,012** rows.
     - Per-province row counts: Bắc Ninh: 25,849; Gia Lai: 32,638; HCM: 101,475; Hà Nội: 50,482; Hưng Yên: 51,327; Hải Phòng: 25,626; Lâm Đồng: 25,590; Ninh Bình: 24,849; Quảng Ngãi: 25,617; Tây Ninh: 25,483; Đà Nẵng: 51,076 (sum = 440,012).
     - Parquet compression: 100% `SNAPPY`.
   - `lakehouse_storage/silver/tedp_province/`:
     - Exactly **11 partition subdirectories**.
     - Total rows: exactly **280,632** rows ($11 \times 25,512$ hours).
     - Each of the 11 provinces contains exactly **25,512** continuous hourly rows.
     - Parquet compression: 100% `SNAPPY`.
   - `lakehouse_storage/silver/weather_province/`:
     - Exactly **11 partition subdirectories**.
     - Total rows: exactly **280,632** rows ($11 \times 25,512$ hours).
     - Parquet compression: 100% `SNAPPY`.

2. **Remediation Implementation in Source Code**:
   - `code/lakehouse/spark_preprocess_tedp.py`: Line 277 defines `purge_output_directory(target_dir: Path)` with `shutil.rmtree`, `stat.S_IWRITE` permission reset, and 3 retries (0.5s pause). Called at line 325 directly prior to `df_final.write.mode("overwrite").partitionBy("province_name").parquet(...)`.
   - `code/lakehouse/spark_sync_province.py`: Line 382 defines identical `purge_output_directory(target_dir: Path)`. Called at line 441 in `sync_weather_province` and at line 467 in `process_province_silver`.
   - AST analysis: Zero occurrences of prohibited APIs (`toPandas`, `applyInPandas`, `inferSchema=True`).

3. **Documentation Alignment**:
   - `docs/review_checkpoint_step2.md`: Section 6 ("Ghi Nhận Khắc Phục Sự Cố Phân Vùng (Remediation Record)") is present at lines 197–217, detailing the root cause (Windows URL encoding vs Linux POSIX naming divergence under `df.write.mode("overwrite")`), remediation actions, and cross-platform verification results. Section 5 provides the verified per-province breakdown.

4. **Test Execution Results**:
   - **Windows Host Python 3.11**:
     ```
     pytest tests/test_step2_bronze_silver.py -v
     ============================== 7 passed in 2.43s ==============================
     pytest tests/test_step2_adversarial.py -v
     ============================== 8 passed in 4.63s ==============================
     ```
   - **Linux Docker Container (`airquality-spark:3.5.1`, Spark 3.5.1, Python 3.8)**:
     ```
     docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest /workspace/tests/test_step2_bronze_silver.py -v
     ============================== 7 passed in 5.13s ===============================
     docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest /workspace/tests/test_step2_adversarial.py -v
     ============================== 8 passed in 5.71s ===============================
     ```

5. **Post-Execution Idempotency**:
   - Following execution of the test suite both on Windows host and inside the Linux container, a re-check of `lakehouse_storage/silver/` confirmed partition counts remained exactly 11 for all 3 tables with zero duplicate folders or file corruption.

---

## 2. Logic Chain

1. **Finding Verification**:
   The initial Milestone 2 audit failure identified 15 partition directories in `tedp_station` (109,088 duplicate rows) caused by the juxtaposition of Windows URL encoding (`%20`) and Linux POSIX literal spaces when re-running across host and container without deleting target folders.
2. **Remediation Analysis**:
   The addition of `purge_output_directory(target_dir: Path)` into both `spark_preprocess_tedp.py` and `spark_sync_province.py` ensures that target directories are wiped clean prior to Parquet partitioning, thereby enforcing physical write idempotency across heterogeneous operating systems.
3. **Artifact Integrity Confirmation**:
   Direct physical inspection confirmed that all 3 Silver tables (`tedp_station`, `tedp_province`, and `weather_province`) now have exactly 11 partitions, exactly the required row counts (440,012 for station; 280,632 for province and weather), and 100% Snappy compression.
4. **Independent Numerical & Parity Verification**:
   Both `test_step2_bronze_silver.py` (7 tests) and `test_step2_adversarial.py` (8 tests) evaluate genuine mathematical differences against the Pandas reference dataset (`cleaned_tedp_province_4y.csv`), confirming that all differences are $< 8.53 \times 10^{-14} \ll 10^{-5}$, temporal continuity holds for 25,512 continuous hours across all 11 provinces, and zero null values exist in core metrics.
5. **Integrity Mandate Compliance**:
   No hardcoded constants, facades, or test bypasses were found. The code uses 100% pure Catalyst Window expressions and standard PySpark transformations.

---

## 3. Caveats

- **No Caveats**: The pipeline executes deterministically, handles cross-platform partitioning without directory collision, meets all numerical and schema constraints, and achieves 100% test passage on both host and Docker environments.

---

## 4. Conclusion

Milestone 2 (Step 2) remediation is **APPROVED**.
- All audit issues regarding partition collision and duplicate station records are resolved.
- Core Silver tables (`lakehouse_storage/silver/tedp_province/` and `lakehouse_storage/silver/weather_province/`) remain fully intact with exactly 11 partitions, 280,632 continuous hourly rows, and zero nulls.
- `docs/review_checkpoint_step2.md` Section 6 and Section 5 accurately reflect the remediated state.
- The project is fully unblocked and ready to advance to Milestone 3 (Step 3: Gold Layer & 49 Feature Extraction).

---

## 5. Verification Method

To independently reproduce and verify this assessment:

1. **Inspect Partition Counts and Row Counts on Disk**:
   ```powershell
   python -c "import pandas as pd, pathlib; [print(f'{tbl}: partitions={len([d for d in (pathlib.Path(\"lakehouse_storage/silver/\" + tbl)).iterdir() if d.is_dir() and d.name.startswith(\"province_name=\")])}, rows={len(pd.read_parquet(\"lakehouse_storage/silver/\" + tbl))}') for tbl in ['tedp_station', 'tedp_province', 'weather_province']]"
   ```
   *Expected output*:
   - `tedp_station`: partitions=11, rows=440012
   - `tedp_province`: partitions=11, rows=280632
   - `weather_province`: partitions=11, rows=280632

2. **Execute Host Test Suites**:
   ```powershell
   pytest tests/test_step2_bronze_silver.py -v
   pytest tests/test_step2_adversarial.py -v
   ```
   *Expected output*: 7/7 PASSED and 8/8 PASSED.

3. **Execute Docker Container Test Suites**:
   ```powershell
   docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest /workspace/tests/test_step2_bronze_silver.py -v
   docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest /workspace/tests/test_step2_adversarial.py -v
   ```
   *Expected output*: 7/7 PASSED and 8/8 PASSED.

4. **Verify AST for Zero Prohibited APIs**:
   ```powershell
   python -c "import ast, pathlib; prohibited = ['toPandas', 'applyInPandas']; [print(p.name, [n.attr for n in ast.walk(ast.parse(p.read_text(encoding='utf-8'))) if isinstance(n, ast.Attribute) and n.attr in prohibited]) for p in pathlib.Path('code/lakehouse').glob('*.py')]"
   ```
   *Expected output*: Empty lists `[]` for all files.
