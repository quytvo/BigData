# Task Assignment: Worker Step 2 Remediation (Resolve Partition Directory Collision & Purge)

Read ORIGINAL_REQUEST.md at: `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\ORIGINAL_REQUEST.md`
Read PROJECT.md at: `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\orchestrator\PROJECT.md`
Read Explorer Remediation Strategy at: `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\explorer_step2_remediation\remediation_strategy.md`
Read Forensic Audit Evidence at: `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\auditor_step2_1\audit.md`

## Objectives
Implement the remediation fix per Explorer strategy:
1. `code/lakehouse/spark_preprocess_tedp.py`:
   - Add `purge_output_directory(target_dir: Path)` helper function with retry loop and read-only permission handler.
   - Call `purge_output_directory(output_dir)` immediately before writing partitioned Parquet.
2. `code/lakehouse/spark_sync_province.py`:
   - Add `purge_output_directory(target_dir: Path)`.
   - Call `purge_output_directory(province_output_dir)` and `purge_output_directory(weather_output_dir)` before writes.
3. Storage Regeneration:
   - Purge `lakehouse_storage/silver/tedp_station/` completely.
   - Re-run `python code/lakehouse/spark_preprocess_tedp.py` (or inside Docker container).
   - Verify that `lakehouse_storage/silver/tedp_station/` contains exactly 11 partition directories matching `province_name=*` and exactly 440,012 rows (zero duplicate records).
   - Re-verify `spark_sync_province.py` outputs.
4. Verification:
   - Run `pytest tests/test_step2_bronze_silver.py -v` on host (confirm 7/7 passed).
   - Run `docker run --rm -v "${PWD}:/workspace" airquality-spark:3.5.1 pytest /workspace/tests/test_step2_bronze_silver.py -v` (confirm 7/7 passed).
   - Update `docs/review_checkpoint_step2.md` to document the partition integrity remediation and test results.

## Write Ownership
- Files you own:
  - `code/lakehouse/spark_preprocess_tedp.py`
  - `code/lakehouse/spark_sync_province.py`
  - `docs/review_checkpoint_step2.md`
  - `lakehouse_storage/silver/` directories
- DO NOT touch files in `tests/`.

## MANDATORY INTEGRITY WARNING
DO NOT CHEAT. All implementations must be genuine. DO NOT hardcode test results, create dummy/facade implementations, or circumvent the intended task. A teamwork_auditor will independently verify your work. Integrity violations WILL be detected and your work WILL be rejected.

## Deliverables
- Patched `spark_preprocess_tedp.py` and `spark_sync_province.py`
- Clean `lakehouse_storage/silver/` Parquet datasets with exactly 11 partition folders
- Updated `docs/review_checkpoint_step2.md`
- Write handoff report to `.agents/teamwork/worker_step2_remediation/handoff.md` with verification commands and outputs.
- Send completion message to orchestrator.

