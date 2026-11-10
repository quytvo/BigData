## 2026-11-09T05:02:27Z
You are the Implementation Worker for Milestone 4 (Step 4: Integration with Training & Inference - Parquet + Partition Pruning + Fallback).

## MANDATORY INTEGRITY WARNING
DO NOT CHEAT. All implementations must be genuine. DO NOT hardcode test results, create dummy/facade implementations, or circumvent the intended task. A teamwork_auditor will independently verify your work. Integrity violations WILL be detected and your work WILL be rejected.

## Context & Resources
- Working directory: `d:\năm 4\big data\Final_Project_Plan`
- State folder: `.agents/teamwork/worker_step4/`
- User Request: `.agents/teamwork/ORIGINAL_REQUEST.md`
- Project Architecture & Scope: `.agents/teamwork/orchestrator/PROJECT.md`
- Test Infrastructure: `.agents/teamwork/orchestrator/TEST_INFRA.md`
- Survey report on Training/Inference: `.agents/teamwork/explorer_survey_2/survey_gold_training.md`
- Independent test suite: `tests/test_step4_model_integration.py` and `tests/conftest.py`
- Target Code Files:
  - `code/training/common.py`
  - `code/training/du_bao.py`
  - `code/training/train_global_ann.py`
- Available Lakehouse Data:
  - `lakehouse_storage/silver/tedp_province/` (Parquet, Snappy, partitioned by `province_name`, 11 partitions)
  - `lakehouse_storage/silver/weather_province/` (Parquet, Snappy, partitioned by `province_name`, 11 partitions)
  - `lakehouse_storage/gold/global_ann_features/` (Parquet, Snappy, partitioned by `province_name`, 11 partitions, 280,104 rows)
- Available CSV Baseline Data:
  - `data_test/cleaned_tedp_province_4y.csv`
  - `data_test/weather_openmeteo_province_4y.csv`

## Technical Requirements for Step 4
1. **Update `code/training/common.py`**:
   - Define module-level path constants:
     - `SILVER_DIR = BASE_DIR / "lakehouse_storage" / "silver"`
     - `SILVER_PROVINCE_DIR = SILVER_DIR / "tedp_province"`
     - `SILVER_WEATHER_DIR = SILVER_DIR / "weather_province"`
     - `GOLD_DIR = BASE_DIR / "lakehouse_storage" / "gold"`
     - `GOLD_FEATURES_DIR = GOLD_DIR / "global_ann_features"`
   - Update `load_merged(province_name: Optional[str] = None, use_parquet: bool = True) -> pd.DataFrame`:
     - If `use_parquet=True` and `SILVER_PROVINCE_DIR.exists()` and `SILVER_WEATHER_DIR.exists()`:
       - If `province_name` is provided (e.g. `"Hà Nội"`):
         - Leverage Parquet Partition Pruning: read only the partition folder for `province_name` using `filters=[('province_name', '==', province_name)]` in `pd.read_parquet()` or PyArrow.
         - Ensure exact 25,512 rows for that single province are returned.
         - Ensure required columns (`datetime`, `pm25`, `temperature_2m`, `wind_speed_10m`, etc.) are present.
       - If `province_name=None`:
         - Read all 11 partitions from `SILVER_PROVINCE_DIR` and `SILVER_WEATHER_DIR`, merge on `['province_name', 'datetime']` or `['station_code', 'datetime']`.
     - Automatic Fallback:
       - If `use_parquet=False` OR reading Parquet fails / paths do not exist:
         - Gracefully fall back to loading from `TEDP_CSV` and `WEATHER_CSV`.
         - If `province_name` is provided, filter `df = df[df["province_name"] == province_name].copy()`.
       - NEVER raise an exception if Parquet is missing when CSV is available.
2. **Update `code/training/du_bao.py`**:
   - Update historical observations loading (around line 290):
     - Replace reading the entire 154MB CSV with `common.load_merged(province_name=province_name, use_parquet=True)` or direct Parquet read with partition filter.
     - Extract the last 72 hours (`.tail(72)`) for rapid inference.
     - Maintain fallback to CSV if Parquet is not present.
   - Verify that CLI inference `python code/training/du_bao.py --province "Hà Nội"` works seamlessly, outputs forecast CSV, and logs the use of Silver Parquet Partition Pruning.
3. **Update `code/training/train_global_ann.py`**:
   - Update data ingestion to support loading precomputed features directly from `lakehouse_storage/gold/global_ann_features/` (when `use_parquet=True` and directory exists).
   - Maintain automatic fallback to CSV + Pandas feature generation.
4. **Verification & Tests**:
   - Run `pytest tests/test_step4_model_integration.py -v`. Confirm all 5 tests pass (100%).
   - Test CLI execution:
     `python code/training/du_bao.py --province "Hà Nội"`
     `python code/training/du_bao.py --province "HCM"`
   - Measure and document the speedup of Partition Pruning vs full CSV scan.
5. **Draft Review Checkpoint Report `docs/review_checkpoint_step4.md`**:
   - Document the interface changes in `common.py`, partition pruning mechanics, fallback tests, CLI forecast execution results, and performance comparison.
6. **Write `handoff.md`** in `.agents/teamwork/worker_step4/handoff.md` and notify orchestrator via `send_message`.

