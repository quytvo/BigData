## 2026-11-09T05:13:39Z
[Message] sender=3888b5ae-cede-4e9e-8868-75bef4ef4193 priority=MESSAGE_PRIORITY_HIGH
You are Challenger 1 for Milestone 4 (Step 4: Integration with Training & Inference - Parquet + Partition Pruning + Fallback).

## Scope & Instructions
- Working directory: `d:\năm 4\big data\Final_Project_Plan`
- State folder: `.agents/teamwork/challenger_step4_1/`
- User Request: `.agents/teamwork/ORIGINAL_REQUEST.md`
- Target Code: `code/training/common.py`, `code/training/du_bao.py`, `code/training/train_global_ann.py`

## Adversarial Tasks
1. Empirical Partition Pruning Verification:
   - Does `common.load_merged("Hà Nội", use_parquet=True)` really prune partitions?
   - Measure file I/O or scan paths: verify that files from the other 10 province folders are NEVER touched when requesting a single province.
   - Verify that the returned DataFrame has exactly 25,512 rows, zero duplicates, and only `province_name == 'Hà Nội'`.
   - Test all 11 provinces in a loop to ensure no province partition has corrupt schema or missing columns.
2. Fallback Stress Testing:
   - Test fallback behavior when `SILVER_PROVINCE_DIR` is temporarily pointed to an empty or non-existent path. Does it gracefully fallback to CSV without raising unhandled errors?
   - Test `use_parquet=False` for both single-province and all-provinces modes.
3. Run `pytest tests/test_step4_model_integration.py -v`.
4. Output your detailed challenge report to `.agents/teamwork/challenger_step4_1/challenge.md` and handoff report to `.agents/teamwork/challenger_step4_1/handoff.md`.
5. Explicitly conclude with **APPROVE** or **REQUEST_CHANGES**, and notify the orchestrator via `send_message`.
