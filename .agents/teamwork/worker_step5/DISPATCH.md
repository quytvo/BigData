## 2026-11-09T05:29:46Z
Bạn là Worker phụ trách triển khai Milestone 5 (Step 5: Multi-Scale Benchmark 1x, 10x, 50x & Full-Stack Docker Container Execution).

Đọc kỹ tài liệu yêu cầu gốc tại:
`d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\ORIGINAL_REQUEST.md`

Đọc tài liệu kiến trúc toàn cục và khảo sát tại:
- `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\orchestrator\PROJECT.md`
- `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\orchestrator\TEST_INFRA.md`
- `d:\năm 4\big data\Final_Project_Plan\.agents\teamwork\explorer_survey_3\survey_infra_benchmark.md`
- `d:\năm 4\big data\Final_Project_Plan\tests\test_step5_benchmark_docker.py`

MANDATORY INTEGRITY WARNING:
DO NOT CHEAT. All implementations must be genuine. DO NOT hardcode test results, create dummy/facade implementations, or circumvent the intended task. A teamwork_auditor will independently verify your work. Integrity violations WILL be detected and your work WILL be rejected.

QUYỀN SỞ HỮU FILE (WRITE OWNERSHIP):
Bạn sở hữu độc quyền các file sau:
- `code/lakehouse/benchmark_spark_vs_pandas.py`
- `output/cloudsim_benchmark/spark_storage_benchmark.csv`
- `docs/review_checkpoint_step5.md`
- `.agents/teamwork/worker_step5/handoff.md`
- `.agents/teamwork/worker_step5/progress.md`

NHIỆM VỤ CỤ THỂ CẦN THỰC HIỆN:

1. Xây dựng module đo kiểm `code/lakehouse/benchmark_spark_vs_pandas.py`:
   - Module phải đo kiểm và đối chứng hiệu năng giữa Pandas (CSV) và PySpark (Parquet Snappy) ở 3 mức quy mô (Multi-Scale Workload):
     * 1x: 280.632 dòng (bộ dữ liệu Silver thực tế 11 tỉnh x 25.512 giờ)
     * 10x: 2.806.320 dòng (nhân bản x10 chuỗi thời gian / trạm)
     * 50x: 14.031.600 dòng (nhân bản x50 chuỗi thời gian / trạm)
   - Lưu trữ kết quả đo kiểm ra file `output/cloudsim_benchmark/spark_storage_benchmark.csv` (đảm bảo tạo thư mục cha nếu chưa có).
   - Schema của file CSV kết quả phải đáp ứng chính xác các assertion trong `tests/test_step5_benchmark_docker.py`:
     * Cột tỷ lệ quy mô: `scale` (chứa các giá trị "1x", "10x", "50x")
     * Cột công cụ: `engine` hoặc `framework` (chứa "Pandas", "Spark")
     * Cột định dạng: `format` hoặc `storage_format` (chứa "CSV", "Parquet")
     * Cột số dòng: `rows` hoặc `row_count` (chính xác 280632 cho 1x, 2806320 cho 10x, 14031600 cho 50x)
     * Cột dung lượng lưu trữ: `storage_size_mb` (hoặc `storage_mb`, `size_mb`)
     * Cột thời gian xử lý: `etl_runtime_sec` (hoặc `processing_sec`, `runtime_sec`, `duration_sec`)
     * Cột tỷ lệ nén: `compression_ratio_pct` (hoặc tỷ lệ nén, xác nhận Parquet nhỏ hơn CSV và tỷ lệ nén > 50%)
     * Các cột đo kiểm truy vấn và partition pruning bổ trợ: `query_1prov_latency_ms`, `bytes_scanned_mb`, `pruning_speedup_factor`
   - Quản lý bộ nhớ chống OOM (Out Of Memory):
     * Container Docker chỉ có 8GB RAM (`-m 8g`).
     * Không giữ đồng thời các DataFrame 50x của cả Spark và Pandas trong bộ nhớ.
     * Chạy tuần tự: Đo Spark, dọn dẹp biến, xóa cache Spark `spark.catalog.clearCache()`, thu gom rác Python `gc.collect()`, sau đó mới đo Pandas (dùng chunking/generator nếu cần để tránh OOM).
     * Dữ liệu giả lập 10x, 50x phải được dọn dẹp các thư mục tạm sau khi đo xong, không để rác phình đĩa.
   - Kích hoạt Spark event logging vào `output/spark_event_logs/` thông qua `code/lakehouse/spark_session.py`.

2. Thực thi Benchmark và Kiểm thử trong Docker Container:
   - Chạy lệnh Docker chuẩn hóa với tài nguyên 8GB RAM, 4 CPUs:
     `docker run --rm -m 8g --cpus 4 -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 python3 code/lakehouse/benchmark_spark_vs_pandas.py`
   - Xác nhận file `output/cloudsim_benchmark/spark_storage_benchmark.csv` đã được tạo ra với số liệu thực tế đo được.
   - Chạy bộ kiểm thử tự động của Step 5:
     `docker run --rm -m 8g --cpus 4 -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 pytest tests/test_step5_benchmark_docker.py -v`
     (Đồng thời có thể kiểm tra thêm với `pytest tests/test_step5_benchmark_docker.py -v` trên host).
     Xác nhận 100% test case (6/6 tests) đạt PASS.

3. Lập Báo cáo Nghiệm thu Step 5:
   - Tạo file `docs/review_checkpoint_step5.md` với đầy đủ cấu trúc chuẩn:
     * Tổng quan mục tiêu Step 5
     * Bảng số liệu benchmark đa quy mô (1x, 10x, 50x) chi tiết: dung lượng (MB), thời gian ETL (s), độ trễ truy vấn 1 tỉnh (ms), tỷ lệ nén Snappy Parquet vs CSV
     * Phân tích kỹ thuật: tại sao Spark vượt trội khi dữ liệu lớn dần, hiệu quả của Partition Pruning, Columnar Projection
     * Minh chứng thực thi Docker: thông số container (-m 8g --cpus 4), không gặp lỗi OOM
     * Minh chứng Event Logs trong `output/spark_event_logs/`
     * Kết quả thực thi `pytest tests/test_step5_benchmark_docker.py` (6/6 PASS).

4. Báo cáo hoàn thành:
   - Viết `.agents/teamwork/worker_step5/handoff.md` theo Handoff Protocol (Observation, Logic Chain, Caveats, Conclusion, Verification Method).
   - Gửi thông báo hoàn thành qua `send_message` cho orchestrator.
