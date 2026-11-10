## 2026-11-09T02:29:56Z

Chuyển đổi toàn bộ pipeline xử lý dữ liệu chất lượng không khí từ Pandas sang PySpark trên kiến trúc Lakehouse Medallion (Bronze - Silver - Gold) và kiểm thử thực thi trong Docker container phục vụ đồ án môn học. Quá trình thực hiện tuần tự từ Step 1 đến Step 5 theo đặc tả với bộ kiểm thử độc lập và các báo cáo nghiệm thu chi tiết sau từng bước.

Working directory: d:\năm 4\big data\Final_Project_Plan
Integrity mode: development

## Verification Resources
- Bảng dữ liệu đối chứng Pandas: `data_test/cleaned_tedp_province_4y.csv` và `data_test/weather_openmeteo_province_4y.csv`
- Script đối chứng logic Pandas: `code/crawl_data/preprocess_tedp.py` và `code/crawl_data/sync_tedp_province.py`
- Script kiểm thử tích hợp mô hình: `code/training/du_bao.py` và `code/training/train_global_ann.py`

## Requirements

### R1. Triển khai Môi trường Docker và Cấu hình SparkSession (Step 1)
Xây dựng Dockerfile Spark cho môi trường Linux container (Docker 6GB–8GB RAM, 4 CPUs) và module cấu hình `SparkSession` tối ưu hóa cho máy local, kích hoạt Arrow, AQE, múi giờ UTC, ghi event logs vào `output/spark_event_logs/` và hỗ trợ giữ Spark Web UI trên cổng 4040 qua cờ `--keep-ui`.

### R2. Chuyển đổi Pipeline Tiền Xử Lý Dữ Liệu Bronze sang Silver (Step 2)
Chuyển đổi các logic lọc QCVN, nội suy Window thuần Catalyst cấp trạm và cấp tỉnh sang PySpark trong `code/lakehouse/spark_preprocess_tedp.py` và `code/lakehouse/spark_sync_province.py`. Dữ liệu làm sạch cấp trạm và dữ liệu tổng hợp cấp tỉnh (280.632 dòng x 11 tỉnh) phải được lưu trữ dạng Parquet nén Snappy tại `lakehouse_storage/silver/`.

### R3. Trích Xuất 49 Đặc Trưng Silver sang Gold và Pipeline Điều Phối (Step 3)
Triển khai trích xuất 49 đặc trưng khí quyển bằng Spark Window functions thuần túy trong `code/lakehouse/spark_feature_engineering.py` và lưu trữ tại `lakehouse_storage/gold/global_ann_features/`. Xây dựng script điều phối liên hoàn `code/lakehouse/build_lakehouse_spark.py` tự động chạy toàn bộ pipeline và đối chứng dữ liệu (Data Diff) với bản Pandas.

### R4. Tích Hợp Tiêu Thụ Dữ Liệu Parquet vào Mô Hình Huấn Luyện và Dự Báo (Step 4)
Cập nhật `code/training/common.py`, `code/training/train_global_ann.py` và `code/training/du_bao.py` để ưu tiên nạp dữ liệu từ Parquet trong `lakehouse_storage/` và tận dụng Partition Pruning khi truy vấn theo tỉnh, đồng thời duy trì cơ chế tự động fallback về CSV trong `data_test/`.

### R5. Benchmark Hiệu Năng và Kiểm Thử Toàn Trình trong Docker (Step 5)
Xây dựng module đo kiểm đa quy mô 1x, 10x, 50x (`code/lakehouse/benchmark_spark_vs_pandas.py`), build Docker image và thực thi toàn bộ pipeline kiểm thử trong Docker container 8GB RAM, xuất kết quả đo kiểm ra `output/cloudsim_benchmark/spark_storage_benchmark.csv`.

### R6. Xây Dựng Bộ Kiểm Thử Độc Lập và Báo Cáo Nghiệm Thu Từng Bước
Xây dựng một bộ kiểm thử tự động độc lập (test suite) để kiểm thử programmatic cho từng bước từ Step 1 đến Step 5 (kiểm tra tính đúng đắn của schema, số dòng, phân vùng, độ liên tục chuỗi thời gian, và sai số tuyệt đối so với Pandas baseline). Báo cáo kiểm tra nghiệm thu (review checkpoint) chi tiết được lập sau mỗi bước trước khi chuyển sang bước kế tiếp.


## 2026-11-09T02:56:00Z
[CHỈ THỊ BẮT BUỘC TỪ NGƯỜI DÙNG - ÁP DỤNG NGAY TỪ STEP 2 / TASK 2 TRỞ ĐI]
Người dùng yêu cầu CHỈ sử dụng mô hình hiện tại của parent (`Model: "inherit"`) cho tất cả các subagent được gọi trong team (tức là luôn truyền `Model: "inherit"` hoặc để mặc định, TUYỆT ĐỐI KHÔNG dùng `flash`, `flash_lite` hay bất kỳ model nào khác khi gọi `invoke_subagent` từ Step 2 trở đi).
Vui lòng cập nhật ngay ràng buộc này vào BRIEFING.md của orchestrator và áp dụng nghiêm ngặt cho toàn bộ các lượt dispatch subagent từ Step 2 trở đi.


## 2026-11-09T02:58:40Z
[CẬP NHẬT CHỈ THỊ LỰA CHỌN MODEL TỪ NGƯỜI DÙNG]
Người dùng cập nhật quy định về Model:
- Đối với các subagent viết code triển khai phức tạp vào dự án (Implementation Workers cho Step 2, Step 3, Step 4, Step 5): BẮT BUỘC sử dụng `Model: "inherit"` (Gemini 4 Argon).
- Đối với các subagent làm nhiệm vụ review, challenge, kiểm thử hoặc tác vụ phụ trợ: ĐƯỢC PHÉP sử dụng các mô hình khác (`flash`, `pro`, v.v.) để tối ưu hóa tốc độ và tài nguyên.
Vui lòng cập nhật ngay vào BRIEFING.md của orchestrator và áp dụng vào các lượt dispatch tiếp theo.
