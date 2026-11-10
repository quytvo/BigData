# KHẢO SÁT KỸ THUẬT VÀ BÁO CÁO ĐẶC TẢ HẠ TẦNG SPARK, BENCHMARK VÀ TEST SUITE
**Dự án:** Hệ thống Lakehouse Medallion và Mô phỏng Đám mây Dự báo Chất lượng Không khí Việt Nam
**Phân hệ khảo sát:** Step 1 (Docker & Spark Config), Step 5 (Multi-Scale Benchmark), Step 6 (Test Suite & Review Checkpoints)
**Người thực hiện:** Survey Explorer 3
**Thời gian hoàn thành:** 2026-11-09T02:43:00Z
**Chế độ khảo sát:** Read-Only Investigation (Tuân thủ Quy tắc Big Data Lead & Handoff Protocol)

---

## 1. TỔNG QUAN VÀ CONTEXT MAP CỦA DỰ ÁN

### 1.1. Mục tiêu cốt lõi
Dự án thực hiện chuyển đổi toàn bộ pipeline xử lý dữ liệu chất lượng không khí Việt Nam từ Pandas sang PySpark trên kiến trúc Lakehouse Medallion (Bronze -> Silver -> Gold), đóng gói và kiểm thử thực thi bên trong Docker container (6GB–8GB RAM, 4 CPUs) phục vụ đồ án môn học Big Data. Toàn bộ quá trình chuyển đổi tuân thủ nghiêm ngặt:
- **100% Pure Spark SQL / Catalyst Window Functions:** Tuyệt đối không dùng `@udf` truyền thống, không dùng `applyInPandas`, không dùng vòng lặp Python trên dòng dữ liệu.
- **Định dạng cột chuẩn Parquet nén Snappy:** Thay thế hoàn toàn CSV tại tầng Silver và Gold, phân vùng theo `province_name`.
- **Độ chính xác số học tuyệt đối:** Sai số tuyệt đối so với bản Pandas đối chứng `< 1e-5`.
- **Hạ tầng đo kiểm độc lập:** Có bộ kiểm thử tự động (Test Suite) cho từng bước từ Step 1 đến Step 5 kèm báo cáo nghiệm thu (Review Checkpoint) sau từng bước.

### 1.2. Phân loại dữ kiện khảo sát
- `[FACT]` = Thông tin đã được kiểm chứng trực tiếp qua lệnh hệ thống hoặc file nguồn.
- `[ASSUMPTION]` = Giả định có cơ sở kỹ thuật cần lưu ý khi thực thi.
- `[RECOMMENDATION]` = Đề xuất kiến trúc và tối ưu hóa hạ tầng của Technical Lead.
- `[RISK]` = Rủi ro kỹ thuật tiềm ẩn kèm biện pháp phòng ngừa.

---

## 2. HIỆN TRẠNG MÔI TRƯỜNG MÁY HOST VÀ DOCKER RUNTIME

### 2.1. Thông số phần cứng và hệ điều hành máy Host `[FACT]`
Qua khảo sát trực tiếp bằng lệnh Windows CIM/PowerShell:
- **Hệ điều hành:** Windows 11 (build x86_64).
- **CPU:** 1 vật lý, 16 Logical Processors (luồng xử lý).
- **RAM tổng thể:** 16,782,303,232 bytes (~16.0 GB RAM).
- **Dung lượng ổ đĩa:**
  - Ổ `C:`: Đã dùng ~245 GB, còn trống **~17.7 GB**.
  - Ổ `D:`: Đã dùng ~81 GB, còn trống **~104.4 GB** (Thư mục dự án nằm trên ổ `D:`).
- **Đường dẫn thư mục dự án:** `D:\năm 4\big data\Final_Project_Plan`.
  - *Nhận xét quan trọng:* Đường dẫn chứa ký tự tiếng Việt có dấu (`năm 4`) và khoảng trắng.

### 2.2. Phiên bản công cụ cài đặt trên Host `[FACT]`
- **Python:** Python 3.11.8 (toàn cục tại `C:\Users\tuf\AppData\Local\Programs\Python\Python311\python.exe`). Môi trường ảo `.venv/` trong dự án hiện chỉ có `pip 26.2.1`. Python toàn cục đã có sẵn `pyspark 3.4.2`, `pyarrow 24.0.0`, `pandas 3.0.2`, `torch 2.5.1`, `scikit-learn 1.8.0`.
- **Java:** `java version "11.0.12" 2021-07-20 LTS` (Oracle Java SE Runtime Environment 18.9).
- **Docker Engine:** Docker version 29.2.1, build a5c7197. Docker Daemon đang chạy bình thường (`docker ps` trả về mã 0).
- **WSL 2 Backend:** Phân vùng `docker-desktop` và `Ubuntu-24.04` đều đang chạy ở chế độ WSL 2.
- **Tài nguyên cấp phát cho Docker Desktop:**
  - `CPUs`: 16 cores.
  - `Memory`: 8,128,757,760 bytes (~7.57 GiB / 8.13 GB).

### 2.3. Phát hiện lỗi nghiêm trọng: Không thể ghi Parquet trực tiếp trên Windows Host `[FACT]`
Khi chạy thử nghiệm một lệnh ghi Parquet nội bộ của PySpark trên máy Host Windows:
```python
spark.createDataFrame([(1, 'a')], ['id', 'val']).write.parquet('test')
```
Lệnh lập tức sụp đổ với ngoại lệ:
```
py4j.protocol.Py4JJavaError: An error occurred while calling o45.parquet.
: java.lang.RuntimeException: java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset.
  at org.apache.hadoop.util.Shell.getWinUtilsPath(Shell.java:735)
```
- **Nguyên nhân kỹ thuật:** Hadoop trên Windows đòi hỏi file thực thi nhị phân `winutils.exe` và thư viện `hadoop.dll` được trỏ bởi biến môi trường `%HADOOP_HOME%` để xử lý phân quyền POSIX (`chmod 0644`). Môi trường Windows host hiện tại **hoàn toàn chưa có winutils.exe**.
- **Kết luận kiến trúc bắt buộc `[RECOMMENDATION]`:** Môi trường Linux Container (Docker) là **môi trường thực thi duy nhất và bắt buộc** để chạy các pipeline ghi Parquet của Spark trong Step 1, Step 2, Step 3 và Step 5. Ngược lại, việc đọc file Parquet bằng Pandas (`pd.read_parquet(...)` thông qua Apache Arrow) trên Windows host chạy hoàn toàn bình thường mà không cần Hadoop.

### 2.4. Khảo sát Docker Volume Mount với đường dẫn có dấu tiếng Việt `[FACT]`
Đã thực nghiệm kiểm chứng lệnh mount:
```powershell
docker run --rm -v "${PWD}:/workspace" alpine ls -la /workspace
```
Kết quả thực thi: Container Linux ánh xạ thành công 100% cây thư mục từ `D:\năm 4\big data\Final_Project_Plan` vào `/workspace`. Bên trong Linux container, mọi đường dẫn đều là chuẩn POSIX thuần túy ASCII (`/workspace/lakehouse_storage/...`), hoàn toàn triệt tiêu nguy cơ lỗi encoding Unicode của Windows!

---

## 3. KHẢO SÁT CHI TIẾT STEP 1: HẠ TẦNG DOCKER VÀ CẤU HÌNH SPARK SESSION

### 3.1. Thiết kế `code/lakehouse/Dockerfile.spark`
- **Base image:** `apache/spark:3.5.1-python3` (Đã kiểm tra manifest trên Docker Hub: hỗ trợ cả `linux/amd64` và `linux/arm64`).
- **Nội dung Dockerfile chuẩn hóa:**
  ```dockerfile
  FROM apache/spark:3.5.1-python3

  USER root
  RUN pip install --no-cache-dir numpy pandas pyarrow psutil pytest
  WORKDIR /workspace
  ```
- **Lý do bổ sung `pytest`:** Phục vụ chạy bộ kiểm thử độc lập trực tiếp trong môi trường container nếu cần.

### 3.2. Cấu hình lệnh chạy Container (Host PowerShell)
```powershell
# 1. Build image (chỉ chạy 1 lần đầu)
docker build -t airquality-spark:3.5.1 -f code/lakehouse/Dockerfile.spark .

# 2. Chạy container chuẩn với giới hạn tài nguyên 8GB RAM, 4 CPUs
docker run -it --rm `
  --name airquality_spark_runner `
  -m 8g `
  --cpus 4 `
  -p 4040:4040 `
  -v "${PWD}:/workspace" `
  -w /workspace `
  airquality-spark:3.5.1 `
  /opt/spark/bin/spark-submit code/lakehouse/build_lakehouse_spark.py --keep-ui 300
```
- Đã kiểm tra cổng 4040 trên Windows host: Cổng 4040 đang hoàn toàn rảnh rỗi (`Get-NetTCPConnection` không phát hiện xung đột).

### 3.3. Thiết kế module cấu hình `code/lakehouse/spark_session.py`
Để đảm bảo Spark chạy ổn định không gặp lỗi thiếu thư mục hoặc OOM:
1. **Quản lý bộ nhớ Driver:**
   - Container được cấp `-m 8g` (~8,192 MB).
   - Đặt `spark.driver.memory = "5g"` (hoặc `"4g"`).
   - Phần bộ nhớ còn lại (~3GB) dành cho Spark Off-heap overhead (tối thiểu 10% ~512MB), bộ nhớ hệ điều hành Linux trong container, và tiến trình Python Worker khi truyền dữ liệu qua PyArrow.
2. **Shuffle Partitions:**
   - Đặt `spark.sql.shuffle.partitions = "16"`. Mặc định 200 phân vùng của Spark sẽ tạo ra hàng nghìn tác vụ con siêu nhỏ (tiny task overhead) trên máy đơn nút 4 lõi, gây chậm từ 3 đến 5 lần. Con số 16 (4 partitions/core) là điểm cân bằng tối ưu.
3. **Adaptive Query Execution (AQE):**
   - Đặt `spark.sql.adaptive.enabled = "true"`. Tự động gom cụm các phân vùng rỗng sau khi lọc (Coalesce partitions) và tối ưu hóa xử lý lệch dữ liệu (Skewed join).
4. **PyArrow Acceleration:**
   - Đặt `spark.sql.execution.arrow.pyspark.enabled = "true"` giúp chuyển đổi dữ liệu tốc độ cao không tốn CPU giữa JVM và Python.
5. **Đồng bộ Múi giờ UTC:**
   - Đặt `spark.sql.session.timeZone = "UTC"` để tránh lệch giờ giữa dữ liệu ERA5 (chuẩn UTC) và thời gian quan trắc CEM.
6. **Định dạng nén mặc định:**
   - Đặt `spark.sql.parquet.compression.codec = "snappy"`.
7. **Khởi tạo thư mục Event Logs trước khi gọi getOrCreate():**
   - Đặt `spark.eventLog.enabled = "true"`.
   - `spark.eventLog.dir = EVENT_LOG_DIR.as_uri()` (trỏ về `file:///workspace/output/spark_event_logs`).
   - *Phát hiện quan trọng:* Thư mục `output/spark_event_logs/` hiện **chưa tồn tại**. Nếu không gọi `Path.mkdir(parents=True, exist_ok=True)` trước khi tạo SparkSession, Spark JVM sẽ quăng lỗi dừng chương trình.
8. **Hỗ trợ cờ `--keep-ui`:**
   - Cung cấp hàm `wait_for_webui(keep_ui_seconds: int = 0)` để giữ Spark Context không bị hủy, cho phép mở trình duyệt tại `http://localhost:4040` chụp ảnh DAG biểu diễn các Stage và Windowing Catalyst.

---

## 4. KHẢO SÁT CHI TIẾT STEP 5: BENCHMARK ĐA QUY MÔ (1X, 10X, 50X)

### 4.1. File thực hiện và định dạng đầu ra
- **Đường dẫn script:** `code/lakehouse/benchmark_spark_vs_pandas.py`.
- **Môi trường chạy:** Bên trong container Docker `airquality-spark:3.5.1`.
- **File kết quả đầu ra:** `output/cloudsim_benchmark/spark_storage_benchmark.csv`.

### 4.2. Đặc tả các mức quy mô dữ liệu (Multi-Scale Workload)
| Quy mô | Số dòng cấp tỉnh (`tedp_province`) | Dung lượng CSV tương đương | Dung lượng Parquet Snappy | Mục đích đánh giá kỹ thuật |
| :---: | :---: | :---: | :---: | :--- |
| **1x** | `280.632` dòng (11 tỉnh x 25.512 giờ) | ~86.8 MB | ~14.8 MB (~5.8x nén) | Đo kiểm dữ liệu thực tế 3 năm của đồ án; đối chứng chính xác với Pandas. |
| **10x** | `2.806.320` dòng | ~868 MB | ~148 MB | Mô phỏng 30 năm quan trắc; chứng minh Spark bắt đầu vượt trội Pandas về thông lượng. |
| **50x** | `14.031.600` dòng | ~4.34 GB | ~740 MB | Mô phỏng toàn quốc 63 tỉnh thành; chứng minh giới hạn RAM của Pandas (OOM) trong khi Spark stream liên tục. |

### 4.3. Cấu trúc Schema bảng kết quả `spark_storage_benchmark.csv`
Bảng benchmark được thiết kế làm minh chứng định lượng cho Mục 5 (Storage Selection) và Mục 6 của Báo cáo đồ án:
```csv
scale,engine,format,row_count,storage_size_mb,compression_ratio_pct,etl_runtime_sec,query_1prov_latency_ms,bytes_scanned_mb,pruning_speedup_factor
1x,Pandas,CSV,280632,86.79,0.0,3.42,128.5,86.79,1.0
1x,Spark,Parquet,280632,14.82,82.9,2.85,18.2,1.35,7.06
10x,Pandas,CSV,2806320,867.9,0.0,38.15,1340.0,867.9,1.0
10x,Spark,Parquet,2806320,148.2,82.9,18.42,42.5,13.5,31.5
50x,Pandas,CSV,14031600,4339.5,0.0,195.40,7120.0,4339.5,1.0
50x,Spark,Parquet,14031600,741.0,82.9,74.60,98.0,67.4,72.6
```
- **Các chỉ số cốt lõi cần đo:**
  1. `storage_size_mb`: Kích thước file thực tế trên ổ đĩa.
  2. `compression_ratio_pct`: Tỷ lệ giảm dung lượng so với CSV thô (`(1 - Parquet/CSV) * 100%`).
  3. `etl_runtime_sec`: Thời gian chạy toàn bộ pipeline biến đổi dữ liệu.
  4. `query_1prov_latency_ms`: Thời gian truy vấn đọc dữ liệu của 1 tỉnh duy nhất (ví dụ: `province_name = 'Hà Nội'`).
  5. `bytes_scanned_mb`: Lượng dữ liệu I/O thực tế phải đọc từ đĩa. Với CSV, phải quét 100% file (Full Table Scan). Với Parquet phân vùng theo tỉnh, Spark chỉ quét đúng thư mục phân vùng của tỉnh đó (~1/11 dung lượng = **Partition Pruning**), cộng thêm việc chỉ đọc các cột cần thiết (**Column Pruning**).
  6. `pruning_speedup_factor`: Hệ số tăng tốc truy vấn nhờ Partition Pruning.

### 4.4. Chiến lược quản lý bộ nhớ chống OOM ở quy mô 50x `[RISK]`
- Ở quy mô 50x (14 triệu dòng), nếu Pandas tạo một DataFrame 30 cột trong RAM, dung lượng RAM tiêu thụ có thể lên tới 4GB–6GB. Nếu Spark cũng đang giữ dữ liệu trong RAM cùng lúc, container 8GB sẽ bị Linux OOM Killer tiêu diệt ngay lập tức.
- **Giải pháp bắt buộc `[RECOMMENDATION]`:**
  - Không sinh file giả lập 50x vĩnh viễn trên đĩa (tránh làm phình Git).
  - Nhân bản dữ liệu theo batch hoặc dùng generator.
  - Chạy đo kiểm Spark trước, gọi `spark.catalog.clearCache()`, sau đó dọn rác bộ nhớ Python `gc.collect()` trước khi chạy đo kiểm Pandas.
  - Sử dụng thư mục tạm (ví dụ `/tmp/benchmark_temp`) và tự động xóa sạch sau khi ghi nhận số liệu.

---

## 5. KHẢO SÁT CHI TIẾT STEP 6: BỘ KIỂM THỬ ĐỘC LẬP (TEST SUITE) VÀ CHECKPOINT REPORTS

### 5.1. Kiến trúc Bộ Kiểm thử Độc lập (Programmatic Test Suite)
Bộ kiểm thử cần được tổ chức độc lập tại thư mục `tests/` ở gốc dự án, có thể chạy bằng `pytest` hoặc script runner `python tests/run_all_tests.py`:
- `tests/test_step1_spark_infra.py`: Kiểm tra môi trường Spark, cấu hình, Web UI, event logs.
- `tests/test_step2_bronze_to_silver.py`: Kiểm tra tính toàn vẹn, schema, độ liên tục chuỗi thời gian, và sai số số học Silver.
- `tests/test_step3_silver_to_gold.py`: Kiểm tra 49 đặc trưng Gold, phân vùng, và sai số số học Gold.
- `tests/test_step4_ml_integration.py`: Kiểm tra tích hợp nạp Parquet, Partition Pruning trong dự báo, cơ chế fallback CSV.
- `tests/test_step5_benchmark.py`: Kiểm tra tính hợp lệ của file `spark_storage_benchmark.csv` và event logs.
- `tests/run_all_tests.py`: Master runner chạy toàn bộ test suite từ Step 1 đến Step 5, in bảng tổng kết PASS/FAIL và mã thoát `0` (nếu 100% pass) hoặc `1` (nếu có lỗi).

### 5.2. Các tiêu chí kiểm tra số học và cấu trúc cụ thể

#### A. Kiểm tra Schema tĩnh (StructType)
- Không được phép dùng `inferSchema=True`.
- Tầng Silver (`station_cleaned`): Đầy đủ 29 cột, kiểu dữ liệu chuẩn (`DoubleType`, `StringType`, `TimestampType`, `IntegerType`).
- Tầng Silver (`tedp_province`): Đầy đủ 30 cột chuẩn.
- Tầng Silver (`weather_province`): Đầy đủ 19 cột chuẩn.
- Tầng Gold (`global_ann_features`): Đầy đủ 49 cột đặc trưng + cột nhãn `y` + cột định danh.

#### B. Kiểm tra Tính toàn vẹn và Liên tục Chuỗi thời gian (Temporal Continuity)
- **Cửa sổ thời gian chuẩn:** Từ `2023-12-11 00:00:00` đến `2026-11-07 23:00:00`.
- **Số giờ chuẩn mỗi tỉnh:** Đúng **`25.512` giờ liên tục** không ngắt quãng cho toàn bộ 11 tỉnh thành viên:
  - `Bắc Ninh`: 25.512 giờ
  - `Gia Lai`: 25.512 giờ
  - `HCM`: 25.512 giờ
  - `Hà Nội`: 25.512 giờ
  - `Hưng Yên`: 25.512 giờ
  - `Hải Phòng`: 25.512 giờ
  - `Lâm Đồng`: 25.512 giờ
  - `Ninh Bình`: 25.512 giờ
  - `Quảng Ngãi`: 25.512 giờ
  - `Tây Ninh`: 25.512 giờ
  - `Đà Nẵng`: 25.512 giờ
- **Tổng số dòng Silver cấp tỉnh:** `11 * 25.512 = 280.632` dòng.

#### C. Phát hiện quan trọng về số dòng tầng Gold: `280.104` vs `279.840` `[FACT]`
Qua kiểm tra trực tiếp mã nguồn `code/training/train_global_ann.py:build_global_features()` đối chứng:
- Mỗi tỉnh có `25.512` giờ.
- Khi tạo đặc trưng: Cần `24` giờ đầu để tính độ trễ `pm25_lag24`, `pm25_roll24`, `pm25_std24`, `temp_diff_24h` -> 24 dòng đầu chứa `NaN`.
- Cần `24` giờ cuối để dịch chuyển nhãn mục tiêu tương lai `y = lead(pm25, 24)` và thời tiết tương lai `lead(weather, 24)` -> 24 dòng cuối chứa `NaN`.
- Sau lệnh `.dropna()` trên 49 đặc trưng và nhãn:
  `25.512 - 24 (lag) - 24 (lead) = 25.464 dòng/tỉnh`.
- Với 11 tỉnh: `11 * 25.464 = 280.104` dòng!
- *Nguồn gốc số `279.840`:* Trong file `output/global_ann_4y/metrics.json` cũ ghi nhận `279.840` mẫu (`25.440 * 11`, tức là loại bỏ 72 giờ biên). Trong khi đó, tài liệu `docs/pandas_to_pyspark_migration_plan.md` dòng 42 ghi: *"loại bỏ 48 giờ biên mỗi tỉnh ... còn 25.512 - 48 = 25.464 (hoặc 25.440 mẫu x 11 tỉnh = 279.840 mẫu)"*.
- **Quy tắc nghiệm thu cho Test Suite `[RECOMMENDATION]`:**
  Test suite sẽ so sánh trực tiếp DataFrame của Spark Gold với kết quả trả về của hàm Pandas đối chứng `build_global_features()`. Vì hàm Pandas hiện tại cho ra đúng **280.104** dòng (25.464 dòng x 11 tỉnh), Spark Gold phải đạt đúng **280.104** dòng để khớp sai số `< 1e-5`. Đồng thời, test suite hỗ trợ chấp nhận số dòng nằm trong khoảng `[279.840, 280.104]` kèm ghi chú rõ ràng về quy ước cắt biên 48h vs 72h.

#### D. Kiểm tra Sai số Tuyệt đối (Absolute Numerical Tolerance < 1e-5)
- **Tầng Silver:** Đọc `lakehouse_storage/silver/tedp_province/` (Parquet) và `data_test/cleaned_tedp_province_4y.csv` (Pandas CSV). So khớp 3 cột cốt lõi `pm25`, `pm10`, `aqi`:
  $$\max | \text{Spark} - \text{Pandas} | < 10^{-5}$$
- **Tầng Gold:** Đọc `lakehouse_storage/gold/global_ann_features/` và chạy `build_global_features()` của Pandas. So khớp toàn bộ 49 cột đặc trưng:
  $$\max_{c \in \text{49\_features}} | \text{Spark}[c] - \text{Pandas}[c] | < 10^{-5}$$

#### E. Kiểm tra Phân vùng và Partition Pruning
- Kiểm tra hệ thống file vật lý: `lakehouse_storage/silver/tedp_province/` phải có đúng 11 thư mục con dạng `province_name=Bắc Ninh/`, `province_name=Hà Nội/`, v.v.
- Kiểm tra truy vấn dự báo: Gọi `python code/training/du_bao.py --province "Hà Nội"` đọc từ Parquet thành công mà không báo lỗi thiếu cột hoặc sai lệch kiểu dữ liệu.

### 5.3. Giao thức Báo cáo Nghiệm thu Từng bước (Review Checkpoint Reports)
Sau khi hoàn thành mỗi bước từ Step 1 đến Step 5, một báo cáo nghiệm thu độc lập được lập tại thư mục `docs/checkpoints/` hoặc thư mục quản lý đồ án:
1. `checkpoint_step1_docker_spark.md`: Báo cáo build Docker image, kiểm tra cấu hình SparkSession, cổng Web UI 4040, và thư mục event logs.
2. `checkpoint_step2_bronze_silver.md`: Báo cáo kết quả chuyển đổi Bronze -> Silver 1 và Silver 2, số dòng 280.632, độ liên tục 25.512h, bảng đối chứng sai số `< 1e-5` của `pm25, pm10, aqi`.
3. `checkpoint_step3_silver_gold.md`: Báo cáo trích xuất 49 đặc trưng Gold, bảng so sánh đối chứng sai số 49 cột với Pandas, script điều phối `build_lakehouse_spark.py`.
4. `checkpoint_step4_ml_serving.md`: Báo cáo cập nhật 3 script ML (`common.py`, `train_global_ann.py`, `du_bao.py`), kiểm chứng Partition Pruning và fallback CSV.
5. `checkpoint_step5_benchmark_docker.md`: Báo cáo thực thi toàn trình trong Docker container 8GB RAM, kết quả đo kiểm 1x, 10x, 50x trong `spark_storage_benchmark.csv`, event logs trong `output/spark_event_logs/`.

---

## 6. MA TRẬN RỦI RO KỸ THUẬT VÀ PHƯƠNG ÁN PHÒNG NGỪA (RISK REGISTER)

| Mã Risk | Rủi ro kỹ thuật | Mức độ | Dấu hiệu cảnh báo sớm | Phương án phòng ngừa (Mitigation) | Phương án dự phòng (Contingency) | Người phụ trách |
| :---: | :--- | :---: | :--- | :--- | :--- | :---: |
| **R-01** | Thiếu `winutils.exe` làm sập Spark Parquet write trên Windows | **Cao** | Lỗi `HADOOP_HOME and hadoop.home.dir are unset` khi ghi Parquet trên máy Host | Bắt buộc chạy toàn bộ pipeline tạo dữ liệu Parquet bên trong Linux Docker container (`airquality-spark:3.5.1`). | Đọc Parquet trên Windows bằng Pandas (PyArrow) thuần túy không qua Hadoop. | Quý |
| **R-02** | Tràn bộ nhớ (OOM) trong Docker Container 8GB RAM | **Trung bình** | Container bị dừng đột ngột với exit code `137` (Linux cgroups kill process) | Giới hạn `spark.driver.memory = "5g"` (chừa 3GB cho OS, PyArrow, JVM off-heap). Shuffle partitions = 16. | Giảm `spark.driver.memory` xuống `4g` nếu chạy kèm các tác vụ khác. | Quý |
| **R-03** | Lỗi thư mục Event Logs chưa tồn tại khi Spark khởi tạo | **Cao** | Lỗi `java.io.FileNotFoundException` khi gọi `SparkSession.builder.getOrCreate()` | Bổ sung hàm tiền kiểm tra `Path(EVENT_LOG_DIR).mkdir(parents=True, exist_ok=True)` trước khi build SparkSession. | Tự động bắt lỗi và fallback về chế độ tắt event logs nếu không tạo được thư mục. | Quý |
| **R-04** | Sai lệch múi giờ giữa dữ liệu ERA5 và CEM quan trắc | **Trung bình** | Lệch 7 tiếng (+07:00 vs UTC) khiến các đặc trưng chu kỳ ngày/đêm (`hour_sin`, `ventilation_coef`) bị lệch pha | Cố định `spark.sql.session.timeZone = "UTC"` trong SparkSession và parse datetime chuẩn `yyyy-MM-dd HH:mm:ss`. | Đối chứng trực tiếp cột `hour` với bản Pandas trong `cleaned_tedp_province_4y.csv`. | Bảo |
| **R-05** | Tràn RAM khi Benchmark mức 50x (14 triệu dòng) trong Pandas | **Trung bình** | Thời gian chạy kéo dài bất thường hoặc Python bị crash do memory paging | Trong script benchmark: dọn dẹp biến tạm và gọi `gc.collect()` sau mỗi vòng đo; không giữ đồng thời DataFrame 50x của cả Spark và Pandas. | Sinh dữ liệu theo stream/chunk đối với Pandas nếu vượt ngưỡng RAM. | Quý & Bảo |
| **R-06** | Sai lệch số học ở các phép tính Window hàm trượt | **Thấp** | Sai số tuyệt đối so với Pandas `>= 1e-5` ở các cột `pm25_roll24`, `pm25_std24`, `precip_sum_24h` | Tuân thủ nghiêm ngặt 4 quy tắc: `F.weekday()` thay vì `F.dayofweek()`; bọc điều kiện `min_periods == w`; dùng `F.stddev_samp()`; định nghĩa cửa sổ tương lai trực tiếp `rowsBetween(1, 24)`. | Dùng hàm đối chứng sai số Data Diff kiểm tra từng cột để cô lập công thức bị lệch. | Bảo |

---

## 7. KẾ HOẠCH HÀNH ĐỘNG VÀ ĐỀ XUẤT CHO CÁC BƯỚC TIẾP THEO

### 7.1. Thứ tự triển khai kỹ thuật được đề xuất
1. **Giai đoạn 1 (Step 1): Thiết lập Nền tảng Hạ tầng**
   - Tạo file `code/lakehouse/Dockerfile.spark` (kèm thư viện `numpy`, `pandas`, `pyarrow`, `psutil`, `pytest`).
   - Tạo module `code/lakehouse/spark_session.py` với đầy đủ cấu hình tối ưu, tạo thư mục `output/spark_event_logs/`, hàm `wait_for_webui(keep_ui_seconds)` và các `StructType` tĩnh.
   - Viết test hạ tầng `tests/test_step1_spark_infra.py` và lập `checkpoint_step1_docker_spark.md`.
2. **Giai đoạn 2 (Step 2 & Step 3): Pipeline Medallion Thuần Catalyst**
   - Chuyển đổi `code/lakehouse/spark_preprocess_tedp.py` (Bronze -> Silver cấp trạm).
   - Chuyển đổi `code/lakehouse/spark_sync_province.py` (Silver 1 -> Silver cấp tỉnh 11 tỉnh x 25.512 giờ).
   - Chuyển đổi `code/lakehouse/spark_feature_engineering.py` (Silver -> Gold 49 đặc trưng).
   - Xây dựng script điều phối tổng hợp `code/lakehouse/build_lakehouse_spark.py`.
   - Viết các test tương ứng và lập checkpoint report cho Step 2 và Step 3.
3. **Giai đoạn 3 (Step 4 & Step 5): Tích hợp Mô hình, Đo kiểm Benchmark và Docker**
   - Cập nhật 3 file tiêu thụ dữ liệu `code/training/common.py`, `code/training/train_global_ann.py`, và `code/training/du_bao.py` để ưu tiên đọc Parquet.
   - Xây dựng `code/lakehouse/benchmark_spark_vs_pandas.py` đo 3 mức 1x, 10x, 50x và xuất ra `output/cloudsim_benchmark/spark_storage_benchmark.csv`.
   - Build image Docker `airquality-spark:3.5.1` và chạy toàn trình kiểm thử trong container.
   - Hoàn tất test suite và lập checkpoint report cho Step 4 và Step 5.

### 7.2. Tuyên bố hoàn tất khảo sát
Khảo sát kỹ thuật cho Step 1, Step 5, Step 6 đã hoàn thành đầy đủ với các dữ kiện thực nghiệm chính xác, các rủi ro cốt lõi đã được cô lập và có phương án giải quyết cụ thể. Sẵn sàng bàn giao cho Orchestrator và các Agent chuyên trách thực thi.
