# Dự báo Chất lượng Không khí Việt Nam — PySpark Medallion Lakehouse + Spatio-Temporal Global ANN + CloudSim

> Đồ án môn **Big Data in Cloud Computing**

Hệ thống dữ liệu lớn đầu-cuối (End-to-End Big Data Pipeline): thu thập dữ liệu quan trắc không khí thực tế từ **CEM/TEDP** gần 3 năm, đồng bộ với **8 biến khí tượng Open-Meteo ERA5** tại tọa độ trạm, xử lý phân tán bằng **Apache Spark 3.5.1 (100% Pure Catalyst Window Functions)** trên kiến trúc **Medallion Lakehouse (Bronze - Silver - Gold)** lưu trữ định dạng cột **Parquet nén Snappy** phân vùng theo tỉnh, huấn luyện mạng nơ-ron toàn cục **Spatio-Temporal Global ANN (With Spatial Attention)** dự báo chỉ số bụi mịn PM2.5 (`I_PM2.5`) trước **24 giờ** cho **11 tỉnh/thành phố** (đạt mức cải thiện **+16,50%** so với Baseline, chiến thắng trên cả 11/11 tỉnh), phục vụ luồng dự báo thời gian thực và mô phỏng hiệu năng đa quy mô (`1x`, `10x`, `50x`) kết hợp **CloudSim**.

---

## 📚 Danh mục Tài liệu Kỹ thuật (`docs/`)

Toàn bộ tài liệu thiết kế, đặc tả kỹ thuật và báo cáo nghiệm thu của dự án được lưu trữ trong thư mục `docs/`:

| Tài liệu | Nội dung chính |
| :--- | :--- |
| [docs/project_overview.md](docs/project_overview.md) | Tổng quan kiến trúc hệ thống, phân tích 5 chữ V của Big Data, Data Contract, cơ chế Spatial Attention và đánh đổi công nghệ |
| [docs/preprocessing_data_normalization_plan.md](docs/preprocessing_data_normalization_plan.md) | Quy chuẩn kiểm toán dữ liệu 8 bước, cơ chế Auto-align 3 năm, lọc độ phủ 17 trạm / 11 tỉnh, và schema đặc trưng 49 biến |
| [docs/pipeline_detail.md](docs/pipeline_detail.md) | Đặc tả chi tiết kỹ thuật luồng luân chuyển dữ liệu End-to-End từ Ingestion, Preprocessing, Weather Syncing đến Inference Serving |
| [docs/pandas_to_pyspark_migration_plan.md](docs/pandas_to_pyspark_migration_plan.md) | Bản đặc tả kiến trúc chuyển đổi từ Pandas sang PySpark 3.5.1 (Pure Catalyst Window, Medallion Parquet, Docker 8GB RAM & Benchmark) |
| [docs/overview.md](docs/overview.md) | Hướng dẫn tổng quan trực quan, dễ hiểu từ A đến Z về bức tranh tổng thể và vai trò của từng file code |
| [docs/task_breakdown_schedule.md](docs/task_breakdown_schedule.md) | Bảng phân rã công việc chi tiết cho 4 thành viên, sơ đồ đường găng Critical Path và tiến độ đến ngày 21/11/2026 |
| [docs/context.md](docs/context.md) | Đề bài gốc môn học, Rubric đánh giá, chính sách sử dụng AI và ghi nhận hiện trạng giải pháp của nhóm |
| [docs/plan.md](docs/plan.md) | Bản kế hoạch đánh giá và kiểm toán tính đồng bộ của toàn bộ hồ sơ tài liệu dự án |
| [docs/review_checkpoint_step1.md](docs/review_checkpoint_step1.md) | Báo cáo nghiệm thu Step 1: Hạ tầng Docker (`airquality-spark:3.5.1`) và cấu hình tối ưu `SparkSession` |
| [docs/review_checkpoint_step2.md](docs/review_checkpoint_step2.md) | Báo cáo nghiệm thu Step 2: Pipeline PySpark Bronze sang Silver (sai số số học so với Pandas `<= 8.53e-14`) |
| [docs/review_checkpoint_step3.md](docs/review_checkpoint_step3.md) | Báo cáo nghiệm thu Step 3: Trích xuất 49 đặc trưng khí quyển Silver sang Gold bằng Pure Catalyst (sai số `<= 1.31e-7`) |
| [docs/review_checkpoint_step4.md](docs/review_checkpoint_step4.md) | Báo cáo nghiệm thu Step 4: Tích hợp Parquet Partition Pruning vào Huấn luyện & Dự báo (tăng tốc I/O `15,57 lần`) |
| [docs/review_checkpoint_step5.md](docs/review_checkpoint_step5.md) | Báo cáo nghiệm thu Step 5: Benchmark đa quy mô `1x`, `10x`, `50x` trong Docker (nén `90,09%`, tăng tốc truy vấn `87,82 lần`) |

---

## 📊 Quy mô Dữ liệu & Kết quả Thực nghiệm Cốt lõi

| Hạng mục | Giá trị thực tế đã triển khai & kiểm chứng |
| :--- | :--- |
| **Quy mô thời gian** | Gần 3 năm liên tục (`11/12/2023 00:00:00` đến `07/11/2026 23:00:00`, **25.512 giờ liên tục/tỉnh**) |
| **Tầng Bronze (Raw CSV)** | **474.286 dòng** thô từ 28 trạm quan trắc trên 14 tỉnh thành (lưu trữ checkpoint theo tháng) |
| **Tầng Silver (Parquet Snappy)** | **440.012 dòng** sạch cấp trạm (`tedp_station/`) & **280.632 dòng** cấp tỉnh (`tedp_province/`, `weather_province/`) phân vùng theo **11 tỉnh đại diện** (sai số so với Pandas `<= 8.53e-14`) |
| **Tầng Gold (Parquet Snappy)** | **280.104 mẫu** (tập đầy đủ) / **279.840 mẫu** (tập đối chứng) x **49 đặc trưng vật lý khí quyển** (`global_ann_features/`, sai số so với Pandas `<= 1.31e-7`) |
| **Hiệu năng Lưu trữ & Truy vấn** | Nén Snappy giảm **90,09%** dung lượng đĩa (`4.339,56 MB` CSV xuống `430,24 MB` Parquet ở mức `50x`); ETL nhanh gấp **4,10 lần** ở mức `50x`; Partition Pruning tăng tốc truy vấn 1 tỉnh gấp **87,82 lần** (`51.363 ms` xuống `584,9 ms`) |
| **Mô hình Học máy** | **Spatio-Temporal Global ANN** (49 đặc trưng, `Test RMSE = 17,68` vs Baseline `21,17`, cải thiện **+16,50%**, thắng trên cả 11/11 tỉnh) |
| **Biến mục tiêu** | Chỉ số AQI thành phần của PM2.5 (`I_PM2.5`, thang điểm 0–500 theo QĐ 1459/QĐ-TCMT) dự báo ở chân trời `t + 24h` |
| **Kiểm thử Tự động (`pytest`)** | **34/34 test cases PASSED (100%)** bên trong Docker container `airquality-spark:3.5.1` (`8GB RAM`, `4 CPUs`) |

---

## 🚀 Quy trình Chạy Pipeline End-to-End

### 1. Chạy Pipeline PySpark Medallion Lakehouse & Benchmark trong Docker (Khuyến nghị)

```powershell
# 1. Build Docker Image Apache Spark 3.5.1 (chỉ cần chạy lần đầu)
docker build -f code/lakehouse/Dockerfile.spark -t airquality-spark:3.5.1 .

# 2. Chạy toàn trình Pipeline Lakehouse (Bronze -> Silver -> Gold) + Tự động đối chứng sai số với Pandas
docker run --rm -m 8g --cpus 4 -p 4040:4040 -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 python3 code/lakehouse/build_lakehouse_spark.py

# (Tuỳ chọn) Giữ Spark Web UI tại http://localhost:4040 trong 300 giây để quan sát và chụp ảnh DAG
docker run --rm -m 8g --cpus 4 -p 4040:4040 -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 python3 code/lakehouse/build_lakehouse_spark.py --keep-ui 300

# 3. Chạy Benchmark đa quy mô (1x, 10x, 50x) giữa Pandas CSV và Spark Parquet
docker run --rm -m 8g --cpus 4 -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 python3 code/lakehouse/benchmark_spark_vs_pandas.py

# 4. Chạy bộ 34 bài kiểm thử độc lập toàn trình (Steps 1 -> 5)
docker run --rm -m 8g --cpus 4 -v "${PWD}:/workspace" -w /workspace airquality-spark:3.5.1 python3 -m pytest tests/test_step1_docker_spark.py tests/test_step2_bronze_silver.py tests/test_step3_silver_gold.py tests/test_step4_model_integration.py tests/test_step5_benchmark_docker.py -v
```

### 2. Chạy Thu thập Dữ liệu, Huấn luyện, Dự báo 24h & Profiling trên Máy Local

```powershell
# 1. Kích hoạt môi trường ảo Python
.venv\Scripts\activate

# 2. Thu thập dữ liệu chất lượng không khí mới nhất từ CEM/TEDP (Bronze)
python code/crawl_data/test2.py

# 3. Cào dự báo thời tiết 24h tới của 17 trạm thành viên và gộp cấp tỉnh
python code/crawl_data/fetch_forecast_24h.py

# 4. Chạy suy luận dự báo PM2.5 24h tới cho từng tỉnh (tự động đọc Silver Parquet qua Partition Pruning, tăng tốc 15,57 lần; tự động fallback về CSV nếu thiếu Parquet)
python code/training/du_bao.py --province "Hà Nội"
python code/training/du_bao.py --province "HCM"
python code/training/du_bao.py --province "Đà Nẵng"

# 5. Huấn luyện mô hình Spatio-Temporal Global ANN (tự động đọc 49 đặc trưng từ Gold Parquet)
python code/training/train_global_ann.py

# 6. Chạy CloudSim sau khi đo workload và storage theo docs/cloudsim_benchmark.md
python code/simulation/run_cloudsim_benchmark.py
```

Lệnh cuối chạy CloudSim Plus Java 8.5.7 trong Docker, xuất dấu vết Cloudlet và
kết quả 9 kịch bản. Xem [hướng dẫn benchmark](docs/cloudsim_benchmark.md) để
biết cách chạy đo dữ liệu bằng Docker và các giả định mô phỏng.

---

## 🔗 Cấu trúc Thư mục Dự án

```text
Final_Project_Plan/
├── code/                                      # Toàn bộ mã nguồn hệ thống
│   ├── lakehouse/                             # Pipeline PySpark 3.5.1 Medallion Lakehouse & Benchmark
│   │   ├── Dockerfile.spark                   # Đóng gói môi trường Spark 3.5.1 + PyArrow + PyTorch CPU
│   │   ├── spark_session.py                   # Khởi tạo SparkSession (5GB Driver, AQE, Arrow, Event Logs, Static Schemas)
│   │   ├── spark_preprocess_tedp.py           # Bronze -> Silver cấp trạm (lọc QCVN, nội suy Window limit=3, trung vị trạm)
│   │   ├── spark_sync_province.py             # Silver trạm -> Silver cấp tỉnh (lưới 25.512h x 11 tỉnh, nội suy 2 chiều)
│   │   ├── spark_feature_engineering.py       # Silver -> Gold (trích xuất 49 đặc trưng khí quyển bằng Pure Catalyst Window)
│   │   ├── build_lakehouse_spark.py           # Điều phối liên hoàn Bronze -> Silver -> Gold + Data Diff tự động (< 1e-5)
│   │   └── benchmark_spark_vs_pandas.py       # Benchmark đa quy mô 1x, 10x, 50x (Storage, ETL Runtime, Partition Pruning)
│   │
│   ├── crawl_data/                            # Thu thập dữ liệu & Baseline Pandas đối chứng
│   │   ├── test2.py                           # Cào dữ liệu theo chunk tháng từ API TEDP
│   │   ├── preprocess_tedp.py                 # Bản đối chứng Pandas tiền xử lý cấp trạm
│   │   ├── sync_tedp_province.py              # Bản đối chứng Pandas gom cụm 17 trạm thành 11 tỉnh
│   │   ├── fetch_openmeteo_weather.py         # Lấy 8 biến khí tượng lịch sử ERA5 từ Open-Meteo
│   │   └── fetch_forecast_24h.py              # Cào dự báo khí tượng 24h tới cho 17 trạm thành viên
│   │
│   ├── training/                              # Huấn luyện mô hình và suy luận dự báo (tích hợp Parquet + CSV Fallback)
│   │   ├── common.py                          # Nạp dữ liệu thông minh (Parquet Partition Pruning + Zero-Downtime CSV Fallback)
│   │   ├── train_global_ann.py                # Huấn luyện mạng nơ-ron Spatio-Temporal Global ANN (đọc trực tiếp Gold Parquet)
│   │   └── du_bao.py                          # Pipeline suy luận dự báo 24h tới cho từng tỉnh (đọc Silver Parquet)
│   │
│   └── simulation/                            # Mô phỏng hạ tầng điện toán đám mây CloudSim
│       └── profile_workload.py                # Đo tải CPU, tính kích thước Input/Output và Cloudlet MI (11 Map + 1 Reduce)
│
├── lakehouse_storage/                         # Kho lưu trữ dữ liệu theo kiến trúc Medallion (Parquet nén Snappy)
│   ├── bronze/                                # Tầng dữ liệu thô ban đầu (Raw Storage)
│   │   ├── tedp_raw_monthly/                  # Checkpoint dữ liệu thô từng tháng
│   │   ├── tedp_aqi_pm25_hourly.csv           # Bảng thô tổng hợp 474.286 dòng từ 28 trạm
│   │   ├── cleaned_tedp.csv                   # Dữ liệu cấp trạm đối chứng (478.926 dòng)
│   │   ├── era5_cds_blh/                      # Dữ liệu vệ tinh NetCDF chiều cao tầng biên
│   │   └── tedp_stations.csv                  # Metadata danh mục trạm quan trắc
│   ├── silver/                                # Tầng dữ liệu sạch chuẩn hóa Parquet Snappy phân vùng theo province_name
│   │   ├── tedp_station/                      # Dữ liệu sạch cấp trạm (440.012 dòng, 11 phân vùng tỉnh)
│   │   ├── tedp_province/                     # Dữ liệu sạch cấp tỉnh (280.632 dòng = 11 tỉnh x 25.512 giờ)
│   │   └── weather_province/                  # Dữ liệu khí tượng ERA5 cấp tỉnh (280.632 dòng, 11 phân vùng tỉnh)
│   └── gold/                                  # Tầng đặc trưng phục vụ trực tiếp cho huấn luyện Global ANN
│       └── global_ann_features/               # Bảng 49 đặc trưng + nhãn mục tiêu y (Parquet phân vùng theo 11 tỉnh)
│
├── data_test/                                 # Tập dữ liệu CSV chuẩn hóa (Dùng làm đối chứng Ground-Truth & Fallback)
│   ├── cleaned_tedp_province_4y.csv           # Bảng sạch 11 tỉnh ~3 năm (280.632 dòng x 30 cột)
│   ├── weather_openmeteo_province_4y.csv      # 8 biến khí tượng tương ứng (280.632 dòng x 19 cột)
│   ├── province_station_members_4y.csv        # Danh sách 17 trạm thành viên thuộc 11 tỉnh
│   ├── province_coverage_4y.csv               # Báo cáo tỷ lệ độ phủ đo thật của các tỉnh
│   └── du_bao/                                # Dữ liệu dự báo thời gian thực 24h tới
│       ├── openmeteo_forecast_next_24h_11provinces.csv # Dự báo thời tiết 24h tới của 11 tỉnh
│       ├── ha_noi.csv                         # Kết quả dự báo PM2.5 chi tiết 24h tới của Hà Nội
│       └── hcm.csv                            # Kết quả dự báo PM2.5 chi tiết 24h tới của TP.HCM
│
├── output/                                    # Nơi xuất kết quả thực nghiệm, log Spark và báo cáo kiểm định
│   ├── global_ann_4y/                         # Mô hình Spatio-Temporal Global ANN tốt nhất
│   │   ├── best_ann_model.pt                  # Trọng số mô hình nơ-ron đã tối ưu (506 KB)
│   │   ├── metrics.json                       # Hồ sơ chi tiết siêu tham số và chỉ số đánh giá
│   │   ├── predictions.csv                    # Bảng kết quả dự báo trên tập Test (12,4 MB)
│   │   └── feature_importance_pct.csv         # Tỷ trọng đóng góp của 49 đặc trưng
│   ├── province_models_4y/                    # Kết quả đối sánh các mô hình cơ sở theo từng tỉnh
│   ├── cloudsim_benchmark/                    # Kết quả đo tải CloudSim và Benchmark đa quy mô Spark vs. Pandas
│   │   ├── workload_profile.csv               # Hồ sơ tải tính toán 11 tỉnh và Global ANN (Cloudlet MI)
│   │   └── spark_storage_benchmark.csv        # Bảng đo kiểm thực nghiệm 1x, 10x, 50x (Pandas CSV vs. Spark Parquet)
│   ├── spark_event_logs/                      # Nhật ký thực thi DAG/Stage của Apache Spark phục vụ History Server
│   └── lakehouse_verification_report.txt      # Báo cáo đối chứng sai số số học tự động giữa Spark và Pandas
│
├── tests/                                     # Bộ kiểm thử tự động độc lập toàn trình (Pytest)
│   ├── conftest.py                            # Cấu hình fixtures và hằng số kiểm định
│   ├── test_step1_docker_spark.py             # Kiểm thử Step 1: Docker & cấu hình SparkSession (9 tests)
│   ├── test_step2_bronze_silver.py            # Kiểm thử Step 2: Bronze -> Silver & sai số < 1e-5 (7 tests)
│   ├── test_step3_silver_gold.py              # Kiểm thử Step 3: Silver -> Gold 49 đặc trưng (7 tests)
│   ├── test_step4_model_integration.py        # Kiểm thử Step 4: Partition Pruning & CSV Fallback (5 tests)
│   └── test_step5_benchmark_docker.py         # Kiểm thử Step 5: Multi-Scale Benchmark & Event Logs (6 tests)
│
├── b_quy_t_c_chuy_n_i_pandas_sang_pyspark.md  # Bộ 7 quy tắc vàng chuyển đổi Pandas sang PySpark
└── docs/                                      # Tài liệu kỹ thuật và báo cáo nghiệm thu chi tiết (xem danh mục trên)
```

---

## 🔗 Nguồn dữ liệu tham chiếu

- **CEM / TEDP** — Cổng thông tin quan trắc môi trường quốc gia (Cục Kiểm soát ô nhiễm môi trường): https://tedp.vn
- **Open-Meteo** — Historical Weather & Forecast API: https://open-meteo.com
- **Copernicus Climate Data Store (CDS)** — ERA5 Hourly Boundary Layer Height: https://cds.climate.copernicus.eu
