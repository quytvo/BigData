# 🇻🇳 Distributed Spatio-Temporal Air Quality Forecasting Lakehouse on Google Cloud Storage: Coupling National In-Situ Monitoring (CEM) with ERA5 Atmospheric Thermodynamics

> **Nền tảng Cloud-Native Lakehouse phân tán thu nạp, làm sạch và tích hợp dữ liệu quan trắc mặt đất (CEM) với dữ liệu tái phân tích khí quyển dạng lưới (ERA5) trên Google Cloud Storage (GCS), phục vụ huấn luyện song song các mô hình Machine Learning dự báo nồng độ bụi mịn ($PM_{2.5}$) 24–48 giờ cấp trạm/tỉnh thành tại Việt Nam.**

---

## 1. Bối cảnh & Vấn đề Cốt lõi (Problem Statement)

Ô nhiễm bụi mịn ($PM_{2.5}$) tại các trung tâm kinh tế – công nghiệp của Việt Nam (Hà Nội, Bắc Ninh, Quảng Ninh, TP.HCM...) diễn biến phức tạp, chịu sự chi phối chặt chẽ của cả hoạt động phát thải nhân sinh lẫn các hiện tượng vi khí hậu cực đoan (nghịch nhiệt mùa đông, gió mùa, trần khí quyển thấp).

Tuy nhiên, việc xây dựng hệ thống giám sát và dự báo chất lượng không khí hiện nay gặp phải **3 rào cản kỹ thuật lớn**:
1. **Thiếu tính dự báo chủ động (Reactive, Not Predictive):** Các hệ thống hiện tại chỉ dừng ở mức hiển thị số liệu tức thời (quá khứ/hiện tại), thiếu khả năng cảnh báo sớm nồng độ $PM_{2.5}$ trước 24–48 giờ để phục vụ điều hành và bảo vệ sức khỏe cộng đồng.
2. **Nút thắt về bối cảnh vật lý (Physical Blindness):** Dữ liệu từ các trạm quan trắc mặt đất (CEM) chỉ cung cấp nồng độ bụi tại điểm đo, hoàn toàn không đo được các thông số nhiệt động lực học thẳng đứng (độ cao tầng biên khí quyển $PBLH$, nghịch nhiệt bức xạ, vector gió 3 chiều). Điều này khiến các mô hình học máy truyền thống chỉ "đoán mò" dựa trên chuỗi số quá khứ mà không hiểu bản chất động lực học không khí.
3. **Thách thức Big Data & Dị thể Không gian ("Mỗi nơi một tính nết"):** Mỗi địa phương chịu một cơ chế phát thải và vi khí hậu riêng biệt. Việc ép một mô hình chung sẽ làm sai lệch dự báo; trong khi việc huấn luyện riêng biệt hàng trăm mô hình chuỗi thời gian kết hợp dữ liệu khí quyển đa chiều lại đòi hỏi hạ tầng tính toán phân tán và lưu trữ đối tượng (Object Storage) có khả năng co giãn linh hoạt (Elastic Scale-out).

---

## 2. Mục tiêu Dự án (Project Objectives)

* **Xây dựng Cloud-Native Lakehouse chuẩn Medallion trên Google Cloud Storage (GCS):** Thiết lập quy trình thu nạp tự động, chuẩn hóa dữ liệu qua các tầng Bronze – Silver – Gold lưu trữ trực tiếp trên GCS (`gs://`), bảo đảm tính toàn vẹn (ACID transactions) và kiểm soát phiên bản (Time Travel) bằng Delta Lake.
* **Tích hợp Đa nguồn Không gian - Thời gian (Spatio-Temporal Data Fusion):** Sử dụng Apache Spark và Apache Sedona để khớp nối song song chuỗi đo thực tế mặt đất (Ground Truth $Y$ từ CEM/TEDP) với dữ liệu thời tiết theo giờ tại **đúng toạ độ từng trạm** (Features $X$ từ Open-Meteo). Hai nguồn được ghép theo khoá `station_code` + `datetime`.
* **Kiểm toán & Chuẩn hóa Chất lượng Dữ liệu Môi trường (Veracity Control):** Khắc phục lỗi metadata hành chính bằng `Point-in-Polygon Spatial Join` với bản đồ ranh giới tỉnh thành GeoJSON; tự động phát hiện trôi cảm biến (sensor drift), kiểm toán tính nhất quán của số liệu đo đạc và bù đắp dữ liệu thiếu hụt bằng phương pháp nội suy theo chuỗi thời gian của từng trạm.
* **Mô hình hóa Dự báo Phân tán (Distributed Multi-Model Forecasting):** Tận dụng tính toán song song trên Spark (`applyInPandas` / Pandas UDF) để huấn luyện đồng thời hàng trăm mô hình học máy cục bộ (LightGBM), xuất ra bảng dự báo nồng độ $PM_{2.5}$ và chỉ số AQI cho 24–48 giờ tiếp theo.

---

## 3. Kiến trúc Hệ thống & Luồng Dữ liệu (System Architecture)

![Kiến trúc hệ thống](image/overview.png)

### Công nghệ sử dụng:
* **Lưu trữ Đám mây (Cloud Object Storage):** Google Cloud Storage (GCS) thông qua connector `gcs-connector-hadoop`.
* **Hạ tầng Tính toán Phân tán:** Apache Spark (PySpark), Apache Sedona (Spatial Computing).
* **Quản trị Lưu trữ Lakehouse:** Delta Lake (ACID Transactions, Time Travel, Z-Order Optimization).
* **Mô hình Dự báo & Đánh giá:** LightGBM, Scikit-learn (phân tán qua Spark `applyInPandas`).
* **Nguồn dữ liệu:**
  * **Cổng Quan trắc Môi trường Quốc gia (CEM / TEDP):** Dữ liệu quan trắc mặt đất (Ground Truth $Y$).
  * **Cổng Quan trắc Môi trường Quốc gia (CEM / TEDP):** Dữ liệu quan trắc mặt đất theo giờ (Ground Truth $Y$). Giai đoạn hiện tại: đã đồng bộ về 30 ngày × 24 giờ (xem Mục 9).
  * **Open-Meteo (Historical Forecast API, thư viện `openmeteo-requests`):** Dữ liệu thời tiết theo giờ tại toạ độ từng trạm (Feature Matrix $X$): nhiệt độ, độ ẩm tương đối, áp suất bề mặt, lượng mưa, tốc độ gió. Có thể chuyển sang ERA5 (`archive-api`) khi cần dữ liệu tái phân tích chính thức.
  * **Bản đồ Ranh giới Hành chính Việt Nam:** File GeoJSON 63 tỉnh/thành.

---

## 4. Cấu hình Kỹ thuật Spark kết nối GCS & Delta Lake

Để Spark làm việc trực tiếp với GCS bucket thay cho HDFS hay MinIO, hệ thống cấu hình `GCS Connector` với cơ chế xác thực Service Account:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CEM_ERA5_GCS_Lakehouse") \
    .config("spark.jars.packages", 
            "io.delta:delta-spark_2.12:3.1.0,"
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.14,"
            "org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.5.1") \
    .config("spark.sql.extensions", 
            "io.delta.sql.DeltaSparkSessionExtension,"
            "org.apache.sedona.sql.SedonaSqlExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/path/to/gcp-sa-key.json") \
    .getOrCreate()
```

---

## 5. Bốn Biến số Nhiệt động lực học Cốt lõi (Thermodynamic Core)

Hệ thống tập trung giải quyết triệt để 4 cơ chế vật lý tác động trực tiếp đến sự tích tụ bụi mịn:

> **Lưu ý phạm vi hiện tại:** Pipeline Open-Meteo mới thu 5 biến cơ bản (`temperature_2m`, `relative_humidity_2m`, `surface_pressure`, `precipitation`, `wind_speed_10m`). Các biến nâng cao dưới đây (vector gió $u_{10}, v_{10}$, nhiệt độ bề mặt $T_{skin}$, độ cao tầng biên $PBLH$, điểm sương $T_d$) **chưa được thu thập**, sẽ bổ sung ở giai đoạn sau nếu cần.

* **Động lực học Gió (Wind Vector Dynamics):** Phân rã vector gió ($u_{10}, v_{10}$) thành hướng lượng giác ($\sin/\cos$) và vận tốc tuyệt đối ($WS = \sqrt{u^2 + v^2}$) để mô hình hóa hiện tượng khuếch tán ngang.
* **Cơ chế Nghịch nhiệt Bức xạ (Temperature Inversion):** Tính toán độ chênh nhiệt độ $\Delta T_{inv} = T_{2\text{m}} - T_{\text{skin}}$. Khi $\Delta T_{inv} > 0$, kích hoạt cờ nghịch nhiệt (khí quyển ổn định, khói bụi bị giữ sát mặt đất).
* **Dung tích Thông gió Khí quyển (Ventilation Coefficient - $V_c$):** Tích số giữa tốc độ gió và độ cao tầng biên khí quyển ($V_c = WS \times PBLH$), định lượng khả năng tự pha loãng ô nhiễm của môi trường.
* **Độ ẩm & Rửa trôi Ướt (Moisture & Precipitation Scavenging):** Lượng mưa ($Precipitation$) cuốn trôi bụi xuống đất và độ hụt điểm sương ($DPD = T - T_d$) thể hiện mức độ bão hòa hơi nước.

---

## 6. Ánh xạ Tiêu chí 5V của Big Data

| Chữ "V" | Hiện thực hóa trong dự án |
| :--- | :--- |
| **Volume** | Tích hợp chuỗi thời gian liên tục từ các trạm CEM/TEDP với dữ liệu thời tiết Open-Meteo theo toạ độ trạm, lưu trữ tối ưu dưới dạng nén Snappy Parquet / Delta Lake trên Google Cloud Storage. Giai đoạn hiện tại: 19 trạm × 720 giờ = 13.680 bản ghi/nguồn; thiết kế sẵn để mở rộng nhiều năm và nhiều trạm hơn. |
| **Velocity** | Thiết kế luồng Ingestion tự động nạp định kỳ số liệu theo giờ từ cổng CEM lên GCS Bronze layer, sẵn sàng mở rộng luồng stream tính toán cảnh báo thời gian thực. |
| **Variety** | Kết hợp dữ liệu chuỗi thời gian trạm đo dạng bảng (Tabular Time-series), dữ liệu lưới khí tượng nhiều chiều (Multidimensional Grid) và dữ liệu hình học ranh giới hành chính (GeoJSON Polygons). |
| **Veracity** | Xây dựng pipeline kiểm toán tự động: sửa lỗi trạm Unknown qua tọa độ thực tế, phát hiện trôi cảm biến (sensor drift), gắn cờ kiểm tra tỷ lệ bất thường và bù đắp dữ liệu đứt gãy bằng nội suy chuỗi thời gian. |
| **Value** | Xây dựng hệ thống cảnh báo sớm ô nhiễm trước 24–48 giờ cho từng địa phương, cung cấp dữ liệu phục vụ nghiên cứu và hỗ trợ các cơ quan quản lý môi trường. |

---

## 7. Sản phẩm Đầu ra của Dự án (Deliverables)

1. **Kiểm toán Chất lượng Dữ liệu Môi trường (Data Audit Report):** Báo cáo đánh giá tỷ lệ sai lệch metadata, tỷ lệ trôi cảm biến và phát hiện dị thường trên mạng lưới quan trắc CEM.
2. **Feature Store Tầng Gold (`gs://<bucket>/lakehouse/gold/thermo_feature_store/`):** Bộ dữ liệu tích hợp hoàn chỉnh giữa trạm đo mặt đất và các biến số vi khí hậu, tối ưu hóa layout vật lý bằng Z-Order (`station_id, datetime`).
3. **Bảng Dự báo Chuỗi thời gian (`gs://<bucket>/lakehouse/gold/air_quality_forecast/`):** Bảng kết quả dự báo chi tiết nồng độ $PM_{2.5}$ và cấp độ AQI theo từng mốc thời gian trong tương lai cho từng trạm/tỉnh thành.
4. **Báo cáo Hiệu năng Phân tán (`gs://<bucket>/lakehouse/gold/model_evaluation_metrics/`):** Bảng so sánh các chỉ số sai số ($RMSE, MAE, R^2$) và thời gian huấn luyện song song giữa các trạm trên cụm Spark kết nối GCS.

---

## 8. Lưu ý Kỹ thuật khi chạy Spark với GCS

* **Tối ưu hóa ghi file Delta trên GCS:** Khi ghi file Delta Lake trực tiếp lên GCS, cần lưu ý cấu hình `spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore` (hoặc sử dụng Delta Lake 3.x với cơ chế `Coordinated Commits`) để tránh vấn đề ghi đè tệp nhật ký giao dịch đồng thời (transaction log consistency).
* **Truy vấn ngoại vi bằng BigQuery (BigQuery External Table):** Tầng Gold lưu trên GCS có thể được khai thác trực tiếp từ BigQuery qua BigLake / External Table mà không cần sao chép dữ liệu, phục vụ kết nối trực tiếp với Looker Studio để dựng Dashboard trực quan hóa.

---

## 9. Phạm vi Dữ liệu & Pipeline Thu nạp Hiện tại

### 9.1. Phạm vi dữ liệu đã chốt

| Hạng mục | Giá trị |
| :--- | :--- |
| **Cửa sổ thời gian** | 30 ngày: `2026-09-01 00:00` → `2026-09-30 23:00` |
| **Độ phân giải** | Theo **giờ** (720 giờ/trạm) |
| **Số trạm / province** | **19 trạm / 12 province** (từ 26 trạm / 14 province ban đầu) |
| **Tổng bản ghi** | 13.680 dòng mỗi nguồn (TEDP và thời tiết), khớp 100% theo `station_code` + `datetime` |

**12 province còn lại:** Bắc Ninh, Cần Thơ, Gia Lai, HCM, Hà Nội, Hưng Yên, Hải Phòng, Lâm Đồng, Ninh Bình, Quảng Ngãi, Tây Ninh, Đà Nẵng.

### 9.2. Quy tắc đồng bộ thời gian

1. **Cắt phần thừa:** bỏ các bản ghi ngoài cửa sổ 30 ngày (các ngày 01–03/10/2026).
2. **Loại province thiếu ngày:** nếu có trạm không đủ 30 ngày dữ liệu thì xoá **toàn bộ province** đó. Đã loại **HCM KV3** (3 trạm chỉ có 29/30 ngày) và **Quảng Ninh** (2 trạm chỉ có 20–21/30 ngày).
3. **Bù giờ lẻ:** các giờ thiếu rải rác (tổng 26 giờ ở 7 trạm) được chèn thêm và nội suy tuyến tính theo thời gian trong từng trạm; gắn cờ `*_imputed = 1` và cột `is_inserted = 1`. Mọi trạm cuối cùng có cùng tập timestamp.

### 9.3. Các script và file đầu ra

| Bước | Script | Đầu vào | Đầu ra |
| :--- | :--- | :--- | :--- |
| Tiền xử lý TEDP | `preprocess_tedp.py` | `tedp_aqi_pm25_hourly.csv` | `cleaned_tedp.csv` |
| Đồng bộ 30 ngày | `code/sync_tedp_30days.py` | `cleaned_tedp.csv` | `cleaned_tedp_synced_30d.csv` |
| Tải thời tiết theo trạm | `code/fetch_openmeteo_weather.py` | `cleaned_tedp_synced_30d.csv` | `weather_openmeteo_station_30d.csv` |

**Biến thời tiết (Open-Meteo, theo giờ, múi giờ `Asia/Bangkok`):**

| Biến | Tên Open-Meteo | Đơn vị |
| :--- | :--- | :--- |
| Nhiệt độ | `temperature_2m` | °C |
| Độ ẩm tương đối | `relative_humidity_2m` | % |
| Áp suất khí quyển | `surface_pressure` | hPa |
| Lượng mưa | `precipitation` | mm |
| Tốc độ gió | `wind_speed_10m` | m/s |

> **Hạn chế đã biết:** 19 trạm có thể rơi vào ít ô lưới hơn của Open-Meteo, nên các trạm rất gần nhau dùng chung một bộ số liệu thời tiết. Với cửa sổ chỉ 30 ngày, các mô hình dự báo 24–48 giờ chỉ mang tính thử nghiệm quy trình; cần mở rộng dữ liệu theo thời gian để đánh giá có ý nghĩa thống kê.