# PROJECT CONTEXT — Big Data in Cloud Computing

> **File này là SOURCE OF TRUTH của toàn bộ dự án.**
> Cập nhật mỗi khi có quyết định mới được khóa.
> Last updated: 2026-10-05 (Cập nhật chốt bộ dữ liệu CEM/TEDP + Open-Meteo đồng bộ 30 ngày)

---

## 1. THÔNG TIN CƠ BẢN

| Field | Value |
|---|---|
| **Môn học** | Big Data in Cloud Computing |
| **Trọng số điểm** | 30% tổng điểm môn học |
| **Quy mô nhóm** | 4 sinh viên |
| **Current Week** | **Week 2** ✅ |
| **Deployment** | **Google Cloud Platform (GCP)** ✅ |
| **Ngôn ngữ báo cáo** | English |
| **Ngôn ngữ thuyết trình** | English |

---

## 2. TEAM MEMBERS & ROLES [LOCKED]

| Member | Tên | Official Role | Owns | Deliverable chính |
|---|---|---|---|---|
| Member 1 | **Quý** | Data Engineer | Ingestion + Storage | OpenAQ crawler → GCS → Parquet |
| Member 2 | **Bảo** | Data Processor | Processing + Analytics | Spark jobs → BigQuery + SQL analytics |
| Member 3 | **Hiếu** | QA + Visualization | Validation + Dashboard | QA reports + Looker Studio + Deployment notes |
| Member 4 | **Dương** | Docs + PM | Report + Slides + Cost | Technical Report (EN) + Slide deck + Cost tracking |

> 💡 **Lý do không giữ nguyên role gốc:**
> - "Bảo: code" quá vague → redefined thành **Data Processor** với deliverable cụ thể
> - "Hiếu: review" không phải deliverable → redefined thành **QA + owns dashboard**
> - Mỗi người phải **own** một end-to-end layer, không chỉ một công nghệ

---

## 3. TIMELINE [LOCKED]

| Milestone | Week | Mô tả | Status | Còn bao lâu |
|---|---|---|---|---|
| Team Formation | S1 | Thành lập nhóm | ✅ Done | — |
| **NOW** | **S2** | Setup + Ingestion bắt đầu | 🔄 In Progress | — |
| Project Proposal | S7 | Nộp đề xuất | ⏳ | ~5 tuần |
| Project Check-in | S10 | Kiểm tra tiến độ | ⏳ | ~8 tuần |
| Final Presentation | S11 | Báo cáo & thuyết trình (15 phút) | ⏳ | ~9 tuần |

---

## 4. ĐỀ TÀI & PROBLEM STATEMENT [LOCKED]

### 4.1. Chủ đề chính

**Dự báo Chất lượng Không khí Phân tán tại Việt Nam: Tích hợp Quan trắc Mặt đất (CEM / TEDP) với Dữ liệu Thời tiết Vi khí hậu (Open-Meteo)**

Xây dựng nền tảng Cloud-Native Lakehouse (GCS + Spark + Delta Lake) thu nạp, làm sạch và tích hợp dữ liệu chuỗi thời gian nồng độ bụi mịn ($PM_{2.5}$) từ mạng lưới trạm quan trắc mặt đất quốc gia với dữ liệu khí tượng vi mô dạng lưới theo giờ tại đúng vị trí trạm, phục vụ huấn luyện song song các mô hình Machine Learning dự báo $PM_{2.5}$ và AQI cấp trạm.

### 4.2. Mục tiêu phân tích & kỹ thuật

- Khớp nối không gian - thời gian (Spatio-Temporal Data Fusion) giữa chuỗi đo $PM_{2.5}$, AQI với 5 biến số thời tiết (nhiệt độ, độ ẩm, áp suất khí quyển, lượng mưa, tốc độ gió) theo từng giờ.
- Kiểm toán dữ liệu (Data Auditing): phát hiện thiếu hụt, trôi cảm biến, tự động đồng bộ hóa chuỗi thời gian (time-grid synchronization).
- Huấn luyện phân tán mô hình học máy cục bộ (LightGBM) trên Apache Spark (`applyInPandas`).
- Dự báo nồng độ bụi mịn và cấp độ AQI cho 24–48 giờ tiếp theo.

### 4.3. Phạm vi địa lý & Trạm quan trắc

- **Quốc gia:** 🇻🇳 Việt Nam
- **Quy mô:** **19 trạm quan trắc** thuộc **12 tỉnh/thành phố** (đã qua bước lọc chất lượng và đồng bộ 30 ngày):
  Bắc Ninh, Cần Thơ, Gia Lai, TP.HCM, Hà Nội, Hưng Yên, Hải Phòng, Lâm Đồng, Ninh Bình, Quảng Ngãi, Tây Ninh, Đà Nẵng.

---

## 5. DATASET & TIỀN XỬ LÝ [LOCKED]

### 5.1. Hai nguồn dữ liệu chính

| Nguồn | Vai trò | Tần suất & Cửa sổ | File dữ liệu đã chốt |
|---|---|---|---|
| **Cổng Môi trường Quốc gia (CEM / TEDP)** | Ground Truth $Y$: $PM_{2.5}$, AQI, $PM_{10}$, $NO_2$, $SO_2$, $O_3$, $CO$ | Theo giờ (hourly), 30 ngày (01/09/2026 – 30/09/2026) | `cleaned_tedp_synced_30d.csv` (13.680 dòng) |
| **Open-Meteo Historical Forecast API** | Feature Matrix $X$: nhiệt độ, độ ẩm, áp suất, mưa, gió tại toạ độ trạm | Theo giờ (hourly), 30 ngày (01/09/2026 – 30/09/2026) | `weather_openmeteo_station_30d.csv` (13.680 dòng) |

### 5.2. Các biến thời tiết vi khí hậu thu nạp từ Open-Meteo

| Biến | Tên trường Open-Meteo | Đơn vị | Ý nghĩa vật lý đối với ô nhiễm |
|---|---|---|---|
| Nhiệt độ 2m | `temperature_2m` | °C | Ảnh hưởng nghịch nhiệt và xáo trộn đối lưu |
| Độ ẩm tương đối | `relative_humidity_2m` | % | Tác động phản ứng ngưng tụ hạt keo aerosol |
| Áp suất bề mặt | `surface_pressure` | hPa | Điều kiện khí áp cao/thấp quyết định đối lưu |
| Lượng mưa | `precipitation` | mm | Cơ chế rửa trôi ướt bụi mịn |
| Tốc độ gió 10m | `wind_speed_10m` | m/s | Cơ chế khuếch tán và pha loãng chất ô nhiễm |

### 5.3. Quy tắc đồng bộ dữ liệu 30 ngày (Data Synchronization)

1. **Cửa sổ thời gian cố định:** Đúng 30 ngày từ `2026-09-01 00:00:00` đến `2026-09-30 23:00:00` = 720 giờ. Các mốc ngoài cửa sổ bị cắt bỏ.
2. **Quy tắc xóa tỉnh thiếu ngày (Strict Province Drop):** Nếu bất kỳ trạm nào trong tỉnh thiếu ngày quan trắc trong tháng → loại bỏ toàn bộ tỉnh đó.
   - Loại **HCM KV3** (cả 3 trạm chỉ có 29/30 ngày).
   - Loại **Quảng Ninh** (2 trạm chỉ có 20–21 ngày do bắt đầu muộn từ 10–11/09).
3. **Bù lấp giờ thiếu (Linear Temporal Interpolation):** 7 trạm còn lại có tổng cộng 26 giờ lẻ bị khuyết được bổ sung vào lưới giờ và nội suy tuyến tính, đánh dấu rõ ràng với `is_inserted = 1` và cờ `*_imputed = 1`.
4. **Đồng bộ thời tiết:** Dữ liệu Open-Meteo được lấy đúng theo tọa độ (`latitude`, `longitude`) của từng trạm cho cùng khung 720 giờ. Khớp 100% khi merge theo `station_code` + `datetime`.

---

## 6. DEPLOYMENT & ARCHITECTURE [LOCKED]

### 6.1. Deployment Target: **GCP**

### 6.2. Architecture Overview

```
Nguồn TEDP (Mặt đất)            Nguồn Open-Meteo (Thời tiết)
  (cleaned_tedp_synced_30d.csv)   (weather_openmeteo_station_30d.csv)
                │                               │
                └───────────────┬───────────────┘
                                ▼
        Ingestion Layer: Google Cloud Storage (GCS)
             Bronze: gs://<bucket>/lakehouse/bronze/
                                │
                                ▼
        Cloud Dataproc (Apache Spark + Sedona + Delta Lake)
             Silver: Tích hợp Spatio-Temporal Join
                     Key: (station_code, datetime)
                                │
                                ▼
        Gold Layer: Feature Store & Forecast Models (Spark applyInPandas)
             Gold: gs://<bucket>/lakehouse/gold/
             - thermo_feature_store
             - air_quality_forecast (24-48h predictions)
             - model_metrics
                                │
                                ▼
        Serving & Analytics: BigQuery External Tables / Looker Studio
```

### 6.3. GCP Services sử dụng

| Service | Mục đích | Owner | Ghi chú |
|---|---|---|---|
| Cloud Storage (GCS) | Data Lake (raw + parquet) | Quý | Set lifecycle policy |
| Cloud Dataproc | Managed Spark cluster | Bảo | **TẮT sau mỗi job** |
| BigQuery | Data Warehouse, analytics | Bảo | Free tier 1TB/month |
| Looker Studio | Dashboard | Hiếu | Free, native BigQuery |
| IAM | Access control | Quý | Service account per role |

### 6.4. Không dùng (lý do)

| Technology | Lý do loại |
|---|---|
| Kafka / Pub/Sub | Data OpenAQ là batch, không cần streaming |
| Kubernetes | Overengineering cho đồ án sinh viên |
| Hadoop HDFS | GCS thay thế tốt hơn, không cần manage |
| CloudSim | Đã chọn GCP thực tế |

---

## 7. DATA CONTRACT [Quý → Bảo interface]

### Data Contract: TEDP Ground Truth (`cleaned_tedp_synced_30d.csv`)

```
Schema:
- province_id         STRING
- province_name       STRING    (12 tỉnh/thành)
- station_id          STRING
- station_code        STRING    (19 mã trạm)
- station_name        STRING
- latitude            DOUBLE
- longitude           DOUBLE
- datetime            TIMESTAMP (Định dạng YYYY-MM-DD HH:MM:SS, UTC+7)
- pm25, aqi           DOUBLE    (Nồng độ bụi mịn PM2.5 và chỉ số AQI)
- pm10, no2, so2, o3, co DOUBLE (Các thông số ô nhiễm phụ)
- *_imputed           INT       (Cờ đánh dấu giá trị nội suy: 0 hoặc 1)
- is_inserted         INT       (1: dòng được chèn bù giờ thiếu, 0: dữ liệu gốc)
- date, hour, day_name, is_weekend, time_slot, aqi_category STRING/INT
```

### Data Contract: Open-Meteo Weather Features (`weather_openmeteo_station_30d.csv`)

```
Schema:
- station_id          STRING
- station_code        STRING    (Khóa khớp nối với TEDP)
- station_name        STRING
- province_name       STRING
- latitude, longitude DOUBLE    (Tọa độ trạm)
- grid_latitude, grid_longitude DOUBLE (Tọa độ ô lưới khí tượng Open-Meteo)
- elevation           DOUBLE    (Độ cao trạm so với mực nước biển)
- datetime            TIMESTAMP (Khóa khớp nối với TEDP)
- temperature_2m      DOUBLE    (°C)
- relative_humidity_2m DOUBLE   (%)
- surface_pressure    DOUBLE    (hPa)
- precipitation       DOUBLE    (mm)
- wind_speed_10m      DOUBLE    (m/s)
```

---

## 8. SCORING TARGETS

| Tiêu chí | Weight | Chiến lược |
|---|---|---|
| Technical Solution & Results | **40%** | Pipeline end-to-end + kết quả phân tích có ý nghĩa |
| Architecture & Cost Justification | **20%** | GCP architecture rõ ràng, cost breakdown, trade-offs |
| Technical Report (English) | **20%** | 8–12 trang, đầy đủ 9 sections yêu cầu |
| Presentation & Teamwork | **20%** | Cả 4 người trình bày, Q&A tốt |

---

## 9. DELIVERABLES CHECKLIST

- [ ] Working source code (ingestion + Spark jobs)
- [ ] Deployment notes (step-by-step reproducible trên GCP)
- [ ] Technical Report (English, 8–12 pages)
- [ ] Final Presentation (15 phút, English)
- [ ] GCP Cost Analysis (actual cost breakdown)
- [ ] Architecture Analysis + Trade-offs
- [ ] Citations & References
- [ ] AI-use Statement

---

## 10. OPEN QUESTIONS — CÒN MỞ

| # | Câu hỏi | Priority | Ai hỏi |
|---|---|---|---|
| Q1 | Budget GCP: có GCP credits (student/research) không? | 🔴 P0 | Dương hỏi thầy |
| Q2 | Proposal nộp theo format gì? | 🟡 P1 | Dương hỏi thầy |
| Q3 | Thu thập dữ liệu bao nhiêu năm (2020–2025 hay ngắn hơn)? | 🟡 P1 | Nhóm quyết định |
| Q4 | ML component có bắt buộc không? | 🟠 P2 | Hỏi thầy |

---

## 11. DECISIONS LOG

| Date | Quyết định | Status |
|---|---|---|
| 2026-10-01 | Deployment target: **GCP** (GCS, Dataproc, BigQuery, Looker Studio) | ✅ LOCKED |
| 2026-10-01 | Kiến trúc: Medallion Lakehouse (Delta Lake trên GCS) | ✅ LOCKED |
| 2026-10-01 | Không dùng Kafka, Kubernetes, HDFS cục bộ | ✅ LOCKED |
| 2026-10-05 | Chuyển đổi trọng tâm dữ liệu sang **CEM / TEDP (Việt Nam) + Open-Meteo vi khí hậu** | ✅ LOCKED |
| 2026-10-05 | Chuẩn hóa cửa sổ 30 ngày (`2026-09-01` → `2026-09-30`), độ phân giải 1 giờ (720h/trạm) | ✅ LOCKED |
| 2026-10-05 | Loại bỏ 2 tỉnh không đủ 30 ngày: HCM KV3 (29 ngày) và Quảng Ninh (20-21 ngày) | ✅ LOCKED |
| 2026-10-05 | Nội suy 26 giờ lẻ ở 7 trạm, gắn cờ `is_inserted=1` và `*_imputed=1` | ✅ LOCKED |
| 2026-10-05 | Thu thập thời tiết theo đúng tọa độ từng trạm (19 trạm, 5 biến cốt lõi) qua `openmeteo-requests` | ✅ LOCKED |

---

## 12. NEXT STEPS — GIAI ĐOẠN HIỆN TẠI

**Quý (Data Engineer):**
1. Đẩy 2 tệp dữ liệu đã đồng bộ (`cleaned_tedp_synced_30d.csv` và `weather_openmeteo_station_30d.csv`) lên GCS Bronze bucket: `gs://<bucket>/lakehouse/bronze/`.
2. Viết pipeline PySpark để nạp Bronze và ghi vào Silver dưới dạng Delta Lake table với Partition theo `province_name`.

**Bảo (Data Processor):**
1. Viết Spark Job thực hiện Spatio-Temporal Join giữa bảng TEDP và Weather theo cặp khóa `(station_code, datetime)`.
2. Tạo các trường đặc trưng nâng cao (lag features: $PM_{2.5}$ các giờ trước $t-1, t-2, t-24$; rolling mean 6h/24h).
3. Thiết lập khung huấn luyện phân tán LightGBM qua `applyInPandas` dự báo $PM_{2.5}$ cho 24h tới.

**Hiếu (QA + Visualization):**
1. Kiểm tra tính toàn vẹn (Data Integrity): xác nhận không có null sau khi join giữa TEDP và Weather.
2. Dựng bảng dữ liệu Gold trên BigQuery (External Table qua BigLake) và thiết lập kết nối Looker Studio.

**Dương (Docs + PM):**
1. Cập nhật Proposal và Technical Report theo phạm vi dữ liệu mới (CEM/TEDP 19 trạm + Open-Meteo).
2. Theo dõi hạn mức chi phí trên GCP Billing.

> 📄 Xem chi tiết WBS đầy đủ tại: **master_plan.md**
