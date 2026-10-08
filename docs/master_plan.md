# BIG DATA PROJECT — MASTER PLAN
**Distributed Spatio-Temporal Air Quality Forecasting Lakehouse on GCP (CEM/TEDP & Open-Meteo)**
> Version: 2.0 | Updated: 2026-10-05 | Current Week: **Week 2**
> Deployment: **Google Cloud Platform (GCP)**

---

## 1. EXECUTIVE SUMMARY

Nhóm 4 người xây dựng hệ thống Cloud-Native Lakehouse phân tán trên GCP (GCS, Dataproc Spark, Delta Lake, BigQuery) tích hợp chuỗi đo chất lượng không khí mặt đất tại Việt Nam (CEM / TEDP) với dữ liệu thời tiết vi mô dạng lưới (Open-Meteo) theo giờ tại toạ độ từng trạm. Hệ thống phục vụ kiểm toán dữ liệu và huấn luyện song song các mô hình học máy (LightGBM) dự báo $PM_{2.5}$ và AQI cho 24–48 giờ tiếp theo.

**Deliverables chính:**
- Pipeline nạp và đồng bộ đa nguồn (TEDP + Open-Meteo) → GCS Medallion Lakehouse (Bronze / Silver / Gold với Delta Lake)
- Spark jobs trên Dataproc: Spatio-Temporal Join, feature engineering (lags, rolling stats), distributed multi-model forecasting (`applyInPandas`)
- Kết quả kiểm toán dữ liệu & đánh giá mô hình ($RMSE, MAE, R^2$)
- Looker Studio Dashboard trực quan hóa chuỗi thời gian & dự báo
- Báo cáo kỹ thuật tiếng Anh (8–12 trang)
- Thuyết trình 15 phút

---

## 2. PROJECT CONTEXT [LOCKED]

| Field | Value |
|---|---|
| Môn học | Big Data in Cloud Computing |
| Trọng số | 30% tổng điểm |
| Deployment | **Google Cloud Platform (GCP)** |
| Current Week | **Week 2** |
| Proposal Deadline | **Week 7** (còn 5 tuần) |
| Check-in | **Week 10** (còn 8 tuần) |
| Final Presentation | **Week 11** (còn 9 tuần) |

---

## 3. TEAM ROLES [REDEFINED]

> ⚠️ **Lý do redefined:** "Quý: data" và "Bảo: code" quá vague và overlap.
> "Hiếu: review" không có deliverable cụ thể. Tôi redefine để mỗi người
> sở hữu một **end-to-end deliverable** thay vì chỉ một công nghệ.

| Member | Tên | Official Role | Owns | Primary Deliverable |
|---|---|---|---|---|
| Member 1 | **Quý** | Data Engineer | Ingestion + Storage layer | Pipelines thu nạp TEDP & Open-Meteo → GCS Bronze & Silver (Delta Lake) |
| Member 2 | **Bảo** | Data Processor | Processing + ML layer | Spark jobs (Spatio-Temporal Join, Feature Store, Distributed LightGBM) |
| Member 3 | **Hiếu** | QA + Integration | Testing + Dashboard + Deployment | Data validation, BigQuery Gold view, Looker Studio dashboard |
| Member 4 | **Dương** | Tech Writer + PM | Report + Presentation + Cost Analysis | Technical Report (EN) + Slide deck + Cost analysis |

> 💡 **Note:** Dương tracking tiến độ và chi phí GCP. Hiếu kiểm soát data contract, QA kết quả join và dựng Looker Studio.

---

## 4. SYSTEM ARCHITECTURE [GCP]

```
DATA SOURCES
    1. Cổng Quan trắc Môi trường (CEM / TEDP) -> Ground Truth Y (PM2.5, AQI)
    2. Open-Meteo Historical API -> Features X (Nhiệt độ, Độ ẩm, Khí áp, Mưa, Gió)
         │
         ▼
INGESTION & SYNCHRONIZATION [Quý]
    - sync_tedp_30days.py (Cửa sổ 30 ngày, 720h/trạm, nội suy giờ lẻ)
    - fetch_openmeteo_weather.py (Thu nạp thời tiết theo toạ độ 19 trạm)
         │
         ▼
BRONZE / SILVER STORAGE (LAKEHOUSE) [Quý + Bảo]
    Google Cloud Storage (GCS) + Delta Lake
    gs://<bucket>/lakehouse/bronze/
    gs://<bucket>/lakehouse/silver/ (Spatio-Temporal Join: station_code + datetime)
         │
         ▼
DISTRIBUTED PROCESSING & ML [Bảo]
    Cloud Dataproc (Apache Spark + Sedona + Delta Lake)
    - Feature engineering (lags, rolling averages, wind dynamics)
    - Distributed LightGBM training qua Spark applyInPandas
         │
         ▼
GOLD STORAGE & ANALYTICS [Bảo + Hiếu]
    gs://<bucket>/lakehouse/gold/
    - thermo_feature_store
    - air_quality_forecast (24-48h predictions)
    - model_metrics (RMSE, MAE, R2 per station)
         │
         ▼
SERVING + VISUALIZATION [Hiếu]
    BigQuery External Tables / Looker Studio Dashboard
    - Bản đồ trạm quan trắc mặt đất
    - Chuỗi thời gian đo đạc vs. dự báo
    - Bảng chỉ số sai số theo từng trạm/tỉnh thành
         │
         ▼
REPORT + PRESENTATION [Dương]
    Technical Report (8-12 pages, English)
    Slide deck (15 min presentation)
```

---

## 5. INFRASTRUCTURE DETAIL

| Layer | Technology | Purpose | Owner | GCP Service |
|---|---|---|---|---|
| Compute (Ingestion) | Python 3.11 | Crawl OpenAQ API | Quý | Local / Cloud Function |
| Raw Storage | GCS | Data Lake, lưu raw JSON + Parquet | Quý | Cloud Storage |
| Big Data Processing | Apache Spark | Cleaning, transform, aggregate | Bảo | Cloud Dataproc |
| Analytical DB | BigQuery | SQL analytics, curated tables | Bảo | BigQuery |
| Orchestration | Python scripts / cron | Chạy pipeline theo thứ tự | Quý | Local (MVP) |
| Visualization | Looker Studio | Dashboard kết nối BigQuery | Hiếu | Looker Studio (free) |
| Monitoring | GCP Console | Theo dõi job, cost | Dương | GCP Console |
| Version Control | Git + GitHub | Quản lý code | Bảo | GitHub |
| CI/CD | Không cần | Quá phức tạp cho đồ án | — | — |
| Kubernetes | Không cần | Overengineering | — | — |

> **Lý do không dùng Kafka:** Dataset OpenAQ không phải real-time streaming.
> Batch ingestion theo giờ/ngày là đủ và phù hợp hơn.

---

## 6. DATA FLOW

```
1. Quý chạy ingestion script
   → GET locations cho 6 quốc gia SEA (VN, TH, ID, MY, PH, SG)
   → GET sensors của từng location
   → GET measurements của từng sensor (có date range filter)
   → Lưu raw JSON vào GCS: gs://project/raw/{country}/{year}/{month}/

2. Quý convert JSON → Parquet
   → gs://project/parquet/{country}/{year}/{month}/

3. Bảo chạy Spark job trên Dataproc
   → Đọc Parquet từ GCS
   → Clean: remove null values, negative values, duplicates
   → Normalize: convert units, standardize timezone về UTC
   → Aggregate: tính daily/monthly averages theo (country, city, parameter)
   → Anomaly detection: flag records > 3 standard deviations
   → Ghi kết quả vào BigQuery

4. Hiếu validate output
   → Kiểm tra row counts, null rates, value ranges
   → Build Looker Studio dashboard
   → Viết deployment notes

5. Dương theo dõi cost trên GCP Console
   → Ghi nhận cost từng service
   → Viết Technical Report
   → Build slide deck
```

---

## 7. DATA CONTRACT [QUAN TRỌNG — INTERFACE GIỮA CÁC THÀNH VIÊN]

### 7.1. Schema: Raw Parquet (Quý → Bảo)

```
gs://project/parquet/{iso_country}/{year}/{month}/*.parquet

Columns:
- location_id     INT       ID của trạm
- location_name   STRING    Tên trạm
- country         STRING    ISO country code (VN, TH, ID, ...)
- city            STRING    Tên thành phố (nullable)
- sensor_id       INT       ID sensor
- parameter       STRING    pm25, pm10, no2, o3, co, so2
- value           DOUBLE    Giá trị đo
- unit            STRING    Đơn vị đo
- datetime_utc    TIMESTAMP Thời điểm đo (UTC)
- latitude        DOUBLE    (nullable)
- longitude       DOUBLE    (nullable)
```

### 7.2. Schema: BigQuery Table: measurements_clean (Bảo → Hiếu)

```
Table: air_quality_sea.measurements_clean
Partition: DATE(datetime_utc)
Cluster: country, parameter

Columns:
- location_id     INT64
- location_name   STRING
- country         STRING
- city            STRING
- sensor_id       INT64
- parameter       STRING
- value           FLOAT64
- unit            STRING
- datetime_utc    TIMESTAMP
- date_local      DATE
- is_anomaly      BOOL      (True nếu > 3 std dev)
- latitude        FLOAT64
- longitude       FLOAT64
```

---

## 8. WORK BREAKDOWN STRUCTURE (WBS)

### PHASE 0 — Setup & Architecture (Week 2–3)

| Task ID | Task | Owner | Dependency | Priority | Definition of Done |
|---|---|---|---|---|---|
| INF-01 | Tạo GCP Project, enable APIs | Quý | — | P0 | GCP project active, billing enabled |
| INF-02 | Setup IAM, service accounts | Quý | INF-01 | P0 | Tất cả members có access |
| INF-03 | Tạo GCS buckets (raw, parquet, logs) | Quý | INF-01 | P0 | Buckets tồn tại, naming convention thống nhất |
| INF-04 | Tạo BigQuery dataset | Bảo | INF-01 | P0 | Dataset `air_quality_sea` tồn tại |
| INF-05 | Setup GitHub repo + branch strategy | Bảo | — | P0 | Repo có, tất cả members đã clone |
| DOC-01 | Lock Problem Statement + Data Contract | Dương | Master Plan | P0 | Tất cả members ký off |

### PHASE 1 — Data Ingestion & Synchronization (Week 3–4)

| Task ID | Task | Owner | Dependency | Priority | Definition of Done |
|---|---|---|---|---|---|
| ING-01 | Khám phá & tiền xử lý dữ liệu CEM/TEDP | Quý | — | P0 | `cleaned_tedp.csv` hoàn thiện, hiểu đặc điểm trạm |
| ING-02 | Đồng bộ thời gian 30 ngày cho TEDP | Quý | ING-01 | P0 | `cleaned_tedp_synced_30d.csv` (19 trạm, 720h/trạm) |
| ING-03 | Viết crawler Open-Meteo theo toạ độ trạm | Quý | ING-02 | P0 | `weather_openmeteo_station_30d.csv` (5 biến, 720h/trạm) |
| ING-04 | Upload Bronze data lên GCS Lakehouse | Quý | ING-02, ING-03 | P0 | Dữ liệu xuất hiện trên `gs://<bucket>/lakehouse/bronze/` |
| QA-01 | Kiểm toán chất lượng & tính khớp nối 100% | Hiếu | ING-04 | P0 | Báo cáo xác nhận không lệch giờ, không mất dữ liệu sau merge |

### PHASE 2 — Data Processing & Lakehouse Silver/Gold (Week 4–5)

| Task ID | Task | Owner | Dependency | Priority | Definition of Done |
|---|---|---|---|---|---|
| PRO-01 | Setup Dataproc cluster (Spark 3.x + Delta Lake) | Bảo | INF-01, INF-03 | P0 | Cluster chạy được, hỗ trợ GCS connector & Delta |
| PRO-02 | Spark job: Spatio-Temporal Join (TEDP + Weather) | Bảo | ING-04, PRO-01 | P0 | Bảng Silver Delta Lake được lưu trên GCS |
| PRO-03 | Feature Engineering: Lags & Rolling Statistics | Bảo | PRO-02 | P0 | Bảng Gold Feature Store với lag $t-1..t-24$, rolling mean |
| PRO-04 | Distributed Machine Learning: LightGBM Forecast | Bảo | PRO-03 | P0 | Huấn luyện song song qua `applyInPandas`, sinh dự báo 24-48h |
| QA-02 | Validate Gold features & forecast output | Hiếu | PRO-04 | P0 | Row count khớp, đánh giá RMSE/MAE trên tập test |

### PHASE 3 — Analytics (Week 5–6)

| Task ID | Task | Owner | Dependency | Priority | Definition of Done |
|---|---|---|---|---|---|
| ANA-01 | SQL: Trend analysis (pm25 per country theo year) | Bảo | PRO-04 | P0 | Query chạy được, kết quả có ý nghĩa |
| ANA-02 | SQL: Country comparison (average pm25, pm10, no2) | Bảo | PRO-04 | P0 | Bảng so sánh 6 nước |
| ANA-03 | SQL: WHO threshold violation rate | Bảo | PRO-04 | P0 | % ngày vượt ngưỡng WHO theo nước |
| ANA-04 | SQL: Seasonal patterns | Bảo | PRO-04 | P1 | Phân tích theo mùa |
| VIZ-01 | Looker Studio: Time series dashboard | Hiếu | ANA-01 | P0 | Dashboard có ≥3 charts, connect to BigQuery |
| VIZ-02 | Looker Studio: Country comparison map | Hiếu | ANA-02 | P1 | Map chart theo country |

### PHASE 4 — Integration + Proposal (Week 6–7)

| Task ID | Task | Owner | Dependency | Priority | Definition of Done |
|---|---|---|---|---|---|
| INT-01 | End-to-end test: chạy pipeline từ đầu đến cuối | Bảo + Quý | PRO-05 | P0 | Pipeline chạy không lỗi, output đúng |
| INT-02 | Deployment notes (step-by-step reproducible) | Hiếu | INT-01 | P0 | Người khác có thể chạy lại từ README |
| DOC-02 | Project Proposal document | Dương | DOC-01 | P0 | Nộp trước Week 7 |
| DOC-03 | Cost analysis: ghi nhận GCP cost | Dương | INF-01 | P0 | Bảng cost breakdown theo service |

### PHASE 5 — Report + Presentation (Week 8–11)

| Task ID | Task | Owner | Dependency | Priority | Definition of Done |
|---|---|---|---|---|---|
| DOC-04 | Technical Report draft (English) | Dương | ANA-03 | P0 | 8–12 trang, đầy đủ sections |
| DOC-05 | Technical Report review | Hiếu | DOC-04 | P0 | Review grammar + technical accuracy |
| DOC-06 | Final Presentation slide deck | Dương | DOC-04 | P0 | 15 phút, tất cả members đã luyện tập |
| DOC-07 | AI-use Statement | Dương | — | P0 | Nộp cùng report |

---

## 9. CRITICAL PATH

```
INF-01 (GCP Setup)
    │
    ├─→ INF-03 (GCS buckets)
    │       │
    │       └─→ ING-02 (Ingestion script)
    │               │
    │               └─→ ING-03 (Upload raw)
    │                       │
    │                       └─→ ING-04 (Parquet)
    │                               │
    │                               └─→ PRO-02 (Spark clean)
    │                                       │
    │                                       └─→ PRO-04 (Aggregation → BigQuery)
    │                                               │
    │                                               └─→ ANA-01..03 (Analytics)
    │                                                       │
    │                                                       └─→ VIZ-01 (Dashboard)
    │                                                               │
    │                                                               └─→ INT-01 (E2E test)
    │                                                                       │
    │                                                                       └─→ DOC-04 (Report)
    │
    └─→ INF-04 (BigQuery dataset)
            │
            └─→ PRO-01 (Dataproc setup)
```

> **Bottleneck chính:** ING-04 (Parquet) → Bảo không thể bắt đầu PRO-01 cho đến khi Quý xong.
> **Giải pháp:** Quý ưu tiên xong ING-04 trước Week 4.

---

## 10. TIMELINE (Week 2 → Week 11)

| Week | Focus | Quý (Data Eng) | Bảo (Processing) | Hiếu (QA+Viz) | Dương (Docs+PM) |
|---|---|---|---|---|---|
| **W2** | Setup | INF-01, 02, 03 | INF-04, 05 | — | DOC-01 (Problem Statement) |
| **W3** | Ingestion | ING-01, 02 | PRO-01 (Dataproc) | QA-01 | Track tiến độ |
| **W4** | Ingestion + Process | ING-03, 04, 05 | PRO-02, 03 | QA-01 validate | DOC-03 (Cost tracking bắt đầu) |
| **W5** | Processing | Monitor + support | PRO-04, 05 | QA-02 validate | — |
| **W6** | Analytics + Viz | — | ANA-01, 02, 03 | VIZ-01 | Draft Proposal |
| **W7** | **PROPOSAL DEADLINE** | Support INT-01 | ANA-04, INT-01 | VIZ-02, INT-02 | **DOC-02 nộp Proposal** |
| **W8** | Refinement | Optimize ingestion | Optimize Spark | Polish dashboard | DOC-04 Report draft |
| **W9** | Testing + Cost | — | INT-01 re-run | DOC-05 review | DOC-04 continue |
| **W10** | **CHECK-IN** | — | — | — | DOC-03 finalize cost |
| **W11** | **FINAL PRESENTATION** | Present | Present | Present | **DOC-06 Slides + Present** |

---

## 11. MVP vs STRETCH GOALS

### MVP — BẮT BUỘC PHẢI CÓ

- [ ] Ingestion pipeline: OpenAQ → GCS → Parquet (≥3 quốc gia, ≥1 năm)
- [ ] Spark processing: cleaning + aggregation → BigQuery
- [ ] ≥3 analytics queries có kết quả ý nghĩa (trend, comparison, WHO violation)
- [ ] Dashboard ≥3 charts trên Looker Studio
- [ ] Deployment notes reproducible
- [ ] Technical Report (8–12 trang, English)
- [ ] Cost Analysis GCP
- [ ] Presentation 15 phút

### STRETCH GOALS — Chỉ làm nếu MVP ổn

- [ ] Machine Learning: time series forecast (Prophet/ARIMA)
- [ ] Cover đủ 6 nước SEA (MVP có thể chỉ cần 3)
- [ ] Real-time ingestion với Cloud Functions trigger
- [ ] AQI Index calculation (composite index từ nhiều parameters)
- [ ] Export report tự động từ BigQuery

---

## 12. RISK REGISTER

| Risk ID | Rủi ro | Probability | Impact | Dấu hiệu sớm | Mitigation |
|---|---|---|---|---|---|
| R-01 | GCP cost vượt budget | Medium | High | Bill > $20 sau W3 | Dùng free tier, tắt Dataproc khi không dùng, set budget alert |
| R-02 | Trạm OpenAQ tại SEA có quá ít data | Medium | High | ING-01 thấy < 50 trạm active | Mở rộng time range hoặc thêm parameter |
| R-03 | Dataproc quá chậm / tốn tiền | Medium | Medium | Job > 30 phút cho 1M rows | Thay bằng BigQuery SQL thuần (không cần Spark) |
| R-04 | Dependency block: Quý chậm → Bảo bị chờ | High | High | W4 mà chưa có Parquet | Quý cần xong ING-04 trước W4 bằng mọi cách |
| R-05 | Data quality quá kém (null > 50%) | Low | High | QA-01 báo null rate cao | Expand countries, kết hợp nhiều sources |
| R-06 | Không demo được end-to-end | Low | Critical | INT-01 fail W7 | Giữ pipeline đơn giản, test sớm từ W5 |
| R-07 | Dương không có data để viết Report | Medium | High | Analytics chưa xong W8 | Ưu tiên ANA-01..03 xong trước W8 |
| R-08 | Version conflict Python/Spark | Low | Medium | Spark job fail với lỗi import | Pin version ngay từ đầu trong requirements.txt |

---

## 13. TECHNOLOGY DECISION LOG

| Quyết định | Lý do chọn | Alternative bị loại | Trade-off |
|---|---|---|---|
| GCP | Yêu cầu môn học, kết quả thực tế | CloudSim | Tốn tiền thực, cần quản lý budget |
| BigQuery | Serverless, SQL quen thuộc, free tier 1TB/month | PostgreSQL, Hive | Không cần manage cluster, dễ connect Looker |
| Dataproc + Spark | Justify "Big Data" requirement, GCP managed | Local Spark, Databricks | Tốn tiền hơn local, nhưng có thể shutdown sau job |
| GCS | Native với GCP, rẻ, object storage | HDFS | Không cần manage HDFS namenode |
| Looker Studio | Free, native connect BigQuery, dễ dùng | Grafana, Tableau, custom web | Không tốn thêm time learning new tool |
| Không dùng Kafka | Data OpenAQ là batch, không streaming | Kafka, Pub/Sub | Đơn giản hơn, phù hợp với use case |
| Không dùng Kubernetes | Overengineering cho đồ án sinh viên | K8s, Docker Swarm | Tiết kiệm thời gian đáng kể |
| Python cho ingestion | Team đã biết, OpenAQ có SDK | Java, Scala | Không cần học thêm |

---

## 14. QUESTIONS CÒN MỞ

| # | Câu hỏi | Priority | Cần ai trả lời |
|---|---|---|---|
| Q1 | Budget GCP cho phép bao nhiêu? (USD) | P0 | Cả nhóm / thầy hướng dẫn |
| Q2 | Có GCP credits (student/research) không? | P0 | Cả nhóm |
| Q3 | Phải thu thập data mấy năm? (ảnh hưởng volume + cost) | P1 | Nhóm quyết định |
| Q4 | Proposal nộp theo format gì? | P1 | Dương hỏi thầy |
| Q5 | ML component có bắt buộc trong đề bài không? | P2 | Hỏi thầy |

---

## 15. IMMEDIATE ACTIONS — TUẦN NÀY (WEEK 2)

**Quý:**
1. Tạo GCP Project → enable APIs: Cloud Storage, BigQuery, Dataproc, IAM
2. Tạo GCS buckets: `raw/`, `parquet/`, `logs/`
3. Chạy ING-01: liệt kê tất cả trạm active tại VN, TH, ID, MY, PH, SG

**Bảo:**
1. Setup GitHub repo, tạo branch: `main`, `dev`, `feature/*`
2. Tạo BigQuery dataset `air_quality_sea`
3. Setup Dataproc cluster nhỏ (2 worker nodes, n1-standard-2) — **QUAN TRỌNG: TẮT khi không dùng**

**Hiếu:**
1. Đọc Data Contract (Section 7) và confirm hiểu schema
2. Chuẩn bị checklist QA cho ING-03 (sẽ validate tuần sau)

**Dương:**
1. Draft Problem Statement → send cho cả nhóm review
2. Setup GCP Billing Alert: alert khi > $10, hard limit $50
3. Hỏi thầy về: budget, Proposal format, ML requirement

---

*Master Plan v1.0 — Cập nhật khi có quyết định mới hoặc sau mỗi phase review.*
