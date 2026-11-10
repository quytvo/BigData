# Big Data in Cloud Computing — Final Project Challenge

> **Final Project Challenge** chiếm **30% tổng điểm của môn học Big Data in Cloud Computing**.

---

## 1. Project Objective

Sinh viên cần xây dựng một **giải pháp dữ liệu lớn hoàn chỉnh (End-to-End Big Data Solution)** được triển khai trên một trong hai nền tảng:

- **Google Cloud Platform (GCP)** — Public Cloud
- **CloudSim** — Cloud Simulator

Mục tiêu của đồ án là xây dựng, triển khai và đánh giá một giải pháp Big Data có khả năng thể hiện:

- Data processing
- Cloud infrastructure
- Storage
- Big Data architecture
- Service selection
- Cost analysis
- Performance evaluation
- Technical trade-offs

---

## 2. Team Size & Project Timeline

### 2.1. Team Size

- Quy mô nhóm: **2–5 sinh viên**.

### 2.2. Timeline

| Milestone | Week | Description |
|---|---:|---|
| Team Formation | S1 | Thành lập nhóm |
| Project Proposal | S7 | Nộp đề xuất dự án |
| Project Check-in | S10 | Kiểm tra tiến độ |
| Final Presentation | S11 | Báo cáo và thuyết trình cuối kỳ |

---

## 3. Dataset Options

Nhóm có thể lựa chọn một trong các hướng dữ liệu sau:

### 3.1. Public Big Data Sets

Sử dụng các tập dữ liệu công cộng có quy mô lớn.

### 3.2. E-commerce Behavior Data

Sử dụng tập dữ liệu Kaggle:

> **eCommerce Behavior Data from Multi Category Store**

Mục tiêu có thể tập trung vào việc phân tích hành vi người dùng trong môi trường thương mại điện tử.

### 3.3. Financial Fraud Detection

Sử dụng:

> **PaySim Mobile Money Dataset**

Tập dữ liệu phục vụ các bài toán như:

- Fraud detection
- Anomaly detection
- Large-scale transaction analysis

> **Lưu ý:** Đây là các hướng dữ liệu được môn học gợi ý. Nhóm cần kiểm tra lại yêu cầu của giảng viên trước khi khóa dataset và problem statement.

---

## 4. Deployment Options

Nhóm có thể lựa chọn một trong hai phương án triển khai:

| Tiêu chí | Public Cloud — GCP | Cloud Simulator — CloudSim |
|---|---|---|
| Môi trường | Google Cloud Platform | CloudSim |
| Hình thức | Triển khai pipeline thực tế | Mô phỏng pipeline |
| Execution Time | Đo thời gian thực tế | Đo thời gian mô phỏng |
| Cost | Báo cáo chi phí thực tế phát sinh | Báo cáo chi phí mô phỏng |
| Configuration | Cấu hình cloud thực tế | Ít nhất **2 cấu hình khác nhau** |
| Resource Modeling | Dựa trên tài nguyên GCP | Phải lập luận dựa trên giá và hiệu năng thực tế |

### 4.1. Public Cloud — GCP

Nếu lựa chọn GCP, nhóm cần:

1. Triển khai pipeline thực tế trên GCP.
2. Đo thời gian thực thi.
3. Ghi nhận và báo cáo chi phí phát sinh.
4. Phân tích lựa chọn các dịch vụ cloud.
5. Giải thích architecture và các trade-offs.

### 4.2. Cloud Simulator — CloudSim

Nếu lựa chọn CloudSim, nhóm cần:

1. Cài đặt logic pipeline cục bộ.
2. Mô phỏng việc triển khai pipeline trên CloudSim.
3. Sử dụng **ít nhất 2 cấu hình tài nguyên khác nhau**.
4. Báo cáo:
   - Execution time
   - Simulated cost
   - Resource configuration
5. Lập luận về mô hình tài nguyên dựa trên:
   - Giá thực tế
   - Hiệu năng thực tế
   - Các giả định được sử dụng

---

## 5. Required Deliverables

### 5.1. Working Code & Deployment Notes

Nhóm phải cung cấp:

- Mã nguồn hoạt động được.
- Hướng dẫn triển khai.
- Deployment notes.
- Các thông tin cần thiết để tái lập hệ thống.

Giải pháp phải có khả năng được tái lập trên:

- GCP

**hoặc**

- CloudSim

---

### 5.2. Technical Report

Báo cáo phải:

- Viết bằng **tiếng Anh**.
- Có độ dài **8–12 trang**.

Báo cáo cần bao gồm các nội dung chính:

1. Problem Definition
2. System / Big Data Architecture
3. Service Selection
4. Cost Estimation
5. Storage Selection
6. Technical Trade-offs
7. Algorithms / Processing Methods
8. Experimental Results
9. Lessons Learned

Báo cáo cần thể hiện rõ **lý do** đằng sau các quyết định kỹ thuật thay vì chỉ liệt kê công nghệ được sử dụng.

---

### 5.3. Final Presentation

- Thời lượng: **15 phút**.
- Thời điểm: **Week 11 (S11)**.
- Ngôn ngữ: **English**.
- **Tất cả thành viên trong nhóm đều phải tham gia trình bày.**

---

## 6. Assessment Rubric

Đồ án được đánh giá theo 4 nhóm tiêu chí chính:

| Criteria | Weight |
|---|---:|
| Technical Solution & Results | **40%** |
| Architecture & Cost Justification (CLO5) | **20%** |
| Technical Report — English Quality | **20%** |
| Presentation & Teamwork | **20%** |
| **Total** | **100%** |

### 6.1. Technical Solution & Results — 40%

Đánh giá:

- Chất lượng giải pháp kỹ thuật.
- Khả năng triển khai.
- Kết quả thực nghiệm.
- Tính phù hợp của phương pháp xử lý dữ liệu.

### 6.2. Architecture & Cost Justification — 20%

Đánh giá:

- Kiến trúc hệ thống.
- Lựa chọn dịch vụ.
- Lựa chọn storage.
- Phân tích chi phí.
- Khả năng giải thích technical trade-offs.

Tiêu chí này liên quan đến **CLO5**.

### 6.3. Technical Report — 20%

Đánh giá:

- Chất lượng nội dung.
- Cấu trúc báo cáo.
- Chất lượng tiếng Anh.
- Technical explanation.
- Tính rõ ràng và nhất quán.

### 6.4. Presentation & Teamwork — 20%

Đánh giá:

- Chất lượng thuyết trình.
- Khả năng giải thích hệ thống.
- Khả năng trả lời câu hỏi.
- Mức độ tham gia của các thành viên.
- Khả năng làm việc nhóm.

> **Peer Evaluation:** Điểm cá nhân có thể được điều chỉnh dựa trên đánh giá đồng đẳng giữa các thành viên trong nhóm.

---

## 7. Important Policies & Requirements

### 7.1. Language

Các thành phần chính của đồ án phải sử dụng **English**:

- Technical Report
- Final Presentation

---

### 7.2. Citations & References

Nhóm bắt buộc phải trích dẫn các nguồn bên ngoài, bao gồm:

- Dataset
- Libraries
- Frameworks
- Technical documentation
- Research papers
- Other external references

Tất cả nguồn được sử dụng trong đồ án cần được ghi nhận phù hợp trong báo cáo.

---

### 7.3. AI Usage Policy

Sinh viên **được phép sử dụng AI tools** để hỗ trợ thực hiện đồ án.

Tuy nhiên, nhóm phải cung cấp:

> **AI-use Statement**

AI-use Statement cần phân định rõ:

- Nội dung nào được AI hỗ trợ.
- AI được sử dụng ở mức độ nào.
- Phần nào do sinh viên tự thực hiện.

Sinh viên phải:

1. Hiểu nội dung mình nộp.
2. Có khả năng giải thích các quyết định kỹ thuật.
3. Có khả năng bảo vệ toàn bộ nội dung của đồ án khi được giảng viên yêu cầu.

> **Nguyên tắc:** Việc AI tạo ra một phần nội dung không thay thế trách nhiệm của sinh viên đối với nội dung cuối cùng.

---

# 8. Project Constraints Summary

Các ràng buộc chính của Final Project có thể được tóm tắt như sau:

| Constraint | Requirement |
|---|---|
| Course | Big Data in Cloud Computing |
| Project Weight | 30% of total course grade |
| Team Size | 2–5 students |
| Deployment | GCP hoặc CloudSim |
| Proposal | Week 7 |
| Check-in | Week 10 |
| Final Presentation | Week 11 |
| Presentation Duration | 15 minutes |
| Presentation Language | English |
| Report Language | English |
| Report Length | 8–12 pages |
| Working Code | Required |
| Deployment Notes | Required |
| Cost Analysis | Required |
| Architecture Analysis | Required |
| Technical Trade-offs | Required |
| Citations | Required |
| AI-use Statement | Required |
| Individual Evaluation | Có thể điều chỉnh bằng peer evaluation |

---

# 9. Core Project Success Criteria

Dựa trên các yêu cầu trên, một project hoàn chỉnh cần thể hiện được các thành phần cốt lõi:

```text
DATA SOURCE
     ↓
DATA INGESTION
     ↓
STORAGE
     ↓
BIG DATA PROCESSING
     ↓
ANALYTICS / ALGORITHM
     ↓
RESULT
     ↓
EVALUATION
```

---

# 10. Current Project Selection & Implementation Status (Updated 08/11/2026)

Trên cơ sở các yêu cầu của học phần **Big Data in Cloud Computing**, nhóm 4 thành viên đã lựa chọn hướng **Public Big Data Sets (Mục 3.1)** kết hợp phương án triển khai **CloudSim + Local Medallion Lakehouse (Mục 4.2)** với các thông số thực tế đã triển khai như sau:

- **Bài toán (Problem Statement):** Dự báo không gian - thời gian chỉ số bụi mịn `PM2.5` trước 24 giờ (`t+24h`) trên quy mô liên vùng lãnh thổ Việt Nam dựa trên sự kết hợp giữa lịch sử quan trắc chất lượng không khí quốc gia (CEM/TEDP) và dữ liệu tái phân tích/dự báo khí tượng (Open-Meteo ERA5).
- **Quy mô dữ liệu thực tế (Dataset Scale):**
  - **Tầng Bronze (`lakehouse_storage/bronze/tedp_aqi_pm25_hourly.csv`):** **474.286 bản ghi theo giờ** thu thập từ 28 trạm quan trắc trên 14 tỉnh/thành phố, chuẩn hóa về 3 chỉ số ô nhiễm cốt lõi (`pm25`, `pm10`, `aqi`).
  - **Tầng Silver cấp tỉnh (`data_test/cleaned_tedp_province_4y.csv` & `data_test/weather_openmeteo_province_4y.csv`):** Gom cụm **17 trạm thành viên đạt chuẩn độ phủ >= 80%** thành **11 nút tỉnh/thành phố** trải dài gần 3 năm liên tục (`2023-12-11 00:00:00` đến `2026-11-07 23:00:00`, **25.512 giờ/tỉnh**, **280.632 dòng** cho mỗi bảng TEDP và Open-Meteo).
- **Mô hình Học máy & Suy luận Dự báo (Analytics & Serving):**
  - **Mô hình trung tâm:** Mạng nơ-ron toàn cục **Spatio-Temporal Global ANN (With Spatial Attention)** huấn luyện trên **279.840 mẫu hợp lệ x 49 đặc trưng vật lý khí quyển**, đạt **Test RMSE = 17,68** (vượt Baseline Persistence `21,17` với mức cải thiện **+16,50%**, chiến thắng trên cả **11/11 tỉnh/thành phố**).
  - **Dự báo 24 giờ thời gian thực:** Tích hợp pipeline [`code/crawl_data/fetch_forecast_24h.py`](file:///d:/năm%204/big%20data/Final_Project_Plan/code/crawl_data/fetch_forecast_24h.py) và [`code/training/du_bao.py`](file:///d:/năm%204/big%20data/Final_Project_Plan/code/training/du_bao.py) tự động thu thập dự báo khí tượng 24 giờ tới cho 11 tỉnh và xuất bảng dự báo `PM2.5` kèm phân loại cảnh báo `VN_AQI` theo từng giờ.
- **Tài liệu tham chiếu chi tiết trong thư mục `docs/`:**
  - Tổng quan kiến trúc và phân tích 5V: [`docs/project_overview.md`](file:///d:/năm%204/big%20data/Final_Project_Plan/docs/project_overview.md)
  - Đặc tả tiền xử lý và chuẩn hóa dữ liệu: [`docs/preprocessing_data_normalization_plan.md`](file:///d:/năm%204/big%20data/Final_Project_Plan/docs/preprocessing_data_normalization_plan.md)
  - Kiến trúc kỹ thuật pipeline End-to-End: [`docs/pipeline_detail.md`](file:///d:/năm%204/big%20data/Final_Project_Plan/docs/pipeline_detail.md)
  - Hướng dẫn luồng hoạt động dễ hiểu: [`docs/overview.md`](file:///d:/năm%204/big%20data/Final_Project_Plan/docs/overview.md)
  - Phân rã công việc và lịch trình: [`docs/task_breakdown_schedule.md`](file:///d:/năm%204/big%20data/Final_Project_Plan/docs/task_breakdown_schedule.md)
