# Preprocessing và Data Normalization Plan

## 1. Mục tiêu

Xây dựng dataset đáng tin cậy cho bài toán dự báo PM2.5 24 giờ, sử dụng dữ liệu:

- CEM/TEDP: ground truth về PM2.5, AQI và các chất ô nhiễm.
- Open-Meteo: các biến thời tiết theo giờ tại vị trí từng trạm.

Dataset cuối phải:

- Có khóa logic duy nhất `(station_code, datetime)`.
- Có timestamp liên tục theo giờ.
- Khớp 1-1 giữa dữ liệu CEM/TEDP và Open-Meteo.
- Phân biệt rõ dữ liệu gốc, dữ liệu nội suy và dữ liệu bị loại.
- Không có data leakage giữa train, validation và test.
- Có thể dùng cho Linear Regression, SVR, Random Forest, XGBoost và LightGBM.

## 2. Phạm vi dữ liệu hiện tại

| Nguồn | File | Quy mô chuẩn hóa |
|---|---|---:|
| CEM/TEDP (120 ngày) | `data_test/cleaned_tedp_synced_120d.csv` | 15 trạm × 2.880 giờ = **43.200 dòng** (35 cột) |
| Open-Meteo (120 ngày) | `data_test/weather_openmeteo_station_120d.csv` | 15 trạm × 2.880 giờ = **43.200 dòng** (18 cột) |
| *(Bản đối chiếu 30 ngày)* | `data_test/cleaned_tedp_synced_30d.csv` & `weather_..._30d.csv` | 19 trạm × 720 giờ = 13.680 dòng |

Khoảng thời gian chuẩn 120 ngày:

```text
2026-06-01 00:00:00 -> 2026-09-28 23:00:00
```

Tần suất: 1 giờ (Đơn vị biến mục tiêu `pm25`: Chỉ số AQI thành phần $I_{\text{PM2.5}}$ theo chuẩn VN_AQI).

## 3. Kiến trúc các tầng dữ liệu

### Bronze — Raw

Giữ nguyên dữ liệu đầu vào bất biến (Immutable Raw), không sửa trực tiếp:

```text
lakehouse_storage/bronze/tedp/
lakehouse_storage/bronze/openmeteo/
```

Mục đích:

- Audit dữ liệu nguồn.
- Có thể chạy lại pipeline bất kỳ lúc nào.
- Không mất thông tin gốc.

### Silver — Cleaned and Synced (Partitioned Parquet)

Thực hiện:

- Chuẩn hóa schema và kiểu dữ liệu.
- Kiểm tra duplicate và lọc ngưỡng vật lý QCVN.
- Đồng bộ lưới thời gian 120 ngày (`2.880` giờ/trạm) và giữ các quality flags (`*_imputed`, `is_inserted`).
- Lưu trữ dưới định dạng **Apache Parquet (nén Snappy) phân vùng theo tỉnh (`province=...`)**.
- Join CEM/TEDP với Open-Meteo theo `(station_code, datetime)` (khớp 1-1 tuyệt đối `43.200` dòng).

Đầu ra dự kiến:

```text
lakehouse_storage/silver/tedp/province=.../
lakehouse_storage/silver/weather/province=.../
```

### Gold — Modeling Features & Spatial Forecast Mart

Thực hiện:

- Tạo lag features (`pm25_lag0..lag24`) và rolling statistics (`pm25_roll6`, `pm25_roll24`).
- Tạo các đặc trưng nhiệt động lực học (`wind_dir_sin`, `wind_dir_cos`, `dew_point_deficit`, `ventilation_coef`) và biến thiên thời tiết 24h (`temp_diff_24h`, `pressure_diff_24h`, `pblh_diff_24h`, `precip_sum_24h`).
- Encode station (`station_*`) và kết hợp tọa độ (`latitude`, `longitude`) vào bảng kết quả dự báo để phục vụ **UI Bản đồ Không gian** và **Mô phỏng CloudSim**.

Đầu ra dự kiến:

```text
lakehouse_storage/gold/thermo_feature_store/province=.../
lakehouse_storage/gold/spatial_forecasts/
lakehouse_storage/gold/model_metrics/
```

## 4. Data profiling

Trước khi xử lý, tạo báo cáo cho từng nguồn:

- Tên cột, kiểu dữ liệu và cột bắt buộc.
- Số dòng, số trạm và số tỉnh.
- Khoảng thời gian.
- Null count và null percentage.
- Duplicate count theo `(station_code, datetime)`.
- Min, max, mean, median và percentile của các cột số.

### Cột bắt buộc của CEM/TEDP

```text
station_code, province_name, datetime
pm25, aqi, pm10, no2, so2, o3, co
latitude, longitude
```

### Cột bắt buộc của Open-Meteo

```text
station_code, datetime
temperature_2m, relative_humidity_2m, dew_point_2m
surface_pressure, precipitation
wind_speed_10m, wind_direction_10m, boundary_layer_height
```

Nếu thiếu cột bắt buộc, pipeline phải dừng với lỗi rõ ràng.

## 5. Chuẩn hóa schema và kiểu dữ liệu

### Định danh

Giữ các cột sau dưới dạng `string`:

```text
province_id
station_id
station_code
```

Không chuyển định danh thành số vì có thể mất leading zero hoặc ký tự.

### Datetime

- Parse bằng `pd.to_datetime(..., errors="coerce")`.
- Không trộn timezone-aware và timezone-naive.
- Chuẩn hóa timezone thống nhất (`Asia/Bangkok`, UTC+7) trong toàn bộ pipeline cục bộ, Parquet Lakehouse và giao diện UI Bản đồ.

### Numeric

Chuyển các cột đo, tọa độ và thời tiết sang kiểu số:

```text
pm25, aqi, pm10, no2, so2, o3, co
temperature_2m, relative_humidity_2m, dew_point_2m
surface_pressure, precipitation
wind_speed_10m, wind_direction_10m, boundary_layer_height
latitude, longitude, elevation
```

Giá trị không parse được phải thành `NaN` và được ghi log.

## 6. Kiểm tra khóa và timestamp

### Duplicate

Kiểm tra duplicate theo:

```text
(station_code, datetime)
```

Quy tắc:

- Hai dòng giống nhau: giữ một dòng.
- Hai dòng khác giá trị: đưa vào quarantine hoặc dừng pipeline.
- Không tự động chọn ngẫu nhiên một bản ghi khác nhau.

### Coverage

Kiểm tra (cho bộ 120 ngày):

```text
nunique(station_code) == 15 (9 tỉnh)
2880 timestamp mỗi station_code (43.200 dòng tổng)
min(datetime) == 2026-06-01 00:00:00
max(datetime) == 2026-09-28 23:00:00
frequency == 1 hour
```

Tập timestamp của mọi trạm phải giống nhau.

### Join

Join bằng:

```text
station_code + datetime
```

Phải xác nhận:

- Không mất dòng CEM/TEDP.
- Không có dòng weather không ghép được.
- Không có duplicate sau join.
- Join là quan hệ 1-1.

## 7. Missing values và nội suy

### Phân loại

Phân biệt:

1. Missing từ dữ liệu nguồn.
2. Missing do dòng được chèn vào lưới giờ.
3. Missing do lỗi join.

Không gộp các loại missing này thành một loại duy nhất.

### CEM/TEDP

- Nội suy tuyến tính theo thời gian.
- Gắn `*_imputed = 1`.
- Gắn `is_inserted = 1` cho dòng được chèn.

Khuyến nghị:

| Khoảng thiếu | Xử lý |
|---|---|
| 1–3 giờ | Nội suy tuyến tính |
| 4–6 giờ | Có thể nội suy, nhưng gắn quality flag |
| > 6 giờ | Không tự động nội suy |
| Thiếu ở đầu/cuối chuỗi | Không coi là nội suy đáng tin cậy |
| Thiếu quá nhiều ngày | Loại trạm hoặc tỉnh khỏi tập chính |

### Open-Meteo

- 1–3 giờ: có thể nội suy.
- Khoảng dài hơn: kiểm tra lại API hoặc loại batch.
- Ưu tiên gọi lại API thay vì fill bằng median toàn bộ dataset.
- Nếu phải fill, tạo `weather_imputed = 1`.

Không xóa các cờ:

```text
is_inserted
pm25_imputed, aqi_imputed, pm10_imputed
no2_imputed, so2_imputed, o3_imputed, co_imputed
weather_imputed
```

## 8. Outlier và kiểm tra tính hợp lý

### Hard bounds ban đầu

| Biến | Khoảng kiểm tra |
|---|---|
| `pm25` | 0–500 |
| `pm10` | 0–600 |
| `no2` | 0–1000 |
| `so2` | 0–1000 |
| `o3` | 0–500 |
| `co` | 0–30000 |
| `aqi` | 0–500 |
| `relative_humidity_2m` | 0–100 |
| `precipitation` | >= 0 |
| `wind_speed_10m` | >= 0 |

Giá trị âm hoặc vượt giới hạn vật lý phải được chuyển thành missing hoặc đưa vào
quarantine, tùy quy tắc của từng biến.

### Statistical outlier

Không xóa ngay PM2.5 cao chỉ vì vượt `mean + 3 * std`; đó có thể là một đợt
ô nhiễm thật.

Phân loại:

```text
hard_invalid
statistical_outlier
valid_extreme
```

Nên kiểm tra theo từng `station_code` bằng IQR, rolling median hoặc MAD.
Giá trị nhảy đột biến nên được gắn cờ, không xóa tự động:

```text
pm25_jump_flag = 1
```

## 9. Feature engineering chống leakage

### Lag features

Chỉ dùng dữ liệu tại hoặc trước thời điểm `t`:

```text
pm25_lag0
pm25_lag1
pm25_lag3
pm25_lag6
pm25_lag12
pm25_lag24
```

### Rolling features

Đề xuất:

```text
pm25_roll6
pm25_roll24
```

Rolling phải được tính theo từng trạm và không được dùng `center=True`.

### Weather tại target time

Weather tại `t + 24` chỉ hợp lệ nếu mô hình giả định có forecast weather cho
thời điểm cần dự báo. Assumption này phải được ghi rõ trong báo cáo.

Nếu chỉ có weather quan sát trong quá khứ, không được dùng giá trị tương lai
quan sát được như thể đó là forecast thực tế.

### Time features

Có thể thêm cyclic encoding:

```text
hour_sin, hour_cos
day_of_week_sin, day_of_week_cos
```

Điều này biểu diễn đúng tính chu kỳ của 23:00 gần 00:00 và Chủ nhật gần
thứ Hai.

## 10. Data normalization cho từng model

### Không cần scaling

Các model cây không cần StandardScaler:

- Random Forest.
- XGBoost.
- LightGBM.

### Cần scaling

Các model nhạy với scale:

- Linear Regression.
- SVR.

Dùng pipeline:

```python
Pipeline([
    ("scaler", StandardScaler()),
    ("model", LinearRegression()),
])
```

```python
Pipeline([
    ("scaler", StandardScaler()),
    ("model", SVR()),
])
```

Scaler chỉ được `fit` trên `X_train`, sau đó mới transform validation và test.
Không được fit scaler trên toàn bộ dataset.

### Target

Giữ target PM2.5 ở đơn vị gốc để RMSE và MAE dễ giải thích.
Chỉ thử `log1p(pm25)` như một thí nghiệm tùy chọn khi phân phối lệch phải
mạnh; phải inverse-transform trước khi tính metric.

## 11. Dataset Gold cho modeling

### Metadata

```text
station_code
province_name
origin_time
target_time
```

### Target

```text
y_pm25
```

Có thể bổ sung `y_aqi` ở một bài toán riêng.

### Features (30–31 đặc trưng trong `code/training/common.py`)

```text
pm25_lag0, pm25_lag1, pm25_lag3
pm25_lag6, pm25_lag12, pm25_lag24
pm25_roll6, pm25_roll24
temperature_2m, relative_humidity_2m, dew_point_2m
surface_pressure, precipitation
wind_speed_10m, wind_direction_10m, boundary_layer_height
wind_dir_sin, wind_dir_cos
dew_point_deficit, ventilation_coef
temp_diff_24h, pressure_diff_24h, pblh_diff_24h, precip_sum_24h
hour, day_of_week, is_weekend
station_* (One-hot encoding các trạm nội tỉnh)
```

### Quality flags

```text
is_inserted
pm25_imputed
weather_imputed
pm25_jump_flag
```

Nên chạy thêm sensitivity analysis có và không có quality flags.

## 12. Chia train, validation và test

Tiếp tục chia theo thời gian, không random split:

```text
70% train
10% validation
20% test
```

Quy trình:

1. Chia theo `target_time`.
2. Fit imputer và scaler trên train.
3. Áp dụng cho validation và test.
4. Chọn model/hyperparameter bằng validation.
5. Chỉ đánh giá test sau khi đã khóa model.

Không được để timestamp của test xuất hiện trong train hoặc validation.

## 13. So sánh và chọn model

Các model cần đánh giá:

- Linear Regression.
- SVR.
- Random Forest.
- XGBoost.
- LightGBM.

Mỗi model cần lưu:

```text
RMSE
MAE
R2
improvement so với persistence baseline
train time
model size
metrics theo từng trạm
```

Tiêu chí:

1. Ưu tiên RMSE thấp hơn baseline.
2. Nếu RMSE gần nhau, ưu tiên MAE thấp hơn.
3. Nếu vẫn gần nhau, ưu tiên model đơn giản và dễ giải thích hơn.

## 14. Kiểm thử chất lượng bắt buộc

Pipeline phải dừng nếu:

```text
duplicate_keys > 0
missing_weather_join > 0
invalid_datetime > 0
station_timestamp_count != 2880
unexpected_negative_values > 0
```

Trước khi train cần kiểm tra:

- Không có `NaN` trong `X_train`, `X_val`, `X_test`.
- Không có `inf` hoặc `-inf`.
- Target là số hữu hạn.
- Số mẫu train/validation/test đúng.
- Quality flags có giá trị hợp lệ.

## 15. Artifact và báo cáo đầu ra

Tạo các báo cáo:

```text
data_quality_report.json
schema_report.json
duplicate_report.csv
missing_report.csv
```

Artifact model:

```text
output/province_models/<province>/<model>/
├── model.joblib hoặc model.txt
├── metrics.json
├── predictions.csv
└── feature_importance.csv nếu model hỗ trợ
```

Bảng tổng hợp nên có các cột:

```text
province
model
rmse
mae
r2
baseline_rmse
improvement_pct
train_time_seconds
n_train
n_val
n_test
```

## 16. Lộ trình triển khai

### Phase 1 — Data audit

Tạo schema report, missing report, duplicate report và distribution report.

### Phase 2 — Silver join

Chuẩn hóa hai nguồn và tạo bảng join 1-1 theo `(station_code, datetime)`.

### Phase 3 — Gold features

Tạo lag, rolling, weather, cyclic time features và quality flags.

### Phase 4 — Model preprocessing

Tạo pipeline scaler riêng cho Linear Regression và SVR. Tree models dùng
feature gốc.

### Phase 5 — Training

Train từng model theo từng tỉnh, lưu model và metrics riêng.

### Phase 6 — Evaluation

So sánh RMSE, MAE, R2 với persistence baseline; báo cáo thêm kết quả theo trạm.

### Phase 7 — Sensitivity analysis

So sánh:

- Có và không có dòng nội suy.
- Có và không có quality flags.
- Có và không có weather features.
- Horizon 6h, 12h và 24h.
- Model riêng theo tỉnh và model chung toàn quốc.

## 17. MVP đã hoàn thành & Bước tiếp theo (Phương án B)

Các hạng mục cốt lõi đã hoàn tất:

1. Schema validation & 8-step auditing (`code/crawl_data/preprocess_tedp.py`).
2. Duplicate validation & QCVN physical bounds filtering.
3. Đồng bộ 15 trạm (9 tỉnh) × `2.880` giờ = `43.200` dòng (`code/crawl_data/sync_tedp_120days.py`).
4. Join 1-1 CEM/TEDP và Open-Meteo 8 biến (`43.200` dòng).
5. Trích xuất 30–31 đặc trưng: Lag `0, 1, 3, 6, 12, 24`, Rolling `6h, 24h`, và nhóm đặc trưng nhiệt động lực học ($PBLH, V_c, DPD, \sin/\cos\text{ wind}, \Delta_{24\text{h}}$).
6. Chronological split 70/10/20 theo thời gian.
7. Scaling chuẩn cho Linear Regression và SVR; Early Stopping & Regularization công bằng cho LightGBM và XGBoost.
8. Huấn luyện & đối sánh 5 mô hình với `Persistence 24h` trên cả 9 tỉnh (`output/province_models/`).
9. **Bước tiếp theo (Phương án B):** Đóng gói `lakehouse_storage/` dạng Partitioned Parquet, chạy mô phỏng đa cấu hình trên **CloudSim** và dựng **UI Bản đồ Dự báo Không gian** theo tọa độ `(latitude, longitude)`.

## 18. Lưu ý diễn giải kết quả

Bộ dữ liệu chuẩn hóa gồm **120 ngày liên tục (4 tháng: `01/06/2026 – 28/09/2026`)**, bao phủ giai đoạn mùa hè sang đầu mùa thu tại Việt Nam.

Khi diễn giải kết quả trong Báo cáo & Bảo vệ đồ án cần nhấn mạnh:

- **Sự đánh đổi giữa Mô hình Cây (`LightGBM`, `XGBoost`) và Mô hình Tuyến tính (`Linear Regression`):** Các tỉnh có mật độ trạm cao ($\ge 2$ trạm) và biến động thời tiết mạnh (`Hà Nội`, `Hưng Yên`, `Ninh Bình`, `Đà Nẵng`) chứng kiến sự vượt trội của `LightGBM`/`XGBoost`. Ngược lại, các tỉnh chỉ có 1 trạm đơn lẻ (`2.880` dòng) chịu hiện tượng trôi phân phối giao mùa (`Bắc Ninh`, `Quảng Ngãi`, `Lâm Đồng`) hoặc quán tính chu kỳ 24h lớn (`TP.HCM`) phát huy ưu thế ngoại suy tuyến tính của `Linear Regression`.
- **Khả năng mở rộng hạ tầng (CloudSim Scalability):** Thiết kế lưu trữ Parquet phân vùng theo tỉnh (`province=...`) cho phép huấn luyện song song độc lập (*Embarrassingly Parallel*) trên nhiều máy ảo đám mây được đánh giá thông qua bộ mô phỏng **CloudSim**.
