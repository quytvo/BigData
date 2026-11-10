# Preprocessing và Data Normalization Plan

## 1. Mục tiêu

Xây dựng bộ dữ liệu chuẩn hóa, đồng bộ không gian - thời gian cho bài toán dự báo chỉ số bụi mịn `PM2.5` trước 24 giờ, kết hợp hai nguồn dữ liệu:

- **CEM/TEDP (`https://tedp.vn/api`):** Dữ liệu quan trắc thực tế mặt đất (Ground Truth) gồm 3 chỉ số cốt lõi: `pm25`, `pm10`, `aqi` (đã loại bỏ hoàn toàn các biến phụ `no2, so2, o3, co`).
- **Open-Meteo (ERA5 Archive & Forecast API):** 8 biến khí tượng và chiều cao lớp biên khí quyển theo giờ, đồng bộ theo tọa độ trạm và trọng tâm tỉnh.

Bộ dữ liệu cuối phải đảm bảo:

- Có khóa logic duy nhất `(station_code, datetime)` (trong đó mỗi nút cấp tỉnh có `station_code = PROV_TEN_TINH`).
- Có lưới timestamp liên tục từng giờ (`freq = 1h`, `25.512` giờ/tỉnh).
- Khớp 1-1 tuyệt đối giữa dữ liệu CEM/TEDP và Open-Meteo (`280.632` dòng mỗi nguồn).
- Phân biệt rõ giá trị đo thật, giá trị nội suy ngắn hạn (`<= 3h`) và khoảng trống dài hạn (`pm25_gap_hours`).
- Không có rò rỉ dữ liệu tương lai (Zero Data Leakage) giữa tập Train (`70%`), Validation (`10%`) và Test (`20%`).
- Dùng trực tiếp cho cả mạng nơ-ron không gian - thời gian toàn cục (`Spatio-Temporal Global ANN`), bộ 5 mô hình học máy đối sánh (`Linear Regression`, `SVR`, `Random Forest`, `XGBoost`, `LightGBM`) và pipeline suy luận dự báo 24h tới (`du_bao.py`).

---

## 2. Phạm vi dữ liệu thực tế hiện tại (Gần 3 năm: 2023 – 2026)

| Tầng / Nguồn | Đường dẫn File | Quy mô chuẩn hóa thực tế |
|---|---|---|
| **Bronze — TEDP Raw Monthly** | [`lakehouse_storage/bronze/tedp_raw_monthly/*.csv`](file:///d:/năm%204/big%20data/Final_Project_Plan/lakehouse_storage/bronze/tedp_raw_monthly) | 47 phân vùng tháng (Checkpoint tự động bỏ qua khi chạy lại) |
| **Bronze — TEDP Raw Pooled** | [`lakehouse_storage/bronze/tedp_aqi_pm25_hourly.csv`](file:///d:/năm%204/big%20data/Final_Project_Plan/lakehouse_storage/bronze/tedp_aqi_pm25_hourly.csv) | **474.286 dòng × 11 cột** (28 trạm thô thuộc 14 tỉnh) |
| **Bronze/Silver — Cleaned Station** | [`lakehouse_storage/bronze/cleaned_tedp.csv`](file:///d:/năm%204/big%20data/Final_Project_Plan/lakehouse_storage/bronze/cleaned_tedp.csv) | **474.286 dòng × 29 cột** (Đã lọc ngưỡng QCVN & gắn cờ cấp trạm) |
| **Silver — TEDP Province (3 năm)** | [`data_test/cleaned_tedp_province_4y.csv`](file:///d:/năm%204/big%20data/Final_Project_Plan/data_test/cleaned_tedp_province_4y.csv) | 11 tỉnh × 25.512 giờ = **280.632 dòng × 30 cột** |
| **Silver — Open-Meteo Province** | [`data_test/weather_openmeteo_province_4y.csv`](file:///d:/năm%204/big%20data/Final_Project_Plan/data_test/weather_openmeteo_province_4y.csv) | 11 tỉnh × 25.512 giờ = **280.632 dòng × 19 cột** |
| **Metadata — Trạm thành viên** | [`data_test/province_station_members_4y.csv`](file:///d:/năm%204/big%20data/Final_Project_Plan/data_test/province_station_members_4y.csv) | **17 dòng × 8 cột** (17 trạm vượt bộ lọc thuộc 11 tỉnh) |
| **Metadata — Báo cáo độ phủ** | [`data_test/province_coverage_4y.csv`](file:///d:/năm%204/big%20data/Final_Project_Plan/data_test/province_coverage_4y.csv) | **14 dòng × 12 cột** (Thống kê độ phủ 14 tỉnh ban đầu) |
| **Serving — Dự báo thời tiết 24h** | [`data_test/du_bao/openmeteo_forecast_next_24h_11provinces.csv`](file:///d:/năm%204/big%20data/Final_Project_Plan/data_test/du_bao/openmeteo_forecast_next_24h_11provinces.csv) | 11 tỉnh × 25 mốc giờ = **275 dòng × 22 cột** (Trung bình từ 17 trạm) |

### Khoảng thời gian đồng bộ chuẩn (Auto-Aligned Window)

```text
2023-12-11 00:00:00 -> 2026-11-07 23:00:00 (25.512 giờ = 1.063 ngày ~ 2,91 năm)
```

* **Giải thích cơ chế Auto-Align trong [`sync_tedp_province.py`](file:///d:/năm%204/big%20data/Final_Project_Plan/code/crawl_data/sync_tedp_province.py):**
  Mặc định tham số cấu hình đặt `SYNC_YEARS = 4` (gắn hậu tố `_4y`), nhưng trên máy chủ CEM/TEDP, phần lớn các tỉnh chủ lực (Hà Nội, Gia Lai, Lâm Đồng, TP.HCM, Đà Nẵng...) chỉ bắt đầu có dữ liệu đo thật đồng loạt từ cuối tháng 11 và đầu tháng 12/2023. Để **không nội suy khống cả năm 2022–2023 khi máy chủ chưa có dữ liệu**, thuật toán tự động căn chỉnh mốc bắt đầu (`aligned_start`) về `2023-12-11 00:00:00` — ngày đầu tiên toàn bộ 11 tỉnh chủ lực đều đã vận hành ổn định.

### Bản chất và Đơn vị của biến mục tiêu `pm25`

* Dữ liệu thu thập từ endpoint `/api/aqi_hour` của CEM/TEDP trả về trường `PM205` (được ánh xạ vào cột `pm25`).
* Đây là **Chỉ số chất lượng không khí thành phần của bụi mịn PM2.5 (`I_PM2.5`, thang điểm 0–500 không thứ nguyên theo Quyết định 1459/QĐ-TCMT)**, không phải nồng độ khối lượng thô (`ug/m3`).
* Mô hình học máy học và dự báo trực tiếp cột **`pm25` (`PM205`)** thay vì cột `aqi` tổng hợp, vì `aqi` tổng hợp là giá trị cực đại của nhiều chất ô nhiễm khác nhau, trong khi `PM205` phản ánh thuần túy động lực học vật lý của hạt bụi mịn `PM2.5`.

---

## 3. Kiến trúc các tầng dữ liệu (Medallion Architecture)

### 3.1. Bronze — Raw Storage (`lakehouse_storage/bronze/`)
* Lưu trữ dữ liệu thô bất biến từ API CEM/TEDP trong `tedp_raw_monthly/tedp_YYYY-MM.csv` và gộp thành `tedp_aqi_pm25_hourly.csv`.
* Chỉ giữ 11 cột cốt lõi:
  ```text
  province_id, province_name, station_id, station_code, station_name,
  latitude, longitude, datetime, pm25, aqi, pm10
  ```

### 3.2. Silver — Cleaned & Province-Aggregated (`data_test/`)
Thực hiện qua 2 giai đoạn liên hoàn khi chạy [`code/crawl_data/preprocess_tedp.py`](file:///d:/năm%204/big%20data/Final_Project_Plan/code/crawl_data/preprocess_tedp.py):
1. **Giai đoạn 1 — Làm sạch cấp trạm (`cleaned_tedp.csv`):**
   - Ép kiểu định danh chuỗi 29 chữ số (`dtype=str` cho `province_id`, `station_id`, `station_code`).
   - Xóa trùng lặp `(station_id, datetime)` và lọc ngưỡng vật lý QCVN.
   - Nội suy tuyến tính tối đa 3 giờ liên tiếp (`limit=3`), gắn cờ `*_imputed = 1`.
   - Các khoảng trống dài `> 3h` được điền tạm bằng trung vị trạm nhưng **bắt buộc gắn cờ `*_median_filled = 1`** để loại trừ khi tính trung bình tỉnh.
2. **Giai đoạn 2 — Lọc trạm, Lọc tỉnh & Gộp nút cấp tỉnh (`sync_tedp_province.py`):**
   - **Bước 2a (Khôi phục số đo thật):** Chuyển toàn bộ các ô có `*_median_filled == 1` về `NaN` để giá trị giả định không làm sai lệch trung bình tỉnh.
   - **Bước 2b (Lọc trạm lắp muộn):** Trong một tỉnh có nhiều trạm, nếu tỉnh đã có trạm dài hạn đạt chuẩn (`>= 80%`) thì loại bỏ các trạm mới lắp muộn có độ phủ `< 80%` (`STATION_MIN_COVERAGE = 0.8`) để tránh hiện tượng nhảy bậc chuỗi trung bình.
   - **Bước 2c (Lọc tỉnh theo độ phủ):** Chỉ giữ lại các tỉnh có tỷ lệ giờ có ít nhất 1 trạm đo thật `pm25 >= 80%` (`PROVINCE_MIN_COVERAGE = 0.8`). Kết quả giữ **11 tỉnh (17 trạm thành viên)** và loại 3 tỉnh lắp đặt muộn năm 2026 (`Thái Nguyên`, `HCM KV3`, `Quảng Ninh`).
   - **Bước 2d (Trung bình theo tỉnh & Trọng tâm tọa độ):** Tại mỗi giờ, giá trị `pm25, pm10, aqi` của tỉnh bằng trung bình cộng (`mean`, bỏ qua `NaN`) của các trạm thành viên có số đo thật tại giờ đó. Tọa độ `(latitude, longitude)` của nút tỉnh là trung bình cộng tọa độ của các trạm thành viên.
   - **Bước 2e (Nội suy lưới giờ cấp tỉnh):** Đưa mỗi tỉnh về lưới chuẩn `25.512` giờ; các giờ không có trạm nào đo được nội suy theo thời gian và gắn cờ `is_inserted = 1`, `pm25_imputed = 1`, đồng thời ghi nhận độ dài khoảng trống vào cột `pm25_gap_hours`.

### 3.3. Gold & Serving — Modeling Features & 24h Real-Time Forecast (`output/global_ann_4y/` & `data_test/du_bao/`)
* Tạo 49 đặc trưng không gian - thời gian cho mô hình `Spatio-Temporal Global ANN`.
* Thu thập dự báo thời tiết 24h tới tại tọa độ của đúng 17 trạm thành viên, gộp trung bình về 11 tỉnh (`openmeteo_forecast_next_24h_11provinces.csv`) và xuất dự báo `PM2.5` 24h tới cho từng tỉnh (`ha_noi.csv`, `hcm.csv`...).

---

## 4. Quy chuẩn Schema chi tiết tại Tầng Silver

### 4.1. Danh sách 11 Tỉnh và 17 Trạm thành viên (`province_station_members_4y.csv`)

| STT | Tỉnh/Thành (`province_name`) | Mã nút (`station_code`) | Số trạm | Mã các trạm thành viên (`station_codes`) | Tọa độ trọng tâm (`lat, lon`) | Độ phủ đo thật `PM2.5` |
|:---:|:---|:---|:---:|:---|:---:|:---:|
| 1 | **Đà Nẵng** | `PROV_DA_NANG` | 2 | `DNa_NGLB_KHIKXQ; QNa_TAKY_KHIKXQ` | `15.8122, 108.3231` | `99.9%` |
| 2 | **Bắc Ninh** | `PROV_BAC_NINH` | 1 | `BG_XUGI_KHIKXQ` | `21.3015, 106.2260` | `99.8%` |
| 3 | **Hưng Yên** | `PROV_HUNG_YEN` | 2 | `HY_NGVL_KHIKXQ; TB_TRTT_KHIKXQ` | `20.5598, 106.2012` | `99.7%` |
| 4 | **HCM** | `PROV_HCM` | 4 | `BD_DLBD_KHIKXQ; BV_BRVT_KHIKXQ; HCM_LYCT_KHIKXQ; HCM_THDI_KHIKXQ` | `10.7633, 106.8421` | `99.4%` |
| 5 | **Hà Nội** | `PROV_HA_NOI` | 2 | `HN_BKHN_KHIKXQ; HN_CVTX_KHIKXQ` | `21.0042, 105.8183` | `99.4%` |
| 6 | **Quảng Ngãi** | `PROV_QUANG_NGAI` | 1 | `QNg_NGNG_KHIKXQ` | `15.1210, 108.8041` | `99.3%` |
| 7 | **Lâm Đồng** | `PROV_LAM_DONG` | 1 | `LD_DALA_KHIKXQ` | `11.9527, 108.4298` | `99.1%` |
| 8 | **Tây Ninh** | `PROV_TAY_NINH` | 1 | `LA_TAAN_KHIKXQ` | `10.5391, 106.4045` | `98.2%` |
| 9 | **Gia Lai** | `PROV_GIA_LAI` | 1 | `BDi_QNBD_KHIKXQ` | `13.7851, 109.2198` | `98.1%` |
| 10 | **Ninh Bình** | `PROV_NINH_BINH` | 1 | `HNa_CVNC_KHIKXQ` | `20.5360, 105.9165` | `96.1%` |
| 11 | **Hải Phòng** | `PROV_HAI_PHONG` | 1 | `HD_NGLB_KHIKXQ` | `20.9381, 106.3357` | `84.8%` |

### 4.2. Schema 30 cột của `data_test/cleaned_tedp_province_4y.csv`

```text
1. Định danh & Không gian (8 cột):
   province_id, province_name, station_id, station_code, station_name,
   latitude, longitude, datetime

2. Chỉ số Ô nhiễm Cốt lõi (3 cột):
   pm25, aqi, pm10

3. Cờ Nội suy & Chất lượng Nút Tỉnh (7 cột):
   pm25_imputed, pm10_imputed, aqi_imputed,
   is_inserted, n_stations_member, n_stations_valid_pm25, pm25_gap_hours

4. Đặc trưng Thời gian & Phân loại AQI (12 cột):
   year, month, day, hour, day_of_week, day_name, is_weekend,
   date, time_slot, aqi_category, province_extracted, station_short_name
```

### 4.3. Schema 19 cột của `data_test/weather_openmeteo_province_4y.csv`

```text
1. Định danh & Lưới tọa độ (10 cột):
   station_id, station_code, station_name, province_name,
   latitude, longitude, grid_latitude, grid_longitude, elevation, datetime

2. Biến Khí tượng Theo giờ (8 cột):
   temperature_2m, relative_humidity_2m, dew_point_2m,
   surface_pressure, precipitation, wind_speed_10m,
   wind_direction_10m, boundary_layer_height

3. Nguồn dữ liệu (1 cột):
   weather_source (archive / archive+forecast_fill)
```

---

## 5. Kiểm tra tính hợp lý & Ngưỡng vật lý (Hard Bounds)

| Biến | Khoảng hợp lệ | Xử lý khi vượt ngưỡng |
|---|---|---|
| `pm25` | `0 – 500` | Chuyển thành `NaN` trước khi nội suy |
| `pm10` | `0 – 600` | Chuyển thành `NaN` trước khi nội suy |
| `aqi` | `0 – 500` | Chuyển thành `NaN` trước khi nội suy |
| `relative_humidity_2m` | `0 – 100` (%) | Kiểm tra từ nguồn Open-Meteo |
| `precipitation` | `>= 0` (mm) | Kiểm tra từ nguồn Open-Meteo |
| `wind_speed_10m` | `>= 0` (`m/s`) | Thống nhất đơn vị `ms` (`m/s`) trên toàn bộ script |

---

## 6. Kỹ thuật Đặc trưng Không gian - Thời gian (Feature Engineering — Zero Data Leakage)

Trong [`code/training/train_global_ann.py`](file:///d:/năm%204/big%20data/Final_Project_Plan/code/training/train_global_ann.py) và [`code/training/du_bao.py`](file:///d:/năm%204/big%20data/Final_Project_Plan/code/training/du_bao.py), mô hình sử dụng **49 đặc trưng đầu vào** để dự báo `y = pm25(t + 24h)` từ thời điểm gốc `t`:

1. **Nhóm Độ trễ, Trung bình trượt & Biến động Ô nhiễm tại thời điểm `t` (13 đặc trưng):**
   - 6 độ trễ `PM2.5`: `pm25_lag0, pm25_lag1, pm25_lag3, pm25_lag6, pm25_lag12, pm25_lag24`.
   - 2 trung bình trượt: `pm25_roll6, pm25_roll24`.
   - 3 biến tốc độ thay đổi & độ lệch chuẩn: `pm25_diff_1h`, `pm25_diff_24h`, `pm25_std24`.
   - 2 chỉ số đồng hành tại `t`: `pm10_lag0`, `aqi_lag0`.
2. **Nhóm 8 Biến Khí tượng tại thời điểm mục tiêu `t + 24h` (8 đặc trưng):**
   - `temperature_2m`, `relative_humidity_2m`, `dew_point_2m`, `surface_pressure`, `precipitation`, `wind_speed_10m`, `wind_direction_10m`, `boundary_layer_height`.
3. **Nhóm 8 Đặc trưng Nhiệt động lực học & Biến thiên 24h (8 đặc trưng):**
   - `wind_dir_sin`, `wind_dir_cos` (biểu diễn tuần hoàn hướng gió).
   - `dew_point_deficit` (`temperature_2m - dew_point_2m`).
   - `ventilation_coef` (`wind_speed_10m * boundary_layer_height`).
   - `temp_diff_24h`, `pressure_diff_24h`, `pblh_diff_24h`, `precip_sum_24h`.
4. **Nhóm Chu kỳ Thời gian & Tọa độ Chuẩn hóa (9 đặc trưng):**
   - `hour, day_of_week, is_weekend, hour_sin, hour_cos, dow_sin, dow_cos`.
   - `latitude_norm, longitude_norm` (chuẩn hóa về `[-1, 1]`).
5. **Nhóm Định danh Nút Tỉnh One-Hot (11 đặc trưng):**
   - `station_id_ID` cho 11 tỉnh (kết hợp với tầng `station_embedding` 16 chiều và ma trận `Spatial Prior Matrix` dựa trên khoảng cách địa lý Haversine).

---

## 7. Chia tập Train / Validation / Test & Chuẩn hóa (Normalization)

1. **Chia theo trục thời gian (`Chronological Split 70 / 10 / 20`):**
   - Tổng số mẫu hợp lệ sau khi tạo lag 24h và nhãn `t+24h`: **`279.840` mẫu** (`11 tỉnh × 25.440 mẫu`).
   - **Tập Train (`70%`):** `195.888` mẫu (kết thúc tại `2025-12-23 23:00:00`).
   - **Tập Validation (`10%`):** `27.984` mẫu (từ `2025-12-24 00:00:00` đến `2026-04-08 23:00:00`).
   - **Tập Test (`20%`):** `55.968` mẫu (từ `2026-04-09 00:00:00` đến hết chuỗi).
2. **Quy tắc Chuẩn hóa (Z-score Scaling):**
   - Bộ `StandardScaler` (`scaler_mean`, `scaler_scale`) **chỉ được `fit` trên tập Train** của 38 biến liên tục, sau đó áp dụng biến đổi cho Validation, Test và dữ liệu dự báo thời gian thực trong `du_bao.py`.
   - `scaler_mean` và `scaler_scale` được đóng gói trực tiếp vào bên trong file checkpoint `output/global_ann_4y/best_ann_model.pt` để đảm bảo tính nhất quán tuyệt đối khi suy luận.

---

## 8. Kết quả Thực nghiệm & Kiểm thử Chất lượng Đã Hoàn thành

1. **Kiểm định đồng bộ dữ liệu Silver:**
   - `11` tỉnh, mỗi tỉnh đúng `25.512` giờ, cùng tập timestamp, `0` giá trị `NaN` ở các cột bắt buộc (`pm25, pm10, aqi` và 8 biến thời tiết).
2. **Kết quả huấn luyện `Spatio-Temporal Global ANN` (`output/global_ann_4y/metrics.json`):**
   - **Validation:** `RMSE = 25.54` (so với Baseline `31.84`), `MAE = 17.84`, `R2 = 0.649`.
   - **Test:** `RMSE = 17.68` (so với Baseline `21.17`, **cải thiện `+16.50%`**), `MAE = 12.35`, `R2 = 0.586`.
   - **Số tỉnh vượt Baseline 24h trên tập Test:** **`11/11` tỉnh thành (100%)**.
   - **Tỷ trọng đóng góp theo nhóm đặc trưng (Gradient × Input Saliency):**
     - Độ trễ & lịch sử ô nhiễm (`pm25_lags_and_pollutants`): **`53.61%`**
     - Khí tượng & Nhiệt động lực học (`weather_and_thermodynamics`): **`32.26%`**
     - Chu kỳ thời gian & Tọa độ không gian (`time_and_spatial_coords`): **`12.41%`**
     - Định danh trạm/tỉnh (`station_id_identity`): **`1.72%`**
