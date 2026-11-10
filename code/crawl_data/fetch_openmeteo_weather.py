"""
Tải dữ liệu thời tiết theo giờ từ Open-Meteo (thư viện openmeteo-requests)
cho từng NÚT trong file TEDP đã đồng bộ (bản trung bình tỉnh: 1 nút = 1 tỉnh,
toạ độ = trọng tâm các trạm thành viên), đồng bộ khung thời gian với TEDP.

Khung thời gian: đọc trực tiếp từ file TEDP (t_min -> t_max) -> luôn khớp với
cửa sổ 4 năm đã chọn ở sync_tedp_province.py.

Nguồn (WEATHER_API):
    - "archive" (mặc định): Historical Weather API = tái phân tích ERA5 — cùng
      họ dữ liệu với KnowAir [REF: previous_project]. ERA5 trễ ~5 ngày nên các
      giờ cuối còn thiếu sẽ được bù từ Historical Forecast API (cột weather_source
      ghi rõ nguồn từng dòng).
    - "forecast": Historical Forecast API (nguồn đã dùng cho bản 120 ngày).

Biến lấy về (hourly):
    - temperature_2m        : nhiệt độ 2m (°C)
    - relative_humidity_2m  : độ ẩm tương đối 2m (%)
    - dew_point_2m          : điểm sương 2m (°C)
    - surface_pressure      : áp suất khí quyển bề mặt (hPa)
    - precipitation         : lượng mưa (mm)
    - wind_speed_10m        : tốc độ gió 10m (m/s)
    - wind_direction_10m    : hướng gió 10m (°)
    - boundary_layer_height : độ cao lớp biên (m)

Cài đặt:
    pip install openmeteo-requests pandas

Chạy:
    python code/crawl_data/fetch_openmeteo_weather.py
"""

import io
import os
import sys
import time
from pathlib import Path

# Fix encoding cho terminal Windows (cp1258 khong ho tro unicode tieng Viet)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import openmeteo_requests
import pandas as pd

# ============================================================
# CẤU HÌNH
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_TEST_DIR = BASE_DIR / "data_test"
DATA_TEST_DIR.mkdir(parents=True, exist_ok=True)

# Hậu tố bộ dữ liệu (phải trùng SYNC_TAG của sync_tedp_province.py)
TAG = os.getenv("SYNC_TAG", "4y")
TEDP_CSV = DATA_TEST_DIR / f"cleaned_tedp_province_{TAG}.csv"
OUTPUT_CSV = DATA_TEST_DIR / f"weather_openmeteo_province_{TAG}.csv"

# Dữ liệu TEDP ghi theo giờ địa phương Việt Nam (UTC+7)
TIMEZONE = "Asia/Bangkok"

WEATHER_API = os.getenv("WEATHER_API", "archive").lower()
API_URLS = {
    "archive": "https://archive-api.open-meteo.com/v1/archive",
    "forecast": "https://historical-forecast-api.open-meteo.com/v1/forecast",
}
# Bù các giờ còn thiếu của ERA5 (vài ngày cuối) bằng Historical Forecast API
FILL_FROM_FORECAST = os.getenv("WEATHER_FILL_FROM_FORECAST", "1") == "1"

# Thứ tự biến QUAN TRỌNG: phải giống thứ tự đọc Variables(i) bên dưới
HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "surface_pressure",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "boundary_layer_height",
]

# 4 năm x nhiều nút trong 1 request sẽ vượt giới hạn "weighted calls" của gói
# free -> chia request theo (nút, khối CHUNK_DAYS ngày) và nghỉ giữa các request.
CHUNK_DAYS = int(os.getenv("WEATHER_CHUNK_DAYS", "366"))
REQUEST_SLEEP = float(os.getenv("WEATHER_REQUEST_SLEEP", "2"))
MAX_RETRIES = 4


# ============================================================
# 1. ĐỌC TEDP: LẤY KHUNG THỜI GIAN + DANH SÁCH NÚT (TOẠ ĐỘ)
# ============================================================

STATION_COLS = ["station_id", "station_code", "station_name", "province_name", "latitude", "longitude"]


def load_tedp_info(csv_path):
    if not Path(csv_path).exists():
        raise FileNotFoundError(f"Không tìm thấy {csv_path} -> chạy sync_tedp_province.py trước")

    df = pd.read_csv(
        csv_path,
        usecols=STATION_COLS + ["datetime"],
        dtype={"station_id": str},
        parse_dates=["datetime"],
    )

    t_min = df["datetime"].min()
    t_max = df["datetime"].max()

    # Mỗi nút 1 dòng với toạ độ của nút
    stations = (
        df[STATION_COLS]
        .drop_duplicates("station_code")
        .sort_values("station_code")
        .reset_index(drop=True)
    )

    return stations, t_min, t_max


# ============================================================
# 2. GỌI OPEN-METEO (1 NÚT x 1 KHỐI THỜI GIAN / REQUEST)
# ============================================================

client = openmeteo_requests.Client()


def fetch_one(api, lat, lon, start_date, end_date):
    params = {
        "latitude": round(float(lat), 6),
        "longitude": round(float(lon), 6),
        "start_date": start_date,
        "end_date": end_date,
        "hourly": HOURLY_VARS,
        "timezone": TIMEZONE,
        "wind_speed_unit": "ms",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return client.weather_api(API_URLS[api], params=params)[0]
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            wait = 65 if "limit" in msg.lower() else 5 * attempt
            print(f"    [WARN] Lần {attempt}/{MAX_RETRIES} thất bại: {msg[:150]} -> chờ {wait}s")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(wait)


def date_chunks(t_min, t_max):
    cur = t_min.normalize()
    last = t_max.normalize()
    while cur <= last:
        chunk_end = min(cur + pd.Timedelta(days=CHUNK_DAYS - 1), last)
        yield cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
        cur = chunk_end + pd.Timedelta(days=1)


# ============================================================
# 3. CHUYỂN RESPONSE -> DATAFRAME
# ============================================================

def response_to_df(response, station_row):
    hourly = response.Hourly()
    utc_offset = pd.Timedelta(seconds=response.UtcOffsetSeconds())

    # Time() trả về unix UTC -> cộng offset để ra giờ địa phương, bỏ tz cho khớp TEDP
    time_index = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )
    local_time = (time_index + utc_offset).tz_localize(None)

    data = {"datetime": local_time}
    for i, var in enumerate(HOURLY_VARS):
        data[var] = hourly.Variables(i).ValuesAsNumpy()

    out = pd.DataFrame(data)
    for pos, col in enumerate(STATION_COLS):
        out.insert(pos, col, station_row[col])
    n = len(STATION_COLS)
    out.insert(n, "grid_latitude", response.Latitude())
    out.insert(n + 1, "grid_longitude", response.Longitude())
    out.insert(n + 2, "elevation", response.Elevation())
    return out


def fetch_station(api, station_row, t_min, t_max):
    frames = []
    for s, e in date_chunks(t_min, t_max):
        print(f"  [{api}] {station_row['station_code']}: {s} -> {e}")
        resp = fetch_one(api, station_row["latitude"], station_row["longitude"], s, e)
        frames.append(response_to_df(resp, station_row))
        time.sleep(REQUEST_SLEEP)
    out = pd.concat(frames, ignore_index=True)
    out = out[(out["datetime"] >= t_min) & (out["datetime"] <= t_max)]
    return out.drop_duplicates(["station_code", "datetime"])


def fill_missing_from_forecast(weather, stations):
    """Bù các ô NaN ở 7 biến bề mặt (thường là vài ngày cuối ERA5 chưa phát hành) từ Historical Forecast API."""
    weather = weather.copy()
    if "weather_source" not in weather.columns:
        weather["weather_source"] = WEATHER_API
    else:
        weather["weather_source"] = weather["weather_source"].fillna(WEATHER_API)

    surface_vars = [v for v in HOURLY_VARS if v != "boundary_layer_height"]
    nan_rows = weather[surface_vars].isna().any(axis=1)
    if not nan_rows.any():
        return weather

    print(f"\nCó {int(nan_rows.sum()):,} dòng thiếu biến bề mặt từ {WEATHER_API} -> bù từ Historical Forecast API")
    for code, g in weather[nan_rows].groupby("station_code"):
        row = stations[stations["station_code"] == code].iloc[0]
        fc = fetch_station("forecast", row, g["datetime"].min(), g["datetime"].max())
        fc = fc.set_index("datetime")[HOURLY_VARS]

        idx = weather.index[(weather["station_code"] == code) & nan_rows]
        for i in idx:
            t = weather.at[i, "datetime"]
            if t not in fc.index:
                continue
            for var in HOURLY_VARS:
                if pd.isna(weather.at[i, var]):
                    weather.at[i, var] = fc.at[t, var]
            weather.at[i, "weather_source"] = f"{WEATHER_API}+forecast_fill"
    return weather


# ============================================================
# MAIN
# ============================================================

def main():
    if WEATHER_API not in API_URLS:
        raise ValueError(f"WEATHER_API phải là một trong {list(API_URLS)}")

    stations, t_min, t_max = load_tedp_info(TEDP_CSV)
    n_hours = int((t_max - t_min) / pd.Timedelta(hours=1)) + 1

    print(f"File TEDP: {TEDP_CSV}")
    print(f"Khung thời gian tải: {t_min} -> {t_max} ({n_hours:,} giờ ~ {n_hours / 24 / 365.25:.2f} năm)")
    print(f"Nguồn: {WEATHER_API} ({API_URLS[WEATHER_API]})")
    print(f"Số nút: {len(stations)} | Số tỉnh: {stations['province_name'].nunique()}")

    frames = []
    done_codes = set()
    if OUTPUT_CSV.exists():
        try:
            existing_df = pd.read_csv(
                OUTPUT_CSV,
                dtype={"station_id": str, "station_code": str},
                parse_dates=["datetime"],
            )
            existing_df = existing_df[
                (existing_df["datetime"] >= t_min) & (existing_df["datetime"] <= t_max)
            ].drop_duplicates(["station_code", "datetime"])
            cnts = existing_df.groupby("station_code")["datetime"].nunique()
            done_codes = set(cnts[cnts == n_hours].index)
            if done_codes:
                print(f"\n[RESUME] Giữ lại {len(done_codes)} tỉnh đã có đủ {n_hours:,} giờ trong {OUTPUT_CSV.name}: {sorted(done_codes)}")
                frames.append(existing_df[existing_df["station_code"].isin(done_codes)].copy())
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] Không đọc được {OUTPUT_CSV.name} cũ ({exc}), sẽ tải lại.")

    for _, row in stations.iterrows():
        if row["station_code"] in done_codes:
            continue
        st_df = fetch_station(WEATHER_API, row, t_min, t_max)
        st_df["weather_source"] = WEATHER_API
        frames.append(st_df)

    weather = pd.concat(frames, ignore_index=True)

    if WEATHER_API == "archive" and FILL_FROM_FORECAST:
        weather = fill_missing_from_forecast(weather, stations)
    elif "weather_source" not in weather.columns:
        weather["weather_source"] = WEATHER_API

    weather = weather.sort_values(["station_code", "datetime"]).reset_index(drop=True)

    # Kiểm tra mỗi nút đủ số giờ
    counts = weather.groupby("station_code")["datetime"].nunique()
    if (counts == n_hours).all():
        print(f"[OK] Mỗi nút đủ {n_hours:,} giờ")
    else:
        print(f"[WARN] Nút không đủ {n_hours:,} giờ:\n{counts[counts != n_hours].to_string()}")

    # Lưu an toàn qua file tạm (.tmp) để bấm Ctrl+C không làm cụt file CSV
    out_save = weather.copy()
    out_save["datetime"] = out_save["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    tmp_csv = OUTPUT_CSV.with_suffix(".csv.tmp")
    out_save.to_csv(tmp_csv, index=False, encoding="utf-8-sig")
    tmp_csv.replace(OUTPUT_CSV)

    print(f"\nĐã lưu {len(weather):,} dòng -> {OUTPUT_CSV}")

    # Nếu còn thiếu boundary_layer_height và đã có file NetCDF trong era5_cds_blh -> tự động điền luôn
    if int(weather["boundary_layer_height"].isna().sum()) > 0:
        current_dir = Path(__file__).resolve().parent
        if str(current_dir) not in sys.path:
            sys.path.insert(0, str(current_dir))
        import fill_era5_blh_cds
        fill_era5_blh_cds.main()
    else:
        print("Số giá trị thiếu theo biến (phải = 0 thì training mới chạy):")
        print(weather[HOURLY_VARS].isna().sum().to_string())


if __name__ == "__main__":
    main()
