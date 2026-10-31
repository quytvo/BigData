"""
Tải dữ liệu thời tiết theo giờ từ Open-Meteo (thư viện openmeteo-requests)
cho từng TRẠM quan trắc có trong cleaned_tedp.csv (theo đúng toạ độ trạm),
đồng bộ khung thời gian với dữ liệu TEDP.

Biến lấy về (hourly):
    - temperature_2m        : nhiệt độ 2m (°C)
    - relative_humidity_2m  : độ ẩm tương đối 2m (%)
    - surface_pressure      : áp suất khí quyển bề mặt (hPa)
    - precipitation         : lượng mưa (mm)
    - wind_speed_10m        : tốc độ gió 10m (m/s)

Cài đặt:
    pip install openmeteo-requests pandas

Chạy:
    python code/fetch_openmeteo_weather.py
"""

import io
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

# Số ngày cần tải: 4 tháng (120 ngày = 2880 giờ hoặc 122 ngày nếu từ 01/06 -> 30/09)
N_DAYS = 120

# Ngày bắt đầu tải thời tiết (None = tự động lấy t_min từ TEDP_CSV; nếu đặt "2026-06-01" sẽ lấy 4 tháng từ 01/06)
START_DATE = "2026-06-01"

# Tìm file TEDP để lấy thông tin toạ độ các trạm
DATA_TEST_DIR = BASE_DIR / "data_test"
DATA_TEST_DIR.mkdir(parents=True, exist_ok=True)
TEDP_CANDIDATES = [
    DATA_TEST_DIR / f"cleaned_tedp_synced_{N_DAYS}d.csv",
    BASE_DIR / f"cleaned_tedp_synced_{N_DAYS}d.csv",
]
TEDP_CSV = next((p for p in TEDP_CANDIDATES if p.exists()), DATA_TEST_DIR / f"cleaned_tedp_synced_{N_DAYS}d.csv")

OUTPUT_CSV = DATA_TEST_DIR / f"weather_openmeteo_station_{N_DAYS}d.csv"

# Dữ liệu TEDP ghi theo giờ địa phương Việt Nam (UTC+7)
TIMEZONE = "Asia/Bangkok"

# Historical Forecast API: dữ liệu có gần như tới hiện tại (phù hợp vì TEDP kết thúc rất gần hôm nay).
# Nếu muốn dùng tái phân tích ERA5 (trễ ~5 ngày) thì đổi sang:
#   "https://archive-api.open-meteo.com/v1/archive"
API_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"

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

MAX_RETRIES = 3


# ============================================================
# 1. ĐỌC TEDP: LẤY KHUNG THỜI GIAN + DANH SÁCH TRẠM (TOẠ ĐỘ RIÊNG)
# ============================================================

STATION_COLS = ["station_id", "station_code", "station_name", "province_name", "latitude", "longitude"]


def load_tedp_info(csv_path):
    df = pd.read_csv(
        csv_path,
        usecols=STATION_COLS + ["datetime"],
        dtype={"station_id": str},
        parse_dates=["datetime"],
    )

    t_min = df["datetime"].min()
    t_max = df["datetime"].max()

    # Mỗi trạm 1 dòng với toạ độ thật của trạm
    stations = (
        df[STATION_COLS]
        .drop_duplicates("station_code")
        .sort_values("station_code")
        .reset_index(drop=True)
    )

    return stations, t_min, t_max


# ============================================================
# 2. GỌI OPEN-METEO (NHIỀU TOẠ ĐỘ TRONG 1 REQUEST)
# ============================================================

def fetch_weather(stations, start_date, end_date):
    client = openmeteo_requests.Client()

    params = {
        "latitude": stations["latitude"].round(6).tolist(),
        "longitude": stations["longitude"].round(6).tolist(),
        "start_date": start_date,
        "end_date": end_date,
        "hourly": HOURLY_VARS,
        "timezone": TIMEZONE,
        "wind_speed_unit": "ms",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return client.weather_api(API_URL, params=params)
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] Lần {attempt}/{MAX_RETRIES} thất bại: {exc}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(5 * attempt)


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


# ============================================================
# MAIN
# ============================================================

def main():
    stations, tedp_min, tedp_max = load_tedp_info(TEDP_CSV)
    print(f"File trạm: {TEDP_CSV}")
    print(f"Khung thời gian TEDP trong file: {tedp_min} -> {tedp_max}")

    # Cửa sổ N_DAYS ngày (nếu có START_DATE thì tính từ START_DATE, ngược lại lấy từ tedp_min)
    if START_DATE:
        t_min = pd.Timestamp(START_DATE).normalize()
    else:
        t_min = tedp_min.normalize()
    t_max = t_min + pd.Timedelta(days=N_DAYS) - pd.Timedelta(hours=1)

    start_date = t_min.strftime("%Y-%m-%d")
    end_date = t_max.strftime("%Y-%m-%d")

    print(f"Khung thời gian tải: {t_min} -> {t_max} ({N_DAYS} ngày)")
    print(f"Số trạm: {len(stations)} | Số province: {stations['province_name'].nunique()}")

    responses = fetch_weather(stations, start_date, end_date)

    # Responses trả về đúng thứ tự toạ độ đã gửi
    frames = [
        response_to_df(resp, stations.iloc[i])
        for i, resp in enumerate(responses)
    ]
    weather = pd.concat(frames, ignore_index=True)

    # Cắt đúng khung giờ
    weather = weather[(weather["datetime"] >= t_min) & (weather["datetime"] <= t_max)]
    weather = weather.sort_values(["station_code", "datetime"]).reset_index(drop=True)

    # Kiểm tra mỗi trạm đủ N_DAYS * 24 giờ
    expected = N_DAYS * 24
    counts = weather.groupby("station_code")["datetime"].nunique()
    if (counts == expected).all():
        print(f"[OK] Mỗi trạm đủ {expected} giờ")
    else:
        print(f"[WARN] Trạm không đủ {expected} giờ:\n{counts[counts != expected].to_string()}")

    weather["datetime"] = weather["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

    weather.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print(f"Đã lưu {len(weather)} dòng -> {OUTPUT_CSV}")
    print("Số giá trị thiếu theo biến:")
    print(weather[HOURLY_VARS].isna().sum().to_string())


if __name__ == "__main__":
    main()
