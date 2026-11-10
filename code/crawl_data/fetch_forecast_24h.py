# -*- coding: utf-8 -*-
"""
fetch_forecast_24h.py
=====================
Thu thập dữ liệu dự báo thời tiết theo giờ (24h tới) từ Open-Meteo Forecast API
theo quy trình đồng bộ với `preprocess_tedp.py` và `sync_tedp_province.py`:

1. Chỉ lấy danh sách các trạm quan trắc đã vượt qua bộ lọc của `preprocess_tedp.py`
   và `sync_tedp_province.py` (17 trạm thuộc 11 tỉnh/thành phố trong
   `data_test/province_station_members_4y.csv` hoặc trích xuất trực tiếp từ
   `lakehouse_storage/bronze/cleaned_tedp.csv`).
2. Gọi Open-Meteo Forecast API theo tọa độ thực tế (`latitude`, `longitude`)
   của từng trạm thành viên.
3. Tính trung bình cộng các chỉ số thời tiết của các trạm thành viên theo từng
   tỉnh và từng mốc giờ (`groupby(["node_id", "province_name", "datetime"]).mean()`),
   đồng thời tính trọng tâm tọa độ tỉnh giống hệt `sync_tedp_province.py`.
4. Tổng hợp và lưu duy nhất 1 file CSV dự báo tại thư mục `data_test/du_bao/`:
   `data_test/du_bao/openmeteo_forecast_next_24h_11provinces.csv`
   (tự động dọn dẹp các file dự báo rời rạc cũ nếu có).

Chạy:
    python code/crawl_data/fetch_forecast_24h.py
"""

import io
import os
import sys
import time
from pathlib import Path

# Fix encoding trên Windows terminal
if hasattr(sys.stdout, "buffer") and getattr(sys.stdout, "encoding", "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import pandas as pd
import requests

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

import sync_tedp_province as stp

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_TEST_DIR = BASE_DIR / "data_test"
OUT_DIR = DATA_TEST_DIR / "du_bao"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TAG = os.getenv("SYNC_TAG", "4y")
MEMBERS_CSV = DATA_TEST_DIR / f"province_station_members_{TAG}.csv"
CLEANED_TEDP_CSV = BASE_DIR / "lakehouse_storage" / "bronze" / "cleaned_tedp.csv"

# File CSV tổng hợp duy nhất ở thư mục dự báo (24h tới) và file cầu nối khí tượng 72h (48h quá khứ + 24h tới)
OUTPUT_FORECAST_CSV = OUT_DIR / "openmeteo_forecast_next_24h_11provinces.csv"
OUTPUT_BRIDGE_72H_CSV = OUT_DIR / "openmeteo_weather_bridge_72h_11provinces.csv"

# Open-Meteo Forecast API endpoint
FORECAST_API_URL = "https://api.open-meteo.com/v1/forecast"
TIMEZONE = "Asia/Bangkok"

# 8 biến khí tượng theo giờ khớp 100% với mô hình huấn luyện
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

MEMBER_COLS = [
    "node_id",
    "province_id",
    "province_name",
    "station_id",
    "station_code",
    "station_name",
    "latitude",
    "longitude",
]

OUTPUT_COLS = [
    "province_id",
    "station_id",
    "station_code",
    "station_name",
    "province_name",
    "latitude",
    "longitude",
    "grid_latitude",
    "grid_longitude",
    "elevation",
    "n_stations_member",
    "n_stations_valid",
    "datetime",
] + HOURLY_VARS + ["weather_source"]


def load_filtered_stations() -> pd.DataFrame:
    """
    Lấy danh sách các trạm quan trắc đã vượt qua bộ lọc của `preprocess_tedp.py`
    và `sync_tedp_province.py`.
    - Ưu tiên đọc từ `data_test/province_station_members_4y.csv` (output của bước lọc).
    - Nếu chưa có file này, tự động chạy hàm lọc trạm/tỉnh của `sync_tedp_province.py`
      trên `lakehouse_storage/bronze/cleaned_tedp.csv`.
    """
    if MEMBERS_CSV.exists():
        df_members = pd.read_csv(
            MEMBERS_CSV,
            dtype={
                "node_id": str,
                "province_id": str,
                "station_id": str,
                "station_code": str,
            },
        )
        df_members = (
            df_members[MEMBER_COLS]
            .drop_duplicates("station_code")
            .sort_values(["province_name", "station_code"])
            .reset_index(drop=True)
        )
        return df_members

    print(f"[INFO] Không tìm thấy {MEMBERS_CSV.name}, đang chạy bộ lọc từ {CLEANED_TEDP_CSV}...")
    df = stp.load_data(CLEANED_TEDP_CSV)
    df = stp.mask_non_measured(df)
    df, _, _, n_hours = stp.cut_window(df)
    df, station_cov = stp.filter_stations(df, n_hours)
    agg = stp.aggregate_province(df)
    cov_df = stp.province_coverage(agg, n_hours, station_cov, df)
    keep = stp.select_provinces(cov_df)
    members, _ = stp.build_members(df, keep)
    return members.reset_index(drop=True)


def fetch_forecast_for_station(row: pd.Series, forecast_days: int = 2, past_days: int = 2) -> pd.DataFrame:
    """Gọi Open-Meteo Forecast API cho tọa độ của 1 trạm quan trắc thành viên (kèm past_days để lấp cầu nối thời tiết)."""
    params = {
        "latitude": round(float(row["latitude"]), 6),
        "longitude": round(float(row["longitude"]), 6),
        "hourly": ",".join(HOURLY_VARS),
        "timezone": TIMEZONE,
        "wind_speed_unit": "ms",
        "forecast_days": forecast_days,
        "past_days": past_days,
    }

    resp = requests.get(FORECAST_API_URL, params=params, timeout=20)
    resp.raise_for_status()
    payload = resp.json()

    hourly = payload["hourly"]
    time_series = pd.to_datetime(hourly["time"])

    data = {"datetime": time_series}
    for var in HOURLY_VARS:
        data[var] = pd.to_numeric(hourly.get(var, [None] * len(time_series)), errors="coerce")

    df_st = pd.DataFrame(data)
    for col in MEMBER_COLS:
        df_st[col] = row[col]

    df_st["grid_latitude"] = float(payload.get("latitude", row["latitude"]))
    df_st["grid_longitude"] = float(payload.get("longitude", row["longitude"]))
    df_st["elevation"] = float(payload.get("elevation", 0.0))
    return df_st


def aggregate_forecast_by_province(station_forecasts: pd.DataFrame, members_df: pd.DataFrame) -> pd.DataFrame:
    """
    Tính trung bình các chỉ số dự báo thời tiết của các trạm thành viên theo từng tỉnh,
    áp dụng đúng quy tắc gộp nút tỉnh của `sync_tedp_province.py`:
      - Trung bình cộng (`mean`, bỏ qua NaN) 8 biến thời tiết theo (`node_id`, `province_name`, `datetime`).
      - Tọa độ tỉnh (`latitude`, `longitude`) = trọng tâm (mean) tọa độ các trạm thành viên.
      - Định danh nút tỉnh:
          + `station_id` = `node_id`
          + `station_code` = `PROV_<SLUG>`
          + `station_name` = `<Tên tỉnh>: Trung bình <N> trạm`
    """
    # Trọng tâm tọa độ và số trạm thành viên của từng tỉnh (giống build_members trong sync_tedp_province.py)
    members_meta = (
        members_df.groupby(["node_id", "province_id", "province_name"], as_index=False)
        .agg(
            latitude=("latitude", "mean"),
            longitude=("longitude", "mean"),
            n_stations_member=("station_code", "nunique"),
        )
    )

    # Trung bình các biến thời tiết + thông tin lưới Open-Meteo theo (node_id, province_name, datetime)
    avg_cols = HOURLY_VARS + ["grid_latitude", "grid_longitude", "elevation"]
    agg_df = (
        station_forecasts.groupby(["node_id", "province_name", "datetime"], as_index=False)[avg_cols]
        .mean()
    )

    # Đếm số trạm có dữ liệu hợp lệ tại mỗi mốc giờ
    valid_cnt = (
        station_forecasts.groupby(["node_id", "province_name", "datetime"])["temperature_2m"]
        .count()
        .rename("n_stations_valid")
        .reset_index()
    )

    out = agg_df.merge(valid_cnt, on=["node_id", "province_name", "datetime"], how="left")
    out = out.merge(members_meta, on=["node_id", "province_name"], how="left")

    # Thiết lập định danh cấp tỉnh giống sync_tedp_province.py
    out["station_id"] = out["node_id"]
    out["station_code"] = out["province_name"].map(lambda name: f"PROV_{stp.slugify(name).upper()}")
    out["station_name"] = out.apply(
        lambda r: f"{r['province_name']}: Trung bình {int(r['n_stations_member'])} trạm",
        axis=1,
    )
    out["weather_source"] = "open_meteo_forecast_station_avg"

    out = out.sort_values(["province_name", "datetime"]).reset_index(drop=True)
    return out[OUTPUT_COLS]


def cleanup_old_forecast_files() -> None:
    """Xóa các file dự báo rời rạc trước đó để chỉ giữ lại 1 file CSV tổng hợp duy nhất."""
    old_patterns = [
        "openmeteo_forecast_48h_all_provinces.csv",
        "kiem_tra_chat_luong_du_bao.txt",
        "*_raw_response.json",
    ]
    removed = []
    for pattern in old_patterns:
        for file_path in OUT_DIR.glob(pattern):
            try:
                file_path.unlink()
                removed.append(file_path.name)
            except OSError:
                pass
    if removed:
        print(f"\n[CLEANUP] Đã xóa {len(removed)} file dự báo cũ trong {OUT_DIR}: {', '.join(removed)}")


def main() -> None:
    print("=" * 75)
    print("THU THẬP DỰ BÁO THỜI TIẾT OPEN-METEO THEO TRẠM ĐÃ LỌC & TRUNG BÌNH THEO TỈNH")
    print("=" * 75)

    members_df = load_filtered_stations()
    n_stations = len(members_df)
    n_provinces = members_df["province_name"].nunique()
    print(f"Đã tải danh sách {n_stations} trạm quan trắc vượt qua bộ lọc thuộc {n_provinces} tỉnh/thành:\n")
    for prov_name, grp in members_df.groupby("province_name"):
        st_list = ", ".join(f"{r.station_code} ({r.latitude:.4f}, {r.longitude:.4f})" for r in grp.itertuples())
        print(f" - {prov_name:<12} ({len(grp)} trạm): {st_list}")

    current_time_vn = pd.Timestamp.now(tz=TIMEZONE).tz_localize(None)
    start_hour = current_time_vn.floor("h")
    end_hour = start_hour + pd.Timedelta(hours=24)
    print(f"\nThời điểm gọi API (Giờ VN - UTC+7): {current_time_vn}")
    print(f"Cửa sổ dự báo 24h tới             : {start_hour} -> {end_hour}")

    station_dfs = []
    for idx, (_, row) in enumerate(members_df.iterrows(), start=1):
        print(
            f"\n[{idx:02d}/{n_stations:02d}] Đang lấy dự báo cho trạm {row['station_code']} "
            f"({row['province_name']} - Lat {row['latitude']:.4f}, Lon {row['longitude']:.4f})..."
        )
        try:
            df_st = fetch_forecast_for_station(row, forecast_days=2)
            station_dfs.append(df_st)
            print(f" -> Thành công! Nhận {len(df_st)} mốc giờ ({df_st['datetime'].min()} -> {df_st['datetime'].max()}).")
            time.sleep(0.8)  # Giãn cách request để tránh chạm rate-limit của Open-Meteo
        except Exception as exc:  # noqa: BLE001
            print(f" [LỖI] Không thể lấy dữ liệu cho trạm {row['station_code']} ({row['province_name']}): {exc}")

    if not station_dfs:
        print("[LỖI] Không thu thập được dữ liệu dự báo từ bất kỳ trạm nào.")
        return

    all_stations_forecast = pd.concat(station_dfs, ignore_index=True)

    # 1. Cắt đúng cửa sổ 24h tiếp theo (từ start_hour đến end_hour) cho file dự báo chính
    stations_next_24h = all_stations_forecast[
        (all_stations_forecast["datetime"] >= start_hour)
        & (all_stations_forecast["datetime"] <= end_hour)
    ].copy()

    # 2. Cửa sổ cầu nối 72h (48h quá khứ vừa qua + 24h tới) giúp du_bao.py có thời tiết thật cho ngày hôm qua & hôm nay
    bridge_start_hour = start_hour - pd.Timedelta(hours=48)
    stations_bridge_72h = all_stations_forecast[
        (all_stations_forecast["datetime"] >= bridge_start_hour)
        & (all_stations_forecast["datetime"] <= end_hour)
    ].copy()

    # Tính trung bình các chỉ số theo từng tỉnh như sync_tedp_province.py
    province_forecast_24h = aggregate_forecast_by_province(stations_next_24h, members_df)
    province_bridge_72h = aggregate_forecast_by_province(stations_bridge_72h, members_df)

    # Xóa các file dự báo cũ trước khi lưu file tổng hợp
    cleanup_old_forecast_files()

    # Lưu tại data_test/du_bao/
    province_forecast_24h.to_csv(OUTPUT_FORECAST_CSV, index=False, encoding="utf-8-sig")
    province_bridge_72h.to_csv(OUTPUT_BRIDGE_72H_CSV, index=False, encoding="utf-8-sig")

    print("\n" + "=" * 75)
    print("HOÀN TẤT TỔNG HỢP DỮ LIỆU DỰ BÁO CẤP TỈNH (TRUNG BÌNH CÁC TRẠM THÀNH VIÊN)")
    print("=" * 75)
    print(f" - File CSV duy nhất đã lưu : {OUTPUT_FORECAST_CSV}")
    print(f" - Tổng số dòng             : {len(province_forecast_24h)} dòng "
          f"({province_forecast_24h['province_name'].nunique()} tỉnh x "
          f"{province_forecast_24h['datetime'].nunique()} mốc giờ)")
    print("\nBảng tóm tắt trung bình 24h tới theo tỉnh:")
    summary = (
        province_forecast_24h.groupby(["province_name", "station_code", "n_stations_member"], as_index=False)[
            ["latitude", "longitude", "temperature_2m", "relative_humidity_2m", "wind_speed_10m", "boundary_layer_height"]
        ]
        .mean()
        .round(2)
    )
    print(summary.to_string(index=False))
    print("=" * 75)


if __name__ == "__main__":
    main()
