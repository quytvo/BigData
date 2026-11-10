"""
export_to_pm25_gnn.py
=====================
Chuyển đổi bộ dữ liệu Việt Nam đã xử lý (`cleaned_tedp_province_4y.csv` +
`weather_openmeteo_province_4y.csv`) sang định dạng Tensor 3 chiều `.npy`
`[T=25488, N_locs=11, F=13]` và file toạ độ `vietnam_cities.txt` tương thích
100% với kiến trúc `previous_project/pm2.5/train.py` (GRU, GC_GRU, GraphConv_GRU,
GNN_GRU, Attn_GNN_GRU).

Quy ước thứ tự cột bắt buộc của `previous_project/pm2.5/dataset.py` (dòng 35-37):
    - Cột `0 .. -4`: Các biến khí tượng & chất lượng không khí phụ trợ
      (`temperature_2m`, `relative_humidity_2m`, `dew_point_2m`, `surface_pressure`,
       `precipitation`, `boundary_layer_height`, `dew_point_deficit`,
       `ventilation_coef`, `pm10`, `aqi`)
    - Cột `-3`     : `u10` (thành phần gió Đông-Tây m/s: `-wind_speed * sin(wind_dir)`)
    - Cột `-2`     : `v10` (thành phần gió Bắc-Nam m/s : `-wind_speed * cos(wind_dir)`)
    - Cột `-1`     : `pm25` (biến mục tiêu dự báo)

Chạy:
    python -X utf8 code/crawl_data/export_to_pm25_gnn.py
"""

import io
import os
import sys
from pathlib import Path

if hasattr(sys.stdout, "buffer") and getattr(sys.stdout, "encoding", "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_TEST_DIR = BASE_DIR / "data_test"
PM25_DATA_DIR = BASE_DIR / "previous_project" / "pm2.5" / "data"
PM25_DATA_DIR.mkdir(parents=True, exist_ok=True)

TAG = os.getenv("SYNC_TAG", "4y")
TEDP_CSV = DATA_TEST_DIR / f"cleaned_tedp_province_{TAG}.csv"
WEATHER_CSV = DATA_TEST_DIR / f"weather_openmeteo_province_{TAG}.csv"

OUT_NPY = PM25_DATA_DIR / f"vietnam_{TAG}.npy"
OUT_CITIES = PM25_DATA_DIR / "vietnam_cities.txt"

WEATHER_COLS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "surface_pressure",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "boundary_layer_height",
]

FEATURE_ORDER = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "surface_pressure",
    "precipitation",
    "boundary_layer_height",
    "dew_point_deficit",
    "ventilation_coef",
    "pm10",
    "aqi",
    "u10",   # [-3]: Bắt buộc cho GNN_GRU / Attn_GNN_GRU
    "v10",   # [-2]: Bắt buộc cho GNN_GRU / Attn_GNN_GRU
    "pm25",  # [-1]: Biến mục tiêu
]


def main():
    print("=" * 70)
    print("  EXPORT VIETNAM DATASET -> PREVIOUS_PROJECT/PM2.5 (.npy + cities.txt)")
    print("=" * 70)

    tedp = pd.read_csv(
        TEDP_CSV,
        dtype={"station_id": str, "province_id": str, "station_code": str},
        parse_dates=["datetime"],
    )
    wx = pd.read_csv(
        WEATHER_CSV,
        usecols=["station_code", "datetime"] + WEATHER_COLS,
        dtype={"station_code": str},
        parse_dates=["datetime"],
    )

    df = tedp.merge(wx, on=["station_code", "datetime"], how="left", validate="1:1")
    if int(df[WEATHER_COLS].isna().sum().sum()) > 0:
        raise ValueError("Dữ liệu thời tiết còn ô NaN!")

    # Tính các biến dẫn xuất và thành phần vector gió khí tượng (u10, v10)
    df["dew_point_deficit"] = df["temperature_2m"] - df["dew_point_2m"]
    df["ventilation_coef"] = df["wind_speed_10m"] * df["boundary_layer_height"]

    wind_rad = np.deg2rad(df["wind_direction_10m"].to_numpy(dtype=np.float64))
    ws = df["wind_speed_10m"].to_numpy(dtype=np.float64)
    df["u10"] = -ws * np.sin(wind_rad)
    df["v10"] = -ws * np.cos(wind_rad)

    # Sắp xếp thứ tự nút cố định theo station_code và thứ tự thời gian tăng dần
    stations = (
        df[["station_code", "province_name", "longitude", "latitude"]]
        .drop_duplicates("station_code")
        .sort_values("station_code")
        .reset_index(drop=True)
    )
    station_order = stations["station_code"].tolist()
    n_locs = len(station_order)

    df = df.sort_values(["datetime", "station_code"]).reset_index(drop=True)
    n_times = df["datetime"].nunique()
    assert len(df) == n_times * n_locs, "Lưới không-thời gian không đồng bộ!"

    # Xuất file toạ độ `vietnam_cities.txt` theo đúng chuẩn `idx name lon lat` của `city.txt`
    with open(OUT_CITIES, "w", encoding="utf-8") as f:
        for idx, row in stations.iterrows():
            f.write(f"{idx} {row['station_code']} {row['longitude']:.6f} {row['latitude']:.6f}\n")

    # Tạo Tensor 3 chiều [T, N_locs, F]
    arr_2d = df[FEATURE_ORDER].to_numpy(dtype=np.float64)
    tensor_3d = arr_2d.reshape(n_times, n_locs, len(FEATURE_ORDER))
    assert not np.isnan(tensor_3d).any(), "Tensor chứa NaN!"

    np.save(OUT_NPY, tensor_3d)

    print(f"Đã lưu Tensor 3D : {OUT_NPY} | Shape: {tensor_3d.shape} | Dtype: {tensor_3d.dtype}")
    print(f"Đã lưu Toạ độ    : {OUT_CITIES} ({n_locs} tỉnh)")
    print(f"Khung thời gian  : {df['datetime'].min()} -> {df['datetime'].max()} ({n_times:,} giờ)")
    print(f"Các cột (F={len(FEATURE_ORDER)}): {FEATURE_ORDER}")


if __name__ == "__main__":
    main()

