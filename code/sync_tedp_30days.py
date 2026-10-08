"""
Đồng bộ thời gian dữ liệu TEDP (cleaned_tedp.csv) về đúng 30 ngày theo giờ.

Quy tắc:
    1. Cửa sổ thời gian: 30 ngày tính từ ngày đầu tiên có dữ liệu
       (mặc định 2026-09-01 00:00 -> 2026-09-30 23:00 = 720 giờ).
       Các bản ghi nằm ngoài cửa sổ (phần thừa) bị xoá.
    2. Trạm nào KHÔNG có dữ liệu đủ 30 ngày trong cửa sổ -> xoá TOÀN BỘ province chứa trạm đó.
    3. Các trạm còn lại được đưa về lưới đủ 720 giờ; những giờ lẻ bị thiếu được
       nội suy tuyến tính theo thời gian (trong từng trạm) và đánh dấu *_imputed = 1,
       is_inserted = 1.
    4. Tính lại các cột thời gian dẫn xuất và aqi_category cho các dòng chèn thêm.

Kết quả: mọi trạm có cùng một tập timestamp -> đồng bộ thời gian giữa các bản ghi.

Chạy:
    python code/sync_tedp_30days.py
"""

from pathlib import Path

import numpy as np
import pandas as pd

# ============================================================
# CẤU HÌNH
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_CSV = BASE_DIR / "cleaned_tedp.csv"
OUTPUT_CSV = BASE_DIR / "cleaned_tedp_synced_30d.csv"

N_DAYS = 30
# None = lấy ngày đầu tiên có dữ liệu; hoặc đặt cố định, vd "2026-09-01"
START_DATE = None

# Cột cố định theo trạm (không đổi theo thời gian)
STATION_COLS = [
    "province_id", "province_name", "station_id", "station_code", "station_name",
    "latitude", "longitude", "province_extracted", "station_short_name",
]

# Cột đo -> cột cờ imputed tương ứng
MEASURE_COLS = {
    "pm25": "pm25_imputed",
    "aqi": "aqi_imputed",
    "pm10": "pm10_imputed",
    "no2": "no2_imputed",
    "so2": "so2_imputed",
    "o3": "o3_imputed",
    "co": "co_imputed",
}

# Ngưỡng AQI (Việt Nam) -> nhãn đang dùng trong dữ liệu
AQI_BINS = [0, 50, 100, 150, 200, 300, np.inf]
AQI_LABELS = ["Tot", "Trung binh", "Kem", "Xau", "Rat xau", "Nguy hai"]


# ============================================================
# HÀM PHỤ
# ============================================================

def time_slot(hour):
    if hour < 6:
        return "night (0-6h)"
    if hour < 12:
        return "morning (6-12h)"
    if hour < 18:
        return "afternoon (12-18h)"
    return "evening (18-24h)"


def add_time_features(df):
    dt = df["datetime"]
    df["year"] = dt.dt.year
    df["month"] = dt.dt.month
    df["day"] = dt.dt.day
    df["hour"] = dt.dt.hour
    df["day_of_week"] = dt.dt.dayofweek          # Monday = 0 (giống dữ liệu gốc)
    df["day_name"] = dt.dt.day_name()
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    df["date"] = dt.dt.strftime("%Y-%m-%d")
    df["time_slot"] = df["hour"].map(time_slot)
    return df


def aqi_category(aqi):
    return pd.cut(aqi, bins=AQI_BINS, labels=AQI_LABELS, right=False).astype(object)


# ============================================================
# 1. ĐỌC DỮ LIỆU
# ============================================================

def load_data(path):
    df = pd.read_csv(
        path,
        dtype={"province_id": str, "station_id": str},
        parse_dates=["datetime"],
    )
    df = df.drop_duplicates(["station_code", "datetime"])
    return df


# ============================================================
# 2. CẮT CỬA SỔ 30 NGÀY
# ============================================================

def cut_window(df):
    start = pd.Timestamp(START_DATE) if START_DATE else df["datetime"].min().normalize()
    end = start + pd.Timedelta(days=N_DAYS) - pd.Timedelta(hours=1)

    before = len(df)
    df = df[(df["datetime"] >= start) & (df["datetime"] <= end)].copy()
    print(f"Cửa sổ: {start} -> {end} ({N_DAYS} ngày = {N_DAYS * 24} giờ)")
    print(f"Xoá phần thừa ngoài cửa sổ: {before - len(df)} dòng")
    return df, start, end


# ============================================================
# 3. XOÁ PROVINCE CÓ TRẠM THIẾU NGÀY
# ============================================================

def drop_incomplete_provinces(df):
    days_per_station = (
        df.groupby(["province_name", "station_code"])["datetime"]
        .apply(lambda s: s.dt.normalize().nunique())
    )
    bad = days_per_station[days_per_station < N_DAYS]
    bad_provinces = sorted(bad.index.get_level_values("province_name").unique())

    if len(bad):
        print("\nTrạm không đủ ngày:")
        for (prov, code), n in bad.items():
            print(f"  - {prov} | {code}: {n}/{N_DAYS} ngày")
    print(f"Xoá {len(bad_provinces)} province: {bad_provinces}")

    return df[~df["province_name"].isin(bad_provinces)].copy(), bad_provinces


# ============================================================
# 4. ĐƯA MỖI TRẠM VỀ LƯỚI ĐỦ GIỜ + NỘI SUY GIỜ THIẾU
# ============================================================

def fill_station(group, full_index):
    group = group.set_index("datetime").sort_index()
    existing = group.index
    group = group.reindex(full_index)
    group.index.name = "datetime"

    inserted = ~group.index.isin(existing)
    group["is_inserted"] = inserted.astype(int)

    # Thông tin trạm: lấy từ dòng có sẵn
    for col in STATION_COLS:
        group[col] = group[col].ffill().bfill()

    # Nội suy giá trị đo theo thời gian, chỉ áp dụng cho dòng chèn thêm
    for col, flag in MEASURE_COLS.items():
        interpolated = group[col].interpolate(method="time", limit_direction="both")
        group.loc[inserted, col] = interpolated[inserted]
        group.loc[inserted, flag] = 1
        group[flag] = group[flag].astype(int)

    return group.reset_index()


def make_full_grid(df, start, end):
    full_index = pd.date_range(start, end, freq="h")
    parts = [fill_station(g, full_index) for _, g in df.groupby("station_code")]
    out = pd.concat(parts, ignore_index=True)

    # Tính lại cột dẫn xuất (nhất quán cho mọi dòng)
    out = add_time_features(out)
    ins = out["is_inserted"] == 1
    out.loc[ins, "aqi_category"] = aqi_category(out.loc[ins, "aqi"])
    return out


# ============================================================
# 5. KIỂM TRA ĐỒNG BỘ
# ============================================================

def validate(df, start, end):
    expected = N_DAYS * 24
    counts = df.groupby("station_code")["datetime"].nunique()
    assert (counts == expected).all(), f"Có trạm không đủ {expected} giờ:\n{counts[counts != expected]}"

    ts_sets = df.groupby("station_code")["datetime"].apply(lambda s: tuple(sorted(s)))
    assert ts_sets.nunique() == 1, "Các trạm không có cùng tập timestamp"
    assert df["datetime"].min() == start and df["datetime"].max() == end
    print(f"\n[OK] {len(counts)} trạm, mỗi trạm đúng {expected} giờ, cùng tập timestamp.")


# ============================================================
# MAIN
# ============================================================

def main():
    df = load_data(INPUT_CSV)
    original_cols = list(df.columns)
    print(f"Đọc {len(df)} dòng, {df['station_code'].nunique()} trạm, "
          f"{df['province_name'].nunique()} province")

    df, start, end = cut_window(df)
    df, _ = drop_incomplete_provinces(df)
    df = make_full_grid(df, start, end)
    validate(df, start, end)

    n_inserted = int(df["is_inserted"].sum())
    print(f"Số giờ được chèn + nội suy: {n_inserted}")
    print(df.groupby("station_code")["is_inserted"].sum().loc[lambda s: s > 0].to_string())

    df = df.sort_values(["province_name", "station_code", "datetime"])
    df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df = df[original_cols + ["is_inserted"]]
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print(f"\nKết quả: {len(df)} dòng, {df['station_code'].nunique()} trạm, "
          f"{df['province_name'].nunique()} province -> {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
