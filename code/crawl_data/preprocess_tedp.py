"""
preprocess_tedp.py
==================
Tien xu ly du lieu AQI / PM2.5 tu TEDP (tedp_aqi_pm25_hourly.csv).

Cac buoc thuc hien:
  1. Load & kiem tra so bo (data profiling)
  2. Ep kieu (datetime, numeric)
  3. Lam sach: loai bo trung lap, loc gia tri phi ly
  4. Xu ly missing values (noi suy + flag)
  5. Tao cot thoi gian phai sinh (hour, day_of_week, month, ...)
  6. Tinh AQI_category (nhan muc do o nhiem)
  7. Chuan hoa ten tram / tinh
  8. Luu ket qua ra cleaned_tedp.csv + bao cao chat luong
"""

import sys
import io

# Fix encoding cho terminal Windows (cp1258 khong ho tro unicode tieng Viet)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from pathlib import Path
import pandas as pd
import numpy as np
import os

# ------------------------------------------------------------------ #
# CONFIG
# ------------------------------------------------------------------ #
BASE_DIR = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = BASE_DIR / "lakehouse_storage" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FILE  = BRONZE_DIR / "tedp_aqi_pm25_hourly.csv"
OUTPUT_FILE = BRONZE_DIR / "cleaned_tedp.csv"
REPORT_FILE = BRONZE_DIR / "preprocessing_report.txt"

# Nguong loai bo gia tri phi ly (outlier cung theo QCVN)
THRESHOLDS = {
    "pm25": (0,   500),   # AQI sub-index / ug/m3
    "pm10": (0,   600),   # AQI sub-index / ug/m3
    "aqi" : (0,   500),
}

# Cot chi so do dac (se duoc noi suy)
POLLUTANT_COLS = ["pm25", "pm10", "aqi"]

# Phan nguong AQI (US EPA / TCVN)
AQI_BINS   = [0, 50, 100, 150, 200, 300, 500]
AQI_LABELS = ["Tot", "Trung binh", "Kem", "Xau", "Rat xau", "Nguy hiem"]


# ------------------------------------------------------------------ #
# UTILITIES
# ------------------------------------------------------------------ #
class Report:
    """Thu thap log va ghi ra file bao cao."""

    def __init__(self):
        self._lines = []

    def log(self, msg=""):
        print(msg)
        self._lines.append(str(msg))

    def section(self, title):
        sep = "=" * 65
        self.log(f"\n{sep}")
        self.log(f"  {title}")
        self.log(sep)

    def save(self, path):
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(self._lines))
        print(f"\nBao cao da luu: {os.path.abspath(path)}")


report = Report()


# ------------------------------------------------------------------ #
# STEP 1: LOAD & PROFILE
# ------------------------------------------------------------------ #
def load_and_profile(path):
    report.section("BUOC 1 -- LOAD & KIEM TRA SO BO")

    path = Path(path)
    if not path.exists():
        fallback = BASE_DIR / "data_test" / path.name
        if fallback.exists():
            report.log(f"[INFO] Khong tim thay {path}, dung fallback: {fallback}")
            path = fallback
        else:
            raise FileNotFoundError(f"Khong tim thay tap tin dau vao: {path}")

    df = pd.read_csv(
        path,
        dtype={"province_id": str, "station_id": str, "station_code": str},
        low_memory=False,
    )
    report.log(f"File      : {os.path.abspath(path)}")
    report.log(f"Shape     : {df.shape[0]:,} hang  x  {df.shape[1]} cot")
    report.log(f"Columns   : {df.columns.tolist()}")

    report.log("\n--- Kieu du lieu tho ---")
    report.log(df.dtypes.to_string())

    report.log("\n--- Gia tri null (raw) ---")
    null_counts = df.isnull().sum()
    null_pct    = (null_counts / len(df) * 100).round(2)
    null_df     = pd.DataFrame({"null_count": null_counts, "null_%": null_pct})
    report.log(null_df[null_df["null_count"] > 0].to_string())

    report.log(f"\nTram duy nhat   : {df['station_id'].nunique()}")
    report.log(f"Tinh/thanh      : {df['province_name'].nunique()}")
    report.log(f"Khoang thoi gian: {df['datetime'].min()}  ->  {df['datetime'].max()}")

    return df


# ------------------------------------------------------------------ #
# STEP 2: EP KIEU
# ------------------------------------------------------------------ #
def cast_types(df):
    report.section("BUOC 2 -- EP KIEU DU LIEU")

    # Datetime
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    invalid_dt = df["datetime"].isna().sum()
    report.log(f"datetime khong hop le (coerce -> NaT): {invalid_dt}")

    # Numeric
    for col in POLLUTANT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Toa do
    df["latitude"]  = pd.to_numeric(df["latitude"],  errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    report.log("Ep kieu hoan tat.")
    return df


# ------------------------------------------------------------------ #
# STEP 3: LAM SACH
# ------------------------------------------------------------------ #
def clean(df):
    report.section("BUOC 3 -- LAM SACH")
    n0 = len(df)

    # 3a. Loai bo hang datetime null
    df = df.dropna(subset=["datetime"])
    report.log(f"Loai bo hang datetime null           : -{n0 - len(df)} hang")

    # 3b. Loai bo trung lap (cung station + datetime)
    n1 = len(df)
    df = df.drop_duplicates(subset=["station_id", "datetime"])
    report.log(f"Loai bo duplicate (station+time)     : -{n1 - len(df)} hang")

    # 3c. Loc gia tri phi ly (ngoai nguong vat ly)
    for col, (lo, hi) in THRESHOLDS.items():
        if col not in df.columns:
            continue
        mask_bad = df[col].notna() & ((df[col] < lo) | (df[col] > hi))
        n_bad = mask_bad.sum()
        if n_bad:
            df.loc[mask_bad, col] = np.nan
            report.log(f"  {col:6s}: {n_bad} gia tri ngoai [{lo}, {hi}] -> NaN")

    # LUU Y: KHONG ap dung rule "pm25 > pm10 -> xoa dong".
    # Cac cot pm25 (PM205) / pm10 / co... lay tu endpoint AQI cua TEDP
    # nhieu kha nang la chi so AQI thanh phan, khong phai nong do ug/m3,
    # nen AQI_PM2.5 > AQI_PM10 la binh thuong.

    report.log(f"\nShape sau lam sach: {df.shape}")
    return df


# ------------------------------------------------------------------ #
# STEP 4: XU LY MISSING VALUES
# ------------------------------------------------------------------ #
def handle_missing(df):
    report.section("BUOC 4 -- XU LY MISSING VALUES")

    # Sap xep theo tram + thoi gian de noi suy chinh xac
    df = df.sort_values(["station_id", "datetime"]).reset_index(drop=True)

    for col in POLLUTANT_COLS:
        if col not in df.columns:
            continue

        # Danh flag truoc khi noi suy (luon tao cot *_imputed de co dinh schema)
        flag_col = f"{col}_imputed"
        df[flag_col] = df[col].isna().astype(int)
        # Flag rieng cho gia tri bi fill bang MEDIAN (gap > 3h) -> day KHONG phai
        # gia tri do that; buoc tinh trung binh theo tinh (sync_tedp_province.py)
        # se LOAI cac gia tri nay de khong lam sai lech trung binh tinh.
        median_col = f"{col}_median_filled"
        df[median_col] = 0

        before = df[col].isna().sum()
        if before == 0:
            report.log(f"  {col:6s}: khong co gia tri null.")
            continue

        # Noi suy tuyen tinh theo tung tram (gioi han 3 gio lien tiep)
        df[col] = (
            df.groupby("station_id")[col]
            .transform(lambda s: s.interpolate(method="linear", limit=3))
        )

        after  = df[col].isna().sum()
        filled = before - after
        report.log(
            f"  {col:6s}: {before} null -> noi suy {filled} "
            f"-> con {after} null (fill bang median tram)"
        )

        # Phan con lai (gap > 3h) -> fill bang median cua tram do
        if after > 0:
            df[median_col] = df[col].isna().astype(int)
            station_median = df.groupby("station_id")[col].transform("median")
            df[col] = df[col].fillna(station_median)

    report.log(f"\nNull sau xu ly:")
    remaining = df[POLLUTANT_COLS].isnull().sum()
    report.log(remaining[remaining > 0].to_string() or "  (khong con null)")
    return df


# ------------------------------------------------------------------ #
# STEP 5: COT THOI GIAN PHAI SINH
# ------------------------------------------------------------------ #
def add_time_features(df):
    report.section("BUOC 5 -- COT THOI GIAN PHAI SINH")

    df["year"]        = df["datetime"].dt.year
    df["month"]       = df["datetime"].dt.month
    df["day"]         = df["datetime"].dt.day
    df["hour"]        = df["datetime"].dt.hour
    df["day_of_week"] = df["datetime"].dt.dayofweek   # 0=Thu 2
    df["day_name"]    = df["datetime"].dt.day_name()
    df["is_weekend"]  = df["day_of_week"].isin([5, 6]).astype(int)
    df["date"]        = df["datetime"].dt.date

    def time_slot(h):
        if   0 <= h <  6: return "night (0-6h)"
        elif 6 <= h < 12: return "morning (6-12h)"
        elif 12 <= h < 18: return "afternoon (12-18h)"
        else:              return "evening (18-24h)"

    df["time_slot"] = df["hour"].apply(time_slot)

    report.log("Cac cot moi: year, month, day, hour, day_of_week, "
               "day_name, is_weekend, date, time_slot")
    return df


# ------------------------------------------------------------------ #
# STEP 6: PHAN LOAI MUC DO O NHIEM (AQI CATEGORY)
# ------------------------------------------------------------------ #
def add_aqi_category(df):
    report.section("BUOC 6 -- PHAN LOAI AQI")

    if "aqi" not in df.columns:
        report.log("Khong co cot 'aqi', bo qua.")
        return df

    df["aqi_category"] = pd.cut(
        df["aqi"],
        bins=AQI_BINS,
        labels=AQI_LABELS,
        right=True,
        include_lowest=True,
    )

    report.log("Phan bo muc AQI:")
    dist = df["aqi_category"].value_counts().sort_index()
    report.log(dist.to_string())
    return df


# ------------------------------------------------------------------ #
# STEP 7: CHUAN HOA TEN TRAM / TINH
# ------------------------------------------------------------------ #
def normalize_names(df):
    report.section("BUOC 7 -- CHUAN HOA TEN TRAM / TINH")

    def extract_province(name):
        if isinstance(name, str) and ":" in name:
            return name.split(":")[0].strip()
        return name

    def extract_short_name(name):
        if isinstance(name, str) and ":" in name:
            part = name.split(":", 1)[1].strip()
            return part.replace("(KK)", "").strip()
        return name

    df["province_extracted"] = df["station_name"].apply(extract_province)
    df["station_short_name"] = df["station_name"].apply(extract_short_name)

    # Cap nhat province_name neu hien tai la "Unknown"
    mask_unknown = df["province_name"].str.lower().isin(["unknown", ""])
    df.loc[mask_unknown, "province_name"] = df.loc[mask_unknown, "province_extracted"]

    report.log(f"Tinh/thanh sau chuan hoa ({df['province_name'].nunique()}):")
    report.log(sorted(df["province_name"].unique().tolist()))
    return df


# ------------------------------------------------------------------ #
# STEP 8: BAO CAO CUOI & LUU FILE
# ------------------------------------------------------------------ #
def final_report_and_save(df):
    report.section("BUOC 8 -- BAO CAO CUOI & LUU FILE")

    report.log(f"Shape cuoi : {df.shape[0]:,} hang  x  {df.shape[1]} cot")
    report.log(f"Khoang TG  : {df['datetime'].min()}  ->  {df['datetime'].max()}")
    report.log(f"Tram       : {df['station_id'].nunique()}")
    report.log(f"Tinh       : {df['province_name'].nunique()}")

    report.log("\n--- Thong ke mo ta cac chi so o nhiem ---")
    desc = df[POLLUTANT_COLS].describe().round(3)
    report.log(desc.to_string())

    report.log("\n--- Null con lai ---")
    remaining_null = df[POLLUTANT_COLS].isnull().sum()
    report.log(remaining_null.to_string())

    # Luu CSV
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    report.log(f"\nFile cleaned: {os.path.abspath(OUTPUT_FILE)}")

    # Luu bao cao
    report.save(REPORT_FILE)


# ------------------------------------------------------------------ #
# PIPELINE CHINH
# ------------------------------------------------------------------ #
def main():
    report.log("=" * 65)
    report.log("  TEDP AQI -- TIEN XU LY DU LIEU")
    report.log("=" * 65)

    df = load_and_profile(INPUT_FILE)
    df = cast_types(df)
    df = clean(df)
    df = handle_missing(df)
    df = add_time_features(df)
    df = add_aqi_category(df)
    df = normalize_names(df)
    final_report_and_save(df)

    print("\n[DONE] Tien xu ly cap tram hoan tat!")
    print(f"   Input : {INPUT_FILE}")
    print(f"   Output: {OUTPUT_FILE}")
    print(f"   Report: {REPORT_FILE}")

    # Buoc 9: Tu dong chay loc tinh + tinh trung binh cac tram thuoc tinh
    # de xuat ra file clean_data cuoi cung (data_test/cleaned_tedp_province_4y.csv)
    print("\n" + "=" * 65)
    print("  BUOC 9 -- LOC TINH & TINH TRUNG BINH CAC TRAM THUOC TINH")
    print("=" * 65)
    current_dir = Path(__file__).resolve().parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    import sync_tedp_province
    sync_tedp_province.main()


if __name__ == "__main__":
    main()
