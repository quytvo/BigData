"""
sync_tedp_province.py
=====================
Đồng bộ thời gian + LỌC TỈNH + TÍNH TRUNG BÌNH CÁC TRẠM THEO TỈNH
(thay cho sync_tedp_120days.py khi dùng dữ liệu nhiều năm).

Ý tưởng giống KnowAir [REF: previous_project/pm2.5/data/city_dict.pkl]:
mỗi NÚT không gian = 1 đơn vị hành chính (thành phố/tỉnh), giá trị PM2.5 của
nút = trung bình các trạm quan trắc thuộc đơn vị đó (KnowAir: 1.498 trạm ->
184 thành phố). Ở đây: N trạm TEDP -> 1 nút / tỉnh.

Input : lakehouse_storage/bronze/cleaned_tedp.csv   (output của preprocess_tedp.py)
Output: data_test/cleaned_tedp_province_<TAG>.csv   (file clean_data cuối, TAG mặc định "4y")
        data_test/province_station_members_<TAG>.csv  (tỉnh <- những trạm nào)
        data_test/province_coverage_<TAG>.csv        (độ phủ dữ liệu từng tỉnh)

Quy trình:
    1. Cắt cửa sổ thời gian: mặc định N_YEARS = 4 năm gần nhất, kết thúc ở giờ
       23:00 của ngày đầy đủ cuối cùng có dữ liệu.
    2. Đánh dấu giá trị ĐO THẬT theo trạm: loại các ô bị preprocess fill bằng
       median (cột *_median_filled = 1) vì đó không phải số đo thật.
       (Nội suy ngắn <= 3h của preprocess vẫn được giữ.)
    3. (Tuỳ chọn) Loại trạm có độ phủ PM2.5 thấp hơn STATION_MIN_COVERAGE.
    4. LỌC TỈNH: tỉnh được giữ nếu tỉ lệ giờ có >= 1 trạm đo thật PM2.5
       >= PROVINCE_MIN_COVERAGE (mặc định 80%).
       Lý do: với 4 năm, yêu cầu "mọi trạm đủ 100% số ngày" như bản 120 ngày
       gần như chắc chắn loại hết tỉnh. Muốn quay lại luật cũ: set
       PROVINCE_MIN_COVERAGE=1.0
       (Tuỳ chọn) chỉ giữ một danh sách tỉnh cố định: SYNC_PROVINCES="Hà Nội,Đà Nẵng"
    5. TRUNG BÌNH THEO TỈNH: tại mỗi giờ, giá trị tỉnh = mean các trạm CÓ số đo
       thật ở giờ đó (trạm mất dữ liệu không kéo trung bình).
    6. Đưa mỗi tỉnh về lưới đủ giờ; giờ không có trạm nào đo -> nội suy theo
       thời gian, đánh dấu *_imputed = 1, is_inserted = 1, pm25_gap_hours = độ dài gap.
    7. Định danh nút: station_id = province_id gốc của TEDP (để phân biệt tỉnh;
       nếu 2 tên tỉnh dùng chung 1 province_id như "HCM"/"HCM KV3" thì thêm hậu tố
       _<slug>), station_code = "PROV_<slug>", toạ độ = trọng tâm các trạm thành viên.
       -> schema giữ nguyên các cột cũ nên code training/fetch weather dùng lại được.

Chạy:
    python code/crawl_data/sync_tedp_province.py

Biến môi trường (tuỳ chọn):
    SYNC_YEARS=4                 số năm của cửa sổ
    SYNC_START_DATE=2022-11-07   cố định ngày bắt đầu (ghi đè SYNC_YEARS)
    SYNC_END_DATE=2026-11-06     cố định ngày kết thúc (lấy tới 23:00 ngày đó)
    PROVINCE_MIN_COVERAGE=0.8
    STATION_MIN_COVERAGE=0.0
    SYNC_PROVINCES="Hà Nội,Đà Nẵng"
    SYNC_TAG=4y                  hậu tố tên file output
"""

import io
import os
import re
import sys
import unicodedata
from pathlib import Path

# Fix encoding cho terminal Windows (cp1258 khong ho tro unicode tieng Viet)
if hasattr(sys.stdout, "buffer") and getattr(sys.stdout, "encoding", "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import numpy as np
import pandas as pd

# ============================================================
# CẤU HÌNH
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = BASE_DIR / "lakehouse_storage" / "bronze"
DATA_TEST_DIR = BASE_DIR / "data_test"
DATA_TEST_DIR.mkdir(parents=True, exist_ok=True)
INPUT_CSV = BRONZE_DIR / "cleaned_tedp.csv"

N_YEARS = int(os.getenv("SYNC_YEARS", "4"))
START_DATE = os.getenv("SYNC_START_DATE") or None
END_DATE = os.getenv("SYNC_END_DATE") or None
PROVINCE_MIN_COVERAGE = float(os.getenv("PROVINCE_MIN_COVERAGE", "0.8"))
STATION_MIN_COVERAGE = float(os.getenv("STATION_MIN_COVERAGE", "0.8"))
FORCED_PROVINCES = [
    p.strip() for p in os.getenv("SYNC_PROVINCES", "").split(",") if p.strip()
]
TAG = os.getenv("SYNC_TAG", f"{N_YEARS}y")

OUTPUT_CSV = DATA_TEST_DIR / f"cleaned_tedp_province_{TAG}.csv"
MEMBERS_CSV = DATA_TEST_DIR / f"province_station_members_{TAG}.csv"
COVERAGE_CSV = DATA_TEST_DIR / f"province_coverage_{TAG}.csv"

# Cột đo -> cột cờ imputed tương ứng (chỉ giữ pm25, aqi, pm10)
MEASURE_COLS = {
    "pm25": "pm25_imputed",
    "aqi": "aqi_imputed",
    "pm10": "pm10_imputed",
}
# Các cột mô hình bắt buộc phải đầy đủ (train_global_ann dùng pm25, pm10, aqi)
REQUIRED_COLS = ["pm25", "pm10", "aqi"]

# Ngưỡng AQI (Việt Nam) -> nhãn đang dùng trong dữ liệu
AQI_BINS = [0, 50, 100, 150, 200, 300, np.inf]
AQI_LABELS = ["Tot", "Trung binh", "Kem", "Xau", "Rat xau", "Nguy hai"]

# Thứ tự cột output = schema cũ + cột mới của bản trung bình tỉnh (đã bỏ no2, so2, o3, co)
OUTPUT_COLS = [
    "province_id", "province_name", "station_id", "station_code", "station_name",
    "latitude", "longitude", "datetime",
    "pm25", "aqi", "pm10",
    "pm25_imputed", "pm10_imputed", "aqi_imputed",
    "year", "month", "day", "hour", "day_of_week", "day_name", "is_weekend",
    "date", "time_slot", "aqi_category", "province_extracted", "station_short_name",
    "is_inserted",
    # --- cột mới ---
    "n_stations_member",       # số trạm thuộc tỉnh (cố định)
    "n_stations_valid_pm25",   # số trạm có PM2.5 đo thật tại giờ đó
    "pm25_gap_hours",          # độ dài gap (giờ) nếu giờ này được nội suy, 0 nếu đo thật
]


# ============================================================
# HÀM PHỤ
# ============================================================

def slugify(name):
    """'Hà Nội' -> 'ha_noi'."""
    s = unicodedata.normalize("NFD", str(name)).replace("đ", "d").replace("Đ", "D")
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


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
    df["day_of_week"] = dt.dt.dayofweek          # Monday = 0
    df["day_name"] = dt.dt.day_name()
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    df["date"] = dt.dt.strftime("%Y-%m-%d")
    df["time_slot"] = df["hour"].map(time_slot)
    return df


def gap_lengths(is_missing):
    """Độ dài của chuỗi NaN liên tiếp chứa mỗi ô (0 nếu ô không thiếu)."""
    m = is_missing.to_numpy().astype(bool)
    run_id = np.cumsum(np.r_[True, m[1:] != m[:-1]])
    lengths = pd.Series(m.astype(int)).groupby(run_id).transform("sum").to_numpy()
    return np.where(m, lengths, 0)


# ============================================================
# 1. ĐỌC DỮ LIỆU
# ============================================================

def load_data(path):
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file đầu vào: {path} (chạy preprocess_tedp.py trước)")

    df = pd.read_csv(
        path,
        dtype={"province_id": str, "station_id": str, "station_code": str},
        parse_dates=["datetime"],
        low_memory=False,
    )
    df["datetime"] = df["datetime"].dt.floor("h")
    df = df.drop_duplicates(["station_code", "datetime"])
    df["node_id"] = make_node_ids(df)
    return df


def make_node_ids(df):
    """
    Mã nút duy nhất cho mỗi province_name. TEDP có trường hợp 2 tên dùng chung
    1 province_id (vd "HCM" và "HCM KV3") -> khi đó node_id = <province_id>_<slug>,
    còn lại node_id = province_id gốc (giữ nguyên ID TEDP).
    """
    pairs = df[["province_id", "province_name"]].drop_duplicates()
    shared = pairs["province_id"].value_counts()
    shared = set(shared[shared > 1].index)
    mapping = {
        r.province_name: (f"{r.province_id}_{slugify(r.province_name)}"
                          if r.province_id in shared else r.province_id)
        for r in pairs.itertuples()
    }
    return df["province_name"].map(mapping)


# ============================================================
# 2. CẮT CỬA SỔ N NĂM
# ============================================================

def cut_window(df):
    if END_DATE:
        end = pd.Timestamp(END_DATE).normalize() + pd.Timedelta(hours=23)
    else:
        t_max = df["datetime"].max()
        # chỉ lấy tới ngày đầy đủ cuối cùng (23:00)
        end = t_max if t_max.hour == 23 else t_max.normalize() - pd.Timedelta(hours=1)

    if START_DATE:
        start = pd.Timestamp(START_DATE).normalize()
    else:
        target_start = (end + pd.Timedelta(hours=1)) - pd.DateOffset(years=N_YEARS)
        start = target_start

        # Kiểm tra thời điểm thực tế có dữ liệu đo thật trên TEDP của các tỉnh dài hạn:
        # Nếu máy chủ TEDP trống dữ liệu ở giai đoạn đầu (vd năm 2022-2023), tự động
        # căn mốc `start` về ngày đầu tiên mà tất cả các tỉnh chủ lực đều đã vận hành,
        # tránh việc chia mẫu số cho cả năm trống khiến mọi tỉnh bị loại hoặc nội suy khống.
        df_in = df[(df["datetime"] >= target_start) & (df["datetime"] <= end) & df["pm25"].notna()]
        if not df_in.empty:
            prov_stats = df_in.groupby("node_id")["datetime"].agg(
                hours="nunique", first="min"
            )
            max_prov_hours = prov_stats["hours"].max()
            # Tỉnh dài hạn = các tỉnh đạt ít nhất 50% số giờ của tỉnh nhiều dữ liệu nhất
            core_provs = prov_stats[prov_stats["hours"] >= 0.5 * max_prov_hours]
            latest_core_first = core_provs["first"].max()
            aligned_start = (
                latest_core_first.normalize()
                if latest_core_first.hour == 0
                else latest_core_first.normalize() + pd.Timedelta(days=1)
            )
            if aligned_start > target_start:
                print(
                    f"[AUTO-ALIGN] Cửa sổ mục tiêu {N_YEARS} năm bắt đầu từ {target_start}, "
                    f"nhưng {len(core_provs)} tỉnh dài hạn trên TEDP chỉ đồng bộ đầy đủ từ {latest_core_first}."
                )
                print(
                    f"             -> Tự động căn mốc bắt đầu về: {aligned_start} "
                    f"(không nội suy khống giai đoạn máy chủ TEDP chưa có dữ liệu)."
                )
                start = aligned_start

    before = len(df)
    df = df[(df["datetime"] >= start) & (df["datetime"] <= end)].copy()
    n_hours = int((end - start) / pd.Timedelta(hours=1)) + 1

    print(f"Cửa sổ đồng bộ: {start} -> {end} ({n_hours:,} giờ = {n_hours // 24:,} ngày ~ {n_hours / 24 / 365.25:.2f} năm)")
    print(f"Xoá phần ngoài cửa sổ: {before - len(df):,} dòng")
    return df, start, end, n_hours


# ============================================================
# 3. ĐÁNH DẤU GIÁ TRỊ ĐO THẬT (LOẠI Ô FILL MEDIAN)
# ============================================================

def mask_non_measured(df):
    df = df.copy()
    for col in MEASURE_COLS:
        if col not in df.columns:
            df[col] = np.nan
            continue
        med_flag = f"{col}_median_filled"
        if med_flag in df.columns:
            bad = df[med_flag].fillna(0).astype(int) == 1
            df.loc[bad, col] = np.nan
            if bad.any():
                print(f"  {col:5s}: loại {int(bad.sum()):,} ô fill median (không phải số đo thật)")
        else:
            print(
                f"  [CANH BAO] Thiếu cột {med_flag} -> chạy lại preprocess_tedp.py bản mới "
                f"để loại ô fill median khỏi trung bình tỉnh."
            )
    return df


# ============================================================
# 4. LỌC TRẠM + LỌC TỈNH THEO ĐỘ PHỦ
# ============================================================

def filter_stations(df, n_hours):
    cov = df.groupby("station_code")["pm25"].count() / n_hours
    if STATION_MIN_COVERAGE > 0:
        bad_codes = []
        for (nid, pname), g in df.groupby(["node_id", "province_name"]):
            st_codes = sorted(g["station_code"].unique())
            # Nếu tỉnh có nhiều trạm và có trạm lắp muộn (độ phủ < STATION_MIN_COVERAGE)
            # trong khi tỉnh đã có trạm dài hạn đạt chuẩn -> loại trạm lắp muộn để tránh nhảy bậc trung bình.
            good_in_prov = [c for c in st_codes if cov.get(c, 0.0) >= STATION_MIN_COVERAGE]
            bad_in_prov = [c for c in st_codes if cov.get(c, 0.0) < STATION_MIN_COVERAGE]
            if good_in_prov and bad_in_prov:
                bad_codes.extend((pname, c, cov.get(c, 0.0)) for c in bad_in_prov)

        if bad_codes:
            print(f"\nLoại {len(bad_codes)} trạm lắp muộn (độ phủ PM2.5 < {STATION_MIN_COVERAGE:.0%}) khỏi trung bình tỉnh:")
            for pname, code, c in bad_codes:
                print(f"  - {pname} | {code}: {c:.1%}")
            drop_set = {code for _, code, _ in bad_codes}
            df = df[~df["station_code"].isin(drop_set)].copy()
    return df, cov


def aggregate_province(df):
    """Trung bình các trạm theo (tỉnh, giờ) — chỉ trên giá trị đo thật."""
    measure = list(MEASURE_COLS)
    agg = (
        df.groupby(["node_id", "province_name", "datetime"])[measure]
        .mean()                                   # skipna: trạm mất dữ liệu không tính
        .reset_index()
    )
    n_valid = (
        df.groupby(["node_id", "province_name", "datetime"])["pm25"]
        .count()
        .rename("n_stations_valid_pm25")
        .reset_index()
    )
    pid = df.groupby("node_id")["province_id"].first().rename("province_id")
    agg = agg.merge(n_valid, on=["node_id", "province_name", "datetime"], how="left")
    return agg.merge(pid, left_on="node_id", right_index=True, how="left")


def province_coverage(agg, n_hours, station_cov, df):
    rows = []
    for (nid, pname), g in agg.groupby(["node_id", "province_name"]):
        members = sorted(df.loc[df["node_id"] == nid, "station_code"].unique())
        rows.append({
            "node_id": nid,
            "province_id": g["province_id"].iloc[0],
            "province_name": pname,
            "n_stations_member": len(members),
            "pm25_hours_covered": int((g["n_stations_valid_pm25"] > 0).sum()),
            "pm25_coverage": float((g["n_stations_valid_pm25"] > 0).sum() / n_hours),
            "pm10_coverage": float(g["pm10"].notna().sum() / n_hours),
            "aqi_coverage": float(g["aqi"].notna().sum() / n_hours),
            "first_time": g.loc[g["n_stations_valid_pm25"] > 0, "datetime"].min(),
            "last_time": g.loc[g["n_stations_valid_pm25"] > 0, "datetime"].max(),
            "station_codes": ";".join(members),
            "station_coverage_pct": ";".join(f"{station_cov.get(m, 0) * 100:.1f}" for m in members),
        })
    return pd.DataFrame(rows).sort_values("pm25_coverage", ascending=False)


def select_provinces(cov_df):
    if FORCED_PROVINCES:
        missing = [p for p in FORCED_PROVINCES if p not in set(cov_df["province_name"])]
        if missing:
            print(f"[CANH BAO] Không có dữ liệu cho tỉnh: {missing}")
        keep = cov_df[cov_df["province_name"].isin(FORCED_PROVINCES)]
        print(f"\nGiữ theo danh sách SYNC_PROVINCES: {keep['province_name'].tolist()}")
    else:
        keep = cov_df[
            (cov_df["pm25_coverage"] >= PROVINCE_MIN_COVERAGE)
            & (cov_df["pm10_coverage"] > 0)
            & (cov_df["aqi_coverage"] > 0)
        ]

    dropped = cov_df[~cov_df["node_id"].isin(keep["node_id"])]
    print(f"\nLọc tỉnh (ngưỡng độ phủ PM2.5 >= {PROVINCE_MIN_COVERAGE:.0%}):")
    print(f"{'Tỉnh':<14}{'Số trạm':>8}{'Phủ PM2.5':>11}{'Phủ PM10':>10}  Trạng thái")
    for _, r in cov_df.iterrows():
        status = "GIỮ" if r["node_id"] in set(keep["node_id"]) else "LOẠI"
        print(
            f"{r['province_name']:<14}{r['n_stations_member']:>8}"
            f"{r['pm25_coverage']:>10.1%}{r['pm10_coverage']:>10.1%}  {status}"
        )
    print(f"-> Giữ {len(keep)} tỉnh, loại {len(dropped)} tỉnh")
    return keep


# ============================================================
# 5. LƯỚI ĐỦ GIỜ + NỘI SUY GIỜ KHÔNG CÓ TRẠM NÀO ĐO
# ============================================================

def fill_province(g, full_index):
    g = g.set_index("datetime").sort_index().reindex(full_index)
    g.index.name = "datetime"

    g["n_stations_valid_pm25"] = g["n_stations_valid_pm25"].fillna(0).astype(int)
    g["is_inserted"] = (g["n_stations_valid_pm25"] == 0).astype(int)
    g["pm25_gap_hours"] = gap_lengths(g["pm25"].isna())

    for col, flag in MEASURE_COLS.items():
        missing = g[col].isna()
        g[flag] = missing.astype(int)
        if missing.any() and g[col].notna().any():
            g[col] = g[col].interpolate(method="time", limit_direction="both")
    return g.reset_index()


def make_full_grid(agg, keep, members_meta, start, end):
    full_index = pd.date_range(start, end, freq="h")
    parts = []
    for _, prow in keep.iterrows():
        g = agg[agg["node_id"] == prow["node_id"]]
        g = fill_province(g.drop(columns=["node_id", "province_id", "province_name"]), full_index)

        meta = members_meta.loc[prow["node_id"]]
        g.insert(0, "province_id", prow["province_id"])
        g.insert(1, "province_name", prow["province_name"])
        g["station_id"] = prow["node_id"]                # 1 nút = 1 tỉnh (ID TEDP gốc)
        g["station_code"] = f"PROV_{slugify(prow['province_name']).upper()}"
        g["station_name"] = f"{prow['province_name']}: Trung bình {meta['n_stations_member']} trạm"
        g["latitude"] = meta["latitude"]
        g["longitude"] = meta["longitude"]
        g["province_extracted"] = prow["province_name"]
        g["station_short_name"] = f"TB {meta['n_stations_member']} trạm"
        g["n_stations_member"] = int(meta["n_stations_member"])
        parts.append(g)

    out = pd.concat(parts, ignore_index=True)
    out = add_time_features(out)
    out["aqi_category"] = (
        pd.cut(out["aqi"], bins=AQI_BINS, labels=AQI_LABELS, right=False).astype(object)
    )
    return out


def build_members(df, keep):
    """Bảng tỉnh -> các trạm thành viên + trọng tâm toạ độ."""
    st = (
        df[df["node_id"].isin(keep["node_id"])]
        .drop_duplicates("station_code")[
            ["node_id", "province_id", "province_name", "station_id", "station_code",
             "station_name", "latitude", "longitude"]
        ]
        .sort_values(["province_name", "station_code"])
    )
    meta = st.groupby("node_id").agg(
        latitude=("latitude", "mean"),
        longitude=("longitude", "mean"),
        n_stations_member=("station_code", "nunique"),
    )
    return st, meta


# ============================================================
# 6. KIỂM TRA
# ============================================================

def validate(df, n_hours):
    counts = df.groupby("station_code")["datetime"].nunique()
    assert (counts == n_hours).all(), f"Có tỉnh không đủ {n_hours} giờ:\n{counts[counts != n_hours]}"
    ts_sets = df.groupby("station_code")["datetime"].apply(lambda s: (s.min(), s.max(), len(s)))
    assert ts_sets.nunique() == 1, "Các tỉnh không có cùng tập timestamp"

    nan_req = df[REQUIRED_COLS].isna().sum()
    if nan_req.any():
        raise ValueError(f"Còn NaN ở cột bắt buộc sau nội suy:\n{nan_req[nan_req > 0]}")
    print(f"\n[OK] {len(counts)} tỉnh (nút), mỗi tỉnh đúng {n_hours:,} giờ, cùng tập timestamp, "
          f"không NaN ở {REQUIRED_COLS}.")


# ============================================================
# MAIN
# ============================================================

def main():
    df = load_data(INPUT_CSV)
    print(f"Đọc {len(df):,} dòng, {df['station_code'].nunique()} trạm, "
          f"{df['province_name'].nunique()} tỉnh")

    print("\nĐánh dấu giá trị đo thật:")
    df = mask_non_measured(df)

    df, start, end, n_hours = cut_window(df)

    df, station_cov = filter_stations(df, n_hours)

    agg = aggregate_province(df)
    cov_df = province_coverage(agg, n_hours, station_cov, df)
    keep = select_provinces(cov_df)
    if keep.empty:
        raise SystemExit(
            "\n[DỪNG] Không còn tỉnh nào đạt ngưỡng. Xem bảng độ phủ phía trên, rồi hạ "
            "PROVINCE_MIN_COVERAGE hoặc thu hẹp cửa sổ (SYNC_START_DATE)."
        )

    members, members_meta = build_members(df, keep)
    out = make_full_grid(agg, keep, members_meta, start, end)
    validate(out, n_hours)

    # --- Thống kê nội suy ---
    print("\nTỉ lệ giờ phải nội suy PM2.5 (không trạm nào đo):")
    summary = out.groupby("province_name").agg(
        n_stations=("n_stations_member", "first"),
        inserted_pct=("is_inserted", lambda s: 100 * s.mean()),
        max_gap_h=("pm25_gap_hours", "max"),
        mean_valid_stations=("n_stations_valid_pm25", "mean"),
    )
    print(summary.round(2).to_string())
    long_gap = out["pm25_gap_hours"] > 24
    if long_gap.any():
        print(f"[RISK] {int(long_gap.sum()):,} giờ nằm trong gap > 24h (nội suy tuyến tính dài) "
              f"-> có thể lọc bằng cột pm25_gap_hours khi đánh giá mô hình.")

    # --- Lưu ---
    out = out.sort_values(["province_name", "datetime"])
    out["datetime"] = out["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    out = out[OUTPUT_COLS]
    out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    members.to_csv(MEMBERS_CSV, index=False, encoding="utf-8-sig")
    cov_df.to_csv(COVERAGE_CSV, index=False, encoding="utf-8-sig")

    print(f"\nKết quả: {len(out):,} dòng = {out['station_code'].nunique()} tỉnh x {n_hours:,} giờ")
    print(f"  - {OUTPUT_CSV}")
    print(f"  - {MEMBERS_CSV}")
    print(f"  - {COVERAGE_CSV}")


if __name__ == "__main__":
    main()
