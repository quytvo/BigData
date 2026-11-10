"""
fill_era5_blh_cds.py
====================
Tải dữ liệu `boundary_layer_height` (blh) từ ERA5 Final (Copernicus Climate Data Store - CDS)
thông qua thư viện `cdsapi` trong MỘT LẦN GỌI (1 request duy nhất) để tránh phải xếp hàng chờ
nhiều lần trên hệ thống CDS, đồng thời tận dụng lại mọi file `.nc` đã tải sẵn trong thư mục
checkpoint `lakehouse_storage/bronze/era5_cds_blh/`.

Cơ chế hoạt động:
    1. Đọc `data_test/weather_openmeteo_province_4y.csv`.
    2. Nếu trong `lakehouse_storage/bronze/era5_cds_blh/` đã có sẵn các file `.nc` tải từ trước
       (ví dụ `era5_blh_2024-01.nc`), tự động đọc và điền trước vào các ô đang khuyết để không
       phải tải lại tháng đó.
    3. Kiểm tra các ô `boundary_layer_height` còn đang bị NaN, quy đổi từ giờ Việt Nam (UTC+7)
       sang giờ chuẩn UTC của ERA5 và gộp TẤT CẢ các tháng/ngày còn thiếu vào **1 request
       `client.retrieve()` duy nhất**.
    4. Đọc file NetCDF bằng bộ đệm RAM (`netCDF4.Dataset(..., memory=...)`) để tránh lỗi đường
       dẫn tiếng Việt có dấu (`d:\\năm 4\\...`) của thư viện C `libnetcdf` trên Windows.
    5. Nội suy song tuyến (`bilinear`) tại đúng toạ độ `(latitude, longitude)` của 11 tỉnh,
       cộng `+7 giờ` về múi giờ `Asia/Bangkok` (UTC+7), điền hoàn chỉnh vào
       `data_test/weather_openmeteo_province_4y.csv` và kiểm tra đảm bảo 0% NaN.

Chạy:
    python -X utf8 code/crawl_data/fill_era5_blh_cds.py
"""

import io
import os
import sys
import time
import zipfile
from pathlib import Path

# Đảm bảo hiển thị tiếng Việt chuẩn trên terminal Windows
if hasattr(sys.stdout, "buffer") and getattr(sys.stdout, "encoding", "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import cdsapi
import netCDF4
import numpy as np
import pandas as pd
import xarray as xr

# ============================================================
# CẤU HÌNH CDS API & ĐƯỜNG DẪN
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_TEST_DIR = BASE_DIR / "data_test"
BRONZE_DIR = BASE_DIR / "lakehouse_storage" / "bronze"
CDS_CACHE_DIR = BRONZE_DIR / "era5_cds_blh"
CDS_CACHE_DIR.mkdir(parents=True, exist_ok=True)

TAG = os.getenv("SYNC_TAG", "4y")
WEATHER_CSV = DATA_TEST_DIR / f"weather_openmeteo_province_{TAG}.csv"

# Thông tin xác thực Copernicus CDS API
CDS_URL = os.getenv("CDSAPI_URL", "https://cds.climate.copernicus.eu/api")
CDS_KEY = os.getenv("CDSAPI_KEY", "c6e398d9-89e1-4c3a-a789-140f1ee9ae8b")

CDS_DATASET = "reanalysis-era5-single-levels"
UTC_OFFSET_HOURS = 7  # Asia/Bangkok (UTC+7)
MAX_RETRIES = 3

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


# ============================================================
# 1. TIỆN ÍCH GIẢI NÉN & ĐỌC NETCDF AN TOÀN VỚI ĐƯỜNG DẪN TIẾNG VIỆT
# ============================================================

def ensure_unpacked_netcdf(file_path: Path) -> Path:
    """
    CDS-Beta đôi khi trả về file ZIP chứa file `.nc` bên trong ngay cả khi
    chọn `netcdf`. Hàm này kiểm tra chữ ký ZIP (PK\\x03\\x04) và tự giải nén
    ra file `.nc` chuẩn nếu cần.
    """
    with open(file_path, "rb") as f:
        magic = f.read(4)

    if magic == b"PK\x03\x04":
        tmp_zip = file_path.with_suffix(".zip")
        file_path.replace(tmp_zip)
        with zipfile.ZipFile(tmp_zip, "r") as zf:
            nc_names = [n for n in zf.namelist() if n.endswith(".nc")]
            if not nc_names:
                raise RuntimeError(f"File ZIP từ CDS không chứa file .nc: {zf.namelist()}")
            extracted = zf.extract(nc_names[0], path=file_path.parent)
            Path(extracted).replace(file_path)
        tmp_zip.unlink(missing_ok=True)

    return file_path


def open_nc_unicode_safe(nc_path: Path) -> xr.Dataset:
    """
    Thư viện C `libnetcdf` trên Windows bị lỗi `FileNotFoundError` khi đường dẫn
    chứa ký tự tiếng Việt có dấu (như `d:\\năm 4\\...`).
    Giải pháp: đọc bytes bằng Python `Path.read_bytes()` rồi nạp từ bộ nhớ RAM.
    """
    ensure_unpacked_netcdf(nc_path)
    raw_bytes = nc_path.read_bytes()
    nc4_ds = netCDF4.Dataset(nc_path.name, mode="r", memory=raw_bytes)
    store = xr.backends.NetCDF4DataStore(nc4_ds)
    return xr.open_dataset(store)


def extract_station_blh_from_nc(nc_files: list, stations: pd.DataFrame) -> pd.DataFrame:
    """
    Đọc danh sách file NetCDF từ CDS, nội suy không gian tại toạ độ từng tỉnh
    và chuyển trục thời gian từ UTC sang giờ địa phương Việt Nam (UTC+7).
    """
    records = []

    for nc_path in nc_files:
        ds = open_nc_unicode_safe(nc_path)

        # Tìm tên biến BLH trong NetCDF ('blh' hoặc 'boundary_layer_height')
        var_name = None
        for cand in ["blh", "boundary_layer_height"]:
            if cand in ds.data_vars:
                var_name = cand
                break
        if var_name is None:
            var_name = list(ds.data_vars)[0]

        da = ds[var_name]

        # Chuẩn hóa tên chiều thời gian (CDS-Beta dùng 'valid_time', CDS cũ dùng 'time')
        if "valid_time" in da.dims and "time" not in da.dims:
            da = da.rename({"valid_time": "time"})

        # Nếu có chiều 'expver' (khi gộp ERA5 Final & ERA5T), gộp lấy giá trị hợp lệ
        if "expver" in da.dims:
            da = da.bfill("expver").isel(expver=0)

        # Bỏ các chiều đơn (như number=1 nếu có)
        for dim in list(da.dims):
            if dim not in ("time", "latitude", "longitude") and da.sizes[dim] == 1:
                da = da.isel({dim: 0}, drop=True)

        # Đảm bảo latitude và longitude tăng dần để nội suy song tuyến (bilinear)
        if da.latitude.values[0] > da.latitude.values[-1]:
            da = da.sortby("latitude")
        if da.longitude.values[0] > da.longitude.values[-1]:
            da = da.sortby("longitude")

        # Chuyển thời gian từ UTC sang giờ địa phương Việt Nam (UTC+7)
        utc_index = pd.to_datetime(da["time"].values)
        local_index = utc_index + pd.Timedelta(hours=UTC_OFFSET_HOURS)

        # Nội suy giá trị BLH cho từng tỉnh
        for _, row in stations.iterrows():
            lat = float(row["latitude"])
            lon = float(row["longitude"])

            interp_da = da.interp(latitude=lat, longitude=lon, method="linear")
            vals = interp_da.values.astype(np.float64)
            if np.isnan(vals).any():
                nearest_vals = da.sel(latitude=lat, longitude=lon, method="nearest").values.astype(np.float64)
                vals = np.where(np.isnan(vals), nearest_vals, vals)

            records.append(
                pd.DataFrame({
                    "station_code": row["station_code"],
                    "datetime": local_index,
                    "blh_era5_cds": vals,
                })
            )

        ds.close()

    out = pd.concat(records, ignore_index=True)
    out = out.drop_duplicates(subset=["station_code", "datetime"], keep="last")
    return out


def apply_blh_fill(df: pd.DataFrame, blh_df: pd.DataFrame):
    """Điền các giá trị `blh_era5_cds` vào những dòng đang bị NaN của `df`."""
    df = df.merge(blh_df, on=["station_code", "datetime"], how="left", validate="1:1")
    fill_mask = df["boundary_layer_height"].isna() & df["blh_era5_cds"].notna()
    n_filled = int(fill_mask.sum())

    df.loc[fill_mask, "boundary_layer_height"] = df.loc[fill_mask, "blh_era5_cds"]
    if "weather_source" in df.columns:
        df.loc[fill_mask, "weather_source"] = "archive+cds_era5_blh"
    df = df.drop(columns=["blh_era5_cds"])
    return df, n_filled


# ============================================================
# 2. XÁC ĐỊNH CỬA SỔ CÒN THIẾU & TẢI TRONG 1 REQUEST DUY NHẤT
# ============================================================

def build_single_cds_request(df: pd.DataFrame):
    """
    Xác định toàn bộ năm, tháng, ngày (UTC) còn thiếu `boundary_layer_height`
    để gộp thành 1 request `cdsapi` duy nhất.
    """
    missing_mask = df["boundary_layer_height"].isna()
    n_missing = int(missing_mask.sum())
    if n_missing == 0:
        return None, None, 0

    df_miss = df.loc[missing_mask]
    local_min = df_miss["datetime"].min()
    local_max = df_miss["datetime"].max()

    utc_times = df_miss["datetime"] - pd.Timedelta(hours=UTC_OFFSET_HOURS)
    utc_min = utc_times.min()
    utc_max = utc_times.max()

    years = sorted(utc_times.dt.year.unique().tolist())
    months = sorted(utc_times.dt.month.unique().tolist())
    days = sorted(utc_times.dt.day.unique().tolist())

    lat_min = float(df["latitude"].min())
    lat_max = float(df["latitude"].max())
    lon_min = float(df["longitude"].min())
    lon_max = float(df["longitude"].max())

    north = float(np.ceil((lat_max + 0.5) * 4) / 4)
    south = float(np.floor((lat_min - 0.5) * 4) / 4)
    west = float(np.floor((lon_min - 0.5) * 4) / 4)
    east = float(np.ceil((lon_max + 0.5) * 4) / 4)
    area = [north, west, south, east]

    print(f"\nCòn {n_missing:,} ô thiếu `boundary_layer_height` cần tải:")
    print(f"  - Giờ Việt Nam (UTC+7): {local_min} -> {local_max}")
    print(f"  - Giờ chuẩn ERA5 (UTC): {utc_min} -> {utc_max}")
    print(f"  - Năm UTC             : {years}")
    print(f"  - Tháng UTC (gộp 1 lần): {[f'{m:02d}' for m in months]}")
    print(f"  - Bounding Box [N,W,S,E]: {area}")

    request = {
        "product_type": ["reanalysis"],
        "variable": ["boundary_layer_height"],
        "year": [f"{y:04d}" for y in years],
        "month": [f"{m:02d}" for m in months],
        "day": [f"{d:02d}" for d in days],
        "time": [f"{h:02d}:00" for h in range(24)],
        "data_format": "netcdf",
        "download_format": "unarchived",
        "area": area,
    }
    tag_name = f"era5_blh_{'_'.join(str(y) for y in years)}_m{months[0]:02d}_to_m{months[-1]:02d}.nc"
    out_nc = CDS_CACHE_DIR / tag_name
    return request, out_nc, n_missing


def download_single_batch(client: cdsapi.Client, request: dict, out_nc: Path) -> Path:
    """Gửi 1 request duy nhất lên CDS để lấy toàn bộ các tháng còn thiếu."""
    if out_nc.exists() and out_nc.stat().st_size > 1000:
        print(f"  [SKIP] Đã có sẵn file {out_nc.name} ({out_nc.stat().st_size:,} bytes)")
        return ensure_unpacked_netcdf(out_nc)

    tmp_nc = out_nc.with_suffix(".nc.part")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(
                f"\n[CDS SINGLE REQUEST] Đang gửi 1 request gộp cho các tháng {request['month']} "
                f"(lần {attempt}/{MAX_RETRIES})..."
            )
            client.retrieve(CDS_DATASET, request, str(tmp_nc))
            tmp_nc.replace(out_nc)
            ensure_unpacked_netcdf(out_nc)
            print(f"[OK] Đã tải xong file gộp: {out_nc.name} ({out_nc.stat().st_size:,} bytes)")
            return out_nc
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] Lần {attempt}/{MAX_RETRIES} lỗi khi tải từ CDS: {exc}")
            tmp_nc.unlink(missing_ok=True)
            if attempt == MAX_RETRIES:
                raise
            time.sleep(5 * attempt)

    return out_nc


# ============================================================
# 3. MAIN PIPELINE
# ============================================================

def save_weather_csv(df: pd.DataFrame) -> None:
    """Lưu file an toàn (atomic write qua file .tmp) để bấm Ctrl+C không làm cụt file CSV."""
    out = df.sort_values(["station_code", "datetime"]).reset_index(drop=True).copy()
    out["datetime"] = out["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    tmp_csv = WEATHER_CSV.with_suffix(".csv.tmp")
    out.to_csv(tmp_csv, index=False, encoding="utf-8-sig")
    tmp_csv.replace(WEATHER_CSV)


def main():
    print("=" * 72)
    print("  ERA5 FINAL (CDS API - 1 REQUEST DUY NHẤT) -- BỔ SUNG BOUNDARY_LAYER_HEIGHT")
    print("=" * 72)

    if not WEATHER_CSV.exists():
        raise FileNotFoundError(
            f"Không tìm thấy file thời tiết: {WEATHER_CSV}\n"
            f"Hãy chạy `python -X utf8 code/crawl_data/fetch_openmeteo_weather.py` trước."
        )

    df = pd.read_csv(
        WEATHER_CSV,
        dtype={"station_id": str, "station_code": str},
        parse_dates=["datetime"],
    )
    print(f"Đọc file: {WEATHER_CSV} ({len(df):,} dòng)")

    stations = (
        df[["station_code", "province_name", "latitude", "longitude"]]
        .drop_duplicates("station_code")
        .sort_values("station_code")
        .reset_index(drop=True)
    )

    # Bước 1: Tận dụng ngay các file .nc đã có sẵn trong thư mục cache (vd: era5_blh_2024-01.nc)
    existing_nc = sorted(
        p for p in CDS_CACHE_DIR.glob("*.nc") if p.is_file() and p.stat().st_size > 1000
    )
    if existing_nc and int(df["boundary_layer_height"].isna().sum()) > 0:
        print(f"\nTìm thấy {len(existing_nc)} file NetCDF đã tải sẵn trong {CDS_CACHE_DIR.name}:")
        for p in existing_nc:
            print(f"  - {p.name} ({p.stat().st_size:,} bytes)")
        blh_cached = extract_station_blh_from_nc(existing_nc, stations)
        df, n_filled_cache = apply_blh_fill(df, blh_cached)
        if n_filled_cache > 0:
            save_weather_csv(df)
            print(f"  -> Đã điền ngay {n_filled_cache:,} ô từ file có sẵn và lưu vào {WEATHER_CSV.name}!")

    # Bước 2: Kiểm tra số ô còn thiếu và gửi 1 request duy nhất cho toàn bộ phần còn lại
    request, out_nc, n_missing_rem = build_single_cds_request(df)
    if n_missing_rem == 0:
        print("\n[OK] Toàn bộ cột `boundary_layer_height` đã đầy đủ 100% (0 NaN)!")
        print(df[HOURLY_VARS].isna().sum().to_string())
        return

    print(f"\nKết nối Copernicus CDS API: {CDS_URL}")
    client = cdsapi.Client(url=CDS_URL, key=CDS_KEY)

    nc_path = download_single_batch(client, request, out_nc)

    print("\nĐang trích xuất & nội suy không gian `boundary_layer_height` cho 11 tỉnh...")
    blh_new = extract_station_blh_from_nc([nc_path], stations)
    df, n_filled_new = apply_blh_fill(df, blh_new)

    n_missing_after = int(df["boundary_layer_height"].isna().sum())
    print(f"\nĐã điền thêm: {n_filled_new:,} / {n_missing_rem:,} ô `boundary_layer_height` từ ERA5 Final (CDS).")
    print(f"Số ô `boundary_layer_height` còn thiếu sau khi điền: {n_missing_after}")

    print("\nKiểm tra toàn bộ 8 biến thời tiết sau khi cập nhật:")
    print(df[HOURLY_VARS].isna().sum().to_string())

    if "weather_source" in df.columns:
        print("\nThống kê nguồn dữ liệu (`weather_source`):")
        print(df["weather_source"].value_counts().to_string())

    save_weather_csv(df)
    print(f"\n[DONE] Đã cập nhật hoàn chỉnh file: {WEATHER_CSV}")


if __name__ == "__main__":
    main()
