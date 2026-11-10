import io
import os
import sys
from pathlib import Path

# Fix encoding cho terminal Windows (cp1258 khong ho tro unicode tieng Viet)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import requests
import pandas as pd
import time
from datetime import datetime, timedelta

# ============================================================
# CONFIG
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = BASE_DIR / "lakehouse_storage" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://tedp.vn/api"

# Khoảng thời gian muốn crawl: mặc định 4 NĂM GẦN NHẤT (giống chiến lược KnowAir
# 2015-2018 = 4 năm trọn chu kỳ mùa), kết thúc lúc 23:59:59 của ngày hôm qua.
#   - Đổi số năm         : set TEDP_YEARS=3
#   - Cố định khoảng ngày : set TEDP_START_DATE=2022-11-07T00:00:00 / TEDP_END_DATE=...
CRAWL_YEARS = int(os.getenv("TEDP_YEARS", "4"))


def _default_window(years):
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end = today - timedelta(seconds=1)                  # 23:59:59 hôm qua
    try:
        start = today.replace(year=today.year - years)
    except ValueError:                                   # 29/02 -> 28/02
        start = today.replace(year=today.year - years, day=28)
    return start, end


_DEFAULT_START, _DEFAULT_END = _default_window(CRAWL_YEARS)
START_DATE = os.getenv("TEDP_START_DATE") or _DEFAULT_START.strftime("%Y-%m-%dT%H:%M:%S")
END_DATE = os.getenv("TEDP_END_DATE") or _DEFAULT_END.strftime("%Y-%m-%dT%H:%M:%S")

# Mỗi request lấy tối đa khoảng 24 giờ
CHUNK_HOURS = 24

# Số station ID gửi trong một request
STATION_BATCH_SIZE = 30

REQUEST_DELAY = 0.05
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

# Checkpoint theo THÁNG: 4 năm ~ 1.461 ngày ~ hàng nghìn request -> nếu crawl bị
# ngắt giữa chừng, chạy lại sẽ bỏ qua các tháng đã lưu đủ (resume).
#   - Bắt buộc crawl lại toàn bộ: set TEDP_FORCE_RECRAWL=1
RAW_PARTITION_DIR = BRONZE_DIR / "tedp_raw_monthly"
RAW_PARTITION_DIR.mkdir(parents=True, exist_ok=True)
FORCE_RECRAWL = os.getenv("TEDP_FORCE_RECRAWL", "0") == "1"

RAW_COLUMNS = [
    "province_id", "province_name", "station_id", "station_code", "station_name",
    "latitude", "longitude", "datetime", "pm25", "aqi", "pm10",
]

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0"
}


# ============================================================
# HTTP HELPER
# ============================================================

session = requests.Session()
session.headers.update(HEADERS)


def get_json(url, params=None):
    """
    Gửi GET request và trả về JSON.
    """

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT
            )

            response.raise_for_status()

            return response.json()

        except (requests.RequestException, ValueError) as e:
            print(f"[ERROR] {url} (lần {attempt}/{MAX_RETRIES})")
            print(e)
            if attempt < MAX_RETRIES:
                time.sleep(2 * attempt)

    return None


# ============================================================
# 1. LẤY DANH SÁCH TỈNH/THÀNH
# ============================================================

def get_provinces():

    url = f"{BASE_URL}/province"

    params = {
        "size": 1000
    }

    data = get_json(url, params)

    if not data:
        return []

    provinces = data.get("_embedded", {}).get("province", [])

    result = []

    for p in provinces:

        result.append({
            "province_id": p.get("id"),
            "province_name": (
                p.get("name")
                or p.get("provinceName")
                or p.get("province_name")
            )
        })

    return result


# ============================================================
# 2. LẤY DANH SÁCH TRẠM QUAN TRẮC KHÔNG KHÍ
# ============================================================

def get_stations():

    url = (
        f"{BASE_URL}/stations/search/"
        "findByIsPublicAndStationTypeAndNullableProvinceId"
    )

    params = {
        "stationType": 4,
        "isPublic": "true"
    }

    data = get_json(url, params)

    if not data:
        return []

    stations = data.get("_embedded", {}).get("stations", [])

    result = []

    for s in stations:

        result.append({
            "province_id": s.get("provinceId"),
            "station_id": s.get("stationId"),
            "station_code": s.get("stationCode"),
            "station_name": s.get("stationName"),
            "latitude": s.get("latitude"),
            "longitude": s.get("longitude"),
            "station_type": s.get("stationType"),
            "is_public": s.get("isPublic")
        })

    return result


# ============================================================
# 3. LẤY AQI + PM2.5 THEO GIỜ CHO NHIỀU TRẠM
# ============================================================

def get_hourly_aqi(station_ids, start_time, end_time):

    if not station_ids:
        return []

    url = (
        f"{BASE_URL}/aqi_hour/search/"
        "findByStationIdInAndGetTimeBetweenOrderByGetTimeDesc"
    )

    params = {
        "stationIds": ",".join(map(str, station_ids)),
        "getTimeStart": start_time,
        "getTimeEnd": end_time
    }

    data = get_json(url, params)

    # None = request lỗi sau MAX_RETRIES lần (khác với "không có dữ liệu")
    if data is None:
        return None

    if not data:
        return []

    # TEDP có thể trả dạng:
    # {"_embedded": {"aqi_hour": [...]}}
    #
    # hoặc trực tiếp là list tùy response.
    if isinstance(data, dict):

        records = (
            data.get("_embedded", {}).get("aqi_hour")
            or data.get("_embedded", {}).get("aqiHour")
            or data.get("content")
            or []
        )

    elif isinstance(data, list):

        records = data

    else:
        records = []

    return records


# ============================================================
# 4. CHUYỂN RESPONSE TEDP THÀNH ROW DATASET
# ============================================================

def parse_aqi_records(records, station_lookup):

    rows = []

    for record in records:

        station_id = record.get("stationId")

        station = station_lookup.get(str(station_id), {})

        data = record.get("data") or {}

        # ----------------------------------------------------
        # PM205 trong TEDP = PM2.5
        # ----------------------------------------------------

        pm25 = data.get("PM205")

        row = {
            "province_id": station.get("province_id"),
            "province_name": station.get("province_name"),

            "station_id": station_id,
            "station_code": station.get("station_code"),
            "station_name": station.get("station_name"),

            "latitude": station.get("latitude"),
            "longitude": station.get("longitude"),

            "datetime": record.get("getTime"),

            "pm25": pm25,

            "aqi": data.get("aqi"),

            "pm10": data.get("PM10"),
        }

        rows.append(row)

    return rows


# ============================================================
# 5. CRAWL THEO CỬA SỔ THỜI GIAN (CHECKPOINT THEO THÁNG)
# ============================================================

def month_windows(start, end):
    """
    Chia [start, end] thành từng tháng dương lịch.
    Trả về (m_start, m_end, is_full_month): is_full_month = True nếu cửa sổ
    phủ trọn tháng (dùng để quyết định có lưu checkpoint hay không).
    """

    cur = start

    while cur < end:

        month_first = cur.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        next_month = (month_first + timedelta(days=32)).replace(day=1)
        m_end = min(next_month - timedelta(seconds=1), end)

        is_full = (cur == month_first) and (m_end == next_month - timedelta(seconds=1))

        yield cur, m_end, is_full

        cur = next_month


def crawl_range(station_ids, station_lookup, start, end):
    """
    Crawl [start, end] theo từng khối CHUNK_HOURS giờ x STATION_BATCH_SIZE trạm.
    Trả về (rows, số request lỗi).
    """

    rows_out = []
    n_failed = 0
    current_start = start

    while current_start < end:

        current_end = min(
            current_start + timedelta(hours=CHUNK_HOURS),
            end
        )

        start_str = current_start.strftime("%Y-%m-%dT%H:%M:%S")
        end_str = current_end.strftime("%Y-%m-%dT%H:%M:%S")

        n_records = 0

        for i in range(0, len(station_ids), STATION_BATCH_SIZE):

            batch = station_ids[i:i + STATION_BATCH_SIZE]

            records = get_hourly_aqi(batch, start_str, end_str)

            if records is None:
                n_failed += 1
                print(f"  [FAIL] {start_str} | trạm {i + 1}-{i + len(batch)}")
                continue

            rows_out.extend(parse_aqi_records(records, station_lookup))
            n_records += len(records)

            time.sleep(REQUEST_DELAY)

        print(f"  {start_str} -> {end_str}: {n_records} records")

        current_start = current_end

    return rows_out, n_failed


# ============================================================
# 6. MAIN CRAWLER
# ============================================================

def crawl():

    print("=" * 70)
    print("TEDP AQI / PM2.5 CRAWLER")
    print("=" * 70)

    # --------------------------------------------------------
    # Provinces
    # --------------------------------------------------------

    print("\n[1/4] Getting provinces...")

    provinces = get_provinces()

    print(f"Found {len(provinces)} provinces")

    province_lookup = {
        str(p["province_id"]): p["province_name"]
        for p in provinces
    }

    # --------------------------------------------------------
    # Stations
    # --------------------------------------------------------

    print("\n[2/4] Getting air-quality stations...")

    stations = get_stations()

    print(f"Found {len(stations)} stations")

    # Gắn province name
    for station in stations:

        province_id = str(station["province_id"])

        station["province_name"] = province_lookup.get(
            province_id,
            "Unknown"
        )

    # Lookup station
    station_lookup = {
        str(s["station_id"]): s
        for s in stations
    }

    # --------------------------------------------------------
    # Save station list
    # --------------------------------------------------------

    stations_df = pd.DataFrame(stations)

    stations_file = BRONZE_DIR / "tedp_stations.csv"
    stations_df.to_csv(
        stations_file,
        index=False,
        encoding="utf-8-sig"
    )

    print(f"Saved: {stations_file}")

    # --------------------------------------------------------
    # Time range (mặc định 4 năm gần nhất) + crawl theo từng THÁNG
    # --------------------------------------------------------

    start = datetime.fromisoformat(START_DATE)
    end = datetime.fromisoformat(END_DATE)

    total_hours = int(
        (end - start).total_seconds() / 3600
    )

    print(
        f"\nTime range: {start} -> {end}"
    )

    print(
        f"Approximately {total_hours:,} hours "
        f"(~{total_hours / 24 / 365.25:.2f} năm)"
    )

    station_ids = [
        s["station_id"]
        for s in stations
        if s["station_id"]
    ]

    print(f"Checkpoint dir: {RAW_PARTITION_DIR}")

    month_frames = []
    n_skipped, n_crawled, n_incomplete = 0, 0, 0

    for m_start, m_end, is_full_month in month_windows(start, end):

        tag = m_start.strftime("%Y-%m")
        part_file = RAW_PARTITION_DIR / f"tedp_{tag}.csv"

        # Tháng đã crawl đủ trước đó -> bỏ qua (resume)
        if is_full_month and part_file.exists() and not FORCE_RECRAWL:
            month_frames.append(
                pd.read_csv(part_file, dtype={"province_id": str, "station_id": str})
            )
            n_skipped += 1
            print(f"\n[SKIP] {tag}: đã có checkpoint {part_file.name}")
            continue

        print(f"\n[MONTH] {tag}: {m_start} -> {m_end}")

        rows, n_failed = crawl_range(
            station_ids,
            station_lookup,
            m_start,
            m_end
        )

        month_df = pd.DataFrame(rows, columns=RAW_COLUMNS)
        month_frames.append(month_df)
        n_crawled += 1

        # Chỉ lưu checkpoint khi tháng TRỌN VẸN và KHÔNG có request lỗi
        # -> lần chạy sau sẽ crawl lại các tháng thiếu/lỗi.
        if is_full_month and n_failed == 0:
            month_df.to_csv(part_file, index=False, encoding="utf-8-sig")
            print(f"  -> {len(month_df):,} records, lưu {part_file.name}")
        else:
            n_incomplete += 1
            reason = "tháng chưa trọn" if not is_full_month else f"{n_failed} request lỗi"
            print(f"  -> {len(month_df):,} records, KHÔNG lưu checkpoint ({reason})")

    print(
        f"\nTháng: crawl mới {n_crawled} | bỏ qua (đã có) {n_skipped} | "
        f"chưa checkpoint {n_incomplete}"
    )

    # ========================================================
    # DATAFRAME
    # ========================================================

    print("\n[3/4] Creating dataset...")

    month_frames = [f for f in month_frames if not f.empty]

    if not month_frames:

        print("No data returned.")

        return

    df = pd.concat(month_frames, ignore_index=True)

    # --------------------------------------------------------
    # Convert datetime
    # --------------------------------------------------------

    df["datetime"] = pd.to_datetime(
        df["datetime"],
        errors="coerce"
    )

    # --------------------------------------------------------
    # Convert numeric columns
    # --------------------------------------------------------

    numeric_columns = [
        "latitude",
        "longitude",
        "pm25",
        "aqi",
        "pm10",
    ]

    for column in numeric_columns:

        if column in df.columns:

            df[column] = pd.to_numeric(
                df[column],
                errors="coerce"
            )

    # --------------------------------------------------------
    # Remove duplicate records
    # --------------------------------------------------------

    df = df.drop_duplicates(
        subset=[
            "station_id",
            "datetime"
        ]
    )

    # Chỉ giữ bản ghi nằm trong cửa sổ crawl (biên chunk có thể trả dư)
    df = df[
        (df["datetime"] >= start) & (df["datetime"] <= end)
    ]

    # --------------------------------------------------------
    # Sort
    # --------------------------------------------------------

    df = df.sort_values(
        [
            "province_name",
            "station_name",
            "datetime"
        ]
    )

    # --------------------------------------------------------
    # Save
    # --------------------------------------------------------

    output_file = BRONZE_DIR / "tedp_aqi_pm25_hourly.csv"

    df.to_csv(
        output_file,
        index=False,
        encoding="utf-8-sig"
    )

    # ========================================================
    # COVERAGE REPORT
    # ========================================================

    print("\n[4/4] Coverage report")
    print("=" * 70)

    print(
        f"Total records : {len(df):,}"
    )

    print(
        f"Stations      : "
        f"{df['station_id'].nunique():,}"
    )

    print(
        f"Provinces     : "
        f"{df['province_id'].nunique():,}"
    )

    print(
        f"PM2.5 records : "
        f"{df['pm25'].notna().sum():,}"
    )

    print(
        f"Date range    : "
        f"{df['datetime'].min()} -> "
        f"{df['datetime'].max()}"
    )

    print("\nProvince coverage:")

    coverage = (
        df.groupby(
            ["province_id", "province_name"]
        )
        .agg(
            stations=(
                "station_id",
                "nunique"
            ),
            records=(
                "station_id",
                "size"
            ),
            pm25_records=(
                "pm25",
                "count"
            )
        )
        .reset_index()
    )

    print(
        coverage.to_string(index=False)
    )

    coverage_file = BRONZE_DIR / "tedp_province_coverage.csv"
    coverage.to_csv(
        coverage_file,
        index=False,
        encoding="utf-8-sig"
    )

    # --------------------------------------------------------
    # Station coverage: với 4 năm, nhiều trạm có thể mới lắp
    # -> cần biết trạm bắt đầu / kết thúc có dữ liệu khi nào
    # --------------------------------------------------------

    expected_hours = int((end - start).total_seconds() // 3600) + 1

    station_cov = (
        df.groupby(["province_name", "station_code", "station_id"])
        .agg(
            first_time=("datetime", "min"),
            last_time=("datetime", "max"),
            records=("datetime", "size"),
            pm25_records=("pm25", "count"),
        )
        .reset_index()
    )
    station_cov["pm25_coverage_pct"] = (
        100 * station_cov["pm25_records"] / expected_hours
    ).round(2)
    station_cov = station_cov.sort_values(["province_name", "station_code"])

    print(f"\nStation coverage (trên {expected_hours:,} giờ của cửa sổ):")
    print(
        station_cov[
            ["province_name", "station_code", "first_time", "last_time", "pm25_coverage_pct"]
        ].to_string(index=False)
    )

    station_cov_file = BRONZE_DIR / "tedp_station_coverage.csv"
    station_cov.to_csv(
        station_cov_file,
        index=False,
        encoding="utf-8-sig"
    )

    print("\nFiles created:")
    print(f"  - {stations_file}")
    print(f"  - {output_file}")
    print(f"  - {coverage_file}")
    print(f"  - {station_cov_file}")


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    crawl()