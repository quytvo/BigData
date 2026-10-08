import requests
import pandas as pd
import time
from datetime import datetime, timedelta

# ============================================================
# CONFIG
# ============================================================

BASE_URL = "https://tedp.vn/api"

# Khoảng thời gian muốn crawl
START_DATE = "2026-09-01T00:00:00"
END_DATE = "2026-10-03T23:59:59"

# Mỗi request lấy tối đa khoảng 24 giờ
CHUNK_HOURS = 24

# Số station ID gửi trong một request
STATION_BATCH_SIZE = 30

REQUEST_DELAY = 0.3
REQUEST_TIMEOUT = 30

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

    try:
        response = session.get(
            url,
            params=params,
            timeout=REQUEST_TIMEOUT
        )

        response.raise_for_status()

        return response.json()

    except requests.RequestException as e:
        print(f"[ERROR] {url}")
        print(e)
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
            "no2": data.get("NO2"),
            "so2": data.get("SO2"),
            "o3": data.get("O3"),
            "co": data.get("CO")
        }

        rows.append(row)

    return rows


# ============================================================
# 5. MAIN CRAWLER
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

    stations_df.to_csv(
        "tedp_stations.csv",
        index=False,
        encoding="utf-8-sig"
    )

    print("Saved: tedp_stations.csv")

    # --------------------------------------------------------
    # Time range
    # --------------------------------------------------------

    start = datetime.fromisoformat(START_DATE)
    end = datetime.fromisoformat(END_DATE)

    all_rows = []

    current_start = start

    # --------------------------------------------------------
    # Crawl in time chunks
    # --------------------------------------------------------

    total_hours = int(
        (end - start).total_seconds() / 3600
    )

    print(
        f"\nTime range: {start} -> {end}"
    )

    print(
        f"Approximately {total_hours:,} hours"
    )

    while current_start < end:

        current_end = min(
            current_start + timedelta(hours=CHUNK_HOURS),
            end
        )

        start_str = current_start.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

        end_str = current_end.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

        print(
            f"\nTime window: {start_str} -> {end_str}"
        )

        # ----------------------------------------------------
        # Split stations into batches
        # ----------------------------------------------------

        station_ids = [
            s["station_id"]
            for s in stations
            if s["station_id"]
        ]

        for i in range(
            0,
            len(station_ids),
            STATION_BATCH_SIZE
        ):

            batch = station_ids[
                i:i + STATION_BATCH_SIZE
            ]

            print(
                f"  Stations "
                f"{i + 1}-{i + len(batch)} "
                f"/ {len(station_ids)}"
            )

            records = get_hourly_aqi(
                batch,
                start_str,
                end_str
            )

            rows = parse_aqi_records(
                records,
                station_lookup
            )

            all_rows.extend(rows)

            print(
                f"    Received: {len(records)} records"
            )

            time.sleep(REQUEST_DELAY)

        current_start = current_end

    # ========================================================
    # DATAFRAME
    # ========================================================

    print("\n[3/4] Creating dataset...")

    df = pd.DataFrame(all_rows)

    if df.empty:

        print("No data returned.")

        return

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
        "no2",
        "so2",
        "o3",
        "co"
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

    output_file = "tedp_aqi_pm25_hourly.csv"

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

    # Save coverage
    coverage.to_csv(
        "tedp_province_coverage.csv",
        index=False,
        encoding="utf-8-sig"
    )

    print("\nFiles created:")
    print("  - tedp_stations.csv")
    print("  - tedp_aqi_pm25_hourly.csv")
    print("  - tedp_province_coverage.csv")


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    crawl()