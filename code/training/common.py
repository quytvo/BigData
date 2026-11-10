"""
Thư viện dùng chung cho việc train mô hình dự báo PM2.5 theo TỪNG TỈNH.

Mỗi file train_<tinh>.py chỉ khai báo tên tỉnh rồi gọi run_province(); toàn bộ
logic (nạp dữ liệu, tạo đặc trưng, chia train/test, baseline, train/evaluate,
lưu kết quả) nằm ở đây để đảm bảo mọi tỉnh được xử lý GIỐNG HỆT nhau.

Bài toán
    - Dự báo pm25 tại thời điểm (t + HORIZON) từ thông tin biết được tại t.
    - HORIZON = 24 giờ.

Đặc trưng (tính cho từng trạm rồi gộp mọi trạm của tỉnh)
    - pm25 hiện tại và các độ trễ: lag 0, 1, 3, 6, 12, 24 giờ
    - trung bình trượt pm25: 6 giờ, 24 giờ
    - thời tiết TẠI THỜI ĐIỂM CẦN DỰ BÁO (t + HORIZON): nhiệt độ, độ ẩm, áp suất, mưa, gió
      [ASSUMPTION] giả định có dự báo thời tiết cho 24 giờ tới.
    - giờ trong ngày, thứ trong tuần, cuối tuần của thời điểm cần dự báo
    - mã trạm (để phân biệt các trạm trong cùng tỉnh)

Chia dữ liệu: THEO THỜI GIAN (không ngẫu nhiên), tỉ lệ train/val/test = 70/10/20.
    - train: dùng để học
    - val  : dùng để early stopping (chọn số cây tối ưu), không học trực tiếp
    - test : chỉ dùng để đánh giá cuối
Baseline: "giờ này giống 24 giờ trước" (persistence). Mỗi model được so sánh
độc lập với baseline.
"""

import io
import json
import os
import re
import sys
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

# Fix encoding cho terminal Windows (cp1258 khong ho tro unicode tieng Viet)
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import numpy as np
import pandas as pd

try:
    import joblib
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.svm import SVR
except ImportError:
    joblib = None  # type: ignore
    SVR = None  # type: ignore

try:
    import lightgbm as lgb
except ImportError:
    lgb = None  # type: ignore

try:
    import xgboost as xgb
except ImportError:
    xgb = None  # type: ignore

# ============================================================
# CẤU HÌNH CHUNG
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data_test"

# ============================================================
# CẤU HÌNH ĐƯỜNG DẪN LAKEHOUSE STORAGE (SILVER & GOLD)
# ============================================================
LAKEHOUSE_DIR = BASE_DIR / "lakehouse_storage"
SILVER_DIR = LAKEHOUSE_DIR / "silver"
SILVER_PROVINCE_DIR = SILVER_DIR / "tedp_province"
SILVER_WEATHER_DIR = SILVER_DIR / "weather_province"
GOLD_DIR = LAKEHOUSE_DIR / "gold"
GOLD_FEATURES_DIR = GOLD_DIR / "global_ann_features"
PARQUET_DIR = SILVER_PROVINCE_DIR

# Chọn bộ dữ liệu bằng biến môi trường DATASET_TAG:
#   - "4y"  (mặc định): bản 4 năm, TRUNG BÌNH CÁC TRẠM THEO TỈNH (1 nút = 1 tỉnh)
#                       <- sync_tedp_province.py + fetch_openmeteo_weather.py
#   - "120d"          : bản cũ 120 ngày, từng trạm riêng (để tái lập kết quả cũ)
DATASET_TAG = os.getenv("DATASET_TAG", "4y")
if DATASET_TAG == "120d":
    TEDP_CSV = DATA_DIR / "cleaned_tedp_synced_120d.csv"
    WEATHER_CSV = DATA_DIR / "weather_openmeteo_station_120d.csv"
    OUTPUT_SUFFIX = ""          # giữ nguyên thư mục output cũ
else:
    TEDP_CSV = DATA_DIR / f"cleaned_tedp_province_{DATASET_TAG}.csv"
    WEATHER_CSV = DATA_DIR / f"weather_openmeteo_province_{DATASET_TAG}.csv"
    OUTPUT_SUFFIX = f"_{DATASET_TAG}"   # không ghi đè kết quả 120 ngày
OUTPUT_DIR = BASE_DIR / "output" / f"province_models{OUTPUT_SUFFIX}"

MIN_ROWS = 1000          # ngưỡng số dòng tối thiểu để một tỉnh được train
HORIZON = 24             # dự báo trước bao nhiêu giờ
TRAIN_FRAC = 0.70        # tỉ lệ mốc thời gian dùng để train
VAL_FRAC = 0.10          # tỉ lệ dùng để validation (test = phần còn lại = 20%)
EARLY_STOPPING_ROUNDS = 30
PM25_LAGS = [0, 1, 3, 6, 12, 24]
ROLL_WINDOWS = [6, 24]
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
THERMO_DERIVED_COLS = [
    "wind_dir_sin",
    "wind_dir_cos",
    "dew_point_deficit",
    "ventilation_coef",
    "temp_diff_24h",
    "pressure_diff_24h",
    "pblh_diff_24h",
    "precip_sum_24h",
]

LGB_PARAMS = dict(
    objective="regression",
    n_estimators=500,      # tối đa; early stopping sẽ dừng sớm theo tập val
    learning_rate=0.05,
    num_leaves=8,          # nhỏ để hạn chế overfit (ít dữ liệu)
    min_child_samples=20,
    subsample=0.8,
    subsample_freq=1,
    colsample_bytree=0.8,
    reg_lambda=1.0,
    random_state=42,
    verbose=-1,
)

MODEL_ALIASES = {
    "linear_regression": "linear_regression",
    "linear": "linear_regression",
    "svr": "svr",
    "random_forest": "random_forest",
    "rf": "random_forest",
    "xgboost": "xgboost",
    "xgb": "xgboost",
    "lightgbm": "lightgbm",
    "lgbm": "lightgbm",
}
DEFAULT_MODELS = ["lightgbm"]
SUPPORTED_MODELS = tuple(dict.fromkeys(MODEL_ALIASES.values()))


# ============================================================
# TIỆN ÍCH
# ============================================================

def slugify(name):
    """'Hà Nội' -> 'ha_noi' (dùng làm tên file/thư mục)."""
    s = unicodedata.normalize("NFD", name).replace("đ", "d").replace("Đ", "D")
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def rmse(y_true, y_pred):
    yt = np.asarray(y_true, dtype=np.float64)
    yp = np.asarray(y_pred, dtype=np.float64)
    return float(np.sqrt(np.mean((yt - yp) ** 2)))


def metrics(y_true, y_pred):
    yt = np.asarray(y_true, dtype=np.float64)
    yp = np.asarray(y_pred, dtype=np.float64)
    mse_val = float(np.mean((yt - yp) ** 2))
    mae_val = float(np.mean(np.abs(yt - yp)))
    ss_res = float(np.sum((yt - yp) ** 2))
    ss_tot = float(np.sum((yt - np.mean(yt)) ** 2))
    r2_val = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
    return {
        "rmse": float(np.sqrt(mse_val)),
        "mae": mae_val,
        "r2": float(r2_val),
    }


# ============================================================
# 1. NẠP DỮ LIỆU + LỌC TỈNH THEO NGƯỠNG
# ============================================================

def load_merged(province_name: Optional[str] = None, use_parquet: bool = True) -> pd.DataFrame:
    """Nạp TEDP + thời tiết rồi ghép theo (station_code, datetime).

    Hỗ trợ đọc từ tầng Silver Parquet với Partition Pruning theo province_name.
    Tự động fallback về file CSV nếu use_parquet=False hoặc không tìm thấy Parquet.
    """
    can_use_parquet = (
        use_parquet
        and SILVER_DIR.exists()
        and SILVER_PROVINCE_DIR.exists()
        and SILVER_WEATHER_DIR.exists()
    )

    if can_use_parquet:
        try:
            if province_name is not None:
                filters = [("province_name", "==", province_name)]
                tedp = pd.read_parquet(SILVER_PROVINCE_DIR, filters=filters)
                weather = pd.read_parquet(
                    SILVER_WEATHER_DIR,
                    columns=["station_code", "datetime"] + WEATHER_COLS,
                    filters=filters,
                )
            else:
                tedp = pd.read_parquet(SILVER_PROVINCE_DIR)
                weather = pd.read_parquet(
                    SILVER_WEATHER_DIR,
                    columns=["station_code", "datetime"] + WEATHER_COLS,
                )

            # Chuẩn hóa kiểu dữ liệu định danh và timestamp
            if "province_name" in tedp.columns:
                tedp["province_name"] = tedp["province_name"].astype(str)
            if "station_code" in tedp.columns:
                tedp["station_code"] = tedp["station_code"].astype(str)
            if "station_code" in weather.columns:
                weather["station_code"] = weather["station_code"].astype(str)
            if "datetime" in tedp.columns:
                tedp["datetime"] = pd.to_datetime(tedp["datetime"])
            if "datetime" in weather.columns:
                weather["datetime"] = pd.to_datetime(weather["datetime"])

            df = tedp.merge(weather, on=["station_code", "datetime"], how="left", validate="1:1")

            # Đảm bảo chỉ chứa đúng province_name nếu được chỉ định
            if province_name is not None and "province_name" in df.columns:
                df = df[df["province_name"] == province_name].copy()

            missing = int(df[WEATHER_COLS].isna().sum().sum())
            if missing:
                # Nếu thiếu dữ liệu thời tiết sau khi ghép từ Parquet, fallback về CSV
                pass
            else:
                return df
        except Exception:
            # Fallback trong trường hợp Parquet bị lỗi hoặc không thể đọc
            pass

    # Fallback tự động: Nạp từ file CSV thô
    if not TEDP_CSV.exists() or not WEATHER_CSV.exists():
        raise FileNotFoundError(f"Không tìm thấy file CSV tại {TEDP_CSV} hoặc {WEATHER_CSV}")

    tedp = pd.read_csv(TEDP_CSV, parse_dates=["datetime"])
    weather = pd.read_csv(
        WEATHER_CSV,
        usecols=["station_code", "datetime"] + WEATHER_COLS,
        parse_dates=["datetime"],
    )
    df = tedp.merge(weather, on=["station_code", "datetime"], how="left", validate="1:1")

    if province_name is not None:
        df = df[df["province_name"] == province_name].copy()

    missing = int(df[WEATHER_COLS].isna().sum().sum())
    if missing:
        raise ValueError(f"Có {missing} ô thời tiết bị thiếu sau khi ghép - kiểm tra lại dữ liệu")
    return df


def province_row_counts(df):
    """Số dòng và số trạm của từng tỉnh."""
    return (
        df.groupby("province_name")
        .agg(rows=("datetime", "size"), stations=("station_code", "nunique"))
        .sort_values("rows", ascending=False)
    )


def eligible_provinces(df, min_rows=MIN_ROWS):
    counts = province_row_counts(df)
    return counts[counts["rows"] >= min_rows]


# ============================================================
# 2. TẠO ĐẶC TRƯNG
# ============================================================

def build_features(df_province):
    """Tạo bảng (đặc trưng, nhãn) cho một tỉnh. Mỗi trạm được xử lý riêng rồi gộp lại."""
    parts = []
    for code, g in df_province.groupby("station_code"):
        g = g.sort_values("datetime").reset_index(drop=True)
        out = pd.DataFrame({"station_code": code, "origin_time": g["datetime"]})

        for lag in PM25_LAGS:
            out[f"pm25_lag{lag}"] = g["pm25"].shift(lag)
        for w in ROLL_WINDOWS:
            out[f"pm25_roll{w}"] = g["pm25"].rolling(w, min_periods=w).mean()

        # Thời điểm cần dự báo và nhãn
        target_time = g["datetime"].shift(-HORIZON)
        out["target_time"] = target_time
        out["y"] = g["pm25"].shift(-HORIZON)

        # Thời tiết tại thời điểm cần dự báo (t + HORIZON)
        for col in WEATHER_COLS:
            out[col] = g[col].shift(-HORIZON)

        # Đặc trưng Nhiệt động lực học & Biến thiên 24h (Thermodynamic Core)
        wind_rad = np.deg2rad(out["wind_direction_10m"])
        out["wind_dir_sin"] = np.sin(wind_rad)
        out["wind_dir_cos"] = np.cos(wind_rad)
        out["dew_point_deficit"] = out["temperature_2m"] - out["dew_point_2m"]
        out["ventilation_coef"] = out["wind_speed_10m"] * out["boundary_layer_height"]

        # Biến thiên thời tiết giữa (t + HORIZON) và hiện tại (t) + Tổng mưa tích lũy 24h
        out["temp_diff_24h"] = out["temperature_2m"] - g["temperature_2m"]
        out["pressure_diff_24h"] = out["surface_pressure"] - g["surface_pressure"]
        out["pblh_diff_24h"] = out["boundary_layer_height"] - g["boundary_layer_height"]
        out["precip_sum_24h"] = (
            g["precipitation"].rolling(HORIZON, min_periods=1).sum().shift(-HORIZON)
        )

        out["hour"] = target_time.dt.hour
        out["day_of_week"] = target_time.dt.dayofweek
        out["is_weekend"] = (out["day_of_week"] >= 5).astype(int)
        parts.append(out)

    feats = pd.concat(parts, ignore_index=True).dropna().reset_index(drop=True)
    return feats


def feature_columns(feats, stations):
    cols = (
        [f"pm25_lag{l}" for l in PM25_LAGS]
        + [f"pm25_roll{w}" for w in ROLL_WINDOWS]
        + WEATHER_COLS
        + THERMO_DERIVED_COLS
        + ["hour", "day_of_week", "is_weekend"]
    )
    # Mã trạm -> one-hot nếu tỉnh có nhiều hơn 1 trạm
    if len(stations) > 1:
        for s in stations:
            feats[f"station_{s}"] = (feats["station_code"] == s).astype(int)
            cols.append(f"station_{s}")
    return cols


# ============================================================
# 3. CHIA TRAIN / VAL / TEST THEO THỜI GIAN (70/10/20)
# ============================================================

def time_split(feats):
    times = np.sort(feats["target_time"].unique())
    n = len(times)
    train_end = times[int(n * TRAIN_FRAC) - 1]
    val_end = times[int(n * (TRAIN_FRAC + VAL_FRAC)) - 1]
    train = feats[feats["target_time"] <= train_end]
    val = feats[(feats["target_time"] > train_end) & (feats["target_time"] <= val_end)]
    test = feats[feats["target_time"] > val_end]
    return train, val, test, pd.Timestamp(train_end), pd.Timestamp(val_end)


def normalize_model_names(model_names=None):
    """Resolve model aliases and reject unknown names before training starts."""
    requested = DEFAULT_MODELS if model_names is None else model_names
    if isinstance(requested, str):
        requested = requested.split(",")
    normalized = []
    unknown = []
    for name in requested:
        key = name.strip().lower().replace("-", "_")
        canonical = MODEL_ALIASES.get(key)
        if canonical is None:
            unknown.append(name)
        elif canonical not in normalized:
            normalized.append(canonical)
    if unknown:
        supported = ", ".join(SUPPORTED_MODELS)
        raise ValueError(f"Model không hỗ trợ: {', '.join(unknown)}. Chọn: {supported}")
    if not normalized:
        raise ValueError("Phải chọn ít nhất một model.")
    return normalized


def create_model(model_name):
    """Create an estimator; scaling is included for scale-sensitive models."""
    if model_name == "linear_regression":
        return make_pipeline(StandardScaler(), LinearRegression())
    if model_name == "svr":
        return make_pipeline(StandardScaler(), SVR(C=10.0, epsilon=0.1, kernel="rbf"))
    if model_name == "random_forest":
        return RandomForestRegressor(
            n_estimators=300, min_samples_leaf=2, random_state=42, n_jobs=-1
        )
    if model_name == "xgboost":
        return xgb.XGBRegressor(
            n_estimators=500,
            learning_rate=0.05,
            max_depth=3,
            min_child_weight=20,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_lambda=1.0,
            early_stopping_rounds=EARLY_STOPPING_ROUNDS,
            objective="reg:squarederror",
            eval_metric="rmse",
            random_state=42,
            n_jobs=-1,
        )
    if model_name == "lightgbm":
        return lgb.LGBMRegressor(**LGB_PARAMS)
    raise ValueError(f"Model không hỗ trợ: {model_name}")


def fit_and_predict(model_name, X_train, y_train, X_val, y_val, X_test):
    model = create_model(model_name)
    if model_name == "lightgbm":
        model.fit(
            X_train,
            y_train,
            eval_set=[(X_val, y_val)],
            eval_metric="rmse",
            callbacks=[lgb.early_stopping(EARLY_STOPPING_ROUNDS, verbose=False)],
        )
        best_iteration = int(model.best_iteration_ or LGB_PARAMS["n_estimators"])
    elif model_name == "xgboost":
        model.fit(
            X_train,
            y_train,
            eval_set=[(X_val, y_val)],
            verbose=False,
        )
        best_iteration = int(getattr(model, "best_iteration", 499) + 1)
    else:
        model.fit(X_train, y_train)
        best_iteration = None
    pred_val = np.clip(model.predict(X_val), 0, None)
    pred_test = np.clip(model.predict(X_test), 0, None)
    return model, pred_val, pred_test, best_iteration


# ============================================================
# 4. QUY TRÌNH CHO MỘT TỈNH
# ============================================================

def run_province(province_name, model_names=None):
    model_names = normalize_model_names(model_names)
    slug = slugify(province_name)
    out_dir = OUTPUT_DIR / slug
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 64)
    print(f"TỈNH: {province_name}  (slug: {slug})")
    print("=" * 64)

    # --- Nạp + kiểm tra ngưỡng (ưu tiên Silver Parquet Partition Pruning) ---
    df = load_merged(province_name=province_name, use_parquet=True)
    counts = province_row_counts(df)
    if province_name not in counts.index:
        raise ValueError(f"Không có tỉnh '{province_name}' trong dữ liệu")
    n_rows = int(counts.loc[province_name, "rows"])
    if n_rows < MIN_ROWS:
        print(f"[BỎ QUA] {province_name} chỉ có {n_rows} dòng < ngưỡng {MIN_ROWS}")
        return None

    df_p = df[df["province_name"] == province_name]
    stations = sorted(df_p["station_code"].unique())
    print(f"Số dòng: {n_rows} | Số trạm: {len(stations)} -> {stations}")

    # --- Đặc trưng + chia 70/10/20 ---
    feats = build_features(df_p)
    cols = feature_columns(feats, stations)
    train, val, test, train_end, val_end = time_split(feats)
    n = len(feats)
    print(
        f"Mẫu sau tạo đặc trưng: {n} | train: {len(train)} ({len(train)/n:.0%}) | "
        f"val: {len(val)} ({len(val)/n:.0%}) | test: {len(test)} ({len(test)/n:.0%})"
    )
    print(f"Train đến: {train_end} | Val đến: {val_end} | Test: sau {val_end}")
    print(f"Số đặc trưng: {len(cols)}")

    X_train, y_train = train[cols], train["y"]
    X_val, y_val = val[cols], val["y"]
    X_test, y_test = test[cols], test["y"]

    # --- Baseline: giờ này giống 24 giờ trước = pm25 tại t ---
    base_pred = test["pm25_lag0"].to_numpy()
    base_m = metrics(y_test, base_pred)
    base_val_m = metrics(y_val, val["pm25_lag0"].to_numpy())

    print("\n--- KẾT QUẢ ---")
    print(f"{'':<18}{'RMSE':>9}{'MAE':>9}{'R2':>9}")
    print(f"{'Val  Baseline':<18}{base_val_m['rmse']:>9.2f}{base_val_m['mae']:>9.2f}{base_val_m['r2']:>9.3f}")
    print(f"{'Test Baseline':<18}{base_m['rmse']:>9.2f}{base_m['mae']:>9.2f}{base_m['r2']:>9.3f}")

    pred_df = test[["station_code", "origin_time", "target_time", "y"]].copy()
    pred_df = pred_df.rename(columns={"y": "y_true"})
    pred_df["y_baseline"] = base_pred
    results = {}
    for model_name in model_names:
        print(f"\n[{model_name}] Đang train...")
        model_dir = out_dir / model_name
        model_dir.mkdir(parents=True, exist_ok=True)
        model, pred_val, pred, best_iter = fit_and_predict(
            model_name, X_train, y_train, X_val, y_val, X_test
        )
        val_m = metrics(y_val, pred_val)
        model_m = metrics(y_test, pred)
        improvement = 100 * (base_m["rmse"] - model_m["rmse"]) / base_m["rmse"]
        print(f"Val  {model_name:<16}{val_m['rmse']:>9.2f}{val_m['mae']:>9.2f}{val_m['r2']:>9.3f}")
        print(f"Test {model_name:<16}{model_m['rmse']:>9.2f}{model_m['mae']:>9.2f}{model_m['r2']:>9.3f}")
        print(f"RMSE giảm {improvement:+.1f}% -> {'THẮNG' if improvement > 0 else 'KHÔNG thắng'} baseline")

        if model_name == "lightgbm":
            (model_dir / "model.txt").write_text(
                model.booster_.model_to_string(), encoding="utf-8"
            )
            importance = pd.DataFrame(
                {"feature": cols, "gain": model.booster_.feature_importance("gain")}
            ).sort_values("gain", ascending=False)
            importance.to_csv(model_dir / "feature_importance.csv", index=False)
        else:
            joblib.dump(model, model_dir / "model.joblib")
        pred_df[f"y_{model_name}"] = pred

        per_station = {}
        for station in stations:
            mask = (test["station_code"] == station).to_numpy()
            per_station[station] = {
                "n_test": int(mask.sum()),
                "baseline": metrics(y_test[mask], base_pred[mask]),
                model_name: metrics(y_test[mask], pred[mask]),
            }
        results[model_name] = {
            "best_iteration": best_iter,
            "val": val_m,
            "test": model_m,
            "rmse_improvement_pct": improvement,
            "beats_baseline": bool(improvement > 0),
            "per_station": per_station,
        }

    pred_df.to_csv(out_dir / "predictions.csv", index=False, encoding="utf-8-sig")

    summary = {
        "province": province_name,
        "slug": slug,
        "n_rows_province": n_rows,
        "stations": stations,
        "horizon_hours": HORIZON,
        "n_train": int(len(train)),
        "n_val": int(len(val)),
        "n_test": int(len(test)),
        "split_ratio": {"train": TRAIN_FRAC, "val": VAL_FRAC, "test": round(1 - TRAIN_FRAC - VAL_FRAC, 2)},
        "train_end": str(train_end),
        "val_end": str(val_end),
        "models": model_names,
        "features": cols,
        "baseline": {"val": base_val_m, "test": base_m},
        "results": results,
    }
    with open(out_dir / "metrics.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"\nĐã lưu kết quả -> {out_dir}")
    return summary
