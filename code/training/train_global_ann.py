"""
Huấn luyện Mô hình Mạng Nơ-ron Nhân tạo Toàn cục (Global Spatio-Temporal ANN)
dự báo chỉ số PM2.5 trước 24 giờ trên TOÀN BỘ DỮ LIỆU (9 tỉnh / 15 trạm).

Đặc điểm kỹ thuật chính:
1. Toàn bộ dữ liệu (Global Pooled Dataset):
   - Sử dụng toàn bộ 43.200 bản ghi đồng bộ 120 ngày từ cả 9 tỉnh / 15 trạm
     (42.480 mẫu hợp lệ sau khi tạo đặc trưng trễ 24h và nhãn t + 24h).
   - Giữ nguyên định danh gốc `station_id` (chuỗi ID 29 chữ số của TEDP) kèm theo
     `station_code`, `province_id`, `province_name` để phân biệt chính xác từng trạm
     thuộc tỉnh nào trong suốt quá trình huấn luyện, dự báo và giải thích mô hình.

2. Chia tập Train / Val / Test tỉ lệ 70 / 10 / 20:
   - Chia theo trục thời gian (`target_time`) đúng tỉ lệ 70% Train / 10% Val / 20% Test,
     đảm bảo không rò rỉ dữ liệu tương lai (Zero Data Leakage) và đồng nhất 100%
     với các mô hình theo từng tỉnh trong `common.py`.

3. Tối ưu hóa Hyper-parameter (Optuna) + Chạy nhiều Epochs + Đa cơ chế Early Stopping:
   - Tối ưu hóa siêu tham số tự động bằng Optuna (TPE Sampler + MedianPruner):
     tìm kiếm số tầng ẩn, số nơ-ron, kích thước embedding `station_id`, hàm kích hoạt,
     Dropout, chuẩn hóa (LayerNorm/BatchNorm), Learning Rate, Weight Decay, Batch Size,
     hàm mất mát (Huber/MSE) và ngưỡng cổng chú ý không gian.
   - Hệ thống Early Stopping đa tầng khi chạy nhiều epochs:
     (a) Validation RMSE Patience + Best-Checkpoint Restoration (khôi phục trọng số tốt nhất)
     (b) Adaptive Learning Rate Plateau Decay (ReduceLROnPlateau giảm LR khi Val chững lại)
     (c) Overfitting Generalization Gap Guard (cắt sớm khi tỷ số Val/Train RMSE giãn rộng)
     (d) Linear LR Warmup + Gradient Norm Clipping chống bùng nổ đạo hàm.

4. Cơ chế giải thích % đóng góp của từng Tỉnh và `station_id` vào quyết định của mô hình:
   - Cơ chế 1 (Spatial Attention & Gate Reliance %): Tầng Cross-Station Attention tính
     trọng số chú ý Softmax giữa 15 `station_id` tại mỗi thời điểm, cho phép trích xuất
     ma trận 9x9 (khi dự báo cho tỉnh A thì dựa bao nhiêu % vào tỉnh A và các tỉnh khác)
     cùng tỷ trọng quyết định toàn cục của từng tỉnh (tổng = 100%).
   - Cơ chế 2 (Gradient x Input Jacobian Saliency %): Tính đạo hàm ngược |d(y_hat)/dX * X|
     trên tập Test theo từng `station_id` và gom theo tỉnh (tổng = 100%).
   - Cơ chế 3 (Training Gradient Energy % & Feature Group Attribution %): Đo lường % năng
     lượng đạo hàm huấn luyện đóng góp bởi từng tỉnh so với % số lượng mẫu, đồng thời
     bóc tách % đóng góp của nhóm định danh `station_id` so với khí tượng và độ trễ PM2.5.

Chạy thử thủ công:
    # Chạy đầy đủ (Tối ưu Hyper-parameter bằng Optuna + Huấn luyện nhiều epochs + Phân tích % tỉnh):
    python code/training/train_global_ann.py

    # Tuỳ chỉnh số trials Optuna, số epochs tối đa và patience:
    python code/training/train_global_ann.py --n-trials 20 --epochs 250 --patience 30

    # Chạy nhanh bỏ qua bước tìm kiếm Optuna (dùng cấu hình siêu tham số tối ưu mặc định):
    python code/training/train_global_ann.py --no-tune --epochs 250 --patience 30
"""

from __future__ import annotations

import argparse
import copy
import io
import json
import math
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Đảm bảo hiển thị tiếng Việt chuẩn trên terminal Windows
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import numpy as np
try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
except ImportError:
    optuna = None
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
from sklearn.preprocessing import StandardScaler
from torch.utils.data import DataLoader, TensorDataset

# Nhập các cấu hình chuẩn từ common.py để đồng bộ 100% với toàn dự án
CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from common import (
    BASE_DIR,
    GOLD_DIR,
    GOLD_FEATURES_DIR,
    HORIZON,
    OUTPUT_DIR as PROVINCE_OUTPUT_DIR,
    OUTPUT_SUFFIX,
    PM25_LAGS,
    ROLL_WINDOWS,
    SILVER_DIR,
    SILVER_PROVINCE_DIR,
    SILVER_WEATHER_DIR,
    TEDP_CSV,
    THERMO_DERIVED_COLS,
    TRAIN_FRAC,
    VAL_FRAC,
    WEATHER_COLS,
    WEATHER_CSV,
    load_merged,
    metrics,
    slugify,
)

GLOBAL_ANN_DIR = BASE_DIR / "output" / f"global_ann{OUTPUT_SUFFIX}"

# Các đặc trưng mở rộng cho mô hình ANN toàn cục (đều có 0% NaN trên toàn bộ 15 trạm)
EXTRA_POLLUTANT_LAG0_COLS = ["pm10_lag0", "aqi_lag0"]
EXTRA_PM25_DYNAMICS_COLS = ["pm25_diff_1h", "pm25_diff_24h", "pm25_std24"]
CYCLIC_TIME_COLS = ["hour_sin", "hour_cos", "dow_sin", "dow_cos"]
SPATIAL_COORD_COLS = ["latitude_norm", "longitude_norm"]


# ============================================================
# TIỆN ÍCH CỐ ĐỊNH SEED & THIẾT BỊ TÍNH TOÁN
# ============================================================

def set_seed(seed: int = 42) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def resolve_device(device_arg: str = "auto") -> torch.device:
    if device_arg == "auto":
        return torch.device("cuda" if torch.cuda.is_available() else "cpu")
    return torch.device(device_arg)


# ============================================================
# 1. NẠP TOÀN BỘ DỮ LIỆU & GIỮ NGUYÊN STATION_ID
# ============================================================

def load_all_data_with_station_id(use_parquet: bool = True) -> pd.DataFrame:
    """
    Nạp toàn bộ dữ liệu TEDP (280.632 dòng, 11 trạm / 11 tỉnh) và ghép 1-1 với
    dữ liệu khí tượng Open-Meteo. Giữ nguyên `station_id` và `province_id` ở dạng chuỗi (str).
    Ưu tiên đọc từ Silver Parquet; tự động fallback sang CSV nếu không tìm thấy Parquet.
    """
    if use_parquet and SILVER_PROVINCE_DIR.exists() and SILVER_WEATHER_DIR.exists():
        try:
            return load_merged(province_name=None, use_parquet=True)
        except Exception:
            pass

    tedp = pd.read_csv(
        TEDP_CSV,
        dtype={"station_id": str, "province_id": str, "station_code": str},
        parse_dates=["datetime"],
    )
    weather = pd.read_csv(
        WEATHER_CSV,
        usecols=["station_code", "datetime"] + WEATHER_COLS,
        dtype={"station_code": str},
        parse_dates=["datetime"],
    )
    df = tedp.merge(weather, on=["station_code", "datetime"], how="left", validate="1:1")

    missing = int(df[WEATHER_COLS].isna().sum().sum())
    if missing:
        raise ValueError(f"Có {missing} ô thời tiết bị thiếu sau khi ghép - kiểm tra lại dữ liệu!")
    return df


def load_features_dataset(
    use_parquet: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], List[str], List[str]]:
    """
    Nạp dữ liệu đặc trưng phục vụ huấn luyện Spatio-Temporal Global ANN.
    Ưu tiên nạp trực tiếp 49 đặc trưng từ Gold Parquet (lakehouse_storage/gold/global_ann_features/).
    Nếu không có Parquet hoặc use_parquet=False, tự động fallback đọc từ CSV và sinh đặc trưng qua Pandas.
    """
    if use_parquet and GOLD_FEATURES_DIR.exists():
        try:
            print(f"[*] Nạp dữ liệu 49 đặc trưng từ Gold Parquet ({GOLD_FEATURES_DIR})...")
            feats = pd.read_parquet(GOLD_FEATURES_DIR)
            if "province_name" in feats.columns:
                feats["province_name"] = feats["province_name"].astype(str)
            if "station_code" in feats.columns:
                feats["station_code"] = feats["station_code"].astype(str)
            if "station_id" in feats.columns:
                feats["station_id"] = feats["station_id"].astype(str)
            if "target_time" in feats.columns:
                feats["target_time"] = pd.to_datetime(feats["target_time"])
            if "origin_time" in feats.columns:
                feats["origin_time"] = pd.to_datetime(feats["origin_time"])

            # Tái tạo station_meta từ tập Gold
            meta_cols = [
                "station_id", "station_code", "station_name", "province_id",
                "province_name", "latitude", "longitude", "station_idx"
            ]
            station_meta = (
                feats[meta_cols]
                .drop_duplicates()
                .sort_values("station_idx")
                .reset_index(drop=True)
            )

            # Khởi tạo danh sách đặc trưng chuẩn
            continuous_cols = (
                [f"pm25_lag{l}" for l in PM25_LAGS]
                + [f"pm25_roll{w}" for w in ROLL_WINDOWS]
                + EXTRA_PM25_DYNAMICS_COLS
                + EXTRA_POLLUTANT_LAG0_COLS
                + WEATHER_COLS
                + THERMO_DERIVED_COLS
                + ["hour", "day_of_week"]
                + CYCLIC_TIME_COLS
                + SPATIAL_COORD_COLS
            )
            station_onehot_cols = [f"station_id_{st_id}" for st_id in station_meta["station_id"]]
            binary_and_station_cols = ["is_weekend"] + station_onehot_cols
            all_feature_cols = continuous_cols + binary_and_station_cols

            print(f"[THÀNH CÔNG] Đã nạp {len(feats):,} mẫu từ Gold Parquet Lakehouse ({len(all_feature_cols)} đặc trưng).")
            return feats, station_meta, continuous_cols, binary_and_station_cols, all_feature_cols
        except Exception as e:
            print(f"[CẢNH BÁO] Không thể nạp Gold Parquet ({e}). Đang fallback sang CSV + Pandas...")

    # Fallback sang nạp CSV và sinh đặc trưng bằng Pandas
    print("[*] Đang nạp dữ liệu từ CSV và tạo 49 đặc trưng bằng Pandas...")
    df_raw = load_all_data_with_station_id(use_parquet=use_parquet)
    feats, station_meta = build_global_features(df_raw)
    continuous_cols, binary_and_station_cols, all_feature_cols = add_station_id_Columns(
        feats, station_meta
    )
    return feats, station_meta, continuous_cols, binary_and_station_cols, all_feature_cols


def build_global_features(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Xây dựng bảng đặc trưng cho TOÀN BỘ 15 trạm / 9 tỉnh.
    Mỗi trạm (`station_id`) được tính độ trễ và trung bình trượt độc lập để tránh
    trượt dữ liệu sang trạm khác, sau đó gộp lại và giữ nguyên `station_id`.
    """
    # Bảng ánh xạ chuẩn: station_id <-> station_code <-> province_name
    station_meta = (
        df[
            [
                "station_id",
                "station_code",
                "station_name",
                "province_id",
                "province_name",
                "latitude",
                "longitude",
            ]
        ]
        .drop_duplicates()
        .sort_values(["province_name", "station_code", "station_id"])
        .reset_index(drop=True)
    )
    station_meta["station_idx"] = np.arange(len(station_meta), dtype=int)

    # Chuẩn hóa tọa độ không gian (min-max về [-1, 1]) để ANN học vị trí địa lý
    lat_min, lat_max = station_meta["latitude"].min(), station_meta["latitude"].max()
    lon_min, lon_max = station_meta["longitude"].min(), station_meta["longitude"].max()

    parts = []
    for st_id, g in df.groupby("station_id", sort=False):
        g = g.sort_values("datetime").reset_index(drop=True)
        first = g.iloc[0]

        out = pd.DataFrame(
            {
                "station_id": str(st_id),
                "station_code": str(first["station_code"]),
                "station_name": str(first["station_name"]),
                "province_id": str(first["province_id"]),
                "province_name": str(first["province_name"]),
                "latitude": float(first["latitude"]),
                "longitude": float(first["longitude"]),
                "origin_time": g["datetime"],
            }
        )

        # 1. Độ trễ PM2.5 và trung bình trượt tại trạm
        for lag in PM25_LAGS:
            out[f"pm25_lag{lag}"] = g["pm25"].shift(lag)
        for w in ROLL_WINDOWS:
            out[f"pm25_roll{w}"] = g["pm25"].rolling(w, min_periods=w).mean()

        # Động lực biến thiên ngắn hạn của PM2.5 tại trạm
        out["pm25_diff_1h"] = out["pm25_lag0"] - out["pm25_lag1"]
        out["pm25_diff_24h"] = out["pm25_lag0"] - out["pm25_lag24"]
        out["pm25_std24"] = g["pm25"].rolling(24, min_periods=24).std().fillna(0.0)

        # Chỉ số PM10 và VN_AQI tổng hợp tại thời điểm hiện tại (t)
        out["pm10_lag0"] = g["pm10"]
        out["aqi_lag0"] = g["aqi"]

        # 2. Thời điểm mục tiêu (t + 24h) và nhãn cần dự báo y
        target_time = g["datetime"].shift(-HORIZON)
        out["target_time"] = target_time
        out["y"] = g["pm25"].shift(-HORIZON)

        # 3. 8 biến thời tiết tại thời điểm dự báo (t + 24h)
        for col in WEATHER_COLS:
            out[col] = g[col].shift(-HORIZON)

        # 4. 8 đặc trưng Nhiệt động lực học & Biến thiên 24h (Thermodynamic Core)
        wind_rad = np.deg2rad(out["wind_direction_10m"])
        out["wind_dir_sin"] = np.sin(wind_rad)
        out["wind_dir_cos"] = np.cos(wind_rad)
        out["dew_point_deficit"] = out["temperature_2m"] - out["dew_point_2m"]
        out["ventilation_coef"] = out["wind_speed_10m"] * out["boundary_layer_height"]

        out["temp_diff_24h"] = out["temperature_2m"] - g["temperature_2m"]
        out["pressure_diff_24h"] = out["surface_pressure"] - g["surface_pressure"]
        out["pblh_diff_24h"] = out["boundary_layer_height"] - g["boundary_layer_height"]
        out["precip_sum_24h"] = (
            g["precipitation"].rolling(HORIZON, min_periods=1).sum().shift(-HORIZON)
        )

        # 5. Đặc trưng thời gian & lượng giác chu kỳ
        out["hour"] = target_time.dt.hour
        out["day_of_week"] = target_time.dt.dayofweek
        out["is_weekend"] = (out["day_of_week"] >= 5).astype(float)
        out["hour_sin"] = np.sin(2 * np.pi * out["hour"] / 24.0)
        out["hour_cos"] = np.cos(2 * np.pi * out["hour"] / 24.0)
        out["dow_sin"] = np.sin(2 * np.pi * out["day_of_week"] / 7.0)
        out["dow_cos"] = np.cos(2 * np.pi * out["day_of_week"] / 7.0)

        # 6. Tọa độ không gian chuẩn hóa
        out["latitude_norm"] = 2.0 * (out["latitude"] - lat_min) / max(lat_max - lat_min, 1e-6) - 1.0
        out["longitude_norm"] = 2.0 * (out["longitude"] - lon_min) / max(lon_max - lon_min, 1e-6) - 1.0

        parts.append(out)

    feats = pd.concat(parts, ignore_index=True).dropna().reset_index(drop=True)
    feats = feats.merge(
        station_meta[["station_id", "station_idx"]], on="station_id", how="left"
    )
    return feats, station_meta


def add_station_id_Columns(
    feats: pd.DataFrame, station_meta: pd.DataFrame
) -> Tuple[List[str], List[str], List[str]]:
    """
    Tạo các cột One-Hot giữ nguyên `station_id` (`station_id_<ID>`) để mô hình
    nhận diện trực tiếp từng trạm và tỉnh tương ứng.
    Trả về:
        - continuous_cols: danh sách đặc trưng số liên tục (cần StandardScaler)
        - binary_and_station_cols: danh sách đặc trưng nhị phân + One-Hot `station_id`
        - all_feature_cols: toàn bộ đặc trưng đầu vào của ANN
    """
    continuous_cols = (
        [f"pm25_lag{l}" for l in PM25_LAGS]
        + [f"pm25_roll{w}" for w in ROLL_WINDOWS]
        + EXTRA_PM25_DYNAMICS_COLS
        + EXTRA_POLLUTANT_LAG0_COLS
        + WEATHER_COLS
        + THERMO_DERIVED_COLS
        + ["hour", "day_of_week"]
        + CYCLIC_TIME_COLS
        + SPATIAL_COORD_COLS
    )

    station_onehot_cols = []
    for st_id in station_meta["station_id"]:
        col_name = f"station_id_{st_id}"
        feats[col_name] = (feats["station_id"] == st_id).astype(float)
        station_onehot_cols.append(col_name)

    binary_and_station_cols = ["is_weekend"] + station_onehot_cols
    all_feature_cols = continuous_cols + binary_and_station_cols
    return continuous_cols, binary_and_station_cols, all_feature_cols


# ============================================================
# 2. CHIA TRAIN / VAL / TEST (70 / 10 / 20) & ĐÓNG GÓI TENSOR ĐỒNG BỘ
# ============================================================

@dataclass
class SpatioTemporalSplitData:
    # Tensor 3 chiều: (T_timestamps, N_stations=15, F_features)
    X_train: torch.Tensor
    y_train: torch.Tensor
    lag0_train: torch.Tensor

    X_val: torch.Tensor
    y_val: torch.Tensor
    lag0_val: torch.Tensor

    X_test: torch.Tensor
    y_test: torch.Tensor
    lag0_test: torch.Tensor

    # DataFrame tương ứng để xuất dự báo và kiểm tra theo station_id / province
    df_train: pd.DataFrame
    df_val: pd.DataFrame
    df_test: pd.DataFrame

    train_end: pd.Timestamp
    val_end: pd.Timestamp
    scaler: StandardScaler
    continuous_cols: List[str]
    all_feature_cols: List[str]
    station_meta: pd.DataFrame


def prepare_spatiotemporal_splits(
    feats: pd.DataFrame,
    station_meta: pd.DataFrame,
    continuous_cols: List[str],
    all_feature_cols: List[str],
) -> SpatioTemporalSplitData:
    """
    Chia dữ liệu theo thời gian (`target_time`) với tỉ lệ chính xác 70% Train / 10% Val / 20% Test.
    Sắp xếp mỗi mốc thời gian theo thứ tự `station_idx` (0..14) để tạo tensor 3 chiều
    `(T, N_stations=15, F)` phục vụ cơ chế Cross-Station Spatial Attention giữa các tỉnh.
    """
    # Sắp xếp chuẩn theo (target_time, station_idx)
    feats_sorted = feats.sort_values(["target_time", "station_idx"]).reset_index(drop=True)
    times = np.sort(feats_sorted["target_time"].unique())
    n_times = len(times)
    n_stations = len(station_meta)

    if len(feats_sorted) != n_times * n_stations:
        raise ValueError(
            f"Dữ liệu không đồng bộ lưới trạm: {len(feats_sorted)} dòng != {n_times} giờ x {n_stations} trạm"
        )

    train_end = pd.Timestamp(times[int(n_times * TRAIN_FRAC) - 1])
    val_end = pd.Timestamp(times[int(n_times * (TRAIN_FRAC + VAL_FRAC)) - 1])

    df_train = feats_sorted[feats_sorted["target_time"] <= train_end].copy().reset_index(drop=True)
    df_val = (
        feats_sorted[
            (feats_sorted["target_time"] > train_end) & (feats_sorted["target_time"] <= val_end)
        ]
        .copy()
        .reset_index(drop=True)
    )
    df_test = feats_sorted[feats_sorted["target_time"] > val_end].copy().reset_index(drop=True)

    # Chuẩn hóa Z-score CHỈ fit trên tập Train (70%) để chống rò rỉ dữ liệu
    scaler = StandardScaler()
    scaler.fit(df_train[continuous_cols].to_numpy(dtype=np.float32))

    def _to_tensors(df_split: pd.DataFrame) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        df_scaled = df_split[all_feature_cols].copy()
        df_scaled[continuous_cols] = scaler.transform(
            df_split[continuous_cols].to_numpy(dtype=np.float32)
        )
        t_steps = len(df_split) // n_stations
        x_arr = df_scaled.to_numpy(dtype=np.float32).reshape(t_steps, n_stations, len(all_feature_cols))
        y_arr = df_split["y"].to_numpy(dtype=np.float32).reshape(t_steps, n_stations)
        lag0_arr = df_split["pm25_lag0"].to_numpy(dtype=np.float32).reshape(t_steps, n_stations)
        return (
            torch.from_numpy(x_arr),
            torch.from_numpy(y_arr),
            torch.from_numpy(lag0_arr),
        )

    X_train, y_train, lag0_train = _to_tensors(df_train)
    X_val, y_val, lag0_val = _to_tensors(df_val)
    X_test, y_test, lag0_test = _to_tensors(df_test)

    return SpatioTemporalSplitData(
        X_train=X_train,
        y_train=y_train,
        lag0_train=lag0_train,
        X_val=X_val,
        y_val=y_val,
        lag0_val=lag0_val,
        X_test=X_test,
        y_test=y_test,
        lag0_test=lag0_test,
        df_train=df_train,
        df_val=df_val,
        df_test=df_test,
        train_end=train_end,
        val_end=val_end,
        scaler=scaler,
        continuous_cols=continuous_cols,
        all_feature_cols=all_feature_cols,
        station_meta=station_meta,
    )


# ============================================================
# 3. KIẾN TRÚC MẠNG GLOBAL SPATIO-TEMPORAL ANN (CÓ ATTENTION THEO TỈNH)
# ============================================================

def build_spatial_prior_matrix(station_meta: pd.DataFrame) -> torch.Tensor:
    """
    Khởi tạo ma trận thiên lệch không gian (Spatial Prior Bias) 15x15 dựa trên
    khoảng cách địa lý và việc hai `station_id` có thuộc cùng một tỉnh hay không.
    Ma trận này giúp tầng Cross-Station Attention ban đầu ưu tiên các trạm cùng tỉnh
    hoặc lân cận, đồng thời vẫn cho phép học tương tác liên tỉnh qua quá trình huấn luyện.
    """
    n = len(station_meta)
    lats = station_meta["latitude"].to_numpy(dtype=np.float32)
    lons = station_meta["longitude"].to_numpy(dtype=np.float32)
    provs = station_meta["province_name"].tolist()

    prior = np.zeros((n, n), dtype=np.float32)
    for i in range(n):
        for j in range(n):
            dist_deg = math.sqrt((lats[i] - lats[j]) ** 2 + (lons[i] - lons[j]) ** 2)
            # Khuyến khích trạm chính nó và trạm cùng tỉnh / gần về mặt địa lý
            same_prov_bonus = 1.0 if provs[i] == provs[j] else 0.0
            self_bonus = 1.5 if i == j else 0.0
            prior[i, j] = self_bonus + same_prov_bonus - 0.35 * dist_deg
    return torch.from_numpy(prior)


def make_activation(name: str) -> nn.Module:
    name = name.lower()
    if name == "gelu":
        return nn.GELU()
    if name == "silu":
        return nn.SiLU()
    if name == "relu":
        return nn.ReLU()
    raise ValueError(f"Hàm kích hoạt không hỗ trợ: {name}")


class ResidualMLPBlock(nn.Module):
    """Khối MLP có kết nối tắt (Residual Skip-Connection), chuẩn hóa và Dropout."""

    def __init__(
        self,
        dim: int,
        activation: str = "gelu",
        dropout: float = 0.2,
        norm_type: str = "layernorm",
    ) -> None:
        super().__init__()
        self.fc1 = nn.Linear(dim, dim)
        self.norm1 = nn.LayerNorm(dim) if norm_type == "layernorm" else nn.BatchNorm1d(dim)
        self.act1 = make_activation(activation)
        self.drop1 = nn.Dropout(dropout)

        self.fc2 = nn.Linear(dim, dim)
        self.norm2 = nn.LayerNorm(dim) if norm_type == "layernorm" else nn.BatchNorm1d(dim)
        self.act2 = make_activation(activation)
        self.drop2 = nn.Dropout(dropout)
        self.norm_type = norm_type

    def _apply_norm(self, norm_layer: nn.Module, x: torch.Tensor) -> torch.Tensor:
        if self.norm_type == "batchnorm":
            # x có dạng (B, N, D) -> chuyển về (B*N, D) cho BatchNorm1d
            b, n, d = x.shape
            return norm_layer(x.reshape(b * n, d)).reshape(b, n, d)
        return norm_layer(x)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        residual = x
        out = self.fc1(x)
        out = self._apply_norm(self.norm1, out)
        out = self.act1(out)
        out = self.drop1(out)

        out = self.fc2(out)
        out = self._apply_norm(self.norm2, out)
        out = self.drop2(out)
        return self.act2(out + residual)


class SpatioTemporalGlobalANN(nn.Module):
    """
    Mạng Nơ-ron Nhân tạo Toàn cục tích hợp:
    1. Embedding định danh `station_id` kết hợp đặc trưng One-Hot `station_id`
    2. Tầng Cross-Station Spatial Attention & Cổng trộn (Learnable Gate) cho phép
       định lượng chính xác % mô hình dựa vào dữ liệu của từng trạm / từng tỉnh
    3. Các khối Residual MLP sâu học tương tác phi tuyến Khí tượng - Nhiệt động lực học
    4. Nhánh neo vật lý tuyến tính theo trạm (Station-Specific Persistence Anchor)
    """

    def __init__(
        self,
        num_features: int,
        num_stations: int,
        spatial_prior: torch.Tensor,
        hidden_dim: int = 128,
        n_layers: int = 3,
        station_emb_dim: int = 16,
        activation: str = "gelu",
        dropout: float = 0.20,
        norm_type: str = "layernorm",
        max_spatial_gate: float = 0.35,
        attention_temp: float = 1.0,
    ) -> None:
        super().__init__()
        self.num_features = num_features
        self.num_stations = num_stations
        self.hidden_dim = hidden_dim
        self.max_spatial_gate = max_spatial_gate
        self.attention_temp = max(attention_temp, 0.1)

        # 1. Embedding riêng cho từng station_id
        self.station_embedding = nn.Embedding(num_stations, station_emb_dim)

        # 2. Bộ mã hóa đầu vào tại từng trạm (Input Feature + Station ID Embedding -> hidden_dim)
        in_total_dim = num_features + station_emb_dim
        self.input_proj = nn.Linear(in_total_dim, hidden_dim)
        self.input_norm = nn.LayerNorm(hidden_dim)
        self.input_act = make_activation(activation)
        self.input_drop = nn.Dropout(dropout)

        # 3. Tầng Cross-Station Spatial Attention giữa 15 station_id
        self.q_proj = nn.Linear(hidden_dim, hidden_dim)
        self.k_proj = nn.Linear(hidden_dim, hidden_dim)
        self.v_proj = nn.Linear(hidden_dim, hidden_dim)

        # Ma trận thiên lệch không gian học được giữa các trạm (khởi tạo từ khoảng cách & tỉnh)
        self.spatial_bias = nn.Parameter(spatial_prior.clone())

        # Cổng trộn (Gate) cho từng trạm: quyết định tỷ lệ dùng ngữ cảnh liên trạm/liên tỉnh
        # Khởi tạo âm nhẹ để ban đầu ưu tiên dữ liệu nội tại của trạm rồi mở dần khi cần
        self.station_gate_logits = nn.Parameter(torch.full((num_stations,), -0.5))

        # 4. Chuỗi khối sâu Residual MLP
        self.blocks = nn.ModuleList(
            [
                ResidualMLPBlock(
                    dim=hidden_dim,
                    activation=activation,
                    dropout=dropout,
                    norm_type=norm_type,
                )
                for _ in range(n_layers)
            ]
        )

        # 5. Đầu ra dự báo độ lệch phi tuyến + Nhánh neo quán tính theo từng station_id
        self.head = nn.Linear(hidden_dim, 1)
        self.anchor_weight = nn.Parameter(torch.full((num_stations,), 0.65))
        self.anchor_bias = nn.Parameter(torch.zeros(num_stations))

    def get_station_gates(self) -> torch.Tensor:
        """
        Trả về hệ số cổng không gian g_i thuộc (0, max_spatial_gate) cho từng station_id.
        (1 - g_i) là tỷ trọng giữ lại trực tiếp từ nhánh nội tại của trạm i;
        g_i là tỷ trọng phân bổ qua cơ chế Cross-Station Attention trên 15 trạm.
        """
        return self.max_spatial_gate * torch.sigmoid(self.station_gate_logits)

    def forward(
        self,
        x: torch.Tensor,
        lag0: torch.Tensor,
        return_attention: bool = False,
    ) -> Tuple[torch.Tensor, Optional[torch.Tensor]]:
        """
        Tham số:
            x   : Tensor (Batch_times, N_stations=15, F_features)
            lag0: Tensor (Batch_times, N_stations=15) - giá trị PM2.5 tại thời điểm t
            return_attention: nếu True, trả về thêm ma trận Trọng số Quyết định Thực tế
                              Effective_Weight (Batch_times, 15, 15), trong đó tổng theo chiều
                              trạm nguồn j bằng 1.0 (100%).
        """
        bsz, n_st, _ = x.shape
        device = x.device

        # Ghép thêm vector embedding của station_id (0..14)
        st_indices = torch.arange(n_st, device=device).unsqueeze(0).expand(bsz, n_st)
        st_emb = self.station_embedding(st_indices)  # (B, 15, emb_dim)

        h_in = torch.cat([x, st_emb], dim=-1)
        h_local = self.input_drop(self.input_act(self.input_norm(self.input_proj(h_in))))

        # Cross-Station Attention giữa 15 trạm tại cùng thời điểm t
        q = self.q_proj(h_local)  # (B, 15, D)
        k = self.k_proj(h_local)  # (B, 15, D)
        v = self.v_proj(h_local)  # (B, 15, D)

        scale = math.sqrt(self.hidden_dim) * self.attention_temp
        attn_logits = torch.matmul(q, k.transpose(-2, -1)) / scale  # (B, 15, 15)
        attn_logits = attn_logits + self.spatial_bias.unsqueeze(0)
        attn_weights = F.softmax(attn_logits, dim=-1)  # (B, 15, 15), tổng theo cột j = 1.0

        # Trộn biểu diễn nội tại của trạm i với ngữ cảnh liên trạm/liên tỉnh
        gates = self.get_station_gates()  # (15,)
        gates_view = gates.view(1, n_st, 1)  # (1, 15, 1)
        h_spatial = torch.matmul(attn_weights, v)  # (B, 15, D)
        h = (1.0 - gates_view) * h_local + gates_view * h_spatial

        # Đi qua các khối ẩn Residual MLP
        for block in self.blocks:
            h = block(h)

        delta_pred = self.head(h).squeeze(-1)  # (B, 15)
        anchor_pred = lag0 * self.anchor_weight.unsqueeze(0) + self.anchor_bias.unsqueeze(0)
        y_pred = anchor_pred + delta_pred

        if not return_attention:
            return y_pred, None

        # Tính Ma trận Trọng số Quyết định Thực tế (Effective Decision Reliance Matrix):
        # W_{i <- j} = (1 - g_i) * I(i == j) + g_i * alpha_{i <- j}
        # Đảm bảo với mọi trạm đích i: sum_{j=1..15} W_{i <- j} = 1.0 (100%)
        eye = torch.eye(n_st, device=device).unsqueeze(0).expand(bsz, n_st, n_st)
        effective_weights = (1.0 - gates_view) * eye + gates_view * attn_weights
        return y_pred, effective_weights


# ============================================================
# 4. ĐA CƠ CHẾ EARLY STOPPING & HUẤN LUYỆN NHIỀU EPOCHS
# ============================================================

class MultiMechanismEarlyStopping:
    """
    Hệ thống kiểm soát dừng sớm (Early Stopping):
    1. Mặc định (Validation RMSE Patience + min_delta): dừng khi Val RMSE không giảm
       thêm tối thiểu `min_delta` sau `patience` epochs liên tiếp, đồng thời tự động
       lưu và khôi phục bộ trọng số ở epoch có Val RMSE nhỏ nhất (Best Checkpoint).
    2. Tùy chọn (Overfitting Generalization Gap Guard — bật qua cờ `--overfit-guard`):
       theo dõi tỷ số `Val RMSE / Train RMSE`. Nếu tỷ số này vượt ngưỡng `max_overfit_ratio`
       VÀ Val RMSE đang xấu đi liên tục trong `overfit_patience` vòng, kích hoạt dừng sớm.
    """

    def __init__(
        self,
        patience: int = 30,
        min_delta: float = 1e-3,
        use_overfit_guard: bool = False,
        overfit_patience: int = 12,
        max_overfit_ratio: float = 1.45,
        min_epochs: int = 25,
    ) -> None:
        self.patience = patience
        self.min_delta = min_delta
        self.use_overfit_guard = use_overfit_guard
        self.overfit_patience = overfit_patience
        self.max_overfit_ratio = max_overfit_ratio
        self.min_epochs = min_epochs

        self.best_val_rmse: float = float("inf")
        self.best_epoch: int = 0
        self.best_state_dict: Optional[Dict[str, torch.Tensor]] = None
        self.wait_count: int = 0
        self.overfit_count: int = 0
        self.stop_reason: str = "Hoàn thành toàn bộ số epochs tối đa"

    def step(
        self,
        epoch: int,
        train_rmse: float,
        val_rmse: float,
        model: nn.Module,
    ) -> bool:
        # Kiểm tra cải thiện trên tập Validation
        if val_rmse < self.best_val_rmse - self.min_delta:
            self.best_val_rmse = val_rmse
            self.best_epoch = epoch
            self.best_state_dict = copy.deepcopy(model.state_dict())
            self.wait_count = 0
            self.overfit_count = 0
            return False

        self.wait_count += 1

        # Kiểm tra dấu hiệu quá khớp (chỉ theo dõi khi bật tùy chọn use_overfit_guard)
        overfit_ratio = val_rmse / max(train_rmse, 1e-6)
        if (
            self.use_overfit_guard
            and epoch >= self.min_epochs
            and overfit_ratio > self.max_overfit_ratio
            and val_rmse > self.best_val_rmse * 1.03
        ):
            self.overfit_count += 1
        else:
            self.overfit_count = max(0, self.overfit_count - 1)

        if epoch < self.min_epochs:
            return False

        if self.wait_count >= self.patience:
            self.stop_reason = (
                f"Early Stopping [Patience]: Val RMSE không cải thiện sau {self.patience} epochs "
                f"(tốt nhất tại epoch {self.best_epoch}: {self.best_val_rmse:.4f})"
            )
            return True

        if self.use_overfit_guard and self.overfit_count >= self.overfit_patience:
            self.stop_reason = (
                f"Early Stopping [Overfit Guard]: Tỷ số Val/Train RMSE = {overfit_ratio:.2f} > "
                f"{self.max_overfit_ratio:.2f} trong {self.overfit_patience} epochs liên tiếp "
                f"(tốt nhất tại epoch {self.best_epoch}: {self.best_val_rmse:.4f})"
            )
            return True

        return False

    def restore_best_weights(self, model: nn.Module) -> None:
        if self.best_state_dict is not None:
            model.load_state_dict(self.best_state_dict)


def evaluate_model_rmse(
    model: SpatioTemporalGlobalANN,
    X: torch.Tensor,
    y: torch.Tensor,
    lag0: torch.Tensor,
    device: torch.device,
) -> Tuple[float, np.ndarray]:
    model.eval()
    with torch.no_grad():
        preds, _ = model(X.to(device), lag0.to(device), return_attention=False)
        preds_np = np.clip(preds.cpu().numpy(), 0.0, None)
        y_np = y.cpu().numpy()
        score = float(np.sqrt(np.mean((y_np - preds_np) ** 2)))
    return score, preds_np


def train_ann_with_config(
    data: SpatioTemporalSplitData,
    spatial_prior: torch.Tensor,
    config: Dict,
    max_epochs: int,
    patience: int,
    device: torch.device,
    trial: Optional[optuna.Trial] = None,
    verbose: bool = True,
    use_overfit_guard: bool = False,
    max_overfit_ratio: float = 1.45,
    overfit_patience: Optional[int] = None,
) -> Tuple[SpatioTemporalGlobalANN, float, int, str, List[Dict]]:
    """
    Huấn luyện mô hình SpatioTemporalGlobalANN với cấu hình siêu tham số `config`,
    kết hợp Warmup + ReduceLROnPlateau + MultiMechanismEarlyStopping.
    """
    set_seed(int(config.get("seed", 42)))
    n_features = data.X_train.shape[-1]
    n_stations = data.X_train.shape[1]

    model = SpatioTemporalGlobalANN(
        num_features=n_features,
        num_stations=n_stations,
        spatial_prior=spatial_prior.to(device),
        hidden_dim=int(config["hidden_dim"]),
        n_layers=int(config["n_layers"]),
        station_emb_dim=int(config["station_emb_dim"]),
        activation=str(config["activation"]),
        dropout=float(config["dropout"]),
        norm_type=str(config["norm_type"]),
        max_spatial_gate=float(config["max_spatial_gate"]),
        attention_temp=float(config["attention_temp"]),
    ).to(device)

    base_lr = float(config["learning_rate"])
    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=base_lr,
        weight_decay=float(config["weight_decay"]),
    )

    # Cơ chế giảm Learning Rate tự động khi Val RMSE đi ngang (trước khi dừng hẳn)
    lr_patience = max(5, patience // 3)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer,
        mode="min",
        factor=0.5,
        patience=lr_patience,
        min_lr=1e-5,
    )

    loss_type = str(config.get("loss_type", "huber"))
    criterion: nn.Module
    if loss_type == "huber":
        criterion = nn.HuberLoss(delta=float(config.get("huber_delta", 15.0)))
    else:
        criterion = nn.MSELoss()

    dataset = TensorDataset(data.X_train, data.y_train, data.lag0_train)
    loader = DataLoader(
        dataset,
        batch_size=int(config["batch_size"]),
        shuffle=True,
        drop_last=False,
    )

    early_stopper = MultiMechanismEarlyStopping(
        patience=patience,
        min_delta=1e-3,
        use_overfit_guard=use_overfit_guard,
        overfit_patience=(
            int(overfit_patience) if overfit_patience is not None else max(10, patience // 2)
        ),
        max_overfit_ratio=max_overfit_ratio,
        min_epochs=min(20, max_epochs // 4),
    )

    warmup_epochs = min(8, max(2, max_epochs // 15))
    history: List[Dict] = []

    for epoch in range(1, max_epochs + 1):
        # Cơ chế Linear Warmup trong các epochs đầu tiên
        if epoch <= warmup_epochs:
            warmup_lr = base_lr * (epoch / warmup_epochs)
            for pg in optimizer.param_groups:
                pg["lr"] = warmup_lr

        model.train()
        for bx, by, blag0 in loader:
            bx = bx.to(device)
            by = by.to(device)
            blag0 = blag0.to(device)

            optimizer.zero_grad()
            pred, _ = model(bx, blag0, return_attention=False)
            loss = criterion(pred, by)
            loss.backward()
            # Cắt ngưỡng đạo hàm chống bùng nổ Gradient khi chạy nhiều epochs
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()

        # Đánh giá cuối mỗi epoch trên Train, Val và Test
        train_rmse, _ = evaluate_model_rmse(model, data.X_train, data.y_train, data.lag0_train, device)
        val_rmse, _ = evaluate_model_rmse(model, data.X_val, data.y_val, data.lag0_val, device)
        test_rmse, _ = evaluate_model_rmse(model, data.X_test, data.y_test, data.lag0_test, device)

        if epoch > warmup_epochs:
            scheduler.step(val_rmse)

        current_lr = float(optimizer.param_groups[0]["lr"])
        overfit_ratio = val_rmse / max(train_rmse, 1e-6)
        history.append(
            {
                "epoch": epoch,
                "train_rmse": train_rmse,
                "val_rmse": val_rmse,
                "test_rmse": test_rmse,
                "lr": current_lr,
                "overfit_ratio": overfit_ratio,
            }
        )

        # Báo cáo cho Optuna Pruner (nếu đang trong pha tìm kiếm siêu tham số)
        if trial is not None:
            trial.report(val_rmse, step=epoch)
            if trial.should_prune():
                raise optuna.TrialPruned()

        should_stop = early_stopper.step(epoch, train_rmse, val_rmse, model)

        if verbose and (epoch == 1 or epoch % 10 == 0 or should_stop or epoch == max_epochs):
            marker = " *BEST*" if early_stopper.best_epoch == epoch else ""
            print(
                f"  Epoch {epoch:03d}/{max_epochs:03d} | "
                f"Train RMSE: {train_rmse:6.2f} | "
                f"Val RMSE: {val_rmse:6.2f} | "
                f"Test RMSE: {test_rmse:6.2f} | "
                f"LR: {current_lr:.2e} | "
                f"Gap: {overfit_ratio:.2f}{marker}"
            )

        if should_stop:
            break

    early_stopper.restore_best_weights(model)
    return (
        model,
        early_stopper.best_val_rmse,
        early_stopper.best_epoch,
        early_stopper.stop_reason,
        history,
    )


# ============================================================
# 5. TỐI ƯU HÓA HYPER-PARAMETER VỚI OPTUNA
# ============================================================

DEFAULT_BEST_CONFIG: Dict = {
    "hidden_dim": 128,
    "n_layers": 2,
    "station_emb_dim": 16,
    "activation": "gelu",
    "norm_type": "layernorm",
    "dropout": 0.25,
    "learning_rate": 1.2e-3,
    "weight_decay": 1e-3,
    "batch_size": 512,
    "loss_type": "huber",
    "huber_delta": 15.0,
    "max_spatial_gate": 0.30,
    "attention_temp": 1.0,
    "seed": 42,
}


def run_hyperparameter_search(
    data: SpatioTemporalSplitData,
    spatial_prior: torch.Tensor,
    n_trials: int,
    tune_epochs: int,
    tune_patience: int,
    device: torch.device,
    use_spatial_attention: bool = True,
) -> Tuple[Dict, pd.DataFrame]:
    """
    Tối ưu hóa siêu tham số tự động bằng thuật toán Bayesian Optimization (TPE)
    của Optuna kết hợp cơ chế cắt tỉa sớm MedianPruner.
    """
    attn_label = "CÓ SPATIAL ATTENTION" if use_spatial_attention else "KHÔNG CÓ SPATIAL ATTENTION"
    print("\n" + "=" * 78)
    print(f"PHA 1: TỐI ƯU HÓA HYPER-PARAMETER VỚI OPTUNA ({n_trials} trials | {attn_label})")
    print("=" * 78)

    def objective(trial: optuna.Trial) -> float:
        max_gate = (
            trial.suggest_float("max_spatial_gate", 0.15, 0.45, step=0.05)
            if use_spatial_attention
            else 0.0
        )
        attn_temp = (
            trial.suggest_float("attention_temp", 0.5, 2.0, step=0.25)
            if use_spatial_attention
            else 1.0
        )
        cfg = {
            "hidden_dim": trial.suggest_categorical("hidden_dim", [64, 128, 192, 256]),
            "n_layers": trial.suggest_int("n_layers", 1, 3),
            "station_emb_dim": trial.suggest_categorical("station_emb_dim", [8, 16, 32]),
            "activation": trial.suggest_categorical("activation", ["gelu", "silu", "relu"]),
            "norm_type": trial.suggest_categorical("norm_type", ["layernorm", "batchnorm"]),
            "dropout": trial.suggest_float("dropout", 0.10, 0.40, step=0.05),
            "learning_rate": trial.suggest_float("learning_rate", 2e-4, 3e-3, log=True),
            "weight_decay": trial.suggest_float("weight_decay", 1e-5, 1e-2, log=True),
            "batch_size": trial.suggest_categorical("batch_size", [64, 128, 256, 512]),
            "loss_type": trial.suggest_categorical("loss_type", ["huber", "mse"]),
            "huber_delta": trial.suggest_categorical("huber_delta", [10.0, 15.0, 20.0]),
            "max_spatial_gate": max_gate,
            "attention_temp": attn_temp,
            "seed": 42,
        }
        _, best_val_rmse, best_ep, _, _ = train_ann_with_config(
            data=data,
            spatial_prior=spatial_prior,
            config=cfg,
            max_epochs=tune_epochs,
            patience=tune_patience,
            device=device,
            trial=trial,
            verbose=False,
        )
        trial.set_user_attr("best_epoch", best_ep)
        print(
            f"  [Trial {trial.number + 1:02d}/{n_trials:02d}] "
            f"Val RMSE = {best_val_rmse:6.3f} (epoch {best_ep:02d}) | "
            f"dim={cfg['hidden_dim']}, layers={cfg['n_layers']}, "
            f"act={cfg['activation']}, drop={cfg['dropout']:.2f}, "
            f"lr={cfg['learning_rate']:.2e}, loss={cfg['loss_type']}"
        )
        return best_val_rmse

    sampler = optuna.samplers.TPESampler(seed=42)
    pruner = optuna.pruners.MedianPruner(n_startup_trials=4, n_warmup_steps=12)
    study = optuna.create_study(direction="minimize", sampler=sampler, pruner=pruner)

    # Đưa cấu hình mặc định tốt vào làm Trial đầu tiên để làm mốc chuẩn
    init_trial = dict(DEFAULT_BEST_CONFIG)
    if not use_spatial_attention:
        init_trial["max_spatial_gate"] = 0.0
    study.enqueue_trial(init_trial)
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    best_cfg = dict(DEFAULT_BEST_CONFIG)
    best_cfg.update(study.best_trial.params)
    if not use_spatial_attention:
        best_cfg["max_spatial_gate"] = 0.0
        best_cfg["attention_temp"] = 1.0

    trials_records = []
    for t in study.trials:
        rec = {
            "trial_number": t.number,
            "state": str(t.state.name),
            "val_rmse": t.value if t.value is not None else np.nan,
            "best_epoch": t.user_attrs.get("best_epoch", None),
        }
        rec.update(t.params)
        trials_records.append(rec)
    trials_df = pd.DataFrame(trials_records)

    print("-" * 78)
    print(f"Best Trial: #{study.best_trial.number + 1} | Best Val RMSE: {study.best_value:.4f}")
    print(f"Siêu tham số tối ưu: {json.dumps(best_cfg, ensure_ascii=False)}")
    return best_cfg, trials_df


# ============================================================
# 6. CƠ CHẾ PHÂN TÍCH % MÔ HÌNH DỰA VÀO DỮ LIỆU CỦA TỪNG TỈNH & STATION_ID
# ============================================================

def analyze_province_and_station_contributions(
    model: SpatioTemporalGlobalANN,
    data: SpatioTemporalSplitData,
    device: torch.device,
) -> Dict:
    """
    Tính toán định lượng mô hình ANN đang dựa vào dữ liệu của tỉnh nào và `station_id`
    nào bao nhiêu % để đưa ra quyết định, thông qua 3 cơ chế độc lập:

    1. Cơ chế 1 — Trọng số Quyết định Không gian (`Spatial Attention & Gate Reliance %`):
       Trích xuất ma trận trọng số thực tế W (15x15) trên tập Test, gom theo 9 tỉnh thành
       ma trận 9x9 (khi dự báo cho tỉnh i thì dựa bao nhiêu % vào tỉnh j) và tính
       tỷ trọng đóng góp trung bình trên toàn bộ quyết định (Global Attention Share %).

    2. Cơ chế 2 — Độ nhạy Đạo hàm Ngược (`Gradient x Input Jacobian Attribution %`):
       Tính đạo hàm riêng |d(y_hat)/d(X_station) * X_station| bằng Backpropagation
       cho từng `station_id` và từng `province_name`, chuẩn hóa về tổng 100%.

    3. Cơ chế 3 — Đóng góp Năng lượng Gradient Huấn luyện (`Training Gradient Energy %`):
       Đo lường mỗi tỉnh đóng góp bao nhiêu % năng lượng đạo hàm (L1 Gradient Norm)
       trong quá trình học so với tỷ lệ số dòng dữ liệu (`Sample Share %`), và kết hợp
       cả 3 cơ chế thành chỉ số Tổng hợp (`Composite Decision Reliance %`).
    """
    model.eval()
    station_meta = data.station_meta.sort_values("station_idx").reset_index(drop=True)
    n_stations = len(station_meta)
    provinces = sorted(station_meta["province_name"].unique().tolist())

    # --------------------------------------------------------
    # CƠ CHẾ 1: SPATIAL ATTENTION & GATE RELIANCE (%)
    # --------------------------------------------------------
    with torch.no_grad():
        _, eff_weights = model(
            data.X_test.to(device),
            data.lag0_test.to(device),
            return_attention=True,
        )
        # eff_weights: (T_test, 15_target, 15_source), mỗi hàng tổng = 1.0
        mean_station_attn = eff_weights.mean(dim=0).cpu().numpy()  # (15, 15)

    # Gom ma trận 15x15 theo tỉnh thành ma trận 9x9:
    # Hàng: Tỉnh đang cần dự báo (Target Province)
    # Cột : Tỉnh cung cấp dữ liệu tham chiếu (Source Province) -> Tổng mỗi hàng = 100%
    prov_to_indices = {
        p: station_meta.index[station_meta["province_name"] == p].tolist()
        for p in provinces
    }

    prov_attn_matrix = pd.DataFrame(0.0, index=provinces, columns=provinces)
    for target_p in provinces:
        t_idxs = prov_to_indices[target_p]
        for source_p in provinces:
            s_idxs = prov_to_indices[source_p]
            # Trung bình trên các trạm thuộc target_p, cộng tổng các trạm thuộc source_p
            share = float(mean_station_attn[np.ix_(t_idxs, s_idxs)].sum(axis=1).mean() * 100.0)
            prov_attn_matrix.loc[target_p, source_p] = share

    # Trọng số chú ý toàn cục (trên tất cả 15 trạm dự báo, mỗi trạm nguồn j đóng góp bao nhiêu %)
    station_global_attn_pct = mean_station_attn.mean(axis=0) * 100.0  # (15,), tổng = 100%
    prov_global_attn_pct = {
        p: float(station_global_attn_pct[prov_to_indices[p]].sum()) for p in provinces
    }

    # --------------------------------------------------------
    # CƠ CHẾ 2: GRADIENT x INPUT JACOBIAN SALIENCY (%) TRÊN TẬP TEST
    # --------------------------------------------------------
    X_test_grad = data.X_test.clone().detach().to(device).requires_grad_(True)
    lag0_test_grad = data.lag0_test.clone().detach().to(device).requires_grad_(True)

    preds_test, _ = model(X_test_grad, lag0_test_grad, return_attention=False)
    # Tổng dự báo trên toàn bộ tập Test để lấy đạo hàm đối với mọi đầu vào
    total_output = preds_test.sum()
    model.zero_grad()
    total_output.backward()

    # Đạo hàm theo đặc trưng chuẩn hóa X_test và theo neo lag0
    grad_x = (X_test_grad.grad.detach().abs() * X_test_grad.detach().abs()).cpu().numpy()  # (T, 15, F)
    grad_lag0 = (lag0_test_grad.grad.detach().abs() * lag0_test_grad.detach().abs()).cpu().numpy()  # (T, 15)

    # Mức đóng góp Gradient x Input của từng trạm (gồm toàn bộ đặc trưng F + nhánh neo lag0)
    station_grad_raw = grad_x.sum(axis=(0, 2)) + grad_lag0.sum(axis=0)  # (15,)
    station_grad_pct = 100.0 * station_grad_raw / max(float(station_grad_raw.sum()), 1e-12)
    prov_grad_pct = {
        p: float(station_grad_pct[prov_to_indices[p]].sum()) for p in provinces
    }

    # Độ quan trọng của từng biến đặc trưng (Feature Importance % theo Gradient x Input)
    feat_grad_raw = grad_x.sum(axis=(0, 1))  # (F,)
    # Cộng phần đạo hàm trực tiếp trên nhánh neo lag0 vào cột pm25_lag0
    if "pm25_lag0" in data.all_feature_cols:
        lag0_col_idx = data.all_feature_cols.index("pm25_lag0")
        feat_grad_raw[lag0_col_idx] += grad_lag0.sum()
    feat_importance_pct = 100.0 * feat_grad_raw / max(float(feat_grad_raw.sum()), 1e-12)

    feature_importance_df = (
        pd.DataFrame(
            {
                "feature": data.all_feature_cols,
                "importance_pct": feat_importance_pct,
            }
        )
        .sort_values("importance_pct", ascending=False)
        .reset_index(drop=True)
    )

    # --------------------------------------------------------
    # CƠ CHẾ 3: TỶ TRỌNG NĂNG LƯỢNG GRADIENT HUẤN LUYỆN THEO TỈNH (%)
    # --------------------------------------------------------
    X_train_g = data.X_train.clone().detach().to(device).requires_grad_(True)
    lag0_train_g = data.lag0_train.clone().detach().to(device).requires_grad_(True)
    y_train_g = data.y_train.to(device)

    pred_train, _ = model(X_train_g, lag0_train_g, return_attention=False)
    # Sai số bình phương tại từng trạm để xem tỉnh nào tạo ra lực cập nhật trọng số lớn nhất
    loss_train = F.smooth_l1_loss(pred_train, y_train_g, reduction="sum", beta=15.0)
    model.zero_grad()
    loss_train.backward()

    train_grad_energy_station = (
        X_train_g.grad.detach().abs().sum(dim=(0, 2)).cpu().numpy()
        + lag0_train_g.grad.detach().abs().sum(dim=0).cpu().numpy()
    )
    train_grad_energy_pct = (
        100.0 * train_grad_energy_station / max(float(train_grad_energy_station.sum()), 1e-12)
    )

    # --------------------------------------------------------
    # TỔNG HỢP BẢNG ĐÓNG GÓP THEO STATION_ID VÀ THEO TỈNH
    # --------------------------------------------------------
    gates_np = model.get_station_gates().detach().cpu().numpy() * 100.0

    station_contrib_df = station_meta[
        ["station_idx", "station_id", "station_code", "province_name", "station_name"]
    ].copy()
    station_contrib_df["sample_share_pct"] = 100.0 / n_stations
    station_contrib_df["spatial_gate_open_pct"] = gates_np
    station_contrib_df["spatial_attention_share_pct"] = station_global_attn_pct
    station_contrib_df["gradient_saliency_share_pct"] = station_grad_pct
    station_contrib_df["train_gradient_energy_pct"] = train_grad_energy_pct
    station_contrib_df["composite_decision_pct"] = (
        0.40 * station_contrib_df["spatial_attention_share_pct"]
        + 0.40 * station_contrib_df["gradient_saliency_share_pct"]
        + 0.20 * station_contrib_df["train_gradient_energy_pct"]
    )
    station_contrib_df = station_contrib_df.sort_values(
        "composite_decision_pct", ascending=False
    ).reset_index(drop=True)

    prov_rows = []
    for p in provinces:
        idxs = prov_to_indices[p]
        st_ids = station_meta.loc[idxs, "station_id"].tolist()
        st_codes = station_meta.loc[idxs, "station_code"].tolist()
        sample_pct = 100.0 * len(idxs) / n_stations
        attn_pct = prov_global_attn_pct[p]
        grad_pct = prov_grad_pct[p]
        train_e_pct = float(train_grad_energy_pct[idxs].sum())
        self_reliance_pct = float(prov_attn_matrix.loc[p, p])
        cross_prov_reliance_pct = 100.0 - self_reliance_pct
        composite_pct = 0.40 * attn_pct + 0.40 * grad_pct + 0.20 * train_e_pct

        prov_rows.append(
            {
                "province_name": p,
                "n_stations": len(idxs),
                "station_ids": ";".join(st_ids),
                "station_codes": ";".join(st_codes),
                "sample_share_pct": sample_pct,
                "spatial_attention_share_pct": attn_pct,
                "gradient_saliency_share_pct": grad_pct,
                "train_gradient_energy_pct": train_e_pct,
                "composite_decision_pct": composite_pct,
                "self_province_reliance_pct": self_reliance_pct,
                "other_provinces_reliance_pct": cross_prov_reliance_pct,
            }
        )

    province_contrib_df = (
        pd.DataFrame(prov_rows)
        .sort_values("composite_decision_pct", ascending=False)
        .reset_index(drop=True)
    )

    # Nhóm đóng góp theo loại đặc trưng (Feature Group Breakdown %)
    station_id_cols = [c for c in data.all_feature_cols if c.startswith("station_id_")]
    lag_cols = [c for c in data.all_feature_cols if c.startswith("pm25_") or c in EXTRA_POLLUTANT_LAG0_COLS]
    weather_thermo_cols = WEATHER_COLS + THERMO_DERIVED_COLS
    time_space_cols = ["hour", "day_of_week", "is_weekend"] + CYCLIC_TIME_COLS + SPATIAL_COORD_COLS

    feat_map = dict(zip(feature_importance_df["feature"], feature_importance_df["importance_pct"]))
    feature_group_pct = {
        "pm25_lags_and_pollutants_pct": float(sum(feat_map.get(c, 0.0) for c in lag_cols)),
        "weather_and_thermodynamics_pct": float(sum(feat_map.get(c, 0.0) for c in weather_thermo_cols)),
        "station_id_identity_pct": float(sum(feat_map.get(c, 0.0) for c in station_id_cols)),
        "time_and_spatial_coords_pct": float(sum(feat_map.get(c, 0.0) for c in time_space_cols)),
    }

    return {
        "province_contrib_df": province_contrib_df,
        "station_contrib_df": station_contrib_df,
        "prov_attn_matrix_df": prov_attn_matrix,
        "feature_importance_df": feature_importance_df,
        "feature_group_pct": feature_group_pct,
    }


# ============================================================
# 7. ĐÁNH GIÁ KẾT QUẢ TOÀN CỤC, THEO TỈNH & THEO STATION_ID
# ============================================================

def load_existing_province_best_rmse(province_name: str) -> Tuple[Optional[str], Optional[float]]:
    """Đọc kết quả tốt nhất trong 5 mô hình ML từng tỉnh (nếu có) để đối sánh trực tiếp."""
    p_file = PROVINCE_OUTPUT_DIR / slugify(province_name) / "metrics.json"
    if not p_file.exists():
        return None, None
    try:
        m = json.loads(p_file.read_text(encoding="utf-8"))
        best_model = None
        best_rmse = float("inf")
        for m_name, res in m.get("results", {}).items():
            r = float(res["test"]["rmse"])
            if r < best_rmse:
                best_rmse = r
                best_model = m_name
        return best_model, best_rmse
    except Exception:
        return None, None


def run_pipeline(args: argparse.Namespace) -> None:
    t0 = time.perf_counter()
    use_spatial_attention = not args.no_spatial_attention
    out_dir = (
        GLOBAL_ANN_DIR
        if use_spatial_attention
        else BASE_DIR / "output" / f"global_ann_no_attention{OUTPUT_SUFFIX}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    device = resolve_device(args.device)

    attn_mode_str = (
        "CÓ SPATIAL ATTENTION (Cross-Province Attention Gate > 0)"
        if use_spatial_attention
        else "KHÔNG CÓ SPATIAL ATTENTION (Ablation: max_spatial_gate = 0.0)"
    )
    print("=" * 78)
    print("MÔ HÌNH ANN TOÀN CỤC (GLOBAL SPATIO-TEMPORAL ANN) — TOÀN BỘ 9 TỈNH / 15 TRẠM")
    print(f"Chế độ kiến trúc  : {attn_mode_str}")
    print("=" * 78)
    print(f"Thiết bị thực thi : {device} (PyTorch {torch.__version__})")
    print(f"Thư mục lưu đầu ra: {out_dir}")

    # 1. Nạp dữ liệu & tạo đặc trưng giữ nguyên station_id (ưu tiên Gold Lakehouse Parquet)
    use_parquet = not getattr(args, "no_parquet", False)
    feats, station_meta, continuous_cols, binary_and_station_cols, all_feature_cols = (
        load_features_dataset(use_parquet=use_parquet)
    )

    print(f"Tổng bản ghi mẫu đặc trưng : {len(feats):,} dòng | Số tỉnh: {station_meta['province_name'].nunique()} | Số trạm (station_id): {len(station_meta)}")
    print("\n--- BẢNG ÁNH XẠ STATION_ID <-> STATION_CODE <-> TỈNH ---")
    print(f"{'Idx':<4} {'station_id (Giữ nguyên 100%)':<31} {'station_code':<17} {'Tỉnh/Thành':<12}")
    for _, row in station_meta.iterrows():
        print(
            f"{int(row['station_idx']):<4} {row['station_id']:<31} "
            f"{row['station_code']:<17} {row['province_name']:<12}"
        )

    # 2. Chia Train / Val / Test đúng tỉ lệ 70 / 10 / 20 theo thời gian
    data = prepare_spatiotemporal_splits(
        feats=feats,
        station_meta=station_meta,
        continuous_cols=continuous_cols,
        all_feature_cols=all_feature_cols,
    )
    n_total = len(feats)
    n_train = len(data.df_train)
    n_val = len(data.df_val)
    n_test = len(data.df_test)

    print("\n--- CHIA TẬP DỮ LIỆU THEO THỜI GIAN (70% TRAIN / 10% VAL / 20% TEST) ---")
    print(
        f"Tổng mẫu sau tạo đặc trưng: {n_total:,} mẫu ({len(all_feature_cols)} đặc trưng + Embedding station_id)\n"
        f"  - Train (70%): {n_train:,} mẫu ({n_train/n_total:.1%}) | Đến: {data.train_end}\n"
        f"  - Val   (10%): {n_val:,} mẫu ({n_val/n_total:.1%}) | Đến: {data.val_end}\n"
        f"  - Test  (20%): {n_test:,} mẫu ({n_test/n_total:.1%}) | Sau: {data.val_end}"
    )

    spatial_prior = build_spatial_prior_matrix(station_meta)

    # 3. Tối ưu hóa Hyper-parameter bằng Optuna (hoặc dùng cấu hình tối ưu mặc định)
    if not args.no_tune and args.n_trials > 0 and optuna is not None:
        best_config, trials_df = run_hyperparameter_search(
            data=data,
            spatial_prior=spatial_prior,
            n_trials=args.n_trials,
            tune_epochs=args.tune_epochs,
            tune_patience=args.tune_patience,
            device=device,
            use_spatial_attention=use_spatial_attention,
        )
        trials_df.to_csv(out_dir / "optuna_trials.csv", index=False, encoding="utf-8-sig")
    else:
        if not args.no_tune and args.n_trials > 0 and optuna is None:
            print("\n[CẢNH BÁO: Thư viện 'optuna' chưa được cài đặt -> Tự động bỏ qua HPO và dùng bộ siêu tham số tối ưu đã lưu]")
        else:
            print("\n[Bỏ qua Optuna HPO theo cờ --no-tune -> Sử dụng bộ siêu tham số đã tối ưu]")
        # Nếu đã có kết quả HPO trước đó trong output/global_ann/metrics.json thì kế thừa để so sánh công bằng
        existing_metrics_path = GLOBAL_ANN_DIR / "metrics.json"
        if existing_metrics_path.exists():
            try:
                prev_m = json.loads(existing_metrics_path.read_text(encoding="utf-8"))
                best_config = dict(prev_m.get("best_hyperparameters", DEFAULT_BEST_CONFIG))
            except Exception:
                best_config = dict(DEFAULT_BEST_CONFIG)
        else:
            best_config = dict(DEFAULT_BEST_CONFIG)

    if not use_spatial_attention:
        best_config["max_spatial_gate"] = 0.0
        best_config["attention_temp"] = 1.0

    # 4. Huấn luyện chính thức nhiều epochs với Early Stopping
    overfit_guard_str = (
        f"BẬT (ratio>{args.max_overfit_ratio}, patience={args.overfit_patience or max(10, args.patience // 2)})"
        if args.overfit_guard
        else "TẮT (chỉ dừng theo Patience)"
    )
    print("\n" + "=" * 78)
    print(
        f"PHA 2: HUẤN LUYỆN MÔ HÌNH GLOBAL ANN NHIỀU EPOCHS "
        f"(max_epochs={args.epochs}, patience={args.patience}, overfit_guard={overfit_guard_str})"
    )
    print("=" * 78)

    model, best_val_rmse, best_epoch, stop_reason, history = train_ann_with_config(
        data=data,
        spatial_prior=spatial_prior,
        config=best_config,
        max_epochs=args.epochs,
        patience=args.patience,
        device=device,
        trial=None,
        verbose=True,
        use_overfit_guard=args.overfit_guard,
        max_overfit_ratio=args.max_overfit_ratio,
        overfit_patience=args.overfit_patience,
    )
    print("-" * 78)
    print(f"Kết thúc huấn luyện: {stop_reason}")
    print(f"Đã khôi phục trọng số tốt nhất tại Epoch {best_epoch} (Best Val RMSE = {best_val_rmse:.4f})")

    pd.DataFrame(history).to_csv(
        out_dir / "epoch_history.csv", index=False, encoding="utf-8-sig"
    )

    # 5. Dự báo và Đánh giá trên toàn bộ dữ liệu, từng tỉnh và từng station_id
    _, pred_val_grid = evaluate_model_rmse(model, data.X_val, data.y_val, data.lag0_val, device)
    _, pred_test_grid = evaluate_model_rmse(model, data.X_test, data.y_test, data.lag0_test, device)

    y_val_flat = data.df_val["y"].to_numpy(dtype=np.float64)
    base_val_flat = data.df_val["pm25_lag0"].to_numpy(dtype=np.float64)
    pred_val_flat = pred_val_grid.reshape(-1).astype(np.float64)

    y_test_flat = data.df_test["y"].to_numpy(dtype=np.float64)
    base_test_flat = data.df_test["pm25_lag0"].to_numpy(dtype=np.float64)
    pred_test_flat = pred_test_grid.reshape(-1).astype(np.float64)

    global_base_val_m = metrics(y_val_flat, base_val_flat)
    global_base_test_m = metrics(y_test_flat, base_test_flat)
    global_ann_val_m = metrics(y_val_flat, pred_val_flat)
    global_ann_test_m = metrics(y_test_flat, pred_test_flat)
    global_imp_pct = (
        100.0 * (global_base_test_m["rmse"] - global_ann_test_m["rmse"]) / global_base_test_m["rmse"]
    )

    print("\n" + "=" * 78)
    print("KẾT QUẢ ĐÁNH GIÁ TOÀN CỤC TRÊN TẬP VALIDATION & TEST (TOÀN BỘ 9 TỈNH / 15 TRẠM)")
    print("=" * 78)
    print(f"{'Mô hình':<28}{'RMSE':>10}{'MAE':>10}{'R2':>10}{'So với Baseline':>18}")
    print(
        f"{'Val  Baseline (24h)':<28}{global_base_val_m['rmse']:>10.2f}"
        f"{global_base_val_m['mae']:>10.2f}{global_base_val_m['r2']:>10.3f}"
    )
    print(
        f"{'Val  Global ANN':<28}{global_ann_val_m['rmse']:>10.2f}"
        f"{global_ann_val_m['mae']:>10.2f}{global_ann_val_m['r2']:>10.3f}"
    )
    print(
        f"{'Test Baseline (24h)':<28}{global_base_test_m['rmse']:>10.2f}"
        f"{global_base_test_m['mae']:>10.2f}{global_base_test_m['r2']:>10.3f}"
    )
    print(
        f"{'Test Global ANN':<28}{global_ann_test_m['rmse']:>10.2f}"
        f"{global_ann_test_m['mae']:>10.2f}{global_ann_test_m['r2']:>10.3f}"
        f"{global_imp_pct:>+16.2f}%"
    )

    # Đánh giá chi tiết theo từng tỉnh (9 tỉnh) và từng station_id (15 trạm)
    pred_df = data.df_test[
        [
            "station_id",
            "station_code",
            "station_name",
            "province_id",
            "province_name",
            "latitude",
            "longitude",
            "origin_time",
            "target_time",
            "y",
        ]
    ].copy()
    pred_df = pred_df.rename(columns={"y": "y_true"})
    pred_df["y_baseline"] = base_test_flat
    pred_df["y_global_ann"] = pred_test_flat
    pred_df.to_csv(out_dir / "predictions.csv", index=False, encoding="utf-8-sig")

    print("\n" + "=" * 78)
    print("ĐÁNH GIÁ CHI TIẾT THEO TỪNG TỈNH TRÊN TẬP TEST (GLOBAL ANN VS. BASELINE & LOCAL ML)")
    print("=" * 78)
    print(
        f"{'Tỉnh/Thành':<13}{'Số mẫu':>7}{'Base RMSE':>11}{'Global ANN':>12}"
        f"{'Cải thiện':>11}{'Best Local ML (RMSE)':>24}"
    )

    per_province_metrics = {}
    provinces_beat_baseline = 0
    for prov in sorted(station_meta["province_name"].unique()):
        mask = (pred_df["province_name"] == prov).to_numpy()
        bm = metrics(y_test_flat[mask], base_test_flat[mask])
        am = metrics(y_test_flat[mask], pred_test_flat[mask])
        imp = 100.0 * (bm["rmse"] - am["rmse"]) / bm["rmse"]
        if imp > 0:
            provinces_beat_baseline += 1

        best_local_name, best_local_rmse = load_existing_province_best_rmse(prov)
        local_str = (
            f"{best_local_name} ({best_local_rmse:.2f})"
            if best_local_name is not None and best_local_rmse is not None
            else "N/A"
        )
        print(
            f"{prov:<13}{int(mask.sum()):>7}{bm['rmse']:>11.2f}{am['rmse']:>12.2f}"
            f"{imp:>+10.1f}%{local_str:>24}"
        )
        per_province_metrics[prov] = {
            "n_test": int(mask.sum()),
            "baseline": bm,
            "global_ann": am,
            "rmse_improvement_pct": imp,
            "beats_baseline": bool(imp > 0),
            "best_single_province_model": best_local_name,
            "best_single_province_rmse": best_local_rmse,
        }
    print(f"-> Global ANN vượt Baseline tại {provinces_beat_baseline}/{len(per_province_metrics)} tỉnh thành.")

    per_station_metrics = {}
    for _, srow in station_meta.iterrows():
        st_id = srow["station_id"]
        st_code = srow["station_code"]
        prov = srow["province_name"]
        mask = (pred_df["station_id"] == st_id).to_numpy()
        bm = metrics(y_test_flat[mask], base_test_flat[mask])
        am = metrics(y_test_flat[mask], pred_test_flat[mask])
        imp = 100.0 * (bm["rmse"] - am["rmse"]) / bm["rmse"]
        per_station_metrics[st_id] = {
            "station_id": st_id,
            "station_code": st_code,
            "province_name": prov,
            "n_test": int(mask.sum()),
            "baseline": bm,
            "global_ann": am,
            "rmse_improvement_pct": imp,
        }

    # 6. Phân tích % đóng góp của từng tỉnh và từng station_id vào quyết định của mô hình
    contrib = analyze_province_and_station_contributions(model, data, device)
    prov_contrib_df: pd.DataFrame = contrib["province_contrib_df"]
    station_contrib_df: pd.DataFrame = contrib["station_contrib_df"]
    prov_attn_matrix_df: pd.DataFrame = contrib["prov_attn_matrix_df"]
    feature_importance_df: pd.DataFrame = contrib["feature_importance_df"]
    feature_group_pct: Dict = contrib["feature_group_pct"]

    prov_contrib_df.to_csv(
        out_dir / "province_contribution_summary.csv", index=False, encoding="utf-8-sig"
    )
    station_contrib_df.to_csv(
        out_dir / "station_id_contribution_pct.csv", index=False, encoding="utf-8-sig"
    )
    prov_attn_matrix_df.to_csv(
        out_dir / "province_attention_matrix_pct.csv", encoding="utf-8-sig"
    )
    feature_importance_df.to_csv(
        out_dir / "feature_importance_pct.csv", index=False, encoding="utf-8-sig"
    )

    print("\n" + "=" * 78)
    print("PHÂN TÍCH % MÔ HÌNH DỰA VÀO DỮ LIỆU CỦA TỪNG TỈNH ĐỂ ĐƯA RA QUYẾT ĐỊNH")
    print("=" * 78)
    print(
        f"{'Tỉnh/Thành':<13}{'Trạm':>5}{'% Mẫu':>8}{'% Attention':>13}"
        f"{'% GradxInput':>14}{'% Grad Train':>14}{'% Tổng hợp':>12}"
    )
    for _, r in prov_contrib_df.iterrows():
        print(
            f"{r['province_name']:<13}{int(r['n_stations']):>5}"
            f"{r['sample_share_pct']:>7.2f}%"
            f"{r['spatial_attention_share_pct']:>12.2f}%"
            f"{r['gradient_saliency_share_pct']:>13.2f}%"
            f"{r['train_gradient_energy_pct']:>13.2f}%"
            f"{r['composite_decision_pct']:>11.2f}%"
        )
    print("-" * 78)
    print(
        f"{'TỔNG CỘNG':<13}{len(station_meta):>5}"
        f"{prov_contrib_df['sample_share_pct'].sum():>7.2f}%"
        f"{prov_contrib_df['spatial_attention_share_pct'].sum():>12.2f}%"
        f"{prov_contrib_df['gradient_saliency_share_pct'].sum():>13.2f}%"
        f"{prov_contrib_df['train_gradient_energy_pct'].sum():>13.2f}%"
        f"{prov_contrib_df['composite_decision_pct'].sum():>11.2f}%"
    )

    print("\n--- MA TRẬN TRỌNG SỐ QUYẾT ĐỊNH KHÔNG GIAN LIÊN TỈNH (ĐƠN VỊ: %, TỔNG MỖI HÀNG = 100%) ---")
    print("(Hàng = Tỉnh cần dự báo | Cột = % dữ liệu mô hình tham chiếu từ tỉnh tương ứng)")
    print(prov_attn_matrix_df.round(2).to_string())

    print("\n--- PHÂN BỔ % ĐÓNG GÓP THEO TỪNG STATION_ID (GIỮ NGUYÊN STATION_ID GỐC) ---")
    print(
        f"{'station_id':<31}{'station_code':<17}{'Tỉnh':<12}"
        f"{'% Attention':>12}{'% GradxInput':>13}{'% Tổng hợp':>12}"
    )
    for _, r in station_contrib_df.iterrows():
        print(
            f"{r['station_id']:<31}{r['station_code']:<17}{r['province_name']:<12}"
            f"{r['spatial_attention_share_pct']:>11.2f}%"
            f"{r['gradient_saliency_share_pct']:>12.2f}%"
            f"{r['composite_decision_pct']:>11.2f}%"
        )

    print("\n--- TỶ TRỌNG ĐÓNG GÓP THEO NHÓM ĐẶC TRƯNG (%) ---")
    for grp_name, grp_val in feature_group_pct.items():
        print(f"  - {grp_name:<35}: {grp_val:6.2f}%")

    # 7. Lưu checkpoint PyTorch và báo cáo JSON đầy đủ
    torch.save(
        {
            "model_state_dict": model.state_dict(),
            "best_config": best_config,
            "best_epoch": best_epoch,
            "best_val_rmse": best_val_rmse,
            "stop_reason": stop_reason,
            "continuous_cols": continuous_cols,
            "all_feature_cols": all_feature_cols,
            "scaler_mean": data.scaler.mean_.tolist(),
            "scaler_scale": data.scaler.scale_.tolist(),
            "station_meta": station_meta.to_dict(orient="records"),
        },
        out_dir / "best_ann_model.pt",
    )

    elapsed_sec = time.perf_counter() - t0
    summary = {
        "model_type": (
            "SpatioTemporalGlobalANN (With Spatial Attention)"
            if use_spatial_attention
            else "GlobalANN (No Spatial Attention - Ablation)"
        ),
        "use_spatial_attention": use_spatial_attention,
        "device": str(device),
        "elapsed_seconds": round(elapsed_sec, 2),
        "n_total_samples": n_total,
        "n_stations": len(station_meta),
        "n_provinces": int(station_meta["province_name"].nunique()),
        "split_ratio": {"train": 0.70, "val": 0.10, "test": 0.20},
        "n_train": n_train,
        "n_val": n_val,
        "n_test": n_test,
        "train_end": str(data.train_end),
        "val_end": str(data.val_end),
        "best_hyperparameters": best_config,
        "best_epoch": best_epoch,
        "max_epochs": args.epochs,
        "early_stopping_patience": args.patience,
        "use_overfit_guard": bool(args.overfit_guard),
        "stop_reason": stop_reason,
        "global_metrics": {
            "baseline_val": global_base_val_m,
            "baseline_test": global_base_test_m,
            "global_ann_val": global_ann_val_m,
            "global_ann_test": global_ann_test_m,
            "test_rmse_improvement_pct": global_imp_pct,
            "provinces_beating_baseline": f"{provinces_beat_baseline}/{len(per_province_metrics)}",
        },
        "feature_group_contribution_pct": feature_group_pct,
        "province_decision_contribution_pct": prov_contrib_df.to_dict(orient="records"),
        "station_id_decision_contribution_pct": station_contrib_df.to_dict(orient="records"),
        "province_attention_matrix_pct": prov_attn_matrix_df.round(4).to_dict(),
        "per_province_test_metrics": per_province_metrics,
        "per_station_id_test_metrics": per_station_metrics,
    }

    with open(out_dir / "metrics.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # Nếu đang chạy chế độ --no-spatial-attention và đã có kết quả Có Attention trước đó -> in bảng đối sánh
    with_attn_file = GLOBAL_ANN_DIR / "metrics.json"
    if not use_spatial_attention and with_attn_file.exists():
        try:
            with_m = json.loads(with_attn_file.read_text(encoding="utf-8"))
            w_val = with_m["global_metrics"]["global_ann_val"]
            w_test = with_m["global_metrics"]["global_ann_test"]
            print("\n" + "=" * 78)
            print("ĐỐI SÁNH TRỰC TIẾP: GLOBAL ANN CÓ VS. KHÔNG CÓ SPATIAL ATTENTION")
            print("=" * 78)
            print(f"{'Phiên bản Mô hình':<36}{'Val RMSE':>10}{'Test RMSE':>11}{'Test MAE':>10}{'Test R2':>10}")
            print(
                f"{'1. Có Spatial Attention':<36}{w_val['rmse']:>10.2f}"
                f"{w_test['rmse']:>11.2f}{w_test['mae']:>10.2f}{w_test['r2']:>10.3f}"
            )
            print(
                f"{'2. Không có Spatial Attention':<36}{global_ann_val_m['rmse']:>10.2f}"
                f"{global_ann_test_m['rmse']:>11.2f}{global_ann_test_m['mae']:>10.2f}{global_ann_test_m['r2']:>10.3f}"
            )
            print("-" * 78)
            print(f"{'Tỉnh/Thành':<15}{'Có Attention RMSE':>20}{'Không Attention RMSE':>22}{'Chênh lệch (No - With)':>21}")
            for prov in sorted(per_province_metrics.keys()):
                r_with = with_m["per_province_test_metrics"][prov]["global_ann"]["rmse"]
                r_no = per_province_metrics[prov]["global_ann"]["rmse"]
                diff = r_no - r_with
                print(f"{prov:<15}{r_with:>20.2f}{r_no:>22.2f}{diff:>+21.2f}")
        except Exception:
            pass

    print("\n" + "=" * 78)
    print(f"HOÀN TẤT TRONG {elapsed_sec:.1f} GIÂY! Toàn bộ kết quả đã lưu tại: {out_dir}")
    print("  1. best_ann_model.pt                 (Trọng số mô hình PyTorch + Scaler + Station ID Map)")
    print("  2. metrics.json                      (Báo cáo đánh giá tổng hợp & theo từng tỉnh/station_id)")
    print("  3. predictions.csv                   (Dự báo tập Test giữ nguyên station_id & province_name)")
    print("  4. province_contribution_summary.csv (Bảng % đóng góp quyết định của 9 tỉnh)")
    print("  5. province_attention_matrix_pct.csv (Ma trận 9x9 % tham chiếu không gian liên tỉnh)")
    print("  6. station_id_contribution_pct.csv   (Bảng % đóng góp chi tiết theo từng station_id)")
    print("  7. feature_importance_pct.csv        (Bảng % đóng góp của từng biến đặc trưng)")
    print("  8. epoch_history.csv                 (Lịch sử hội tụ qua các epochs)")
    print("=" * 78)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Huấn luyện mô hình Global Spatio-Temporal ANN trên toàn bộ dữ liệu 9 tỉnh / 15 trạm."
    )
    parser.add_argument(
        "--no-spatial-attention",
        action="store_true",
        help="Tắt hoàn toàn cơ chế Cross-Station Spatial Attention (khóa max_spatial_gate = 0.0) và lưu ra output/global_ann_no_attention/.",
    )
    parser.add_argument(
        "--n-trials",
        type=int,
        default=15,
        help="Số lượng trials tìm kiếm siêu tham số bằng Optuna (mặc định: 15).",
    )
    parser.add_argument(
        "--tune-epochs",
        type=int,
        default=80,
        help="Số epochs tối đa cho mỗi trial trong pha Optuna HPO (mặc định: 80).",
    )
    parser.add_argument(
        "--tune-patience",
        type=int,
        default=15,
        help="Early stopping patience trong pha Optuna HPO (mặc định: 15).",
    )
    parser.add_argument(
        "--no-tune",
        action="store_true",
        help="Bỏ qua pha Optuna HPO và dùng ngay bộ siêu tham số tối ưu mặc định.",
    )
    parser.add_argument(
        "--epochs",
        type=int,
        default=250,
        help="Số epochs tối đa khi huấn luyện mô hình cuối cùng (mặc định: 250).",
    )
    parser.add_argument(
        "--patience",
        type=int,
        default=30,
        help="Số epochs kiên nhẫn (Early Stopping Patience) khi huấn luyện chính thức (mặc định: 30).",
    )
    parser.add_argument(
        "--overfit-guard",
        action="store_true",
        help="Tùy chọn bật thêm cơ chế Overfitting Generalization Gap Guard (mặc định: TẮT, chỉ dùng Patience).",
    )
    parser.add_argument(
        "--max-overfit-ratio",
        type=float,
        default=1.45,
        help="Ngưỡng tỷ số Val/Train RMSE tối đa khi bật --overfit-guard (mặc định: 1.45).",
    )
    parser.add_argument(
        "--overfit-patience",
        type=int,
        default=None,
        help="Số vòng liên tiếp vượt ngưỡng Val/Train RMSE khi bật --overfit-guard (mặc định: patience // 2).",
    )
    parser.add_argument(
        "--no-parquet",
        action="store_true",
        help="Không sử dụng dữ liệu Parquet từ Gold Lakehouse, ép buộc nạp từ CSV và sinh đặc trưng bằng Pandas.",
    )
    parser.add_argument(
        "--device",
        type=str,
        default="auto",
        choices=["auto", "cuda", "cpu"],
        help="Thiết bị chạy PyTorch: auto (ưu tiên GPU CUDA nếu có), cuda hoặc cpu.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    run_pipeline(parse_args())
