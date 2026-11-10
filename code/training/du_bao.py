# -*- coding: utf-8 -*-
"""
Dự báo nồng độ bụi mịn PM2.5 cho 1 tỉnh từ dữ liệu dự báo thời tiết 24h tới
`data_test/du_bao/openmeteo_forecast_next_24h_11provinces.csv` kết hợp với
mô hình Global ANN đã huấn luyện (`code/training/train_global_ann.py`).

Kết quả xuất ra:
    data_test/du_bao/<ten_tinh>.csv   (ví dụ: data_test/du_bao/ha_noi.csv)

Cách chạy:
    # Mặc định dự báo cho tỉnh Hà Nội:
    python code/training/du_bao.py

    # Hoặc chọn bất kỳ tỉnh nào trong 11 tỉnh:
    python code/training/du_bao.py --province "Hà Nội"
    python code/training/du_bao.py --province "HCM"
    python code/training/du_bao.py --province "Đà Nẵng"
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import torch

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from common import (
    BASE_DIR,
    HORIZON,
    PM25_LAGS,
    ROLL_WINDOWS,
    SILVER_PROVINCE_DIR,
    SILVER_WEATHER_DIR,
    TEDP_CSV,
    WEATHER_COLS,
    WEATHER_CSV,
    load_merged,
    slugify,
)
from train_global_ann import (
    GLOBAL_ANN_DIR,
    SpatioTemporalGlobalANN,
    build_spatial_prior_matrix,
    resolve_device,
)

DU_BAO_DIR = BASE_DIR / "data_test" / "du_bao"
FORECAST_WEATHER_CSV = DU_BAO_DIR / "openmeteo_forecast_next_24h_11provinces.csv"
BRIDGE_WEATHER_72H_CSV = DU_BAO_DIR / "openmeteo_weather_bridge_72h_11provinces.csv"
SILVER_STATION_DIR = BASE_DIR / "lakehouse_storage" / "silver" / "tedp_station"


def classify_pm25_level(pm25_val: float) -> str:
    """Phân loại mức độ chất lượng không khí theo nồng độ PM2.5 (µg/m³ - tham chiếu QCVN 05:2023)."""
    if pd.isna(pm25_val):
        return "Không xác định (N/A)"
    if pm25_val <= 25.0:
        return "Tốt (Good)"
    if pm25_val <= 50.0:
        return "Trung bình (Moderate)"
    if pm25_val <= 75.0:
        return "Kém (Unhealthy for Sensitive)"
    if pm25_val <= 150.0:
        return "Xấu - Haze (Unhealthy)"
    return "Rất xấu / Nguy hại (Very Unhealthy)"


def load_partial_day_station_tedp(province_name: str, after_dt: pd.Timestamp) -> pd.DataFrame:
    """Nạp bổ sung các giờ đo thật trong ngày chưa trọn vẹn từ silver/tedp_station (sau mốc 23:00 ngày chẵn)."""
    if not SILVER_STATION_DIR.exists():
        return pd.DataFrame(columns=["datetime", "pm25", "pm10", "aqi"])
    try:
        st_df = pd.read_parquet(
            SILVER_STATION_DIR,
            filters=[("province_name", "==", province_name)],
        )
        if st_df.empty:
            return pd.DataFrame(columns=["datetime", "pm25", "pm10", "aqi"])
        st_df["datetime"] = pd.to_datetime(st_df["datetime"]).dt.floor("h")
        st_df = st_df[
            (st_df["datetime"] > after_dt)
            & (st_df["station_code"] != "BDi_KKXQ_KHITPH")
        ].copy()
        if st_df.empty:
            return pd.DataFrame(columns=["datetime", "pm25", "pm10", "aqi"])
        for col in ["pm25", "pm10", "aqi"]:
            med_col = f"{col}_median_filled"
            if med_col in st_df.columns:
                st_df.loc[st_df[med_col] == 1, col] = np.nan
        agg_partial = (
            st_df.groupby("datetime", as_index=False)[["pm25", "pm10", "aqi"]]
            .mean()
            .dropna(subset=["pm25"])
            .sort_values("datetime")
            .reset_index(drop=True)
        )
        return agg_partial
    except Exception:
        return pd.DataFrame(columns=["datetime", "pm25", "pm10", "aqi"])


def load_trained_ann_checkpoint(checkpoint_path: Path, device: torch.device) -> Tuple[SpatioTemporalGlobalANN, Dict]:
    if not checkpoint_path.exists():
        fallback = BASE_DIR / "output" / "global_ann" / "best_ann_model.pt"
        if fallback.exists():
            checkpoint_path = fallback
        else:
            raise FileNotFoundError(
                f"Không tìm thấy trọng số mô hình ANN tại {checkpoint_path}. "
                f"Hãy chạy `python code/training/train_global_ann.py` trước."
            )

    ckpt = torch.load(checkpoint_path, map_location=device, weights_only=False)
    station_meta = pd.DataFrame(ckpt["station_meta"]).sort_values("station_idx").reset_index(drop=True)
    best_config = ckpt["best_config"]
    all_feature_cols = ckpt["all_feature_cols"]

    spatial_prior = build_spatial_prior_matrix(station_meta).to(device)
    model = SpatioTemporalGlobalANN(
        num_features=len(all_feature_cols),
        num_stations=len(station_meta),
        spatial_prior=spatial_prior,
        hidden_dim=int(best_config["hidden_dim"]),
        n_layers=int(best_config["n_layers"]),
        station_emb_dim=int(best_config["station_emb_dim"]),
        activation=str(best_config["activation"]),
        dropout=float(best_config["dropout"]),
        norm_type=str(best_config["norm_type"]),
        max_spatial_gate=float(best_config["max_spatial_gate"]),
        attention_temp=float(best_config["attention_temp"]),
    ).to(device)

    model.load_state_dict(ckpt["model_state_dict"])
    model.eval()
    return model, ckpt


def resolve_target_province(province_query: str, station_meta: pd.DataFrame) -> pd.Series:
    """Tìm thông tin trạm/tỉnh khớp với tên tỉnh hoặc mã tỉnh người dùng truyền vào."""
    q_slug = slugify(province_query)
    for _, row in station_meta.iterrows():
        if (
            row["province_name"].lower() == province_query.strip().lower()
            or slugify(row["province_name"]) == q_slug
            or row["station_code"].lower() == province_query.strip().lower()
        ):
            return row

    available = ", ".join(station_meta["province_name"].tolist())
    raise ValueError(f"Không tìm thấy tỉnh '{province_query}'. Các tỉnh hỗ trợ: {available}")


@torch.no_grad()
def predict_single_province_ann(
    model: SpatioTemporalGlobalANN,
    x_prov: torch.Tensor,
    lag0_prov: torch.Tensor,
    station_idx: int,
    device: torch.device,
) -> np.ndarray:
    """
    Chạy suy luận (Inference) cho 1 tỉnh duy nhất (`station_idx`) bằng bộ tham số
    đã học của tỉnh đó trong mô hình SpatioTemporalGlobalANN.
    """
    model.eval()
    x_prov = x_prov.to(device)
    lag0_prov = lag0_prov.to(device)
    bsz = x_prov.shape[0]

    st_idx_tensor = torch.full((bsz,), station_idx, dtype=torch.long, device=device)
    st_emb = model.station_embedding(st_idx_tensor)  # (T, emb_dim)

    h_in = torch.cat([x_prov, st_emb], dim=-1)
    h_local = model.input_drop(model.input_act(model.input_norm(model.input_proj(h_in))))  # (T, D)

    # Khi chỉ đưa dữ liệu của 1 tỉnh vào, nhánh giá trị không gian hội tụ về v_proj(h_local)
    v_local = model.v_proj(h_local)
    gate_i = model.get_station_gates()[station_idx]
    h = ((1.0 - gate_i) * h_local + gate_i * v_local).unsqueeze(1)  # (T, 1, D)

    for block in model.blocks:
        h = block(h)

    delta_pred = model.head(h).squeeze(-1).squeeze(-1)  # (T,)
    anchor_pred = lag0_prov * model.anchor_weight[station_idx] + model.anchor_bias[station_idx]
    y_pred = anchor_pred + delta_pred
    return np.clip(y_pred.cpu().numpy(), 0.0, None)


def build_single_province_feature_block(
    combined_df: pd.DataFrame,
    target_times: pd.Series,
    prov_meta: pd.Series,
    station_meta: pd.DataFrame,
    continuous_cols: List[str],
    all_feature_cols: List[str],
    scaler_mean: np.ndarray,
    scaler_scale: np.ndarray,
) -> Tuple[torch.Tensor, torch.Tensor, pd.DataFrame]:
    """
    Tính toán đúng 49 đặc trưng đầu vào của `train_global_ann.py` cho các mốc `target_times`
    dựa trên chuỗi kết hợp (Lịch sử quan trắc + Dự báo thời tiết của đúng 1 tỉnh).
    """
    g = combined_df.sort_values("datetime").reset_index(drop=True)

    lat_min, lat_max = station_meta["latitude"].min(), station_meta["latitude"].max()
    lon_min, lon_max = station_meta["longitude"].min(), station_meta["longitude"].max()

    out = pd.DataFrame(
        {
            "station_id": str(prov_meta["station_id"]),
            "station_code": str(prov_meta["station_code"]),
            "station_name": str(prov_meta["station_name"]),
            "province_id": str(prov_meta["province_id"]),
            "province_name": str(prov_meta["province_name"]),
            "latitude": float(prov_meta["latitude"]),
            "longitude": float(prov_meta["longitude"]),
            "origin_time": g["datetime"],
        }
    )

    # 1. Độ trễ PM2.5 và trung bình trượt tại thời điểm gốc t (origin_time)
    for lag in PM25_LAGS:
        out[f"pm25_lag{lag}"] = g["pm25"].shift(lag)
    for w in ROLL_WINDOWS:
        out[f"pm25_roll{w}"] = g["pm25"].rolling(w, min_periods=w).mean()

    out["pm25_diff_1h"] = out["pm25_lag0"] - out["pm25_lag1"]
    out["pm25_diff_24h"] = out["pm25_lag0"] - out["pm25_lag24"]
    out["pm25_std24"] = g["pm25"].rolling(24, min_periods=24).std().fillna(0.0)

    out["pm10_lag0"] = g["pm10"]
    out["aqi_lag0"] = g["aqi"]

    # 2. Thời điểm mục tiêu cần dự báo (t + 24h)
    out["target_time"] = g["datetime"].shift(-HORIZON)

    # 3. 8 biến thời tiết dự báo tại thời điểm mục tiêu (t + 24h)
    for col in WEATHER_COLS:
        out[col] = g[col].shift(-HORIZON)

    # 4. 8 đặc trưng Nhiệt động lực học & Biến thiên 24h
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

    # 5. Đặc trưng chu kỳ thời gian của thời điểm mục tiêu (target_time)
    out["hour"] = out["target_time"].dt.hour
    out["day_of_week"] = out["target_time"].dt.dayofweek
    out["is_weekend"] = (out["day_of_week"] >= 5).astype(float)
    out["hour_sin"] = np.sin(2 * np.pi * out["hour"] / 24.0)
    out["hour_cos"] = np.cos(2 * np.pi * out["hour"] / 24.0)
    out["dow_sin"] = np.sin(2 * np.pi * out["day_of_week"] / 7.0)
    out["dow_cos"] = np.cos(2 * np.pi * out["day_of_week"] / 7.0)

    # 6. Tọa độ không gian chuẩn hóa
    out["latitude_norm"] = 2.0 * (out["latitude"] - lat_min) / max(lat_max - lat_min, 1e-6) - 1.0
    out["longitude_norm"] = 2.0 * (out["longitude"] - lon_min) / max(lon_max - lon_min, 1e-6) - 1.0

    # 7. Các cột One-Hot định danh station_id
    for st_id in station_meta["station_id"]:
        out[f"station_id_{st_id}"] = 1.0 if str(st_id) == str(prov_meta["station_id"]) else 0.0

    # Lọc đúng các dòng ứng với target_times cần dự báo
    sub = out[out["target_time"].isin(target_times)].sort_values("target_time").reset_index(drop=True)

    # Chuẩn hóa Z-score bằng đúng scaler của tập Train
    df_scaled = sub[all_feature_cols].copy()
    cont_vals = df_scaled[continuous_cols].to_numpy(dtype=np.float32)
    df_scaled[continuous_cols] = (cont_vals - scaler_mean) / np.where(scaler_scale == 0, 1.0, scaler_scale)

    x_tensor = torch.from_numpy(df_scaled.to_numpy(dtype=np.float32))
    lag0_tensor = torch.from_numpy(sub["pm25_lag0"].to_numpy(dtype=np.float32))
    return x_tensor, lag0_tensor, sub


def run_forecast_for_province(
    province_query: str,
    forecast_csv: Path,
    checkpoint_path: Path,
    output_dir: Path,
    device_arg: str = "auto",
) -> Path:
    device = resolve_device(device_arg)
    model, ckpt = load_trained_ann_checkpoint(checkpoint_path, device)

    station_meta = pd.DataFrame(ckpt["station_meta"]).sort_values("station_idx").reset_index(drop=True)
    prov_meta = resolve_target_province(province_query, station_meta)
    province_name = str(prov_meta["province_name"])
    station_code = str(prov_meta["station_code"])
    station_idx = int(prov_meta["station_idx"])

    continuous_cols: List[str] = ckpt["continuous_cols"]
    all_feature_cols: List[str] = ckpt["all_feature_cols"]
    scaler_mean = np.array(ckpt["scaler_mean"], dtype=np.float32)
    scaler_scale = np.array(ckpt["scaler_scale"], dtype=np.float32)

    print("=" * 75)
    print(f"DỰ BÁO NỒNG ĐỘ BỤI MỊN PM2.5 BẰNG MÔ HÌNH GLOBAL ANN CHO TỈNH: {province_name.upper()}")
    print("=" * 75)
    print(f" - Mã trạm/tỉnh       : {station_code} (station_id: {prov_meta['station_id']}, idx: {station_idx})")
    print(f" - Trọng số ANN       : {checkpoint_path} (Best Epoch {ckpt['best_epoch']}, Val RMSE = {ckpt['best_val_rmse']:.2f})")
    print(f" - File thời tiết 24h : {forecast_csv}")

    # 1. Đọc dữ liệu dự báo thời tiết 24h tới của ĐÚNG 1 TỈNH được chọn
    if not forecast_csv.exists():
        raise FileNotFoundError(f"Không tìm thấy file dự báo thời tiết: {forecast_csv}")

    fc_all = pd.read_csv(forecast_csv, dtype={"station_id": str, "station_code": str}, parse_dates=["datetime"])
    fc_prov = fc_all[fc_all["station_code"] == station_code].sort_values("datetime").reset_index(drop=True)
    if fc_prov.empty:
        raise ValueError(f"Không có dữ liệu của tỉnh {province_name} ({station_code}) trong {forecast_csv}")

    # 2. Đọc lịch sử quan trắc gần nhất của tỉnh đó (lấy 72 giờ cuối để tính lag 24h & rolling 24h)
    # Tối ưu hóa: Đọc từ Silver Parquet với Partition Pruning thay vì quét toàn bộ 154MB CSV
    hist_merged = None
    if SILVER_PROVINCE_DIR.exists() and SILVER_WEATHER_DIR.exists():
        try:
            print(f"[*] Nạp lịch sử quan trắc qua Silver Parquet Partition Pruning (tỉnh '{province_name}')...")
            prov_merged = load_merged(province_name=province_name, use_parquet=True)
            hist_merged = prov_merged.sort_values("datetime").tail(72).copy()
        except Exception as e:
            print(f"[!] Không thể đọc Silver Parquet ({e}), fallback sang CSV...")
            hist_merged = None

    if hist_merged is None:
        print(f"[*] Fallback nạp lịch sử quan trắc từ file CSV cho tỉnh '{province_name}'...")
        tedp_hist = pd.read_csv(
            TEDP_CSV,
            dtype={"station_id": str, "province_id": str, "station_code": str},
            parse_dates=["datetime"],
        )
        tedp_prov = tedp_hist[tedp_hist["station_code"] == station_code].sort_values("datetime").tail(72).copy()

        wea_hist = pd.read_csv(
            WEATHER_CSV,
            usecols=["station_code", "datetime"] + WEATHER_COLS,
            dtype={"station_code": str},
            parse_dates=["datetime"],
        )
        wea_prov = wea_hist[wea_hist["station_code"] == station_code].sort_values("datetime").tail(72).copy()

        hist_merged = tedp_prov.merge(wea_prov, on=["station_code", "datetime"], how="inner")

    # Tỷ lệ thực nghiệm pm10/pm25 và aqi/pm25 của tỉnh để ước lượng biến phụ khi rollout
    pm10_ratio = float((hist_merged["pm10"] / hist_merged["pm25"].clip(lower=1.0)).median())
    aqi_ratio = float((hist_merged["aqi"] / hist_merged["pm25"].clip(lower=1.0)).median())

    # Chuẩn bị lưới thời gian liên tục 1 giờ (freq='1h') nối từ lịch sử đến hết cửa sổ dự báo 24h
    fc_rows = fc_prov[["datetime"] + WEATHER_COLS].copy()
    fc_rows["pm25"] = np.nan
    fc_rows["pm10"] = np.nan
    fc_rows["aqi"] = np.nan

    hist_rows = hist_merged[["datetime", "pm25", "pm10", "aqi"] + WEATHER_COLS].copy()
    hist_max_dt = hist_rows["datetime"].max()

    # 2a. Bổ sung các giờ đo thật trong ngày lẻ từ silver/tedp_station (nếu có)
    partial_tedp = load_partial_day_station_tedp(province_name, hist_max_dt)

    # 2b. Bổ sung thời tiết thực tế 48h gần nhất từ file cầu nối openmeteo_weather_bridge_72h (nếu có)
    bridge_wea = pd.DataFrame(columns=["datetime"] + WEATHER_COLS)
    if BRIDGE_WEATHER_72H_CSV.exists():
        try:
            b_all = pd.read_csv(BRIDGE_WEATHER_72H_CSV, dtype={"station_code": str}, parse_dates=["datetime"])
            bridge_wea = b_all[b_all["station_code"] == station_code][["datetime"] + WEATHER_COLS].copy()
        except Exception:
            pass

    if not partial_tedp.empty or not bridge_wea.empty:
        extra_rows = pd.merge(partial_tedp, bridge_wea, on="datetime", how="outer")
        extra_rows = extra_rows[
            (extra_rows["datetime"] > hist_max_dt)
            & (extra_rows["datetime"] < fc_prov["datetime"].min())
        ].copy()
        if not extra_rows.empty:
            print(f"[*] Bổ sung {len(extra_rows)} giờ dữ liệu lẻ gần nhất ({extra_rows['datetime'].min()} -> {extra_rows['datetime'].max()})...")
            hist_rows = pd.concat([hist_rows, extra_rows], ignore_index=True)

    raw_combined = (
        pd.concat([hist_rows, fc_rows], ignore_index=True)
        .drop_duplicates(subset=["datetime"], keep="last")
        .sort_values("datetime")
        .reset_index(drop=True)
    )

    full_hourly_idx = pd.date_range(
        start=raw_combined["datetime"].min(),
        end=raw_combined["datetime"].max(),
        freq="1h",
    )
    combined = (
        raw_combined.set_index("datetime")
        .reindex(full_hourly_idx)
        .rename_axis("datetime")
        .reset_index()
    )

    # 2c. Cơ chế Tự phục hồi Đa tầng (Multi-Pass Diurnal Bridge + Autoregressive Rollout)
    # Đảm bảo nếu giữa cuối lịch sử và đầu dự báo 24h có khoảng trống (dù <= 24h hay > 24h),
    # toàn bộ chuỗi thời tiết và nền PM2.5 trước fc_min_time đều được lấp kín 100% (0 NaN).
    fc_min_time = fc_prov["datetime"].min()

    # Bước 1: Điền kín 8 biến thời tiết trước fc_min_time bằng vòng lặp chu kỳ 24h (Multi-pass shift(24))
    for col in WEATHER_COLS:
        passes = 0
        while passes < 30:
            pre_fc_mask = (combined["datetime"] < fc_min_time) & (combined[col].isna())
            if not pre_fc_mask.any():
                break
            shifted = combined[col].shift(24)
            if shifted.loc[pre_fc_mask].isna().all():
                combined[col] = combined[col].ffill().bfill()
                break
            combined.loc[pre_fc_mask, col] = shifted.loc[pre_fc_mask]
            passes += 1

    # Bước 2: Điền kín pm25, pm10, aqi trước fc_min_time theo từng chặng 24h bằng mô hình Global ANN
    # (kết hợp mượn chu kỳ 24h làm neo an toàn để tuyệt đối không bao giờ phát sinh NaN)
    gap_pm25_mask = (combined["datetime"] < fc_min_time) & (combined["pm25"].isna())
    if gap_pm25_mask.any():
        gap_times = combined.loc[gap_pm25_mask, "datetime"].sort_values().reset_index(drop=True)
        print(
            f"[*] Phát hiện {len(gap_times)} giờ thiếu PM2.5 trước giờ dự báo ({gap_times.min()} -> {gap_times.max()}). "
            f"Đang chạy cơ chế tự phục hồi cuốn chiếu (Multi-Pass Bridge)..."
        )
        # Lấp nền sơ bộ bằng multi-pass shift(24) cho pm25, pm10, aqi
        for col in ["pm25", "pm10", "aqi"]:
            passes = 0
            while passes < 30:
                m = (combined["datetime"] < fc_min_time) & (combined[col].isna())
                if not m.any():
                    break
                shifted = combined[col].shift(24)
                if shifted.loc[m].isna().all():
                    combined[col] = combined[col].ffill().bfill()
                    break
                combined.loc[m, col] = shifted.loc[m]
                passes += 1

        # Tinh chỉnh các giờ gap bằng chính mô hình Global ANN theo từng chặng 24h
        for chunk_start in range(0, len(gap_times), 24):
            chunk_targets = gap_times.iloc[chunk_start : chunk_start + 24]
            try:
                x_gap, lag0_gap, sub_gap = build_single_province_feature_block(
                    combined_df=combined,
                    target_times=chunk_targets,
                    prov_meta=prov_meta,
                    station_meta=station_meta,
                    continuous_cols=continuous_cols,
                    all_feature_cols=all_feature_cols,
                    scaler_mean=scaler_mean,
                    scaler_scale=scaler_scale,
                )
                if not sub_gap.empty and not torch.isnan(x_gap).any():
                    preds_gap = predict_single_province_ann(model, x_gap, lag0_gap, station_idx, device)
                    for t_val, p_val in zip(sub_gap["target_time"], preds_gap):
                        if not np.isnan(p_val):
                            mask = combined["datetime"] == t_val
                            combined.loc[mask, "pm25"] = float(p_val)
                            combined.loc[mask, "pm10"] = float(p_val * pm10_ratio)
                            combined.loc[mask, "aqi"] = float(p_val * aqi_ratio)
            except Exception:
                pass

    fc_timestamps = fc_prov["datetime"].sort_values().reset_index(drop=True)
    day1_times = fc_timestamps.iloc[:24]   # 24 mốc giờ đầu tiên trong file next_24h
    day2_times = fc_timestamps.iloc[24:]   # Mốc giờ thứ 25 (t+24h) nếu có

    # 3. Dự báo 24 mốc giờ đầu tiên
    x_day1, lag0_day1, sub_day1 = build_single_province_feature_block(
        combined_df=combined,
        target_times=day1_times,
        prov_meta=prov_meta,
        station_meta=station_meta,
        continuous_cols=continuous_cols,
        all_feature_cols=all_feature_cols,
        scaler_mean=scaler_mean,
        scaler_scale=scaler_scale,
    )
    preds_day1 = predict_single_province_ann(model, x_day1, lag0_day1, station_idx, device)
    sub_day1["pm25_pred_ann"] = np.round(preds_day1, 2)
    sub_day1["pm25_baseline_24h"] = np.round(sub_day1["pm25_lag0"], 2)
    sub_day1["forecast_mode"] = "Next_24h_Forecast"

    for t_val, p_val in zip(sub_day1["target_time"], preds_day1):
        mask = combined["datetime"] == t_val
        combined.loc[mask, "pm25"] = float(p_val)
        combined.loc[mask, "pm10"] = float(p_val * pm10_ratio)
        combined.loc[mask, "aqi"] = float(p_val * aqi_ratio)

    results_parts = [sub_day1]

    # 4. Dự báo mốc giờ cuối (nếu file next_24h có 25 mốc từ t+0h đến t+24h)
    if not day2_times.empty:
        x_day2, lag0_day2, sub_day2 = build_single_province_feature_block(
            combined_df=combined,
            target_times=day2_times,
            prov_meta=prov_meta,
            station_meta=station_meta,
            continuous_cols=continuous_cols,
            all_feature_cols=all_feature_cols,
            scaler_mean=scaler_mean,
            scaler_scale=scaler_scale,
        )
        preds_day2 = predict_single_province_ann(model, x_day2, lag0_day2, station_idx, device)
        sub_day2["pm25_pred_ann"] = np.round(preds_day2, 2)
        sub_day2["pm25_baseline_24h"] = np.round(sub_day2["pm25_lag0"], 2)
        sub_day2["forecast_mode"] = "Next_24h_Forecast"
        results_parts.append(sub_day2)

    final_sub = pd.concat(results_parts, ignore_index=True).sort_values("target_time").reset_index(drop=True)
    final_sub["horizon_hour"] = np.arange(1, len(final_sub) + 1)
    final_sub["air_quality_level"] = final_sub["pm25_pred_ann"].apply(classify_pm25_level)

    # Chọn các cột rõ ràng, đầy đủ thông tin để xuất ra file CSV của tỉnh
    export_cols = [
        "station_id",
        "station_code",
        "province_name",
        "latitude",
        "longitude",
        "origin_time",
        "target_time",
        "horizon_hour",
        "forecast_mode",
        "pm25_pred_ann",
        "pm25_baseline_24h",
        "air_quality_level",
    ] + WEATHER_COLS + ["dew_point_deficit", "ventilation_coef", "precip_sum_24h"]

    out_df = final_sub[export_cols].rename(columns={"target_time": "forecast_datetime"}).copy()
    for col in ["pm25_pred_ann", "pm25_baseline_24h"] + WEATHER_COLS + ["dew_point_deficit", "ventilation_coef", "precip_sum_24h"]:
        out_df[col] = out_df[col].astype(np.float64).round(2)

    output_dir.mkdir(parents=True, exist_ok=True)
    prov_filename = f"{slugify(province_name)}.csv"
    out_path = output_dir / prov_filename
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"\n[THÀNH CÔNG] Đã dự báo {len(out_df)} mốc giờ cho tỉnh {province_name}!")
    print(f" -> File kết quả đã lưu tại: {out_path}")
    print(f" -> PM2.5 dự báo trung bình: {out_df['pm25_pred_ann'].mean():.2f} µg/m³ "
          f"(Min: {out_df['pm25_pred_ann'].min():.2f}, Max: {out_df['pm25_pred_ann'].max():.2f})")
    print("\n12 mốc giờ dự báo đầu tiên:")
    preview_cols = [
        "forecast_datetime",
        "horizon_hour",
        "forecast_mode",
        "pm25_pred_ann",
        "pm25_baseline_24h",
        "temperature_2m",
        "boundary_layer_height",
        "precipitation",
        "air_quality_level",
    ]
    print(out_df[preview_cols].head(12).to_string(index=False))
    print("=" * 75)
    return out_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dự báo PM2.5 cho 1 tỉnh từ dữ liệu thời tiết Open-Meteo 48h bằng mô hình Global ANN."
    )
    parser.add_argument(
        "--province",
        type=str,
        default="Hà Nội",
        help="Tên tỉnh cần dự báo (mặc định: 'Hà Nội', ví dụ: 'HCM', 'Đà Nẵng', 'Bắc Ninh'...).",
    )
    parser.add_argument(
        "--forecast-csv",
        type=Path,
        default=FORECAST_WEATHER_CSV,
        help="Đường dẫn tới file dự báo thời tiết 48h.",
    )
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=GLOBAL_ANN_DIR / "best_ann_model.pt",
        help="Đường dẫn tới file trọng số mô hình Global ANN (best_ann_model.pt).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DU_BAO_DIR,
        help="Thư mục lưu file kết quả dự báo (mặc định: data_test/du_bao).",
    )
    parser.add_argument(
        "--device",
        type=str,
        default="auto",
        choices=["auto", "cuda", "cpu"],
        help="Thiết bị chạy suy luận PyTorch.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_forecast_for_province(
        province_query=args.province,
        forecast_csv=args.forecast_csv,
        checkpoint_path=args.checkpoint,
        output_dir=args.output_dir,
        device_arg=args.device,
    )
