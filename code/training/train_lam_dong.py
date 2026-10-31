"""
Train mô hình dự báo PM2.5 (trước 24 giờ) cho tỉnh: Lâm Đồng

Chạy:
    python code/training/train_lam_dong.py

Kết quả lưu tại: output/province_models/lam_dong/
    model.txt, metrics.json, predictions.csv, feature_importance.csv
Toàn bộ logic nằm trong common.py (dùng chung cho mọi tỉnh).
"""

import argparse

from common import normalize_model_names, run_province

PROVINCE = "Lâm Đồng"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", default="lightgbm")
    args = parser.parse_args()
    run_province(PROVINCE, normalize_model_names(args.models))

