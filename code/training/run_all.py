"""
Chạy lần lượt tất cả file train_<tinh>.py và in bảng so sánh kết quả giữa các tỉnh.

Mỗi tỉnh chạy trong một tiến trình riêng để lỗi của tỉnh này không làm hỏng tỉnh khác.

Chạy:
    python code/training/run_all.py
"""

import json
import argparse
import subprocess
import sys
from pathlib import Path

from common import OUTPUT_DIR, slugify

TRAINING_DIR = Path(__file__).resolve().parent
PROVINCES = [
    "HCM",
    "Hưng Yên",
    "Hà Nội",
    "Đà Nẵng",
    "Bắc Ninh",
    "Lâm Đồng",
    "Ninh Bình",
    "Quảng Ngãi",
    "Tây Ninh",
]
DEFAULT_MODELS = ["lightgbm"]


def main(model_names):
    for prov in PROVINCES:
        script = TRAINING_DIR / f"train_{slugify(prov)}.py"
        print(f"\n>>> Chạy {script.name}")
        subprocess.run([sys.executable, str(script), "--models", ",".join(model_names)], check=False)

    print("\n" + "=" * 78)
    print("BẢNG TỔNG HỢP (tập test)")
    print("=" * 78)
    print(f"{'Tỉnh':<12}{'Train/Val/Test':>16}" + "".join(f"{name + ' RMSE':>18}" for name in model_names))
    for prov in PROVINCES:
        path = OUTPUT_DIR / slugify(prov) / "metrics.json"
        if not path.exists():
            print(f"{prov:<12}  (chưa có kết quả)")
            continue
        m = json.loads(path.read_text(encoding="utf-8"))
        split = f"{m['n_train']}/{m['n_val']}/{m['n_test']}"
        scores = "".join(
            f"{m['results'][name]['test']['rmse']:>9.2f}"
            f" ({m['results'][name]['rmse_improvement_pct']:+.1f}%)"
            for name in model_names
        )
        print(f"{prov:<12}{split:>16}{scores}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train one or more province models.")
    parser.add_argument(
        "--models",
        default=",".join(DEFAULT_MODELS),
        help="Comma-separated models: linear_regression, svr, random_forest, xgboost, lightgbm",
    )
    args = parser.parse_args()
    from common import normalize_model_names

    main(normalize_model_names(args.models))
