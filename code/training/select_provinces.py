"""
Lọc các tỉnh đủ ngưỡng số dòng (MIN_ROWS = 1000 trong common.py) và in danh sách.

Dùng để kiểm tra tỉnh nào được train. Mỗi tỉnh đủ ngưỡng có một file train_<tinh>.py riêng.
Nếu có tỉnh đủ ngưỡng nhưng CHƯA có file train tương ứng, script sẽ cảnh báo.

Chạy:
    python code/training/select_provinces.py
"""

from pathlib import Path

from common import MIN_ROWS, eligible_provinces, load_merged, province_row_counts, slugify

TRAINING_DIR = Path(__file__).resolve().parent


def main():
    df = load_merged()
    counts = province_row_counts(df)
    ok = eligible_provinces(df)

    print(f"Ngưỡng: >= {MIN_ROWS} dòng/tỉnh\n")
    print(f"{'Tỉnh':<14}{'Số trạm':>8}{'Số dòng':>9}  Trạng thái")
    for prov, r in counts.iterrows():
        passed = prov in ok.index
        script = TRAINING_DIR / f"train_{slugify(prov)}.py"
        if passed:
            note = f"ĐẠT  -> {script.name}" + ("" if script.exists() else "  [THIẾU FILE TRAIN!]")
        else:
            note = "loại (không đủ dòng)"
        print(f"{prov:<14}{int(r['stations']):>8}{int(r['rows']):>9}  {note}")

    print(f"\nSố tỉnh đạt ngưỡng: {len(ok)}/{len(counts)}")


if __name__ == "__main__":
    main()
