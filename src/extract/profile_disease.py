from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "disease"

FILES = {
    "2568": RAW_DIR / "disease_cases_2568_sample.json",
    "2569": RAW_DIR / "disease_cases_2569_sample.json",
}


def profile_data(year, file_path):

    print("\n" + "=" * 65)
    print(f"Raw Data Profiling ปี {year}")
    print("=" * 65)

    if not file_path.exists():
        print(f"File not found: {file_path}")
        return

    df = pd.read_json(file_path)

    print(f"Rows             : {len(df):,}")
    print(f"Columns          : {len(df.columns)}")
    print(f"Missing values   : {df.isna().sum().sum():,}")
    print(f"Duplicate rows   : {df.duplicated().sum():,}")

    print("\nColumns:")
    for column in df.columns:
        print(f"  - {column}")

    # Disease
    if "ชื่อกลุ่มโรค" in df.columns:
        print("\nDisease Distribution:")
        print(
            df["ชื่อกลุ่มโรค"]
            .value_counts(dropna=False)
            .to_string()
        )

    # Gender
    if "เพศ" in df.columns:
        print("\nGender Distribution:")
        print(
            df["เพศ"]
            .value_counts(dropna=False)
            .to_string()
        )

    # District
    if "อำเภอ/เขต" in df.columns:
        print(
            f"\nUnique Districts : "
            f"{df['อำเภอ/เขต'].nunique()}"
        )

    # Subdistrict
    if "ตำบล/แขวง" in df.columns:
        print(
            f"Unique Subdistricts : "
            f"{df['ตำบล/แขวง'].nunique()}"
        )

    # Date range
    if "วันที่เริ่มป่วย" in df.columns:

        dates = pd.to_datetime(
            df["วันที่เริ่มป่วย"],
            errors="coerce"
        )

        print(f"\nEarliest Date    : {dates.min()}")
        print(f"Latest Date      : {dates.max()}")

    print("\nNOTE:")
    print(
        "ผลการ Profiling นี้มาจาก Sample Data "
        "จึงไม่ใช้สรุปสถิติของประชากรทั้งหมด"
    )


if __name__ == "__main__":

    print("=" * 65)
    print("Disease Surveillance - Raw Data Profiling")
    print("=" * 65)

    for year, file_path in FILES.items():
        profile_data(year, file_path)