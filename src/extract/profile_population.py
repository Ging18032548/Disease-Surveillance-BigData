from pathlib import Path

import pandas as pd


# ============================================================
# Configuration
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

REFERENCE_DIR = PROJECT_ROOT / "data" / "raw" / "reference"

FILES = {
    "2568": REFERENCE_DIR / "ประชากรและครัวเรือน_2568.xlsx",
    "2569": REFERENCE_DIR / "ประชากรและครัวเรือน_2569.xlsx",
}

SHEET_NAME = "จำนวนประชากรและครัวเรือน"


# ============================================================
# Profile Population Data
# ============================================================

def profile_population(year, file_path):

    print("\n" + "=" * 65)
    print(f"Population Reference Profiling ปี {year}")
    print("=" * 65)

    if not file_path.exists():
        print(f"File not found: {file_path}")
        return

    try:
        # header=1 เพราะแถวแรกเป็นชื่อรายงาน
        # แถวที่สองจึงเป็นชื่อคอลัมน์จริง
        df = pd.read_excel(
            file_path,
            sheet_name=SHEET_NAME,
            header=1
        )

        print(f"Source File      : {file_path.name}")
        print(f"Rows (raw)       : {len(df):,}")
        print(f"Columns          : {len(df.columns)}")
        print(f"Missing values   : {df.isna().sum().sum():,}")
        print(f"Duplicate rows   : {df.duplicated().sum():,}")

        print("\nColumns:")
        for column in df.columns:
            print(f"  - {column}")

        print("\nRaw Data:")
        print(df.to_string(index=False))

        # ----------------------------------------------------
        # ตรวจเฉพาะแถวข้อมูลแขวง
        # ไม่แก้ไข Raw File
        # ----------------------------------------------------

        subdistrict_rows = df[
            df["แขวง"].isin(
                ["คลองกุ่ม", "นวมินทร์", "นวลจันทร์"]
            )
        ]

        print("\nSubdistrict Records:")
        print(subdistrict_rows.to_string(index=False))

        print(
            f"\nNumber of Subdistricts : "
            f"{len(subdistrict_rows)}"
        )

        if not subdistrict_rows.empty:

            population_sum = (
                subdistrict_rows["ประชากรรวม"].sum()
            )

            male_sum = (
                subdistrict_rows["ประชากรชาย"].sum()
            )

            female_sum = (
                subdistrict_rows["ประชากรหญิง"].sum()
            )

            household_sum = (
                subdistrict_rows["จำนวนครัวเรือน"].sum()
            )

            print(f"Households Total       : {household_sum:,.0f}")
            print(f"Male Population        : {male_sum:,.0f}")
            print(f"Female Population      : {female_sum:,.0f}")
            print(f"Population Total       : {population_sum:,.0f}")

        print("\nNOTE:")
        print(
            "ข้อมูลชุดนี้เป็นข้อมูลอ้างอิงเฉพาะสำนักงานเขตบึงกุ่ม "
            "ไม่ได้ครอบคลุมทั้งกรุงเทพมหานคร"
        )

    except Exception as error:
        print(f"Error: {error}")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    print("=" * 65)
    print("Population Reference - Raw Data Profiling")
    print("=" * 65)

    for year, file_path in FILES.items():
        profile_population(year, file_path)