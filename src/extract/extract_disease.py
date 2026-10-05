import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv


# ============================================================
# Configuration
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "disease"
RAW_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv(PROJECT_ROOT / ".env")

TOKEN = os.getenv("DATA_GO_TH_TOKEN")

if not TOKEN:
    raise ValueError(
        "ไม่พบ DATA_GO_TH_TOKEN กรุณาตรวจสอบไฟล์ .env"
    )


BASE_URL = "https://opend.data.go.th/get-ckan/datastore_search"

RESOURCES = {
    "2568": "ae882ad6-e057-4419-b3f7-1ffdbcfb6593",
    "2569": "b59f2279-ced4-4c41-8452-e1c2cf60b2f1",
}

# จำกัดจำนวนข้อมูลเพื่อการพัฒนาและทดสอบ
SAMPLE_LIMIT = 100

HEADERS = {
    "api-key": TOKEN
}


# ============================================================
# Extract Sample Data
# ============================================================

def extract_disease_sample(year, resource_id):

    print("\n" + "=" * 60)
    print(f"Extracting Disease Sample ปี {year}")
    print("=" * 60)

    params = {
        "resource_id": resource_id,
        "limit": SAMPLE_LIMIT,
        "offset": 0
    }

    try:
        response = requests.get(
            BASE_URL,
            params=params,
            headers=HEADERS,
            timeout=30
        )

        print(f"HTTP Status: {response.status_code}")

        response.raise_for_status()

        data = response.json()

        if not data.get("success"):
            print("API returned success=False")
            return False

        result = data.get("result", {})

        total = result.get("total", 0)
        records = result.get("records", [])

        print(f"Total available : {total:,}")
        print(f"Requested       : {SAMPLE_LIMIT:,}")
        print(f"Received        : {len(records):,}")

        # ----------------------------------------------------
        # Save Raw JSON
        # ----------------------------------------------------

        output_file = (
            RAW_DIR /
            f"disease_cases_{year}_sample.json"
        )

        with open(
            output_file,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                records,
                file,
                ensure_ascii=False,
                indent=2
            )

        print(f"Saved           : {output_file}")
        print("Status          : SUCCESS")

        return True

    except requests.exceptions.RequestException as error:

        print(f"Request Error: {error}")
        return False

    except ValueError as error:

        print(f"JSON Error: {error}")
        return False


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    print("=" * 60)
    print("Disease Surveillance - Ethical Sample Ingestion")
    print("=" * 60)

    results = {}

    for year, resource_id in RESOURCES.items():

        results[year] = extract_disease_sample(
            year,
            resource_id
        )

    print("\n" + "=" * 60)
    print("Extraction Summary")
    print("=" * 60)

    for year, success in results.items():

        status = "SUCCESS" if success else "FAILED"

        print(f"{year}: {status}")