import os

import requests
from dotenv import load_dotenv


load_dotenv()

BASE_URL = "https://opend.data.go.th/get-ckan/datastore_search"

RESOURCES = {
    "2568": "ae882ad6-e057-4419-b3f7-1ffdbcfb6593",
    "2569": "b59f2279-ced4-4c41-8452-e1c2cf60b2f1",
}

TOKEN = os.getenv("DATA_GO_TH_TOKEN")


def get_token() -> str:
    if TOKEN is None:
        raise ValueError(
            "ไม่พบ DATA_GO_TH_TOKEN กรุณาตรวจสอบไฟล์ .env"
        )
    return TOKEN


def run_api_test(year, resource_id):
    print("=" * 60)
    print(f"Testing Disease API ปี {year}")
    print("=" * 60)

    params = {
        "resource_id": resource_id,
        "limit": 5,
    }

    headers = {"api-key": get_token()}

    try:
        response = requests.get(
            BASE_URL,
            params=params,
            headers=headers,
            timeout=30
        )

        print(f"HTTP Status: {response.status_code}")

        # ช่วย debug โดยไม่แสดง Token
        if response.status_code != 200:
            print("Response:")
            print(response.text[:500])

        response.raise_for_status()

        data = response.json()

        print(f"Success: {data.get('success')}")

        result = data.get("result", {})

        print(f"Total Records: {result.get('total')}")

        print("\nFields:")
        for field in result.get("fields", []):
            print(
                f"  - {field.get('id')} "
                f"({field.get('type')})"
            )

        records = result.get("records", [])

        print(f"\nReceived Records: {len(records)}")

        if records:
            print("\nFirst Record:")

            for key, value in records[0].items():
                print(f"  {key}: {value}")

    except requests.exceptions.RequestException as error:
        print(f"Request Error: {error}")

    except ValueError as error:
        print(f"JSON Error: {error}")

    print()


if __name__ == "__main__":
    for year, resource_id in RESOURCES.items():
        run_api_test(year, resource_id)