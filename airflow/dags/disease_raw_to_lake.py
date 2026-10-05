"""Validate the extracted disease samples and land them in the S3-compatible raw zone."""

from __future__ import annotations

import csv
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator


YEARS = ("2568", "2569")
POPULATION_REFERENCE_DIR = Path("disease") / "reference"
POPULATION_FIELDS = ("ปี", "เขต", "ประชากรรวม")
EXPECTED_BANGKOK_DISTRICTS = {
    "พระนคร", "ดุสิต", "หนองจอก", "บางรัก", "บางเขน",
    "บางกะปิ", "ปทุมวัน", "ป้อมปราบศัตรูพ่าย", "พระโขนง",
    "มีนบุรี", "ลาดกระบัง", "ยานนาวา", "สัมพันธวงศ์",
    "พญาไท", "ธนบุรี", "บางกอกใหญ่", "ห้วยขวาง",
    "คลองสาน", "ตลิ่งชัน", "บางกอกน้อย", "บางขุนเทียน",
    "ภาษีเจริญ", "หนองแขม", "ราษฎร์บูรณะ", "บางพลัด",
    "ดินแดง", "บึงกุ่ม", "สาทร", "บางซื่อ", "จตุจักร",
    "บางคอแหลม", "ประเวศ", "คลองเตย", "สวนหลวง",
    "จอมทอง", "ดอนเมือง", "ราชเทวี", "ลาดพร้าว", "วัฒนา",
    "บางแค", "หลักสี่", "สายไหม", "คันนายาว", "สะพานสูง",
    "วังทองหลาง", "คลองสามวา", "บางนา", "ทวีวัฒนา",
    "ทุ่งครุ", "บางบอน",
}
EXPECTED_FIELDS = {
    "_id", "ชื่อกลุ่มโรค", "อายุ (เต็ม) ปี", "อายุ (เต็ม) เดือน", "อาย (เต็ม) วัน",
    "เพศ", "สถานภาพสมรส", "สัญชาติ", "อาชีพ", "จังหวัด", "อำเภอ/เขต",
    "ตำบล/แขวง", "วันที่เริ่มป่วย", "สภาพผู้ป่วย", "ประเภทผู้ป่วย", "สถานที่รักษา",
}


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
    )


def ensure_bucket():
    client = _s3_client()
    bucket = os.environ["DATA_LAKE_BUCKET"]
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        client.create_bucket(Bucket=bucket)


def land_sample(year: str):
    source = Path(os.environ["RAW_DATA_DIR"]) / "disease" / f"disease_cases_{year}_sample.json"
    if not source.is_file():
        raise FileNotFoundError(f"Required raw input is missing: {source.name}")

    payload = source.read_bytes()
    records = json.loads(payload.decode("utf-8"))
    if not isinstance(records, list) or len(records) != 100:
        raise ValueError(f"{source.name}: expected a JSON array of 100 records")
    if not records or set(records[0]) != EXPECTED_FIELDS:
        raise ValueError(f"{source.name}: source fields do not match the 16-field data contract")
    if any(set(record) != EXPECTED_FIELDS for record in records):
        raise ValueError(f"{source.name}: inconsistent fields between records")

    digest = hashlib.sha256(payload).hexdigest()
    bucket = os.environ["DATA_LAKE_BUCKET"]
    key = f"raw/disease/year={year}/{source.name}"
    client = _s3_client()

    try:
        existing = client.head_object(Bucket=bucket, Key=key)
        existing_digest = existing.get("Metadata", {}).get("sha256")
        if existing_digest != digest:
            raise ValueError(
                f"Raw object already exists with different content: s3://{bucket}/{key}. "
                "Use a new versioned filename/key to preserve raw history."
            )
        print(f"Already landed; checksum unchanged: s3://{bucket}/{key}")
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code not in {"404", "NoSuchKey", "NotFound"}:
            raise
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=payload,
            ContentType="application/json",
            Metadata={"sha256": digest, "record-count": str(len(records)), "source-year-be": year},
        )

    manifest = {
        "dataset": "disease_cases",
        "source": "Data.go.th Data API",
        "year_be": year,
        "filename": source.name,
        "object_key": key,
        "format": "json",
        "record_count": len(records),
        "size_bytes": len(payload),
        "sha256": digest,
        "landed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    client.put_object(
        Bucket=bucket,
        Key=f"raw/disease/year={year}/_metadata/{source.stem}.manifest.json",
        Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"Landed {len(records)} records: s3://{bucket}/{key}; sha256={digest}")


def _population_source(year: str) -> Path:
    return (
        Path(os.environ["RAW_DATA_DIR"])
        / POPULATION_REFERENCE_DIR
        / f"population_summary_{year}.csv"
    )


def _validate_population(year: str, payload: bytes, source: Path) -> int:
    try:
        rows = list(csv.DictReader(payload.decode("utf-8-sig").splitlines()))
    except (UnicodeDecodeError, csv.Error) as error:
        raise ValueError(f"{source.name}: invalid UTF-8 CSV: {error}") from error

    actual_fields = tuple(rows[0].keys()) if rows else ()
    if actual_fields != POPULATION_FIELDS:
        raise ValueError(
            f"{source.name}: expected columns {POPULATION_FIELDS}, "
            f"got {actual_fields}"
        )

    districts = []
    for row_number, row in enumerate(rows, start=2):
        if row["ปี"].strip() != year:
            raise ValueError(
                f"{source.name}:{row_number}: year must be {year}"
            )
        district = row["เขต"].strip()
        if not district:
            raise ValueError(f"{source.name}:{row_number}: district is empty")
        try:
            population = int(row["ประชากรรวม"].strip())
        except ValueError as error:
            raise ValueError(
                f"{source.name}:{row_number}: population must be an integer"
            ) from error
        if population <= 0:
            raise ValueError(
                f"{source.name}:{row_number}: population must be positive"
            )
        districts.append(district)

    duplicate_districts = sorted(
        district for district in set(districts) if districts.count(district) > 1
    )
    missing = sorted(EXPECTED_BANGKOK_DISTRICTS - set(districts))
    unexpected = sorted(set(districts) - EXPECTED_BANGKOK_DISTRICTS)
    if duplicate_districts or missing or unexpected:
        raise ValueError(
            f"{source.name}: population coverage is not exactly Bangkok's "
            f"50 districts; duplicates={duplicate_districts}, "
            f"missing={missing}, unexpected={unexpected}"
        )

    return len(rows)


def land_population(year: str):
    source = _population_source(year)
    if not source.is_file():
        raise FileNotFoundError(
            f"Required population reference is missing: {source}"
        )

    payload = source.read_bytes()
    record_count = _validate_population(year, payload, source)
    digest = hashlib.sha256(payload).hexdigest()
    bucket = os.environ["DATA_LAKE_BUCKET"]
    key = f"raw/population/year={year}/{source.name}"
    client = _s3_client()

    try:
        existing = client.head_object(Bucket=bucket, Key=key)
        existing_digest = existing.get("Metadata", {}).get("sha256")
        if existing_digest != digest:
            raise ValueError(
                f"Raw object already exists with different content: s3://{bucket}/{key}. "
                "Use a new versioned filename/key to preserve raw history."
            )
        print(f"Already landed; checksum unchanged: s3://{bucket}/{key}")
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code not in {"404", "NoSuchKey", "NotFound"}:
            raise
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=payload,
            ContentType="text/csv",
            Metadata={
                "sha256": digest,
                "record-count": str(record_count),
                "source-year-be": year,
                "coverage": "Bangkok-50-districts",
            },
        )

    manifest = {
        "dataset": "population_reference",
        "source": "Bangkok population reference CSV",
        "year_be": year,
        "filename": source.name,
        "object_key": key,
        "format": "csv",
        "record_count": record_count,
        "district_count": len(EXPECTED_BANGKOK_DISTRICTS),
        "coverage": "Bangkok-50-districts",
        "size_bytes": len(payload),
        "sha256": digest,
        "landed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    client.put_object(
        Bucket=bucket,
        Key=f"raw/population/year={year}/_metadata/{source.stem}.manifest.json",
        Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(
        f"Landed {record_count} population rows: "
        f"s3://{bucket}/{key}; sha256={digest}"
    )


with DAG(
    dag_id="disease_raw_to_lake",
    description="Validate existing Data.go.th disease samples and land immutable raw objects in S3-compatible storage",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 2},
    tags=["data-lake", "raw", "disease"],
) as dag:
    create_bucket = PythonOperator(task_id="ensure_raw_bucket", python_callable=ensure_bucket)
    for data_year in YEARS:
        upload = PythonOperator(
            task_id=f"land_disease_{data_year}",
            python_callable=land_sample,
            op_kwargs={"year": data_year},
        )
        create_bucket >> upload
        population_upload = PythonOperator(
            task_id=f"land_population_{data_year}",
            python_callable=land_population,
            op_kwargs={"year": data_year},
        )
        create_bucket >> population_upload

    trigger_spark = TriggerDagRunOperator(
        task_id="trigger_spark_processing",
        trigger_dag_id="spark_processing",
        wait_for_completion=False,
    )
    create_bucket >> trigger_spark
    for data_year in YEARS:
        trigger_spark.set_upstream(
            dag.task_dict[f"land_disease_{data_year}"]
        )
        trigger_spark.set_upstream(
            dag.task_dict[f"land_population_{data_year}"]
        )
