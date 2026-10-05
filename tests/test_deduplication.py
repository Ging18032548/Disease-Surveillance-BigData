from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from spark.deduplication import count_duplicate_keys, deduplicate_cases


def test_deduplicate_by_case_id(spark):
    schema = StructType([
        StructField("case_id", StringType(), True),
        StructField("year_be", IntegerType(), True),
        StructField("report_date", StringType(), True),  # ระบุ Type ให้ชัดเจน
        StructField("disease_name_raw", StringType(), True),
        StructField("district_name_raw", StringType(), True),
        StructField("case_count", DoubleType(), True),
        StructField("sex", StringType(), True),
        StructField("age", DoubleType(), True),
        StructField("source_file", StringType(), True),
    ])

    rows = [
        ("C1", 2568, None, "ไข้เลือดออก", "บางกะปิ", 1.0, "M", 30.0, "f1"),
        ("C1", 2568, None, "ไข้เลือดออก", "บางกะปิ", 1.0, "M", 30.0, "f1"),
        ("C2", 2568, None, "ไข้หวัดใหญ่", "ดินแดง", 1.0, "F", 25.0, "f1"),
    ]

    data = spark.createDataFrame(rows, schema=schema)

    result, removed = deduplicate_cases(data)

    assert result.count() == 2
    assert removed == 1
    assert count_duplicate_keys(result, ["case_id"]) == 0