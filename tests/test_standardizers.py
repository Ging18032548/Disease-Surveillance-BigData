from pyspark.sql import functions as F

from spark.standardizers import (
    standardize_disease,
    standardize_district,
    standardize_sex,
)


def test_standardize_district(spark):
    data = spark.createDataFrame(
        [("เขตบางกะปิ",), ("ข. ลาดพร้าว",), ("ป้อมปราบฯ กทม.",)],
        ["raw"],
    )

    results = data.select(
        standardize_district(F.col("raw")).alias("value")
    ).collect()

    assert [row["value"] for row in results] == [
        "บางกะปิ",
        "ลาดพร้าว",
        "ป้อมปราบศัตรูพ่าย",
    ]


def test_standardize_disease(spark):
    data = spark.createDataFrame(
        [("โรคไข้เลือดออก",), ("Dengue Fever",), ("COVID-19",)],
        ["raw"],
    )

    results = data.select(
        standardize_disease(F.col("raw")).alias("value")
    ).collect()

    assert [row["value"] for row in results] == [
        "ไข้เลือดออก",
        "ไข้เลือดออก",
        "โควิด-19",
    ]


def test_standardize_sex(spark):
    data = spark.createDataFrame(
        [("ชาย",), ("Female",), ("ไม่ระบุ",)], ["raw"]
    )

    results = data.select(
        standardize_sex(F.col("raw")).alias("value")
    ).collect()

    assert [row["value"] for row in results] == ["M", "F", "U"]