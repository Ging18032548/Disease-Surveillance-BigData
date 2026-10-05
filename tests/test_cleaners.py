from pyspark.sql import functions as F

from spark.cleaners import (
    clean_disease_data,
    convert_year_to_be,
    to_numeric,
)


def test_to_numeric_handles_thousand_separator(spark):
    data = spark.createDataFrame([("1,234",), ("abc",)], ["raw"])

    results = data.select(
        to_numeric(F.col("raw")).alias("value")
    ).collect()

    assert results[0]["value"] == 1234.0
    assert results[1]["value"] is None


def test_convert_year_to_be(spark):
    data = spark.createDataFrame([(2025,), (2568,)], ["year"])

    results = data.select(
        convert_year_to_be(F.col("year")).alias("value")
    ).collect()

    assert [row["value"] for row in results] == [2568, 2568]


def test_clean_removes_invalid_rows(spark, settings):
    data = spark.createDataFrame(
        [
            (2568, "ไข้เลือดออก", "บางกะปิ", 1.0, 30.0),
            (2568, None, "บางกะปิ", 1.0, 30.0),
            (2568, "ไข้เลือดออก", "บางกะปิ", 0.0, 30.0),
            (2568, "ไข้เลือดออก", "บางกะปิ", 1.0, 500.0),
        ],
        [
            "year_be",
            "disease_name_raw",
            "district_name_raw",
            "case_count",
            "age",
        ],
    )

    assert clean_disease_data(data, settings).count() == 1