from typing import List, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BUSINESS_KEY_COLUMNS = [
    "year_be",
    "report_date",
    "disease_name_raw",
    "district_name_raw",
    "case_count",
    "sex",
    "age",
    "source_file",
]


def build_surrogate_key(columns: List[str]) -> Column:
    """สร้าง hash key จากคอลัมน์ธุรกิจสำหรับแถวที่ไม่มี case_id"""

    expressions = [
        F.coalesce(F.col(column).cast("string"), F.lit(""))
        for column in columns
    ]

    return F.sha2(F.concat_ws("||", *expressions), 256)


def assign_case_id(dataframe: DataFrame) -> DataFrame:
    """เติม case_id ให้แถวที่ไม่มีค่า"""

    surrogate = build_surrogate_key(BUSINESS_KEY_COLUMNS)

    return dataframe.withColumn(
        "case_id",
        F.when(
            F.col("case_id").isNull()
            | (F.length(F.trim(F.col("case_id"))) == 0),
            surrogate,
        ).otherwise(F.trim(F.col("case_id"))),
    )


def deduplicate_cases(
    dataframe: DataFrame,
) -> Tuple[DataFrame, int]:
    """ลบแถวซ้ำโดยเก็บรายการล่าสุดไว้ และคืนจำนวนที่ถูกตัด"""

    prepared = assign_case_id(dataframe)

    window = (
        Window
        .partitionBy("case_id")
        .orderBy(
            F.col("report_date").desc_nulls_last(),
            F.col("source_file").desc_nulls_last(),
        )
    )

    ranked = prepared.withColumn(
        "_row_number", F.row_number().over(window)
    )

    deduplicated = (
        ranked
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    removed_rows = prepared.count() - deduplicated.count()

    return deduplicated, removed_rows


def count_duplicate_keys(
    dataframe: DataFrame,
    key_columns: List[str],
) -> int:
    """นับจำนวนคีย์ที่ปรากฏมากกว่าหนึ่งครั้ง"""

    return (
        dataframe
        .groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )