from typing import Iterable, Optional, cast

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from spark.config import Settings
from spark.schemas import DISEASE_COLUMN_ALIASES


# ---------------------------------------------------------
# Column utilities
# ---------------------------------------------------------

def normalize_column_key(value: str) -> str:
    import re

    return re.sub(r"\s+", "", str(value).strip().lower())


def find_column(
    dataframe: object,
    aliases: Iterable[str],
) -> Optional[str]:
    """หาชื่อคอลัมน์จริงจากรายการ alias"""

    available = {
        normalize_column_key(column): column
        for column in cast(Iterable[str], getattr(dataframe, "columns"))
    }

    for alias in aliases:
        matched = available.get(normalize_column_key(alias))

        if matched is not None:
            return matched

    return None


def required_column(
    dataframe: object,
    aliases: Iterable[str],
    logical_name: str,
) -> str:
    column = find_column(dataframe, aliases)

    if column is None:
        raise ValueError(
            f"ไม่พบคอลัมน์ {logical_name}; "
            f"คอลัมน์ที่มีคือ {getattr(dataframe, 'columns')}"
        )

    return column


# ---------------------------------------------------------
# Value utilities
# ---------------------------------------------------------

def to_numeric(column: Column) -> Column:
    """แปลงข้อความตัวเลขที่มีลูกน้ำหรือช่องว่างให้เป็น double"""

    value = F.trim(column.cast("string"))
    value = F.regexp_replace(value, ",", "")
    value = F.regexp_replace(value, "\u00a0", "")
    value = F.regexp_replace(value, r"\s+", "")

    return F.when(value.rlike(r"^-?\d+(\.\d+)?$"), value).cast("double")


def convert_year_to_be(column: Column) -> Column:
    """แปลง ค.ศ. เป็น พ.ศ. หากตรวจพบว่าเป็นปีคริสต์ศักราช"""

    year = to_numeric(column).cast("int")

    return (
        F.when(year.between(1900, 2200), year + F.lit(543))
        .otherwise(year)
    )


def parse_report_date(column: Column) -> Column:
    """รองรับทั้ง dd/MM/yyyy แบบ พ.ศ. และ ISO date"""

    text = F.trim(column.cast("string"))
    pattern = r"^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$"

    day_text = F.regexp_extract(text, pattern, 1)
    month_text = F.regexp_extract(text, pattern, 2)
    year_text = F.regexp_extract(text, pattern, 3)

    day = F.when(day_text != "", day_text.cast("int"))
    month = F.when(month_text != "", month_text.cast("int"))
    raw_year = F.when(year_text != "", year_text.cast("int"))

    christian_year = (
        F.when(raw_year > 2400, raw_year - F.lit(543))
        .otherwise(raw_year)
    )

    parsed_dmy = F.when(
        raw_year.isNotNull(),
        F.make_date(christian_year, month, day),
    )

    return F.coalesce(
        parsed_dmy,
        F.try_to_date(
            F.substring(text, 1, 10),
            "yyyy-MM-dd",
        ),
        F.try_to_date(
            F.substring(text, 1, 10),
            "yyyy/MM/dd",
        ),
    )


# ---------------------------------------------------------
# Canonicalize + Clean
# ---------------------------------------------------------

def canonicalize_disease_data(dataframe: DataFrame) -> DataFrame:
    """แปลงคอลัมน์ต้นทางให้ตรงกับ Canonical Schema"""

    disease_column = required_column(
        dataframe,
        DISEASE_COLUMN_ALIASES["disease_name"],
        "disease_name",
    )

    district_column = required_column(
        dataframe,
        DISEASE_COLUMN_ALIASES["district_name"],
        "district_name",
    )

    case_id_column = find_column(
        dataframe, DISEASE_COLUMN_ALIASES["case_id"]
    )
    date_column = find_column(
        dataframe, DISEASE_COLUMN_ALIASES["report_date"]
    )
    year_column = find_column(
        dataframe, DISEASE_COLUMN_ALIASES["year"]
    )
    count_column = find_column(
        dataframe, DISEASE_COLUMN_ALIASES["case_count"]
    )
    sex_column = find_column(
        dataframe, DISEASE_COLUMN_ALIASES["sex"]
    )
    age_column = find_column(
        dataframe, DISEASE_COLUMN_ALIASES["age"]
    )

    source_file = (
        F.col("_source_file")
        if "_source_file" in dataframe.columns
        else F.input_file_name()
    )

    file_year = F.regexp_extract(
        source_file, r"disease_cases_(\d{4})", 1
    ).cast("int")

    report_date = (
        parse_report_date(F.col(date_column))
        if date_column
        else F.lit(None).cast("date")
    )

    explicit_year = (
        convert_year_to_be(F.col(year_column))
        if year_column
        else F.lit(None).cast("int")
    )

    year_from_date = F.when(
        report_date.isNotNull(), F.year(report_date) + F.lit(543)
    )

    return dataframe.select(
        (
            F.col(case_id_column).cast("string")
            if case_id_column
            else F.lit(None).cast("string")
        ).alias("case_id"),
        report_date.alias("report_date"),
        F.coalesce(explicit_year, file_year, year_from_date)
        .cast("int")
        .alias("year_be"),
        F.trim(F.col(disease_column).cast("string"))
        .alias("disease_name_raw"),
        F.trim(F.col(district_column).cast("string"))
        .alias("district_name_raw"),
        (
            to_numeric(F.col(count_column))
            if count_column
            else F.lit(1.0)
        ).alias("case_count"),
        (
            F.trim(F.col(sex_column).cast("string"))
            if sex_column
            else F.lit(None).cast("string")
        ).alias("sex"),
        (
            to_numeric(F.col(age_column))
            if age_column
            else F.lit(None).cast("double")
        ).alias("age"),
        source_file.alias("source_file"),
    )


def clean_disease_data(
    dataframe: DataFrame,
    settings: Settings,
) -> DataFrame:
    """กรองแถวที่ไม่สมบูรณ์หรือมีค่าผิดปกติออก"""

    return (
        dataframe
        .filter(
            F.col("year_be").between(
                settings.minimum_year_be,
                settings.maximum_year_be,
            )
        )
        .filter(F.col("disease_name_raw").isNotNull())
        .filter(F.length(F.trim(F.col("disease_name_raw"))) > 0)
        .filter(F.col("district_name_raw").isNotNull())
        .filter(F.length(F.trim(F.col("district_name_raw"))) > 0)
        .filter(F.col("case_count").isNotNull())
        .filter(F.col("case_count") > 0)
        .filter(
            F.col("age").isNull()
            | F.col("age").between(0, settings.maximum_age)
        )
    )


def quarantine_disease_data(
    dataframe: DataFrame,
    settings: Settings,
) -> DataFrame:
    """เก็บแถวที่ไม่ผ่าน schema/type/range validation พร้อมสาเหตุ"""

    invalid_reason = (
        F.when(
            ~F.col("year_be").between(
                settings.minimum_year_be,
                settings.maximum_year_be,
            ),
            F.lit("invalid_year"),
        )
        .when(
            F.col("disease_name_raw").isNull()
            | (F.length(F.trim(F.col("disease_name_raw"))) == 0),
            F.lit("missing_disease_name"),
        )
        .when(
            F.col("district_name_raw").isNull()
            | (F.length(F.trim(F.col("district_name_raw"))) == 0),
            F.lit("missing_district_name"),
        )
        .when(
            F.col("case_count").isNull() | (F.col("case_count") <= 0),
            F.lit("invalid_case_count"),
        )
        .when(
            F.col("age").isNotNull()
            & ~F.col("age").between(0, settings.maximum_age),
            F.lit("invalid_age"),
        )
        .when(
            F.col("report_date").isNull(),
            F.lit("invalid_or_missing_report_date"),
        )
        .otherwise(F.lit(None).cast("string"))
    )

    return (
        dataframe
        .withColumn("_quarantine_reason", invalid_reason)
        .filter(F.col("_quarantine_reason").isNotNull())
    )