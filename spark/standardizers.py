import re
from typing import Dict

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from spark.schemas import (
    DISEASE_ALIASES,
    DISTRICT_ALIASES,
    SEX_ALIASES,
)


def _mapping_expression(values: Dict[str, str]) -> Column:
    """สร้าง Spark map literal จาก dict ของ Python"""

    pairs = []

    for key, value in values.items():
        pairs.extend([F.lit(key), F.lit(value)])

    return F.create_map(*pairs)


def _normalize_key(column: Column) -> Column:
    """ลดรูปข้อความเพื่อใช้เป็น key ในการเทียบ alias"""

    value = F.lower(F.trim(column.cast("string")))

    return F.regexp_replace(value, r"[\s_./\-]+", "")


def standardize_district(column: Column) -> Column:
    """ตัดคำนำหน้าเขต ตัดท้าย กทม. และแมปชื่อที่สะกดต่างกัน"""

    value = F.trim(column.cast("string"))
    value = F.regexp_replace(value, r"\s+", " ")
    value = F.regexp_replace(value, r"^(เขต|ข\.?)\s*", "")
    value = F.regexp_replace(
        value,
        r"\s*(กรุงเทพมหานคร|กรุงเทพฯ|กทม\.?)$",
        "",
    )
    value = F.trim(value)

    mapping = _mapping_expression(DISTRICT_ALIASES)

    return F.coalesce(F.element_at(mapping, value), value)


def standardize_disease(column: Column) -> Column:
    """แมปชื่อโรคทั้งไทยและอังกฤษให้เป็นชื่อมาตรฐานเดียว"""

    original = F.trim(column.cast("string"))
    key = _normalize_key(column)

    normalized_aliases = {
        re.sub(r"[\s_./\-]+", "", alias.lower()): value
        for alias, value in DISEASE_ALIASES.items()
    }

    mapping = _mapping_expression(normalized_aliases)

    return F.coalesce(F.element_at(mapping, key), original)


def standardize_sex(column: Column) -> Column:
    """แปลงเพศให้เหลือ M, F หรือ U"""

    key = _normalize_key(column)
    mapping = _mapping_expression(SEX_ALIASES)

    return F.coalesce(F.element_at(mapping, key), F.lit("U"))


def standardize_disease_data(dataframe: DataFrame) -> DataFrame:
    """สร้างคอลัมน์มาตรฐานจากคอลัมน์ raw"""

    return (
        dataframe
        .withColumn(
            "district_name",
            standardize_district(F.col("district_name_raw")),
        )
        .withColumn(
            "disease_name",
            standardize_disease(F.col("disease_name_raw")),
        )
        .withColumn("sex", standardize_sex(F.col("sex")))
        .filter(F.length(F.trim(F.col("district_name"))) > 0)
        .filter(F.length(F.trim(F.col("disease_name"))) > 0)
    )