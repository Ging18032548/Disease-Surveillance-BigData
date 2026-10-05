from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.cleaners import (
    convert_year_to_be,
    required_column,
    to_numeric,
)
from spark.config import Settings
from spark.schemas import POPULATION_COLUMN_ALIASES
from spark.standardizers import standardize_district


def canonicalize_population_data(
    dataframe: DataFrame,
    settings: Settings,
) -> DataFrame:
    """แปลงไฟล์ประชากรให้เหลือ year_be, district_name, population"""

    year_column = required_column(
        dataframe,
        POPULATION_COLUMN_ALIASES["year"],
        "population year",
    )

    district_column = required_column(
        dataframe,
        POPULATION_COLUMN_ALIASES["district_name"],
        "population district",
    )

    population_column = required_column(
        dataframe,
        POPULATION_COLUMN_ALIASES["population"],
        "population",
    )

    canonical = dataframe.select(
        convert_year_to_be(F.col(year_column)).alias("year_be"),
        standardize_district(
            F.col(district_column)
        ).alias("district_name"),
        to_numeric(F.col(population_column)).alias("population"),
    )

    return (
        canonical
        .filter(
            F.col("year_be").between(
                settings.minimum_year_be,
                settings.maximum_year_be,
            )
        )
        .filter(F.col("district_name").isNotNull())
        .filter(F.length(F.trim(F.col("district_name"))) > 0)
        .filter(F.col("population").isNotNull())
        .filter(F.col("population") > 0)
        .filter(~F.col("district_name").rlike(r"^(รวม|ทั้งหมด|total)"))
        .dropDuplicates(["year_be", "district_name", "population"])
        .groupBy("year_be", "district_name")
        .agg(F.sum("population").alias("population"))
    )


def fill_missing_years(
    population: DataFrame,
    disease: DataFrame,
) -> DataFrame:
    """หากปีใดไม่มีข้อมูลประชากร ให้ใช้ปีล่าสุดที่มีแทน"""

    required_years = disease.select("year_be").distinct()
    available_years = population.select("year_be").distinct()

    missing_years = required_years.join(
        available_years, on="year_be", how="left_anti"
    )

    if missing_years.count() == 0:
        return population

    latest_year = (
        population
        .agg(F.max("year_be").alias("latest"))
        .collect()[0]["latest"]
    )

    fallback = (
        population
        .filter(F.col("year_be") == latest_year)
        .drop("year_be")
        .crossJoin(missing_years)
        .select("year_be", "district_name", "population")
    )

    return population.unionByName(fallback)