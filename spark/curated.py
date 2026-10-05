from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.schemas import GOLD_PRIMARY_KEY

CASES_PER_POPULATION = 100000


def aggregate_cases(dataframe: DataFrame) -> DataFrame:
    """รวมจำนวนผู้ป่วยตามปี เขต และโรค"""

    return (
        dataframe
        .groupBy(*GOLD_PRIMARY_KEY)
        .agg(
            F.sum("case_count").alias("total_cases"),
            F.count_distinct("case_id").alias("source_records"),
        )
    )


def join_population(
    cases: DataFrame,
    population: DataFrame,
) -> DataFrame:
    """Join ข้อมูลผู้ป่วยกับประชากรด้วยปีและชื่อเขต"""

    return cases.join(
        population,
        on=["year_be", "district_name"],
        how="left",
    )


def calculate_incidence_rate(dataframe: DataFrame) -> DataFrame:
    """คำนวณ Incidence Rate ต่อประชากร 100,000 คน"""

    return dataframe.withColumn(
        "incidence_rate_per_100k",
        F.when(
            F.col("population") > 0,
            F.round(
                F.col("total_cases")
                / F.col("population")
                * F.lit(CASES_PER_POPULATION),
                2,
            ),
        ).otherwise(F.lit(None).cast("double")),
    )


def build_curated_dataset(
    disease: DataFrame,
    population: DataFrame,
) -> DataFrame:
    """สร้างชุดข้อมูล Gold Layer ที่พร้อมใช้งานใน Dashboard"""

    aggregated = aggregate_cases(disease)
    joined = join_population(aggregated, population)
    with_rate = calculate_incidence_rate(joined)

    return (
        with_rate
        .withColumn("total_cases", F.round("total_cases").cast("long"))
        .withColumn("population", F.round("population").cast("long"))
        .withColumn("source_records", F.col("source_records").cast("long"))
        .select(
            "year_be",
            "district_name",
            "disease_name",
            "total_cases",
            "population",
            "incidence_rate_per_100k",
            "source_records",
        )
        .orderBy("year_be", "district_name", "disease_name")
    )