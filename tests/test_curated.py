from spark.curated import build_curated_dataset


def test_incidence_rate_calculation(spark):
    disease = spark.createDataFrame(
        [
            (2568, "บางกะปิ", "ไข้เลือดออก", 1.0, "C1"),
            (2568, "บางกะปิ", "ไข้เลือดออก", 2.0, "C2"),
        ],
        [
            "year_be",
            "district_name",
            "disease_name",
            "case_count",
            "case_id",
        ],
    )

    population = spark.createDataFrame(
        [(2568, "บางกะปิ", 150000.0)],
        ["year_be", "district_name", "population"],
    )

    row = build_curated_dataset(disease, population).collect()[0]

    assert row["total_cases"] == 3
    assert row["population"] == 150000
    assert row["incidence_rate_per_100k"] == 2.0
    assert row["source_records"] == 2


def test_missing_population_gives_null_rate(spark):
    disease = spark.createDataFrame(
        [(2569, "ทุ่งครุ", "ไข้หวัดใหญ่", 5.0, "C3")],
        [
            "year_be",
            "district_name",
            "disease_name",
            "case_count",
            "case_id",
        ],
    )

    population = spark.createDataFrame(
        [(2568, "บางกะปิ", 150000.0)],
        ["year_be", "district_name", "population"],
    )

    row = build_curated_dataset(disease, population).collect()[0]

    assert row["population"] is None
    assert row["incidence_rate_per_100k"] is None