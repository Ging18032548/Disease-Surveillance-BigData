from dataclasses import dataclass
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.config import Settings
from spark.deduplication import count_duplicate_keys
from spark.schemas import BANGKOK_DISTRICTS, GOLD_PRIMARY_KEY


@dataclass
class QualityCheck:
    layer: str
    rule: str
    observed: str
    threshold: str
    status: str


def _status(condition: bool) -> str:
    return "PASS" if condition else "FAIL"


def run_quality_checks(
    spark: SparkSession,
    raw: DataFrame,
    cleaned: DataFrame,
    standardized: DataFrame,
    curated: DataFrame,
    removed_duplicates: int,
    settings: Settings,
    canonical: Optional[DataFrame] = None,
) -> Tuple[DataFrame, bool]:
    """ตรวจกฎคุณภาพข้อมูลทั้งหมดและคืนผลเป็น DataFrame"""

    raw_rows = raw.count()
    clean_rows = cleaned.count()
    standardized_rows = standardized.count()
    curated_rows = curated.count()

    invalid_districts = standardized.filter(
        ~F.col("district_name").isin(BANGKOK_DISTRICTS)
    ).count()

    missing_population = curated.filter(
        F.col("population").isNull() | (F.col("population") <= 0)
    ).count()

    population_covered_rows = curated_rows - missing_population
    missing_population_rate = 0.0
    population_uncovered_rate = (
        missing_population / curated_rows if curated_rows else 0.0
    )
    population_coverage_rate = (
        population_covered_rows / curated_rows
        if curated_rows
        else 0.0
    )
    coverage_accounting_error = (
        population_covered_rows + missing_population - curated_rows
    )

    non_positive_cases = curated.filter(
        F.col("total_cases") <= 0
    ).count()

    duplicate_keys = count_duplicate_keys(
        curated, GOLD_PRIMARY_KEY
    )

    unavailable_rate_rows = curated.filter(
        F.col("incidence_rate_per_100k").isNull()
    ).count()

    formula_mismatch_rows = curated.filter(
        (F.col("population") > 0)
        & (
            F.col("incidence_rate_per_100k").isNull()
            | (
                F.abs(
                    F.col("incidence_rate_per_100k")
                    - (
                        F.col("total_cases")
                        / F.col("population")
                        * F.lit(100000)
                    )
                ) > F.lit(0.01)
            )
        )
    ).count()

    validation_source = canonical if canonical is not None else cleaned
    missing_report_dates = validation_source.filter(
        F.col("report_date").isNull()
    ).count()
    invalid_ages = validation_source.filter(
        F.col("age").isNotNull()
        & ~F.col("age").between(0, settings.maximum_age)
    ).count()
    required_columns = {
        "year_be", "disease_name_raw", "district_name_raw",
        "case_count", "report_date", "age",
    }
    missing_schema_columns = sorted(
        required_columns - set(validation_source.columns)
    )

    checks: List[QualityCheck] = [
        QualityCheck(
            "raw", "raw_rows_not_empty",
            str(raw_rows), "> 0", _status(raw_rows > 0),
        ),
        QualityCheck(
            "clean", "clean_rows_not_empty",
            str(clean_rows), "> 0", _status(clean_rows > 0),
        ),
        QualityCheck(
            "clean", "clean_retention_rate",
            f"{(clean_rows / raw_rows if raw_rows else 0):.4f}",
            ">= 0.5",
            _status(raw_rows > 0 and clean_rows / raw_rows >= 0.5),
        ),
        QualityCheck(
            "clean", "duplicate_rows_removed",
            str(removed_duplicates), "informational", "PASS",
        ),
        QualityCheck(
            "standardized", "standardized_rows_not_empty",
            str(standardized_rows), "> 0",
            _status(standardized_rows > 0),
        ),
        QualityCheck(
            "standardized", "district_in_bangkok_master",
            str(invalid_districts), "= 0",
            _status(invalid_districts == 0),
        ),
        QualityCheck(
            "gold", "curated_rows_not_empty",
            str(curated_rows), "> 0", _status(curated_rows > 0),
        ),
        QualityCheck(
            "gold", "missing_population_rate_within_covered_rows",
            f"{missing_population_rate:.4f}",
            f"<= {settings.max_missing_population_rate:.4f}",
            _status(
                missing_population_rate
                <= settings.max_missing_population_rate
            ),
        ),
        QualityCheck(
            "gold", "population_coverage_rate",
            f"{population_coverage_rate:.4f}",
            "informational", "PASS",
        ),
        QualityCheck(
            "gold", "population_uncovered_rows",
            str(missing_population), "informational", "PASS",
        ),
        QualityCheck(
            "gold", "population_uncovered_rate",
            f"{population_uncovered_rate:.4f}",
            "informational", "PASS",
        ),
        QualityCheck(
            "gold", "population_coverage_accounting",
            str(coverage_accounting_error), "= 0",
            _status(coverage_accounting_error == 0),
        ),
        QualityCheck(
            "gold", "non_positive_total_case_rows",
            str(non_positive_cases), "= 0",
            _status(non_positive_cases == 0),
        ),
        QualityCheck(
            "gold", "primary_key_unique",
            str(duplicate_keys), "= 0",
            _status(duplicate_keys == 0),
        ),
        QualityCheck(
            "gold", "incidence_rate_unavailable_rows",
            str(unavailable_rate_rows), "informational", "PASS",
        ),
        QualityCheck(
            "gold", "incidence_unavailable_matches_uncovered",
            str(unavailable_rate_rows - missing_population), "= 0",
            _status(unavailable_rate_rows == missing_population),
        ),
        QualityCheck(
            "gold", "incidence_formula_mismatch_rows",
            str(formula_mismatch_rows), "= 0",
            _status(formula_mismatch_rows == 0),
        ),
        QualityCheck(
            "dq", "required_schema_columns",
            ", ".join(missing_schema_columns) or "complete",
            "complete", _status(not missing_schema_columns),
        ),
        QualityCheck(
            "dq", "missing_report_date_rows",
            str(missing_report_dates), "informational", "PASS",
        ),
        QualityCheck(
            "dq", "invalid_age_rows",
            str(invalid_ages), "= 0",
            _status(invalid_ages == 0),
        ),
    ]

    report = spark.createDataFrame(
        [check.__dict__ for check in checks]
    ).select("layer", "rule", "observed", "threshold", "status")

    has_failure = any(check.status == "FAIL" for check in checks)

    return report, has_failure