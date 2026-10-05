import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession


@dataclass
class ProjectPaths:
    """รวม Path ทั้งหมดที่ Part 3 ใช้งาน"""

    root: Path

    @property
    def raw_disease_dir(self) -> Path:
        return self.root / "data" / "raw" / "disease"

    @property
    def raw_reference_dir(self) -> Path:
        return self.raw_disease_dir / "reference"

    @property
    def processed_dir(self) -> Path:
        return self.root / "data" / "processed"

    @property
    def clean_dir(self) -> Path:
        return self.processed_dir / "clean" / "disease_cleaned"

    @property
    def standardized_dir(self) -> Path:
        return (
            self.processed_dir
            / "standardized"
            / "disease_standardized"
        )

    @property
    def gold_dir(self) -> Path:
        return (
            self.processed_dir
            / "gold"
            / "disease_with_population"
        )

    @property
    def gold_csv(self) -> Path:
        return self.gold_dir / "curated_disease_data.csv"

    @property
    def gold_parquet(self) -> Path:
        return self.gold_dir / "curated_disease_data"

    @property
    def quarantine_dir(self) -> Path:
        return self.processed_dir / "quarantine" / "disease_rejected"

    @property
    def quality_csv(self) -> Path:
        return (
            self.processed_dir
            / "quality"
            / "data_quality_report.csv"
        )

    @property
    def error_log_csv(self) -> Path:
        return self.processed_dir / "quality" / "data_quality_errors.csv"


@dataclass
class Settings:
    """ค่าตั้งต้นที่อ่านจากไฟล์ .env"""

    spark_master: str = "local[*]"
    spark_log_level: str = "WARN"
    timezone: str = "Asia/Bangkok"
    shuffle_partitions: int = 8
    max_missing_population_rate: float = 0.05
    minimum_year_be: int = 2400
    maximum_year_be: int = 2700
    maximum_age: int = 120
    valid_sex_values: tuple = field(
        default_factory=lambda: ("M", "F", "U")
    )

    @classmethod
    def from_env(cls, project_root: Path) -> "Settings":
        load_dotenv(project_root / ".env")

        return cls(
            spark_master=os.getenv(
                "SPARK_MASTER", "local[*]"
            ),
            spark_log_level=os.getenv(
                "SPARK_LOG_LEVEL", "WARN"
            ),
            timezone=os.getenv(
                "SPARK_TIMEZONE", "Asia/Bangkok"
            ),
            shuffle_partitions=int(
                os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")
            ),
            max_missing_population_rate=float(
                os.getenv(
                    "DQ_MAX_MISSING_POPULATION_RATE",
                    "0.05",
                )
            ),
        )


def create_spark_session(
    settings: Settings,
    app_name: str = "DiseaseSurveillancePart3",
) -> SparkSession:
    """สร้าง SparkSession ตามค่าใน Settings"""

    session = (
        SparkSession.builder
        .appName(app_name)
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", settings.timezone)
        .config(
            "spark.sql.shuffle.partitions",
            settings.shuffle_partitions,
        )
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy",
        )
        .config(
            "spark.sql.legacy.timeParserPolicy",
            "CORRECTED",
        )
        .getOrCreate()
    )

    session.sparkContext.setLogLevel(settings.spark_log_level)

    return session