"""Run the Spark processing pipeline after the raw lake ingestion succeeds."""

from datetime import datetime, timedelta

# Airflow is installed in the Docker image, not the local interpreter.
from airflow import DAG  # pyright: ignore[reportAttributeAccessIssue, reportMissingImports]
from airflow.operators.python import PythonOperator  # pyright: ignore[reportMissingImports]


def run_spark_pipeline():
    import subprocess

    completed = subprocess.run(
        [
            "python",
            "-m",
            "spark.main",
            "--project-root",
            "/opt/airflow/project",
            "--fail-on-dq",
        ],
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"Spark processing pipeline failed with exit code "
            f"{completed.returncode}"
        )


with DAG(
    dag_id="spark_processing",
    description="Process landed disease and population data with the Spark pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["spark", "curated"],
) as dag:
    run_spark = PythonOperator(
        task_id="run_spark_pipeline",
        python_callable=run_spark_pipeline,
    )
