import shutil
from pathlib import Path

from pyspark.sql import DataFrame


def remove_path(path: Path) -> None:
    """ลบไฟล์หรือโฟลเดอร์เดิมก่อนเขียนทับ"""

    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def write_parquet_directory(
    dataframe: DataFrame,
    output_path: Path,
) -> None:
    """เขียน Parquet แบบหลาย part สำหรับชั้น Clean และ Standardized"""

    remove_path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    (
        dataframe.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(str(output_path))
    )


def write_single_file(
    dataframe: DataFrame,
    target_file: Path,
    output_format: str,
) -> None:
    """เขียนเป็นไฟล์เดียวตามชื่อที่กำหนด เช่น curated_disease_data.csv"""

    if output_format not in {"csv", "parquet"}:
        raise ValueError(
            f"ไม่รองรับรูปแบบไฟล์: {output_format}"
        )

    target_file.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = target_file.parent / f".{target_file.name}.tmp"

    remove_path(temporary_path)
    remove_path(target_file)

    writer = dataframe.coalesce(1).write.mode("overwrite")

    if output_format == "csv":
        (
            writer
            .option("header", True)
            .option("encoding", "UTF-8")
            .csv(str(temporary_path))
        )
    else:
        writer.option("compression", "snappy").parquet(
            str(temporary_path)
        )

    part_files = list(
        temporary_path.glob(f"part-*.{output_format}")
    )

    if len(part_files) != 1:
        remove_path(temporary_path)
        raise RuntimeError(
            f"คาดว่าจะพบ 1 part file ใน {temporary_path} "
            f"แต่พบ {len(part_files)} ไฟล์"
        )

    shutil.move(str(part_files[0]), str(target_file))
    remove_path(temporary_path)


def write_quality_report(
    dataframe: DataFrame,
    target_file: Path,
) -> None:
    """บันทึกรายงานคุณภาพข้อมูลเป็น CSV ไฟล์เดียว"""

    write_single_file(dataframe, target_file, "csv")


def write_error_log(
    dataframe: DataFrame,
    target_file: Path,
) -> None:
    """บันทึกเฉพาะรายการ DQ ที่ไม่ผ่านเพื่อใช้ติดตามแก้ไข"""

    write_single_file(dataframe.filter("status = 'FAIL'"), target_file, "csv")