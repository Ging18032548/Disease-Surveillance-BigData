"""Prepare Bangkok population reference CSVs for the Part 3 pipeline.

The script never invents population values.  It accepts an official CSV/XLSX
file or URL for each year, validates it, aggregates subdistrict rows when
needed, and writes the result only after all checks pass.
"""

from __future__ import annotations

import argparse
import io
import re
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

from spark.schemas import BANGKOK_DISTRICTS

EXPECTED_COLUMNS = ["ปี", "เขต", "ประชากรรวม"]
YEAR_COLUMN_ALIASES = {"ปี", "ปีข้อมูล", "ปีพ.ศ.", "พ.ศ.", "year", "year_be"}
DISTRICT_COLUMN_ALIASES = {
    "เขต", "ชื่อเขต", "อำเภอ/เขต", "district", "district_name",
}
POPULATION_COLUMN_ALIASES = {
    "ประชากรรวม", "ประชากรทั้งหมด", "จำนวนประชากร", "ประชากร",
    "total_population", "population",
}
SUBDISTRICT_COLUMN_ALIASES = {"แขวง", "ชื่อตำบล", "subdistrict"}


def _clean_name(value: object) -> str:
    return re.sub(r"\s+", " ", str(value).replace("\ufeff", "")).strip()


def _find_column(columns: Iterable[object], aliases: set[str]) -> str | None:
    normalized = {_clean_name(column): str(column) for column in columns}
    for alias in aliases:
        if alias in normalized:
            return normalized[alias]
    return None


def _normalize_district(value: object) -> str:
    district = _clean_name(value)
    district = re.sub(r"^(เขต|ข\.?)\s*", "", district)
    district = re.sub(r"\s*(กรุงเทพมหานคร|กรุงเทพฯ|กทม\.?)$", "", district)
    aliases = {
        "ป้อมปราบฯ": "ป้อมปราบศัตรูพ่าย",
        "ป้อมปราบ": "ป้อมปราบศัตรูพ่าย",
        "สัมพันธวงษ์": "สัมพันธวงศ์",
    }
    return aliases.get(district, district)


def _read_source(source: str) -> pd.DataFrame:
    path = Path(source)
    if path.is_file():
        if path.suffix.lower() in {".xlsx", ".xls"}:
            return pd.read_excel(path)
        return pd.read_csv(path, encoding="utf-8-sig")

    response = requests.get(source, timeout=60)
    response.raise_for_status()
    content_type = response.headers.get("content-type", "").lower()
    if "excel" in content_type or source.lower().split("?")[0].endswith(
        (".xlsx", ".xls")
    ):
        return pd.read_excel(io.BytesIO(response.content))
    return pd.read_csv(io.BytesIO(response.content), encoding="utf-8-sig")


def prepare_year(source: str, year: int) -> pd.DataFrame:
    frame = _read_source(source)
    frame.columns = [_clean_name(column) for column in frame.columns]

    year_column = _find_column(frame.columns, YEAR_COLUMN_ALIASES)
    district_column = _find_column(frame.columns, DISTRICT_COLUMN_ALIASES)
    population_column = _find_column(frame.columns, POPULATION_COLUMN_ALIASES)
    subdistrict_column = _find_column(
        frame.columns, SUBDISTRICT_COLUMN_ALIASES
    )

    if district_column is None or population_column is None:
        missing = []
        if district_column is None:
            missing.append("เขต")
        if population_column is None:
            missing.append("ประชากรรวม")
        raise ValueError(
            f"{source}: cannot identify required columns: {', '.join(missing)}"
        )

    selected = frame[[column for column in (
        year_column, district_column, subdistrict_column, population_column
    ) if column is not None]].copy()
    selected = selected.rename(columns={
        district_column: "เขต",
        population_column: "ประชากรรวม",
    })

    if year_column is not None:
        selected["ปี"] = pd.to_numeric(selected[year_column], errors="coerce")
        if selected["ปี"].notna().any() and (selected["ปี"] != year).any():
            raise ValueError(f"{source}: contains a year other than {year}")
    else:
        selected["ปี"] = year

    selected["เขต"] = selected["เขต"].map(_normalize_district)
    selected["ประชากรรวม"] = pd.to_numeric(
        selected["ประชากรรวม"].astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    )
    selected = selected[["ปี", "เขต", "ประชากรรวม"]]
    selected = selected.dropna(subset=["ปี", "เขต", "ประชากรรวม"])
    selected = selected[selected["เขต"] != ""]
    selected = selected[
        ~selected["เขต"].str.lower().isin({"รวม", "ทั้งหมด", "total"})
    ]
    selected = selected[selected["ประชากรรวม"] > 0]

    result = (
        selected.groupby(["ปี", "เขต"], as_index=False)["ประชากรรวม"]
        .sum()
    )
    result["ปี"] = result["ปี"].astype(int)
    result["ประชากรรวม"] = result["ประชากรรวม"].astype("int64")
    validate_population(result, year, source)
    return result[EXPECTED_COLUMNS].sort_values("เขต").reset_index(drop=True)


def validate_population(
    frame: pd.DataFrame, year: int, source: str = "population"
) -> None:
    if list(frame.columns) != EXPECTED_COLUMNS:
        raise ValueError(
            f"{source}: columns must be exactly {EXPECTED_COLUMNS}"
        )
    if frame.isna().any().any():
        raise ValueError(f"{source}: null values are not allowed")
    if len(frame) != 50:
        raise ValueError(f"{source}: expected 50 districts, got {len(frame)}")
    if frame["ปี"].astype(int).ne(year).any():
        raise ValueError(f"{source}: every row must have year {year}")
    if frame["เขต"].astype(str).str.strip().eq("").any():
        raise ValueError(f"{source}: district names cannot be empty")
    if frame["เขต"].duplicated().any():
        raise ValueError(f"{source}: duplicate district names found")
    if set(frame["เขต"]) != set(BANGKOK_DISTRICTS):
        missing = sorted(set(BANGKOK_DISTRICTS) - set(frame["เขต"]))
        unexpected = sorted(set(frame["เขต"]) - set(BANGKOK_DISTRICTS))
        raise ValueError(
            f"{source}: district coverage mismatch; missing={missing}, "
            f"unexpected={unexpected}"
        )
    if not pd.api.types.is_integer_dtype(frame["ประชากรรวม"]):
        raise ValueError(f"{source}: population must be integer")
    if frame["ประชากรรวม"].le(0).any():
        raise ValueError(f"{source}: population must be positive")


def write_reference(frame: pd.DataFrame, output: Path, year: int) -> None:
    if output.exists():
        raise FileExistsError(
            f"{output} already exists; refusing to overwrite an existing file"
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    frame.to_csv(temporary, index=False, encoding="utf-8-sig", lineterminator="\n")
    temporary.replace(output)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Aggregate and validate Bangkok population reference data"
    )
    parser.add_argument("--project-root", type=Path, default=Path("."))
    parser.add_argument("--source-2568", required=True)
    parser.add_argument("--source-2569", required=True)
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace output files explicitly; default is refuse-to-overwrite",
    )
    args = parser.parse_args()

    reference = (
        args.project_root / "data" / "raw" / "disease" / "reference"
    ).resolve()
    outputs = {
        2568: reference / "ประชากรและครัวเรือน_2568.csv",
        2569: reference / "ประชากรและครัวเรือน_2569.csv",
    }
    prepared = {
        year: prepare_year(source, year)
        for year, source in (
            (2568, args.source_2568),
            (2569, args.source_2569),
        )
    }
    if not args.overwrite and any(path.exists() for path in outputs.values()):
        existing = [str(path) for path in outputs.values() if path.exists()]
        raise FileExistsError(
            "Refusing to overwrite existing output(s): " + ", ".join(existing)
        )
    for year, frame in prepared.items():
        if args.overwrite and outputs[year].exists():
            outputs[year].unlink()
        write_reference(frame, outputs[year], year)
        print(f"Wrote {len(frame)} districts for {year}: {outputs[year]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
