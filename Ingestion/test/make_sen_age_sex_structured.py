from __future__ import annotations

import json
from pathlib import Path
import re

import pandas as pd

try:
    from Ingestion.primary_variable import (
        QualitativePrimaryVariable,
        QuantitativePrimaryVariable,
        TOTAL_VALUE,
    )
    from Ingestion.schema import Schema
    from Ingestion.secondary_variable import (
        QualitativeDistributionVariable,
        QuantitativeDistributionVariable,
    )
    from Ingestion.structured_data import StructuredData
except ModuleNotFoundError:  # Allows `python3 Ingestion/test/make_sen_age_sex_structured.py`
    import sys

    ROOT = Path(__file__).resolve().parents[2]
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from Ingestion.primary_variable import (
        QualitativePrimaryVariable,
        QuantitativePrimaryVariable,
        TOTAL_VALUE,
    )
    from Ingestion.schema import Schema
    from Ingestion.secondary_variable import (
        QualitativeDistributionVariable,
        QuantitativeDistributionVariable,
    )
    from Ingestion.structured_data import StructuredData


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
DEFAULT_INPUT = REPO_ROOT / "Data" / "SEN_data-needslfs" / "sen_age_sex_.csv"
OUTPUT_DIR = SCRIPT_DIR / "output"

AGE_COLS = [
    "age_2_and_under",
    "age_3",
    "age_4",
    "age_5",
    "age_6",
    "age_7",
    "age_8",
    "age_9",
    "age_10",
    "age_11",
    "age_12",
    "age_13",
    "age_14",
    "age_15",
    "age_16",
    "age_17",
    "age_18",
    "age_19_and_over",
]


def _slugify(value: str) -> str:
    value = value.strip().lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value


def _resolve_input_csv() -> Path:
    candidates = [
        DEFAULT_INPUT,
        REPO_ROOT / "Data" / "SEN_data" / "sen_age_sex_.csv",
        REPO_ROOT / "SEN_data" / "data" / "sen_age_sex_.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError("Could not find sen_age_sex_.csv in expected locations.")


def _age_display_map() -> dict[str, str]:
    return {c: c.replace("_", " ").title() for c in AGE_COLS}


def _age_numeric_map() -> dict[str, float]:
    mapping: dict[str, float] = {}
    for c in AGE_COLS:
        if c == "age_2_and_under":
            mapping[c] = 2.0
        elif c == "age_19_and_over":
            mapping[c] = 19.0
        else:
            mapping[c] = float(c.replace("age_", ""))
    return mapping


def load_raw() -> pd.DataFrame:
    path = _resolve_input_csv()
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    df["time_period"] = df["time_period"].astype(str)
    df["new_la_code"] = df["new_la_code"].astype(str).str.strip()
    df["la_name"] = df["la_name"].astype(str)
    df.loc[df["new_la_code"].isin(["nan", "None", ""]), "new_la_code"] = pd.NA
    df.loc[df["la_name"].isin(["nan", "None", ""]), "la_name"] = pd.NA

    numeric_cols = ["number_of_pupils", "pupil_sex_male", "pupil_sex_female", *AGE_COLS]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def build_schema(raw: pd.DataFrame) -> Schema:
    base = raw[
        (raw["geographic_level"] == "Local authority")
        & (raw["phase_type_grouping"] == "Total")
        & raw["new_la_code"].notna()
    ].copy()

    years = sorted(base["time_period"].dropna().astype(str).unique().tolist())
    la_map = (
        base[["new_la_code", "la_name"]]
        .dropna(subset=["new_la_code"])
        .drop_duplicates(subset=["new_la_code"])
        .sort_values("new_la_code")
    )
    location_dict = {
        str(code): (name if isinstance(name, str) and name.strip() else str(code))
        for code, name in zip(la_map["new_la_code"], la_map["la_name"])
    }

    sen_type_rows = base[
        (base["sen_status"] == "Education, Health and Care plans")
        & (~base["sen_primary_need"].isin(["Total", "Missing"]))
        & base["sen_primary_need"].notna()
    ].copy()
    sen_labels = sorted(sen_type_rows["sen_primary_need"].astype(str).unique().tolist())
    sen_csv_dict = {_slugify(label): label for label in sen_labels}

    return Schema(
        primary_variables=[
            QuantitativePrimaryVariable(
                "Year",
                "year",
                {y: y for y in years},
                csv_to_number={
                    y: float(y) if y.replace(".", "", 1).isdigit() else float(i)
                    for i, y in enumerate(years)
                },
            ),
            QualitativePrimaryVariable("Location (LA code)", "location_code", location_dict),
        ],
        secondary_variables=[
            QuantitativeDistributionVariable(
                display_name="Age",
                csv_dict=_age_display_map(),
                csv_to_number=_age_numeric_map(),
            ),
            QualitativeDistributionVariable(
                display_name="Sex",
                csv_dict={"male": "Male", "female": "Female"},
            ),
            QualitativeDistributionVariable(
                display_name="SEN type",
                csv_dict=sen_csv_dict,
            ),
        ],
        strict_complete_grid=True,
    )


def build_non_total_rows(raw: pd.DataFrame, schema: Schema) -> pd.DataFrame:
    base = raw[
        (raw["geographic_level"] == "Local authority")
        & (raw["phase_type_grouping"] == "Total")
        & raw["new_la_code"].notna()
    ].copy()

    anchor = base[
        (base["sen_status"] == "Total")
        & (base["sen_primary_need"] == "Total")
    ].copy()

    # One row per (year, LA)
    anchor = anchor.sort_values(["time_period", "new_la_code"]).drop_duplicates(
        subset=["time_period", "new_la_code"], keep="first"
    )

    rows: list[dict[str, object]] = []
    age_sv = schema.get_secondary("age")
    sex_sv = schema.get_secondary("sex")
    sen_sv = schema.get_secondary("sen_type")

    for _, r in anchor.iterrows():
        rec: dict[str, object] = {
            "year": str(r["time_period"]),
            "location_code": str(r["new_la_code"]),
        }

        # Age counts
        for age_key in age_sv.keys():
            rec[age_sv.count_column(age_key)] = pd.to_numeric(r.get(age_key), errors="coerce")

        # Sex counts
        rec[sex_sv.count_column("male")] = pd.to_numeric(r.get("pupil_sex_male"), errors="coerce")
        rec[sex_sv.count_column("female")] = pd.to_numeric(r.get("pupil_sex_female"), errors="coerce")

        rows.append(rec)

    rows_df = pd.DataFrame(rows)

    # SEN type counts: sum EHC rows per (year, LA, sen type)
    sen_rows = base[
        (base["sen_status"] == "Education, Health and Care plans")
        & (~base["sen_primary_need"].isin(["Total", "Missing"]))
        & base["sen_primary_need"].notna()
    ].copy()
    sen_rows["sen_key"] = sen_rows["sen_primary_need"].astype(str).map(_slugify)
    sen_rows["number_of_pupils"] = pd.to_numeric(sen_rows["number_of_pupils"], errors="coerce")

    grouped = (
        sen_rows.groupby(["time_period", "new_la_code", "sen_key"], as_index=False)["number_of_pupils"]
        .sum()
    )
    pivot = grouped.pivot_table(
        index=["time_period", "new_la_code"],
        columns="sen_key",
        values="number_of_pupils",
        aggfunc="sum",
        fill_value=pd.NA,
    ).reset_index()

    pivot = pivot.rename(columns={"time_period": "year", "new_la_code": "location_code"})
    rows_df = rows_df.merge(pivot, on=["year", "location_code"], how="left")

    # Rename pivoted SEN columns to *_count names expected by schema.
    for sen_key in sen_sv.keys():
        if sen_key not in rows_df.columns:
            rows_df[sen_key] = pd.NA
        rows_df[sen_sv.count_column(sen_key)] = pd.to_numeric(rows_df[sen_key], errors="coerce")
    rows_df = rows_df.drop(columns=[c for c in sen_sv.keys() if c in rows_df.columns], errors="ignore")

    return rows_df


def build_structured_data(*, flatten_primary_column: str | None = "year") -> StructuredData:
    raw = load_raw()
    schema = build_schema(raw)
    non_total_rows = build_non_total_rows(raw, schema)

    # Create strict grid with all expected rows/columns, merge observed non-total rows.
    strict_df = schema.generateExampleCSV()
    merge_cols = schema.primary_column_names()
    value_cols = [c for c in non_total_rows.columns if c not in merge_cols]
    strict_df = strict_df.merge(non_total_rows, on=merge_cols, how="left", suffixes=("", "_src"))
    for col in value_cols:
        src_col = f"{col}_src"
        if src_col in strict_df.columns:
            strict_df[col] = strict_df[src_col].combine_first(strict_df[col])
            strict_df = strict_df.drop(columns=[src_col])

    strict_df = schema.generateTotals(strict_df)
    strict_df = schema.generatePercentages(strict_df)
    strict_df = schema.generateAverages(strict_df)
    schema.checkCSV(strict_df)

    structured = StructuredData.from_dataframe(strict_df, schema, validate_final=False)
    if flatten_primary_column is not None:
        structured = structured.flatten_primary_to_secondary(flatten_primary_column)
    return structured


def _assert_primary_types_in_saved_json(json_path: Path) -> None:
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    primary = payload["schema"]["primary_variables"]
    for entry in primary:
        if "class" not in entry or "variable_type" not in entry:
            raise ValueError(
                "Saved schema primary variable is missing 'class' or 'variable_type'. "
                f"Entry: {entry}"
            )


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    structured = build_structured_data()

    json_path = OUTPUT_DIR / "sen_age_sex_structured.json"
    csv_path = OUTPUT_DIR / "sen_age_sex_structured.csv"
    structured.save(json_path=json_path, csv_path=csv_path)
    _assert_primary_types_in_saved_json(json_path)

    print(f"Saved StructuredData JSON: {json_path}")
    print(f"Saved StructuredData CSV:  {csv_path}")
    print(f"Rows: {len(structured.dataframe)}")
    print(f"Defined sections: {len(structured.defined_map)}")


if __name__ == "__main__":
    main()
