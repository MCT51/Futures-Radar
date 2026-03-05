from __future__ import annotations

import json
from pathlib import Path
import re

import pandas as pd

try:
    from Ingestion.primary_variable import QualitativePrimaryVariable, QuantitativePrimaryVariable
    from Ingestion.schema import Schema
    from Ingestion.secondary_variable import QualitativeDistributionVariable
    from Ingestion.structured_data import StructuredData
except ModuleNotFoundError:  # Allows direct script execution
    import sys

    ROOT = Path(__file__).resolve().parents[2]
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from Ingestion.primary_variable import QualitativePrimaryVariable, QuantitativePrimaryVariable
    from Ingestion.schema import Schema
    from Ingestion.secondary_variable import QualitativeDistributionVariable
    from Ingestion.structured_data import StructuredData


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
OUTPUT_DIR = SCRIPT_DIR / "output"


def _slugify(value: str) -> str:
    value = value.strip().lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value


def _resolve_input_csv() -> Path:
    candidates = [
        REPO_ROOT / "Data" / "data-large-needslfs" / "spc_pupils_fsm_ethnicity_yrgp.csv",
        REPO_ROOT / "Data" / "spc_pupils_fsm_ethnicity_yrgp.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError("Could not find spc_pupils_fsm_ethnicity_yrgp.csv in expected locations.")


def load_raw() -> pd.DataFrame:
    path = _resolve_input_csv()
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    df["time_period"] = df["time_period"].astype(str)
    df["number_of_pupils"] = pd.to_numeric(df["number_of_pupils"], errors="coerce")
    df["new_la_code"] = df["new_la_code"].astype(str).str.strip()
    df["la_name"] = df["la_name"].astype(str)
    df.loc[df["new_la_code"].isin(["nan", "None", ""]), "new_la_code"] = pd.NA
    df.loc[df["la_name"].isin(["nan", "None", ""]), "la_name"] = pd.NA
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
    locations = {
        str(code): (name if isinstance(name, str) and name.strip() else str(code))
        for code, name in zip(la_map["new_la_code"], la_map["la_name"])
    }

    fsm_labels = sorted(
        base["fsm_eligibility"].dropna().astype(str).unique().tolist()
    )
    ethnicity_labels = sorted(
        base.loc[base["characteristic_group"] == "Ethnicity Minor", "characteristic"]
        .dropna().astype(str).unique().tolist()
    )
    ncyear_labels = sorted(
        base.loc[base["characteristic_group"] == "NC year group", "characteristic"]
        .dropna().astype(str).unique().tolist()
    )

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
            QualitativePrimaryVariable("Location (LA code)", "location_code", locations),
        ],
        secondary_variables=[
            QualitativeDistributionVariable(
                display_name="FSM eligibility",
                csv_dict={_slugify(v): v for v in fsm_labels},
            ),
            QualitativeDistributionVariable(
                display_name="Ethnicity",
                csv_dict={_slugify(v): v for v in ethnicity_labels},
            ),
            QualitativeDistributionVariable(
                display_name="NC year group",
                csv_dict={_slugify(v): v for v in ncyear_labels},
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
    base["number_of_pupils"] = pd.to_numeric(base["number_of_pupils"], errors="coerce")

    anchors = (
        base[
            (base["characteristic_group"] == "Total")
            & (base["characteristic"] == "Total")
        ][["time_period", "new_la_code"]]
        .drop_duplicates()
        .rename(columns={"time_period": "year", "new_la_code": "location_code"})
    )

    rows = anchors.copy()

    def _add_dist_counts(
        rows_df: pd.DataFrame,
        *,
        sv_name: str,
        source_df: pd.DataFrame,
        category_col: str,
    ) -> pd.DataFrame:
        sv = schema.get_secondary(sv_name)
        tmp = source_df.copy()
        tmp["category_key"] = tmp[category_col].astype(str).map(_slugify)
        grouped = (
            tmp.groupby(["time_period", "new_la_code", "category_key"], as_index=False)["number_of_pupils"]
            .sum()
        )
        pivot = grouped.pivot_table(
            index=["time_period", "new_la_code"],
            columns="category_key",
            values="number_of_pupils",
            aggfunc="sum",
            fill_value=pd.NA,
        ).reset_index()
        pivot = pivot.rename(columns={"time_period": "year", "new_la_code": "location_code"})
        merged = rows_df.merge(pivot, on=["year", "location_code"], how="left")

        for key in sv.keys():
            if key not in merged.columns:
                merged[key] = pd.NA
            merged[sv.count_column(key)] = pd.to_numeric(merged[key], errors="coerce")

        merged = merged.drop(columns=[c for c in sv.keys() if c in merged.columns], errors="ignore")
        return merged

    # FSM eligibility: use characteristic_group=Total and characteristic=Total
    fsm_source = base[
        (base["characteristic_group"] == "Total")
        & (base["characteristic"] == "Total")
        & base["fsm_eligibility"].notna()
    ].copy()
    rows = _add_dist_counts(rows, sv_name="fsm_eligibility", source_df=fsm_source, category_col="fsm_eligibility")

    # Ethnicity: sum across FSM eligibility
    ethnicity_source = base[
        (base["characteristic_group"] == "Ethnicity Minor")
        & base["characteristic"].notna()
    ].copy()
    rows = _add_dist_counts(rows, sv_name="ethnicity", source_df=ethnicity_source, category_col="characteristic")

    # NC year group: sum across FSM eligibility
    ncyear_source = base[
        (base["characteristic_group"] == "NC year group")
        & base["characteristic"].notna()
    ].copy()
    rows = _add_dist_counts(rows, sv_name="nc_year_group", source_df=ncyear_source, category_col="characteristic")

    return rows


def build_structured_data(*, flatten_primary_column: str | None = "year") -> StructuredData:
    raw = load_raw()
    schema = build_schema(raw)
    non_total_rows = build_non_total_rows(raw, schema)

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
    strict_df = schema.generateAverages(strict_df)  # Generates modes for qualitative distributions
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

    json_path = OUTPUT_DIR / "fsm_ethnicity_structured.json"
    csv_path = OUTPUT_DIR / "fsm_ethnicity_structured.csv"
    structured.save(json_path=json_path, csv_path=csv_path)
    _assert_primary_types_in_saved_json(json_path)

    print(f"Saved StructuredData JSON: {json_path}")
    print(f"Saved StructuredData CSV:  {csv_path}")
    print(f"Rows: {len(structured.dataframe)}")
    print(f"Defined sections: {len(structured.defined_map)}")


if __name__ == "__main__":
    main()
