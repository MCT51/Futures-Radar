from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from Ingestion.primary_variable import (
    QualitativePrimaryVariable,
)
from Ingestion.secondary_variable import (
    QuantitativeScalarSecondaryVariable,
    QualitativeScalarSecondaryVariable,
    QualitativeDistributionVariable,
    QuantitativeDistributionVariable,
)
from Ingestion.schema import Schema
from Ingestion.structured_data import StructuredData


# ---------- helpers ----------

def slugify(s: str) -> str:
    s = str(s).strip().lower()
    out = []
    prev_us = False
    for ch in s:
        if ch.isalnum():
            out.append(ch)
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    res = "".join(out).strip("_")
    return res or "value"


def apply_filters(df: pd.DataFrame, filters: list[dict[str, Any]]) -> pd.DataFrame:
    """
    filters items like:
      {"col": "geographic_level", "op": "==", "value": "Local authority"}
      {"col": "phase_type_grouping", "op": "in", "value": ["Total","Primary"]}
    """
    if not filters:
        return df

    out = df.copy()
    for f in filters:
        col = f["col"]
        op = f.get("op", "==")
        val = f.get("value")

        if op == "==":
            out = out[out[col] == val]
        elif op == "!=":
            out = out[out[col] != val]
        elif op == "in":
            out = out[out[col].isin(val)]
        elif op == "not in":
            out = out[~out[col].isin(val)]
        else:
            raise ValueError(f"Unsupported filter op: {op}")
    return out


# ---------- secondary definitions the UI will produce ----------

SecondaryType = Literal["quant_scalar", "qual_scalar", "qual_dist", "quant_dist"]

@dataclass
class SecondarySpec:
    name: str                 # e.g. "fsm_eligibility" (used as variable_name)
    display_name: str         # e.g. "FSM eligibility"
    type: SecondaryType

    # scalar
    value_col: str | None = None
    aggregation: str | None = None

    # distribution (long form input)
    category_col: str | None = None
    count_col: str | None = None

    # quant_dist only
    # mapping from raw category value -> representative number (optional; can be auto-filled later)
    csv_to_number: dict[str, float] | None = None


def parse_structured_from_csv(
    *,
    raw_csv_path: str | Path,
    primary_cols: list[str],
    secondary_specs: list[SecondarySpec],
    filters: list[dict[str, Any]] | None,
    display_name_columns: dict[str, str] | None = None,
) -> StructuredData:
    raw_csv_path = Path(raw_csv_path)

    df_raw = pd.read_csv(raw_csv_path)
    df_raw.columns = df_raw.columns.str.strip()

    # Apply filters first so categories/values reflect the chosen slice
    df_raw = apply_filters(df_raw, filters or [])

    display_name_columns = display_name_columns or {}

    # --- Build PrimaryVariables ---
    primary_vars = []
    for col in primary_cols:
        # mapping raw->display (identity by default)
        vals = (
            df_raw[col]
            .dropna()
            .astype(str)
            .map(lambda x: x.strip())
            .unique()
            .tolist()
        )
        csv_to_display = {v: v for v in sorted(vals)}
        if col in display_name_columns:
            disp_col = display_name_columns[col]
            if disp_col not in df_raw.columns:
                raise ValueError(f"Display column '{disp_col}' not found in dataset.")
            mapping_df = (
                df_raw[[col, disp_col]]
                .dropna()
            )
            mapping_df[col] = mapping_df[col].astype(str).str.strip()
            mapping_df[disp_col] = mapping_df[disp_col].astype(str).str.strip()
            mapping_df = mapping_df.drop_duplicates(subset=col)
            csv_to_display.update(dict(zip(mapping_df[col], mapping_df[disp_col])))
        primary_vars.append(
            QualitativePrimaryVariable(
                title=col.replace("_", " ").title(),
                column_name=col,
                csv_to_display=csv_to_display,
            )
        )

    # --- Build a "raw for schema" dataframe ---
    # Start with just the primary columns
    df_work = df_raw[primary_cols].copy()
    for col in primary_cols:
        df_work[col] = df_work[col].astype(str).str.strip()

    secondary_vars = []

    # helper: ensure uniqueness per primary key for scalar vars (simple default)
    def collapse_scalar(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
        tmp = df[primary_cols + [value_col]].copy()
        for col in primary_cols:
            tmp[col] = tmp[col].astype(str).str.strip()
        return (
            tmp.groupby(primary_cols, as_index=False)[value_col]
            .first()
    )

    for spec in secondary_specs:
        if spec.type == "quant_scalar":
            if not spec.value_col:
                raise ValueError(f"{spec.name}: quant_scalar requires value_col")

            val_col = spec.value_col
            d = collapse_scalar(df_raw, val_col)

            # merge into df_work
            df_work = df_work.merge(d, on=primary_cols, how="left")

            secondary_vars.append(
                QuantitativeScalarSecondaryVariable(
                    display_name=spec.display_name,
                    csv_dict={val_col: spec.display_name},
                    variable_name=spec.name,
                    aggregation=spec.aggregation or "sum",
                )
            )

        elif spec.type == "qual_scalar":
            if not spec.value_col:
                raise ValueError(f"{spec.name}: qual_scalar requires value_col")

            val_col = spec.value_col
            d = collapse_scalar(df_raw, val_col)
            df_work = df_work.merge(d, on=primary_cols, how="left")

            # auto-discover allowed values
            allowed = (
                df_raw[val_col]
                .dropna()
                .astype(str)
                .map(lambda x: x.strip())
                .unique()
                .tolist()
            )
            csv_dict = {v: v for v in sorted(allowed)}

            secondary_vars.append(
                QualitativeScalarSecondaryVariable(
                    display_name=spec.display_name,
                    csv_dict=csv_dict,
                    variable_name=spec.name,
                    value_column_name=val_col,
                )
            )

        elif spec.type in ("qual_dist", "quant_dist"):
            if not spec.category_col or not spec.count_col:
                raise ValueError(f"{spec.name}: dist requires category_col and count_col")

            cat_col = spec.category_col
            cnt_col = spec.count_col

            # discover categories and slugify to safe keys
            raw_cats = (
                df_raw[cat_col]
                .dropna()
                .astype(str)
                .map(lambda x: x.strip())
                .unique()
                .tolist()
            )

            # raw_value -> key (slug)
            raw_to_key = {rc: slugify(rc) for rc in raw_cats}

            # ensure uniqueness if two labels slugify to same key
            seen = {}
            for rc, k in list(raw_to_key.items()):
                if k not in seen:
                    seen[k] = 1
                else:
                    seen[k] += 1
                    raw_to_key[rc] = f"{k}_{seen[k]}"

            # key -> display label
            csv_dict = {raw_to_key[rc]: rc for rc in raw_cats}

            # pivot long -> wide counts
            tmp = df_raw[primary_cols + [cat_col, cnt_col]].copy()
            for col in primary_cols:
                tmp[col] = tmp[col].astype(str).str.strip()
            tmp[cat_col] = tmp[cat_col].astype(str).str.strip().map(raw_to_key)
            tmp[cnt_col] = pd.to_numeric(tmp[cnt_col], errors="coerce")
            

            pivot = (
                tmp.pivot_table(
                    index=primary_cols,
                    columns=cat_col,
                    values=cnt_col,
                    aggfunc="sum",
                )
                .reset_index()
            )

            # rename category columns to "{key}_count"
            rename_map = {k: f"{k}_count" for k in csv_dict.keys() if k in pivot.columns}
            pivot = pivot.rename(columns=rename_map)

            # ensure every category column exists even if absent in pivot result
            for key in csv_dict.keys():
                count_col = f"{key}_count"
                if count_col not in pivot.columns:
                    pivot[count_col] = 0

            # merge into df_work
            df_work = df_work.merge(pivot, on=primary_cols, how="left")
            for key in csv_dict.keys():
                count_col = f"{key}_count"
                if count_col in df_work.columns:
                    df_work[count_col] = df_work[count_col].fillna(0)

            if spec.type == "qual_dist":
                secondary_vars.append(
                    QualitativeDistributionVariable(
                        display_name=spec.display_name,
                        csv_dict=csv_dict,
                        variable_name=spec.name,
                    )
                )
            else:
                # quantitative dist needs csv_to_number for ALL keys
                if spec.csv_to_number is None:
                    # sensible default: try to parse numbers from labels; otherwise require user mapping later
                    csv_to_number = {}
                    for key, label in csv_dict.items():
                        # naive number extraction
                        digits = "".join(ch for ch in label if (ch.isdigit() or ch == "." or ch == "-"))
                        csv_to_number[key] = float(digits) if digits not in ("", "-", ".", "-.") else 0.0
                else:
                    # user mapping is by raw label or by key? we’ll accept either.
                    csv_to_number = {}
                    for key, label in csv_dict.items():
                        if key in spec.csv_to_number:
                            csv_to_number[key] = float(spec.csv_to_number[key])
                        elif label in spec.csv_to_number:
                            csv_to_number[key] = float(spec.csv_to_number[label])
                        else:
                            raise ValueError(f"{spec.name}: csv_to_number missing for category '{label}' (key '{key}')")

                secondary_vars.append(
                    QuantitativeDistributionVariable(
                        display_name=spec.display_name,
                        csv_dict=csv_dict,
                        variable_name=spec.name,
                        csv_to_number=csv_to_number,
                    )
                )

        else:
            raise ValueError(f"Unknown secondary type: {spec.type}")

    # De-duplicate primary columns (df_work started as primaries but got merged)
    df_work = df_work.drop_duplicates(subset=primary_cols)

    # --- Build schema, normalize, generate derived cols, validate ---
    schema = Schema(primary_variables=primary_vars, secondary_variables=secondary_vars)

    # Make strict grid, merge in raw, then generate derived values
    df_strict = schema.normalizeToStrictStructure(df_work)
    df_strict = schema.generateTotals(df_strict)
    df_strict = schema.generatePercentages(df_strict)
    df_strict = schema.generateAverages(df_strict)
    schema.checkCSV(df_strict)

    return StructuredData(dataframe=df_strict, schema=schema)


def save_structured_data(
    *,
    structured: StructuredData,
    dataset_name: str,
    out_dir: str | Path,
) -> tuple[Path, Path]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_out = out_dir / f"{dataset_name}_structured.csv"
    json_out = out_dir / f"{dataset_name}_structured.json"
    structured.save(json_path=json_out, csv_path=csv_out, include_schema=True)
    return json_out, csv_out


def build_structured_from_csv(
    *,
    raw_csv_path: str | Path,
    dataset_name: str,
    primary_cols: list[str],
    secondary_specs: list[SecondarySpec],
    filters: list[dict[str, Any]] | None,
    out_dir: str | Path,
    display_name_columns: dict[str, str] | None = None,
) -> tuple[Path, Path]:
    structured = parse_structured_from_csv(
        raw_csv_path=raw_csv_path,
        primary_cols=primary_cols,
        secondary_specs=secondary_specs,
        filters=filters,
        display_name_columns=display_name_columns,
    )
    return save_structured_data(
        structured=structured,
        dataset_name=dataset_name,
        out_dir=out_dir,
    )
