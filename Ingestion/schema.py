from __future__ import annotations

from dataclasses import dataclass, field
from itertools import product
import json
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from Ingestion.primary_variable import PrimaryVariable, TOTAL_VALUE
    from Ingestion.secondary_variable import (
        DistributionSecondaryVariable,
        QualitativeScalarSecondaryVariable,
        QualitativeDistributionVariable,
        QuantitativeScalarSecondaryVariable,
        QuantitativeDistributionVariable,
        ScalarSecondaryVariable,
        SecondaryVariable,
        secondary_variable_from_dict,
    )
except ModuleNotFoundError:  # Support running as script from Ingestion/
    from primary_variable import PrimaryVariable, TOTAL_VALUE
    from secondary_variable import (
        DistributionSecondaryVariable,
        QualitativeScalarSecondaryVariable,
        QualitativeDistributionVariable,
        QuantitativeScalarSecondaryVariable,
        QuantitativeDistributionVariable,
        ScalarSecondaryVariable,
        SecondaryVariable,
        secondary_variable_from_dict,
    )


@dataclass
class Schema:
    primary_variables: list[PrimaryVariable]
    secondary_variables: list[SecondaryVariable]
    strict_complete_grid: bool = True
    allow_empty_entries: bool = True
    undefined_policy: str = "any_missing_category_marks_variable_undefined"

    _secondary_by_name: dict[str, SecondaryVariable] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.primary_variables:
            raise ValueError("Schema requires at least one primary variable")
        if not self.secondary_variables:
            raise ValueError("Schema requires at least one secondary variable")

        primary_cols = [p.column_name for p in self.primary_variables]
        if len(primary_cols) != len(set(primary_cols)):
            raise ValueError("Primary variable column names must be unique")

        self._secondary_by_name = {}
        for sv in self.secondary_variables:
            name = sv.variable_name or sv.display_name
            if name in self._secondary_by_name:
                raise ValueError(f"Duplicate secondary variable name: {name}")
            self._secondary_by_name[name] = sv

    def to_dict(self) -> dict[str, Any]:
        return {
            "primary_variables": [pv.to_dict() for pv in self.primary_variables],
            "secondary_variables": [sv.to_dict() for sv in self.secondary_variables],
            "strict_complete_grid": self.strict_complete_grid,
            "allow_empty_entries": self.allow_empty_entries,
            "undefined_policy": self.undefined_policy,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Schema":
        return cls(
            primary_variables=[PrimaryVariable.from_dict(d) for d in data["primary_variables"]],
            secondary_variables=[
                secondary_variable_from_dict(d) for d in data["secondary_variables"]
            ],
            strict_complete_grid=bool(data.get("strict_complete_grid", True)),
            allow_empty_entries=bool(data.get("allow_empty_entries", True)),
            undefined_policy=data.get(
                "undefined_policy", "any_missing_category_marks_variable_undefined"
            ),
        )

    def save_json(self, path: str | Path) -> None:
        out_path = Path(path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(self.to_dict(), indent=2), encoding="utf-8")

    @classmethod
    def load_json(cls, path: str | Path) -> "Schema":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls.from_dict(data)

    def primary_column_names(self) -> list[str]:
        return [p.column_name for p in self.primary_variables]

    def secondary_names(self) -> list[str]:
        return list(self._secondary_by_name.keys())

    def get_secondary(self, name: str) -> SecondaryVariable:
        return self._secondary_by_name[name]

    def expected_raw_columns(self) -> set[str]:
        cols = set(self.primary_column_names())
        for sv in self.secondary_variables:
            cols.update(sv.required_raw_columns())
        return cols

    def expected_final_columns(self) -> set[str]:
        cols = set(self.primary_column_names())
        for sv in self.secondary_variables:
            cols.update(sv.required_final_columns())
        return cols

    def optional_raw_generated_columns(self) -> set[str]:
        cols: set[str] = set()
        for sv in self.secondary_variables:
            cols.update(sv.optional_generated_columns())
        return cols

    def expected_row_count(self) -> int:
        sizes = [len(p.expected_values_for_final()) for p in self.primary_variables]
        out = 1
        for s in sizes:
            out *= s
        return out

    def _check_required_columns(self, df: pd.DataFrame, required: set[str], *, stage: str) -> None:
        missing = sorted(required - set(df.columns))
        if missing:
            raise KeyError(f"{stage}: missing required columns: {missing}")

    def _check_no_duplicate_primary_rows(self, df: pd.DataFrame, *, stage: str) -> None:
        pcols = self.primary_column_names()
        dupes = df.duplicated(subset=pcols, keep=False)
        if dupes.any():
            sample = df.loc[dupes, pcols].head(5).to_dict(orient="records")
            raise ValueError(f"{stage}: duplicate primary-key rows found. Sample: {sample}")

    def _check_primary_values_are_known(self, df: pd.DataFrame, *, stage: str, final: bool) -> None:
        for pv in self.primary_variables:
            allowed = set(pv.expected_values_for_final() if final else pv.values())
            if not final:
                allowed.add(TOTAL_VALUE)
            if not allowed:
                continue
            vals = set(df[pv.column_name].dropna().astype(str).unique().tolist())
            unknown = sorted(v for v in vals if v not in allowed)
            if unknown:
                raise ValueError(
                    f"{stage}: unknown values in primary column '{pv.column_name}': {unknown[:10]}"
                )

    def _check_mode_values(self, df: pd.DataFrame, *, stage: str) -> None:
        for sv in self.secondary_variables:
            if isinstance(sv, DistributionSecondaryVariable):
                col = sv.mode_column()
                if col not in df.columns:
                    continue
                valid = set(sv.keys())
                series = df[col]
                non_null = series[~series.isna()].astype(str)
                invalid = sorted(set(non_null.unique()) - valid)
                if invalid:
                    raise ValueError(
                        f"{stage}: invalid mode values in '{col}'. Must be csv_dict keys. "
                        f"Examples: {invalid[:10]}"
                    )

    def _check_qualitative_scalar_values(self, df: pd.DataFrame, *, stage: str) -> None:
        for sv in self.secondary_variables:
            if not isinstance(sv, QualitativeScalarSecondaryVariable):
                continue
            col = sv.value_column
            if col not in df.columns:
                continue
            valid = set(sv.keys())
            non_null = df[col][~df[col].isna()].astype(str)
            invalid = sorted(set(non_null.unique()) - valid)
            if invalid:
                raise ValueError(
                    f"{stage}: invalid values in qualitative scalar column '{col}'. "
                    f"Must be one of csv_dict keys. Examples: {invalid[:10]}"
                )

    def _check_complete_grid(self, df: pd.DataFrame, *, stage: str) -> None:
        if not self.strict_complete_grid:
            return
        expected = self.expected_row_count()
        actual = len(df)
        if actual != expected:
            raise ValueError(f"{stage}: row count {actual} != expected strict grid size {expected}")

    def checkRawCSV(self, df: pd.DataFrame) -> None:
        """
        Raw check:
        - primary columns exist
        - scalar value columns exist
        - distribution count columns exist
        - no duplicate primary-key rows
        - generated columns (percent/totals/averages) optional
        """
        self._check_required_columns(df, self.expected_raw_columns(), stage="checkRawCSV")
        self._check_no_duplicate_primary_rows(df, stage="checkRawCSV")
        self._check_primary_values_are_known(df, stage="checkRawCSV", final=False)

    def checkCSV(self, df: pd.DataFrame) -> None:
        """
        Final strict check (post-normalization + generation):
        - all required final columns exist
        - no duplicate primary-key rows
        - mode values valid for distribution variables
        - strict row count if enabled
        """
        self._check_required_columns(df, self.expected_final_columns(), stage="checkCSV")
        self._check_no_duplicate_primary_rows(df, stage="checkCSV")
        self._check_primary_values_are_known(df, stage="checkCSV", final=True)
        self._check_qualitative_scalar_values(df, stage="checkCSV")
        self._check_mode_values(df, stage="checkCSV")
        self._check_complete_grid(df, stage="checkCSV")

    def generateTotals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Upsert total rows/values.

        Rules:
        - Distribution variables: sum count columns.
        - Quantitative scalar variables: mean of source values.
        - Qualitative scalar variables: mode of source values (stored csv key).
        - Uses available source values (partial totals). If no usable source values exist,
          generated total remains NaN.

        This function only computes total rows (rows where at least one primary value is "Total").
        Percentages and distribution summaries are handled by `generatePercentages` and
        `generateAverages`.
        """
        out = df.copy()
        for col in self.expected_final_columns():
            if col not in out.columns:
                out[col] = pd.NA
        for col in self.primary_column_names():
            if col in out.columns:
                out[col] = out[col].astype(str)

        pcols = self.primary_column_names()

        # Ensure all expected total rows exist (upsert behavior).
        existing_keys = {
            tuple(str(v) for v in row)
            for row in out[pcols].itertuples(index=False, name=None)
        }
        rows_to_add: list[dict[str, object]] = []
        pv_values = [pv.expected_values_for_final() for pv in self.primary_variables]
        for combo in product(*pv_values):
            combo_t = tuple(str(v) for v in combo)
            if not any(v == TOTAL_VALUE for v in combo_t):
                continue
            if combo_t in existing_keys:
                continue
            rec = {col: val for col, val in zip(pcols, combo_t)}
            for col in out.columns:
                if col not in rec:
                    rec[col] = pd.NA
            rows_to_add.append(rec)

        if rows_to_add:
            add_df = pd.DataFrame(rows_to_add, columns=out.columns)
            out = pd.concat([out, add_df], ignore_index=True, sort=False)

        def _source_mask_for_target(target_row: pd.Series) -> pd.Series:
            mask = pd.Series(True, index=out.index)
            for pv in self.primary_variables:
                tval = str(target_row[pv.column_name])
                if tval == TOTAL_VALUE:
                    mask &= out[pv.column_name].astype(str).ne(TOTAL_VALUE)
                else:
                    mask &= out[pv.column_name].astype(str).eq(tval)
            return mask

        def _has_blank_or_na(series: pd.Series) -> pd.Series:
            return series.map(lambda v: pd.isna(v) or (isinstance(v, str) and v.strip() == ""))

        total_row_mask = pd.Series(False, index=out.index)
        for col in pcols:
            total_row_mask |= out[col].astype(str).eq(TOTAL_VALUE)

        total_indices = out.index[total_row_mask].tolist()

        for idx in total_indices:
            target_row = out.loc[idx]
            source_mask = _source_mask_for_target(target_row)
            source = out.loc[source_mask]

            for sv in self.secondary_variables:
                if isinstance(sv, DistributionSecondaryVariable):
                    count_cols = sv.count_columns()
                    src = source[count_cols].copy()
                    for c in count_cols:
                        src[c] = pd.to_numeric(src[c], errors="coerce")

                    if source.empty:
                        for c in count_cols:
                            out.at[idx, c] = pd.NA
                        continue

                    sums = src.sum(axis=0, min_count=1)
                    for c in count_cols:
                        out.at[idx, c] = sums[c]
                    continue

                if isinstance(sv, QuantitativeScalarSecondaryVariable):
                    col = sv.value_column
                    src = pd.to_numeric(source[col], errors="coerce") if col in source.columns else pd.Series(dtype=float)
                    src = src.dropna()
                    if source.empty or src.empty:
                        out.at[idx, col] = pd.NA
                    else:
                        out.at[idx, col] = src.mean()
                    continue

                if isinstance(sv, QualitativeScalarSecondaryVariable):
                    col = sv.value_column
                    if col not in source.columns or source.empty:
                        out.at[idx, col] = pd.NA
                        continue
                    src = source[col]
                    src = src[~_has_blank_or_na(src)]
                    if src.empty:
                        out.at[idx, col] = pd.NA
                        continue

                    counts = src.astype(str).value_counts()
                    if counts.empty:
                        out.at[idx, col] = pd.NA
                        continue
                    max_count = counts.max()
                    tied = set(counts[counts == max_count].index.tolist())
                    # Deterministic tie-break using schema key order.
                    chosen = next((k for k in sv.keys() if k in tied), None)
                    out.at[idx, col] = chosen if chosen is not None else sorted(tied)[0]
                    continue

                if isinstance(sv, ScalarSecondaryVariable):
                    # Unknown scalar subtype: leave unchanged.
                    continue

        return out

    def generatePercentages(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Upsert percentages for distribution variables when count columns exist and data is defined.
        Leaves NaN where counts are missing.
        """
        out = df.copy()
        for sv in self.secondary_variables:
            if not isinstance(sv, DistributionSecondaryVariable):
                continue
            count_cols = sv.count_columns()
            pct_cols = sv.percent_columns()
            for c in count_cols:
                if c not in out.columns:
                    out[c] = pd.NA
            for p in pct_cols:
                if p not in out.columns:
                    out[p] = pd.NA
            counts = out[count_cols].apply(pd.to_numeric, errors="coerce")
            totals = counts.sum(axis=1, min_count=1)
            for csv_value in sv.keys():
                ccol = sv.count_column(csv_value)
                pcol = sv.percent_column(csv_value)
                out[pcol] = counts[ccol].div(totals.where(totals != 0, pd.NA)) * 100.0
        return out

    def generateAverages(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Upsert mean/median/mode for quantitative distributions and mode for qualitative distributions.
        NaNs are preserved when insufficient count data exists.
        """
        out = df.copy()

        for sv in self.secondary_variables:
            if isinstance(sv, QualitativeDistributionVariable):
                mcol = sv.mode_column()
                if mcol not in out.columns:
                    out[mcol] = pd.NA
                counts = out[sv.count_columns()].apply(pd.to_numeric, errors="coerce")
                idx = counts.idxmax(axis=1, skipna=True)
                # Store csv key (not display name)
                out[mcol] = idx.str.replace("_count", "", regex=False).where(idx.notna(), pd.NA)

            if isinstance(sv, QuantitativeDistributionVariable):
                for col in [sv.mean_column(), sv.median_column(), sv.mode_column()]:
                    if col not in out.columns:
                        out[col] = pd.NA

                counts = out[sv.count_columns()].apply(pd.to_numeric, errors="coerce")

                # Mode (csv key)
                idx = counts.idxmax(axis=1, skipna=True)
                out[sv.mode_column()] = idx.str.replace("_count", "", regex=False).where(idx.notna(), pd.NA)

                # Mean (weighted by representative values)
                weights = pd.Series(
                    {sv.count_column(k): float(sv.numeric_value_for(k)) for k in sv.keys()}
                )
                weighted_sum = counts.mul(weights, axis=1).sum(axis=1, min_count=1)
                total = counts.sum(axis=1, min_count=1)
                out[sv.mean_column()] = weighted_sum.div(total.where(total != 0, pd.NA))

                # Median (weighted discrete categories by representative ordering)
                ordered_keys = sorted(sv.keys(), key=lambda k: sv.numeric_value_for(k))
                ordered_cols = [sv.count_column(k) for k in ordered_keys]
                ordered_values = [sv.numeric_value_for(k) for k in ordered_keys]
                medians: list[float | None] = []
                ordered_counts = counts[ordered_cols]
                row_totals = ordered_counts.sum(axis=1, min_count=1)
                for i in range(len(out)):
                    rt = row_totals.iloc[i]
                    if pd.isna(rt) or rt <= 0:
                        medians.append(None)
                        continue
                    target = rt / 2.0
                    cum = 0.0
                    chosen = None
                    for col, val in zip(ordered_cols, ordered_values):
                        c = ordered_counts.iloc[i][col]
                        if pd.isna(c):
                            continue
                        cum += float(c)
                        if cum >= target:
                            chosen = float(val)
                            break
                    medians.append(chosen)
                out[sv.median_column()] = medians

            if isinstance(sv, QualitativeScalarSecondaryVariable):
                # No averages to generate; qualitative scalar totals are handled in generateTotals.
                if sv.value_column not in out.columns:
                    out[sv.value_column] = pd.NA

            if isinstance(sv, QuantitativeScalarSecondaryVariable):
                if sv.value_column in out.columns:
                    out[sv.value_column] = pd.to_numeric(out[sv.value_column], errors="coerce")

        return out

    def generateExampleCSV(self, df: pd.DataFrame | None = None) -> pd.DataFrame:
        """
        Generate an example/final-structure DataFrame using the schema grid.
        If `df` is provided, columns from it are preserved where names overlap.
        """
        grid_rows = []
        pv_values = [pv.expected_values_for_final() for pv in self.primary_variables]
        for combo in product(*pv_values):
            row = {pv.column_name: str(v) for pv, v in zip(self.primary_variables, combo)}
            grid_rows.append(row)

        out = pd.DataFrame(grid_rows)
        for col in sorted(self.expected_final_columns() - set(out.columns)):
            out[col] = pd.NA

        if df is not None:
            overlap = [c for c in df.columns if c in out.columns]
            if overlap:
                out = out.merge(df[overlap + self.primary_column_names()].drop_duplicates(), on=self.primary_column_names(), how="left", suffixes=("", "_src"))
                for col in overlap:
                    src_col = f"{col}_src"
                    if src_col in out.columns:
                        out[col] = out[src_col].combine_first(out[col])
                        out = out.drop(columns=[src_col])

        return out

    def normalizeToStrictStructure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create exact primary-key grid and retain existing data values.
        Missing sections remain NaN.
        """
        base = self.generateExampleCSV()
        cols = [c for c in df.columns if c in base.columns]
        if not cols:
            return base
        return base.merge(
            df[cols].drop_duplicates(subset=self.primary_column_names()),
            on=self.primary_column_names(),
            how="left",
            suffixes=("", "_src"),
        )


__all__ = [
    "Schema",
    "PrimaryVariable",
    "TOTAL_VALUE",
    "SecondaryVariable",
    "ScalarSecondaryVariable",
    "QuantitativeScalarSecondaryVariable",
    "QualitativeScalarSecondaryVariable",
    "QualitativeDistributionVariable",
    "QuantitativeDistributionVariable",
]
