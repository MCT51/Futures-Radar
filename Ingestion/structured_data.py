from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from Ingestion.schema import Schema
    from Ingestion.primary_variable import TOTAL_VALUE
    from Ingestion.secondary_variable import (
        QualitativeDistributionVariable,
        QuantitativeScalarSecondaryVariable,
    )
except ModuleNotFoundError:  # Support running from Ingestion/
    from schema import Schema
    from primary_variable import TOTAL_VALUE
    from secondary_variable import (
        QualitativeDistributionVariable,
        QuantitativeScalarSecondaryVariable,
    )


DefinedMap = dict[tuple[tuple[str, ...], str], bool]


@dataclass
class StructuredData:
    """
    Post-ingestion strict structured dataset.

    Assumes totals/percentages/averages have already been generated (or upserted)
    according to the schema. Missing sections are represented via NaN values and
    `defined_map[(primary_tuple, secondary_variable_name)] -> bool`.
    """

    dataframe: pd.DataFrame
    schema: Schema
    defined_map: DefinedMap = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.dataframe = self.dataframe.copy()
        if not self.defined_map:
            self.defined_map = self._build_defined_map()

    @classmethod
    def from_dataframe(
        cls,
        dataframe: pd.DataFrame,
        schema: Schema,
        *,
        validate_final: bool = True,
    ) -> "StructuredData":
        if validate_final:
            schema.checkCSV(dataframe)
        return cls(dataframe=dataframe, schema=schema)

    @classmethod
    def from_csv(
        cls,
        path: str | Path,
        schema: Schema,
        *,
        validate_final: bool = True,
        **read_csv_kwargs: Any,
    ) -> "StructuredData":
        df = pd.read_csv(path, **read_csv_kwargs)
        return cls.from_dataframe(df, schema, validate_final=validate_final)

    def to_csv(self, path: str | Path, *, index: bool = False) -> None:
        out_path = Path(path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        self.dataframe.to_csv(out_path, index=index)

    def to_dict(self, *, csv_path: str | None = None, include_schema: bool = True) -> dict[str, Any]:
        data: dict[str, Any] = {
            "csv_path": csv_path,
            "defined_map": [
                {
                    "primary_values": list(primary_tuple),
                    "secondary_variable_name": secondary_name,
                    "defined": bool(is_defined),
                }
                for (primary_tuple, secondary_name), is_defined in self.defined_map.items()
            ],
        }
        if include_schema:
            data["schema"] = self.schema.to_dict()
        return data

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        *,
        dataframe: pd.DataFrame | None = None,
        csv_path: str | Path | None = None,
        schema: Schema | None = None,
        validate_final: bool = True,
    ) -> "StructuredData":
        if schema is None:
            if "schema" not in data:
                raise KeyError("StructuredData JSON missing schema; pass schema explicitly.")
            schema = Schema.from_dict(data["schema"])

        resolved_csv_path = Path(csv_path) if csv_path is not None else None
        if dataframe is None:
            meta_csv = data.get("csv_path")
            if resolved_csv_path is None and meta_csv:
                resolved_csv_path = Path(meta_csv)
            if resolved_csv_path is None:
                raise ValueError("No dataframe/csv_path provided for StructuredData.from_dict")
            dataframe = pd.read_csv(resolved_csv_path)

        defined_map: DefinedMap = {}
        for item in data.get("defined_map", []):
            key = (tuple(str(v) for v in item["primary_values"]), item["secondary_variable_name"])
            defined_map[key] = bool(item["defined"])

        return cls.from_dataframe(dataframe, schema, validate_final=validate_final).with_defined_map(
            defined_map
        )

    def with_defined_map(self, defined_map: DefinedMap) -> "StructuredData":
        self.defined_map = defined_map
        return self

    def save(
        self,
        *,
        json_path: str | Path,
        csv_path: str | Path,
        csv_index: bool = False,
        store_relative_csv_path: bool = True,
        include_schema: bool = True,
    ) -> None:
        json_path = Path(json_path)
        csv_path = Path(csv_path)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.parent.mkdir(parents=True, exist_ok=True)

        self.to_csv(csv_path, index=csv_index)

        meta_csv_path: str | None
        if store_relative_csv_path:
            try:
                meta_csv_path = str(csv_path.relative_to(json_path.parent))
            except ValueError:
                meta_csv_path = str(csv_path)
        else:
            meta_csv_path = str(csv_path)

        payload = self.to_dict(csv_path=meta_csv_path, include_schema=include_schema)
        json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    @classmethod
    def load(
        cls,
        json_path: str | Path,
        *,
        csv_path: str | Path | None = None,
        schema: Schema | None = None,
        validate_final: bool = True,
    ) -> "StructuredData":
        json_path = Path(json_path)
        payload = json.loads(json_path.read_text(encoding="utf-8"))

        resolved_csv_path: Path | None = None
        if csv_path is not None:
            resolved_csv_path = Path(csv_path)
        elif payload.get("csv_path"):
            resolved_csv_path = (json_path.parent / payload["csv_path"]).resolve()

        return cls.from_dict(
            payload,
            csv_path=resolved_csv_path,
            schema=schema,
            validate_final=validate_final,
        )

    def primary_key_tuple(self, row: pd.Series) -> tuple[str, ...]:
        return tuple(str(row[col]) for col in self.schema.primary_column_names())

    def _build_defined_map(self) -> DefinedMap:
        defined: DefinedMap = {}
        pcols = self.schema.primary_column_names()

        for _, row in self.dataframe.iterrows():
            pk = tuple(str(row[c]) for c in pcols)

            for sv in self.schema.secondary_variables:
                key = (pk, sv.variable_name or sv.display_name)

                # Section defined iff all required final columns for that secondary variable are non-empty.
                required_cols = sv.required_final_columns()
                present_cols = [c for c in required_cols if c in self.dataframe.columns]

                if not present_cols:
                    defined[key] = False
                    continue

                values = row[present_cols]
                # Empty entry policy: NaN or blank string counts as undefined.
                non_blank = values.map(lambda v: not (pd.isna(v) or (isinstance(v, str) and v.strip() == "")))
                defined[key] = bool(non_blank.all())

        return defined

    def is_defined(self, primary_values: dict[str, str], secondary_variable_name: str) -> bool:
        pk = tuple(str(primary_values[col]) for col in self.schema.primary_column_names())
        return self.defined_map.get((pk, secondary_variable_name), False)

    def row_for(self, **primary_values: str) -> pd.Series:
        mask = pd.Series(True, index=self.dataframe.index)
        for col in self.schema.primary_column_names():
            if col not in primary_values:
                raise KeyError(f"Missing primary key value for '{col}'")
            mask &= self.dataframe[col].astype(str).eq(str(primary_values[col]))
        matched = self.dataframe.loc[mask]
        if matched.empty:
            raise KeyError(f"No row found for primary values: {primary_values}")
        if len(matched) > 1:
            raise ValueError(f"Multiple rows found for primary values: {primary_values}")
        return matched.iloc[0]

    def flatten_primary_to_secondary(
        self,
        primary_column_name: str,
        *,
        count_secondary_name: str,
        secondary_display_name: str | None = None,
        secondary_variable_name: str | None = None,
    ) -> "StructuredData":
        """
        Convert one primary variable into a qualitative distribution secondary variable.

        The selected primary dimension is removed from row identity. For each remaining
        primary-key row, a new distribution is created whose categories are the original
        primary values and whose counts are summed from the specified scalar secondary.
        """
        target = next(
            (pv for pv in self.schema.primary_variables if pv.column_name == primary_column_name),
            None,
        )
        if target is None:
            raise KeyError(f"Unknown primary column '{primary_column_name}'")

        remaining_primary = [
            pv for pv in self.schema.primary_variables if pv.column_name != primary_column_name
        ]
        if not remaining_primary:
            raise ValueError("Cannot flatten the only primary variable; at least one must remain.")

        display_name = secondary_display_name or f"{target.title} Distribution"
        moved_secondary = QualitativeDistributionVariable(
            display_name=display_name,
            csv_dict=dict(target.csv_to_display),
            variable_name=secondary_variable_name,
        )
        try:
            count_secondary = self.schema.get_secondary(count_secondary_name)
        except KeyError as exc:
            raise KeyError(f"Unknown scalar secondary '{count_secondary_name}'") from exc
        if not isinstance(count_secondary, QuantitativeScalarSecondaryVariable):
            raise ValueError(
                f"Flatten requires a quantitative scalar count basis; '{count_secondary_name}' is not one."
            )
        count_col = count_secondary.value_column

        # Generate totals/summaries on the full source first, then keep rows where
        # the moved dimension is aggregated at TOTAL.
        base = self.schema.generateTotals(self.dataframe)
        base = self.schema.generatePercentages(base)
        base = self.schema.generateAverages(base)
        base = base[base[primary_column_name].astype(str).eq(TOTAL_VALUE)].copy()
        base = base.drop(columns=[primary_column_name])

        # Populate moved-dimension distribution counts using the explicit scalar count basis.
        source = self.dataframe.copy()
        for pv in self.schema.primary_variables:
            source = source[source[pv.column_name].astype(str).ne(TOTAL_VALUE)]
        source[primary_column_name] = source[primary_column_name].astype(str)
        source[count_col] = pd.to_numeric(source[count_col], errors="coerce")
        for key in moved_secondary.keys():
            col = moved_secondary.count_column(key)
            base[col] = pd.NA

        for idx, row in base.iterrows():
            mask = pd.Series(True, index=source.index)
            for pv in remaining_primary:
                row_val = str(row[pv.column_name])
                if row_val == TOTAL_VALUE:
                    mask &= source[pv.column_name].astype(str).ne(TOTAL_VALUE)
                else:
                    mask &= source[pv.column_name].astype(str).eq(row_val)

            subset = source.loc[mask]
            if subset.empty:
                continue
            counts = (
                subset.groupby(primary_column_name, dropna=False)[count_col]
                .sum(min_count=1)
            )
            for key in moved_secondary.keys():
                value = counts.get(key, 0)
                base.at[idx, moved_secondary.count_column(key)] = value

        new_schema = Schema(
            primary_variables=remaining_primary,
            secondary_variables=[*self.schema.secondary_variables, moved_secondary],
            strict_complete_grid=self.schema.strict_complete_grid,
            allow_empty_entries=self.schema.allow_empty_entries,
            undefined_policy=self.schema.undefined_policy,
        )
        base = new_schema.generatePercentages(base)
        base = new_schema.generateAverages(base)
        return StructuredData.from_dataframe(base, new_schema, validate_final=True)
