from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from Ingestion.schema import Schema
except ModuleNotFoundError:  # Support running from Ingestion/
    from schema import Schema


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
