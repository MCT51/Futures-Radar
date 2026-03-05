from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

TOTAL_VALUE = "Total"


@dataclass(frozen=True)
class PrimaryVariable(ABC):
    """Abstract primary row-identity variable (e.g. year, location)."""

    title: str
    column_name: str
    csv_to_display: dict[str, str]
    variable_name: str | None = None

    def __post_init__(self) -> None:
        if not self.csv_to_display:
            raise ValueError("csv_to_display must not be empty")
        if TOTAL_VALUE in self.csv_to_display:
            raise ValueError(
                f"Do not include '{TOTAL_VALUE}' in csv_to_display; totals are handled implicitly."
            )
        if self.variable_name is None:
            object.__setattr__(self, "variable_name", self._slugify(self.title))

    @staticmethod
    def _slugify(value: str) -> str:
        out = "".join(c.lower() if c.isalnum() else "_" for c in value.strip())
        out = "_".join(part for part in out.split("_") if part)
        return out or "primary_variable"

    @property
    @abstractmethod
    def variable_type(self) -> str:
        raise NotImplementedError

    def values(self) -> list[str]:
        return list(self.csv_to_display.keys())

    def display_name_for(self, csv_value: str) -> str:
        if csv_value == TOTAL_VALUE:
            return TOTAL_VALUE
        return self.csv_to_display.get(csv_value, csv_value)

    def expected_values_for_final(self) -> list[str]:
        values = self.values()
        values.append(TOTAL_VALUE)
        return values

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "class": self.__class__.__name__,
            "title": self.title,
            "column_name": self.column_name,
            "csv_to_display": dict(self.csv_to_display),
            "variable_type": self.variable_type,
        }
        if self.variable_name is not None:
            data["variable_name"] = self.variable_name
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PrimaryVariable":
        common = {
            "title": data["title"],
            "column_name": data["column_name"],
            "csv_to_display": dict(data["csv_to_display"]),
        }
        if "variable_name" in data:
            common["variable_name"] = data["variable_name"]

        cls_name = data.get("class")
        vtype = data.get("variable_type")

        if cls_name == "QuantitativePrimaryVariable" or vtype == "quantitative_primary":
            return QuantitativePrimaryVariable(
                **common,
                csv_to_number={k: float(v) for k, v in data.get("csv_to_number", {}).items()},
            )

        # Backwards compatibility: default old payloads to qualitative.
        return QualitativePrimaryVariable(**common)


@dataclass(frozen=True)
class QualitativePrimaryVariable(PrimaryVariable):
    @property
    def variable_type(self) -> str:
        return "qualitative_primary"


@dataclass(frozen=True)
class QuantitativePrimaryVariable(PrimaryVariable):
    csv_to_number: dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.csv_to_number:
            missing = [k for k in self.csv_to_display if k not in self.csv_to_number]
            if missing:
                raise ValueError(f"csv_to_number missing keys: {missing}")

    @property
    def variable_type(self) -> str:
        return "quantitative_primary"

    def to_dict(self) -> dict[str, Any]:
        data = super().to_dict()
        if self.csv_to_number:
            data["csv_to_number"] = dict(self.csv_to_number)
        return data
