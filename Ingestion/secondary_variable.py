from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import re
from typing import Any


def _slugify(value: str) -> str:
    text = value.strip().lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


@dataclass(frozen=True)
class SecondaryVariable(ABC):
    """
    Abstract base class for all secondary variables.

    `csv_dict` maps csv_value -> display_name.
    """

    display_name: str
    csv_dict: dict[str, str]
    variable_name: str | None = None

    def __post_init__(self) -> None:
        if not self.csv_dict:
            raise ValueError("csv_dict must not be empty")
        if self.variable_name is None:
            object.__setattr__(self, "variable_name", _slugify(self.display_name))

    @property
    @abstractmethod
    def variable_type(self) -> str:
        raise NotImplementedError

    def keys(self) -> list[str]:
        return list(self.csv_dict.keys())

    def display_for(self, csv_value: str) -> str:
        return self.csv_dict.get(csv_value, csv_value)

    def mean_column(self) -> str:
        return f"{self.variable_name}_mean"

    def median_column(self) -> str:
        return f"{self.variable_name}_median"

    def mode_column(self) -> str:
        return f"{self.variable_name}_mode"

    @abstractmethod
    def required_raw_columns(self) -> set[str]:
        raise NotImplementedError

    @abstractmethod
    def required_final_columns(self) -> set[str]:
        raise NotImplementedError

    def optional_generated_columns(self) -> set[str]:
        return set()

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "class": self.__class__.__name__,
            "display_name": self.display_name,
            "csv_dict": dict(self.csv_dict),
        }
        if self.variable_name is not None:
            data["variable_name"] = self.variable_name
        return data


@dataclass(frozen=True)
class ScalarSecondaryVariable(SecondaryVariable, ABC):
    """Base class for scalar variables (one value column per row)."""

    def __post_init__(self) -> None:
        super().__post_init__()
        if len(self.csv_dict) != 1:
            raise ValueError("ScalarSecondaryVariable must have exactly one csv_dict entry")

    @property
    def value_column(self) -> str:
        return next(iter(self.csv_dict.keys()))

    def required_raw_columns(self) -> set[str]:
        return {self.value_column}

    def required_final_columns(self) -> set[str]:
        return {self.value_column}


@dataclass(frozen=True)
class QuantitativeScalarSecondaryVariable(ScalarSecondaryVariable):
    aggregation: str = "sum"

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.aggregation not in {"sum", "mean"}:
            raise ValueError("QuantitativeScalarSecondaryVariable aggregation must be 'sum' or 'mean'")

    @property
    def variable_type(self) -> str:
        return "quantitative_scalar"

    def to_dict(self) -> dict[str, Any]:
        data = super().to_dict()
        data["aggregation"] = self.aggregation
        return data


@dataclass(frozen=True)
class QualitativeScalarSecondaryVariable(ScalarSecondaryVariable):
    """
    Qualitative scalar: one scalar column whose values must be in csv_dict keys.

    Unlike quantitative scalars, `csv_dict` here maps allowed stored values -> display names.
    """

    value_column_name: str = ""

    def __post_init__(self) -> None:
        SecondaryVariable.__post_init__(self)
        if not self.value_column_name:
            raise ValueError("QualitativeScalarSecondaryVariable requires value_column_name")

    @property
    def variable_type(self) -> str:
        return "qualitative_scalar"

    @property
    def value_column(self) -> str:
        return self.value_column_name

    def required_raw_columns(self) -> set[str]:
        return {self.value_column}

    def required_final_columns(self) -> set[str]:
        return {self.value_column}

    def to_dict(self) -> dict[str, Any]:
        data = super().to_dict()
        data["value_column_name"] = self.value_column_name
        return data


@dataclass(frozen=True)
class DistributionSecondaryVariable(SecondaryVariable, ABC):
    """
    Base class for qualitative/quantitative distributions.
    """

    def count_column(self, csv_value: str) -> str:
        return f"{csv_value}_count"

    def percent_column(self, csv_value: str) -> str:
        return f"{csv_value}_percent"

    def count_columns(self) -> list[str]:
        return [self.count_column(k) for k in self.keys()]

    def percent_columns(self) -> list[str]:
        return [self.percent_column(k) for k in self.keys()]

    def required_raw_columns(self) -> set[str]:
        # Raw CSV requires count columns only. Generated fields optional at raw stage.
        return set(self.count_columns())

    def required_final_columns(self) -> set[str]:
        return set(self.count_columns()) | set(self.percent_columns()) | self.summary_columns()

    @abstractmethod
    def summary_columns(self) -> set[str]:
        raise NotImplementedError

    def optional_generated_columns(self) -> set[str]:
        return set(self.percent_columns()) | self.summary_columns()

    def to_scalar_mode(self) -> QualitativeScalarSecondaryVariable:
        # Mode stores the csv key, not display name.
        return QualitativeScalarSecondaryVariable(
            display_name=f"{self.display_name} Mode",
            csv_dict=dict(self.csv_dict),
            variable_name=f"{self.variable_name}_mode_scalar",
            value_column_name=self.mode_column(),
        )


@dataclass(frozen=True)
class QualitativeDistributionVariable(DistributionSecondaryVariable):
    @property
    def variable_type(self) -> str:
        return "qualitative_dist"

    def summary_columns(self) -> set[str]:
        return {self.mode_column()}


@dataclass(frozen=True)
class QuantitativeDistributionVariable(DistributionSecondaryVariable):
    """
    Quantitative distribution with numeric mapping for categories.

    `csv_to_number` maps csv values to representative numeric values (e.g. bucket midpoint).
    """

    csv_to_number: dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__post_init__()
        missing = [k for k in self.keys() if k not in self.csv_to_number]
        if missing:
            raise ValueError(f"csv_to_number missing keys: {missing}")

    @property
    def variable_type(self) -> str:
        return "quantitative_dist"

    def summary_columns(self) -> set[str]:
        return {self.mean_column(), self.median_column(), self.mode_column()}

    def numeric_value_for(self, csv_value: str) -> float:
        return float(self.csv_to_number[csv_value])

    def to_scalar_mean(self) -> QuantitativeScalarSecondaryVariable:
        return QuantitativeScalarSecondaryVariable(
            display_name=f"{self.display_name} Mean",
            csv_dict={self.mean_column(): f"{self.display_name} Mean"},
            variable_name=f"{self.variable_name}_mean_scalar",
        )

    def to_scalar_median(self) -> QuantitativeScalarSecondaryVariable:
        return QuantitativeScalarSecondaryVariable(
            display_name=f"{self.display_name} Median",
            csv_dict={self.median_column(): f"{self.display_name} Median"},
            variable_name=f"{self.variable_name}_median_scalar",
        )

    def to_dict(self) -> dict[str, Any]:
        data = super().to_dict()
        data["csv_to_number"] = dict(self.csv_to_number)
        return data


def secondary_variable_from_dict(data: dict[str, Any]) -> SecondaryVariable:
    cls_name = data["class"]
    common = {
        "display_name": data["display_name"],
        "csv_dict": dict(data["csv_dict"]),
    }
    if "variable_name" in data:
        common["variable_name"] = data["variable_name"]

    if cls_name == "QuantitativeScalarSecondaryVariable":
        return QuantitativeScalarSecondaryVariable(
            **common,
            aggregation=data.get("aggregation", "sum"),
        )
    if cls_name == "QualitativeScalarSecondaryVariable":
        return QualitativeScalarSecondaryVariable(
            **common,
            value_column_name=data["value_column_name"],
        )
    if cls_name == "QualitativeDistributionVariable":
        return QualitativeDistributionVariable(**common)
    if cls_name == "QuantitativeDistributionVariable":
        return QuantitativeDistributionVariable(
            **common,
            csv_to_number={k: float(v) for k, v in data["csv_to_number"].items()},
        )

    raise ValueError(f"Unknown secondary variable class: {cls_name}")
