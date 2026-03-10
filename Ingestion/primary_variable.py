from __future__ import annotations

from dataclasses import dataclass
from typing import Any

TOTAL_VALUE = "Total"


@dataclass(frozen=True)
class PrimaryVariable:
    """Primary row-identity variable (e.g. year, location)."""

    title: str
    column_name: str
    csv_to_display: dict[str, str]

    def __post_init__(self) -> None:
        if not self.csv_to_display:
            raise ValueError("csv_to_display must not be empty")
        if TOTAL_VALUE in self.csv_to_display:
            raise ValueError(
                f"Do not include '{TOTAL_VALUE}' in csv_to_display; totals are handled implicitly."
            )

    def values(self) -> list[str]:
        return list(self.csv_to_display.keys())

    def display_name_for(self, csv_value: str) -> str:
        if csv_value == TOTAL_VALUE:
            return TOTAL_VALUE
        return self.csv_to_display.get(csv_value, csv_value) #default to csv_value

    def expected_values_for_final(self) -> list[str]:
        values = self.values()
        values.append(TOTAL_VALUE)
        return values

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "column_name": self.column_name,
            "csv_to_display": dict(self.csv_to_display),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PrimaryVariable":
        return cls(
            title=data["title"],
            column_name=data["column_name"],
            csv_to_display=dict(data["csv_to_display"]),
        )
