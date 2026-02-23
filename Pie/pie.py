from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, dcc, html

try:
    from Ingestion.structured_data import StructuredData
    from Ingestion.secondary_variable import DistributionSecondaryVariable
except ModuleNotFoundError:  # Supports `python3 Pie/demo.py`
    from structured_data import StructuredData
    from secondary_variable import DistributionSecondaryVariable


@dataclass
class Pie:
    """
    Generic pie-chart Dash visualisation driven by an `Ingestion.StructuredData` object.

    Current UI assumes exactly 2 primary variables (e.g. year + location).
    """

    structured_data: StructuredData
    default_metric: str = "percent"  # "count" or "percent"
    title: str = "Pie Chart"

    def __post_init__(self) -> None:
        if self.default_metric == "percentage":
            self.default_metric = "percent"
        if self.default_metric not in {"count", "percent"}:
            raise ValueError("default_metric must be 'count' or 'percent'")

        if len(self.structured_data.schema.primary_variables) != 2:
            raise ValueError("Pie currently requires exactly 2 primary variables (e.g. year and location).")

        self.app = Dash(__name__)
        self._build_layout()
        self._register_callbacks()

    @property
    def schema(self):
        return self.structured_data.schema

    @property
    def dataframe(self) -> pd.DataFrame:
        return self.structured_data.dataframe

    @property
    def primary_1(self):
        return self.schema.primary_variables[0]

    @property
    def primary_2(self):
        return self.schema.primary_variables[1]

    @property
    def distribution_variables(self) -> list[DistributionSecondaryVariable]:
        return [
            sv
            for sv in self.schema.secondary_variables
            if isinstance(sv, DistributionSecondaryVariable)
        ]

    @property
    def variable_options(self) -> list[dict[str, str]]:
        return [{"label": sv.display_name, "value": sv.variable_name} for sv in self.distribution_variables]

    def _values_for_primary(self, primary_var) -> list[str]:
        col = primary_var.column_name
        values = self.dataframe[col].dropna().astype(str).unique().tolist()
        ordered = [v for v in primary_var.expected_values_for_final() if v in values]
        # Include any extra values present (defensive)
        extras = [v for v in sorted(values) if v not in ordered]
        return ordered + extras

    def _primary_options(self, primary_var) -> list[dict[str, str]]:
        return [
            {"label": primary_var.display_name_for(v), "value": v}
            for v in self._values_for_primary(primary_var)
        ]

    def _default_variable(self) -> str | None:
        return self.distribution_variables[0].variable_name if self.distribution_variables else None

    def _default_primary_1_value(self) -> str | None:
        values = self._values_for_primary(self.primary_1)
        if not values:
            return None
        # Prefer latest-like behavior by taking last in schema order.
        return values[-1]

    def _default_primary_2_value(self) -> str | None:
        values = self._values_for_primary(self.primary_2)
        if not values:
            return None
        if "Total" in values:
            return "Total"
        return values[0]

    def _build_layout(self) -> None:
        p1_label = self.primary_1.title
        p2_label = self.primary_2.title

        self.app.layout = html.Div(
            [
                html.H2(self.title),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Label(p1_label),
                                dcc.Dropdown(
                                    id="pie-primary-1-dropdown",
                                    options=self._primary_options(self.primary_1),
                                    value=self._default_primary_1_value(),
                                    clearable=False,
                                ),
                            ],
                            style={"width": "240px"},
                        ),
                        html.Div(
                            [
                                html.Label("Variable"),
                                dcc.Dropdown(
                                    id="pie-variable-dropdown",
                                    options=self.variable_options,
                                    value=self._default_variable(),
                                    clearable=False,
                                ),
                            ],
                            style={"width": "280px"},
                        ),
                        html.Div(
                            [
                                html.Label(p2_label),
                                dcc.Dropdown(
                                    id="pie-primary-2-dropdown",
                                    options=self._primary_options(self.primary_2),
                                    value=self._default_primary_2_value(),
                                    clearable=False,
                                ),
                            ],
                            style={"minWidth": "320px", "flex": "1"},
                        ),
                    ],
                    style={
                        "display": "flex",
                        "gap": "16px",
                        "marginBottom": "16px",
                        "alignItems": "flex-end",
                    },
                ),
                dcc.Graph(id="pie-chart", style={"height": "75vh"}),
                html.Div(id="pie-chart-note", style={"fontSize": "0.9rem", "color": "#444"}),
            ],
            style={"padding": "16px"},
        )

    def _distribution_frame(
        self,
        *,
        row: pd.Series,
        variable: DistributionSecondaryVariable,
        metric: str,
    ) -> pd.DataFrame:
        if metric not in {"count", "percent"}:
            raise ValueError("metric must be 'count' or 'percent'")

        records: list[dict[str, object]] = []
        for key in variable.keys():
            col = variable.count_column(key) if metric == "count" else variable.percent_column(key)
            if col not in row.index:
                value = pd.NA
            else:
                value = row[col]
            records.append(
                {
                    "category_key": key,
                    "label": variable.display_for(key),
                    "value": value,
                    "metric": metric,
                    "variable": variable.variable_name,
                }
            )
        df = pd.DataFrame(records)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df.dropna(subset=["value"])

    def _register_callbacks(self) -> None:
        @self.app.callback(
            Output("pie-chart", "figure"),
            Output("pie-chart-note", "children"),
            Input("pie-primary-1-dropdown", "value"),
            Input("pie-variable-dropdown", "value"),
            Input("pie-primary-2-dropdown", "value"),
        )
        def _update(p1_value: str, variable_name: str, p2_value: str):
            if not p1_value or not variable_name or not p2_value:
                return px.pie(title="Missing selection"), "Select all dropdown values."

            try:
                sv = self.schema.get_secondary(variable_name)
                if not isinstance(sv, DistributionSecondaryVariable):
                    raise ValueError(f"Variable '{variable_name}' is not a distribution.")

                row = self.structured_data.row_for(
                    **{
                        self.primary_1.column_name: str(p1_value),
                        self.primary_2.column_name: str(p2_value),
                    }
                )
                chart_df = self._distribution_frame(row=row, variable=sv, metric=self.default_metric)
            except (KeyError, ValueError) as exc:
                return px.pie(title=str(exc)), str(exc)

            if chart_df.empty:
                is_defined = self.structured_data.is_defined(
                    {
                        self.primary_1.column_name: str(p1_value),
                        self.primary_2.column_name: str(p2_value),
                    },
                    sv.variable_name,
                )
                if not is_defined:
                    return (
                        px.pie(title="No data"),
                        "No plottable data for this section (incomplete source data / undefined section).",
                    )
                return px.pie(title="No data"), "No data for the selected options."

            p1_display = self.primary_1.display_name_for(str(p1_value))
            p2_display = self.primary_2.display_name_for(str(p2_value))
            fig = px.pie(
                chart_df,
                names="label",
                values="value",
                title=f"{sv.display_name} ({p1_display}, {p2_display})",
                hole=0.25,
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(margin=dict(l=20, r=20, t=60, b=20))

            is_defined = self.structured_data.is_defined(
                {
                    self.primary_1.column_name: str(p1_value),
                    self.primary_2.column_name: str(p2_value),
                },
                sv.variable_name,
            )
            metric_label = "counts" if self.default_metric == "count" else "percentages"
            status = "defined" if is_defined else "incomplete source data (partial totals may be shown)"
            note = f"Showing {metric_label}. Section status: {status}."
            return fig, note

    def run(self, **kwargs) -> None:
        defaults = {"debug": True, "host": "127.0.0.1", "port": 8050}
        defaults.update(kwargs)
        self.app.run(**defaults)


def create_app(structured_data: StructuredData, **kwargs) -> Dash:
    return Pie(structured_data=structured_data, **kwargs).app


if __name__ == "__main__":
    raise SystemExit(
        "This module is a reusable visualisation component.\n"
        "Instantiate `Pie(structured_data)` from your app code, then call `.run()`."
    )
