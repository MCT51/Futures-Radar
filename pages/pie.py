"""
Pie / breakdown page — distribution charts driven by StructuredData.

Supports multiple datasets via a top-level dropdown. Both SEN Age/Sex and
FSM/Ethnicity are loaded at startup; selecting one updates all the other
dropdowns to match that dataset's schema.

NOTE on the Pie class (Pie/pie.py):
    The existing Pie class creates its own Dash(...) instance internally and
    registers callbacks with @self.app.callback. That pattern does not work
    inside a multi-page app, where only one Dash instance should exist.
    This page inlines the same layout + callback logic but uses the global
    `from dash import callback` decorator instead. If you want to keep using
    the Pie class elsewhere as a standalone app, the cleanest fix is to add
    an optional `app` argument to Pie.__init__ and fall back to self.app only
    when none is supplied.
"""

from __future__ import annotations

import dash
import pandas as pd
import plotly.express as px
from dash import Input, Output, State, callback, dcc, html

from Ingestion.primary_variable import TOTAL_VALUE
from Ingestion.structured_data import StructuredData
from Ingestion.secondary_variable import DistributionSecondaryVariable
from pages._structured_dataset_registry import dataset_options, load_dataset

dash.register_page(__name__, path="/pie", name="Breakdowns")

_DATASET_OPTIONS = dataset_options()

# ── Helpers (mirrors Pie class internals) ────────────────────────────────────

def _distribution_variables(sd: StructuredData) -> list[DistributionSecondaryVariable]:
    return [sv for sv in sd.schema.secondary_variables if isinstance(sv, DistributionSecondaryVariable)]


def _primary_options(sd: StructuredData, idx: int) -> list[dict]:
    pv = sd.schema.primary_variables[idx]
    values = sd.dataframe[pv.column_name].dropna().astype(str).unique().tolist()
    ordered = [v for v in pv.expected_values_for_final() if v in values]
    extras = [v for v in sorted(values) if v not in ordered]
    return [{"label": pv.display_name_for(v), "value": v} for v in ordered + extras]


def _variable_options(sd: StructuredData) -> list[dict]:
    return [{"label": sv.display_name, "value": sv.variable_name} for sv in _distribution_variables(sd)]


def _default_p2(sd: StructuredData) -> str | None:
    if len(sd.schema.primary_variables) < 2:
        return None
    opts = _primary_options(sd, 1)
    if not opts:
        return None
    values = [o["value"] for o in opts]
    return "Total" if "Total" in values else values[0]


def _pie_primary_selection(
    sd: StructuredData,
    p1_value: str,
    p2_value: str,
) -> dict[str, str]:
    selected: dict[str, str] = {}
    primary_variables = sd.schema.primary_variables

    selected[primary_variables[0].column_name] = str(p1_value)
    selected[primary_variables[1].column_name] = str(p2_value)

    for pv in primary_variables[2:]:
        values = set(sd.dataframe[pv.column_name].dropna().astype(str).unique().tolist())
        if TOTAL_VALUE in values:
            selected[pv.column_name] = TOTAL_VALUE
        else:
            ordered_values = _primary_options(sd, primary_variables.index(pv))
            if ordered_values:
                selected[pv.column_name] = str(ordered_values[0]["value"])

    return selected


def _make_pie_frame(sd: StructuredData, p1_value, p2_value, variable_name, metric):
    sv = sd.schema.get_secondary(variable_name)
    if not isinstance(sv, DistributionSecondaryVariable):
        return None, f"Variable '{variable_name}' is not a distribution."

    p1_col = sd.schema.primary_variables[0].column_name
    p2_col = sd.schema.primary_variables[1].column_name
    selection = _pie_primary_selection(sd, p1_value, p2_value)
    row = sd.row_for(**selection)

    records = []
    for key in sv.keys():
        col = sv.count_column(key) if metric == "count" else sv.percent_column(key)
        value = row[col] if col in row.index else pd.NA
        records.append({"label": sv.display_for(key), "value": value})

    df = pd.DataFrame(records)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna(subset=["value"]), None


# ── Layout ───────────────────────────────────────────────────────────────────
if not _DATASET_OPTIONS:
    layout = html.Div(
        [
            html.H2("Breakdowns"),
            html.P(
                "No StructuredData JSON files found. "
                "Run the scripts in Ingestion/test/ to generate them.",
                style={"color": "#c0392b"},
            ),
        ],
        style={"padding": "16px"},
    )

else:
    _first = _DATASET_OPTIONS[0]["value"]

    layout = html.Div(
        [
            html.H2("Breakdowns"),
            # Dataset selector
            html.Div(
                [
                    html.Label("Dataset"),
                    html.Div(
                        [
                            dcc.Dropdown(
                                id="pie-dataset",
                                options=_DATASET_OPTIONS,
                                value=_first,
                                clearable=False,
                                style={"width": "420px"},
                            ),
                            html.Button("Refresh datasets", id="pie-refresh-datasets", style={"marginLeft": "12px"}),
                        ],
                        style={"display": "flex", "alignItems": "center"},
                    ),
                ],
                style={"marginBottom": "16px"},
            ),
            # Primary + variable dropdowns
            html.Div(
                [
                    html.Div(
                        [html.Label(id="pie-p1-label"), dcc.Dropdown(id="pie-p1", clearable=False)],
                        style={"width": "220px"},
                    ),
                    html.Div(
                        [html.Label("Variable"), dcc.Dropdown(id="pie-variable", clearable=False)],
                        style={"width": "260px"},
                    ),
                    html.Div(
                        [html.Label(id="pie-p2-label"), dcc.Dropdown(id="pie-p2", clearable=False)],
                        style={"flex": "1", "minWidth": "300px"},
                    ),
                ],
                style={
                    "display": "flex",
                    "gap": "16px",
                    "marginBottom": "16px",
                    "alignItems": "flex-end",
                },
            ),
            dcc.Graph(id="pie-chart", style={"height": "70vh"}),
            html.Div(id="pie-note", style={"fontSize": "0.9rem", "color": "#555", "marginTop": "8px"}),
        ],
        style={"padding": "16px"},
    )

    # ── Callback 1: dataset change → update all dropdowns ───────────────────
    @callback(
        Output("pie-dataset", "options"),
        Output("pie-dataset", "value"),
        Input("pie-refresh-datasets", "n_clicks"),
        State("pie-dataset", "value"),
    )
    def refresh_dataset_options(_, current_value):
        load_dataset.cache_clear()
        options = dataset_options()
        option_values = {opt["value"] for opt in options}
        value = current_value if current_value in option_values else (options[0]["value"] if options else None)
        return options, value

    @callback(
        Output("pie-p1-label", "children"),
        Output("pie-p1", "options"),
        Output("pie-p1", "value"),
        Output("pie-p2-label", "children"),
        Output("pie-p2", "options"),
        Output("pie-p2", "value"),
        Output("pie-variable", "options"),
        Output("pie-variable", "value"),
        Input("pie-dataset", "value"),
    )
    def update_dropdowns(dataset_name):
        sd = load_dataset(dataset_name) if dataset_name else None
        if sd is None:
            empty = []
            return "Year", empty, None, "Location", empty, None, empty, None
        if len(sd.schema.primary_variables) < 2:
            empty = []
            return "Primary 1", empty, None, "Primary 2", empty, None, _variable_options(sd), None

        p1_opts = _primary_options(sd, 0)
        p2_opts = _primary_options(sd, 1)
        var_opts = _variable_options(sd)

        p1_label = sd.schema.primary_variables[0].title
        p2_label = sd.schema.primary_variables[1].title

        p1_val = p1_opts[-1]["value"] if p1_opts else None
        p2_val = _default_p2(sd)
        var_val = var_opts[0]["value"] if var_opts else None

        return p1_label, p1_opts, p1_val, p2_label, p2_opts, p2_val, var_opts, var_val

    # ── Callback 2: any dropdown change → update chart ──────────────────────
    @callback(
        Output("pie-chart", "figure"),
        Output("pie-note", "children"),
        Input("pie-dataset", "value"),
        Input("pie-p1", "value"),
        Input("pie-variable", "value"),
        Input("pie-p2", "value"),
    )
    def update_chart(dataset_name, p1_value, variable_name, p2_value):
        if not all([dataset_name, p1_value, variable_name, p2_value]):
            return px.pie(title="Select all options above"), ""

        sd = load_dataset(dataset_name) if dataset_name else None
        if sd is None:
            return px.pie(title="Dataset not loaded"), "Dataset unavailable."
        if len(sd.schema.primary_variables) < 2:
            return px.pie(title="Dataset requires at least two primary variables"), "Pie view needs at least two primary variables."

        try:
            chart_df, err = _make_pie_frame(sd, p1_value, p2_value, variable_name, metric="percent")
        except (KeyError, ValueError) as exc:
            return px.pie(title=str(exc)), str(exc)

        if err:
            return px.pie(title=err), err
        if chart_df is None or chart_df.empty:
            return px.pie(title="No data"), "No data for the selected options."

        sv = sd.schema.get_secondary(variable_name)
        p1_display = sd.schema.primary_variables[0].display_name_for(str(p1_value))
        p2_display = sd.schema.primary_variables[1].display_name_for(str(p2_value))

        fig = px.pie(
            chart_df,
            names="label",
            values="value",
            title=f"{sv.display_name} — {p1_display}, {p2_display}",
            hole=0.25,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(margin=dict(l=20, r=20, t=60, b=20))

        hidden_totals = [
            pv.title
            for pv in sd.schema.primary_variables[2:]
            if TOTAL_VALUE in set(sd.dataframe[pv.column_name].dropna().astype(str).unique().tolist())
        ]
        if hidden_totals:
            note = f"Showing percentages. Hidden primaries fixed at Total: {', '.join(hidden_totals)}."
        else:
            note = f"Showing percentages. Dataset: {dataset_name}."
        return fig, note
