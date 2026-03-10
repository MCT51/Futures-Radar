"""
Bar chart page — primary variable on x-axis, selected secondary on y-axis.

Supports two y-axis modes:
  - Scalar metric (including quantitative distribution to-scalar mean/median)
  - Distribution category metric (choose distribution, category, then count/percent)
"""

from __future__ import annotations

from pathlib import Path

import dash
import pandas as pd
import plotly.express as px
from dash import Input, Output, callback, dcc, html

from Ingestion.primary_variable import TOTAL_VALUE
from Ingestion.structured_data import StructuredData
from Ingestion.secondary_variable import (
    DistributionSecondaryVariable,
    QuantitativeDistributionVariable,
    QuantitativeScalarSecondaryVariable,
)

dash.register_page(__name__, path="/bar", name="Bar")

# ── Load datasets ────────────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parents[1]
_DATASET_PATHS: dict[str, Path] = {
    "SEN — Age & Sex": _REPO_ROOT / "Ingestion/test/output/sen_age_sex_structured.json",
    "FSM — Ethnicity": _REPO_ROOT / "Ingestion/test/output/fsm_ethnicity_structured.json",
}

_datasets: dict[str, StructuredData] = {}
for _name, _path in _DATASET_PATHS.items():
    if _path.exists():
        _datasets[_name] = StructuredData.load(_path)


# ── Helpers ──────────────────────────────────────────────────────────────────
def _primary_value_options(sd: StructuredData, column_name: str, *, include_total: bool = True) -> list[dict]:
    pv = next((p for p in sd.schema.primary_variables if p.column_name == column_name), None)
    if pv is None:
        return []

    values = sd.dataframe[column_name].dropna().astype(str).unique().tolist()
    ordered = [v for v in pv.expected_values_for_final() if v in values]
    extras = [v for v in sorted(values) if v not in ordered]
    full = ordered + extras

    if not include_total:
        full = [v for v in full if v != TOTAL_VALUE]

    return [{"label": pv.display_name_for(v), "value": v} for v in full]


def _numeric_secondary_options(sd: StructuredData) -> list[dict]:
    options: list[dict] = []

    for sv in sd.schema.secondary_variables:
        if isinstance(sv, QuantitativeScalarSecondaryVariable):
            options.append(
                {
                    "label": sv.display_name,
                    "value": sv.value_column,
                }
            )
            continue

        if isinstance(sv, QuantitativeDistributionVariable):
            mean_sv = sv.to_scalar_mean()
            median_sv = sv.to_scalar_median()
            options.append({"label": mean_sv.display_name, "value": mean_sv.value_column})
            options.append({"label": median_sv.display_name, "value": median_sv.value_column})

    return options


def _distribution_variable_options(sd: StructuredData) -> list[dict]:
    options: list[dict] = []
    for sv in sd.schema.secondary_variables:
        if isinstance(sv, DistributionSecondaryVariable):
            options.append({"label": sv.display_name, "value": sv.variable_name})
    return options


def _distribution_category_options(sd: StructuredData, variable_name: str | None) -> list[dict]:
    if not variable_name:
        return []
    try:
        sv = sd.schema.get_secondary(variable_name)
    except KeyError:
        return []
    if not isinstance(sv, DistributionSecondaryVariable):
        return []
    return [{"label": sv.display_for(k), "value": k} for k in sv.keys()]


def _context_primary_column(sd: StructuredData, x_primary_column: str | None) -> str | None:
    other = [pv.column_name for pv in sd.schema.primary_variables if pv.column_name != x_primary_column]
    return other[0] if other else None


# ── Layout ───────────────────────────────────────────────────────────────────
if not _datasets:
    layout = html.Div(
        [
            html.H2("Bar"),
            html.P(
                "No StructuredData JSON files found. "
                "Run the scripts in Ingestion/test/ to generate them.",
                style={"color": "#c0392b"},
            ),
        ],
        style={"padding": "16px"},
    )

else:
    _first = next(iter(_datasets))

    layout = html.Div(
        [
            html.H2("Primary vs Scalar"),
            html.Div(
                [
                    html.Label("Dataset"),
                    dcc.Dropdown(
                        id="bar-dataset",
                        options=[{"label": k, "value": k} for k in _datasets],
                        value=_first,
                        clearable=False,
                        style={"width": "280px"},
                    ),
                ],
                style={"marginBottom": "16px"},
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.Label("X-axis Primary"),
                            dcc.Dropdown(id="bar-x-primary", clearable=False),
                        ],
                        style={"width": "240px"},
                    ),
                    html.Div(
                        [
                            html.Label(id="bar-context-label"),
                            dcc.Dropdown(id="bar-context", clearable=False),
                        ],
                        style={"width": "260px"},
                    ),
                    html.Div(
                        [
                            html.Label("Y-axis Scalar Secondary"),
                            dcc.Dropdown(id="bar-y-secondary", clearable=False),
                        ],
                        style={"flex": "1", "minWidth": "320px"},
                    ),
                ],
                style={"display": "flex", "gap": "16px", "alignItems": "flex-end", "marginBottom": "16px"},
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.Label("Y-axis Mode"),
                            dcc.Dropdown(
                                id="bar-y-mode",
                                options=[
                                    {"label": "Scalar", "value": "scalar"},
                                    {"label": "Distribution Category", "value": "distribution"},
                                ],
                                value="scalar",
                                clearable=False,
                            ),
                        ],
                        style={"width": "240px"},
                    ),
                    html.Div(
                        [
                            html.Label("Distribution Variable"),
                            dcc.Dropdown(id="bar-dist-variable", clearable=False),
                        ],
                        style={"minWidth": "280px", "flex": "1"},
                    ),
                    html.Div(
                        [
                            html.Label("Category"),
                            dcc.Dropdown(id="bar-dist-category", clearable=False),
                        ],
                        style={"minWidth": "220px", "flex": "1"},
                    ),
                    html.Div(
                        [
                            html.Label("Metric"),
                            dcc.Dropdown(
                                id="bar-dist-metric",
                                options=[
                                    {"label": "Count", "value": "count"},
                                    {"label": "Percent", "value": "percent"},
                                ],
                                value="percent",
                                clearable=False,
                            ),
                        ],
                        style={"width": "180px"},
                    ),
                ],
                style={"display": "flex", "gap": "16px", "alignItems": "flex-end", "marginBottom": "16px"},
            ),
            dcc.Graph(id="bar-chart", style={"height": "72vh"}),
            html.Div(id="bar-note", style={"fontSize": "0.9rem", "color": "#555", "marginTop": "8px"}),
        ],
        style={"padding": "16px"},
    )

    # ── Callback 1: dataset/x selection drives control options ─────────────
    @callback(
        Output("bar-x-primary", "options"),
        Output("bar-x-primary", "value"),
        Output("bar-context-label", "children"),
        Output("bar-context", "options"),
        Output("bar-context", "value"),
        Output("bar-y-secondary", "options"),
        Output("bar-y-secondary", "value"),
        Output("bar-dist-variable", "options"),
        Output("bar-dist-variable", "value"),
        Output("bar-dist-variable", "disabled"),
        Output("bar-dist-category", "options"),
        Output("bar-dist-category", "value"),
        Output("bar-dist-category", "disabled"),
        Output("bar-dist-metric", "value"),
        Output("bar-dist-metric", "disabled"),
        Input("bar-dataset", "value"),
        Input("bar-x-primary", "value"),
        Input("bar-context", "value"),
        Input("bar-y-secondary", "value"),
        Input("bar-y-mode", "value"),
        Input("bar-dist-variable", "value"),
        Input("bar-dist-category", "value"),
        Input("bar-dist-metric", "value"),
    )
    def update_controls(
        dataset_name,
        x_primary_value,
        context_value,
        y_secondary_value,
        y_mode,
        dist_variable_value,
        dist_category_value,
        dist_metric_value,
    ):
        sd = _datasets.get(dataset_name)
        if sd is None:
            return [], None, "Context", [], None, [], None, [], None, True, [], None, True, "percent", True

        x_opts = [
            {"label": pv.title, "value": pv.column_name}
            for pv in sd.schema.primary_variables
        ]
        x_allowed = {o["value"] for o in x_opts}
        x_val = x_primary_value if x_primary_value in x_allowed else (x_opts[0]["value"] if x_opts else None)

        context_col = _context_primary_column(sd, x_val)
        if context_col is None:
            context_label = "Context"
            context_opts = []
            context_val = None
        else:
            context_pv = next(p for p in sd.schema.primary_variables if p.column_name == context_col)
            context_label = context_pv.title
            context_opts = _primary_value_options(sd, context_col, include_total=True)
            context_allowed = {o["value"] for o in context_opts}
            if context_value in context_allowed:
                context_val = context_value
            elif TOTAL_VALUE in context_allowed:
                context_val = TOTAL_VALUE
            else:
                context_val = context_opts[0]["value"] if context_opts else None

        y_opts = _numeric_secondary_options(sd)
        y_allowed = {o["value"] for o in y_opts}
        y_val = y_secondary_value if y_secondary_value in y_allowed else (y_opts[0]["value"] if y_opts else None)

        dist_var_opts = _distribution_variable_options(sd)
        dist_var_allowed = {o["value"] for o in dist_var_opts}
        dist_var_val = (
            dist_variable_value
            if dist_variable_value in dist_var_allowed
            else (dist_var_opts[0]["value"] if dist_var_opts else None)
        )

        dist_cat_opts = _distribution_category_options(sd, dist_var_val)
        dist_cat_allowed = {o["value"] for o in dist_cat_opts}
        dist_cat_val = (
            dist_category_value
            if dist_category_value in dist_cat_allowed
            else (dist_cat_opts[0]["value"] if dist_cat_opts else None)
        )

        dist_metric_val = dist_metric_value if dist_metric_value in {"count", "percent"} else "percent"

        use_dist = y_mode == "distribution"
        dist_disabled = not use_dist
        return (
            x_opts,
            x_val,
            context_label,
            context_opts,
            context_val,
            y_opts,
            y_val,
            dist_var_opts,
            dist_var_val,
            dist_disabled,
            dist_cat_opts,
            dist_cat_val,
            dist_disabled,
            dist_metric_val,
            dist_disabled,
        )

    # ── Callback 2: controls -> chart ───────────────────────────────────────
    @callback(
        Output("bar-chart", "figure"),
        Output("bar-note", "children"),
        Input("bar-dataset", "value"),
        Input("bar-x-primary", "value"),
        Input("bar-context", "value"),
        Input("bar-y-mode", "value"),
        Input("bar-y-secondary", "value"),
        Input("bar-dist-variable", "value"),
        Input("bar-dist-category", "value"),
        Input("bar-dist-metric", "value"),
    )
    def update_chart(
        dataset_name,
        x_primary_col,
        context_value,
        y_mode,
        y_column,
        dist_variable_name,
        dist_category_key,
        dist_metric,
    ):
        if not dataset_name:
            return px.bar(title="Select a dataset"), ""

        sd = _datasets.get(dataset_name)
        if sd is None:
            return px.bar(title="Dataset not loaded"), "Dataset unavailable."

        if not x_primary_col:
            return px.bar(title="Select an x-axis primary variable"), ""

        use_dist = y_mode == "distribution"
        if use_dist:
            if not dist_variable_name:
                return px.bar(title="Select a distribution variable"), ""
            try:
                sv = sd.schema.get_secondary(dist_variable_name)
            except KeyError:
                return px.bar(title=f"Unknown distribution variable: {dist_variable_name}"), ""
            if not isinstance(sv, DistributionSecondaryVariable):
                return px.bar(title=f"Not a distribution variable: {dist_variable_name}"), ""
            if not dist_category_key:
                return px.bar(title="Select a category"), ""
            y_column = sv.count_column(dist_category_key) if dist_metric == "count" else sv.percent_column(dist_category_key)
            metric_label = f"{sv.display_name} - {sv.display_for(dist_category_key)} ({'Count' if dist_metric == 'count' else 'Percent'})"
        else:
            if not y_column:
                return (
                    px.bar(title="No numeric scalar secondary available for this dataset"),
                    "Only numeric scalar variables can be shown on the y-axis.",
                )
            metric_label = next(
                (o["label"] for o in _numeric_secondary_options(sd) if o["value"] == y_column),
                y_column,
            )

        if y_column not in sd.dataframe.columns:
            return px.bar(title=f"Column not found: {y_column}"), f"Missing column '{y_column}'."

        context_col = _context_primary_column(sd, x_primary_col)
        d = sd.dataframe.copy()
        if context_col is not None and context_value is not None:
            d = d[d[context_col].astype(str).eq(str(context_value))].copy()

        d = d[d[x_primary_col].astype(str).ne(TOTAL_VALUE)].copy()
        d[y_column] = pd.to_numeric(d[y_column], errors="coerce")
        d = d.dropna(subset=[y_column])

        if d.empty:
            return px.bar(title="No data for selected controls"), "No non-empty numeric values found."

        x_pv = next((p for p in sd.schema.primary_variables if p.column_name == x_primary_col), None)
        if x_pv is None:
            return px.bar(title=f"Unknown primary variable: {x_primary_col}"), ""

        d["x_label"] = d[x_primary_col].astype(str).map(x_pv.display_name_for)

        x_order = [o["label"] for o in _primary_value_options(sd, x_primary_col, include_total=False)]
        x_order = [v for v in x_order if v in set(d["x_label"].tolist())]

        title = f"{metric_label} by {x_pv.title}"
        if context_col is not None:
            context_pv = next((p for p in sd.schema.primary_variables if p.column_name == context_col), None)
            if context_pv is not None and context_value is not None:
                title += f" ({context_pv.title}: {context_pv.display_name_for(str(context_value))})"

        fig = px.bar(
            d,
            x="x_label",
            y=y_column,
            category_orders={"x_label": x_order},
            labels={"x_label": x_pv.title, y_column: metric_label},
            title=title,
        )
        fig.update_layout(margin=dict(l=30, r=20, t=70, b=40))

        note = (
            "Use Scalar mode for numeric scalar metrics (including quantitative distribution mean/median), "
            "or Distribution Category mode for category-specific counts/percentages."
        )
        return fig, note
