import json

import dash
from dash import dcc, html, Input, Output, State, callback
from pathlib import Path
import pandas as pd

from Ingestion.dataset_builder import build_structured_from_csv, SecondarySpec


dash.register_page(__name__, path="/builder", name="Dataset Builder")

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "Data"            # where users pick raw datasets from
OUT_DIR = ROOT / "Ingestion" / "test" / "output"


def list_csvs():
    if RAW_DIR.exists():
        return sorted(RAW_DIR.rglob("*.csv"))
    return []


layout = html.Div(
    [
        html.H2("Dataset Builder"),
        html.Div(
            [
                html.Label("Raw dataset (CSV)"),
                dcc.Dropdown(
                    id="builder-csv",
                    options=[{"label": p.name, "value": str(p)} for p in list_csvs()],
                    placeholder="Select a CSV...",
                    style={"width": "700px"},
                ),
            ],
            style={"marginBottom": "12px"},
        ),
        html.Div(id="builder-columns"),
        html.Hr(),

        html.H4("1) Primary variables"),
        dcc.Dropdown(
            id="builder-primaries",
            multi=True,
            placeholder="Select primary columns...",
            style={"width": "700px"},
        ),
        html.Div(
            [
                html.Label("Location primary (for display labels)"),
                dcc.Dropdown(
                    id="builder-location-primary",
                    placeholder="Select the location code column (must also be in the primary list)...",
                    style={"width": "340px"},
                ),
                dcc.Dropdown(
                    id="builder-location-display",
                    placeholder="Display column (e.g. la_name)",
                    style={"width": "340px", "marginLeft": "8px"},
                ),
            ],
            style={"marginTop": "8px", "marginBottom": "16px", "display": "flex"},
        ),

        html.H4("2) Secondary variable"),
        html.Div(
            [
                dcc.Input(
                    id="builder-sec-name",
                    placeholder="internal slug (lowercase, no spaces, e.g. fsm_eligibility)",
                    style={"width": "340px"},
                ),
                dcc.Input(
                    id="builder-sec-display",
                    placeholder="display name (e.g. FSM Eligibility)",
                    style={"width": "340px", "marginLeft": "8px"},
                ),
            ],
            style={"marginBottom": "8px"},
        ),
        dcc.Dropdown(
            id="builder-sec-type",
            options=[
                {"label": "Quantitative scalar (single numeric value per row)", "value": "quant_scalar"},
                {"label": "Qualitative scalar (single categorical value per row)", "value": "qual_scalar"},
                {"label": "Qualitative distribution (long-form counts per category)", "value": "qual_dist"},
                {"label": "Quantitative distribution (long-form counts with numeric labels)", "value": "quant_dist"},
            ],
            placeholder="Secondary variable type...",
            style={"width": "700px", "marginBottom": "12px"},
        ),
        html.Div(
            [
                dcc.Dropdown(
                    id="builder-value-col",
                    placeholder="Value column (required for scalar types)",
                    style={"width": "700px"},
                ),
                dcc.Dropdown(
                    id="builder-quant-aggregation",
                    options=[
                        {"label": "Sum", "value": "sum"},
                        {"label": "Mean", "value": "mean"},
                    ],
                    placeholder="Aggregation method (required for quantitative scalars)",
                    clearable=False,
                    style={"width": "220px", "marginTop": "8px"},
                ),
            ],
            id="builder-scalar-controls",
            style={"display": "none", "marginBottom": "12px"},
        ),
        html.Div(
            [
                html.Div(
                    [
                        dcc.Dropdown(
                            id="builder-cat-col",
                            placeholder="Category column (e.g. fsm)",
                            style={"width": "340px"},
                        ),
                        dcc.Dropdown(
                            id="builder-cnt-col",
                            placeholder="Count column (e.g. headcount)",
                            style={"width": "340px", "marginLeft": "8px"},
                        ),
                    ],
                    style={"marginBottom": "8px"},
                ),
                dcc.Textarea(
                    id="builder-csv-to-number",
                    placeholder="Quantitative distributions only: optional JSON mapping, e.g. {\"0-10\": 5, \"11-20\": 15}",
                    style={"width": "700px", "height": "80px"},
                ),
            ],
            id="builder-dist-controls",
            style={"display": "none", "marginBottom": "12px"},
        ),

        html.H4("3) Optional filters"),
        html.Div(
            [
                dcc.Dropdown(
                    id="builder-filter-col",
                    placeholder="column (e.g. geographic_level)",
                    style={"width": "260px"},
                    clearable=False,
                ),
                dcc.Dropdown(
                    id="builder-filter-op",
                    options=[{"label": x, "value": x} for x in ["==", "!=", "in", "not in"]],
                    value="==",
                    style={"width": "120px", "marginLeft": "8px"},
                    clearable=False,
                ),
                dcc.Input(
                    id="builder-filter-val",
                    placeholder='value (or comma list for "in")',
                    style={"width": "300px", "marginLeft": "8px"},
                ),
                html.Button("Add filter", id="builder-add-filter", style={"marginLeft": "8px"}),
            ],
            style={"marginBottom": "8px"},
        ),
        dcc.Store(id="builder-filters", data=[]),
        html.Div(id="builder-filter-list", style={"marginBottom": "12px"}),

        html.H4("4) Build"),
        html.Div(
            [
                dcc.Input(
                    id="builder-dataset-name",
                    placeholder="output dataset name (e.g. my_fsm)",
                    style={"width": "340px"},
                ),
                html.Button("Build StructuredData", id="builder-build", style={"marginLeft": "8px"}),
            ]
        ),
        html.Div(id="builder-status", style={"marginTop": "12px"}),
    ],
    style={"padding": "16px"},
)


@callback(
    Output("builder-columns", "children"),
    Output("builder-primaries", "options"),
    Output("builder-location-primary", "options"),
    Output("builder-location-display", "options"),
    Output("builder-value-col", "options"),
    Output("builder-cat-col", "options"),
    Output("builder-cnt-col", "options"),
    Output("builder-filter-col", "options"),
    Input("builder-csv", "value"),
)
def load_csv(csv_path):
    if not csv_path:
        return html.Div("Select a CSV to begin."), [], [], [], [], [], [], []
    df = pd.read_csv(csv_path, nrows=200)
    cols = list(df.columns)
    opts = [{"label": c, "value": c} for c in cols]
    return (
        html.Div([html.Div("Columns: " + ", ".join(cols))]),
        opts,
        opts,
        opts,
        opts,
        opts,
        opts,
        opts,
    )


@callback(
    Output("builder-scalar-controls", "style"),
    Output("builder-dist-controls", "style"),
    Input("builder-sec-type", "value"),
)
def toggle_secondary_controls(sec_type):
    hidden = {"display": "none"}
    if sec_type in ("quant_scalar", "qual_scalar"):
        return {"marginBottom": "12px"}, hidden
    if sec_type in ("qual_dist", "quant_dist"):
        return hidden, {"marginBottom": "12px"}
    return hidden, hidden


@callback(
    Output("builder-filters", "data"),
    Input("builder-add-filter", "n_clicks"),
    State("builder-filters", "data"),
    State("builder-filter-col", "value"),
    State("builder-filter-op", "value"),
    State("builder-filter-val", "value"),
    prevent_initial_call=True,
)
def add_filter(n, filters, col, op, val):
    filters = filters or []
    if not col or val is None:
        return filters
    if op in ("in", "not in"):
        vals = [v.strip() for v in str(val).split(",") if v.strip()]
        value = vals
    else:
        value = val
    filters.append({"col": col, "op": op, "value": value})
    return filters


@callback(
    Output("builder-filter-list", "children"),
    Input("builder-filters", "data"),
)
def show_filters(filters):
    if not filters:
        return html.Div("No filters.")
    return html.Ul([html.Li(f"{f['col']} {f['op']} {f['value']}") for f in filters])


@callback(
    Output("builder-status", "children"),
    Input("builder-build", "n_clicks"),
    State("builder-csv", "value"),
    State("builder-dataset-name", "value"),
    State("builder-primaries", "value"),
    State("builder-location-primary", "value"),
    State("builder-location-display", "value"),
    State("builder-sec-name", "value"),
    State("builder-sec-display", "value"),
    State("builder-sec-type", "value"),
    State("builder-value-col", "value"),
    State("builder-quant-aggregation", "value"),
    State("builder-cat-col", "value"),
    State("builder-cnt-col", "value"),
    State("builder-csv-to-number", "value"),
    State("builder-filters", "data"),
    prevent_initial_call=True,
)
def build(
    n,
    csv_path,
    dataset_name,
    primaries,
    location_primary,
    location_display,
    sec_name,
    sec_display,
    sec_type,
    value_col,
    quant_aggregation,
    cat_col,
    cnt_col,
    csv_to_number_raw,
    filters,
):
    required = [csv_path, dataset_name, primaries, sec_name, sec_display, sec_type]
    if any(val in (None, "") for val in required):
        return html.Div("Missing required fields.", style={"color": "#c0392b"})

    primaries = list(primaries or [])

    spec_kwargs = {
        "name": sec_name,
        "display_name": sec_display,
        "type": sec_type,
    }

    if sec_type in ("quant_scalar", "qual_scalar"):
        if not value_col:
            return html.Div("Select a value column for scalar variables.", style={"color": "#c0392b"})
        spec_kwargs["value_col"] = value_col
        if sec_type == "quant_scalar":
            spec_kwargs["aggregation"] = quant_aggregation or "sum"
    elif sec_type in ("qual_dist", "quant_dist"):
        if not cat_col or not cnt_col:
            return html.Div("Category and count columns are required for distribution variables.", style={"color": "#c0392b"})
        spec_kwargs["category_col"] = cat_col
        spec_kwargs["count_col"] = cnt_col
        if sec_type == "quant_dist" and csv_to_number_raw:
            try:
                mapping = json.loads(csv_to_number_raw)
            except json.JSONDecodeError as exc:
                return html.Div(f"CSV-to-number mapping must be valid JSON: {exc}", style={"color": "#c0392b"})
            if not isinstance(mapping, dict):
                return html.Div("CSV-to-number mapping must be a JSON object.", style={"color": "#c0392b"})
            spec_kwargs["csv_to_number"] = mapping
    else:
        return html.Div("Unsupported secondary variable type.", style={"color": "#c0392b"})

    spec = SecondarySpec(**spec_kwargs)

    display_name_columns = None
    if location_display:
        if not location_primary:
            return html.Div("Select the location primary column to pair with the display column.", style={"color": "#c0392b"})
        if location_primary not in primaries:
            return html.Div("Location primary must also be included in the primary variable list.", style={"color": "#c0392b"})
        display_name_columns = {location_primary: location_display}

    try:
        json_out, csv_out = build_structured_from_csv(
            raw_csv_path=csv_path,
            dataset_name=dataset_name,
            primary_cols=primaries,
            secondary_specs=[spec],
            filters=filters,
            out_dir=OUT_DIR,
            display_name_columns=display_name_columns,
        )
    except Exception as exc:
        return html.Div(str(exc), style={"color": "#c0392b"})

    return html.Div(
        [
            html.Div("Built successfully", style={"color": "#27ae60", "fontWeight": "600"}),
            html.Div(f"JSON: {json_out}"),
            html.Div(f"CSV: {csv_out}"),
            html.Div("Now point your visualisations at the structured JSON."),
        ]
    )
