import json
from pathlib import Path
from uuid import uuid4

import dash
import pandas as pd
from dash import ALL, Input, Output, State, callback, dcc, html

from Ingestion.dataset_builder import (
    SecondarySpec,
    parse_structured_from_csv,
    save_structured_data,
)
from pages._structured_dataset_registry import load_dataset


dash.register_page(__name__, path="/builder", name="Dataset Builder")

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "Data"
OUT_DIR = ROOT / "Ingestion" / "test" / "output"
PARSED_STRUCTURED_CACHE = {}


def list_csvs():
    if RAW_DIR.exists():
        return sorted(RAW_DIR.rglob("*.csv"))
    return []


def build_secondary_spec(
    *,
    sec_name,
    sec_display,
    sec_type,
    value_col,
    quant_aggregation,
    cat_col,
    cnt_col,
    csv_to_number_raw,
):
    spec_kwargs = {
        "name": sec_name,
        "display_name": sec_display,
        "type": sec_type,
    }

    if sec_type in ("quant_scalar", "qual_scalar"):
        if not value_col:
            raise ValueError("Select a value column for scalar variables.")
        spec_kwargs["value_col"] = value_col
        if sec_type == "quant_scalar":
            if not quant_aggregation:
                raise ValueError("Select an aggregation method for quantitative scalars.")
            spec_kwargs["aggregation"] = quant_aggregation
    elif sec_type in ("qual_dist", "quant_dist"):
        if not cat_col or not cnt_col:
            raise ValueError("Category and count columns are required for distribution variables.")
        spec_kwargs["category_col"] = cat_col
        spec_kwargs["count_col"] = cnt_col
        if sec_type == "quant_dist" and csv_to_number_raw:
            try:
                mapping = json.loads(csv_to_number_raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"CSV-to-number mapping must be valid JSON: {exc}") from exc
            if not isinstance(mapping, dict):
                raise ValueError("CSV-to-number mapping must be a JSON object.")
            spec_kwargs["csv_to_number"] = mapping
    else:
        raise ValueError("Unsupported secondary variable type.")

    return SecondarySpec(**spec_kwargs)


layout = html.Div(
    [
        dcc.Store(id="builder-filters", data=[]),
        dcc.Store(id="builder-parse-token"),
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
                    placeholder="internal slug (lowercase, no spaces, e.g. headcount)",
                    style={"width": "340px"},
                ),
                dcc.Input(
                    id="builder-sec-display",
                    placeholder="display name (e.g. Headcount)",
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
                    style={"width": "320px", "marginTop": "8px"},
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
                            placeholder="Category column (e.g. sex)",
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
                    placeholder='Quantitative distributions only: optional JSON mapping, e.g. {"0-10": 5, "11-20": 15}',
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
        html.Div(id="builder-filter-list", style={"marginBottom": "12px"}),
        html.H4("4) Parse CSV"),
        html.Div(
            [
                html.Button("Parse to StructuredData", id="builder-parse"),
            ]
        ),
        html.Div(id="builder-parse-status", style={"marginTop": "12px", "marginBottom": "18px"}),
        html.Div(
            [
                html.H4("5) Primary classification and save"),
                html.Div(
                    "Stage 2 operates on the parsed StructuredData object. Choose whether each primary is qualitative or quantitative, and provide numeric mappings for quantitative primaries.",
                    style={"marginBottom": "12px"},
                ),
                html.Div(id="builder-stage2-controls"),
                html.Div(
                    [
                        dcc.Input(
                            id="builder-dataset-name",
                            placeholder="output dataset name (e.g. sen_age_sex)",
                            style={"width": "340px"},
                        ),
                        html.Button("Save StructuredData", id="builder-save", style={"marginLeft": "8px"}),
                    ],
                    style={"marginTop": "12px"},
                ),
                html.Div(id="builder-save-status", style={"marginTop": "12px"}),
            ],
            id="builder-stage2",
            style={"display": "none"},
        ),
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
        value = [v.strip() for v in str(val).split(",") if v.strip()]
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
    Output("builder-parse-token", "data"),
    Output("builder-parse-status", "children"),
    Output("builder-stage2", "style"),
    Input("builder-parse", "n_clicks"),
    State("builder-csv", "value"),
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
def parse_csv(
    n,
    csv_path,
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
    required = [csv_path, primaries, sec_name, sec_display, sec_type]
    if any(val in (None, "") for val in required):
        return None, html.Div("Missing required fields.", style={"color": "#c0392b"}), {"display": "none"}

    primaries = list(primaries or [])

    try:
        spec = build_secondary_spec(
            sec_name=sec_name,
            sec_display=sec_display,
            sec_type=sec_type,
            value_col=value_col,
            quant_aggregation=quant_aggregation,
            cat_col=cat_col,
            cnt_col=cnt_col,
            csv_to_number_raw=csv_to_number_raw,
        )
    except Exception as exc:
        return None, html.Div(str(exc), style={"color": "#c0392b"}), {"display": "none"}

    display_name_columns = None
    if location_display:
        if not location_primary:
            return None, html.Div("Select the location primary column to pair with the display column.", style={"color": "#c0392b"}), {"display": "none"}
        if location_primary not in primaries:
            return None, html.Div("Location primary must also be included in the primary variable list.", style={"color": "#c0392b"}), {"display": "none"}
        display_name_columns = {location_primary: location_display}

    try:
        structured = parse_structured_from_csv(
            raw_csv_path=csv_path,
            primary_cols=primaries,
            secondary_specs=[spec],
            filters=filters,
            display_name_columns=display_name_columns,
        )
    except Exception as exc:
        return None, html.Div(str(exc), style={"color": "#c0392b"}), {"display": "none"}

    token = str(uuid4())
    PARSED_STRUCTURED_CACHE[token] = structured
    return (
        token,
        html.Div(
            [
                html.Div("Parsed successfully", style={"color": "#27ae60", "fontWeight": "600"}),
                html.Div(f"Rows: {len(structured.dataframe)}"),
                html.Div("Stage 2 is ready. Classify primaries and then save."),
            ]
        ),
        {"display": "block"},
    )


@callback(
    Output("builder-stage2-controls", "children"),
    Input("builder-parse-token", "data"),
)
def render_stage2_controls(parse_token):
    if not parse_token or parse_token not in PARSED_STRUCTURED_CACHE:
        return html.Div("Parse a CSV first.")

    structured = PARSED_STRUCTURED_CACHE[parse_token]
    blocks = []
    for pv in structured.schema.primary_variables:
        mapping_rows = []
        for csv_value in pv.values():
            mapping_rows.append(
                html.Div(
                    [
                        html.Div(csv_value, style={"width": "180px", "paddingTop": "8px"}),
                        dcc.Input(
                            id={"type": "builder-primary-display", "primary": pv.column_name, "value": csv_value},
                            value=pv.csv_to_display.get(csv_value, csv_value),
                            placeholder="Display label",
                            style={"width": "240px", "marginLeft": "8px"},
                        ),
                        dcc.Input(
                            id={"type": "builder-primary-number", "primary": pv.column_name, "value": csv_value},
                            placeholder="Numeric value (required if quantitative)",
                            style={"width": "240px", "marginLeft": "8px"},
                        ),
                    ],
                    style={"display": "flex", "marginBottom": "6px"},
                )
            )

        blocks.append(
            html.Div(
                [
                    html.H5(f"{pv.title} ({pv.column_name})", style={"marginBottom": "8px"}),
                    dcc.Dropdown(
                        id={"type": "builder-primary-type", "primary": pv.column_name},
                        options=[
                            {"label": "Qualitative primary", "value": "qualitative"},
                            {"label": "Quantitative primary", "value": "quantitative"},
                        ],
                        value="qualitative",
                        clearable=False,
                        style={"width": "300px", "marginBottom": "10px"},
                    ),
                    html.Div(mapping_rows),
                ],
                style={"border": "1px solid #ddd", "padding": "12px", "marginBottom": "12px"},
            )
        )

    return blocks


@callback(
    Output("builder-save-status", "children"),
    Input("builder-save", "n_clicks"),
    State("builder-parse-token", "data"),
    State("builder-dataset-name", "value"),
    State({"type": "builder-primary-type", "primary": ALL}, "value"),
    State({"type": "builder-primary-type", "primary": ALL}, "id"),
    State({"type": "builder-primary-display", "primary": ALL, "value": ALL}, "value"),
    State({"type": "builder-primary-display", "primary": ALL, "value": ALL}, "id"),
    State({"type": "builder-primary-number", "primary": ALL, "value": ALL}, "value"),
    State({"type": "builder-primary-number", "primary": ALL, "value": ALL}, "id"),
    prevent_initial_call=True,
)
def save_stage2(
    n,
    parse_token,
    dataset_name,
    primary_types,
    primary_type_ids,
    display_values,
    display_ids,
    numeric_values,
    numeric_ids,
):
    if not parse_token or parse_token not in PARSED_STRUCTURED_CACHE:
        return html.Div("Parse a CSV before saving.", style={"color": "#c0392b"})
    if not dataset_name:
        return html.Div("Enter an output dataset name.", style={"color": "#c0392b"})

    structured = PARSED_STRUCTURED_CACHE[parse_token]

    type_by_primary = {item["primary"]: value for item, value in zip(primary_type_ids, primary_types)}
    display_by_primary = {}
    for item, value in zip(display_ids, display_values):
        display_by_primary.setdefault(item["primary"], {})[item["value"]] = value or item["value"]

    numbers_by_primary = {}
    for item, value in zip(numeric_ids, numeric_values):
        numbers_by_primary.setdefault(item["primary"], {})[item["value"]] = value

    primary_specs = {}
    for pv in structured.schema.primary_variables:
        variable_type = type_by_primary.get(pv.column_name, "qualitative")
        spec = {
            "variable_type": variable_type,
            "csv_to_display": display_by_primary.get(pv.column_name, dict(pv.csv_to_display)),
        }
        if variable_type == "quantitative":
            csv_to_number = {}
            for csv_value in pv.values():
                raw_value = numbers_by_primary.get(pv.column_name, {}).get(csv_value)
                if raw_value in (None, ""):
                    return html.Div(
                        f"Numeric mapping missing for primary '{pv.column_name}' value '{csv_value}'.",
                        style={"color": "#c0392b"},
                    )
                try:
                    csv_to_number[csv_value] = float(raw_value)
                except (TypeError, ValueError):
                    return html.Div(
                        f"Numeric mapping for primary '{pv.column_name}' value '{csv_value}' must be numeric.",
                        style={"color": "#c0392b"},
                    )
            spec["csv_to_number"] = csv_to_number
        primary_specs[pv.column_name] = spec

    try:
        final_structured = structured.retype_primary_variables(primary_specs)
        json_out, csv_out = save_structured_data(
            structured=final_structured,
            dataset_name=dataset_name,
            out_dir=OUT_DIR,
        )
        load_dataset.cache_clear()
    except Exception as exc:
        return html.Div(str(exc), style={"color": "#c0392b"})

    return html.Div(
        [
            html.Div("Saved successfully", style={"color": "#27ae60", "fontWeight": "600"}),
            html.Div(f"JSON: {json_out}"),
            html.Div(f"CSV: {csv_out}"),
        ]
    )
