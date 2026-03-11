from __future__ import annotations

from pathlib import Path

import dash
from dash import Input, Output, State, callback, dcc, html

from Ingestion.secondary_variable import QuantitativeScalarSecondaryVariable
from pages._structured_dataset_registry import dataset_options, load_dataset


dash.register_page(__name__, path="/transform", name="Dataset Transform")

_OUTPUT_DIR = Path(__file__).resolve().parents[1] / "Ingestion" / "test" / "output"


def _primary_options(dataset_value: str | None) -> list[dict[str, str]]:
    sd = load_dataset(dataset_value) if dataset_value else None
    if sd is None:
        return []
    return [
        {"label": f"{pv.title} ({pv.column_name})", "value": pv.column_name}
        for pv in sd.schema.primary_variables
    ]


def _count_secondary_options(dataset_value: str | None) -> list[dict[str, str]]:
    sd = load_dataset(dataset_value) if dataset_value else None
    if sd is None:
        return []
    options = []
    for sv in sd.schema.secondary_variables:
        if isinstance(sv, QuantitativeScalarSecondaryVariable):
            options.append(
                {
                    "label": f"{sv.display_name} ({sv.value_column})",
                    "value": sv.variable_name,
                }
            )
    return options


_DATASET_OPTIONS = dataset_options()

if not _DATASET_OPTIONS:
    layout = html.Div(
        [
            html.H2("Dataset Transform"),
            html.P(
                "No StructuredData JSON files found under Ingestion/test/output.",
                style={"color": "#c0392b"},
            ),
        ],
        style={"padding": "16px"},
    )
else:
    _DEFAULT_DATASET = _DATASET_OPTIONS[0]["value"]
    layout = html.Div(
        [
            html.H2("Dataset Transform"),
            html.Div(
                [
                    html.Label("Source StructuredData dataset"),
                    html.Div(
                        [
                            dcc.Dropdown(
                                id="transform-dataset",
                                options=_DATASET_OPTIONS,
                                value=_DEFAULT_DATASET,
                                clearable=False,
                                style={"width": "420px"},
                            ),
                            html.Button(
                                "Refresh datasets",
                                id="transform-refresh-datasets",
                                style={"marginLeft": "12px"},
                            ),
                        ],
                        style={"display": "flex", "alignItems": "center"},
                    ),
                ],
                style={"marginBottom": "16px"},
            ),
            html.Div(
                [
                    html.Label("Quantitative scalar count basis"),
                    dcc.Dropdown(
                        id="transform-count-secondary",
                        clearable=False,
                        style={"width": "420px"},
                    ),
                ],
                style={"marginBottom": "16px"},
            ),
            html.Div(
                [
                    html.Label("Primary variables to flatten"),
                    dcc.Dropdown(
                        id="transform-primaries",
                        multi=True,
                        placeholder="Select one or more primaries to flatten...",
                        style={"width": "700px"},
                    ),
                    html.Div(
                        "Flattening is applied in the selected order. Each chosen primary is converted into a secondary distribution.",
                        style={"marginTop": "8px", "color": "#555"},
                    ),
                ],
                style={"marginBottom": "16px"},
            ),
            html.Div(
                [
                    html.Label("Output dataset name"),
                    dcc.Input(
                        id="transform-dataset-name",
                        placeholder="e.g. sen_age_sex_flattened",
                        style={"width": "340px"},
                    ),
                    html.Button("Apply transform and save", id="transform-save", style={"marginLeft": "8px"}),
                ],
                style={"marginBottom": "12px"},
            ),
            html.Div(id="transform-status", style={"marginTop": "12px"}),
        ],
        style={"padding": "16px"},
    )


@callback(
    Output("transform-dataset", "options"),
    Output("transform-dataset", "value"),
    Input("transform-refresh-datasets", "n_clicks"),
    State("transform-dataset", "value"),
)
def refresh_transform_datasets(_, current_value):
    load_dataset.cache_clear()
    options = dataset_options()
    option_values = {opt["value"] for opt in options}
    value = current_value if current_value in option_values else (options[0]["value"] if options else None)
    return options, value


@callback(
    Output("transform-count-secondary", "options"),
    Output("transform-count-secondary", "value"),
    Output("transform-primaries", "options"),
    Input("transform-dataset", "value"),
)
def update_transform_controls(dataset_value):
    count_opts = _count_secondary_options(dataset_value)
    primary_opts = _primary_options(dataset_value)
    default_count = count_opts[0]["value"] if count_opts else None
    return count_opts, default_count, primary_opts


@callback(
    Output("transform-status", "children"),
    Input("transform-save", "n_clicks"),
    State("transform-dataset", "value"),
    State("transform-count-secondary", "value"),
    State("transform-primaries", "value"),
    State("transform-dataset-name", "value"),
    prevent_initial_call=True,
)
def apply_transform(n_clicks, dataset_value, count_secondary_name, primary_columns, dataset_name):
    if not dataset_value:
        return html.Div("Select a source dataset.", style={"color": "#c0392b"})
    if not count_secondary_name:
        return html.Div("Select a quantitative scalar count basis.", style={"color": "#c0392b"})
    if not primary_columns:
        return html.Div("Select at least one primary variable to flatten.", style={"color": "#c0392b"})
    if not dataset_name:
        return html.Div("Enter an output dataset name.", style={"color": "#c0392b"})

    structured = load_dataset(dataset_value)
    if structured is None:
        return html.Div("Dataset could not be loaded.", style={"color": "#c0392b"})

    try:
        transformed = structured
        for primary_column_name in primary_columns:
            transformed = transformed.flatten_primary_to_secondary(
                primary_column_name,
                count_secondary_name=count_secondary_name,
            )
        json_out = _OUTPUT_DIR / f"{dataset_name}_structured.json"
        csv_out = _OUTPUT_DIR / f"{dataset_name}_structured.csv"
        transformed.save(json_path=json_out, csv_path=csv_out, include_schema=True)
    except Exception as exc:
        return html.Div(str(exc), style={"color": "#c0392b"})

    return html.Div(
        [
            html.Div("Transform saved successfully", style={"color": "#27ae60", "fontWeight": "600"}),
            html.Div(f"JSON: {json_out}"),
            html.Div(f"CSV: {csv_out}"),
        ]
    )
