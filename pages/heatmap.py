"""
Heatmap page - dataset explorer built on StructuredData output.
"""

import json
from functools import lru_cache
from pathlib import Path

import dash
import pandas as pd
import plotly.express as px
from dash import ALL, Input, Output, State, callback, dcc, html
from dash.exceptions import PreventUpdate

from Ingestion.structured_data import StructuredData
from Ingestion.primary_variable import TOTAL_VALUE
from Ingestion.schema import Schema


dash.register_page(__name__, path="/heatmap", name="Heatmap")

_HEATMAP_DIR = Path(__file__).resolve().parents[1] / "Heatmap"
_BASE_DIR = _HEATMAP_DIR.parent

_output_dir = _BASE_DIR / "Ingestion" / "test" / "output"
_json_candidates = list(_output_dir.rglob("fsm_ethnicity_structured.json"))
_geo_candidates = list(_HEATMAP_DIR.glob("*.geojson")) + list(_BASE_DIR.rglob("*.geojson"))


def _dataset_label(path: Path) -> str:
    try:
        return str(path.relative_to(_output_dir))
    except ValueError:
        return path.name


def _list_dataset_options() -> tuple[list[dict[str, str]], str | None]:
    options: list[dict[str, str]] = []
    for candidate in sorted(_output_dir.rglob("*_structured.json")):
        options.append({"label": _dataset_label(candidate), "value": str(candidate)})
    default = options[0]["value"] if options else None
    return options, default


def _dataset_config(dataset_value: str | None) -> dict[str, object] | None:
    if not dataset_value:
        return None
    path = Path(dataset_value)
    if not path.exists():
        return None
    return _build_dataset_config_cached(str(path.resolve()))


def _dataset_meta(dataset_value: str | None) -> dict[str, object] | None:
    if not dataset_value:
        return None
    path = Path(dataset_value)
    if not path.exists():
        return None
    return _build_dataset_meta_cached(str(path.resolve()))


if not _geo_candidates:
    layout = html.Div(
        [
            html.H2("Heatmap"),
            html.P(
                "Could not find required boundary GeoJSON.",
                style={"color": "#c0392b"},
            ),
        ],
        style={"padding": "16px"},
    )
else:
    _BOUNDARY_PATH = _geo_candidates[0]
    with open(_BOUNDARY_PATH, "r", encoding="utf-8") as f:
        _geojson = json.load(f)

    CODE_PROP = "CTYUA17CD"

    def _guess_primary_column(options: list[dict[str, str]], keywords: list[str]) -> str | None:
        for kw in keywords:
            for opt in options:
                if kw in opt["value"].lower() or kw in opt["label"].lower():
                    return opt["value"]
        return None

    def _primary_value_options(cfg: dict[str, object], column_name: str, *, include_total: bool = True) -> list[dict[str, str]]:
        pv_map = cfg["primary_display_map"].get(column_name, {})  # type: ignore[index]
        full = [*pv_map.keys(), TOTAL_VALUE]
        if not include_total:
            full = [v for v in full if v != TOTAL_VALUE]
        return [{"label": pv_map.get(v, v), "value": v} for v in full]

    def _build_dataset_meta(json_path: Path) -> dict[str, object] | None:
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            schema = Schema.from_dict(payload["schema"])
        except Exception:
            return None

        if not schema.primary_variables or not schema.secondary_variables:
            return None

        primary_options: list[dict[str, str]] = []
        display_schema_options: list[dict[str, str]] = []
        primary_display_map: dict[str, dict[str, str]] = {}

        def _has_non_identity(mapping: dict[str, str]) -> bool:
            return any(raw != disp for raw, disp in mapping.items())

        for pv in schema.primary_variables:
            label = pv.title or pv.column_name
            if pv.column_name not in label:
                label = f"{label} ({pv.column_name})"
            primary_options.append({"label": label, "value": pv.column_name})
            normalized_map = {str(k): str(v) for k, v in pv.csv_to_display.items()}
            primary_display_map[pv.column_name] = normalized_map
            if _has_non_identity(normalized_map):
                display_schema_options.append(
                    {
                        "label": f"{label} (schema labels)",
                        "value": f"__schema__{pv.column_name}",
                    }
                )

        default_primary = primary_options[0]["value"] if primary_options else None
        default_geo = _guess_primary_column(primary_options, ["code", "id"]) or default_primary
        default_display = _guess_primary_column(primary_options, ["name", "label"]) or default_geo
        default_time = _guess_primary_column(primary_options, ["year", "time", "period"]) or default_primary

        secondary_meta: dict[str, dict[str, object]] = {}
        secondary_options: list[dict[str, str]] = []
        for var in schema.secondary_variables:
            info: dict[str, object] = {
                "variable_name": var.variable_name,
                "display_name": var.display_name,
                "type": var.variable_type,
                "categories": [{"label": label, "value": key} for key, label in var.csv_dict.items()],
                "value_column": getattr(var, "value_column", None),
            }
            if var.variable_type in ("qualitative_dist", "quantitative_dist"):
                info["metric_options"] = [
                    {"label": "Percent", "value": "percent"},
                    {"label": "Count", "value": "count"},
                ]
            else:
                info["categories"] = []
                info["metric_options"] = [{"label": var.display_name, "value": "value"}]
            secondary_meta[var.variable_name] = info
            secondary_options.append({"label": var.display_name, "value": var.variable_name})

        if not secondary_options:
            return None

        default_secondary = secondary_options[0]["value"]
        default_category = (secondary_meta.get(default_secondary, {}).get("categories") or [None])[0]
        default_metric = (secondary_meta.get(default_secondary, {}).get("metric_options") or [None])[0]

        if default_geo and _has_non_identity(primary_display_map.get(default_geo, {})):
            default_display_choice = f"__schema__{default_geo}"
        else:
            default_display_choice = default_display

        return {
            "path": str(json_path),
            "label": str(json_path.relative_to(_output_dir)) if json_path.is_relative_to(_output_dir) else json_path.name,
            "primary_options": primary_options,
            "display_schema_options": display_schema_options,
            "secondary_options": secondary_options,
            "secondary_meta": secondary_meta,
            "default_geo": default_geo,
            "default_display_choice": default_display_choice,
            "default_time": default_time,
            "default_secondary": default_secondary,
            "default_category": default_category["value"] if default_category else None,
            "default_metric": default_metric["value"] if default_metric else None,
            "primary_display_map": primary_display_map,
        }

    def _build_dataset_config(json_path: Path) -> dict[str, object] | None:
        try:
            structured = StructuredData.load(json_path)
        except Exception:
            return None

        df = structured.dataframe.copy()
        meta = _build_dataset_meta(json_path)
        if not meta:
            return None
        return {**meta, "df": df}

    @lru_cache(maxsize=32)
    def _build_dataset_config_cached(json_path_str: str) -> dict[str, object] | None:
        return _build_dataset_config(Path(json_path_str))

    @lru_cache(maxsize=32)
    def _build_dataset_meta_cached(json_path_str: str) -> dict[str, object] | None:
        return _build_dataset_meta(Path(json_path_str))

    _DATASET_OPTIONS, _DEFAULT_DATASET = _list_dataset_options()

    def _metric_column(cfg: dict[str, object], variable_name: str, category_value: str | None, metric_value: str | None) -> tuple[str | None, str]:
        info = cfg["secondary_meta"].get(variable_name, {})  # type: ignore[index]
        var_type = info.get("type")
        if var_type in ("qualitative_dist", "quantitative_dist"):
            if not category_value:
                return None, "percent"
            metric_suffix = "percent" if metric_value == "percent" else "count"
            return f"{category_value}_{metric_suffix}", metric_suffix
        return info.get("value_column"), "value"

    if not _DATASET_OPTIONS:
        layout = html.Div(
            [
                html.H2("Heatmap"),
                html.P("No valid structured datasets found under Ingestion/test/output.", style={"color": "#c0392b"}),
            ],
            style={"padding": "16px"},
        )
    else:
        _INITIAL_META = _dataset_meta(_DEFAULT_DATASET)
        assert _INITIAL_META is not None
        initial_cat_style = {"marginBottom": "16px"} if _INITIAL_META.get("default_category") else {"display": "none"}  # type: ignore[arg-type]
        initial_metric_style = (
            {"marginBottom": "16px"}
            if (_INITIAL_META.get("default_metric") and _INITIAL_META.get("default_metric") != "value")
                else {"display": "none"}
        )

        def _initial_time_dropdown(cfg):
            time_col = cfg["default_time"]
            if not time_col:
                return [], None
            options = _primary_value_options(cfg, time_col, include_total=False)
            default_value = options[-1]["value"] if options else None
            return options, default_value

        _INITIAL_YEAR_OPTIONS, _INITIAL_YEAR_VALUE = _initial_time_dropdown(_INITIAL_META)

        layout = html.Div(
            [
                html.H2("Structured Dataset Heatmap"),
                html.Div(
                    [
                        html.Label("Structured dataset"),
                        html.Div(
                            [
                                dcc.Dropdown(
                                    id="heatmap-dataset",
                                    options=_DATASET_OPTIONS,
                                    value=_DEFAULT_DATASET,
                                    clearable=False,
                                    style={"width": "420px"},
                                ),
                                html.Button("Refresh datasets", id="heatmap-refresh-datasets", style={"marginLeft": "12px"}),
                            ],
                            style={"display": "flex", "alignItems": "center"},
                        ),
                    ],
                    style={"marginBottom": "16px"},
                ),
                html.Div(
                    [
                        html.Label("Time column"),
                        dcc.Dropdown(
                            id="heatmap-time-column",
                            options=_INITIAL_META["primary_options"],  # type: ignore[index]
                            value=_INITIAL_META["default_time"],  # type: ignore[index]
                            clearable=False,
                            style={"width": "320px"},
                        ),
                    ],
                    style={"marginBottom": "16px"},
                ),
                html.Div(
                    [
                        html.Label("Time value"),
                        dcc.Dropdown(
                            id="heatmap-year-dropdown",
                            options=_INITIAL_YEAR_OPTIONS,
                            value=_INITIAL_YEAR_VALUE,
                            clearable=False,
                            style={"width": "320px"},
                        ),
                    ],
                    style={"marginBottom": "16px"},
                ),
                html.Div(
                    [
                        html.Label("Location code column (matches GeoJSON IDs)"),
                        dcc.Dropdown(
                            id="heatmap-geo-code-column",
                            options=_INITIAL_META["primary_options"],  # type: ignore[index]
                            value=_INITIAL_META["default_geo"],  # type: ignore[index]
                            clearable=False,
                            style={"width": "420px"},
                        ),
                    ],
                    style={"marginBottom": "16px"},
                ),
                html.Div(
                    [
                        html.Label("Location display column"),
                        dcc.Dropdown(
                            id="heatmap-display-column",
                            options=(_INITIAL_META["primary_options"] + _INITIAL_META["display_schema_options"]),  # type: ignore[index]
                            value=_INITIAL_META["default_display_choice"],  # type: ignore[index]
                            clearable=False,
                            style={"width": "420px"},
                        ),
                    ],
                    style={"marginBottom": "16px"},
                ),
                html.Div(id="heatmap-primary-filter-container", style={"marginBottom": "16px"}),
                html.H4("Secondary variable"),
                html.Div(
                    [
                        dcc.Dropdown(
                            id="heatmap-secondary-variable",
                            options=_INITIAL_META["secondary_options"],  # type: ignore[index]
                            value=_INITIAL_META["default_secondary"],  # type: ignore[index]
                            clearable=False,
                            style={"width": "420px"},
                        ),
                    ],
                    style={"marginBottom": "16px"},
                ),
                html.Div(
                    [
                        html.Label("Category"),
                        dcc.Dropdown(
                            id="heatmap-secondary-category",
                            options=_INITIAL_META["secondary_meta"].get(_INITIAL_META["default_secondary"], {}).get("categories", []),  # type: ignore[index]
                            value=_INITIAL_META["default_category"],  # type: ignore[index]
                            clearable=False,
                            style={"width": "420px"},
                        ),
                    ],
                    id="heatmap-secondary-category-container",
                    style=initial_cat_style,
                ),
                html.Div(
                    [
                        html.Label("Metric"),
                        dcc.Dropdown(
                            id="heatmap-metric-type",
                            options=_INITIAL_META["secondary_meta"].get(_INITIAL_META["default_secondary"], {}).get("metric_options", []),  # type: ignore[index]
                            value=_INITIAL_META["default_metric"],  # type: ignore[index]
                            clearable=False,
                            style={"width": "420px"},
                        ),
                    ],
                    id="heatmap-metric-type-container",
                    style=initial_metric_style,
                ),
                dcc.Graph(id="heatmap-choropleth", style={"height": "80vh"}),
                html.Small(id="heatmap-data-caption", children=f"Data: {_INITIAL_META['label']} - Boundaries: {_BOUNDARY_PATH.name}"),  # type: ignore[index]
            ],
            style={"padding": "16px"},
        )

        @callback(
            Output("heatmap-dataset", "options"),
            Output("heatmap-dataset", "value"),
            Input("heatmap-refresh-datasets", "n_clicks"),
            State("heatmap-dataset", "value"),
        )
        def refresh_dataset_dropdown(_, current_value):
            _build_dataset_config_cached.cache_clear()
            _build_dataset_meta_cached.cache_clear()
            options, default_value = _list_dataset_options()
            option_values = {opt["value"] for opt in options}
            value = current_value if current_value in option_values else default_value
            return options, value

        @callback(
            Output("heatmap-time-column", "options"),
            Output("heatmap-time-column", "value"),
            Output("heatmap-geo-code-column", "options"),
            Output("heatmap-geo-code-column", "value"),
            Output("heatmap-display-column", "options"),
            Output("heatmap-display-column", "value"),
            Output("heatmap-secondary-variable", "options"),
            Output("heatmap-secondary-variable", "value"),
            Output("heatmap-secondary-category", "options"),
            Output("heatmap-secondary-category", "value"),
            Output("heatmap-secondary-category-container", "style"),
            Output("heatmap-metric-type", "options"),
            Output("heatmap-metric-type", "value"),
            Output("heatmap-metric-type-container", "style"),
            Output("heatmap-data-caption", "children"),
            Input("heatmap-dataset", "value"),
        )
        def refresh_dataset(dataset_value: str | None):
            cfg = _dataset_meta(dataset_value)
            if not cfg:
                raise PreventUpdate

            cat_style = {"marginBottom": "16px"} if cfg["default_category"] else {"display": "none"}
            metric_style = (
                {"marginBottom": "16px"}
                if (cfg["default_metric"] and cfg["default_metric"] != "value")
                else {"display": "none"}
            )
            caption = f"Data: {cfg['label']} - Boundaries: {_BOUNDARY_PATH.name}"

            secondary_meta = cfg["secondary_meta"]  # type: ignore[assignment]
            default_secondary = cfg["default_secondary"]
            default_category = cfg["default_category"]
            default_metric = cfg["default_metric"]

            cat_options = secondary_meta.get(default_secondary, {}).get("categories", [])  # type: ignore[index]
            metric_options = secondary_meta.get(default_secondary, {}).get("metric_options", [])  # type: ignore[index]

            return (
                cfg["primary_options"],
                cfg["default_time"],
                cfg["primary_options"],
                cfg["default_geo"],
                cfg["primary_options"] + cfg["display_schema_options"],
                cfg["default_display_choice"],
                cfg["secondary_options"],
                default_secondary,
                cat_options,
                default_category,
                cat_style,
                metric_options,
                default_metric,
                metric_style,
                caption,
            )

        @callback(
            Output("heatmap-year-dropdown", "options"),
            Output("heatmap-year-dropdown", "value"),
            Input("heatmap-dataset", "value"),
            Input("heatmap-time-column", "value"),
        )
        def sync_time_values(dataset_value: str | None, time_column: str | None):
            cfg = _dataset_meta(dataset_value)
            if not cfg or not time_column:
                return [], None
            options = _primary_value_options(cfg, time_column, include_total=False)
            default_value = options[-1]["value"] if options else None
            return options, default_value

        @callback(
            Output("heatmap-primary-filter-container", "children"),
            Input("heatmap-dataset", "value"),
            Input("heatmap-time-column", "value"),
            Input("heatmap-geo-code-column", "value"),
        )
        def sync_primary_filters(dataset_value: str | None, time_column: str | None, geo_code_col: str | None):
            cfg = _dataset_meta(dataset_value)
            if not cfg:
                return []

            excluded = {time_column, geo_code_col}
            filter_columns = [
                opt["value"]
                for opt in cfg["primary_options"]  # type: ignore[index]
                if opt["value"] not in excluded
            ]

            if not filter_columns:
                return []

            children = [html.H4("Other primary filters")]
            for col in filter_columns:
                options = _primary_value_options(cfg, col)
                value = next((opt["value"] for opt in options if opt["value"] == TOTAL_VALUE), None)
                if value is None and options:
                    value = options[0]["value"]
                label = next(
                    (opt["label"] for opt in cfg["primary_options"] if opt["value"] == col),  # type: ignore[index]
                    col,
                )
                children.append(
                    html.Div(
                        [
                            html.Label(label),
                            dcc.Dropdown(
                                id={"type": "heatmap-primary-filter", "column": col},
                                options=options,
                                value=value,
                                clearable=False,
                                style={"width": "320px"},
                            ),
                        ],
                        style={"marginBottom": "12px"},
                    )
                )
            return children

        @callback(
            Output("heatmap-secondary-category", "options", allow_duplicate=True),
            Output("heatmap-secondary-category", "value", allow_duplicate=True),
            Output("heatmap-secondary-category-container", "style", allow_duplicate=True),
            Output("heatmap-metric-type", "options", allow_duplicate=True),
            Output("heatmap-metric-type", "value", allow_duplicate=True),
            Output("heatmap-metric-type-container", "style", allow_duplicate=True),
            Input("heatmap-secondary-variable", "value"),
            State("heatmap-dataset", "value"),
            prevent_initial_call=True,
        )
        def sync_secondary_controls(variable_name: str | None, dataset_value: str | None):
            cfg = _dataset_meta(dataset_value)
            hidden = {"display": "none"}
            if not cfg or not variable_name:
                return [], None, hidden, [], None, hidden

            info = cfg["secondary_meta"].get(variable_name, {})  # type: ignore[index]
            cat_opts = info.get("categories", [])
            cat_value = cat_opts[0]["value"] if cat_opts else None
            cat_style = {"marginBottom": "16px"} if cat_opts else hidden

            metric_opts = info.get("metric_options", [])
            metric_value = metric_opts[0]["value"] if metric_opts else None
            metric_style = {"marginBottom": "16px"} if (metric_value and metric_value != "value") else hidden

            return cat_opts, cat_value, cat_style, metric_opts, metric_value, metric_style

        @callback(
            Output("heatmap-choropleth", "figure"),
            Input("heatmap-dataset", "value"),
            Input("heatmap-year-dropdown", "value"),
            Input("heatmap-time-column", "value"),
            Input("heatmap-geo-code-column", "value"),
            Input("heatmap-display-column", "value"),
            Input("heatmap-secondary-variable", "value"),
            Input("heatmap-secondary-category", "value"),
            Input("heatmap-metric-type", "value"),
            Input({"type": "heatmap-primary-filter", "column": ALL}, "value"),
            State({"type": "heatmap-primary-filter", "column": ALL}, "id"),
        )
        def update_map(
            dataset_value,
            selected_year,
            time_column,
            geo_code_col,
            display_col,
            secondary_var,
            category_value,
            metric_value,
            filter_values,
            filter_ids,
        ):
            cfg = _dataset_config(dataset_value)
            if not cfg or not selected_year or not time_column or not geo_code_col or not secondary_var:
                raise PreventUpdate

            df = cfg["df"]  # type: ignore[assignment]
            metric_col, metric_kind = _metric_column(cfg, secondary_var, category_value, metric_value)
            if not metric_col or metric_col not in df.columns:
                raise PreventUpdate

            if geo_code_col not in df.columns:
                raise PreventUpdate

            if time_column not in df.columns:
                raise PreventUpdate

            d = df[df[time_column].astype(str) == str(selected_year)].copy()
            for filter_id, filter_value in zip(filter_ids, filter_values):
                col = filter_id.get("column")
                if not col or col not in d.columns or filter_value is None:
                    continue
                d = d[d[col].astype(str).str.strip() == str(filter_value)]
            d[geo_code_col] = d[geo_code_col].astype(str).str.strip()
            d = d[d[geo_code_col].str.upper() != "TOTAL"]
            d[metric_col] = pd.to_numeric(d[metric_col], errors="coerce")

            if not display_col:
                display_col = geo_code_col

            if display_col.startswith("__schema__"):
                base_col = display_col.replace("__schema__", "", 1)
                display_map = cfg["primary_display_map"].get(base_col, {})  # type: ignore[index]
                if base_col not in d.columns:
                    raise PreventUpdate
                temp_col = "__display_label__"
                d[temp_col] = (
                    d[base_col]
                    .astype(str)
                    .str.strip()
                    .map(lambda key: display_map.get(key, key))
                )
                display_col = temp_col
            elif display_col not in d.columns:
                display_col = geo_code_col

            hover_data = {
                geo_code_col: True,
                metric_col: ":.2f" if metric_kind == "percent" else True,
            }

            fig = px.choropleth(
                d,
                geojson=_geojson,
                locations=geo_code_col,
                featureidkey=f"properties.{CODE_PROP}",
                color=metric_col,
                color_continuous_scale="Reds",
                hover_name=display_col,
                hover_data=hover_data,
            )
            fig.update_geos(fitbounds="locations", visible=False)
            fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
            return fig
