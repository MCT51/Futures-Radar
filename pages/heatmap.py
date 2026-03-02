"""
Heatmap page — FSM eligibility choropleth by local authority.

Uses StructuredData output:
  Ingestion/test/output/fsm_ethnicity_structured.json
  Ingestion/test/output/fsm_ethnicity_structured.csv
"""

import json
from pathlib import Path

import dash
import pandas as pd
import plotly.express as px
from dash import Input, Output, callback, dcc, html

from Ingestion.structured_data import StructuredData
from Ingestion.schema import Schema

dash.register_page(__name__, path="/heatmap", name="Heatmap")

# ── Locate data files ──────────────────────────────────────────────────────
_HEATMAP_DIR = Path(__file__).resolve().parents[1] / "Heatmap"
_BASE_DIR = _HEATMAP_DIR.parent

_output_dir = _BASE_DIR / "Ingestion" / "test" / "output"
_json_candidates = list(_output_dir.rglob("fsm_ethnicity_structured.json"))
_geo_candidates = list(_HEATMAP_DIR.glob("*.geojson")) + list(_BASE_DIR.rglob("*.geojson"))

# ── Error layout if data is missing ─────────────────────────────────────────
if not _json_candidates or not _geo_candidates:
    layout = html.Div(
        [
            html.H2("Heatmap"),
            html.P(
                "Could not find required files: fsm_ethnicity_structured.json and/or boundary GeoJSON.",
                style={"color": "#c0392b"},
            ),
            html.P(f"Looked for JSON under: {_output_dir}"),
        ],
        style={"padding": "16px"},
    )

else:
    _STRUCTURED_JSON = _json_candidates[0]
    _BOUNDARY_PATH = _geo_candidates[0]

    # ── Load structured data ───────────────────────────────────────────────
    structured = StructuredData.load(_STRUCTURED_JSON)
    df = structured.dataframe.copy()
    schema = structured.schema  # available if you want display labels, etc.

    # Ensure consistent types/formatting
    df["year"] = df["year"].astype(str)
    df["location_code"] = df["location_code"].astype(str).str.strip().str.upper()

    # Pick the FSM eligible percent column from the schema keys
    # (this matches the csv_dict key in the schema JSON)
    FSM_ELIGIBLE_KEY = "known_to_be_eligible_for_free_school_meals"
    eligible_percent_col = f"{FSM_ELIGIBLE_KEY}_percent"
    eligible_count_col = f"{FSM_ELIGIBLE_KEY}_count"

    not_eligible_key = "not_known_to_be_eligible_for_free_school_meals"
    not_eligible_count_col = f"{not_eligible_key}_count"

    # Basic sanity check: if columns missing, show a friendly error
    required_cols = {"year", "location_code", eligible_percent_col, eligible_count_col, not_eligible_count_col}
    missing = required_cols - set(df.columns)
    if missing:
        layout = html.Div(
            [
                html.H2("Heatmap"),
                html.P("Structured CSV does not contain expected columns:", style={"color": "#c0392b"}),
                html.Pre("\n".join(sorted(missing))),
                html.P(f"Loaded: {_STRUCTURED_JSON.name}"),
            ],
            style={"padding": "16px"},
        )
    else:
        # Compute a headcount for hover (eligible + not eligible)
        df["headcount"] = pd.to_numeric(df[eligible_count_col], errors="coerce") + pd.to_numeric(
            df[not_eligible_count_col], errors="coerce"
        )

        # Drop total rows for mapping (they won't match LA codes)
        df_map_base = df[df["location_code"].str.upper() != "TOTAL"].copy()

        # Load boundaries
        with open(_BOUNDARY_PATH, "r", encoding="utf-8") as f:
            _geojson = json.load(f)

        CODE_PROP = "CTYUA17CD"  # must match featureidkey="properties.CTYUA17CD"
        NAME_PROP = "CTYUA17NM"  # name field (change if current geojson uses a different one)

        la_lookup = {
            feat["properties"].get(CODE_PROP): feat["properties"].get(NAME_PROP)
            for feat in _geojson.get("features", [])
        }

        df_map_base["la_name"] = df_map_base["location_code"].map(la_lookup) #used to display location names later

        # Years for dropdown
        _years = sorted(df_map_base["year"].dropna().unique())

        # ── Layout ───────────────────────────────────────────────────────────
        layout = html.Div(
            [
                html.H2("FSM Eligibility Heatmap (Local Authorities)"),
                html.Div(
                    [
                        html.Label("Academic Year"),
                        dcc.Dropdown(
                            id="heatmap-year-dropdown",
                            options=[{"label": y, "value": y} for y in _years],
                            value=_years[-1] if _years else None,
                            clearable=False,
                            style={"width": "420px"},
                        ),
                    ],
                    style={"marginBottom": "16px"},
                ),
                dcc.Graph(id="heatmap-choropleth", style={"height": "80vh"}),
                html.Small(f"Data: {_STRUCTURED_JSON.name} — Boundaries: {_BOUNDARY_PATH.name}"),
            ],
            style={"padding": "16px"},
        )

        # ── Callback ─────────────────────────────────────────────────────────
        @callback(
            Output("heatmap-choropleth", "figure"),
            Input("heatmap-year-dropdown", "value"),
        )
        def update_map(selected_year):
            d = df_map_base[df_map_base["year"] == str(selected_year)].copy()

            # Optional: filter to rows where the FSM section is defined (prevents NaN slices)
            # This relies on StructuredData's defined_map.
            # If your StructuredData class uses a different helper method name, remove this block.
            try:
                d = d[
                    d["location_code"].map(
                        lambda la: structured.is_defined(
                            {"year": str(selected_year), "location_code": la},
                            "fsm_eligibility",
                        )
                    )
                ].copy()
            except Exception:
                # If is_defined isn't available, just proceed without filtering
                pass

            # Ensure numeric
            d[eligible_percent_col] = pd.to_numeric(d[eligible_percent_col], errors="coerce")

            fig = px.choropleth(
                d,
                geojson=_geojson,
                locations="location_code",
                featureidkey="properties.CTYUA17CD",
                color=eligible_percent_col,
                color_continuous_scale="Reds",
                hover_name="la_name",
                hover_data={
                    "location_code": True,
                    eligible_percent_col: ":.2f",
                    "headcount": True,
                    eligible_count_col: True,
                },
            )
            fig.update_geos(fitbounds="locations", visible=False)
            fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
            return fig