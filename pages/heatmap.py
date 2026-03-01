"""
Heatmap page — FSM eligibility choropleth by local authority.

Ported from Heatmap/heatmaptest.py with minimal changes:
  - uses `from dash import callback` instead of `@app.callback`
  - component IDs prefixed with "heatmap-" to avoid clashes
"""

import json
from pathlib import Path

import dash
import pandas as pd
import plotly.express as px
from dash import Input, Output, callback, dcc, html

dash.register_page(__name__, path="/heatmap", name="Heatmap")

# ── Locate data files (same logic as heatmaptest.py) ────────────────────────
_HEATMAP_DIR = Path(__file__).resolve().parents[1] / "Heatmap"
_BASE_DIR = _HEATMAP_DIR.parent

_data_dir = _BASE_DIR / "Data"
_csv_candidates = (
    list(_data_dir.rglob("spc_pupils_fsm*.csv"))
    if _data_dir.exists()
    else list(_BASE_DIR.rglob("spc_pupils_fsm*.csv"))
)
_geo_candidates = list(_HEATMAP_DIR.glob("*.geojson")) + list(_BASE_DIR.rglob("*.geojson"))

# ── Error layout if data is missing ─────────────────────────────────────────
if not _csv_candidates or not _geo_candidates:
    layout = html.Div(
        [
            html.H2("Heatmap"),
            html.P(
                "Could not find required data files (spc_pupils_fsm CSV and/or boundary GeoJSON).",
                style={"color": "#c0392b"},
            ),
        ],
        style={"padding": "16px"},
    )

else:
    # ── Load and normalise data ──────────────────────────────────────────────
    _exact = [p for p in _csv_candidates if p.name.lower() == "spc_pupils_fsm.csv"]
    _DATA_PATH = _exact[0] if _exact else _csv_candidates[0]
    _BOUNDARY_PATH = _geo_candidates[0]

    _fsm = pd.read_csv(_DATA_PATH)
    _fsm.columns = _fsm.columns.str.strip()
    if "fsm_eligibility" in _fsm.columns and "fsm" not in _fsm.columns:
        _fsm = _fsm.rename(columns={"fsm_eligibility": "fsm"})
    if "number_of_pupils" in _fsm.columns and "headcount" not in _fsm.columns:
        _fsm = _fsm.rename(columns={"number_of_pupils": "headcount"})

    _fsm["percent_of_pupils"] = pd.to_numeric(_fsm["percent_of_pupils"], errors="coerce")
    _fsm_map = _fsm.loc[
        (_fsm["geographic_level"] == "Local authority")
        & (_fsm["fsm"] == "known to be eligible for free school meals")
    ].copy()
    _fsm_map["time_period"] = _fsm_map["time_period"].astype(str)
    _fsm_map["new_la_code"] = _fsm_map["new_la_code"].astype(str).str.strip().str.upper()

    with open(_BOUNDARY_PATH, "r", encoding="utf-8") as _f:
        _geojson = json.load(_f)

    _years = sorted(_fsm_map["time_period"].unique())

    # ── Layout ───────────────────────────────────────────────────────────────
    layout = html.Div(
        [
            html.H2("FSM Eligibility Heatmap (Local Authorities)"),
            html.Div(
                [
                    html.Label("Academic Year"),
                    dcc.Dropdown(
                        id="heatmap-year-dropdown",
                        options=[{"label": y, "value": y} for y in _years],
                        value=_years[-1],
                        clearable=False,
                        style={"width": "420px"},
                    ),
                ],
                style={"marginBottom": "16px"},
            ),
            dcc.Graph(id="heatmap-choropleth", style={"height": "80vh"}),
            html.Small(f"Data: {_DATA_PATH.name} — Boundaries: {_BOUNDARY_PATH.name}"),
        ],
        style={"padding": "16px"},
    )

    # ── Callback ─────────────────────────────────────────────────────────────
    @callback(
        Output("heatmap-choropleth", "figure"),
        Input("heatmap-year-dropdown", "value"),
    )
    def update_map(selected_year):
        df = _fsm_map[
            (_fsm_map["time_period"] == selected_year)
            & (_fsm_map["phase_type_grouping"] == "Total")
        ].copy()

        fig = px.choropleth(
            df,
            geojson=_geojson,
            locations="new_la_code",
            featureidkey="properties.CTYUA17CD",
            color="percent_of_pupils",
            color_continuous_scale="Reds",
            hover_name="la_name",
            hover_data={"percent_of_pupils": ":.1f", "headcount": True},
        )
        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
        return fig
