from pathlib import Path
import json
import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, Input, Output

PAR_DIR = Path(__file__).resolve().parent
BASE_DIR = PAR_DIR.parent


data_dir = BASE_DIR / "Data"
if data_dir.exists():
    csv_candidates = list(data_dir.rglob("spc_pupils_fsm*.csv"))
else:
    csv_candidates = list(BASE_DIR.rglob("spc_pupils_fsm*.csv"))

if not csv_candidates:
    raise FileNotFoundError("Couldn't find spc_pupils_fsm CSV under Data/ or repo.")

exact = [p for p in csv_candidates if p.name.lower() == "spc_pupils_fsm.csv"]
if exact:
    DATA_PATH = exact[0]
else:
    print("CSV file not found, LINE 24")

geo_candidates = list(PAR_DIR.glob("*.geojson")) + list(BASE_DIR.rglob("*.geojson"))
BOUNDARY_PATH = geo_candidates[0]


fsm = pd.read_csv(DATA_PATH)

# Normalise column names
fsm.columns = fsm.columns.str.strip()
if "fsm_eligibility" in fsm.columns and "fsm" not in fsm.columns:
    fsm = fsm.rename(columns={"fsm_eligibility": "fsm"})
if "number_of_pupils" in fsm.columns and "headcount" not in fsm.columns:
    fsm = fsm.rename(columns={"number_of_pupils": "headcount"})

required_cols = [
    "geographic_level",
    "fsm",
    "new_la_code",
    "time_period",
    "phase_type_grouping",
    "percent_of_pupils",
]
missing = [c for c in required_cols if c not in fsm.columns]
if missing:
    raise KeyError(f"Missing required columns in {DATA_PATH.name}: {missing}. Available: {list(fsm.columns)}")

# Ensure percent is numeric
fsm["percent_of_pupils"] = pd.to_numeric(fsm["percent_of_pupils"], errors="coerce") #errors = "coerce" removes NaN values
fsm_map = fsm.loc[
    (fsm["geographic_level"] == "Local authority") &
    (fsm["fsm"] == "known to be eligible for free school meals")
].copy()

fsm_map["time_period"] = fsm_map["time_period"].astype(str)
fsm_map["new_la_code"] = fsm_map["new_la_code"].astype(str).str.strip().str.upper()

with open(BOUNDARY_PATH, "r", encoding="utf-8") as f:
    geojson = json.load(f)

#This is the feature id key in the geojson file that matches the "new_la_code" in our data
featureidkey = "properties.CTYUA17CD"

app = Dash(__name__)

years = sorted(fsm_map["time_period"].unique())

app.layout = html.Div([
    html.H2("FSM Eligibility Heatmap (Local Authorities) - Totals"),

    html.Div([
        html.Label("Academic Year"),
        dcc.Dropdown(
            options=[{"label": y, "value": y} for y in years],
            value=years[-1],
            id="year-dropdown",
            clearable=False,
        ),
    ], style={"width": "420px"}),

    dcc.Graph(id="choropleth-map", style={"height": "80vh"}),
    html.Div([html.Small(f"Data: {DATA_PATH.name} — Boundaries: {BOUNDARY_PATH.name}")])
])


@app.callback(
    Output("choropleth-map", "figure"),
    Input("year-dropdown", "value"),
)
def update_map(selected_year):
    # Use totals only for simplicity
    df = fsm_map[
        (fsm_map["time_period"] == selected_year) &
        (fsm_map["phase_type_grouping"] == "Total")
    ].copy()

    fig = px.choropleth(
        df,
        geojson=geojson,
        locations="new_la_code",
        featureidkey=featureidkey,
        color="percent_of_pupils",
        color_continuous_scale="Reds",
        hover_name="la_name",
        hover_data={
            "percent_of_pupils": ":.1f",
            "headcount": True,
        },
    )

    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))

    return fig


if __name__ == "__main__":
    app.run(debug=True)
