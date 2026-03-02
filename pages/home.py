"""
Home / landing page — registered at "/" to fix the 404 on first load.
"""

import dash
from dash import html

dash.register_page(__name__, path="/", name="Home", order=0)

_CARD_STYLE = {
    "border": "1px solid #e0e0e0",
    "borderRadius": "8px",
    "padding": "20px 24px",
    "textDecoration": "none",
    "color": "#111",
    "display": "block",
    "backgroundColor": "#fff",
    "transition": "box-shadow 0.15s",
}

_PAGES = [
    {
        "href": "/heatmap",
        "title": "Heatmap",
        "desc": "FSM eligibility by local authority — choropleth map filterable by academic year.",
    },
    {
        "href": "/pie",
        "title": "Breakdowns",
        "desc": "SEN age/sex and FSM/ethnicity distributions as interactive pie charts.",
    },
    {
        "href": "/sentiment",
        "title": "Sentiment Timeline",
        "desc": "Average monthly sentiment score across education news articles.",
    },
    {
        "href": "/wordcloud",
        "title": "Word Cloud",
        "desc": "Interactive education inclusion keyword cloud with article drill-down and trends.",
    },
]

layout = html.Div(
    [
        html.H2("Education Futures Radar", style={"marginBottom": "6px"}),
        html.P(
            "An evidence-informed dashboard for exploring signals of change in education.",
            style={"color": "#555", "marginBottom": "32px"},
        ),
        html.Div(
            [
                html.A(
                    [
                        html.Strong(p["title"], style={"fontSize": "1rem"}),
                        html.P(p["desc"], style={"margin": "6px 0 0", "color": "#555", "fontSize": "0.9rem"}),
                    ],
                    href=p["href"],
                    style=_CARD_STYLE,
                )
                for p in _PAGES
            ],
            style={"display": "flex", "flexDirection": "column", "gap": "16px", "maxWidth": "560px"},
        ),
    ],
    style={"padding": "32px"},
)
