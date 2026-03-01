"""
Education Futures Radar — unified dashboard entry point.

Run from the repo root:
    python app.py

Pages are auto-discovered from the pages/ directory by Dash.
"""

import dash
from dash import Dash, html, page_container, page_registry

app = Dash(__name__, use_pages=True)

# ── Shared nav styles ────────────────────────────────────────────────────────
_NAV_STYLE = {
    "display": "flex",
    "alignItems": "center",
    "gap": "24px",
    "padding": "12px 20px",
    "borderBottom": "2px solid #e0e0e0",
    "backgroundColor": "#f8f8f8",
    "fontFamily": "Segoe UI, sans-serif",
}

_LINK_STYLE = {
    "textDecoration": "none",
    "color": "#333",
    "fontWeight": "500",
    "fontSize": "0.95rem",
}

_TITLE_STYLE = {
    "fontWeight": "700",
    "fontSize": "1.05rem",
    "marginRight": "8px",
    "color": "#111",
}

app.layout = html.Div(
    [
        html.Nav(
            [
                html.Span("Education Futures Radar", style=_TITLE_STYLE),
                *[
                    html.A(page["name"], href=page["path"], style=_LINK_STYLE)
                    for page in page_registry.values()
                ],
            ],
            style=_NAV_STYLE,
        ),
        page_container,
    ],
    style={"fontFamily": "Segoe UI, sans-serif"},
)

if __name__ == "__main__":
    app.run(debug=True)
