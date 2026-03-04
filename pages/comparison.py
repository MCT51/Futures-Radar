"""
Comparison page — side-by-side view of two selected pages.
"""

import importlib

from dash import html, page_registry, dcc, callback
from dash import Input, Output

import dash

dash.register_page(__name__, path="/comparison", name="Comparison")


def _default_two_modules():
    mods = [p.get("module") for p in page_registry.values() if p.get("module")]
    return mods[:2] if len(mods) >= 2 else (mods + [None])[:2]


_options = [
    {"label": p["name"], "value": p["module"]}
    for p in page_registry.values()
]


# ── Layout ──────────────────────────────────────────────────────────────────

layout = html.Div(
    [
        html.Div(
            [
                html.H3("Compare Visualisations", style={"margin": "0 0 12px 0"}),
                # trigger for page-load callbacks
                dcc.Location(id="comp-url", refresh=False),

                html.Div(
                    [
                        html.Div(
                            [
                                html.Label("First page:", style={"fontWeight": "500"}),
                                dcc.Dropdown(
                                    id="page-selector1",
                                    options=[],
                                    value=None,
                                    multi=False,
                                    clearable=False,
                                    style={"width": "100%"},
                                ),
                            ],
                            style={"flex": "1", "minWidth": "250px"},
                        ),
                        html.Div(
                            [
                                html.Label("Second page:", style={"fontWeight": "500"}),
                                dcc.Dropdown(
                                    id="page-selector2",
                                    options=[],
                                    value=None,
                                    multi=False,
                                    clearable=False,
                                    style={"width": "100%"},
                                ),
                            ],
                            style={"flex": "1", "minWidth": "250px"},
                        ),
                    ],
                    style={"display": "flex", "gap": "16px"},
                ),
            ],
            style={"padding": "16px", "borderBottom": "1px solid #eee"},
        ),

        # Two-column content area
        html.Div(
            [
                html.Div(id="left-panel", style={"flex": "1", "minWidth": "320px"}),
                html.Div(id="right-panel", style={"flex": "1", "minWidth": "320px"}),
            ],
            style={"display": "flex", "gap": "16px", "padding": "16px", "alignItems": "flex-start"},
        ),
    ],
)


# ── Callback ────────────────────────────────────────────────────────────────

@callback(
    Output("left-panel", "children"),
    Output("right-panel", "children"),
    Input("page-selector1", "value"),
    Input("page-selector2", "value"),
)
def render_two_pages(selected_module1, selected_module2):
    """Load and render two page layouts side by side."""
    pages_to_load = [selected_module1, selected_module2]
    children = []

    for module_name in pages_to_load:
        if not module_name:
            children.append(html.Div("Select a page...", style={"padding": "12px", "color": "#666"}))
            continue

        try:
            mod = importlib.import_module(module_name)
            layout = getattr(mod, "layout", None)
            if layout is None:
                children.append(
                    html.Div(
                        f"Page module {module_name} has no `layout` attribute.",
                        style={"padding": "12px", "color": "#c0392b"},
                    )
                )
            else:
                children.append(layout)
        except Exception as exc:
            children.append(
                html.Div(
                    f"Failed to load {module_name}: {exc}",
                    style={"padding": "12px", "color": "#c0392b"},
                )
            )

    # Ensure we have exactly two children
    while len(children) < 2:
        children.append(html.Div("", style={"padding": "12px"}))

    return children[0], children[1]


@callback(
    Output("page-selector1", "options"),
    Output("page-selector2", "options"),
    Output("page-selector1", "value"),
    Output("page-selector2", "value"),
    Input("comp-url", "pathname"),
)
def populate_options(pathname):
    """Populate dropdown options on page load so all registered pages appear."""
    opts = [
        {"label": p["name"], "value": p.get("module")}
        for p in page_registry.values()
        if p.get("module") and p["name"] != "Comparison" and p["name"] != "Home"
    ]

    defaults = _default_two_modules()
    values = [d for d in defaults if d in {o["value"] for o in opts}]

    v1 = values[0] if len(values) > 0 else (opts[0]["value"] if opts else None)
    v2 = values[1] if len(values) > 1 else (opts[1]["value"] if len(opts) > 1 else None)

    return opts, opts, v1, v2
