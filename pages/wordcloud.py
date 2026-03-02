"""
Word cloud page wrapper.

Embeds the interactive word cloud UI served from /wordcloud/embed.
"""

import dash
from dash import html

dash.register_page(__name__, path="/wordcloud", name="Word Cloud")

layout = html.Div(
    [
        html.Iframe(
            src="/wordcloud/embed",
            style={
                "width": "100%",
                "height": "100%",
                "flex": "1 1 auto",
                "minHeight": "0",
                "border": "1px solid #e0e0e0",
                "borderRadius": "8px",
                "backgroundColor": "#fff",
            },
        ),
    ],
    style={
        "padding": "8px 12px",
        "height": "calc(100vh - 54px)",
        "display": "flex",
        "flexDirection": "column",
        "overflow": "hidden",
        "boxSizing": "border-box",
    },
)
