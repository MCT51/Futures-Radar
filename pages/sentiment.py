"""
Sentiment timeline page — average sentiment scores over time.

Reads from the articles database (Wordcloud/wordcloud/bbc_education_inclusion.db).
Groups scored articles by month and plots the averaged sentiment score as a
line chart.  sentiment_score = positive − negative, range −1 (very negative)
to +1 (very positive).

Topic filter supports three modes:
  OR      — articles mentioning any selected term
  AND     — articles mentioning all selected terms simultaneously
  Compare — one line per term overlaid on the same chart
"""

import sqlite3
from pathlib import Path

import dash
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html

dash.register_page(__name__, path="/sentiment", name="Sentiment Timeline")

_DB_PATH = Path(__file__).resolve().parents[1] / "Wordcloud" / "wordcloud" / "bbc_education_inclusion.db"

_CHART_COLORS = [
    "#2980b9", "#27ae60", "#e67e22", "#8e44ad",
    "#c0392b", "#16a085", "#d35400", "#2c3e50",
]


def _get_terms() -> list[dict]:
    """Return dropdown options ordered by article count."""
    if not _DB_PATH.exists():
        return []
    try:
        conn = sqlite3.connect(_DB_PATH)
        rows = conn.execute(
            """
            SELECT term, COUNT(DISTINCT article_id) AS n
            FROM article_terms
            GROUP BY term
            ORDER BY n DESC
            """
        ).fetchall()
        conn.close()
        return [{"label": f"{term} ({n})", "value": term} for term, n in rows]
    except Exception:
        return []


def _query_all() -> pd.DataFrame | None:
    """Monthly average across all scored articles."""
    if not _DB_PATH.exists():
        return None
    try:
        conn = sqlite3.connect(_DB_PATH)
        df = pd.read_sql_query(
            """
            SELECT published_date, sentiment_score
            FROM articles
            WHERE sentiment_score IS NOT NULL
              AND published_date  IS NOT NULL
            """,
            conn,
        )
        conn.close()
    except Exception:
        return None
    if df.empty:
        return None
    return _monthly(df)


def _query_or(terms: list[str]) -> pd.DataFrame | None:
    """Articles mentioning ANY of the terms."""
    try:
        conn = sqlite3.connect(_DB_PATH)
        placeholders = ",".join("?" * len(terms))
        df = pd.read_sql_query(
            f"""
            SELECT DISTINCT a.published_date, a.sentiment_score
            FROM articles a
            JOIN article_terms at ON a.article_id = at.article_id
            WHERE at.term IN ({placeholders})
              AND a.sentiment_score IS NOT NULL
              AND a.published_date  IS NOT NULL
            """,
            conn,
            params=terms,
        )
        conn.close()
    except Exception:
        return None
    return _monthly(df) if not df.empty else None


def _query_and(terms: list[str]) -> pd.DataFrame | None:
    """Articles mentioning ALL of the terms."""
    try:
        conn = sqlite3.connect(_DB_PATH)
        placeholders = ",".join("?" * len(terms))
        df = pd.read_sql_query(
            f"""
            SELECT a.published_date, a.sentiment_score
            FROM articles a
            JOIN article_terms at ON a.article_id = at.article_id
            WHERE at.term IN ({placeholders})
              AND a.sentiment_score IS NOT NULL
              AND a.published_date  IS NOT NULL
            GROUP BY a.article_id, a.published_date, a.sentiment_score
            HAVING COUNT(DISTINCT at.term) = {len(terms)}
            """,
            conn,
            params=terms,
        )
        conn.close()
    except Exception:
        return None
    return _monthly(df) if not df.empty else None


def _query_per_term(terms: list[str]) -> dict[str, pd.DataFrame]:
    """One monthly DataFrame per term, for Compare mode."""
    result = {}
    try:
        conn = sqlite3.connect(_DB_PATH)
        for term in terms:
            df = pd.read_sql_query(
                """
                SELECT DISTINCT a.published_date, a.sentiment_score
                FROM articles a
                JOIN article_terms at ON a.article_id = at.article_id
                WHERE at.term = ?
                  AND a.sentiment_score IS NOT NULL
                  AND a.published_date  IS NOT NULL
                """,
                conn,
                params=[term],
            )
            if not df.empty:
                result[term] = _monthly(df)
        conn.close()
    except Exception:
        pass
    return result


def _monthly(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["month"] = df["published_date"].str[:7]
    return (
        df.groupby("month")["sentiment_score"]
        .mean()
        .reset_index()
        .sort_values("month")
    )


def _base_layout(fig: go.Figure) -> go.Figure:
    fig.add_hline(y=0, line_dash="dash", line_color="#aaa", line_width=1)
    fig.update_layout(
        xaxis=dict(tickformat="%Y-%m", title="Month"),
        yaxis=dict(range=[-1, 1], tickformat=".1f", title="← negative   |   positive →"),
        margin=dict(l=40, r=20, t=60, b=40),
        hovermode="x unified",
        legend=dict(orientation="h", y=-0.2),
    )
    return fig


def _empty_fig(msg: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        title=msg,
        yaxis=dict(range=[-1, 1]),
        annotations=[dict(
            text="No scored articles found for this selection.",
            x=0.5, y=0.5, xref="paper", yref="paper",
            showarrow=False, font=dict(size=13, color="#888"),
        )],
    )
    return fig


# ── Layout ───────────────────────────────────────────────────────────────────
layout = html.Div(
    [
        html.H2("Sentiment Timeline"),
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Filter by topic:", style={"fontWeight": "500", "marginBottom": "4px"}),
                        dcc.Dropdown(
                            id="sentiment-topic-filter",
                            options=_get_terms(),
                            multi=True,
                            placeholder="Search topics (e.g. autism, SEND, Ofsted)…",
                            style={"fontSize": "0.9rem"},
                        ),
                    ],
                    style={"flex": "1", "minWidth": "280px"},
                ),
                html.Div(
                    [
                        html.Label("Mode:", style={"fontWeight": "500", "marginBottom": "4px"}),
                        dcc.RadioItems(
                            id="sentiment-mode",
                            options=[
                                {"label": " OR  ", "value": "or"},
                                {"label": " AND  ", "value": "and"},
                                {"label": " Compare", "value": "compare"},
                            ],
                            value="or",
                            inline=True,
                            style={"fontSize": "0.9rem", "marginTop": "6px"},
                        ),
                    ],
                    style={"marginLeft": "24px"},
                ),
            ],
            style={"display": "flex", "alignItems": "flex-end", "marginBottom": "8px", "flexWrap": "wrap", "gap": "8px"},
        ),
        html.P(id="sentiment-status", style={"color": "#555", "fontSize": "0.9rem", "marginBottom": "4px"}),
        dcc.Graph(id="sentiment-graph", style={"height": "62vh"}),
    ],
    style={"padding": "16px"},
)


# ── Callback ─────────────────────────────────────────────────────────────────
@callback(
    Output("sentiment-graph", "figure"),
    Output("sentiment-status", "children"),
    Input("sentiment-topic-filter", "value"),
    Input("sentiment-mode", "value"),
)
def update_chart(selected_terms, mode):
    terms = selected_terms or []

    # ── No filter: show all articles ─────────────────────────────────────────
    if not terms:
        monthly = _query_all()
        if monthly is None:
            return _empty_fig("Sentiment Timeline"), "No scored articles yet."
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=monthly["month"], y=monthly["sentiment_score"],
            mode="lines+markers", name="All topics",
            line=dict(color="#555"),
            marker=dict(
                color=monthly["sentiment_score"].apply(lambda s: "#27ae60" if s >= 0 else "#c0392b"),
                size=8,
            ),
        ))
        fig.update_layout(title="Monthly Sentiment Score — All topics")
        return _base_layout(fig), f"{len(monthly)} monthly data point(s) · all articles"

    # ── Compare: one line per term ────────────────────────────────────────────
    if mode == "compare":
        per_term = _query_per_term(terms)
        if not per_term:
            return _empty_fig("Compare — no data"), "No data for selected topics."
        fig = go.Figure()
        for i, (term, monthly) in enumerate(per_term.items()):
            colour = _CHART_COLORS[i % len(_CHART_COLORS)]
            fig.add_trace(go.Scatter(
                x=monthly["month"], y=monthly["sentiment_score"],
                mode="lines+markers", name=term,
                line=dict(color=colour),
                marker=dict(color=colour, size=7),
            ))
        fig.update_layout(title="Monthly Sentiment — Compare topics")
        status = f"Comparing: {', '.join(per_term.keys())}"
        return _base_layout(fig), status

    # ── OR / AND ──────────────────────────────────────────────────────────────
    monthly = _query_or(terms) if mode == "or" else _query_and(terms)
    if monthly is None:
        mode_label = "any of" if mode == "or" else "all of"
        return _empty_fig(f"No articles found for {mode_label}: {', '.join(terms)}"), ""

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=monthly["month"], y=monthly["sentiment_score"],
        mode="lines+markers", name=", ".join(terms),
        line=dict(color="#2980b9"),
        marker=dict(
            color=monthly["sentiment_score"].apply(lambda s: "#27ae60" if s >= 0 else "#c0392b"),
            size=8,
        ),
    ))
    mode_label = "any of" if mode == "or" else "all of"
    fig.update_layout(title=f"Monthly Sentiment — {mode_label}: {', '.join(terms)}")
    status = f"{len(monthly)} monthly data point(s) · {mode_label}: {', '.join(terms)}"
    return _base_layout(fig), status
