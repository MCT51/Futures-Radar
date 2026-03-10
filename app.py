"""
Education Futures Radar — unified dashboard entry point.

Run from the repo root:
    python app.py

Pages are auto-discovered from the pages/ directory by Dash.
"""

import re
import sqlite3
from pathlib import Path

import dash
from dash import Dash, html, page_container, page_registry
from flask import Response, jsonify, request

from Wordcloud.wordcloud.wordcloud_server import (
    HTML_PAGE as WORDCLOUD_HTML,
    add_blacklist_term,
    delete_blacklist_term,
    query_articles_for_term,
    query_blacklist,
    query_date_bounds,
    query_terms,
    query_trends,
    validate_database,
)

app = Dash(__name__, use_pages=True)
server = app.server

_REPO_ROOT = Path(__file__).resolve().parent
_WORDCLOUD_DB_PATH = _REPO_ROOT / "Wordcloud" / "wordcloud" / "bbc_education_inclusion.db"
_WORDCLOUD_MIN_FREQUENCY = 2
_WORDCLOUD_LIMIT = 200
_WORDCLOUD_TREND_LOOKBACK_DAYS = 7
_WORDCLOUD_TREND_LIMIT = 12

_WORDCLOUD_ERROR: str | None = None
try:
    validate_database(_WORDCLOUD_DB_PATH)
except Exception as exc:
    _WORDCLOUD_ERROR = str(exc)

# Keep the original standalone HTML/JS, but mount APIs under /wordcloud/api/*.
_WORDCLOUD_DASH_HTML = WORDCLOUD_HTML.replace("/api/", "/wordcloud/api/")


def _is_valid_iso_date(value: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", value or ""))


def _wordcloud_unavailable_response():
    return jsonify({"error": f"Word cloud unavailable: {_WORDCLOUD_ERROR}"}), 503


@server.get("/wordcloud/embed")
def wordcloud_index():
    if _WORDCLOUD_ERROR:
        message = (
            "<h2>Word Cloud Unavailable</h2>"
            f"<p>{_WORDCLOUD_ERROR}</p>"
            f"<p>Expected DB path: {_WORDCLOUD_DB_PATH}</p>"
        )
        return Response(message, mimetype="text/html; charset=utf-8", status=503)
    return Response(_WORDCLOUD_DASH_HTML, mimetype="text/html; charset=utf-8")


@server.get("/wordcloud/api/terms")
def wordcloud_terms():
    if _WORDCLOUD_ERROR:
        return _wordcloud_unavailable_response()
    date_from = (request.args.get("date_from", "") or "").strip()
    date_to = (request.args.get("date_to", "") or "").strip()
    if date_from and not _is_valid_iso_date(date_from):
        return jsonify({"error": "date_from must be YYYY-MM-DD"}), 400
    if date_to and not _is_valid_iso_date(date_to):
        return jsonify({"error": "date_to must be YYYY-MM-DD"}), 400
    try:
        with sqlite3.connect(_WORDCLOUD_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            payload = query_terms(
                conn,
                min_frequency=_WORDCLOUD_MIN_FREQUENCY,
                limit=_WORDCLOUD_LIMIT,
                date_from=date_from or None,
                date_to=date_to or None,
            )
            return jsonify(payload)
    except sqlite3.Error as exc:
        return jsonify({"error": f"SQLite error: {exc}"}), 500


@server.get("/wordcloud/api/articles")
def wordcloud_articles():
    if _WORDCLOUD_ERROR:
        return _wordcloud_unavailable_response()
    term = (request.args.get("term", "") or "").strip()
    if not term:
        return jsonify({"error": "Missing 'term' query parameter."}), 400
    date_from = (request.args.get("date_from", "") or "").strip()
    date_to = (request.args.get("date_to", "") or "").strip()
    if date_from and not _is_valid_iso_date(date_from):
        return jsonify({"error": "date_from must be YYYY-MM-DD"}), 400
    if date_to and not _is_valid_iso_date(date_to):
        return jsonify({"error": "date_to must be YYYY-MM-DD"}), 400
    try:
        with sqlite3.connect(_WORDCLOUD_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            payload = query_articles_for_term(
                conn,
                term=term,
                date_from=date_from or None,
                date_to=date_to or None,
            )
            return jsonify(payload)
    except sqlite3.Error as exc:
        return jsonify({"error": f"SQLite error: {exc}"}), 500


@server.get("/wordcloud/api/date-range")
def wordcloud_date_range():
    if _WORDCLOUD_ERROR:
        return _wordcloud_unavailable_response()
    try:
        with sqlite3.connect(_WORDCLOUD_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            return jsonify(query_date_bounds(conn))
    except sqlite3.Error as exc:
        return jsonify({"error": f"SQLite error: {exc}"}), 500


@server.get("/wordcloud/api/trends")
def wordcloud_trends():
    if _WORDCLOUD_ERROR:
        return _wordcloud_unavailable_response()
    try:
        with sqlite3.connect(_WORDCLOUD_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            payload = query_trends(
                conn,
                lookback_days=_WORDCLOUD_TREND_LOOKBACK_DAYS,
                trend_limit=_WORDCLOUD_TREND_LIMIT,
            )
            return jsonify(payload)
    except sqlite3.Error as exc:
        return jsonify({"error": f"SQLite error: {exc}"}), 500


@server.get("/wordcloud/api/blacklist")
def wordcloud_blacklist():
    if _WORDCLOUD_ERROR:
        return _wordcloud_unavailable_response()
    try:
        with sqlite3.connect(_WORDCLOUD_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            return jsonify(query_blacklist(conn))
    except sqlite3.Error as exc:
        return jsonify({"error": f"SQLite error: {exc}"}), 500


@server.post("/wordcloud/api/blacklist/add")
def wordcloud_blacklist_add():
    if _WORDCLOUD_ERROR:
        return _wordcloud_unavailable_response()
    payload = request.get_json(silent=True) or {}
    term = (payload.get("term") or "").strip().lower()
    if not term:
        return jsonify({"error": "Missing term"}), 400
    if not re.fullmatch(r"[a-z0-9][a-z0-9\- ]{1,80}", term):
        return jsonify({"error": "Invalid term format"}), 400
    reason = (payload.get("reason") or "manual-ui").strip()
    source = (payload.get("source") or "ui").strip()
    try:
        with sqlite3.connect(_WORDCLOUD_DB_PATH) as conn:
            add_blacklist_term(conn, term=term, reason=reason, source=source)
            return jsonify({"ok": True, "message": f"Added '{term}' to blacklist."})
    except sqlite3.Error as exc:
        return jsonify({"error": f"SQLite error: {exc}"}), 500


@server.post("/wordcloud/api/blacklist/delete")
def wordcloud_blacklist_delete():
    if _WORDCLOUD_ERROR:
        return _wordcloud_unavailable_response()
    payload = request.get_json(silent=True) or {}
    term = (payload.get("term") or "").strip().lower()
    if not term:
        return jsonify({"error": "Missing term"}), 400
    if not re.fullmatch(r"[a-z0-9][a-z0-9\- ]{1,80}", term):
        return jsonify({"error": "Invalid term format"}), 400
    try:
        with sqlite3.connect(_WORDCLOUD_DB_PATH) as conn:
            delete_blacklist_term(conn, term=term)
            return jsonify({"ok": True, "message": f"Removed '{term}' from blacklist."})
    except sqlite3.Error as exc:
        return jsonify({"error": f"SQLite error: {exc}"}), 500

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
    import os
    port = int(os.environ.get("PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=False)
