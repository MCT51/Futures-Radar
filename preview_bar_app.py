"""
Standalone preview app for dashboard pages.

Run:
    python3 preview_bar_app.py

Then open:
    http://127.0.0.1:8051/bar
"""

from dash import Dash


# Minimal standalone app that renders the bar page directly at "/".
app = Dash(__name__, suppress_callback_exceptions=True)
app.title = "Bar Page Preview"

import pages.bar as bar_page  # noqa: F401

app.layout = bar_page.layout


if __name__ == "__main__":
    names = list(bar_page._datasets.keys())
    if not names:
        raise RuntimeError(
            "No datasets loaded by pages.bar. "
            "Expected StructuredData JSON files under Ingestion/test/output/."
        )
    print(f"Loaded bar datasets ({len(names)}): {', '.join(names)}")
    app.run(debug=True, port=8051)
