"""
Standalone preview app for the line page.

Run:
    python3 preview_line_app.py

Then open:
    http://127.0.0.1:8052/line
"""

from dash import Dash


# Minimal standalone app that renders the line page directly at "/".
app = Dash(__name__, suppress_callback_exceptions=True)
app.title = "Line Page Preview"

import pages.line as line_page  # noqa: F401

app.layout = line_page.layout


if __name__ == "__main__":
    names = list(line_page._datasets.keys())
    if not names:
        raise RuntimeError(
            "No datasets loaded by pages.line. "
            "Expected StructuredData JSON files under Ingestion/test/output/."
        )
    print(f"Loaded line datasets ({len(names)}): {', '.join(names)}")
    app.run(debug=True, port=8052)
