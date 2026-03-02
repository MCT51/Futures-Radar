"""Local server for interactive education-inclusion word cloud from SQLite."""

import argparse
import json
import re
import sqlite3
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


HTML_PAGE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Education Inclusion Word Cloud</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/echarts-wordcloud@2/dist/echarts-wordcloud.min.js"></script>
  <style>
    :root {
      --bg: #f4f7f2;
      --ink: #17301e;
      --accent: #1f7a44;
      --panel: #ffffff;
      --line: #d8e2d7;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: radial-gradient(1100px 500px at 10% 0%, #e8f5e7, transparent), var(--bg);
      color: var(--ink);
      font-family: "Source Sans 3", "Segoe UI", sans-serif;
      height: 100vh;
      overflow: hidden;
    }
    .layout {
      display: grid;
      grid-template-columns: 1.8fr 1fr;
      gap: 16px;
      padding: 18px;
      height: 100vh;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 12px;
      box-shadow: 0 8px 24px rgba(20, 45, 30, 0.08);
      min-height: 0;
    }
    .panel-right {
      display: flex;
      flex-direction: column;
    }
    #cloud {
      width: 100%;
      height: calc(100vh - 180px);
      min-height: 500px;
    }
    h1 {
      margin: 0 0 4px;
      font-size: 1.1rem;
      letter-spacing: 0.01em;
    }
    .meta { margin: 0 0 10px; font-size: 0.9rem; color: #4f6a59; }
    table { width: 100%; border-collapse: collapse; }
    .result-scroll {
      flex: 1 1 auto;
      min-height: 0;
      overflow-y: auto;
    }
    th, td {
      text-align: left;
      border-bottom: 1px solid var(--line);
      padding: 8px 6px;
      vertical-align: top;
      font-size: 0.9rem;
    }
    th { font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.03em; color: #44604e; }
    .snippet {
      margin-top: 6px;
      color: #365545;
      font-size: 0.82rem;
      line-height: 1.35;
    }
    .right-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
    }
    .blacklist-row {
      display: flex;
      gap: 8px;
      margin: 10px 0 8px;
      align-items: center;
      flex-wrap: wrap;
    }
    .blacklist-panel.hidden {
      display: none;
    }
    .blacklist-row input[type="text"] {
      min-width: 180px;
      padding: 4px 6px;
    }
    #blacklist-status {
      margin: 0 0 8px;
      font-size: 0.82rem;
      color: #365545;
    }
    .blacklist-list {
      margin: 0;
      padding: 0;
      list-style: none;
      border-top: 1px solid var(--line);
      max-height: 220px;
      overflow-y: auto;
    }
    .blacklist-item {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      border-bottom: 1px solid var(--line);
      padding: 6px 2px;
      font-size: 0.86rem;
    }
    .blacklist-term {
      font-family: "Source Sans 3", "Segoe UI", sans-serif;
      font-weight: 600;
      color: #213e2f;
    }
    #trend-panel.hidden {
      display: none;
    }
    #trend-chart {
      width: 100%;
      height: 260px;
      margin-top: 10px;
    }
    a { color: var(--accent); text-decoration: none; }
    a:hover { text-decoration: underline; }
    @media (max-width: 960px) {
      body { height: auto; overflow: auto; }
      .layout { grid-template-columns: 1.0fr; }
      .layout { height: auto; min-height: 100vh; }
      .panel-right { display: block; }
      .result-scroll { overflow: visible; }
      #cloud { height: 80vh; min-height: 520px; }
    }
  </style>
</head>
<body>
  <div class="layout">
    <section class="panel">
      <h1>Education & Inclusion Word Cloud</h1>
      <p class="meta">Click a word to view BBC article URLs and term frequencies.</p>
      <div style="display:flex; gap:8px; flex-wrap:wrap; margin: 10px 0 8px;">
        <label style="font-size:0.86rem;">
          From
          <input id="date-from" type="date" style="margin-left:4px;" />
        </label>
        <label style="font-size:0.86rem;">
          To
          <input id="date-to" type="date" style="margin-left:4px;" />
        </label>
        <button id="apply-filter" type="button">Apply</button>
        <button id="reset-filter" type="button">Reset</button>
      </div>
      <p class="meta" id="range-meta">Date filter: all time</p>
      <div id="cloud"></div>
    </section>
    <section class="panel panel-right">
      <div class="right-header">
        <h1 id="term-title">Select a Word</h1>
        <div style="display:flex; gap:8px;">
          <button id="toggle-trend" type="button">Trend</button>
          <button id="toggle-blacklist" type="button">Manage Blacklist</button>
        </div>
      </div>
      <div id="blacklist-panel" class="blacklist-panel hidden">
        <div class="blacklist-row">
          <input id="blacklist-term" type="text" placeholder="Add noisy term to blacklist" />
          <button id="add-blacklist" type="button">Add</button>
        </div>
        <p id="blacklist-status"></p>
        <ul id="blacklist-list" class="blacklist-list"></ul>
      </div>
      <div id="trend-panel" class="hidden">
        <h1 style="margin-top:14px;">Recent Window Trend</h1>
        <p class="meta" id="trend-meta">Shows rising terms in the latest temporal snapshot.</p>
        <div id="trend-chart"></div>
      </div>
      <p class="meta" id="term-meta">No term selected.</p>
      <div class="result-scroll">
        <table>
          <thead>
            <tr>
              <th>Frequency</th>
              <th>Published</th>
              <th>Article</th>
            </tr>
          </thead>
          <tbody id="result-body">
            <tr><td colspan="3">Click a word in the cloud.</td></tr>
          </tbody>
        </table>
      </div>
    </section>
  </div>

  <script>
    const chart = echarts.init(document.getElementById("cloud"));
    const trendChart = echarts.init(document.getElementById("trend-chart"));
    const termTitle = document.getElementById("term-title");
    const termMeta = document.getElementById("term-meta");
    const resultBody = document.getElementById("result-body");
    const trendMeta = document.getElementById("trend-meta");
    const trendPanel = document.getElementById("trend-panel");
    const toggleTrendBtn = document.getElementById("toggle-trend");
    const toggleBlacklistBtn = document.getElementById("toggle-blacklist");
    const blacklistPanel = document.getElementById("blacklist-panel");
    const blacklistTermInput = document.getElementById("blacklist-term");
    const addBlacklistBtn = document.getElementById("add-blacklist");
    const blacklistStatus = document.getElementById("blacklist-status");
    const blacklistList = document.getElementById("blacklist-list");
    const rangeMeta = document.getElementById("range-meta");
    const dateFromInput = document.getElementById("date-from");
    const dateToInput = document.getElementById("date-to");
    const applyFilterBtn = document.getElementById("apply-filter");
    const resetFilterBtn = document.getElementById("reset-filter");
    let currentTerm = "";

    function dateFilterParams() {
      const params = new URLSearchParams();
      if (dateFromInput.value) params.set("date_from", dateFromInput.value);
      if (dateToInput.value) params.set("date_to", dateToInput.value);
      return params;
    }

    function updateRangeMeta() {
      const from = dateFromInput.value || "start";
      const to = dateToInput.value || "now";
      if (!dateFromInput.value && !dateToInput.value) {
        rangeMeta.textContent = "Date filter: all time";
      } else {
        rangeMeta.textContent = `Date filter: ${from} to ${to}`;
      }
    }

    function renderRows(rows) {
      if (!rows.length) {
        resultBody.innerHTML = "<tr><td colspan='3'>No matching articles.</td></tr>";
        return;
      }
      resultBody.innerHTML = rows.map(row => `
        <tr>
          <td>${row.frequency}</td>
          <td>${row.published_date || row.published || ""}</td>
          <td>
            <a href="${row.url}" target="_blank" rel="noopener noreferrer">${row.title}</a>
            <div class="snippet">${row.context_snippet || ""}</div>
          </td>
        </tr>
      `).join("");
    }

    async function fetchTerms() {
      const params = dateFilterParams();
      const qs = params.toString() ? `?${params.toString()}` : "";
      const resp = await fetch(`/api/terms${qs}`);
      if (!resp.ok) throw new Error("Failed to load terms");
      return await resp.json();
    }

    async function fetchArticles(term) {
      const params = dateFilterParams();
      params.set("term", term);
      const resp = await fetch(`/api/articles?${params.toString()}`);
      if (!resp.ok) throw new Error("Failed to load articles");
      return await resp.json();
    }

    async function fetchDateBounds() {
      const resp = await fetch("/api/date-range");
      if (!resp.ok) return null;
      return await resp.json();
    }

    async function fetchTrends() {
      const resp = await fetch("/api/trends");
      if (!resp.ok) return null;
      return await resp.json();
    }

    async function addBlacklistTerm(term) {
      const resp = await fetch("/api/blacklist/add", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ term, reason: "manual-ui", source: "ui" })
      });
      if (!resp.ok) throw new Error("Failed to add blacklist term");
      return await resp.json();
    }

    async function fetchBlacklist() {
      const resp = await fetch("/api/blacklist");
      if (!resp.ok) throw new Error("Failed to load blacklist");
      return await resp.json();
    }

    async function deleteBlacklistTerm(term) {
      const resp = await fetch("/api/blacklist/delete", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ term })
      });
      if (!resp.ok) throw new Error("Failed to delete blacklist term");
      return await resp.json();
    }

    function renderBlacklist(items) {
      if (!items || !items.length) {
        blacklistList.innerHTML = "<li class='blacklist-item'>No blacklisted terms.</li>";
        return;
      }
      blacklistList.innerHTML = items.map(item => `
        <li class="blacklist-item">
          <span class="blacklist-term">${item.term}</span>
          <button class="delete-blacklist" data-term="${item.term}" type="button">Delete</button>
        </li>
      `).join("");
      blacklistList.querySelectorAll(".delete-blacklist").forEach(btn => {
        btn.addEventListener("click", async () => {
          const term = btn.getAttribute("data-term");
          try {
            const payload = await deleteBlacklistTerm(term);
            blacklistStatus.textContent = payload.message || `Removed '${term}' from blacklist.`;
            await refreshBlacklistAndCharts();
          } catch (err) {
            blacklistStatus.textContent = `Failed to remove '${term}'.`;
          }
        });
      });
    }

    async function refreshBlacklistAndCharts() {
      const [items, terms, trends] = await Promise.all([
        fetchBlacklist(),
        fetchTerms(),
        fetchTrends()
      ]);
      renderBlacklist(items);
      drawCloud(terms);
      drawTrend(trends);
    }

    function drawCloud(data) {
      // Keep fewer terms and larger spacing so smaller words remain selectable.
      const renderData = data.slice(0, 24);
      const maxRaw = Math.max(...renderData.map(d => d.value), 1);
      const scaledData = renderData.map(d => {
        const normalized = d.value / maxRaw;
        const boosted = Math.pow(normalized, 0.78); // Lift smaller words relative to dominant terms.
        return { ...d, raw_value: d.value, value: boosted * 26 };
      });

      const option = {
        tooltip: {
          formatter: p => `${p.name}<br/>total: ${p.data.raw_value}<br/>articles: ${p.data.article_count}`
        },
        series: [{
          type: "wordCloud",
          shape: "circle",
          width: "100%",
          height: "100%",
          sizeRange: [28, 180],
          rotationRange: [-20, 20],
          gridSize: 10,
          drawOutOfBound: false,
          shape: "circle",
          textStyle: {
            fontFamily: "monospace",
            fontWeight: "1000",
            color: () => {
              const palette = ["#1f7a44", "#1a5e73", "#6d5a1e", "#98323e", "#6c3b8a"];
              return palette[Math.floor(Math.random() * palette.length)];
            }
          },
          emphasis: {
            textStyle: { shadowBlur: 10, shadowColor: "rgba(0,0,0,0.22)" }
          },
          data: scaledData
        }]
      };
      chart.clear();
      chart.setOption(option, { notMerge: true, lazyUpdate: false });
      requestAnimationFrame(() => chart.resize());
      setTimeout(() => chart.resize(), 50);
    }

    function drawTrend(payload) {
      const entries = (payload && payload.entries) ? payload.entries : [];
      const labels = entries.map(e => e.term);
      const values = entries.map(e => e.delta_frequency);
      const pctMap = {};
      for (const e of entries) pctMap[e.term] = e.pct_change;

      trendMeta.textContent = entries.length
        ? `As of ${payload.as_of_date} | lookback: ${payload.lookback_days} days`
        : "No temporal trend rows yet. Run pipeline in non-preview mode first.";

      const option = {
        grid: { left: 120, right: 20, top: 20, bottom: 30 },
        xAxis: {
          type: "value",
          axisLabel: { color: "#355647" },
          splitLine: { lineStyle: { color: "#e2ebe1" } }
        },
        yAxis: {
          type: "category",
          data: labels,
          axisLabel: { color: "#355647", fontSize: 11 }
        },
        tooltip: {
          formatter: p => {
            const term = p.name;
            const pct = pctMap[term];
            const pctLabel = (pct === null || pct === undefined) ? "n/a" : `${pct}%`;
            return `${term}<br/>delta: ${p.value}<br/>pct change: ${pctLabel}`;
          }
        },
        series: [{
          type: "bar",
          data: values,
          itemStyle: {
            color: params => params.value >= 0 ? "#1f7a44" : "#9a3040"
          }
        }]
      };
      trendChart.setOption(option, { notMerge: true, lazyUpdate: false });
    }

    async function loadArticlesForTerm(term) {
      try {
        const rows = await fetchArticles(term);
        const articleCount = rows.length;
        const totalFrequency = rows.reduce((acc, row) => acc + row.frequency, 0);
        termTitle.textContent = term;
        termMeta.textContent = `${articleCount} articles | total frequency: ${totalFrequency}`;
        renderRows(rows);
      } catch (err) {
        termTitle.textContent = term;
        termMeta.textContent = "Failed to load article list.";
        resultBody.innerHTML = "<tr><td colspan='3'>Error loading rows.</td></tr>";
      }
    }

    chart.on("click", async params => {
      const term = params.name;
      currentTerm = term;
      await loadArticlesForTerm(term);
    });

    applyFilterBtn.addEventListener("click", async () => {
      updateRangeMeta();
      try {
        const terms = await fetchTerms();
        drawCloud(terms);
        if (currentTerm) {
          await loadArticlesForTerm(currentTerm);
        } else {
          termTitle.textContent = "Select a Word";
          termMeta.textContent = "No term selected.";
          resultBody.innerHTML = "<tr><td colspan='3'>Click a word in the cloud.</td></tr>";
        }
      } catch (err) {
        resultBody.innerHTML = "<tr><td colspan='3'>Failed to refresh with date filter.</td></tr>";
      }
    });

    resetFilterBtn.addEventListener("click", async () => {
      dateFromInput.value = "";
      dateToInput.value = "";
      updateRangeMeta();
      try {
        const terms = await fetchTerms();
        drawCloud(terms);
        if (currentTerm) {
          await loadArticlesForTerm(currentTerm);
        }
      } catch (err) {
        resultBody.innerHTML = "<tr><td colspan='3'>Failed to reset filter.</td></tr>";
      }
    });

    window.addEventListener("resize", () => {
      chart.resize();
      trendChart.resize();
    });

    toggleTrendBtn.addEventListener("click", () => {
      const isHidden = trendPanel.classList.contains("hidden");
      if (isHidden) {
        trendPanel.classList.remove("hidden");
        setTimeout(() => trendChart.resize(), 50);
      } else {
        trendPanel.classList.add("hidden");
      }
    });

    toggleBlacklistBtn.addEventListener("click", async () => {
      const hidden = blacklistPanel.classList.contains("hidden");
      if (hidden) {
        blacklistPanel.classList.remove("hidden");
        try {
          const items = await fetchBlacklist();
          renderBlacklist(items);
        } catch (err) {
          blacklistStatus.textContent = "Failed to load blacklist.";
        }
      } else {
        blacklistPanel.classList.add("hidden");
      }
    });

    addBlacklistBtn.addEventListener("click", async () => {
      const term = (blacklistTermInput.value || "").trim().toLowerCase();
      if (!term) {
        blacklistStatus.textContent = "Enter a term first.";
        return;
      }
      try {
        const payload = await addBlacklistTerm(term);
        blacklistStatus.textContent = payload.message || `Added '${term}' to blacklist.`;
        blacklistTermInput.value = "";
        await refreshBlacklistAndCharts();
        if (currentTerm === term) {
          currentTerm = "";
          termTitle.textContent = "Select a Word";
          termMeta.textContent = "No term selected.";
          resultBody.innerHTML = "<tr><td colspan='3'>Click a word in the cloud.</td></tr>";
        }
      } catch (err) {
        blacklistStatus.textContent = "Failed to add blacklist term.";
      }
    });

    (async () => {
      try {
        const bounds = await fetchDateBounds();
        if (bounds) {
          if (bounds.min_date) dateFromInput.min = bounds.min_date;
          if (bounds.max_date) dateFromInput.max = bounds.max_date;
          if (bounds.min_date) dateToInput.min = bounds.min_date;
          if (bounds.max_date) dateToInput.max = bounds.max_date;
        }
        updateRangeMeta();
        const terms = await fetchTerms();
        drawCloud(terms);
        const trends = await fetchTrends();
        drawTrend(trends);
      } catch (err) {
        resultBody.innerHTML = "<tr><td colspan='3'>Failed to load word cloud data.</td></tr>";
      }
    })();
  </script>
</body>
</html>
"""


def is_valid_iso_date(value: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", value or ""))


def build_date_where_clause(date_from: str | None, date_to: str | None) -> tuple[str, list[str]]:
    clauses = []
    args: list[str] = []
    if date_from:
        clauses.append("a.published_date >= ?")
        args.append(date_from)
    if date_to:
        clauses.append("a.published_date <= ?")
        args.append(date_to)
    if not clauses:
        return "", []
    return " AND " + " AND ".join(clauses), args


def query_terms(
    conn: sqlite3.Connection,
    min_frequency: int,
    limit: int,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    where_sql, date_args = build_date_where_clause(date_from, date_to)
    rows = conn.execute(
        """
        SELECT
            at.term AS term,
            SUM(at.frequency) AS total_frequency,
            COUNT(DISTINCT at.article_id) AS article_count
        FROM article_terms at
        JOIN articles a ON a.article_id = at.article_id
        LEFT JOIN term_blacklist tb ON tb.term = at.term
        WHERE 1=1
          AND tb.term IS NULL
        """ + where_sql + """
        GROUP BY at.term
        HAVING total_frequency >= ?
        ORDER BY total_frequency DESC, article_count DESC, at.term ASC
        LIMIT ?
        """,
        (*date_args, min_frequency, limit),
    ).fetchall()
    return [
        {"name": row["term"], "value": row["total_frequency"], "article_count": row["article_count"]}
        for row in rows
    ]


def query_articles_for_term(
    conn: sqlite3.Connection,
    term: str,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    where_sql, date_args = build_date_where_clause(date_from, date_to)
    rows = conn.execute(
        """
        SELECT
            a.url,
            a.title,
            a.published,
            a.published_date,
            at.frequency,
            COALESCE(at.context_snippet, '') AS context_snippet
        FROM article_terms at
        JOIN articles a ON a.article_id = at.article_id
        WHERE at.term = ?
        """ + where_sql + """
        ORDER BY at.frequency DESC, a.published DESC, a.title ASC
        """,
        (term.lower().strip(), *date_args),
    ).fetchall()
    return [dict(row) for row in rows]


def query_date_bounds(conn: sqlite3.Connection) -> dict:
    row = conn.execute(
        """
        SELECT MIN(published_date) AS min_date, MAX(published_date) AS max_date
        FROM articles
        WHERE published_date IS NOT NULL AND published_date != ''
        """
    ).fetchone()
    return {
        "min_date": row["min_date"] if row else None,
        "max_date": row["max_date"] if row else None,
    }


def query_trends(conn: sqlite3.Connection, lookback_days: int, trend_limit: int) -> dict:
    asof = conn.execute(
        "SELECT MAX(as_of_date) FROM term_temporal_stats",
    ).fetchone()
    as_of_date = asof[0] if asof else None
    if not as_of_date:
        return {"as_of_date": None, "lookback_days": lookback_days, "entries": []}

    rows = conn.execute(
        """
        SELECT
            tts.term,
            tts.delta_frequency,
            tts.pct_change,
            tts.freq_recent_window,
            tts.freq_previous_window
        FROM term_temporal_stats tts
        LEFT JOIN term_blacklist tb ON tb.term = tts.term
        WHERE as_of_date = ? AND lookback_days = ?
          AND tb.term IS NULL
        ORDER BY tts.delta_frequency DESC, tts.freq_recent_window DESC, tts.term ASC
        LIMIT ?
        """,
        (as_of_date, lookback_days, trend_limit),
    ).fetchall()
    return {
        "as_of_date": as_of_date,
        "lookback_days": lookback_days,
        "entries": [dict(row) for row in rows],
    }


def add_blacklist_term(conn: sqlite3.Connection, term: str, reason: str, source: str) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO term_blacklist (term, reason, source)
        VALUES (?, ?, ?)
        """,
        (term.lower().strip(), reason.strip() or "manual-ui", source.strip() or "ui"),
    )
    conn.commit()


def query_blacklist(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT term, reason, source, created_at
        FROM term_blacklist
        ORDER BY term ASC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def delete_blacklist_term(conn: sqlite3.Connection, term: str) -> None:
    conn.execute("DELETE FROM term_blacklist WHERE term = ?", (term.lower().strip(),))
    conn.commit()


def build_handler(
    db_path: Path,
    min_frequency: int,
    limit: int,
    trend_lookback_days: int,
    trend_limit: int,
):
    class Handler(BaseHTTPRequestHandler):
        def _write_json(self, payload: object, status: int = HTTPStatus.OK) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _write_html(self, html: str, status: int = HTTPStatus.OK) -> None:
            body = html.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            parsed = urlparse(self.path)

            if parsed.path == "/":
                self._write_html(HTML_PAGE)
                return

            if parsed.path == "/api/terms":
                params = parse_qs(parsed.query)
                date_from = (params.get("date_from", [""])[0] or "").strip()
                date_to = (params.get("date_to", [""])[0] or "").strip()
                if date_from and not is_valid_iso_date(date_from):
                    self._write_json({"error": "date_from must be YYYY-MM-DD"}, status=HTTPStatus.BAD_REQUEST)
                    return
                if date_to and not is_valid_iso_date(date_to):
                    self._write_json({"error": "date_to must be YYYY-MM-DD"}, status=HTTPStatus.BAD_REQUEST)
                    return
                try:
                    with sqlite3.connect(db_path) as conn:
                        conn.row_factory = sqlite3.Row
                        self._write_json(
                            query_terms(
                                conn,
                                min_frequency=min_frequency,
                                limit=limit,
                                date_from=date_from or None,
                                date_to=date_to or None,
                            )
                        )
                except sqlite3.Error as exc:
                    self._write_json({"error": f"SQLite error: {exc}"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
                return

            if parsed.path == "/api/articles":
                params = parse_qs(parsed.query)
                term = (params.get("term", [""])[0] or "").strip()
                date_from = (params.get("date_from", [""])[0] or "").strip()
                date_to = (params.get("date_to", [""])[0] or "").strip()
                if not term:
                    self._write_json({"error": "Missing 'term' query parameter."}, status=HTTPStatus.BAD_REQUEST)
                    return
                if date_from and not is_valid_iso_date(date_from):
                    self._write_json({"error": "date_from must be YYYY-MM-DD"}, status=HTTPStatus.BAD_REQUEST)
                    return
                if date_to and not is_valid_iso_date(date_to):
                    self._write_json({"error": "date_to must be YYYY-MM-DD"}, status=HTTPStatus.BAD_REQUEST)
                    return
                try:
                    with sqlite3.connect(db_path) as conn:
                        conn.row_factory = sqlite3.Row
                        self._write_json(
                            query_articles_for_term(
                                conn,
                                term,
                                date_from=date_from or None,
                                date_to=date_to or None,
                            )
                        )
                except sqlite3.Error as exc:
                    self._write_json({"error": f"SQLite error: {exc}"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
                return

            if parsed.path == "/api/date-range":
                try:
                    with sqlite3.connect(db_path) as conn:
                        conn.row_factory = sqlite3.Row
                        self._write_json(query_date_bounds(conn))
                except sqlite3.Error as exc:
                    self._write_json({"error": f"SQLite error: {exc}"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
                return

            if parsed.path == "/api/trends":
                try:
                    with sqlite3.connect(db_path) as conn:
                        conn.row_factory = sqlite3.Row
                        self._write_json(
                            query_trends(
                                conn,
                                lookback_days=trend_lookback_days,
                                trend_limit=trend_limit,
                            )
                        )
                except sqlite3.Error as exc:
                    self._write_json({"error": f"SQLite error: {exc}"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
                return

            if parsed.path == "/api/blacklist":
                try:
                    with sqlite3.connect(db_path) as conn:
                        conn.row_factory = sqlite3.Row
                        self._write_json(query_blacklist(conn))
                except sqlite3.Error as exc:
                    self._write_json({"error": f"SQLite error: {exc}"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
                return

            self._write_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path not in {"/api/blacklist/add", "/api/blacklist/delete"}:
                self._write_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
                return

            try:
                content_length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                content_length = 0
            raw_body = self.rfile.read(content_length) if content_length > 0 else b""
            try:
                payload = json.loads(raw_body.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                self._write_json({"error": "Invalid JSON payload"}, status=HTTPStatus.BAD_REQUEST)
                return

            term = (payload.get("term") or "").strip().lower()
            if not term:
                self._write_json({"error": "Missing term"}, status=HTTPStatus.BAD_REQUEST)
                return
            if not re.fullmatch(r"[a-z0-9][a-z0-9\\- ]{1,80}", term):
                self._write_json({"error": "Invalid term format"}, status=HTTPStatus.BAD_REQUEST)
                return

            try:
                with sqlite3.connect(db_path) as conn:
                    if parsed.path == "/api/blacklist/add":
                        reason = (payload.get("reason") or "manual-ui").strip()
                        source = (payload.get("source") or "ui").strip()
                        add_blacklist_term(conn, term=term, reason=reason, source=source)
                        self._write_json({"ok": True, "message": f"Added '{term}' to blacklist."})
                    else:
                        delete_blacklist_term(conn, term=term)
                        self._write_json({"ok": True, "message": f"Removed '{term}' from blacklist."})
            except sqlite3.Error as exc:
                self._write_json({"error": f"SQLite error: {exc}"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

        def log_message(self, fmt: str, *args) -> None:
            return

    return Handler


def validate_database(db_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    with sqlite3.connect(db_path) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        article_columns = {row[1] for row in conn.execute("PRAGMA table_info(articles)")}
    required = {"articles", "article_terms", "term_stats", "term_blacklist"}
    missing = required - tables
    if missing:
        raise RuntimeError(f"Missing required tables in {db_path}: {', '.join(sorted(missing))}")
    if "published_date" not in article_columns:
        raise RuntimeError(
            "Database schema is missing articles.published_date. "
            "Run bbc_inclusion_signals.py once to upgrade/populate the schema, then restart this server."
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Interactive word cloud server for education-inclusion term stats.")
    parser.add_argument("--db-path", default="bbc_education_inclusion.db", help="Path to SQLite DB")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind")
    parser.add_argument("--port", type=int, default=8765, help="Port to bind")
    parser.add_argument("--min-frequency", type=int, default=2, help="Minimum total term frequency for cloud")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of terms in cloud")
    parser.add_argument("--trend-lookback-days", type=int, default=7, help="Lookback window for trend panel")
    parser.add_argument("--trend-limit", type=int, default=12, help="Max terms shown in trend panel")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    db_path = Path(args.db_path)
    validate_database(db_path)

    handler = build_handler(
        db_path=db_path,
        min_frequency=args.min_frequency,
        limit=args.limit,
        trend_lookback_days=args.trend_lookback_days,
        trend_limit=args.trend_limit,
    )
    server = ThreadingHTTPServer((args.host, args.port), handler)

    print(f"Serving word cloud at http://{args.host}:{args.port}")
    print(f"Using database: {db_path}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
