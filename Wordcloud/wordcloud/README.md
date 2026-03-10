# Future Radar: BBC Education & Inclusion Signal Pipeline

This project ingests BBC RSS articles, filters for education + inclusion relevance, extracts keywords/summaries, stores term frequencies in SQLite, and serves an interactive word cloud with date filters, snippets, and trend view.

## Files

- `bbc_inclusion_signals.py`: ingestion + filtering + CSV export + SQLite updates
- `wordcloud_server.py`: local web server for word cloud, article drill-down, and trend chart
- `bbc_education_inclusion.db`: SQLite database output
- `bbc_education_inclusion_signals.csv`: article-level CSV output
- `discovered_terms_preview.csv`: discovered-term preview output

## 1) Run the Pipeline

### Recommended production run (stable vocabulary)

```bash
python3 bbc_inclusion_signals.py --term-mode baseline
```

This will:
- fetch RSS from:
  - `https://feeds.bbci.co.uk/news/uk/rss.xml`
  - `https://feeds.bbci.co.uk/news/education/rss.xml`
- keep only relevant education + inclusion articles
- write/update:
  - `bbc_education_inclusion_signals.csv` (cumulative upsert; existing rows retained, matching rows updated)
  - `bbc_education_inclusion.db`

### Main options

```bash
python3 bbc_inclusion_signals.py --help
```

Important flags:
- `--term-mode baseline|discovered|hybrid`
- `--preview-only` (does not update production `article_terms` / `term_stats`)
- `--temporal-lookback-days N` (default `7`)
- `--max-items N`
- `--db-path PATH`
- `--output PATH`

### Example: baseline run

```bash
python3 bbc_inclusion_signals.py --term-mode baseline --temporal-lookback-days 7\
                              --db-path /your/local/dbpath
```

## 2) Run the Word Cloud App

```bash
python3 wordcloud_server.py --db-path bbc_education_inclusion.db --port 8765  \ --trend-lookback-days 7 \
--trend-limit 12
```

Open:
- `http://127.0.0.1:8765`

UI features:
- date range filter (`From` / `To`)
- clickable word cloud
- article list with:
  - URL
  - frequency in article
  - published date
  - context snippet
- trend panel (toggle with `Trend` button) with recent-window bar chart
- blacklist manager (toggle with `Manage Blacklist` button):
  - view current blacklisted terms
  - add term
  - delete term

Server options:

```bash
python3 wordcloud_server.py --help
```

Important flags:
- `--min-frequency`
- `--limit`
- `--trend-lookback-days`
- `--trend-limit`

## 3) Database Schema

### `articles`
Stores article metadata and extracted text artifacts.

Columns:
- `article_id` (PK)
- `published`
- `published_date` (`YYYY-MM-DD` when available)
- `title`
- `url` (UNIQUE)
- `source`
- `summary`
- `excerpt`
- `keywords_json`
- `education_hits_json`
- `inclusion_hits_json`
- `ingested_at`

### `article_terms`
Production term counts per article (based on selected term mode when not preview-only).

Columns:
- `article_id` (FK -> `articles.article_id`)
- `term`
- `frequency`
- `context_snippet` (up to ~2 relevant sentences)

PK: (`article_id`, `term`)

### `term_stats`
Aggregated production frequencies for word cloud.

Columns:
- `term` (PK)
- `total_frequency`
- `article_count`
- `updated_at`

### `discovered_article_terms`
Discovered n-gram counts per article (always populated for preview/inspection).

Columns:
- `article_id` (FK -> `articles.article_id`)
- `term`
- `ngram`
- `frequency`
- `context_snippet`

PK: (`article_id`, `term`)

### `discovered_term_stats`
Aggregated discovered-term frequencies.

Columns:
- `term` (PK)
- `ngram`
- `total_frequency`
- `article_count`
- `updated_at`

### `term_blacklist`
Noisy/general terms to exclude from term counting.

Columns:
- `term` (PK)
- `reason`
- `source`
- `created_at`

### `term_temporal_stats`
Recent-vs-previous window trend features for production terms.

Columns:
- `term`
- `as_of_date`
- `lookback_days`
- `freq_recent_window`
- `freq_previous_window`
- `article_count_recent_window`
- `article_count_previous_window`
- `delta_frequency`
- `pct_change`
- `updated_at`

PK: (`term`, `as_of_date`, `lookback_days`)

## 4) Useful SQL Queries

### Top cloud terms

```sql
SELECT term, total_frequency, article_count
FROM term_stats
ORDER BY total_frequency DESC;
```

### Click-through article list for one term

```sql
SELECT a.url, a.title, a.published_date, at.frequency, at.context_snippet
FROM article_terms at
JOIN articles a ON a.article_id = at.article_id
WHERE at.term = 'inclusion'
ORDER BY at.frequency DESC, a.published_date DESC;
```

### Top rising terms (today snapshot, 7-day lookback)

```sql
SELECT term, delta_frequency, pct_change, freq_recent_window, freq_previous_window
FROM term_temporal_stats
WHERE as_of_date = date('now') AND lookback_days = 7
ORDER BY delta_frequency DESC
LIMIT 30;
```

### Manage blacklist

You can manage blacklist terms directly from the UI (`Manage Blacklist` button).
If needed, SQL still works:

```sql
INSERT OR IGNORE INTO term_blacklist(term, reason, source)
VALUES ('achievement', 'too generic', 'manual');
```

```sql
SELECT term, reason, source FROM term_blacklist ORDER BY term;
```

## 5) Typical Workflow

1. Run ingestion (`baseline` mode).
2. Launch `wordcloud_server.py`.
3. Explore cloud + date filters; toggle `Trend` when needed.
4. Use `Manage Blacklist` in the UI to add/remove noisy terms.
5. Re-run ingestion to refresh CSV/DB and temporal trend stats.
