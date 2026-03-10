
"""Filter BBC UK RSS to education-inclusion articles and persist article/word stats to SQLite."""

import argparse
import csv
import hashlib
import json
import re
import sqlite3
from collections import Counter
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, List, Tuple

import feedparser
import requests
from googlenewsdecoder import gnewsdecoder
from bs4 import BeautifulSoup


# -----------------------------
# Configuration and vocab lists
# -----------------------------
DEFAULT_FEED_URLS = [
    "https://feeds.bbci.co.uk/news/uk/rss.xml",
    "https://feeds.bbci.co.uk/news/education/rss.xml",
    #"https://news.google.com/rss/search?q=SEND;+education&hl=en-GB&gl=GB&ceid=GB:en",
]
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_CSV = str(BASE_DIR / "bbc_education_inclusion_signals.csv")
DEFAULT_DB_PATH = str(BASE_DIR / "bbc_education_inclusion.db")
DEFAULT_DISCOVERED_OUTPUT_CSV = str(BASE_DIR / "discovered_terms_preview.csv")
REQUEST_TIMEOUT = 20
MAX_EXCERPT_CHARS = 1200
MAX_SUMMARY_CHARS = 750
DISCOVERED_NGRAM_MAX = 3
DISCOVERED_MIN_TOKEN_LEN = 4

STOPWORDS = {
    "about", "after", "also", "been", "before", "between", "could", "from", "have",
    "just", "more", "news", "over", "said", "says", "than", "that", "their", "them",
    "there", "these", "they", "this", "those", "through", "under", "were", "what",
    "when", "where", "which", "while", "will", "with", "would",
}

DISCOVERY_STOPWORDS = STOPWORDS.union(
    {
        "again",
        "being",
        "first",
        "last",
        "made",
        "many",
        "most",
        "people",
        "still",
        "years",
        "year",
        "time",
    }
)

DEFAULT_NOISY_TERMS = {
    "according",
    "announcement",
    "announced",
    "britain",
    "country",
    "editor",
    "government",
    "local",
    "minister",
    "ministers",
    "month",
    "national",
    "news",
    "official",
    "officials",
    "people",
    "public",
    "reported",
    "reportedly",
    "reports",
    "today",
    "week",
    "year",
    "years",
}

EDUCATION_TERMS = [
    "education", "school", "schools", "pupil", "pupils", "student", "students",
    "teacher", "teachers", "classroom", "college", "university", "curriculum",
    "gcse", "a-level", "exam", "ofsted", "department for education", "dfe",
]

INCLUSION_TERMS = [
    "inclusion", "inclusive", "equity", "equality", "disadvantaged", "deprived",
    "special educational needs", "send", "sen", "ehcp", "ehc plan", "disability",
    "disabled", "autism", "adhd", "dyslexia", "visual impairment", "hearing impairment",
    "accessibility", "accessible", "reasonable adjustments", "assistive technology",
    "school exclusion", "excluded", "suspension", "absence", "attendance gap",
    "discrimination", "racism", "barrier", "braille", "sign language", "bsl",
]

# Domain terms are tracked for word-cloud frequency and drill-down to source articles.
DOMAIN_TERMS = sorted(set(EDUCATION_TERMS + INCLUSION_TERMS + [
    "attendance", "attainment", "backlog", "funding", "grant", "intervention",
    "inequality", "policy", "reform", "support", "tribunal", "workforce",
]))


# -----------------------------
# Feed ingestion and extraction
# -----------------------------
def stable_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


def normalize_published_date(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    try:
        dt = parsedate_to_datetime(raw)
        return dt.date().isoformat()
    except Exception:
        pass
    match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", raw)
    if match:
        return match.group(1)
    return ""


def dedupe_items(items: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for item in items:
        key = item["url"].strip().lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out

def convert_google_news_link(google_news_url):
    try:
        decoded_url = gnewsdecoder(google_news_url, interval=1)

        if decoded_url.get("status"):
            return decoded_url["decoded_url"]
        else:
            return "ERROR"
           
    except Exception as e:
        print(f"Error occurred: {e}")
        return "ERROR"

def fetch_rss_items(feed_url: str, max_items: int) -> List[Dict]:
    feed = feedparser.parse(feed_url)
    items = []

    for entry in feed.entries[:max_items]:
        url = entry.get("link", "")
        if not url:
            continue
        if "news.google.com" in url:
            url = convert_google_news_link(url)
            if url == "ERROR":
                continue
            
            
        items.append(
            {
                "id": entry.get("id") or stable_id(url),
                "title": entry.get("title", "").strip(),
                "summary": (entry.get("summary", "") or "").strip(),
                "url": url,
                "published": entry.get("published", "") or entry.get("updated", ""),
                "published_date": normalize_published_date(
                    entry.get("published", "") or entry.get("updated", "")
                ),
                "source": feed_url.split("/")[2] if feed_url.startswith("http") else "unknown",
            }
        )
    return dedupe_items(items)


def fetch_rss_items_from_feeds(feed_urls: List[str], max_items: int) -> List[Dict]:
    all_items: List[Dict] = []
    for feed_url in feed_urls:
        all_items.extend(fetch_rss_items(feed_url, max_items=max_items))
    return dedupe_items(all_items)


def extract_article_text(url: str, timeout: int = REQUEST_TIMEOUT) -> str:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; FutureRadarBot/1.0)"}
    response = requests.get(url, timeout=timeout, headers=headers)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "footer", "nav"]):
        tag.decompose()

    root = soup.select_one("article") or soup.select_one("main") or soup

    parts = []
    bbc_blocks = root.select("div[data-component='text-block'] p")
    if bbc_blocks:
        for p in bbc_blocks:
            txt = p.get_text(" ", strip=True)
            if len(txt) >= 25:
                parts.append(txt)
    else:
        for n in root.find_all(["h1", "h2", "h3", "p", "li"]):
            txt = n.get_text(" ", strip=True)
            if len(txt) >= 25:
                parts.append(txt)

    text = "\n".join(parts)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


# ---------------------------------
# Relevance filter and term builders
# ---------------------------------
def keyword_hits(text: str, terms: List[str]) -> List[str]:
    text_low = text.lower()
    return [term for term in terms if term in text_low]


def fallback_keywords_from_text(text: str, top_n: int = 20) -> List[str]:
    tokens = re.findall(r"[a-z]{4,}", text.lower())
    tokens = [t for t in tokens if t not in STOPWORDS]
    return [w for w, _ in Counter(tokens).most_common(top_n)]


def summarize_text(text: str, max_sentences: int = 3, max_chars: int = MAX_SUMMARY_CHARS) -> str:
    sentences = re.split(r"(?<=[.!?])\s+", text.strip())
    chosen = []
    total_chars = 0
    for sentence in sentences:
        s = sentence.strip()
        if len(s) < 40:
            continue
        if total_chars + len(s) > max_chars and chosen:
            break
        chosen.append(s)
        total_chars += len(s)
        if len(chosen) >= max_sentences:
            break
    return " ".join(chosen) if chosen else text[:max_chars].strip()


def is_education_inclusion_relevant(item: Dict, text: str) -> Tuple[bool, Dict[str, List[str]]]:
    combined = " ".join([item.get("title", ""), item.get("summary", ""), text[:15000]]).lower()
    education_hits = keyword_hits(combined, EDUCATION_TERMS)
    inclusion_hits = keyword_hits(combined, INCLUSION_TERMS)
    return bool(education_hits and inclusion_hits), {
        "education_terms": education_hits,
        "inclusion_terms": inclusion_hits,
    }


def count_domain_terms(text: str, domain_terms: List[str]) -> Dict[str, int]:
    text_low = text.lower()
    counts: Dict[str, int] = {}
    for term in domain_terms:
        pattern = r"\b" + re.escape(term.lower()) + r"\b"
        freq = len(re.findall(pattern, text_low))
        if freq > 0:
            counts[term.lower()] = freq
    return counts


def split_sentences(text: str) -> List[str]:
    parts = re.split(r"(?<=[.!?])\s+", (text or "").strip())
    return [p.strip() for p in parts if p.strip()]


def context_snippet_for_term(
    text: str,
    term: str,
    max_sentences: int = 2,
    max_chars: int = 420,
) -> str:
    sentences = split_sentences(text)
    if not sentences:
        return ""

    pattern = re.compile(r"\b" + re.escape(term.lower()) + r"\b", flags=re.IGNORECASE)
    matched = [s for s in sentences if pattern.search(s)]
    chosen = matched[:max_sentences] if matched else sentences[:max_sentences]
    snippet = " ".join(chosen).strip()
    return snippet[:max_chars].strip()


def build_term_records(text: str, term_counts: Dict[str, int]) -> List[Tuple[str, int, str]]:
    records: List[Tuple[str, int, str]] = []
    for term, frequency in term_counts.items():
        if frequency <= 0:
            continue
        records.append((term, frequency, context_snippet_for_term(text, term)))
    return records


def filter_term_counts(term_counts: Dict[str, int], blacklist_terms: set[str]) -> Dict[str, int]:
    return {
        term: count
        for term, count in term_counts.items()
        if term.lower().strip() not in blacklist_terms
    }


def tokenize_for_discovery(text: str) -> List[str]:
    tokens = re.findall(r"[a-z][a-z\-]{2,}", text.lower())
    cleaned = []
    for token in tokens:
        t = token.strip("-")
        if len(t) < DISCOVERED_MIN_TOKEN_LEN:
            continue
        if t in DISCOVERY_STOPWORDS:
            continue
        cleaned.append(t)
    return cleaned


def count_discovered_terms(text: str, max_ngram: int = DISCOVERED_NGRAM_MAX) -> Dict[str, int]:
    tokens = tokenize_for_discovery(text)
    counts: Counter = Counter()
    for n in range(1, max_ngram + 1):
        if len(tokens) < n:
            break
        for i in range(len(tokens) - n + 1):
            gram_tokens = tokens[i : i + n]
            phrase = " ".join(gram_tokens).strip()
            if not phrase:
                continue
            # Skip ngrams with all identical tokens (e.g., "school school").
            if len(set(gram_tokens)) == 1 and n > 1:
                continue
            counts[phrase] += 1
    return dict(counts)


def merge_term_counts(primary: Dict[str, int], secondary: Dict[str, int]) -> Dict[str, int]:
    merged = dict(primary)
    for term, count in secondary.items():
        merged[term] = merged.get(term, 0) + count
    return merged


def select_active_term_counts(
    baseline_counts: Dict[str, int],
    discovered_counts: Dict[str, int],
    term_mode: str,
) -> Dict[str, int]:
    if term_mode == "baseline":
        return baseline_counts
    if term_mode == "discovered":
        return discovered_counts
    if term_mode == "hybrid":
        return merge_term_counts(baseline_counts, discovered_counts)
    raise ValueError(f"Unsupported term mode: {term_mode}")


# -----------------------------------------
# Database setup, migrations, and seed data
# -----------------------------------------
def initialize_database(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS articles (
            article_id TEXT PRIMARY KEY,
            published TEXT,
            published_date TEXT,
            title TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE,
            source TEXT,
            summary TEXT,
            excerpt TEXT,
            keywords_json TEXT,
            education_hits_json TEXT,
            inclusion_hits_json TEXT,
            ingested_at TEXT DEFAULT CURRENT_TIMESTAMP,
            sentiment_score REAL,
            sentiment_label TEXT
        );

        CREATE TABLE IF NOT EXISTS article_terms (
            article_id TEXT NOT NULL,
            term TEXT NOT NULL,
            frequency INTEGER NOT NULL CHECK (frequency > 0),
            context_snippet TEXT,
            PRIMARY KEY (article_id, term),
            FOREIGN KEY (article_id) REFERENCES articles(article_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS term_stats (
            term TEXT PRIMARY KEY,
            total_frequency INTEGER NOT NULL,
            article_count INTEGER NOT NULL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS discovered_article_terms (
            article_id TEXT NOT NULL,
            term TEXT NOT NULL,
            ngram INTEGER NOT NULL CHECK (ngram >= 1),
            frequency INTEGER NOT NULL CHECK (frequency > 0),
            context_snippet TEXT,
            PRIMARY KEY (article_id, term),
            FOREIGN KEY (article_id) REFERENCES articles(article_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS discovered_term_stats (
            term TEXT PRIMARY KEY,
            ngram INTEGER NOT NULL,
            total_frequency INTEGER NOT NULL,
            article_count INTEGER NOT NULL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS term_blacklist (
            term TEXT PRIMARY KEY,
            reason TEXT,
            source TEXT DEFAULT 'seed',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS term_temporal_stats (
            term TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            lookback_days INTEGER NOT NULL,
            freq_recent_window INTEGER NOT NULL,
            freq_previous_window INTEGER NOT NULL,
            article_count_recent_window INTEGER NOT NULL,
            article_count_previous_window INTEGER NOT NULL,
            delta_frequency INTEGER NOT NULL,
            pct_change REAL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (term, as_of_date, lookback_days)
        );

        CREATE INDEX IF NOT EXISTS idx_article_terms_term ON article_terms(term);
        CREATE INDEX IF NOT EXISTS idx_discovered_article_terms_term ON discovered_article_terms(term);
        CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(url);
        CREATE INDEX IF NOT EXISTS idx_temporal_delta ON term_temporal_stats(delta_frequency);
        CREATE INDEX IF NOT EXISTS idx_temporal_asof ON term_temporal_stats(as_of_date);
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(articles)")}
    if "published_date" not in columns:
        conn.execute("ALTER TABLE articles ADD COLUMN published_date TEXT")
    article_terms_columns = {row[1] for row in conn.execute("PRAGMA table_info(article_terms)")}
    if "context_snippet" not in article_terms_columns:
        conn.execute("ALTER TABLE article_terms ADD COLUMN context_snippet TEXT")
    discovered_terms_columns = {row[1] for row in conn.execute("PRAGMA table_info(discovered_article_terms)")}
    if "context_snippet" not in discovered_terms_columns:
        conn.execute("ALTER TABLE discovered_article_terms ADD COLUMN context_snippet TEXT")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_published_date ON articles(published_date)")
    conn.executemany(
        """
        INSERT OR IGNORE INTO term_blacklist (term, reason, source)
        VALUES (?, ?, 'seed')
        """,
        [(term, "general/noisy term") for term in sorted(DEFAULT_NOISY_TERMS)],
    )
    return conn


# ----------------------------------
# Persistence helpers (row-level I/O)
# ----------------------------------
def upsert_article(conn: sqlite3.Connection, row: Dict) -> None:
    existing = conn.execute(
        "SELECT article_id FROM articles WHERE url = ?",
        (row["url"],),
    ).fetchone()
    if existing:
        # Keep the canonical ID already used in DB for this URL.
        row["id"] = existing[0]

    conn.execute(
        """
        INSERT INTO articles (
            article_id, published, published_date, title, url, source, summary, excerpt, keywords_json,
            education_hits_json, inclusion_hits_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(article_id) DO UPDATE SET
            published = excluded.published,
            published_date = excluded.published_date,
            title = excluded.title,
            url = excluded.url,
            source = excluded.source,
            summary = excluded.summary,
            excerpt = excluded.excerpt,
            keywords_json = excluded.keywords_json,
            education_hits_json = excluded.education_hits_json,
            inclusion_hits_json = excluded.inclusion_hits_json
        """,
        (
            row["id"],
            row["published"],
            row.get("published_date", ""),
            row["title"],
            row["url"],
            row["source"],
            row["summary"],
            row["article_excerpt"],
            json.dumps(row["article_keywords"], ensure_ascii=False),
            json.dumps(row["education_hits"], ensure_ascii=False),
            json.dumps(row["inclusion_hits"], ensure_ascii=False),
        ),
    )


def load_blacklist_terms(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT term FROM term_blacklist").fetchall()
    db_terms = {(row[0] or "").strip().lower() for row in rows}
    return {t for t in db_terms if t}


def replace_article_terms(conn: sqlite3.Connection, article_id: str, term_records: List[Tuple[str, int, str]]) -> None:
    conn.execute("DELETE FROM article_terms WHERE article_id = ?", (article_id,))
    conn.executemany(
        """
        INSERT INTO article_terms (article_id, term, frequency, context_snippet)
        VALUES (?, ?, ?, ?)
        """,
        [(article_id, term, frequency, snippet) for term, frequency, snippet in term_records],
    )


def upsert_article_and_terms(conn: sqlite3.Connection, row: Dict, term_records: List[Tuple[str, int, str]]) -> None:
    upsert_article(conn, row)
    replace_article_terms(conn, row["id"], term_records)


def upsert_discovered_terms(
    conn: sqlite3.Connection,
    article_id: str,
    discovered_records: List[Tuple[str, int, str]],
) -> None:
    conn.execute("DELETE FROM discovered_article_terms WHERE article_id = ?", (article_id,))
    conn.executemany(
        """
        INSERT INTO discovered_article_terms (article_id, term, ngram, frequency, context_snippet)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (article_id, term, len(term.split()), frequency, snippet)
            for term, frequency, snippet in discovered_records
            if frequency > 0
        ],
    )


# ------------------------------------
# Aggregate/statistics refresh routines
# ------------------------------------
def refresh_term_stats(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM term_stats")
    conn.execute(
        """
        INSERT INTO term_stats (term, total_frequency, article_count, updated_at)
        SELECT term, SUM(frequency) AS total_frequency, COUNT(DISTINCT article_id) AS article_count, CURRENT_TIMESTAMP
        FROM article_terms
        GROUP BY term
        """
    )


def refresh_discovered_term_stats(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM discovered_term_stats")
    conn.execute(
        """
        INSERT INTO discovered_term_stats (term, ngram, total_frequency, article_count, updated_at)
        SELECT
            term,
            MAX(ngram) AS ngram,
            SUM(frequency) AS total_frequency,
            COUNT(DISTINCT article_id) AS article_count,
            CURRENT_TIMESTAMP
        FROM discovered_article_terms
        GROUP BY term
        """
    )


def refresh_term_temporal_stats(conn: sqlite3.Connection, lookback_days: int = 7) -> None:
    conn.execute(
        "DELETE FROM term_temporal_stats WHERE as_of_date = date('now') AND lookback_days = ?",
        (lookback_days,),
    )
    recent_start = f"-{lookback_days} days"
    previous_start = f"-{lookback_days * 2} days"
    conn.execute(
        """
        INSERT INTO term_temporal_stats (
            term,
            as_of_date,
            lookback_days,
            freq_recent_window,
            freq_previous_window,
            article_count_recent_window,
            article_count_previous_window,
            delta_frequency,
            pct_change,
            updated_at
        )
        SELECT
            at.term AS term,
            date('now') AS as_of_date,
            ? AS lookback_days,
            SUM(
                CASE
                    WHEN a.published_date >= date('now', ?) AND a.published_date <= date('now')
                    THEN at.frequency ELSE 0
                END
            ) AS freq_recent_window,
            SUM(
                CASE
                    WHEN a.published_date >= date('now', ?)
                     AND a.published_date < date('now', ?)
                    THEN at.frequency ELSE 0
                END
            ) AS freq_previous_window,
            COUNT(
                DISTINCT CASE
                    WHEN a.published_date >= date('now', ?) AND a.published_date <= date('now')
                    THEN at.article_id ELSE NULL
                END
            ) AS article_count_recent_window,
            COUNT(
                DISTINCT CASE
                    WHEN a.published_date >= date('now', ?)
                     AND a.published_date < date('now', ?)
                    THEN at.article_id ELSE NULL
                END
            ) AS article_count_previous_window,
            (
                SUM(
                    CASE
                        WHEN a.published_date >= date('now', ?) AND a.published_date <= date('now')
                        THEN at.frequency ELSE 0
                    END
                ) -
                SUM(
                    CASE
                        WHEN a.published_date >= date('now', ?)
                         AND a.published_date < date('now', ?)
                        THEN at.frequency ELSE 0
                    END
                )
            ) AS delta_frequency,
            CASE
                WHEN SUM(
                    CASE
                        WHEN a.published_date >= date('now', ?)
                         AND a.published_date < date('now', ?)
                        THEN at.frequency ELSE 0
                    END
                ) > 0
                THEN ROUND(
                    (
                        (
                            SUM(
                                CASE
                                    WHEN a.published_date >= date('now', ?) AND a.published_date <= date('now')
                                    THEN at.frequency ELSE 0
                                END
                            ) -
                            SUM(
                                CASE
                                    WHEN a.published_date >= date('now', ?)
                                     AND a.published_date < date('now', ?)
                                    THEN at.frequency ELSE 0
                                END
                            )
                        ) * 100.0
                    ) /
                    SUM(
                        CASE
                            WHEN a.published_date >= date('now', ?)
                             AND a.published_date < date('now', ?)
                            THEN at.frequency ELSE 0
                        END
                    ),
                    2
                )
                ELSE NULL
            END AS pct_change,
            CURRENT_TIMESTAMP AS updated_at
        FROM article_terms at
        JOIN articles a ON a.article_id = at.article_id
        WHERE a.published_date IS NOT NULL AND a.published_date != ''
        GROUP BY at.term
        HAVING freq_recent_window > 0 OR freq_previous_window > 0
        """,
        (
            lookback_days,
            recent_start,
            previous_start,
            recent_start,
            recent_start,
            previous_start,
            recent_start,
            recent_start,
            previous_start,
            recent_start,
            previous_start,
            recent_start,
            recent_start,
            previous_start,
            recent_start,
            previous_start,
            recent_start,
        ),
    )


# --------------------------
# Reporting/export utilities
# --------------------------
def top_discovered_terms(
    conn: sqlite3.Connection, min_frequency: int = 2, top_n: int = 30
) -> List[Dict[str, int]]:
    rows = conn.execute(
        """
        SELECT term, ngram, total_frequency, article_count
        FROM discovered_term_stats
        WHERE total_frequency >= ?
        ORDER BY total_frequency DESC, article_count DESC, term ASC
        LIMIT ?
        """,
        (min_frequency, top_n),
    ).fetchall()
    return [
        {
            "term": row[0],
            "ngram": row[1],
            "total_frequency": row[2],
            "article_count": row[3],
        }
        for row in rows
    ]


def write_csv(rows: List[Dict], output_csv: str) -> None:
    fields = [
        "id",
        "published",
        "published_date",
        "title",
        "url",
        "source",
        "summary",
        "relevance_hits",
        "article_keywords",
        "article_excerpt",
    ]
    def _serialize_row(row: Dict) -> Dict:
        out = dict(row)
        out["relevance_hits"] = json.dumps(
            {"education_terms": row["education_hits"], "inclusion_terms": row["inclusion_hits"]},
            ensure_ascii=False,
        )
        out["article_keywords"] = json.dumps(row["article_keywords"], ensure_ascii=False)
        return {k: out.get(k, "") for k in fields}

    # Cumulative CSV behavior:
    # - keep existing rows
    # - upsert rows from current run by `id` (fallback to `url`)
    csv_path = Path(output_csv)
    merged: Dict[str, Dict] = {}

    if csv_path.exists():
        with open(output_csv, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = row.get("id") or row.get("url") or ""
                if not key:
                    continue
                merged[key] = {k: row.get(k, "") for k in fields}

    for row in rows:
        serialized = _serialize_row(row)
        key = serialized.get("id") or serialized.get("url") or ""
        if not key:
            continue
        merged[key] = serialized

    final_rows = list(merged.values())
    final_rows.sort(key=lambda r: (r.get("published_date", ""), r.get("published", "")), reverse=True)

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(final_rows)


def write_discovered_terms_csv(
    conn: sqlite3.Connection, output_csv: str, min_frequency: int = 2
) -> None:
    rows = conn.execute(
        """
        SELECT term, ngram, total_frequency, article_count
        FROM discovered_term_stats
        WHERE total_frequency >= ?
        ORDER BY total_frequency DESC, article_count DESC, term ASC
        """,
        (min_frequency,),
    ).fetchall()

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["term", "ngram", "total_frequency", "article_count"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "term": row[0],
                    "ngram": row[1],
                    "total_frequency": row[2],
                    "article_count": row[3],
                }
            )


# --------------------------
# End-to-end pipeline runner
# --------------------------
def run_pipeline(
    feed_urls: List[str],
    search_terms: List[str],
    output_csv: str,
    db_path: str,
    max_items: int,
    term_mode: str,
    preview_only: bool,
    discovered_output_csv: str,
    discovered_min_frequency: int,
    preview_discovered_top: int,
    temporal_lookback_days: int,
) -> None:
    
    if search_terms[0] != "":
        for search_term in search_terms:
            google_news_feed = f"https://news.google.com/rss/search?q={requests.utils.quote(search_term)}&hl=en-GB&gl=GB&ceid=GB:en"
            if google_news_feed not in feed_urls:
                feed_urls.append(google_news_feed)

    items = fetch_rss_items_from_feeds(feed_urls, max_items=max_items)
    rows = []
    errors = []
    saved_articles = 0
    conn = initialize_database(db_path)

    try:
        blacklist_terms = load_blacklist_terms(conn)
        for item in items:
            try:
                print(f"Processing article: {item['title']} ({item['url']})")
                text = extract_article_text(item["url"])
            except Exception as exc:
                errors.append({"url": item["url"], "error": str(exc)})
                continue

            if not text:
                continue

            is_relevant, hits = is_education_inclusion_relevant(item, text)
            if not is_relevant:
                continue

            row = {
                "id": item["id"],
                "published": item["published"],
                "published_date": item.get("published_date", ""),
                "title": item["title"],
                "url": item["url"],
                "source": item["source"],
                "summary": summarize_text(text),
                "education_hits": hits["education_terms"],
                "inclusion_hits": hits["inclusion_terms"],
                "article_keywords": fallback_keywords_from_text(text),
                "article_excerpt": text[:MAX_EXCERPT_CHARS],
            }
            baseline_counts = filter_term_counts(
                count_domain_terms(text, DOMAIN_TERMS),
                blacklist_terms=blacklist_terms,
            )
            discovered_counts = filter_term_counts(
                count_discovered_terms(text),
                blacklist_terms=blacklist_terms,
            )
            active_term_counts = select_active_term_counts(
                baseline_counts=baseline_counts,
                discovered_counts=discovered_counts,
                term_mode=term_mode,
            )
            discovered_records = build_term_records(text, discovered_counts)
            active_term_records = build_term_records(text, active_term_counts)

            # Ensure parent article row exists for both preview and production writes.
            upsert_article(conn, row)
            # Always persist candidate discovered terms for side-by-side review.
            upsert_discovered_terms(conn, row["id"], discovered_records)
            if not preview_only:
                replace_article_terms(conn, row["id"], active_term_records)
            rows.append(row)
            saved_articles += 1

        refresh_discovered_term_stats(conn)
        if not preview_only:
            refresh_term_stats(conn)
            refresh_term_temporal_stats(conn, lookback_days=temporal_lookback_days)
        conn.commit()
        if discovered_output_csv:
            write_discovered_terms_csv(
                conn,
                output_csv=discovered_output_csv,
                min_frequency=discovered_min_frequency,
            )
        discovered_preview = top_discovered_terms(
            conn,
            min_frequency=discovered_min_frequency,
            top_n=preview_discovered_top,
        )
        conn.commit()
    finally:
        conn.close()

    if not preview_only:
        write_csv(rows, output_csv)

    print(f"Fetched items: {len(items)}")
    print(f"Relevant articles saved: {saved_articles}")
    print(f"Extraction errors: {len(errors)}")
    if preview_only:
        print("Preview-only mode: production tables 'article_terms' and 'term_stats' were not modified.")
        print("Preview data available in 'discovered_article_terms' and 'discovered_term_stats'.")
    else:
        print(f"CSV output: {output_csv}")
    print(f"SQLite output: {db_path}")
    if discovered_output_csv:
        print(f"Discovered terms CSV: {discovered_output_csv}")
    if not preview_only:
        print(f"Temporal change window: {temporal_lookback_days} days")

    if discovered_preview:
        print(f"Top discovered terms (min_frequency={discovered_min_frequency}):")
        for entry in discovered_preview:
            print(
                f"- {entry['term']} (n={entry['ngram']}): "
                f"freq={entry['total_frequency']}, articles={entry['article_count']}"
            )

    if errors:
        print("Sample errors:")
        for err in errors[:5]:
            print(f"- {err['url']} :: {err['error']}")


# ----------------------
# CLI argument definition
# ----------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Filter BBC UK RSS to education/inclusion articles and persist article/term frequencies."
    )
    parser.add_argument(
        "--feed-url",
        action="append",
        dest="feed_urls",
        help="RSS feed URL. Repeat this flag to add multiple feeds.",
    )
    parser.add_argument("--search-terms", default=[""], help='add google news search feeds (e.g. "special educational needs")')
    parser.add_argument("--output", default=DEFAULT_OUTPUT_CSV, help="Output CSV path")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, help="SQLite output path")
    parser.add_argument("--max-items", type=int, default=80, help="Max RSS items to process")
    parser.add_argument(
        "--term-mode",
        choices=["baseline", "discovered", "hybrid"],
        default="baseline",
        help="Term source for production word counts.",
    )
    parser.add_argument(
        "--preview-only",
        action="store_true",
        help="Only build discovered-term preview tables; do not update production word-count tables.",
    )
    parser.add_argument(
        "--discovered-output-csv",
        default=DEFAULT_DISCOVERED_OUTPUT_CSV,
        help="CSV path for aggregated discovered terms preview.",
    )
    parser.add_argument(
        "--discovered-min-frequency",
        type=int,
        default=2,
        help="Minimum frequency threshold for discovered terms preview output.",
    )
    parser.add_argument(
        "--preview-discovered-top",
        type=int,
        default=30,
        help="Number of top discovered terms to print after run.",
    )
    parser.add_argument(
        "--temporal-lookback-days",
        type=int,
        default=7,
        help="Window size in days for term temporal change features.",
    )
    return parser


# -------------------
# Script entry point
# -------------------
def main() -> None:
    args = build_parser().parse_args()
    feed_urls = args.feed_urls if args.feed_urls else DEFAULT_FEED_URLS
    run_pipeline(
        feed_urls=feed_urls,
        search_terms=args.search_terms,
        output_csv=args.output,
        db_path=args.db_path,
        max_items=args.max_items,
        term_mode=args.term_mode,
        preview_only=args.preview_only,
        discovered_output_csv=args.discovered_output_csv,
        discovered_min_frequency=args.discovered_min_frequency,
        preview_discovered_top=args.preview_discovered_top,
        temporal_lookback_days=args.temporal_lookback_days,
    )


if __name__ == "__main__":
    main()
