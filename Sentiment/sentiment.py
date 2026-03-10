"""
Sentiment analysis pipeline — reads unscored articles from the database,
calls the HuggingFace Inference API, and writes scores back to the DB.

Usage:
    python3 Sentiment/sentiment.py

Environment:
    HF_API_TOKEN  — HuggingFace API token (optional but strongly recommended to
                    avoid rate-limits; set as a GitHub Actions / Render secret)
"""

import os
import sqlite3
import sys
import time

import requests

HF_API_URL = "https://router.huggingface.co/hf-inference/models/cardiffnlp/twitter-roberta-base-sentiment-latest"
# Maximum characters sent to the API per article (well within the token limit)
MAX_CHARS = 1024

# Path to the database, relative to this script
_DB_PATH = os.path.normpath(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..", "Wordcloud", "wordcloud", "bbc_education_inclusion.db",
    )
)


def _hf_headers():
    token = os.environ.get("HF_API_TOKEN", "")
    return {"Authorization": f"Bearer {token}"} if token else {}


def analyse_text(text: str) -> dict | None:
    """Call the HuggingFace Inference API and return sentiment scores.

    Returns a dict with keys: negative, neutral, positive, top_label,
    top_score, sentiment_score (= positive − negative, range −1 to +1).
    Returns None on failure after 3 attempts.
    """
    payload = {"inputs": text[:MAX_CHARS]}
    for attempt in range(3):
        try:
            resp = requests.post(
                HF_API_URL, headers=_hf_headers(), json=payload, timeout=30
            )
            resp.raise_for_status()
            raw = resp.json()
            # Response shape: [[{"label": "negative", "score": 0.7}, ...]]
            items = raw[0] if isinstance(raw[0], list) else raw
            scores = {item["label"].lower(): item["score"] for item in items}
            neg = scores.get("negative", 0.0)
            neu = scores.get("neutral", 0.0)
            pos = scores.get("positive", 0.0)
            top_label = max(scores, key=scores.get)
            return {
                "negative": neg,
                "neutral": neu,
                "positive": pos,
                "top_label": top_label,
                "top_score": scores[top_label],
                "sentiment_score": pos - neg,  # directional: −1 to +1
            }
        except Exception as exc:
            print(f"  API error (attempt {attempt + 1}): {exc}")
            if attempt < 2:
                time.sleep(5)
    return None


def main():
    if not os.path.exists(_DB_PATH):
        print(f"Error: database not found at '{_DB_PATH}'")
        sys.exit(1)

    conn = sqlite3.connect(_DB_PATH)
    cursor = conn.cursor()

    # Fetch articles that haven't been scored yet, using excerpt as the text
    # source (falls back to summary if excerpt is empty)
    rows = cursor.execute(
        """
        SELECT article_id, url,
               COALESCE(NULLIF(TRIM(excerpt), ''), NULLIF(TRIM(summary), ''), title) AS text_source
        FROM   articles
        WHERE  sentiment_score IS NULL
          AND  COALESCE(NULLIF(TRIM(excerpt), ''), NULLIF(TRIM(summary), ''), title) IS NOT NULL
        """
    ).fetchall()

    if not rows:
        print("No unscored articles found — nothing to do.")
        conn.close()
        return

    print(f"Analysing {len(rows)} unscored article(s) via HuggingFace Inference API...\n")

    scored = 0
    for article_id, url, text_source in rows:
        result = analyse_text(text_source)
        if result is None:
            print(f"  [{article_id}] FAILED — skipped")
            continue

        cursor.execute(
            """
            UPDATE articles
            SET sentiment_score    = ?,
                sentiment_label    = ?,
                sentiment_negative = ?,
                sentiment_neutral  = ?,
                sentiment_positive = ?
            WHERE article_id = ?
            """,
            (
                result["sentiment_score"],
                result["top_label"],
                result["negative"],
                result["neutral"],
                result["positive"],
                article_id,
            ),
        )
        scored += 1
        print(
            f"  [{article_id}] {result['top_label']:8s}  "
            f"neg={result['negative']:.3f}  neu={result['neutral']:.3f}  "
            f"pos={result['positive']:.3f}  score={result['sentiment_score']:+.3f}"
        )

    conn.commit()
    conn.close()
    print(f"\nDone — {scored}/{len(rows)} article(s) scored and saved to database.")


if __name__ == "__main__":
    main()
