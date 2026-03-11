import csv
import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = str(BASE_DIR / "bbc_education_inclusion.db")
DEFAULT_SENTIMENT_RESULTS_PATH = "Sentiment/sentiment_results.csv"


def insert_article_sentiment_into_db(
    db_path: str = DEFAULT_DB_PATH,
    sentiment_results_path: str = DEFAULT_SENTIMENT_RESULTS_PATH,
):
    conn = sqlite3.connect(db_path)
    inserted = 0
    try:
        cursor = conn.cursor()
        with open(sentiment_results_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["chunk"] != "avg":
                    continue

                # Reconstruct URL from encoded filename
                url = (
                    row["file"]
                    .split("-", 2)[-1]          # strip YYYY-MM- prefix
                    .replace(".txt", "")
                    .replace("(())", "&")
                    .replace("()", "?")
                    .replace("(_)", "/")
                    .replace("(__)", ":")
                )

                neg = float(row["negative"])
                neu = float(row["neutral"])
                pos = float(row["positive"])
                label = row["top_label"]
                # Directional score: positive end of spectrum → +1, negative → −1
                score = pos - neg

                cursor.execute(
                    """
                    UPDATE articles
                    SET sentiment_score    = ?,
                        sentiment_label    = ?,
                        sentiment_negative = ?,
                        sentiment_neutral  = ?,
                        sentiment_positive = ?
                    WHERE url = ?
                    """,
                    (score, label, neg, neu, pos, url),
                )
                if cursor.rowcount:
                    inserted += 1

        conn.commit()
        print(f"Updated sentiment for {inserted} article(s).")
    finally:
        conn.close()

def test_if_sentiment_insertion_works():
    # This is a simple test function to check if the sentiment insertion works correctly.
    # It will print out the sentiment scores for all articles in the database after insertion.
    conn = sqlite3.connect(DEFAULT_DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT url, sentiment_score, sentiment_label FROM articles")
        for row in cursor.fetchall():
            print(f"URL: {row[0]}, Sentiment Score: {row[1]}, Sentiment Label: {row[2]}")
    finally:
        conn.close()

def main():
    test_if_sentiment_insertion_works()  # Run this test before insertion to see initial state


if __name__ == "__main__":
    main()
