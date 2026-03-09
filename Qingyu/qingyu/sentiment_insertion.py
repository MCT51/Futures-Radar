import csv
import sqlite3
from typing import List



DEFAULT_DB_PATH = "Qingyu/qingyu/bbc_education_inclusion.db"
DEFAULT_SENTIMENT_RESULTS_PATH = "Sentiment/sentiment_results.csv"



def insert_article_sentiment_into_db(db_path: str = DEFAULT_DB_PATH, sentiment_results_path: str = DEFAULT_SENTIMENT_RESULTS_PATH):
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        with open(sentiment_results_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row["file"].split("-")[-1].replace(".txt", "").replace("(())", "&").replace("()", "?").replace("(_)", "/").replace("(__)", ":") # Reconstruct URL from filename
                
                if row["chunk"] != "avg":  # Only insert the average sentiment for each article
                    continue
                sentiment_score = float(row["top_score"])
                sentiment_label = row["top_label"]
                cursor.execute("""
                    UPDATE articles
                    SET sentiment_score = ?, sentiment_label = ?
                    WHERE url = ?
                """, (sentiment_score, sentiment_label, url))

                
        conn.commit()
    finally:
        conn.close()


def main():
    insert_article_sentiment_into_db()


if __name__ == "__main__":
    main()