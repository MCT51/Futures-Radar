import csv
import sqlite3
from pathlib import Path
from typing import List
from bbc_inclusion_signals import extract_article_text


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = str(BASE_DIR / "bbc_education_inclusion.db")
DEFAULT_SENTIMENT_RESULTS_PATH = "Sentiment/sentiment_results1.csv"


def create_txt_files_from_articles(articles: List[tuple]): ##(excerpt, year, month, day)
    for article in articles:
        month = article[2]
        year = article[1]
        day = article[3]
        id = article[0].replace("?", "()").replace("&", "(())").replace("/", "(_)").replace(":", "(__)") # Use the last part of the URL as an ID
        filename = f"Sentiment/articles/{year}-{month}-{day}-{id}.txt"
        text = extract_article_text(article[0])
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(text)

def fetch_links_from_db(db_path: str = DEFAULT_DB_PATH) -> List[tuple]:
    
    records: List[tuple] = []
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT url, published_date FROM articles WHERE sentiment_score IS NULL") # Fetch only articles that haven't been processed for sentiment
        for row in cursor.fetchall():
            if not row or not row[0]:
                continue
            pub = row[1] or ""
            year = ""
            month = ""
            if pub:
                parts = pub.split("-")
                if len(parts) >= 2:
                    year = parts[0]
                    month = parts[1]
                    if len(parts) >= 3:
                        day = parts[2]
                    else: continue  # If day is missing, skip this record as it doesn't fit the expected format
            else:  continue
            
            records.append((row[0], year, month, day))  # row[0] is url
    finally:
        conn.close()
    return records

def main():
    articles = fetch_links_from_db()
    create_txt_files_from_articles(articles)

if __name__ == "__main__":
    main()
