import os
DEFAULT_SENTIMENT_RESULTS_PATH = "Sentiment/sentiment_results.csv"

def cleanup_sentiment_results(sentiment_results_path: str = DEFAULT_SENTIMENT_RESULTS_PATH):
   #remove contents of the sentiment results file
    with open(sentiment_results_path, "w", encoding="utf-8") as f:
        f.seek(0)  
        f.truncate()  
        f.write("file,chunk,top_label,top_score,negative,neutral,positive\n")  # Write the header back to the file
        f.close()

def clean_articles_folder(articles_folder: str = "Sentiment/articles"):
    # Remove all files in the articles folder
    for filename in os.listdir(articles_folder):
        file_path = os.path.join(articles_folder, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)

def main():
    cleanup_sentiment_results()
    clean_articles_folder()

if __name__ == "__main__":
    main()